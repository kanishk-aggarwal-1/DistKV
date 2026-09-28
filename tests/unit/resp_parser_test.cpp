#include "protocol/resp_parser.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace kv::resp {
namespace {

// Feeds `chunks` one at a time, the way a connection does: bytes are
// appended to a buffer, commands are parsed out, and consumed bytes are
// discarded. Returns every command parsed.
std::vector<Command> feed(Parser& parser, const std::vector<std::string>& chunks) {
  std::vector<Command> commands;
  std::string buffer;
  for (const std::string& chunk : chunks) {
    buffer += chunk;
    while (true) {
      Command cmd;
      Parser::Result r = parser.parse(buffer, cmd);
      buffer.erase(0, r.consumed);
      EXPECT_NE(r.status, Parser::Status::kError) << parser.error();
      if (r.status != Parser::Status::kCommand) break;
      commands.push_back(std::move(cmd));
    }
  }
  EXPECT_TRUE(buffer.empty()) << "unconsumed bytes left over";
  return commands;
}

Parser::Status parseOnce(const std::string& input, Command& out, Parser& parser) {
  return parser.parse(input, out).status;
}

TEST(RespParser, ParsesSingleCommand) {
  Parser parser;
  Command cmd;
  std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
  Parser::Result r = parser.parse(input, cmd);
  EXPECT_EQ(r.status, Parser::Status::kCommand);
  EXPECT_EQ(r.consumed, input.size());
  EXPECT_EQ(cmd, (Command{"SET", "key", "value"}));
}

TEST(RespParser, ParsesPipelinedCommandsOneAtATime) {
  Parser parser;
  std::string input = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
  Command cmd;

  Parser::Result first = parser.parse(input, cmd);
  EXPECT_EQ(first.status, Parser::Status::kCommand);
  EXPECT_EQ(cmd, Command{"PING"});

  Parser::Result second = parser.parse(std::string_view(input).substr(first.consumed), cmd);
  EXPECT_EQ(second.status, Parser::Status::kCommand);
  EXPECT_EQ(cmd, (Command{"GET", "a"}));
  EXPECT_EQ(first.consumed + second.consumed, input.size());
}

TEST(RespParser, HandlesEverySplitPoint) {
  // Two pipelined commands split into two reads at every possible offset.
  const std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
  for (size_t split = 0; split <= input.size(); ++split) {
    Parser parser;
    auto commands = feed(parser, {input.substr(0, split), input.substr(split)});
    ASSERT_EQ(commands.size(), 2u) << "split at " << split;
    EXPECT_EQ(commands[0], (Command{"SET", "key", "value"}));
    EXPECT_EQ(commands[1], (Command{"GET", "key"}));
  }
}

TEST(RespParser, HandlesOneByteAtATime) {
  const std::string input = "*2\r\n$3\r\nDEL\r\n$10\r\n0123456789\r\n";
  std::vector<std::string> chunks;
  for (char c : input) chunks.emplace_back(1, c);
  Parser parser;
  auto commands = feed(parser, chunks);
  ASSERT_EQ(commands.size(), 1u);
  EXPECT_EQ(commands[0], (Command{"DEL", "0123456789"}));
}

TEST(RespParser, IncompleteInputConsumesOnlyCompleteArguments) {
  Parser parser;
  Command cmd;
  // The first argument is complete; the second is cut off mid-body.
  std::string input = "*2\r\n$3\r\nGET\r\n$5\r\nke";
  Parser::Result r = parser.parse(input, cmd);
  EXPECT_EQ(r.status, Parser::Status::kNeedMore);
  EXPECT_EQ(r.consumed, std::string("*2\r\n$3\r\nGET\r\n$5\r\n").size());
}

TEST(RespParser, BulkStringsAreBinarySafe) {
  std::string value("a\r\nb\0c", 6);
  std::string input = "*2\r\n$3\r\nGET\r\n$6\r\n" + value + "\r\n";
  Parser parser;
  Command cmd;
  ASSERT_EQ(parseOnce(input, cmd, parser), Parser::Status::kCommand);
  EXPECT_EQ(cmd[1], value);
}

TEST(RespParser, AcceptsEmptyBulkString) {
  Parser parser;
  Command cmd;
  ASSERT_EQ(parseOnce("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n", cmd, parser),
            Parser::Status::kCommand);
  EXPECT_EQ(cmd, (Command{"SET", "k", ""}));
}

TEST(RespParser, SkipsEmptyArrays) {
  Parser parser;
  auto commands = feed(parser, {"*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n"});
  ASSERT_EQ(commands.size(), 1u);
  EXPECT_EQ(commands[0], Command{"PING"});
}

TEST(RespParser, RejectsMalformedInput) {
  const std::vector<std::string> bad = {
      "PING\r\n",                     // inline commands are not supported
      "*x\r\n",                       // non-numeric array length
      "*1\r\n+PING\r\n",              // argument is not a bulk string
      "*1\r\n$-1\r\n",                // null bulk string as an argument
      "*1\r\n$4\r\nPINGxx",           // bulk body not followed by CRLF
      "*1\r\n$99999999999\r\n",       // bulk longer than kMaxBulkLen
      "*9999999999\r\n",              // array longer than kMaxArrayLen
      "*\r\n",                        // missing length
  };
  for (const std::string& input : bad) {
    Parser parser;
    Command cmd;
    EXPECT_EQ(parseOnce(input, cmd, parser), Parser::Status::kError) << input;
    EXPECT_FALSE(parser.error().empty());
  }
}

TEST(RespParser, RejectsOverlongHeaderWithoutWaitingForever) {
  Parser parser;
  Command cmd;
  std::string input = "*" + std::string(kMaxHeaderLen + 1, '1');  // no CRLF
  EXPECT_EQ(parseOnce(input, cmd, parser), Parser::Status::kError);
}

}  // namespace
}  // namespace kv::resp

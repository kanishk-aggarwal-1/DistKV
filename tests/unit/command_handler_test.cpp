#include "server/command_handler.h"

#include <gtest/gtest.h>

#include <string>

namespace kv {
namespace {

class CommandHandlerTest : public ::testing::Test {
 protected:
  std::string run(const resp::Command& cmd) {
    std::string out;
    handler_.execute(cmd, out);
    return out;
  }

  Store store_{16};
  CommandHandler handler_{store_};
};

TEST_F(CommandHandlerTest, Ping) {
  EXPECT_EQ(run({"PING"}), "+PONG\r\n");
  EXPECT_EQ(run({"ping", "hello"}), "$5\r\nhello\r\n");
}

TEST_F(CommandHandlerTest, SetGetDel) {
  EXPECT_EQ(run({"GET", "k"}), "$-1\r\n");
  EXPECT_EQ(run({"SET", "k", "v1"}), "+OK\r\n");
  EXPECT_EQ(run({"GET", "k"}), "$2\r\nv1\r\n");
  EXPECT_EQ(run({"set", "k", "v2"}), "+OK\r\n");
  EXPECT_EQ(run({"get", "k"}), "$2\r\nv2\r\n");
  EXPECT_EQ(run({"DEL", "k", "missing"}), ":1\r\n");
  EXPECT_EQ(run({"GET", "k"}), "$-1\r\n");
}

TEST_F(CommandHandlerTest, CommandNamesAreCaseInsensitive) {
  EXPECT_EQ(run({"SeT", "k", "v"}), "+OK\r\n");
  EXPECT_EQ(run({"gEt", "k"}), "$1\r\nv\r\n");
}

TEST_F(CommandHandlerTest, WrongArity) {
  EXPECT_EQ(run({"GET"}), "-ERR wrong number of arguments for 'get' command\r\n");
  EXPECT_EQ(run({"SET", "k"}), "-ERR wrong number of arguments for 'set' command\r\n");
  EXPECT_EQ(run({"DEL"}), "-ERR wrong number of arguments for 'del' command\r\n");
  EXPECT_EQ(run({"PING", "a", "b"}), "-ERR wrong number of arguments for 'ping' command\r\n");
}

TEST_F(CommandHandlerTest, SetOptionsAreRejected) {
  EXPECT_EQ(run({"SET", "k", "v", "EX", "10"}), "-ERR syntax error\r\n");
}

TEST_F(CommandHandlerTest, UnknownCommand) {
  EXPECT_EQ(run({"FLUSHALL"}), "-ERR unknown command 'FLUSHALL'\r\n");
}

}  // namespace
}  // namespace kv

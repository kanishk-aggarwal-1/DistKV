#include "protocol/resp_parser.h"

#include <algorithm>
#include <charconv>

namespace kv::resp {

namespace {

bool parseInteger(std::string_view text, int64_t& value) {
  if (text.empty()) return false;
  auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  return ec == std::errc() && end == text.data() + text.size();
}

}  // namespace

Parser::Step Parser::fail(std::string message) {
  error_ = "Protocol error: " + std::move(message);
  return Step::kError;
}

Parser::Step Parser::readHeader(std::string_view input, size_t& pos, char prefix,
                                int64_t& value) {
  size_t eol = input.find("\r\n", pos);
  if (eol == std::string_view::npos) {
    if (input.size() - pos > kMaxHeaderLen) return fail("header line too long");
    return Step::kNeedMore;
  }
  std::string_view line = input.substr(pos, eol - pos);
  if (line.empty() || line[0] != prefix) {
    return fail(std::string("expected '") + prefix + "'");
  }
  if (!parseInteger(line.substr(1), value)) return fail("invalid length");
  pos = eol + 2;
  return Step::kOk;
}

Parser::Result Parser::parse(std::string_view input, Command& out) {
  size_t pos = 0;
  auto stop = [&](Step step) {
    return Result{step == Step::kNeedMore ? Status::kNeedMore : Status::kError, pos};
  };

  // Array header. Empty and null arrays carry no command; skip them as Redis does.
  while (args_remaining_ < 0) {
    int64_t count = 0;
    if (Step s = readHeader(input, pos, '*', count); s != Step::kOk) return stop(s);
    if (count > kMaxArrayLen) return stop(fail("too many arguments"));
    if (count <= 0) continue;
    args_remaining_ = count;
    args_.clear();
    // Cap the reservation: the header alone must not trigger a huge allocation.
    args_.reserve(static_cast<size_t>(std::min<int64_t>(count, 16)));
  }

  // Bulk string arguments.
  while (args_remaining_ > 0) {
    if (bulk_len_ < 0) {
      int64_t len = 0;
      if (Step s = readHeader(input, pos, '$', len); s != Step::kOk) return stop(s);
      if (len < 0 || len > kMaxBulkLen) return stop(fail("invalid bulk length"));
      bulk_len_ = len;
    }
    const size_t len = static_cast<size_t>(bulk_len_);
    if (input.size() - pos < len + 2) return stop(Step::kNeedMore);
    if (input.substr(pos + len, 2) != "\r\n") {
      return stop(fail("bulk string not terminated by CRLF"));
    }
    args_.emplace_back(input.substr(pos, len));
    pos += len + 2;
    bulk_len_ = -1;
    --args_remaining_;
  }

  out = std::move(args_);
  args_ = Command();
  args_remaining_ = -1;
  return Result{Status::kCommand, pos};
}

}  // namespace kv::resp

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv::resp {

// Limits that stop a malicious or buggy client from making the server
// allocate unbounded memory from a tiny header.
inline constexpr int64_t kMaxArrayLen = 1024 * 1024;
inline constexpr int64_t kMaxBulkLen = 16 * 1024 * 1024;
inline constexpr size_t kMaxHeaderLen = 64 * 1024;

// A client request: the command name followed by its arguments.
using Command = std::vector<std::string>;

// Incremental parser for client requests. Clients (redis-cli,
// memtier_benchmark) send each command as a RESP array of bulk strings:
//
//   *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
// The parser does no I/O. The caller passes in whatever bytes it has
// buffered, and the parser reports how many of them it consumed so the
// caller can discard them. Arguments of a partially received command are
// kept inside the parser, so consumed bytes are never scanned twice.
class Parser {
 public:
  enum class Status {
    kNeedMore,  // input ended mid-command; call again with more bytes
    kCommand,   // `out` holds one complete command
    kError,     // malformed input; the stream cannot be resynchronised
  };

  struct Result {
    Status status;
    size_t consumed;  // bytes at the front of `input` the caller may discard
  };

  Result parse(std::string_view input, Command& out);

  // Description of the last kError.
  const std::string& error() const { return error_; }

 private:
  enum class Step { kOk, kNeedMore, kError };

  // Reads a "<prefix><integer>\r\n" line at `pos`, advancing `pos` past it.
  Step readHeader(std::string_view input, size_t& pos, char prefix, int64_t& value);
  Step fail(std::string message);

  int64_t args_remaining_ = -1;  // -1: waiting for the next array header
  int64_t bulk_len_ = -1;        // -1: waiting for the next bulk header
  Command args_;                 // arguments of the command being parsed
  std::string error_;
};

}  // namespace kv::resp

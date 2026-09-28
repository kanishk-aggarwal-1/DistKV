#pragma once

#include <cstdint>
#include <string>
#include <string_view>

// Helpers that append RESP2 replies to an output buffer.
namespace kv::resp {

void appendSimpleString(std::string& out, std::string_view text);  // +OK\r\n
void appendError(std::string& out, std::string_view message);      // -ERR ...\r\n
void appendInteger(std::string& out, int64_t value);               // :1\r\n
void appendBulkString(std::string& out, std::string_view data);    // $3\r\nfoo\r\n
void appendNullBulkString(std::string& out);                       // $-1\r\n
void appendArrayHeader(std::string& out, size_t count);            // *2\r\n (elements follow)

}  // namespace kv::resp

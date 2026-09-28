#include "protocol/resp_writer.h"

namespace kv::resp {

void appendSimpleString(std::string& out, std::string_view text) {
  out += '+';
  out += text;
  out += "\r\n";
}

void appendError(std::string& out, std::string_view message) {
  out += '-';
  out += message;
  out += "\r\n";
}

void appendInteger(std::string& out, int64_t value) {
  out += ':';
  out += std::to_string(value);
  out += "\r\n";
}

void appendBulkString(std::string& out, std::string_view data) {
  out += '$';
  out += std::to_string(data.size());
  out += "\r\n";
  out += data;
  out += "\r\n";
}

void appendNullBulkString(std::string& out) { out += "$-1\r\n"; }

}  // namespace kv::resp

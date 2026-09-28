#include "server/command_handler.h"

#include <cctype>
#include <string_view>

#include "protocol/resp_writer.h"

namespace kv {

namespace {

// Command names are case-insensitive, as in Redis. `lower` must be lowercase.
bool isCommand(std::string_view name, std::string_view lower) {
  if (name.size() != lower.size()) return false;
  for (size_t i = 0; i < name.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(name[i])) != lower[i]) return false;
  }
  return true;
}

void appendArityError(std::string& out, std::string_view command) {
  std::string msg = "ERR wrong number of arguments for '";
  msg += command;
  msg += "' command";
  resp::appendError(out, msg);
}

}  // namespace

void CommandHandler::execute(const resp::Command& cmd, std::string& out) {
  // The parser never produces an empty command.
  const std::string& name = cmd[0];
  const size_t argc = cmd.size();

  if (isCommand(name, "ping")) {
    if (argc == 1) {
      resp::appendSimpleString(out, "PONG");
    } else if (argc == 2) {
      resp::appendBulkString(out, cmd[1]);
    } else {
      appendArityError(out, "ping");
    }
  } else if (isCommand(name, "get")) {
    if (argc != 2) return appendArityError(out, "get");
    if (auto value = store_.get(cmd[1])) {
      resp::appendBulkString(out, *value);
    } else {
      resp::appendNullBulkString(out);
    }
  } else if (isCommand(name, "set")) {
    // SET options (EX, NX, ...) are out of scope.
    if (argc < 3) return appendArityError(out, "set");
    if (argc > 3) return resp::appendError(out, "ERR syntax error");
    store_.set(cmd[1], cmd[2]);
    resp::appendSimpleString(out, "OK");
  } else if (isCommand(name, "del")) {
    if (argc < 2) return appendArityError(out, "del");
    int64_t deleted = 0;
    for (size_t i = 1; i < argc; ++i) {
      if (store_.del(cmd[i])) ++deleted;
    }
    resp::appendInteger(out, deleted);
  } else {
    resp::appendError(out, "ERR unknown command '" + name + "'");
  }
}

}  // namespace kv

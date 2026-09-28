#pragma once

#include <string>

#include "protocol/resp_parser.h"
#include "storage/store.h"

namespace kv {

// Executes the supported command subset (PING, GET, SET, DEL) against a
// Store. Stateless apart from the Store reference, so one instance is shared
// by all event-loop threads.
class CommandHandler {
 public:
  explicit CommandHandler(Store& store) : store_(store) {}

  // Appends the RESP reply for `cmd` to `out`.
  void execute(const resp::Command& cmd, std::string& out);

 private:
  Store& store_;
};

}  // namespace kv

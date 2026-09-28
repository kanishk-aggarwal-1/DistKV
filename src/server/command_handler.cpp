#include "server/command_handler.h"

#include <cctype>
#include <span>
#include <string_view>
#include <utility>

#include "cluster/slot.h"
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

void CommandHandler::execute(const resp::Command& cmd, net::Session& session, std::string& out) {
  // ASKING applies to exactly one following command.
  const bool asking = std::exchange(session.asking, false);
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
    get(cmd, asking, out);
  } else if (isCommand(name, "set")) {
    // SET options (EX, NX, ...) are out of scope.
    if (argc < 3) return appendArityError(out, "set");
    if (argc > 3) return resp::appendError(out, "ERR syntax error");
    set(cmd, asking, session, out);
  } else if (isCommand(name, "del")) {
    if (argc < 2) return appendArityError(out, "del");
    del(cmd, asking, session, out);
  } else if (isCommand(name, "asking")) {
    if (argc != 1) return appendArityError(out, "asking");
    session.asking = true;
    resp::appendSimpleString(out, "OK");
  } else if (isCommand(name, "cluster")) {
    if (argc == 2 && isCommand(cmd[1], "slots")) return clusterSlots(out);
    resp::appendError(out, "ERR unknown subcommand or wrong number of arguments for 'cluster'");
  } else {
    resp::appendError(out, "ERR unknown command '" + name + "'");
  }
}

void CommandHandler::get(const resp::Command& cmd, bool asking, std::string& out) {
  std::optional<std::string> value;
  Access access = store_.get(cmd[1], asking, value);
  if (access != Access::kServed) return appendRedirect(access, cluster::keySlot(cmd[1]), out);
  if (value) {
    resp::appendBulkString(out, *value);
  } else {
    resp::appendNullBulkString(out);
  }
}

void CommandHandler::set(const resp::Command& cmd, bool asking, net::Session& session,
                         std::string& out) {
  uint64_t seq = 0;
  Access access = store_.set(cmd[1], cmd[2], asking, seq);
  if (access != Access::kServed) return appendRedirect(access, cluster::keySlot(cmd[1]), out);
  // In cluster mode the reply is held until the backup has the write.
  session.reply_seq = seq;
  resp::appendSimpleString(out, "OK");
}

void CommandHandler::del(const resp::Command& cmd, bool asking, net::Session& session,
                         std::string& out) {
  std::span<const std::string> keys(cmd.begin() + 1, cmd.end());
  const uint16_t slot = cluster::keySlot(keys[0]);
  int64_t total = 0;

  if (cluster_ != nullptr) {
    // As in Redis Cluster, a multi-key command must stay within one slot so
    // that it runs on one node.
    for (const std::string& key : keys) {
      if (cluster::keySlot(key) != slot) {
        return resp::appendError(out, "CROSSSLOT Keys in request don't hash to the same slot");
      }
    }
    uint64_t seq = 0;
    Access access = store_.del(keys, asking, total, seq);
    if (access != Access::kServed) return appendRedirect(access, slot, out);
    session.reply_seq = seq;
  } else {
    // Standalone: every slot is local; delete key by key.
    for (size_t i = 0; i < keys.size(); ++i) {
      int64_t deleted = 0;
      uint64_t seq = 0;  // standalone: nothing is replicated
      store_.del(keys.subspan(i, 1), asking, deleted, seq);
      total += deleted;
    }
  }
  resp::appendInteger(out, total);
}

void CommandHandler::appendRedirect(Access access, uint16_t slot, std::string& out) {
  std::optional<std::string> addr;
  switch (access) {
    case Access::kMoved:
      if (cluster_ != nullptr) addr = cluster_->ownerAddress(slot);
      if (!addr) return resp::appendError(out, "CLUSTERDOWN Hash slot not served");
      return resp::appendError(out, "MOVED " + std::to_string(slot) + " " + *addr);
    case Access::kAsk:
      if (cluster_ != nullptr) addr = cluster_->migrationTarget(slot);
      if (!addr) return resp::appendError(out, "TRYAGAIN Slot is being reconfigured");
      return resp::appendError(out, "ASK " + std::to_string(slot) + " " + *addr);
    case Access::kTryAgain:
      return resp::appendError(out, "TRYAGAIN Key is being migrated, retry the request");
    case Access::kNoReplicas:
      return resp::appendError(out, kNoReplicasError);
    case Access::kServed:
      break;
  }
}

void CommandHandler::clusterSlots(std::string& out) {
  if (cluster_ == nullptr) {
    return resp::appendError(out, "ERR This instance has cluster support disabled");
  }
  // Reply format (as Redis): one entry per contiguous range of slots with the
  // same owner: [start, end, [host, port, node-id]].
  auto map = cluster_->map();
  struct Range {
    uint16_t start, end;
    const cluster::NodeInfo* node;
  };
  std::vector<Range> ranges;
  for (uint16_t slot = 0; slot < cluster::kNumSlots; ++slot) {
    const cluster::NodeInfo* node = map->owner(slot);
    if (node == nullptr) continue;
    if (!ranges.empty() && ranges.back().node == node && ranges.back().end + 1 == slot) {
      ranges.back().end = slot;
    } else {
      ranges.push_back({slot, slot, node});
    }
  }

  resp::appendArrayHeader(out, ranges.size());
  for (const Range& range : ranges) {
    std::string host;
    uint16_t port = 0;
    cluster::splitHostPort(range.node->client_addr, host, port);
    resp::appendArrayHeader(out, 3);
    resp::appendInteger(out, range.start);
    resp::appendInteger(out, range.end);
    resp::appendArrayHeader(out, 3);
    resp::appendBulkString(out, host);
    resp::appendInteger(out, port);
    resp::appendBulkString(out, range.node->id);
  }
}

}  // namespace kv

#pragma once

#include <string>

#include "cluster/cluster_state.h"
#include "net/connection.h"
#include "protocol/resp_parser.h"
#include "storage/store.h"

namespace kv {

// Executes the supported commands against a Store:
//   PING, GET, SET, DEL                 (data)
//   ASKING, CLUSTER SLOTS               (cluster routing, as in Redis Cluster)
//
// In cluster mode (`cluster` non-null) keys of slots not served here are
// answered with MOVED / ASK / TRYAGAIN redirects. In standalone mode every
// slot is served locally.
//
// Stateless apart from the references, so one instance is shared by all
// event-loop threads.
class CommandHandler {
 public:
  CommandHandler(Store& store, const cluster::ClusterState* cluster)
      : store_(store), cluster_(cluster) {}

  // Appends the RESP reply for `cmd` to `out`.
  void execute(const resp::Command& cmd, net::Session& session, std::string& out);

 private:
  void get(const resp::Command& cmd, bool asking, std::string& out);
  void set(const resp::Command& cmd, bool asking, std::string& out);
  void del(const resp::Command& cmd, bool asking, std::string& out);
  void clusterSlots(std::string& out);
  // Writes the redirect or error for a non-served Access.
  void appendRedirect(Access access, uint16_t slot, std::string& out);

  Store& store_;
  const cluster::ClusterState* cluster_;
};

}  // namespace kv

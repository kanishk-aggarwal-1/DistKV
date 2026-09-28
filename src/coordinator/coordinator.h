#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cluster/cluster_map.h"
#include "distkv.grpc.pb.h"

namespace kv::coordinator {

struct CoordinatorConfig {
  size_t vnodes_per_node = 128;
  // Slots moved per migration step. Each step ends with a map update, so
  // clients start going straight to the new owner after at most this many
  // slots instead of after the whole rebalance.
  size_t slots_per_step = 256;
};

// Owns the authoritative cluster map and drives membership changes.
//
// A single process by design (see DESIGN.md): it is the only writer of the
// map, which makes ordering trivial, and it is a single point of failure,
// which Phase 3 documents together with what a Raft-replicated coordinator
// would fix. Its state is in memory only.
class Coordinator {
 public:
  explicit Coordinator(CoordinatorConfig config = {});

  // Adds the node serving NodeService at `grpc_addr`, then moves onto it the
  // slots the hash ring now assigns to it. Blocks until the rebalance is done.
  grpc::Status addNode(const std::string& grpc_addr, distkv::v1::MembershipChange* result);

  // Moves every slot off the node, then removes it from the map.
  grpc::Status removeNode(const std::string& id, distkv::v1::MembershipChange* result);

  cluster::ClusterMap map() const;

 private:
  struct Move {
    uint32_t from;  // node indexes in map_
    uint32_t to;
    std::vector<uint32_t> slots;
  };

  grpc::Status rebalance(const std::vector<std::string>& member_ids,
                         distkv::v1::MembershipChange* result);
  grpc::Status migrateStep(const cluster::NodeInfo& from, const cluster::NodeInfo& to,
                           const std::vector<uint32_t>& slots, distkv::v1::MembershipChange* result);
  // Pushes the current map to `first` (in order, all must succeed), then
  // best-effort to every other node.
  grpc::Status pushMap(const std::vector<std::string>& first_ids);
  distkv::v1::NodeService::Stub& stub(const std::string& grpc_addr);

  const CoordinatorConfig config_;

  // One membership change at a time; also guards stubs_ and next_migration_id_.
  std::mutex change_mutex_;
  std::unordered_map<std::string, std::unique_ptr<distkv::v1::NodeService::Stub>> stubs_;
  uint64_t next_migration_id_ = 1;

  mutable std::mutex map_mutex_;
  cluster::ClusterMap map_;
};

// gRPC front end for the Coordinator.
class CoordinatorService final : public distkv::v1::CoordinatorService::Service {
 public:
  explicit CoordinatorService(Coordinator& coordinator) : coordinator_(coordinator) {}

  grpc::Status AddNode(grpc::ServerContext*, const distkv::v1::AddNodeRequest* request,
                       distkv::v1::MembershipChange* reply) override;
  grpc::Status RemoveNode(grpc::ServerContext*, const distkv::v1::RemoveNodeRequest* request,
                          distkv::v1::MembershipChange* reply) override;
  grpc::Status GetClusterMap(grpc::ServerContext*, const distkv::v1::Empty*,
                             distkv::v1::ClusterMap* reply) override;

 private:
  Coordinator& coordinator_;
};

}  // namespace kv::coordinator

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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
  // Every node is pinged this often...
  std::chrono::milliseconds heartbeat_interval{100};
  // ...and declared dead after this long without a successful ping.
  std::chrono::milliseconds failure_timeout{1000};
};

// Owns the authoritative cluster map and drives every change to it:
// membership (groups, spares), slot migration, failure detection, failover
// and re-syncing backups.
//
// A single process by design (see DESIGN.md): it is the only writer of the
// map, which makes ordering trivial, and it is a single point of failure.
// Its state is in memory only.
//
// Threads:
//   - callers of addGroup/removeGroup/addSpare/rebalance (gRPC handlers),
//     serialised by change_mutex_;
//   - one heartbeat monitor per node, which triggers failover;
//   - a reconciler, which gives groups without a backup a spare, re-syncs
//     backups that fell out of sync, and finishes interrupted rebalances.
class Coordinator {
 public:
  explicit Coordinator(CoordinatorConfig config = {});
  ~Coordinator();

  Coordinator(const Coordinator&) = delete;
  Coordinator& operator=(const Coordinator&) = delete;

  // Adds a replication group (a primary and its backup), syncs the backup,
  // then moves onto the group the slots the hash ring now assigns to it.
  grpc::Status addGroup(const std::string& primary_grpc_addr, const std::string& backup_grpc_addr,
                        distkv::v1::MembershipChange* result);
  // Moves every slot off the group, then removes it and its nodes.
  grpc::Status removeGroup(const std::string& id, distkv::v1::MembershipChange* result);
  // Registers a spare node, used to replace failed backups automatically.
  grpc::Status addSpare(const std::string& grpc_addr, distkv::v1::MembershipChange* result);
  // Moves slots until ownership matches the hash ring.
  grpc::Status rebalance(distkv::v1::MembershipChange* result);

  cluster::ClusterMap map() const;

 private:
  struct Monitor {
    std::thread thread;
    std::atomic<bool> stop{false};
  };

  // ---- Membership and data movement (caller holds change_mutex_).
  grpc::Status fetchNodeInfo(const std::string& grpc_addr, cluster::NodeInfo& node);
  grpc::Status rebalanceLocked(distkv::v1::MembershipChange* result);
  grpc::Status migrateStep(uint32_t from_group, uint32_t to_group,
                           const std::vector<uint32_t>& slots, distkv::v1::MembershipChange* result);
  // Full sync of a group's backup, then marks it ready.
  grpc::Status syncBackup(const std::string& group_id);
  void reconcileLocked();

  // ---- Failure handling (monitor threads).
  void startMonitor(const cluster::NodeInfo& node);
  void stopMonitor(const std::string& node_id);
  void monitorLoop(cluster::NodeInfo node, Monitor* self);
  void onNodeFailed(const std::string& node_id);
  void onReplicationBroken(const std::string& primary_id);

  // Pushes the current map to `first` (in order, all must succeed), then
  // once, best-effort, to every other node.
  grpc::Status pushMap(const std::vector<std::string>& first_ids);
  grpc::Status pushMapTo(const cluster::NodeInfo& node, const distkv::v1::ClusterMap& proto,
                         bool retry);
  distkv::v1::NodeService::Stub& stub(const std::string& grpc_addr);
  void reconcilerLoop();
  void wakeReconciler();

  const CoordinatorConfig config_;

  // One membership change (or reconciliation) at a time.
  std::mutex change_mutex_;
  uint64_t next_migration_id_ = 1;  // guarded by change_mutex_
  int next_group_number_ = 1;       // guarded by change_mutex_
  // Set when a rebalance did not finish; the reconciler retries it.
  std::atomic<bool> needs_rebalance_{false};

  // Failovers are applied one at a time.
  std::mutex failover_mutex_;

  mutable std::mutex map_mutex_;
  cluster::ClusterMap map_;

  std::mutex stubs_mutex_;
  std::unordered_map<std::string, std::unique_ptr<distkv::v1::NodeService::Stub>> stubs_;

  std::mutex monitors_mutex_;
  std::map<std::string, std::unique_ptr<Monitor>> monitors_;

  std::mutex reconciler_mutex_;
  std::condition_variable reconciler_wake_;
  bool reconciler_pending_ = false;
  bool shutting_down_ = false;
  std::thread reconciler_;
};

// gRPC front end for the Coordinator.
class CoordinatorService final : public distkv::v1::CoordinatorService::Service {
 public:
  explicit CoordinatorService(Coordinator& coordinator) : coordinator_(coordinator) {}

  grpc::Status AddGroup(grpc::ServerContext*, const distkv::v1::AddGroupRequest* request,
                        distkv::v1::MembershipChange* reply) override;
  grpc::Status RemoveGroup(grpc::ServerContext*, const distkv::v1::RemoveGroupRequest* request,
                           distkv::v1::MembershipChange* reply) override;
  grpc::Status AddSpare(grpc::ServerContext*, const distkv::v1::AddSpareRequest* request,
                        distkv::v1::MembershipChange* reply) override;
  grpc::Status Rebalance(grpc::ServerContext*, const distkv::v1::Empty*,
                         distkv::v1::MembershipChange* reply) override;
  grpc::Status GetClusterMap(grpc::ServerContext*, const distkv::v1::Empty*,
                             distkv::v1::ClusterMap* reply) override;

 private:
  Coordinator& coordinator_;
};

}  // namespace kv::coordinator

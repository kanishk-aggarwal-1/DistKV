#pragma once

#include <array>
#include <cstdint>
#include <mutex>

#include "cluster/cluster_map.h"
#include "cluster/cluster_state.h"
#include "cluster/slot.h"
#include "distkv.grpc.pb.h"
#include "storage/store.h"

namespace kv::cluster {

// gRPC service through which the coordinator (and, during migration, other
// nodes) control this node. Uses gRPC's synchronous API: handlers run on
// gRPC's own threads and touch only the thread-safe Store and ClusterState,
// never the event-loop threads.
class NodeService final : public distkv::v1::NodeService::Service {
 public:
  NodeService(NodeInfo self, Store& store, ClusterState& state);

  grpc::Status GetNodeInfo(grpc::ServerContext*, const distkv::v1::Empty*,
                           distkv::v1::Node* reply) override;
  grpc::Status ApplyClusterMap(grpc::ServerContext*, const distkv::v1::ClusterMap* request,
                               distkv::v1::Empty*) override;
  grpc::Status SetMigrating(grpc::ServerContext*, const distkv::v1::SlotMigration* request,
                            distkv::v1::Empty*) override;
  grpc::Status SetImporting(grpc::ServerContext*, const distkv::v1::SlotMigration* request,
                            distkv::v1::Empty*) override;
  grpc::Status MigrateSlots(grpc::ServerContext*, const distkv::v1::MigrateSlotsRequest* request,
                            distkv::v1::MigrateSlotsResponse* reply) override;
  grpc::Status ImportKeys(grpc::ServerContext*, const distkv::v1::ImportKeysRequest* request,
                          distkv::v1::Empty*) override;

  // Batch limits for migration; exposed for tests.
  static constexpr size_t kBatchKeys = 128;
  static constexpr size_t kBatchBytes = 1024 * 1024;

 private:
  const NodeInfo self_;
  Store& store_;
  ClusterState& state_;

  // Serialises MigrateSlots calls. If the coordinator retries after a
  // timeout while the first call is still running, two movers sending the
  // same keys concurrently could reorder batches; one at a time avoids that.
  std::mutex mover_mutex_;
  // Next batch seq per slot, for the migration currently moving it
  // (guarded by mover_mutex_).
  std::array<uint64_t, kNumSlots> next_seq_{};
  std::array<uint64_t, kNumSlots> seq_migration_id_{};
};

}  // namespace kv::cluster

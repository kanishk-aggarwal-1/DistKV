#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

#include "cluster/cluster_map.h"
#include "cluster/cluster_state.h"
#include "cluster/slot.h"
#include "distkv.grpc.pb.h"
#include "replication/replication_log.h"
#include "replication/replicator.h"
#include "storage/store.h"

namespace kv::cluster {

// gRPC service through which the coordinator and other nodes control this
// node. Uses gRPC's synchronous API: handlers run on gRPC's own threads and
// touch only thread-safe state (Store, ClusterState, ReplicationLog), never
// the event-loop threads.
//
// Roles come from the cluster map: primary of a group (serves clients,
// replicates to the backup), backup (applies the primary's stream, serves
// no clients), or neither (spare, or removed).
class NodeService final : public distkv::v1::NodeService::Service {
 public:
  NodeService(NodeInfo self, Store& store, ClusterState& state, replication::ReplicationLog& log,
              std::chrono::milliseconds replication_ack_timeout);
  ~NodeService() override;

  grpc::Status GetNodeInfo(grpc::ServerContext*, const distkv::v1::Empty*,
                           distkv::v1::Node* reply) override;
  grpc::Status Ping(grpc::ServerContext*, const distkv::v1::Empty*,
                    distkv::v1::PingReply* reply) override;
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
  grpc::Status StartReplication(grpc::ServerContext*,
                                const distkv::v1::StartReplicationRequest* request,
                                distkv::v1::Empty*) override;
  grpc::Status Replicate(
      grpc::ServerContext*,
      grpc::ServerReaderWriter<distkv::v1::ReplicationAck, distkv::v1::ReplicationBatch>* stream)
      override;

  // Stops replication (node shutdown).
  void shutdown();

  // Batch limits for migration; exposed for tests.
  static constexpr size_t kBatchKeys = 128;
  static constexpr size_t kBatchBytes = 1024 * 1024;

 private:
  // Applies the role this node has in `map` (already installed).
  void applyRole(const ClusterMap& map);
  // Waits for the backup to acknowledge `seq`.
  grpc::Status waitForBackup(uint64_t seq, const std::string& what);

  const NodeInfo self_;
  Store& store_;
  ClusterState& state_;
  replication::ReplicationLog& log_;
  replication::Replicator replicator_;

  // Held exclusively while a new map (and so possibly a new role) is
  // applied, and shared while a replicated batch is validated and applied:
  // once a backup has been promoted it cannot apply another batch from its
  // old primary, even one that was already in flight.
  std::shared_mutex role_mutex_;

  // Backup side of the stream.
  std::mutex apply_mutex_;
  uint64_t applied_log_id_ = 0;
  uint64_t applied_seq_ = 0;

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

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "cluster/cluster_map.h"
#include "replication/replication_log.h"

namespace grpc {
class ClientContext;
}

namespace kv::replication {

// Primary side of replication: streams the ReplicationLog to the backup
// over one gRPC bidirectional stream and feeds the backup's cumulative acks
// back into the log.
//
// Runs on its own threads (a writer and a reader per stream). It never
// touches event-loop state; the log's Progress wakes the event loops.
//
// If the backup rejects the stream (it is no longer our backup, e.g. it was
// promoted: epoch fencing) or does not acknowledge within `ack_timeout`, the
// replicator gives up: pending writes fail, the log stops accepting writes,
// and broken() reports it so the coordinator can re-sync the backup. Giving
// up rather than retrying forever keeps a cut-off primary from holding
// client writes indefinitely, and requiring a full re-sync afterwards means
// the backup never silently misses writes the primary applied.
class Replicator {
 public:
  Replicator(ReplicationLog& log, std::string self_id, std::chrono::milliseconds ack_timeout);
  ~Replicator();

  Replicator(const Replicator&) = delete;
  Replicator& operator=(const Replicator&) = delete;

  // Starts a new log towards `backup` (any previous stream is stopped and its
  // pending writes fail). The backup must then be fully synced (snapshots)
  // before writes are accepted.
  void start(const std::string& group_id, const cluster::NodeInfo& backup);
  // Stops streaming; pending writes fail.
  void stop();

  bool running() const;
  std::string backupId() const;
  bool broken() const { return broken_.load(); }

 private:
  void run(std::string group_id, cluster::NodeInfo backup, uint64_t log_id);
  // One stream. Returns when it breaks, times out or is stopped.
  void runStream(const std::string& group_id, const cluster::NodeInfo& backup, uint64_t log_id);
  bool shouldRun() const;
  void giveUp(const char* reason);

  ReplicationLog& log_;
  const std::string self_id_;
  const std::chrono::milliseconds ack_timeout_;

  mutable std::mutex mutex_;
  bool running_ = false;
  std::string backup_id_;
  grpc::ClientContext* stream_ctx_ = nullptr;  // current stream, for cancellation
  std::thread thread_;
  std::atomic<bool> broken_{false};
};

}  // namespace kv::replication

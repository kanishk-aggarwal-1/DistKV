#include "cluster/node_service.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <string>
#include <thread>

#include "cluster/rpc.h"
#include "replication/ops_proto.h"

namespace kv::cluster {

namespace {

grpc::Status invalidSlot(uint32_t slot) {
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid slot " + std::to_string(slot));
}

grpc::Status noReplicas(const std::string& what) {
  return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                      what + ": this node is not a primary with a synced backup");
}

// Full sync sends one snapshot per slot; wait for the backup to catch up
// every so many slots so the log does not hold the whole dataset at once.
constexpr uint16_t kSnapshotWindow = 256;

}  // namespace

NodeService::NodeService(NodeInfo self, Store& store, ClusterState& state,
                         replication::ReplicationLog& log,
                         std::chrono::milliseconds replication_ack_timeout)
    : self_(std::move(self)),
      store_(store),
      state_(state),
      log_(log),
      replicator_(log, self_.id, replication_ack_timeout) {}

NodeService::~NodeService() { shutdown(); }

void NodeService::shutdown() {
  log_.setAccepting(false);
  replicator_.stop();
}

grpc::Status NodeService::GetNodeInfo(grpc::ServerContext*, const distkv::v1::Empty*,
                                      distkv::v1::Node* reply) {
  *reply = nodeToProto(self_);
  return grpc::Status::OK;
}

grpc::Status NodeService::Ping(grpc::ServerContext*, const distkv::v1::Empty*,
                               distkv::v1::PingReply* reply) {
  reply->set_epoch(state_.map()->epoch);
  reply->set_replication_broken(replicator_.broken());
  return grpc::Status::OK;
}

grpc::Status NodeService::ApplyClusterMap(grpc::ServerContext*,
                                          const distkv::v1::ClusterMap* request,
                                          distkv::v1::Empty*) {
  ClusterMap map = ClusterMap::fromProto(*request);
  if (!map.slot_owner.empty() && map.slot_owner.size() != kNumSlots) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "slot_owner must have 16384 entries");
  }
  std::unique_lock role(role_mutex_);
  // Stale maps (e.g. a delayed retry) are ignored; the newest one wins.
  if (!state_.apply(map)) return grpc::Status::OK;
  applyRole(map);
  return grpc::Status::OK;
}

void NodeService::applyRole(const ClusterMap& map) {
  const GroupInfo* group = map.groupOfNode(self_.id);
  const bool primary = group != nullptr && group->primary == self_.id;

  // Stop accepting first if we are losing the right to, before anything else.
  if (!primary || !group->hasReadyBackup()) log_.setAccepting(false);

  // A backup mirrors its group's slot states (so that when promoted it
  // serves exactly what its primary served); anyone else owns nothing.
  store_.setReplica(!primary);
  const std::optional<uint32_t> my_group =
      group == nullptr ? std::nullopt : map.groupIndex(group->id);
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    bool mine = my_group && slot < map.slot_owner.size() && map.slot_owner[slot] == *my_group;
    store_.applyOwnership(slot, mine);
  }

  if (!primary || group->backup.empty()) {
    replicator_.stop();
    return;
  }
  if (replicator_.backupId() != group->backup) {
    // A new backup: start a new log. Writes stay refused until the
    // coordinator has had it fully synced (StartReplication) and marks it ready.
    const NodeInfo* backup = map.findNode(group->backup);
    if (backup != nullptr) replicator_.start(group->id, *backup);
  }
  if (group->hasReadyBackup() && replicator_.running() && !replicator_.broken()) {
    log_.setAccepting(true);
  }
}

grpc::Status NodeService::StartReplication(grpc::ServerContext*,
                                           const distkv::v1::StartReplicationRequest* request,
                                           distkv::v1::Empty*) {
  {
    std::unique_lock role(role_mutex_);
    auto map = state_.map();
    const GroupInfo* group = map->groupOfNode(self_.id);
    if (group == nullptr || group->primary != self_.id || group->backup != request->backup().id()) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "not the primary of " + request->backup().id() + " in the current map");
    }
    // Always a fresh log: the backup gets a complete copy from scratch.
    log_.setAccepting(false);
    replicator_.start(group->id, nodeFromProto(request->backup()));
  }

  // Full sync: one snapshot per slot, in the same ordered stream as writes.
  // Each snapshot is taken under its slot's lock, so any write to that slot
  // is either in the snapshot or after it in the stream.
  uint64_t last = 0;
  for (uint32_t slot = 0; slot < kNumSlots; ++slot) {
    last = store_.logSlotSnapshot(static_cast<uint16_t>(slot));
    if ((slot + 1) % kSnapshotWindow == 0 && !log_.waitForAck(last, kDataRpcTimeout)) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "backup did not acknowledge full sync");
    }
  }
  if (!log_.waitForAck(last, kDataRpcTimeout)) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "backup did not acknowledge full sync");
  }
  return grpc::Status::OK;
}

grpc::Status NodeService::Replicate(
    grpc::ServerContext*,
    grpc::ServerReaderWriter<distkv::v1::ReplicationAck, distkv::v1::ReplicationBatch>* stream) {
  distkv::v1::ReplicationBatch batch;
  while (stream->Read(&batch)) {
    distkv::v1::ReplicationAck ack;
    {
      std::shared_lock role(role_mutex_);
      // Epoch fencing: accept changes only from the node that our current
      // map names as our primary. Once promoted (or reassigned), this node
      // refuses its old primary, which therefore can never again get a write
      // acknowledged.
      auto map = state_.map();
      const GroupInfo* group = map->findGroup(batch.group_id());
      if (group == nullptr || group->primary != batch.primary_id() || group->backup != self_.id) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                            "epoch " + std::to_string(map->epoch) + ": " + batch.primary_id() +
                                " is not this node's primary");
      }
      std::lock_guard apply(apply_mutex_);
      if (batch.log_id() != applied_log_id_) {
        applied_log_id_ = batch.log_id();
        applied_seq_ = 0;
      }
      for (const auto& proto_op : batch.ops()) {
        if (proto_op.seq() <= applied_seq_) continue;  // resent after a reconnect
        replication::Op op = replication::opFromProto(proto_op);
        store_.applyReplicated(op);
        if (op.type == replication::Op::Type::kSlotMigrating) {
          state_.setMigrationTarget(op.slot, op.peer_addr);
        }
        applied_seq_ = op.seq;
      }
      ack.set_applied_seq(applied_seq_);
    }
    if (!stream->Write(ack)) break;
  }
  return grpc::Status::OK;
}

grpc::Status NodeService::waitForBackup(uint64_t seq, const std::string& what) {
  if (seq == 0 || log_.waitForAck(seq, kDataRpcTimeout)) return grpc::Status::OK;
  return grpc::Status(grpc::StatusCode::UNAVAILABLE, what + ": backup did not acknowledge");
}

grpc::Status NodeService::SetMigrating(grpc::ServerContext*,
                                       const distkv::v1::SlotMigration* request,
                                       distkv::v1::Empty*) {
  auto map = state_.map();
  const NodeInfo* target = map->findNode(request->peer_id());
  if (target == nullptr) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "unknown target node " + request->peer_id());
  }
  // Log the intent for every slot and wait for the backup, then start
  // redirecting: a promoted backup must know about the migration before any
  // client could have been sent to the target.
  uint64_t last = 0;
  for (uint32_t slot : request->slots()) {
    if (slot >= kNumSlots) return invalidSlot(slot);
    uint64_t seq = 0;
    if (!store_.logMigration(static_cast<uint16_t>(slot), target->client_addr, seq)) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "slot " + std::to_string(slot) +
                              " is not owned here, or no synced backup to replicate to");
    }
    if (seq != 0) last = seq;
  }
  grpc::Status status = waitForBackup(last, "SetMigrating");
  if (!status.ok()) return status;
  for (uint32_t slot : request->slots()) {
    state_.setMigrationTarget(static_cast<uint16_t>(slot), target->client_addr);
    store_.beginMigration(static_cast<uint16_t>(slot));
  }
  return grpc::Status::OK;
}

grpc::Status NodeService::SetImporting(grpc::ServerContext*,
                                       const distkv::v1::SlotMigration* request,
                                       distkv::v1::Empty*) {
  for (uint32_t slot : request->slots()) {
    if (slot >= kNumSlots) return invalidSlot(slot);
    if (!store_.beginImport(static_cast<uint16_t>(slot), request->migration_id())) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "slot " + std::to_string(slot) + " cannot import here");
    }
  }
  return waitForBackup(log_.lastSeq(), "SetImporting");
}

grpc::Status NodeService::MigrateSlots(grpc::ServerContext*,
                                       const distkv::v1::MigrateSlotsRequest* request,
                                       distkv::v1::MigrateSlotsResponse* reply) {
  std::lock_guard lock(mover_mutex_);
  auto target = makeNodeStub(request->target().grpc_addr());
  const uint64_t migration_id = request->migration_id();
  uint64_t moved = 0;

  for (uint32_t slot32 : request->slots()) {
    if (slot32 >= kNumSlots) return invalidSlot(slot32);
    const auto slot = static_cast<uint16_t>(slot32);
    if (store_.slotState(slot) != SlotState::kMigrating) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "slot " + std::to_string(slot) + " is not migrating");
    }
    if (seq_migration_id_[slot] != migration_id) {
      seq_migration_id_[slot] = migration_id;
      next_seq_[slot] = 1;
    }

    while (true) {
      std::vector<KeyValue> batch = store_.takeMigrationBatch(slot, kBatchKeys, kBatchBytes);
      if (batch.empty()) break;

      distkv::v1::ImportKeysRequest import;
      import.set_slot(slot);
      import.set_migration_id(migration_id);
      for (const KeyValue& kv : batch) {
        auto* entry = import.add_entries();
        entry->set_key(kv.key);
        entry->set_value(kv.value);
      }

      // Every attempt gets a fresh seq. If an earlier attempt timed out but is
      // still applied later, the target sees a lower seq and ignores it.
      grpc::Status status;
      for (int attempt = 0; attempt < kRpcAttempts; ++attempt) {
        import.set_seq(next_seq_[slot]++);
        grpc::ClientContext ctx;
        setDeadline(ctx, kDataRpcTimeout);
        distkv::v1::Empty empty;
        status = target->ImportKeys(&ctx, import, &empty);
        if (status.ok() || !isRetryable(status)) break;
        std::this_thread::sleep_for(retryBackoff(attempt));
      }
      if (!status.ok()) {
        // The batch stays frozen (in flight) here; a retried MigrateSlots
        // resumes it. Nothing is deleted without the target's ack.
        return grpc::Status(status.error_code(),
                            "ImportKeys to " + request->target().id() + ": " + status.error_message());
      }

      // The target (and its backup) have the batch. Delete it here only once
      // our own backup has the deletion: until then the keys stay frozen and
      // clients are not yet redirected to the target for them, so a promoted
      // backup can never hold a stale copy of a key clients have since
      // changed on the target.
      uint64_t seq = store_.logMigrationDeletes(slot, batch);
      if (seq == 0) return noReplicas("MigrateSlots");
      status = waitForBackup(seq, "MigrateSlots");
      if (!status.ok()) return status;
      store_.finishMigrationBatch(slot, batch);
      moved += batch.size();
    }
  }
  reply->set_keys_moved(moved);
  return grpc::Status::OK;
}

grpc::Status NodeService::ImportKeys(grpc::ServerContext*,
                                     const distkv::v1::ImportKeysRequest* request,
                                     distkv::v1::Empty*) {
  if (request->slot() >= kNumSlots) return invalidSlot(request->slot());
  std::vector<KeyValue> batch;
  batch.reserve(request->entries_size());
  for (const auto& entry : request->entries()) batch.push_back({entry.key(), entry.value()});

  uint64_t repl_seq = 0;
  switch (store_.importBatch(static_cast<uint16_t>(request->slot()), request->migration_id(),
                             request->seq(), batch, repl_seq)) {
    case Store::ImportResult::kApplied:
      // Acknowledge only once our backup has the keys too: the source
      // deletes its copy as soon as we say OK.
      return waitForBackup(repl_seq, "ImportKeys");
    case Store::ImportResult::kStale:
      // Only a delayed duplicate can be stale; the sender has long moved on.
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "stale batch ignored");
    case Store::ImportResult::kNoReplicas:
      return noReplicas("ImportKeys");
    case Store::ImportResult::kRejected:
      break;
  }
  return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                      "slot " + std::to_string(request->slot()) + " is not importing this migration");
}

}  // namespace kv::cluster

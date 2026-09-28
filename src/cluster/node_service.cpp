#include "cluster/node_service.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <string>
#include <thread>

#include "cluster/rpc.h"

namespace kv::cluster {

namespace {

grpc::Status invalidSlot(uint32_t slot) {
  return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid slot " + std::to_string(slot));
}

}  // namespace

NodeService::NodeService(NodeInfo self, Store& store, ClusterState& state)
    : self_(std::move(self)), store_(store), state_(state) {}

grpc::Status NodeService::GetNodeInfo(grpc::ServerContext*, const distkv::v1::Empty*,
                                      distkv::v1::Node* reply) {
  *reply = nodeToProto(self_);
  return grpc::Status::OK;
}

grpc::Status NodeService::ApplyClusterMap(grpc::ServerContext*,
                                          const distkv::v1::ClusterMap* request,
                                          distkv::v1::Empty*) {
  ClusterMap map = ClusterMap::fromProto(*request);
  if (!map.slot_owner.empty() && map.slot_owner.size() != kNumSlots) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "slot_owner must have 16384 entries");
  }
  std::vector<bool> owned(kNumSlots, false);
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    const NodeInfo* owner = map.owner(slot);
    owned[slot] = owner != nullptr && owner->id == self_.id;
  }
  // Stale maps (e.g. a delayed retry) are ignored; the newest one wins.
  if (!state_.apply(std::move(map))) return grpc::Status::OK;
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) store_.applyOwnership(slot, owned[slot]);
  return grpc::Status::OK;
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
  for (uint32_t slot : request->slots()) {
    if (slot >= kNumSlots) return invalidSlot(slot);
    // Record the target before redirects can start pointing at it.
    state_.setMigrationTarget(static_cast<uint16_t>(slot), target->client_addr);
    if (!store_.beginMigration(static_cast<uint16_t>(slot))) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "slot " + std::to_string(slot) + " is not owned here");
    }
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
  return grpc::Status::OK;
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

  switch (store_.importBatch(static_cast<uint16_t>(request->slot()), request->migration_id(),
                             request->seq(), batch)) {
    case Store::ImportResult::kApplied:
      return grpc::Status::OK;
    case Store::ImportResult::kStale:
      // Only a delayed duplicate can be stale; the sender has long moved on.
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "stale batch ignored");
    case Store::ImportResult::kRejected:
      break;
  }
  return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                      "slot " + std::to_string(request->slot()) + " is not importing this migration");
}

}  // namespace kv::cluster

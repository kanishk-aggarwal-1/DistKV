#include "coordinator/coordinator.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cstdio>
#include <functional>
#include <map>
#include <thread>
#include <utility>

#include "cluster/hash_ring.h"
#include "cluster/rpc.h"
#include "cluster/slot.h"

namespace kv::coordinator {

using cluster::ClusterMap;
using cluster::kNumSlots;
using cluster::NodeInfo;

namespace {

// Runs `call` (which must build a fresh ClientContext each time) until it
// succeeds, fails permanently, or runs out of attempts.
grpc::Status withRetries(const std::function<grpc::Status()>& call) {
  grpc::Status status;
  for (int attempt = 0; attempt < cluster::kRpcAttempts; ++attempt) {
    status = call();
    if (status.ok() || !cluster::isRetryable(status)) return status;
    std::this_thread::sleep_for(cluster::retryBackoff(attempt));
  }
  return status;
}

grpc::Status annotate(const grpc::Status& status, const std::string& what) {
  return grpc::Status(status.error_code(), what + ": " + status.error_message());
}

}  // namespace

Coordinator::Coordinator(CoordinatorConfig config) : config_(config) {}

ClusterMap Coordinator::map() const {
  std::lock_guard lock(map_mutex_);
  return map_;
}

distkv::v1::NodeService::Stub& Coordinator::stub(const std::string& grpc_addr) {
  auto& entry = stubs_[grpc_addr];
  if (!entry) entry = cluster::makeNodeStub(grpc_addr);
  return *entry;
}

grpc::Status Coordinator::addNode(const std::string& grpc_addr,
                                  distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);

  distkv::v1::Node reply;
  grpc::Status status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
    return stub(grpc_addr).GetNodeInfo(&ctx, distkv::v1::Empty(), &reply);
  });
  if (!status.ok()) return annotate(status, "GetNodeInfo " + grpc_addr);
  NodeInfo node = cluster::nodeFromProto(reply);
  node.grpc_addr = grpc_addr;  // the address that demonstrably reaches it

  std::vector<std::string> member_ids;
  bool first_node = false;
  {
    std::lock_guard lock(map_mutex_);
    if (map_.findNode(node.id) != nullptr) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "node " + node.id + " already added");
    }
    map_.nodes.push_back(node);
    if (map_.slot_owner.empty()) {
      // The first node owns everything; there is nothing to migrate.
      map_.slot_owner.assign(kNumSlots, static_cast<uint32_t>(map_.nodes.size() - 1));
      first_node = true;
    }
    ++map_.epoch;
    for (const NodeInfo& n : map_.nodes) member_ids.push_back(n.id);
  }

  // Every node (the new one included) must know about the new member before
  // any slot can be migrated to it.
  status = pushMap({node.id});
  if (!status.ok()) return status;
  if (!first_node) {
    status = rebalance(member_ids, result);
    if (!status.ok()) return status;
  }
  result->set_epoch(map().epoch);
  return grpc::Status::OK;
}

grpc::Status Coordinator::removeNode(const std::string& id, distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);

  std::vector<std::string> remaining;
  {
    std::lock_guard lock(map_mutex_);
    if (map_.findNode(id) == nullptr) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown node " + id);
    }
    for (const NodeInfo& n : map_.nodes) {
      if (n.id != id) remaining.push_back(n.id);
    }
  }
  if (remaining.empty()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "cannot remove the last node");
  }

  grpc::Status status = rebalance(remaining, result);
  if (!status.ok()) return status;

  NodeInfo removed;
  {
    std::lock_guard lock(map_mutex_);
    uint32_t removed_index = *map_.indexOf(id);
    removed = map_.nodes[removed_index];
    map_.nodes.erase(map_.nodes.begin() + removed_index);
    for (uint32_t& owner : map_.slot_owner) {
      // The rebalance moved every slot off the removed node.
      if (owner > removed_index) --owner;
    }
    ++map_.epoch;
  }
  status = pushMap({});
  if (!status.ok()) return status;

  // Tell the removed node too, so stale clients that still reach it are
  // redirected (MOVED) to the new owners. Best effort: it may already be gone.
  distkv::v1::ClusterMap proto = map().toProto();
  grpc::ClientContext ctx;
  cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
  distkv::v1::Empty empty;
  stub(removed.grpc_addr).ApplyClusterMap(&ctx, proto, &empty);

  result->set_epoch(map().epoch);
  return grpc::Status::OK;
}

grpc::Status Coordinator::rebalance(const std::vector<std::string>& member_ids,
                                    distkv::v1::MembershipChange* result) {
  cluster::HashRing ring(member_ids, config_.vnodes_per_node);

  // Group the slots whose owner changes by (from, to).
  std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t>> moves;
  ClusterMap current = map();
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    uint32_t from = current.slot_owner[slot];
    uint32_t to = *current.indexOf(ring.ownerOf(slot));
    if (from != to) moves[{from, to}].push_back(slot);
  }

  for (const auto& [endpoints, slots] : moves) {
    const NodeInfo from = current.nodes[endpoints.first];
    const NodeInfo to = current.nodes[endpoints.second];
    for (size_t begin = 0; begin < slots.size(); begin += config_.slots_per_step) {
      size_t end = std::min(slots.size(), begin + config_.slots_per_step);
      std::vector<uint32_t> step(slots.begin() + begin, slots.begin() + end);
      grpc::Status status = migrateStep(from, to, step, result);
      if (!status.ok()) return status;
    }
  }
  return grpc::Status::OK;
}

grpc::Status Coordinator::migrateStep(const NodeInfo& from, const NodeInfo& to,
                                      const std::vector<uint32_t>& slots,
                                      distkv::v1::MembershipChange* result) {
  distkv::v1::SlotMigration request;
  request.mutable_slots()->Add(slots.begin(), slots.end());
  request.set_migration_id(next_migration_id_++);

  // 1. Target first, so that it is ready for the ASK redirects the source
  //    starts sending in step 2.
  request.set_peer_id(from.id);
  grpc::Status status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
    distkv::v1::Empty empty;
    return stub(to.grpc_addr).SetImporting(&ctx, request, &empty);
  });
  if (!status.ok()) return annotate(status, "SetImporting on " + to.id);

  // 2. Source: absent keys now redirect to the target with ASK.
  request.set_peer_id(to.id);
  status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
    distkv::v1::Empty empty;
    return stub(from.grpc_addr).SetMigrating(&ctx, request, &empty);
  });
  if (!status.ok()) return annotate(status, "SetMigrating on " + from.id);

  // 3. Source moves every key of the slots to the target.
  distkv::v1::MigrateSlotsRequest migrate;
  migrate.mutable_slots()->Add(slots.begin(), slots.end());
  *migrate.mutable_target() = cluster::nodeToProto(to);
  migrate.set_migration_id(request.migration_id());
  distkv::v1::MigrateSlotsResponse moved;
  status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kMigrateRpcTimeout);
    return stub(from.grpc_addr).MigrateSlots(&ctx, migrate, &moved);
  });
  if (!status.ok()) return annotate(status, "MigrateSlots on " + from.id);

  // 4. Commit: the target owns the slots from the new epoch on. The target
  //    learns first so that when the source starts answering MOVED, the node
  //    it points at is already serving.
  {
    std::lock_guard lock(map_mutex_);
    uint32_t to_index = *map_.indexOf(to.id);
    for (uint32_t slot : slots) map_.slot_owner[slot] = to_index;
    ++map_.epoch;
  }
  status = pushMap({to.id, from.id});
  if (!status.ok()) return status;

  result->set_slots_moved(result->slots_moved() + static_cast<uint32_t>(slots.size()));
  result->set_keys_moved(result->keys_moved() + moved.keys_moved());
  return grpc::Status::OK;
}

grpc::Status Coordinator::pushMap(const std::vector<std::string>& first_ids) {
  ClusterMap current = map();
  distkv::v1::ClusterMap proto = current.toProto();

  auto push = [&](const NodeInfo& node) {
    return withRetries([&] {
      grpc::ClientContext ctx;
      cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
      distkv::v1::Empty empty;
      return stub(node.grpc_addr).ApplyClusterMap(&ctx, proto, &empty);
    });
  };

  for (const std::string& id : first_ids) {
    const NodeInfo* node = current.findNode(id);
    if (node == nullptr) continue;
    grpc::Status status = push(*node);
    if (!status.ok()) return annotate(status, "ApplyClusterMap on " + id);
  }
  for (const NodeInfo& node : current.nodes) {
    if (std::find(first_ids.begin(), first_ids.end(), node.id) != first_ids.end()) continue;
    // A node that misses this push keeps an older map. That is safe: its
    // redirects point to the previous owner, which redirects again, and the
    // next successful push brings it up to date.
    grpc::Status status = push(node);
    if (!status.ok()) {
      std::fprintf(stderr, "warning: ApplyClusterMap on %s failed: %s\n", node.id.c_str(),
                   status.error_message().c_str());
    }
  }
  return grpc::Status::OK;
}

grpc::Status CoordinatorService::AddNode(grpc::ServerContext*,
                                         const distkv::v1::AddNodeRequest* request,
                                         distkv::v1::MembershipChange* reply) {
  return coordinator_.addNode(request->grpc_addr(), reply);
}

grpc::Status CoordinatorService::RemoveNode(grpc::ServerContext*,
                                            const distkv::v1::RemoveNodeRequest* request,
                                            distkv::v1::MembershipChange* reply) {
  return coordinator_.removeNode(request->id(), reply);
}

grpc::Status CoordinatorService::GetClusterMap(grpc::ServerContext*, const distkv::v1::Empty*,
                                               distkv::v1::ClusterMap* reply) {
  *reply = coordinator_.map().toProto();
  return grpc::Status::OK;
}

}  // namespace kv::coordinator

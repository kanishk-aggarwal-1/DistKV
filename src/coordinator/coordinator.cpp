#include "coordinator/coordinator.h"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <cstdio>
#include <functional>
#include <thread>
#include <utility>

#include "cluster/hash_ring.h"
#include "cluster/rpc.h"
#include "cluster/slot.h"

namespace kv::coordinator {

using cluster::ClusterMap;
using cluster::GroupInfo;
using cluster::kNumSlots;
using cluster::NodeInfo;
using Clock = std::chrono::steady_clock;

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

bool contains(const std::vector<std::string>& ids, const std::string& id) {
  return std::find(ids.begin(), ids.end(), id) != ids.end();
}

// Wall-clock time in milliseconds, for logs that line up with other processes'.
long long nowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

constexpr auto kSyncTimeout = std::chrono::minutes(10);
constexpr auto kSingleAttemptTimeout = std::chrono::milliseconds(500);
constexpr auto kReconcileInterval = std::chrono::milliseconds(200);

}  // namespace

Coordinator::Coordinator(CoordinatorConfig config) : config_(config) {
  reconciler_ = std::thread([this] { reconcilerLoop(); });
}

Coordinator::~Coordinator() {
  {
    std::lock_guard lock(reconciler_mutex_);
    shutting_down_ = true;
  }
  reconciler_wake_.notify_all();
  reconciler_.join();

  std::map<std::string, std::unique_ptr<Monitor>> monitors;
  {
    std::lock_guard lock(monitors_mutex_);
    monitors.swap(monitors_);
  }
  for (auto& [id, monitor] : monitors) monitor->stop = true;
  for (auto& [id, monitor] : monitors) monitor->thread.join();
}

ClusterMap Coordinator::map() const {
  std::lock_guard lock(map_mutex_);
  return map_;
}

distkv::v1::NodeService::Stub& Coordinator::stub(const std::string& grpc_addr) {
  std::lock_guard lock(stubs_mutex_);
  auto& entry = stubs_[grpc_addr];
  if (!entry) entry = cluster::makeNodeStub(grpc_addr);
  return *entry;
}

grpc::Status Coordinator::fetchNodeInfo(const std::string& grpc_addr, NodeInfo& node) {
  distkv::v1::Node reply;
  grpc::Status status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kControlRpcTimeout);
    // A node being (re)added may have just started: wait for the connection
    // (up to the deadline) instead of failing on a channel still backing off.
    ctx.set_wait_for_ready(true);
    return stub(grpc_addr).GetNodeInfo(&ctx, distkv::v1::Empty(), &reply);
  });
  if (!status.ok()) return annotate(status, "GetNodeInfo " + grpc_addr);
  node = cluster::nodeFromProto(reply);
  node.grpc_addr = grpc_addr;  // the address that demonstrably reaches it
  return grpc::Status::OK;
}

// ---- Membership --------------------------------------------------------------

grpc::Status Coordinator::addGroup(const std::string& primary_grpc_addr,
                                   const std::string& backup_grpc_addr,
                                   distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);
  NodeInfo primary, backup;
  grpc::Status status = fetchNodeInfo(primary_grpc_addr, primary);
  if (!status.ok()) return status;
  status = fetchNodeInfo(backup_grpc_addr, backup);
  if (!status.ok()) return status;
  if (primary.id == backup.id) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "primary and backup must differ");
  }

  std::string group_id;
  bool first_group = false;
  {
    std::lock_guard lock(map_mutex_);
    for (const NodeInfo* node : {&primary, &backup}) {
      if (map_.findNode(node->id) != nullptr) {
        return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "node " + node->id + " already added");
      }
    }
    group_id = "g" + std::to_string(next_group_number_++);
    map_.nodes.push_back(primary);
    map_.nodes.push_back(backup);
    map_.groups.push_back({group_id, primary.id, backup.id, false});
    if (map_.slot_owner.empty()) {
      // The first group owns everything; there is nothing to migrate.
      map_.slot_owner.assign(kNumSlots, static_cast<uint32_t>(map_.groups.size() - 1));
      first_group = true;
    }
    ++map_.epoch;
  }
  startMonitor(primary);
  startMonitor(backup);

  // Every node must know the new group before its backup syncs or any slot
  // moves to it.
  status = pushMap({primary.id, backup.id});
  if (!status.ok()) return status;
  status = syncBackup(group_id);
  if (!status.ok()) return status;  // the reconciler keeps retrying the sync
  if (!first_group) {
    status = rebalanceLocked(result);
    if (!status.ok()) return status;
  }
  result->set_epoch(map().epoch);
  return grpc::Status::OK;
}

grpc::Status Coordinator::removeGroup(const std::string& id, distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);
  ClusterMap current = map();
  const GroupInfo* group = current.findGroup(id);
  if (group == nullptr) return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown group " + id);
  if (current.groups.size() == 1) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "cannot remove the last group");
  }

  // Move every slot to the remaining groups, as the ring without `id` says.
  std::vector<std::string> remaining;
  for (const GroupInfo& g : current.groups) {
    if (g.id != id) remaining.push_back(g.id);
  }
  cluster::HashRing ring(remaining, config_.vnodes_per_node);
  for (const GroupInfo& g : current.groups) {
    if (!g.hasReadyBackup()) {
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "group " + g.id + " has no synced backup; migrations need replication");
    }
  }
  const uint32_t removed_index = *current.groupIndex(id);
  std::map<uint32_t, std::vector<uint32_t>> moves;  // to-group -> slots
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    if (current.slot_owner[slot] == removed_index) {
      moves[*current.groupIndex(ring.ownerOf(slot))].push_back(slot);
    }
  }
  for (const auto& [to, slots] : moves) {
    for (size_t begin = 0; begin < slots.size(); begin += config_.slots_per_step) {
      size_t end = std::min(slots.size(), begin + config_.slots_per_step);
      grpc::Status status = migrateStep(removed_index, to,
                                        {slots.begin() + begin, slots.begin() + end}, result);
      if (!status.ok()) return status;
    }
  }

  std::vector<NodeInfo> removed_nodes;
  {
    std::lock_guard lock(map_mutex_);
    uint32_t index = *map_.groupIndex(id);
    GroupInfo removed = map_.groups[index];
    map_.groups.erase(map_.groups.begin() + index);
    for (uint32_t& owner : map_.slot_owner) {
      if (owner > index) --owner;  // no slot is owned by the removed group any more
    }
    for (const std::string& node_id : {removed.primary, removed.backup}) {
      auto it = std::find_if(map_.nodes.begin(), map_.nodes.end(),
                             [&](const NodeInfo& n) { return n.id == node_id; });
      if (it != map_.nodes.end()) {
        removed_nodes.push_back(*it);
        map_.nodes.erase(it);
      }
    }
    ++map_.epoch;
  }
  grpc::Status status = pushMap({});
  if (!status.ok()) return status;
  // Tell the removed nodes too, so stale clients that still reach them are
  // redirected. Best effort: they may already be gone.
  distkv::v1::ClusterMap proto = map().toProto();
  for (const NodeInfo& node : removed_nodes) {
    pushMapTo(node, proto, false);
    stopMonitor(node.id);
  }
  result->set_epoch(map().epoch);
  return grpc::Status::OK;
}

grpc::Status Coordinator::addSpare(const std::string& grpc_addr,
                                   distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);
  NodeInfo node;
  grpc::Status status = fetchNodeInfo(grpc_addr, node);
  if (!status.ok()) return status;
  {
    std::lock_guard lock(map_mutex_);
    if (map_.findNode(node.id) != nullptr) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS, "node " + node.id + " already added");
    }
    map_.nodes.push_back(node);
    map_.spares.push_back(node.id);
    ++map_.epoch;
  }
  startMonitor(node);
  status = pushMap({node.id});
  if (!status.ok()) return status;
  wakeReconciler();  // a group may be waiting for a backup
  result->set_epoch(map().epoch);
  return grpc::Status::OK;
}

grpc::Status Coordinator::rebalance(distkv::v1::MembershipChange* result) {
  std::lock_guard change(change_mutex_);
  grpc::Status status = rebalanceLocked(result);
  result->set_epoch(map().epoch);
  return status;
}

// ---- Data movement -------------------------------------------------------------

grpc::Status Coordinator::rebalanceLocked(distkv::v1::MembershipChange* result) {
  ClusterMap current = map();
  std::vector<std::string> members;
  for (const GroupInfo& g : current.groups) {
    // Strict mode: a migration writes on both groups, which needs replication.
    if (!g.hasReadyBackup()) {
      needs_rebalance_ = true;
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          "group " + g.id + " has no synced backup yet; the rebalance will run "
                          "once it has");
    }
    members.push_back(g.id);
  }
  if (members.empty()) return grpc::Status::OK;
  cluster::HashRing ring(members, config_.vnodes_per_node);

  // Group the slots whose owner changes by (from, to).
  std::map<std::pair<uint32_t, uint32_t>, std::vector<uint32_t>> moves;
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    uint32_t from = current.slot_owner[slot];
    uint32_t to = *current.groupIndex(ring.ownerOf(slot));
    if (from != to) moves[{from, to}].push_back(slot);
  }

  for (const auto& [endpoints, slots] : moves) {
    for (size_t begin = 0; begin < slots.size(); begin += config_.slots_per_step) {
      size_t end = std::min(slots.size(), begin + config_.slots_per_step);
      grpc::Status status = migrateStep(endpoints.first, endpoints.second,
                                        {slots.begin() + begin, slots.begin() + end}, result);
      if (!status.ok()) {
        needs_rebalance_ = true;
        return grpc::Status(status.error_code(),
                            status.error_message() + " (the rebalance will be retried)");
      }
    }
  }
  needs_rebalance_ = false;
  return grpc::Status::OK;
}

grpc::Status Coordinator::migrateStep(uint32_t from_group, uint32_t to_group,
                                      const std::vector<uint32_t>& slots,
                                      distkv::v1::MembershipChange* result) {
  // Resolve the primaries now: a failover may have changed them since the
  // rebalance was planned.
  ClusterMap current = map();
  const NodeInfo* from_ptr = current.findNode(current.groups[from_group].primary);
  const NodeInfo* to_ptr = current.findNode(current.groups[to_group].primary);
  if (from_ptr == nullptr || to_ptr == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "group primary unknown");
  }
  const NodeInfo from = *from_ptr;
  const NodeInfo to = *to_ptr;

  distkv::v1::SlotMigration request;
  request.mutable_slots()->Add(slots.begin(), slots.end());
  request.set_migration_id(next_migration_id_++);

  // 1. Target first, so that it is ready for the ASK redirects the source
  //    starts sending in step 2.
  request.set_peer_id(from.id);
  grpc::Status status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kDataRpcTimeout);
    distkv::v1::Empty empty;
    return stub(to.grpc_addr).SetImporting(&ctx, request, &empty);
  });
  if (!status.ok()) return annotate(status, "SetImporting on " + to.id);

  // 2. Source: absent keys now redirect to the target with ASK.
  request.set_peer_id(to.id);
  status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, cluster::kDataRpcTimeout);
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

  // 4. Commit: the target group owns the slots from the new epoch on. Its
  //    primary learns first so that when the source starts answering MOVED,
  //    the node it points at is already serving.
  {
    std::lock_guard lock(map_mutex_);
    for (uint32_t slot : slots) map_.slot_owner[slot] = to_group;
    ++map_.epoch;
  }
  status = pushMap({to.id, from.id});
  if (!status.ok()) return status;

  result->set_slots_moved(result->slots_moved() + static_cast<uint32_t>(slots.size()));
  result->set_keys_moved(result->keys_moved() + moved.keys_moved());
  return grpc::Status::OK;
}

grpc::Status Coordinator::syncBackup(const std::string& group_id) {
  ClusterMap current = map();
  const GroupInfo* group = current.findGroup(group_id);
  if (group == nullptr || group->backup.empty() || group->backup_ready) return grpc::Status::OK;
  const NodeInfo* primary = current.findNode(group->primary);
  const NodeInfo* backup = current.findNode(group->backup);
  if (primary == nullptr || backup == nullptr) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "group " + group_id + " member unknown");
  }

  const long long started = nowMs();
  distkv::v1::StartReplicationRequest request;
  *request.mutable_backup() = cluster::nodeToProto(*backup);
  grpc::Status status = withRetries([&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, kSyncTimeout);
    distkv::v1::Empty empty;
    return stub(primary->grpc_addr).StartReplication(&ctx, request, &empty);
  });
  if (!status.ok()) return annotate(status, "StartReplication on " + primary->id);

  {
    std::lock_guard lock(map_mutex_);
    GroupInfo* g = map_.findGroup(group_id);
    if (g == nullptr || g->primary != primary->id || g->backup != backup->id) {
      return grpc::Status(grpc::StatusCode::ABORTED, "group " + group_id + " changed during sync");
    }
    g->backup_ready = true;
    ++map_.epoch;
  }
  std::fprintf(stderr, "[%lld] group %s: backup %s synced in %lld ms\n", nowMs(), group_id.c_str(),
               backup->id.c_str(), nowMs() - started);
  return pushMap({primary->id, backup->id});
}

// ---- Failure handling --------------------------------------------------------

void Coordinator::startMonitor(const NodeInfo& node) {
  std::lock_guard lock(monitors_mutex_);
  auto& slot = monitors_[node.id];
  if (slot) {  // a previous incarnation of this node id
    slot->stop = true;
    slot->thread.join();
  }
  slot = std::make_unique<Monitor>();
  Monitor* monitor = slot.get();
  monitor->thread = std::thread([this, node, monitor] { monitorLoop(node, monitor); });
}

void Coordinator::stopMonitor(const std::string& node_id) {
  std::unique_ptr<Monitor> monitor;
  {
    std::lock_guard lock(monitors_mutex_);
    auto it = monitors_.find(node_id);
    if (it == monitors_.end()) return;
    monitor = std::move(it->second);
    monitors_.erase(it);
  }
  monitor->stop = true;
  monitor->thread.join();
}

void Coordinator::monitorLoop(NodeInfo node, Monitor* self) {
  auto last_ok = Clock::now();
  while (!self->stop) {
    const auto started = Clock::now();
    distkv::v1::PingReply reply;
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, std::max(config_.heartbeat_interval * 2, std::chrono::milliseconds(50)));
    grpc::Status status = stub(node.grpc_addr).Ping(&ctx, distkv::v1::Empty(), &reply);

    if (status.ok()) {
      last_ok = Clock::now();
      if (reply.replication_broken()) onReplicationBroken(node.id);
      uint64_t epoch;
      {
        std::lock_guard lock(map_mutex_);
        epoch = map_.epoch;
      }
      // Heal missed best-effort pushes: a node behind the current map gets it.
      if (reply.epoch() < epoch) pushMapTo(node, map().toProto(), false);
    } else if (Clock::now() - last_ok > config_.failure_timeout) {
      std::fprintf(stderr, "[%lld] node %s missed heartbeats for %lld ms: declaring it dead\n",
                   nowMs(), node.id.c_str(),
                   static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              Clock::now() - last_ok)
                                              .count()));
      onNodeFailed(node.id);
      return;
    }
    std::this_thread::sleep_until(started + config_.heartbeat_interval);
  }
}

void Coordinator::onNodeFailed(const std::string& node_id) {
  std::lock_guard failover(failover_mutex_);
  std::vector<std::string> first;
  {
    std::lock_guard lock(map_mutex_);
    if (map_.findNode(node_id) == nullptr) return;
    bool keep_node = false;
    std::erase(map_.spares, node_id);
    for (GroupInfo& g : map_.groups) {
      if (g.primary == node_id) {
        if (g.hasReadyBackup()) {
          // Failover: the backup has every acknowledged write.
          std::fprintf(stderr, "[%lld] group %s: promoting backup %s (primary %s failed)\n",
                       nowMs(), g.id.c_str(), g.backup.c_str(), node_id.c_str());
          g.primary = g.backup;
          g.backup.clear();
          g.backup_ready = false;
          first.push_back(g.primary);
        } else {
          // Without a synced backup there is no copy to promote. The group's
          // slots are unavailable until its primary returns (see DESIGN.md).
          std::fprintf(stderr, "[%lld] group %s LOST: primary %s failed with no synced backup\n",
                       nowMs(), g.id.c_str(), node_id.c_str());
          keep_node = true;
        }
      } else if (g.backup == node_id) {
        std::fprintf(stderr, "[%lld] group %s: backup %s failed\n", nowMs(), g.id.c_str(),
                     node_id.c_str());
        g.backup.clear();
        g.backup_ready = false;
        first.push_back(g.primary);
      }
    }
    if (!keep_node) {
      std::erase_if(map_.nodes, [&](const NodeInfo& n) { return n.id == node_id; });
    }
    ++map_.epoch;
  }
  grpc::Status status = pushMap(first);
  if (!status.ok()) {
    std::fprintf(stderr, "warning: failover map push: %s\n", status.error_message().c_str());
  }
  std::fprintf(stderr, "[%lld] failover map (epoch %llu) pushed\n", nowMs(),
               static_cast<unsigned long long>(map().epoch));
  wakeReconciler();
}

void Coordinator::onReplicationBroken(const std::string& primary_id) {
  std::lock_guard failover(failover_mutex_);
  {
    std::lock_guard lock(map_mutex_);
    GroupInfo* group = nullptr;
    for (GroupInfo& g : map_.groups) {
      if (g.primary == primary_id) group = &g;
    }
    // Already being handled (not ready) or no longer a primary.
    if (group == nullptr || !group->backup_ready) return;
    std::fprintf(stderr, "[%lld] group %s: replication broken, re-syncing backup %s\n", nowMs(),
                 group->id.c_str(), group->backup.c_str());
    group->backup_ready = false;
    ++map_.epoch;
  }
  pushMap({primary_id});
  wakeReconciler();
}

// ---- Reconciliation ------------------------------------------------------------

void Coordinator::wakeReconciler() {
  {
    std::lock_guard lock(reconciler_mutex_);
    reconciler_pending_ = true;
  }
  reconciler_wake_.notify_all();
}

void Coordinator::reconcilerLoop() {
  while (true) {
    {
      std::unique_lock lock(reconciler_mutex_);
      reconciler_wake_.wait_for(lock, kReconcileInterval,
                                [&] { return reconciler_pending_ || shutting_down_; });
      if (shutting_down_) return;
      reconciler_pending_ = false;
    }
    std::lock_guard change(change_mutex_);
    reconcileLocked();
  }
}

void Coordinator::reconcileLocked() {
  ClusterMap current = map();
  for (const GroupInfo& group : current.groups) {
    if (current.findNode(group.primary) == nullptr) continue;
    if (group.backup.empty()) {
      // Give the group a spare as its new backup.
      std::string spare;
      {
        std::lock_guard lock(map_mutex_);
        GroupInfo* g = map_.findGroup(group.id);
        if (g == nullptr || !g->backup.empty() || map_.spares.empty()) continue;
        spare = map_.spares.front();
        map_.spares.erase(map_.spares.begin());
        g->backup = spare;
        g->backup_ready = false;
        ++map_.epoch;
      }
      std::fprintf(stderr, "[%lld] group %s: assigning spare %s as backup\n", nowMs(),
                   group.id.c_str(), spare.c_str());
      grpc::Status status = pushMap({group.primary, spare});
      if (!status.ok()) continue;
    } else if (group.backup_ready) {
      continue;
    }
    grpc::Status status = syncBackup(group.id);
    if (!status.ok()) {
      std::fprintf(stderr, "[%lld] group %s: backup sync failed: %s\n", nowMs(), group.id.c_str(),
                   status.error_message().c_str());
    }
  }

  if (needs_rebalance_) {
    distkv::v1::MembershipChange ignored;
    grpc::Status status = rebalanceLocked(&ignored);
    if (status.ok()) {
      std::fprintf(stderr, "[%lld] interrupted rebalance completed (%u slots moved)\n", nowMs(),
                   ignored.slots_moved());
    }
  }
}

// ---- Map distribution ------------------------------------------------------------

grpc::Status Coordinator::pushMapTo(const NodeInfo& node, const distkv::v1::ClusterMap& proto,
                                    bool retry) {
  auto call = [&] {
    grpc::ClientContext ctx;
    cluster::setDeadline(ctx, retry ? cluster::kControlRpcTimeout
                                    : std::chrono::duration_cast<std::chrono::milliseconds>(
                                          kSingleAttemptTimeout));
    distkv::v1::Empty empty;
    return stub(node.grpc_addr).ApplyClusterMap(&ctx, proto, &empty);
  };
  return retry ? withRetries(call) : call();
}

grpc::Status Coordinator::pushMap(const std::vector<std::string>& first_ids) {
  ClusterMap current = map();
  distkv::v1::ClusterMap proto = current.toProto();

  for (const std::string& id : first_ids) {
    const NodeInfo* node = current.findNode(id);
    if (node == nullptr) continue;
    grpc::Status status = pushMapTo(*node, proto, true);
    if (!status.ok()) return annotate(status, "ApplyClusterMap on " + id);
  }
  for (const NodeInfo& node : current.nodes) {
    if (contains(first_ids, node.id)) continue;
    // A node that misses this push keeps an older map. That is safe: its
    // redirects point to the previous owner, which redirects again, and its
    // heartbeat monitor re-sends the current map when it sees the old epoch.
    pushMapTo(node, proto, false);
  }
  return grpc::Status::OK;
}

// ---- gRPC front end --------------------------------------------------------------

grpc::Status CoordinatorService::AddGroup(grpc::ServerContext*,
                                          const distkv::v1::AddGroupRequest* request,
                                          distkv::v1::MembershipChange* reply) {
  return coordinator_.addGroup(request->primary_grpc_addr(), request->backup_grpc_addr(), reply);
}

grpc::Status CoordinatorService::RemoveGroup(grpc::ServerContext*,
                                             const distkv::v1::RemoveGroupRequest* request,
                                             distkv::v1::MembershipChange* reply) {
  return coordinator_.removeGroup(request->id(), reply);
}

grpc::Status CoordinatorService::AddSpare(grpc::ServerContext*,
                                          const distkv::v1::AddSpareRequest* request,
                                          distkv::v1::MembershipChange* reply) {
  return coordinator_.addSpare(request->grpc_addr(), reply);
}

grpc::Status CoordinatorService::Rebalance(grpc::ServerContext*, const distkv::v1::Empty*,
                                           distkv::v1::MembershipChange* reply) {
  return coordinator_.rebalance(reply);
}

grpc::Status CoordinatorService::GetClusterMap(grpc::ServerContext*, const distkv::v1::Empty*,
                                               distkv::v1::ClusterMap* reply) {
  *reply = coordinator_.map().toProto();
  return grpc::Status::OK;
}

}  // namespace kv::coordinator

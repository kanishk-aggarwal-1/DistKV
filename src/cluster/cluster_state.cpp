#include "cluster/cluster_state.h"

namespace kv::cluster {

bool ClusterState::apply(ClusterMap map) {
  std::lock_guard lock(mutex_);
  if (map.epoch <= map_->epoch) return false;
  map_ = std::make_shared<const ClusterMap>(std::move(map));
  // Migration targets are deliberately kept. A target is only consulted while
  // the Store reports the slot as migrating, and the Store leaves that state
  // *after* this map is installed; clearing targets here would open a window
  // in which an ASK has nowhere to point.
  return true;
}

std::shared_ptr<const ClusterMap> ClusterState::map() const {
  std::lock_guard lock(mutex_);
  return map_;
}

std::optional<std::string> ClusterState::ownerAddress(uint16_t slot) const {
  std::shared_ptr<const ClusterMap> current = map();
  const NodeInfo* owner = current->owner(slot);
  if (owner == nullptr) return std::nullopt;
  return owner->client_addr;
}

void ClusterState::setMigrationTarget(uint16_t slot, std::string client_addr) {
  std::lock_guard lock(mutex_);
  migration_target_[slot] = std::move(client_addr);
}

std::optional<std::string> ClusterState::migrationTarget(uint16_t slot) const {
  std::lock_guard lock(mutex_);
  if (migration_target_[slot].empty()) return std::nullopt;
  return migration_target_[slot];
}

}  // namespace kv::cluster

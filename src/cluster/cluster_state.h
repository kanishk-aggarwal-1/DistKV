#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "cluster/cluster_map.h"
#include "cluster/slot.h"

namespace kv::cluster {

// A node's view of the cluster: the latest map it has received and, for
// slots it is migrating out, where the keys are going. Used to build MOVED
// and ASK redirects and the CLUSTER SLOTS reply.
//
// This is routing *advice* for clients. Whether a request is served is
// decided by the Store's per-slot state under the Store's own locks, so a
// slightly stale view here can only produce a redirect that the client
// follows once more, never a wrong answer.
class ClusterState {
 public:
  explicit ClusterState(std::string self_id) : self_id_(std::move(self_id)) {}

  const std::string& selfId() const { return self_id_; }

  // Installs `map` if it is newer than the current one. Returns false if stale.
  bool apply(ClusterMap map);

  std::shared_ptr<const ClusterMap> map() const;

  // Client address of the slot's owner according to the current map.
  std::optional<std::string> ownerAddress(uint16_t slot) const;

  void setMigrationTarget(uint16_t slot, std::string client_addr);
  std::optional<std::string> migrationTarget(uint16_t slot) const;

 private:
  const std::string self_id_;
  mutable std::mutex mutex_;
  std::shared_ptr<const ClusterMap> map_ = std::make_shared<ClusterMap>();
  std::array<std::string, kNumSlots> migration_target_;  // client addr, empty if none
};

}  // namespace kv::cluster

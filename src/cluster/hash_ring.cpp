#include "cluster/hash_ring.h"

#include <algorithm>

namespace kv::cluster {

uint64_t stableHash(std::string_view data) {
  uint64_t h = 0xcbf29ce484222325ULL;  // FNV-1a offset basis
  for (char c : data) {
    h ^= static_cast<uint8_t>(c);
    h *= 0x100000001b3ULL;  // FNV-1a prime
  }
  // SplitMix64 finalizer: FNV-1a alone clusters similar inputs such as
  // "node1#1" and "node1#2"; this spreads them across the whole range.
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h;
}

HashRing::HashRing(const std::vector<std::string>& node_ids, size_t vnodes_per_node)
    : node_ids_(node_ids) {
  points_.reserve(node_ids_.size() * vnodes_per_node);
  for (size_t n = 0; n < node_ids_.size(); ++n) {
    for (size_t v = 0; v < vnodes_per_node; ++v) {
      points_.emplace_back(stableHash(node_ids_[n] + "#" + std::to_string(v)), n);
    }
  }
  // Ties (vanishingly unlikely) are broken by node index for determinism.
  std::sort(points_.begin(), points_.end());
}

const std::string& HashRing::ownerOf(uint16_t slot) const {
  uint64_t h = stableHash("slot:" + std::to_string(slot));
  auto it = std::upper_bound(points_.begin(), points_.end(), std::make_pair(h, SIZE_MAX));
  if (it == points_.end()) it = points_.begin();  // wrap around the ring
  return node_ids_[it->second];
}

}  // namespace kv::cluster

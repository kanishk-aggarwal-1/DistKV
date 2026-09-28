#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kv::cluster {

// Stable 64-bit hash (FNV-1a followed by a SplitMix64 finalizer). Unlike
// std::hash its output is fixed, so every process and build agrees on ring
// positions.
uint64_t stableHash(std::string_view data);

// Consistent-hash ring with virtual nodes, used to decide which node owns
// each of the 16384 slots.
//
// Each node is placed on a 64-bit ring at `vnodes_per_node` pseudo-random
// points. A slot belongs to the first point clockwise from hash(slot).
// Adding a node only takes over the arcs in front of its new points, so the
// only slots that change owner are those that move *to* the new node
// (about 1/N of them); removing a node only reassigns that node's slots.
// Many virtual nodes per node keep the arcs, and so the load, even.
class HashRing {
 public:
  HashRing(const std::vector<std::string>& node_ids, size_t vnodes_per_node);

  // Returns the id of the node owning `slot`. The ring must not be empty.
  const std::string& ownerOf(uint16_t slot) const;

  bool empty() const { return points_.empty(); }

 private:
  std::vector<std::string> node_ids_;
  std::vector<std::pair<uint64_t, size_t>> points_;  // (position, index into node_ids_), sorted
};

}  // namespace kv::cluster

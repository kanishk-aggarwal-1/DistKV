#include "cluster/hash_ring.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include "cluster/slot.h"

namespace kv::cluster {
namespace {

std::vector<std::string> nodeIds(int count) {
  std::vector<std::string> ids;
  for (int i = 1; i <= count; ++i) ids.push_back("n" + std::to_string(i));
  return ids;
}

std::vector<std::string> owners(const HashRing& ring) {
  std::vector<std::string> result;
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) result.push_back(ring.ownerOf(slot));
  return result;
}

TEST(StableHash, IsFixedAcrossBuilds) {
  // Pinned values: ring positions must never change between versions, or a
  // coordinator upgrade would silently reshuffle every slot.
  EXPECT_EQ(stableHash(""), 0xf52a15e9a9b5e89bULL);
  EXPECT_EQ(stableHash("n1#0"), 0xe73b9b96754f50a4ULL);
  EXPECT_EQ(stableHash("slot:0"), 0xda62498601a23fafULL);
}

TEST(HashRing, IsDeterministicAndIndependentOfNodeOrder) {
  std::vector<std::string> ids = nodeIds(4);
  std::vector<std::string> reversed(ids.rbegin(), ids.rend());
  EXPECT_EQ(owners(HashRing(ids, 128)), owners(HashRing(reversed, 128)));
}

TEST(HashRing, SpreadsSlotsEvenly) {
  for (int nodes : {3, 4, 6}) {
    HashRing ring(nodeIds(nodes), 128);
    std::map<std::string, int> counts;
    for (const std::string& owner : owners(ring)) ++counts[owner];
    ASSERT_EQ(counts.size(), static_cast<size_t>(nodes));
    const double fair = static_cast<double>(kNumSlots) / nodes;
    for (const auto& [id, count] : counts) {
      // With 128 virtual nodes the spread is typically within ~10% of fair;
      // the bound is loose so the test documents intent without being flaky.
      EXPECT_GT(count, 0.75 * fair) << nodes << " nodes, " << id;
      EXPECT_LT(count, 1.25 * fair) << nodes << " nodes, " << id;
      std::printf("%d nodes: %s owns %d slots (%.1f%% of fair share)\n", nodes, id.c_str(), count,
                  100.0 * count / fair);
    }
  }
}

TEST(HashRing, AddingANodeOnlyMovesSlotsToIt) {
  for (int nodes : {1, 3, 5}) {
    std::vector<std::string> before_ids = nodeIds(nodes);
    std::vector<std::string> after_ids = nodeIds(nodes + 1);
    const std::string& added = after_ids.back();
    auto before = owners(HashRing(before_ids, 128));
    auto after = owners(HashRing(after_ids, 128));

    int moved = 0;
    for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
      if (before[slot] != after[slot]) {
        EXPECT_EQ(after[slot], added) << "slot " << slot << " moved between existing nodes";
        ++moved;
      }
    }
    // Roughly 1/(N+1) of the slots move.
    const double expected = static_cast<double>(kNumSlots) / (nodes + 1);
    EXPECT_GT(moved, 0.75 * expected);
    EXPECT_LT(moved, 1.25 * expected);
  }
}

TEST(HashRing, RemovingANodeOnlyMovesItsSlots) {
  std::vector<std::string> before_ids = nodeIds(5);
  std::vector<std::string> after_ids = before_ids;
  after_ids.erase(after_ids.begin() + 2);  // remove n3
  auto before = owners(HashRing(before_ids, 128));
  auto after = owners(HashRing(after_ids, 128));
  for (uint16_t slot = 0; slot < kNumSlots; ++slot) {
    if (before[slot] != "n3") EXPECT_EQ(before[slot], after[slot]) << "slot " << slot;
    EXPECT_NE(after[slot], "n3");
  }
}

}  // namespace
}  // namespace kv::cluster

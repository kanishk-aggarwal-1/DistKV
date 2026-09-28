#include "cluster/slot.h"

#include <gtest/gtest.h>

namespace kv::cluster {
namespace {

TEST(Slot, Crc16MatchesReferenceCheckValue) {
  // The standard check value for CRC-16/XMODEM, also quoted in the Redis
  // Cluster specification.
  EXPECT_EQ(crc16("123456789"), 0x31C3);
}

TEST(Slot, KeySlotMatchesRedis) {
  // Values from Redis's CLUSTER KEYSLOT.
  EXPECT_EQ(keySlot("foo"), 12182);
  EXPECT_EQ(keySlot("bar"), 5061);
  EXPECT_EQ(keySlot(""), 0);
}

TEST(Slot, HashTagsFollowRedisRules) {
  // Only the first non-empty {...} is hashed.
  EXPECT_EQ(keySlot("{user1000}.following"), keySlot("{user1000}.followers"));
  EXPECT_EQ(keySlot("{user1000}.following"), keySlot("user1000"));
  EXPECT_EQ(keySlot("foo{bar}{zap}"), keySlot("bar"));
  EXPECT_EQ(keySlot("foo{{bar}}zap"), keySlot("{bar"));
  // An empty tag means the whole key is hashed.
  EXPECT_EQ(keySlot("foo{}{bar}"), crc16("foo{}{bar}") % kNumSlots);
  // No closing brace: the whole key is hashed.
  EXPECT_EQ(keySlot("foo{bar"), crc16("foo{bar") % kNumSlots);
}

TEST(Slot, AlwaysInRange) {
  for (int i = 0; i < 100000; ++i) EXPECT_LT(keySlot("key:" + std::to_string(i)), kNumSlots);
}

}  // namespace
}  // namespace kv::cluster

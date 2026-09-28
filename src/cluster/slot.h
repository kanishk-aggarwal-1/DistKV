#pragma once

#include <cstdint>
#include <string_view>

namespace kv::cluster {

// Keys are partitioned into 16384 hash slots exactly as in Redis Cluster, so
// cluster-aware clients (redis-cli -c, memtier --cluster-mode) compute the
// same slot for a key that the server does.
inline constexpr uint16_t kNumSlots = 16384;

// CRC16-CCITT (XMODEM), the variant Redis Cluster uses.
uint16_t crc16(std::string_view data);

// Slot of a key: CRC16(key) mod 16384. If the key contains a non-empty
// "{...}" hash tag, only the tag is hashed, so related keys can be forced
// into the same slot (e.g. "{user1}.name" and "{user1}.email").
uint16_t keySlot(std::string_view key);

}  // namespace kv::cluster

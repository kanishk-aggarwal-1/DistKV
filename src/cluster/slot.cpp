#include "cluster/slot.h"

#include <array>

namespace kv::cluster {

namespace {

constexpr std::array<uint16_t, 256> makeCrc16Table() {
  std::array<uint16_t, 256> table{};
  for (unsigned i = 0; i < 256; ++i) {
    uint16_t crc = static_cast<uint16_t>(i << 8);
    for (int bit = 0; bit < 8; ++bit) {
      crc = (crc & 0x8000) ? static_cast<uint16_t>((crc << 1) ^ 0x1021)
                           : static_cast<uint16_t>(crc << 1);
    }
    table[i] = crc;
  }
  return table;
}

constexpr std::array<uint16_t, 256> kCrc16Table = makeCrc16Table();

}  // namespace

uint16_t crc16(std::string_view data) {
  uint16_t crc = 0;
  for (char c : data) {
    uint8_t byte = static_cast<uint8_t>(c);
    crc = static_cast<uint16_t>((crc << 8) ^ kCrc16Table[((crc >> 8) ^ byte) & 0xFF]);
  }
  return crc;
}

uint16_t keySlot(std::string_view key) {
  size_t open = key.find('{');
  if (open != std::string_view::npos) {
    size_t close = key.find('}', open + 1);
    if (close != std::string_view::npos && close > open + 1) {
      key = key.substr(open + 1, close - open - 1);
    }
  }
  return crc16(key) % kNumSlots;
}

}  // namespace kv::cluster

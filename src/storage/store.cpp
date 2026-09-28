#include "storage/store.h"

#include <mutex>
#include <stdexcept>

#include "cluster/slot.h"

namespace kv {

using cluster::keySlot;
using cluster::kNumSlots;

Store::Store(size_t num_stripes, SlotState initial_state)
    : num_stripes_(num_stripes), stripes_(std::make_unique<Stripe[]>(num_stripes)) {
  if (num_stripes == 0) throw std::invalid_argument("Store needs at least one stripe");
  slots_.resize(kNumSlots);
  for (Slot& slot : slots_) slot.state = initial_state;
}

namespace {

// Slot-level routing shared by all client operations. Returns the redirect to
// send, or nullopt if the request may proceed to key-level checks.
std::optional<Access> routeBySlotState(SlotState state, bool asking) {
  switch (state) {
    case SlotState::kNotOwned:
      return Access::kMoved;
    case SlotState::kImporting:
      // Only a client redirected by the source may touch a slot mid-import;
      // anyone else is sent to the current owner.
      if (!asking) return Access::kMoved;
      return std::nullopt;
    case SlotState::kOwned:
    case SlotState::kMigrating:
      return std::nullopt;
  }
  return Access::kMoved;
}

}  // namespace

Access Store::get(std::string_view key, bool asking, std::optional<std::string>& value) const {
  uint16_t s = keySlot(key);
  std::shared_lock lock(lockFor(s));
  const Slot& slot = slots_[s];
  if (auto redirect = routeBySlotState(slot.state, asking)) return *redirect;

  auto it = slot.keys.find(key);
  if (it == slot.keys.end()) {
    // A migrating slot's absent key may already live on the target.
    if (slot.state == SlotState::kMigrating) return Access::kAsk;
    value.reset();
    return Access::kServed;
  }
  // Copy while locked: the entry may be overwritten as soon as we unlock.
  // In-flight keys are still readable; their value cannot change until the
  // target has it.
  value = it->second.value;
  return Access::kServed;
}

Access Store::set(std::string_view key, std::string_view value, bool asking) {
  uint16_t s = keySlot(key);
  std::unique_lock lock(lockFor(s));
  Slot& slot = slots_[s];
  if (auto redirect = routeBySlotState(slot.state, asking)) return *redirect;

  auto it = slot.keys.find(key);
  if (it == slot.keys.end()) {
    // New keys of a migrating slot are created on the target, so the source
    // only ever shrinks and the migration terminates.
    if (slot.state == SlotState::kMigrating) return Access::kAsk;
    slot.keys.emplace(std::string(key), Entry{std::string(value)});
    return Access::kServed;
  }
  if (it->second.in_flight) return Access::kTryAgain;
  it->second.value.assign(value);
  return Access::kServed;
}

Access Store::del(std::span<const std::string> keys, bool asking, int64_t& deleted) {
  deleted = 0;
  if (keys.empty()) return Access::kServed;
  uint16_t s = keySlot(keys[0]);
  std::unique_lock lock(lockFor(s));
  Slot& slot = slots_[s];
  if (auto redirect = routeBySlotState(slot.state, asking)) return *redirect;

  if (slot.state == SlotState::kMigrating) {
    // Same rules as Redis: if every key is absent here they may all be on
    // the target (ASK); if only some are, the request is split across two
    // nodes and cannot be served atomically (TRYAGAIN).
    size_t present = 0;
    for (const std::string& key : keys) {
      auto it = slot.keys.find(key);
      if (it == slot.keys.end()) continue;
      if (it->second.in_flight) return Access::kTryAgain;
      ++present;
    }
    if (present == 0) return Access::kAsk;
    if (present < keys.size()) return Access::kTryAgain;
  }

  for (const std::string& key : keys) {
    auto it = slot.keys.find(key);
    if (it != slot.keys.end()) {
      slot.keys.erase(it);
      ++deleted;
    }
  }
  return Access::kServed;
}

SlotState Store::slotState(uint16_t slot) const {
  std::shared_lock lock(lockFor(slot));
  return slots_[slot].state;
}

void Store::applyOwnership(uint16_t slot, bool owned_here) {
  std::unique_lock lock(lockFor(slot));
  SlotState& state = slots_[slot].state;
  if (owned_here) {
    if (state != SlotState::kMigrating) state = SlotState::kOwned;
  } else {
    if (state != SlotState::kImporting) state = SlotState::kNotOwned;
  }
}

bool Store::beginMigration(uint16_t slot) {
  std::unique_lock lock(lockFor(slot));
  SlotState& state = slots_[slot].state;
  if (state != SlotState::kOwned && state != SlotState::kMigrating) return false;
  state = SlotState::kMigrating;
  return true;
}

bool Store::beginImport(uint16_t slot, uint64_t migration_id) {
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  if (s.state == SlotState::kImporting) return s.import_id == migration_id;
  if (s.state != SlotState::kNotOwned) return false;
  s.state = SlotState::kImporting;
  s.import_id = migration_id;
  s.import_seq = 0;
  return true;
}

std::vector<KeyValue> Store::takeMigrationBatch(uint16_t slot, size_t max_keys,
                                                size_t max_bytes) {
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  std::vector<KeyValue> batch;
  if (s.state != SlotState::kMigrating) return batch;

  // Resume an unfinished batch first: its keys are still frozen.
  for (const auto& [key, entry] : s.keys) {
    if (entry.in_flight) batch.push_back({key, entry.value});
  }
  if (!batch.empty()) return batch;

  size_t bytes = 0;
  for (auto& [key, entry] : s.keys) {
    if (batch.size() >= max_keys || (bytes >= max_bytes && !batch.empty())) break;
    entry.in_flight = true;
    bytes += key.size() + entry.value.size();
    batch.push_back({key, entry.value});
  }
  return batch;
}

void Store::finishMigrationBatch(uint16_t slot, const std::vector<KeyValue>& batch) {
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  for (const KeyValue& kv : batch) {
    auto it = s.keys.find(kv.key);
    if (it != s.keys.end() && it->second.in_flight) s.keys.erase(it);
  }
}

Store::ImportResult Store::importBatch(uint16_t slot, uint64_t migration_id, uint64_t seq,
                                       const std::vector<KeyValue>& batch) {
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  if (s.state != SlotState::kImporting || s.import_id != migration_id) {
    return ImportResult::kRejected;
  }
  if (seq <= s.import_seq) return ImportResult::kStale;
  // Overwriting is safe: a key in this batch is frozen on the source, so no
  // client can have written it here yet (clients only reach this node for a
  // key after the source has deleted it, which happens after this batch).
  for (const KeyValue& kv : batch) s.keys.insert_or_assign(kv.key, Entry{kv.value});
  s.import_seq = seq;
  return ImportResult::kApplied;
}

size_t Store::size() const {
  size_t total = 0;
  for (uint16_t s = 0; s < kNumSlots; ++s) total += keysInSlot(s);
  return total;
}

size_t Store::keysInSlot(uint16_t slot) const {
  std::shared_lock lock(lockFor(slot));
  return slots_[slot].keys.size();
}

bool Store::contains(std::string_view key) const {
  uint16_t s = keySlot(key);
  std::shared_lock lock(lockFor(s));
  return slots_[s].keys.find(key) != slots_[s].keys.end();
}

}  // namespace kv

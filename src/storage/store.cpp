#include "storage/store.h"

#include <mutex>
#include <stdexcept>

#include "cluster/slot.h"
#include "replication/replication_log.h"

namespace kv {

using cluster::keySlot;
using cluster::kNumSlots;
using replication::Op;

Store::Store(size_t num_stripes, SlotState initial_state)
    : num_stripes_(num_stripes), stripes_(std::make_unique<Stripe[]>(num_stripes)) {
  if (num_stripes == 0) throw std::invalid_argument("Store needs at least one stripe");
  slots_.resize(kNumSlots);
  for (Slot& slot : slots_) slot.state = initial_state;
}

std::optional<Access> Store::route(const Slot& slot, bool asking) const {
  // A backup serves no clients; they are redirected to its primary.
  if (replica()) return Access::kMoved;
  switch (slot.state) {
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

bool Store::writesAllowed() const { return log_ == nullptr || log_->accepting(); }

uint64_t Store::logPut(std::string_view key, std::string_view value) {
  if (log_ == nullptr) return 0;
  Op op;
  op.type = Op::Type::kPut;
  op.key = key;
  op.value = value;
  return log_->append(std::move(op));
}

uint64_t Store::logDelete(std::string_view key) {
  if (log_ == nullptr) return 0;
  Op op;
  op.type = Op::Type::kDelete;
  op.key = key;
  return log_->append(std::move(op));
}

Access Store::get(std::string_view key, bool asking, std::optional<std::string>& value) const {
  uint16_t s = keySlot(key);
  std::shared_lock lock(lockFor(s));
  const Slot& slot = slots_[s];
  if (auto redirect = route(slot, asking)) return *redirect;

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

Access Store::set(std::string_view key, std::string_view value, bool asking, uint64_t& seq) {
  seq = 0;
  uint16_t s = keySlot(key);
  std::unique_lock lock(lockFor(s));
  Slot& slot = slots_[s];
  if (auto redirect = route(slot, asking)) return *redirect;

  auto it = slot.keys.find(key);
  if (it == slot.keys.end()) {
    // New keys of a migrating slot are created on the target, so the source
    // only ever shrinks and the migration terminates.
    if (slot.state == SlotState::kMigrating) return Access::kAsk;
    if (!writesAllowed()) return Access::kNoReplicas;
    slot.keys.emplace(std::string(key), Entry{std::string(value)});
  } else {
    if (it->second.in_flight) return Access::kTryAgain;
    if (!writesAllowed()) return Access::kNoReplicas;
    it->second.value.assign(value);
  }
  seq = logPut(key, value);
  return Access::kServed;
}

Access Store::del(std::span<const std::string> keys, bool asking, int64_t& deleted, uint64_t& seq) {
  deleted = 0;
  seq = 0;
  if (keys.empty()) return Access::kServed;
  uint16_t s = keySlot(keys[0]);
  std::unique_lock lock(lockFor(s));
  Slot& slot = slots_[s];
  if (auto redirect = route(slot, asking)) return *redirect;

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
  if (!writesAllowed()) return Access::kNoReplicas;

  for (const std::string& key : keys) {
    auto it = slot.keys.find(key);
    if (it != slot.keys.end()) {
      slot.keys.erase(it);
      ++deleted;
      seq = logDelete(key);
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

bool Store::logMigration(uint16_t slot, std::string_view target_addr, uint64_t& seq) {
  seq = 0;
  std::unique_lock lock(lockFor(slot));
  SlotState state = slots_[slot].state;
  if (replica() || (state != SlotState::kOwned && state != SlotState::kMigrating)) return false;
  if (!writesAllowed()) return false;
  if (log_ != nullptr) {
    Op op;
    op.type = Op::Type::kSlotMigrating;
    op.slot = slot;
    op.peer_addr = target_addr;
    seq = log_->append(std::move(op));
  }
  return true;
}

bool Store::beginMigration(uint16_t slot) {
  std::unique_lock lock(lockFor(slot));
  SlotState& state = slots_[slot].state;
  if (replica() || (state != SlotState::kOwned && state != SlotState::kMigrating)) return false;
  state = SlotState::kMigrating;
  return true;
}

bool Store::beginImport(uint16_t slot, uint64_t migration_id) {
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  if (replica()) return false;
  if (s.state == SlotState::kImporting) {
    if (migration_id == s.import_id) return true;
    // A newer id re-arms the import (the coordinator resumes a migration
    // after a failover). Keys imported so far stay.
    if (migration_id < s.import_id) return false;
  } else if (s.state != SlotState::kNotOwned) {
    return false;
  }
  if (!writesAllowed()) return false;
  s.state = SlotState::kImporting;
  s.import_id = migration_id;
  s.import_seq = 0;
  if (log_ != nullptr) {
    Op op;
    op.type = Op::Type::kSlotImporting;
    op.slot = slot;
    op.migration_id = migration_id;
    log_->append(std::move(op));
  }
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

uint64_t Store::logMigrationDeletes(uint16_t slot, const std::vector<KeyValue>& batch) {
  std::unique_lock lock(lockFor(slot));
  if (log_ == nullptr) return 0;
  if (!writesAllowed()) return 0;
  uint64_t seq = 0;
  for (const KeyValue& kv : batch) seq = logDelete(kv.key);
  return seq;
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
                                       const std::vector<KeyValue>& batch, uint64_t& repl_seq) {
  repl_seq = 0;
  std::unique_lock lock(lockFor(slot));
  Slot& s = slots_[slot];
  if (replica() || s.state != SlotState::kImporting || s.import_id != migration_id) {
    return ImportResult::kRejected;
  }
  if (seq <= s.import_seq) return ImportResult::kStale;
  if (!writesAllowed()) return ImportResult::kNoReplicas;
  // Overwriting is safe: a key in this batch is frozen on the source, so no
  // client can have written it here yet (clients only reach this node for a
  // key after the source has deleted it, which happens after this batch).
  for (const KeyValue& kv : batch) {
    s.keys.insert_or_assign(kv.key, Entry{kv.value});
    repl_seq = logPut(kv.key, kv.value);
  }
  s.import_seq = seq;
  return ImportResult::kApplied;
}

uint64_t Store::logSlotSnapshot(uint16_t slot) {
  std::unique_lock lock(lockFor(slot));
  if (log_ == nullptr) return 0;
  Op op;
  op.type = Op::Type::kSlotSnapshot;
  op.slot = slot;
  for (const auto& [key, entry] : slots_[slot].keys) op.entries.push_back({key, entry.value});
  return log_->append(std::move(op));
}

void Store::applyReplicated(const Op& op) {
  switch (op.type) {
    case Op::Type::kPut: {
      uint16_t s = keySlot(op.key);
      std::unique_lock lock(lockFor(s));
      slots_[s].keys.insert_or_assign(op.key, Entry{op.value});
      break;
    }
    case Op::Type::kDelete: {
      uint16_t s = keySlot(op.key);
      std::unique_lock lock(lockFor(s));
      auto it = slots_[s].keys.find(op.key);
      if (it != slots_[s].keys.end()) slots_[s].keys.erase(it);
      break;
    }
    case Op::Type::kSlotSnapshot: {
      std::unique_lock lock(lockFor(op.slot));
      Map& keys = slots_[op.slot].keys;
      keys.clear();
      for (const KeyValue& kv : op.entries) keys.emplace(kv.key, Entry{kv.value});
      break;
    }
    case Op::Type::kSlotMigrating: {
      std::unique_lock lock(lockFor(op.slot));
      slots_[op.slot].state = SlotState::kMigrating;
      break;
    }
    case Op::Type::kSlotImporting: {
      std::unique_lock lock(lockFor(op.slot));
      Slot& s = slots_[op.slot];
      s.state = SlotState::kImporting;
      s.import_id = op.migration_id;
      s.import_seq = 0;
      break;
    }
  }
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

bool Store::contains(std::string_view key) const { return peek(key).has_value(); }

std::optional<std::string> Store::peek(std::string_view key) const {
  uint16_t s = keySlot(key);
  std::shared_lock lock(lockFor(s));
  auto it = slots_[s].keys.find(key);
  if (it == slots_[s].keys.end()) return std::nullopt;
  return it->second.value;
}

}  // namespace kv

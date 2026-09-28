#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace kv {

// How this node treats a hash slot.
enum class SlotState : uint8_t {
  kOwned,      // served here
  kNotOwned,   // not served here: clients are redirected with MOVED
  kMigrating,  // served here, but keys are being moved out: present keys are
               // served, absent keys are redirected to the target with ASK
  kImporting,  // keys are being moved in: served only to clients that sent
               // ASKING (i.e. were redirected here by the source)
};

// Outcome of a client operation.
enum class Access {
  kServed,    // performed
  kMoved,     // the slot is not served here
  kAsk,       // the slot is migrating and the key is not here (any more)
  kTryAgain,  // a key is being copied to the target right now, or a
              // multi-key request is split between source and target
};

struct KeyValue {
  std::string key;
  std::string value;
};

// Thread-safe in-memory key-value map, partitioned by hash slot.
//
// Keys are grouped by slot so a whole slot can be enumerated and migrated.
// Slots are guarded by a fixed number of lock stripes (slot % stripes), each
// a reader-writer lock. A slot's state lives under the same lock as its keys,
// so checking the state and applying an operation is one atomic step: a
// concurrent migration can never slip in between.
class Store {
 public:
  explicit Store(size_t num_stripes = 256, SlotState initial_state = SlotState::kOwned);

  // ---- Client operations. `asking`: the client sent ASKING first.

  // On kServed, `value` is the value or nullopt if the key does not exist.
  Access get(std::string_view key, bool asking, std::optional<std::string>& value) const;
  Access set(std::string_view key, std::string_view value, bool asking);
  // All keys must hash to the same slot. On kServed, `deleted` counts removed keys.
  Access del(std::span<const std::string> keys, bool asking, int64_t& deleted);

  // ---- Slot administration (driven by the cluster control plane).

  SlotState slotState(uint16_t slot) const;
  // Applies ownership from a cluster map without disturbing a migration in
  // progress: owned -> kOwned unless kMigrating; not owned -> kNotOwned
  // unless kImporting.
  void applyOwnership(uint16_t slot, bool owned_here);
  // Source side. Only valid for kOwned (or already kMigrating) slots.
  bool beginMigration(uint16_t slot);
  // Target side. Only valid for kNotOwned slots (or a retry with the same id).
  bool beginImport(uint16_t slot, uint64_t migration_id);

  // ---- Migration, source side.

  // Marks up to `max_keys` keys (or about `max_bytes`) of a migrating slot
  // as in flight and returns copies of them. While in flight, a key can
  // still be read but writes to it get kTryAgain, so the copy stays exact.
  // If a previous batch was never finished, its keys are returned again.
  // Returns an empty batch once the slot has no keys left.
  std::vector<KeyValue> takeMigrationBatch(uint16_t slot, size_t max_keys, size_t max_bytes);
  // Deletes a batch after the target has acknowledged it.
  void finishMigrationBatch(uint16_t slot, const std::vector<KeyValue>& batch);

  // ---- Migration, target side.

  enum class ImportResult { kApplied, kStale, kRejected };
  // Stores a batch sent by the source. Batches carry an increasing `seq`
  // per migration; a batch at or below the last applied seq is a delayed
  // duplicate and is ignored (kStale), so it cannot overwrite newer writes.
  // kRejected if the slot is not importing under `migration_id`.
  ImportResult importBatch(uint16_t slot, uint64_t migration_id, uint64_t seq,
                           const std::vector<KeyValue>& batch);

  // ---- Introspection (tests and tooling).

  size_t size() const;  // not a consistent snapshot while writers are active
  size_t keysInSlot(uint16_t slot) const;
  bool contains(std::string_view key) const;  // ignores slot state

 private:
  struct Hash {
    using is_transparent = void;
    size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
  };

  struct Entry {
    std::string value;
    bool in_flight = false;  // being copied to a migration target
  };

  using Map = std::unordered_map<std::string, Entry, Hash, std::equal_to<>>;

  struct Slot {
    SlotState state;
    Map keys;
    uint64_t import_id = 0;   // migration being imported (kImporting only)
    uint64_t import_seq = 0;  // last applied batch of that migration
  };

  // Aligned to a cache line so that neighbouring stripes' locks do not
  // share one (false sharing).
  struct alignas(64) Stripe {
    mutable std::shared_mutex mutex;
  };

  std::shared_mutex& lockFor(uint16_t slot) const { return stripes_[slot % num_stripes_].mutex; }

  size_t num_stripes_;
  std::unique_ptr<Stripe[]> stripes_;
  std::vector<Slot> slots_;  // indexed by slot; slots_[s] guarded by lockFor(s)
};

}  // namespace kv

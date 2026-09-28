#include "storage/store.h"

#include "cluster/slot.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace kv {
namespace {

// Wrappers for slots this node owns, where every operation is served.
std::optional<std::string> get(const Store& store, std::string_view key) {
  std::optional<std::string> value;
  EXPECT_EQ(store.get(key, false, value), Access::kServed);
  return value;
}

void set(Store& store, std::string_view key, std::string_view value) {
  uint64_t seq = 0;
  EXPECT_EQ(store.set(key, value, false, seq), Access::kServed);
}

bool del(Store& store, const std::string& key) {
  int64_t deleted = 0;
  uint64_t seq = 0;
  EXPECT_EQ(store.del(std::span<const std::string>(&key, 1), false, deleted, seq), Access::kServed);
  return deleted == 1;
}

TEST(Store, GetMissingKey) {
  Store store;
  EXPECT_FALSE(get(store, "missing").has_value());
}

TEST(Store, SetThenGet) {
  Store store;
  set(store, "k", "v");
  EXPECT_EQ(get(store, "k"), "v");
}

TEST(Store, SetOverwrites) {
  Store store;
  set(store, "k", "v1");
  set(store, "k", "v2");
  EXPECT_EQ(get(store, "k"), "v2");
  EXPECT_EQ(store.size(), 1u);
}

TEST(Store, DelReportsWhetherKeyExisted) {
  Store store;
  set(store, "k", "v");
  EXPECT_TRUE(del(store, "k"));
  EXPECT_FALSE(del(store, "k"));
  EXPECT_FALSE(get(store, "k").has_value());
}

TEST(Store, KeysAndValuesAreBinarySafe) {
  Store store;
  std::string key("a\0b", 3);
  std::string value("\r\n\0", 3);
  set(store, key, value);
  EXPECT_EQ(get(store, key), value);
  EXPECT_FALSE(get(store, "a").has_value());
}

TEST(Store, SingleStripeStillWorks) {
  Store store(1);
  for (int i = 0; i < 100; ++i) set(store, std::to_string(i), std::to_string(i * 2));
  EXPECT_EQ(store.size(), 100u);
  EXPECT_EQ(get(store, "42"), "84");
}

TEST(Store, ZeroStripesIsRejected) { EXPECT_THROW(Store(0), std::invalid_argument); }

// Each thread owns a disjoint set of keys, so the final state is fully
// determined even though the threads run concurrently on shared stripes.
TEST(Store, ConcurrentWritersOnDisjointKeys) {
  Store store(8);  // few stripes, so threads collide on locks
  constexpr int kThreads = 8;
  constexpr int kKeysPerThread = 2000;

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, t] {
      for (int i = 0; i < kKeysPerThread; ++i) {
        std::string key = "t" + std::to_string(t) + ":" + std::to_string(i);
        set(store, key, "first");
        set(store, key, key);  // overwrite
        ASSERT_EQ(get(store, key), key);
        if (i % 2 == 0) ASSERT_TRUE(del(store, key));
      }
    });
  }
  for (auto& th : threads) th.join();

  EXPECT_EQ(store.size(), static_cast<size_t>(kThreads * kKeysPerThread / 2));
  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kKeysPerThread; ++i) {
      std::string key = "t" + std::to_string(t) + ":" + std::to_string(i);
      if (i % 2 == 0) {
        EXPECT_FALSE(get(store, key).has_value()) << key;
      } else {
        EXPECT_EQ(get(store, key), key);
      }
    }
  }
}

// Readers and writers hammer the same few keys. Mostly a target for
// ThreadSanitizer; the functional check is that a reader only ever sees a
// value some writer actually wrote.
TEST(Store, ConcurrentReadersAndWritersOnSameKeys) {
  Store store(4);
  constexpr int kIterations = 20000;
  const std::vector<std::string> keys = {"a", "b", "c"};

  std::vector<std::thread> threads;
  for (int w = 0; w < 2; ++w) {
    threads.emplace_back([&, w] {
      for (int i = 0; i < kIterations; ++i) {
        const std::string& key = keys[i % keys.size()];
        set(store, key, "writer" + std::to_string(w));
        if (i % 7 == 0) del(store, key);
      }
    });
  }
  for (int r = 0; r < 4; ++r) {
    threads.emplace_back([&] {
      for (int i = 0; i < kIterations; ++i) {
        auto value = get(store, keys[i % keys.size()]);
        if (value) ASSERT_TRUE(*value == "writer0" || *value == "writer1") << *value;
      }
    });
  }
  for (auto& th : threads) th.join();
}

// ---- Slot states and migration ----------------------------------------------
//
// Keys with the same {hash tag} share a slot, which lets these tests put
// several keys in one slot on purpose.

class SlotStateTest : public ::testing::Test {
 protected:
  static std::string key(const std::string& name) { return "{tag}" + name; }
  uint16_t slot() const { return cluster::keySlot("{tag}"); }

  Access getAccess(std::string_view k, bool asking = false) {
    std::optional<std::string> value;
    return store_.get(k, asking, value);
  }
  Access setAccess(std::string_view k, std::string_view v, bool asking = false) {
    uint64_t seq = 0;
    return store_.set(k, v, asking, seq);
  }
  Access delAccess(std::vector<std::string> keys, bool asking = false) {
    int64_t deleted = 0;
    uint64_t seq = 0;
    return store_.del(keys, asking, deleted, seq);
  }
  Store::ImportResult import(uint64_t id, uint64_t seq, const std::vector<KeyValue>& batch) {
    uint64_t repl_seq = 0;
    return store_.importBatch(slot(), id, seq, batch, repl_seq);
  }

  Store store_{16};
};

TEST_F(SlotStateTest, NotOwnedSlotRedirectsEverything) {
  store_.applyOwnership(slot(), false);
  EXPECT_EQ(getAccess(key("a")), Access::kMoved);
  EXPECT_EQ(setAccess(key("a"), "v", false), Access::kMoved);
  EXPECT_EQ(delAccess({key("a")}), Access::kMoved);
  // ASKING does not help for a slot that is not being imported.
  EXPECT_EQ(getAccess(key("a"), true), Access::kMoved);
}

TEST_F(SlotStateTest, ImportingSlotServesOnlyAskingClients) {
  store_.applyOwnership(slot(), false);
  ASSERT_TRUE(store_.beginImport(slot(), 7));
  EXPECT_EQ(setAccess(key("a"), "v", false), Access::kMoved);
  EXPECT_EQ(setAccess(key("a"), "v", true), Access::kServed);
  EXPECT_EQ(getAccess(key("a"), true), Access::kServed);
  EXPECT_EQ(getAccess(key("a"), false), Access::kMoved);
}

TEST_F(SlotStateTest, MigratingSlotServesPresentKeysAndAsksForAbsentOnes) {
  set(store_, key("present"), "v");
  ASSERT_TRUE(store_.beginMigration(slot()));

  std::optional<std::string> value;
  EXPECT_EQ(store_.get(key("present"), false, value), Access::kServed);
  EXPECT_EQ(value, "v");
  EXPECT_EQ(setAccess(key("present"), "v2", false), Access::kServed);

  EXPECT_EQ(getAccess(key("absent")), Access::kAsk);
  // New keys are created on the target, never on the source.
  EXPECT_EQ(setAccess(key("absent"), "v", false), Access::kAsk);
  EXPECT_FALSE(store_.contains(key("absent")));
  EXPECT_EQ(delAccess({key("absent")}), Access::kAsk);
  // Split between source and target: cannot be served atomically.
  EXPECT_EQ(delAccess({key("present"), key("absent")}), Access::kTryAgain);
  EXPECT_TRUE(store_.contains(key("present")));
}

TEST_F(SlotStateTest, InFlightKeysAreReadableButFrozen) {
  set(store_, key("a"), "v1");
  ASSERT_TRUE(store_.beginMigration(slot()));
  auto batch = store_.takeMigrationBatch(slot(), 10, 1 << 20);
  ASSERT_EQ(batch.size(), 1u);

  std::optional<std::string> value;
  EXPECT_EQ(store_.get(key("a"), false, value), Access::kServed);
  EXPECT_EQ(value, "v1");
  EXPECT_EQ(setAccess(key("a"), "v2", false), Access::kTryAgain);
  EXPECT_EQ(delAccess({key("a")}), Access::kTryAgain);

  store_.finishMigrationBatch(slot(), batch);
  EXPECT_FALSE(store_.contains(key("a")));
  EXPECT_EQ(getAccess(key("a")), Access::kAsk);  // now lives on the target
}

TEST_F(SlotStateTest, MigrationBatchesDrainTheSlotAndResumeUnfinishedBatches) {
  for (int i = 0; i < 10; ++i) set(store_, key(std::to_string(i)), "v");
  ASSERT_TRUE(store_.beginMigration(slot()));

  auto first = store_.takeMigrationBatch(slot(), 4, 1 << 20);
  ASSERT_EQ(first.size(), 4u);
  // Not finished (e.g. the RPC failed): the same keys come back.
  auto again = store_.takeMigrationBatch(slot(), 4, 1 << 20);
  ASSERT_EQ(again.size(), 4u);
  for (size_t i = 0; i < first.size(); ++i) EXPECT_EQ(first[i].key, again[i].key);

  size_t moved = 0;
  for (auto batch = again; !batch.empty(); batch = store_.takeMigrationBatch(slot(), 4, 1 << 20)) {
    moved += batch.size();
    store_.finishMigrationBatch(slot(), batch);
  }
  EXPECT_EQ(moved, 10u);
  EXPECT_EQ(store_.keysInSlot(slot()), 0u);
}

TEST_F(SlotStateTest, MigrationBatchRespectsByteLimitButTakesAtLeastOneKey) {
  set(store_, key("big"), std::string(1000, 'x'));
  set(store_, key("big2"), std::string(1000, 'y'));
  ASSERT_TRUE(store_.beginMigration(slot()));
  EXPECT_EQ(store_.takeMigrationBatch(slot(), 100, 10).size(), 1u);
}

TEST_F(SlotStateTest, ImportRequiresMatchingMigration) {
  std::vector<KeyValue> batch = {{key("a"), "v"}};
  EXPECT_EQ(import(1, 1, batch), Store::ImportResult::kRejected);  // owned
  store_.applyOwnership(slot(), false);
  ASSERT_TRUE(store_.beginImport(slot(), 5));
  EXPECT_FALSE(store_.beginImport(slot(), 4));  // an older migration
  EXPECT_EQ(import(6, 1, batch), Store::ImportResult::kRejected);  // not the current one
  EXPECT_EQ(import(5, 1, batch), Store::ImportResult::kApplied);
  // A newer migration re-arms the import (resume after a failover); the
  // older one can no longer import.
  EXPECT_TRUE(store_.beginImport(slot(), 6));
  EXPECT_EQ(import(5, 2, batch), Store::ImportResult::kRejected);
  EXPECT_EQ(import(6, 1, batch), Store::ImportResult::kApplied);
}

TEST_F(SlotStateTest, DelayedImportBatchCannotOverwriteNewerWrites) {
  store_.applyOwnership(slot(), false);
  ASSERT_TRUE(store_.beginImport(slot(), 5));
  // Attempt seq=1 times out at the source; seq=2 (same keys) is applied.
  ASSERT_EQ(import(5, 2, {{key("a"), "v1"}}), Store::ImportResult::kApplied);
  // The source deletes its copy; a client redirected here overwrites the key.
  ASSERT_EQ(setAccess(key("a"), "client-write", true), Access::kServed);
  // The slow first attempt finally arrives. It must be ignored.
  EXPECT_EQ(import(5, 1, {{key("a"), "v1"}}), Store::ImportResult::kStale);
  std::optional<std::string> value;
  ASSERT_EQ(store_.get(key("a"), true, value), Access::kServed);
  EXPECT_EQ(value, "client-write");
}

TEST_F(SlotStateTest, ApplyOwnershipPreservesMigrationInProgress) {
  ASSERT_TRUE(store_.beginMigration(slot()));
  store_.applyOwnership(slot(), true);  // map still says we own it
  EXPECT_EQ(store_.slotState(slot()), SlotState::kMigrating);
  store_.applyOwnership(slot(), false);  // commit: the target owns it now
  EXPECT_EQ(store_.slotState(slot()), SlotState::kNotOwned);

  ASSERT_TRUE(store_.beginImport(slot(), 9));
  store_.applyOwnership(slot(), false);  // map still says someone else owns it
  EXPECT_EQ(store_.slotState(slot()), SlotState::kImporting);
  store_.applyOwnership(slot(), true);  // commit
  EXPECT_EQ(store_.slotState(slot()), SlotState::kOwned);
}

TEST_F(SlotStateTest, CannotMigrateASlotThatIsNotOwned) {
  store_.applyOwnership(slot(), false);
  EXPECT_FALSE(store_.beginMigration(slot()));
}

}  // namespace
}  // namespace kv

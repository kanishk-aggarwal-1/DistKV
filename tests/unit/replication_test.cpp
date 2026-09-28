#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "cluster/slot.h"
#include "replication/replication_log.h"
#include "storage/store.h"

namespace kv::replication {
namespace {

using namespace std::chrono_literals;

Op put(std::string key, std::string value) {
  Op op;
  op.type = Op::Type::kPut;
  op.key = std::move(key);
  op.value = std::move(value);
  return op;
}

TEST(ReplicationLog, AssignsIncreasingSeqsAndHandsOutOpsInOrder) {
  Progress progress;
  ReplicationLog log(progress);
  EXPECT_EQ(log.append(put("a", "1")), 1u);
  EXPECT_EQ(log.append(put("b", "2")), 2u);
  EXPECT_EQ(log.append(put("c", "3")), 3u);

  auto ops = log.waitForOps(0, 10, 0ms);
  ASSERT_EQ(ops.size(), 3u);
  EXPECT_EQ(ops[0]->key, "a");
  EXPECT_EQ(ops[2]->seq, 3u);
  EXPECT_EQ(log.waitForOps(1, 1, 0ms).at(0)->key, "b");  // after + max
  EXPECT_TRUE(log.waitForOps(3, 10, 0ms).empty());
}

TEST(ReplicationLog, AcknowledgementReleasesOpsAndAdvancesProgress) {
  Progress progress;
  std::atomic<int> notified{0};
  progress.addListener([&] { ++notified; });
  ReplicationLog log(progress);
  log.append(put("a", "1"));
  log.append(put("b", "2"));

  EXPECT_EQ(progress.status(1), Progress::Status::kPending);
  log.acknowledge(1);
  EXPECT_EQ(progress.status(1), Progress::Status::kAcked);
  EXPECT_EQ(progress.status(2), Progress::Status::kPending);
  EXPECT_EQ(notified.load(), 1);
  // Acknowledged ops are not sent again, e.g. on a new stream.
  auto ops = log.waitForOps(0, 10, 0ms);
  ASSERT_EQ(ops.size(), 1u);
  EXPECT_EQ(ops[0]->seq, 2u);
}

TEST(ReplicationLog, FailPendingFailsOnlyUnacknowledgedOps) {
  Progress progress;
  ReplicationLog log(progress);
  log.append(put("a", "1"));
  log.append(put("b", "2"));
  log.acknowledge(1);
  log.failPending();
  EXPECT_EQ(progress.status(1), Progress::Status::kAcked);
  EXPECT_EQ(progress.status(2), Progress::Status::kFailed);
  EXPECT_TRUE(log.waitForOps(0, 10, 0ms).empty());

  // Seqs keep increasing, so a new backup's ops are not mistaken for failed ones.
  uint64_t next = log.append(put("c", "3"));
  EXPECT_EQ(next, 3u);
  EXPECT_EQ(progress.status(next), Progress::Status::kPending);
}

TEST(ReplicationLog, WaitForAckWakesOnAckOrFailure) {
  Progress progress;
  ReplicationLog log(progress);
  uint64_t seq = log.append(put("a", "1"));
  std::thread acker([&] {
    std::this_thread::sleep_for(20ms);
    log.acknowledge(seq);
  });
  EXPECT_TRUE(log.waitForAck(seq, 5000ms));
  acker.join();

  seq = log.append(put("b", "2"));
  std::thread failer([&] {
    std::this_thread::sleep_for(20ms);
    log.failPending();
  });
  EXPECT_FALSE(log.waitForAck(seq, 5000ms));
  failer.join();

  seq = log.append(put("c", "3"));
  EXPECT_FALSE(log.waitForAck(seq, 10ms));  // timeout
}

// ---- Store + log ------------------------------------------------------------

class ReplicatedStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    primary_.setReplicationLog(&log_);
    log_.setAccepting(true);
    backup_.setReplica(true);
  }

  // Applies everything logged so far to the backup, as the stream would.
  void replicate() {
    for (const auto& op : log_.waitForOps(applied_, 1 << 20, 0ms)) {
      backup_.applyReplicated(*op);
      applied_ = op->seq;
    }
    log_.acknowledge(applied_);
  }

  uint64_t set(const std::string& key, const std::string& value) {
    uint64_t seq = 0;
    EXPECT_EQ(primary_.set(key, value, false, seq), Access::kServed);
    return seq;
  }

  Progress progress_;
  ReplicationLog log_{progress_};
  Store primary_{16};
  Store backup_{16, SlotState::kNotOwned};
  uint64_t applied_ = 0;
};

TEST_F(ReplicatedStoreTest, WritesAreRefusedWithoutASyncedBackup) {
  log_.setAccepting(false);
  uint64_t seq = 0;
  EXPECT_EQ(primary_.set("k", "v", false, seq), Access::kNoReplicas);
  EXPECT_FALSE(primary_.contains("k"));
  EXPECT_EQ(seq, 0u);
  // Reads keep working.
  std::optional<std::string> value;
  EXPECT_EQ(primary_.get("k", false, value), Access::kServed);
}

TEST_F(ReplicatedStoreTest, EveryWriteIsLoggedWithItsSeq) {
  uint64_t first = set("a", "1");
  uint64_t second = set("a", "2");
  EXPECT_LT(first, second);

  std::vector<std::string> keys = {"{t}x", "{t}y", "{t}missing"};
  set("{t}x", "1");
  set("{t}y", "1");
  int64_t deleted = 0;
  uint64_t seq = 0;
  ASSERT_EQ(primary_.del(keys, false, deleted, seq), Access::kServed);
  EXPECT_EQ(deleted, 2);
  EXPECT_EQ(seq, log_.lastSeq());  // the reply waits for the last deletion

  replicate();
  EXPECT_EQ(backup_.peek("a"), "2");
  EXPECT_FALSE(backup_.contains("{t}x"));
}

TEST_F(ReplicatedStoreTest, BackupConvergesToPrimaryUnderRandomOperations) {
  std::mt19937 rng(42);
  for (int i = 0; i < 20000; ++i) {
    std::string key = "k" + std::to_string(rng() % 500);
    if (rng() % 4 == 0) {
      int64_t deleted = 0;
      uint64_t seq = 0;
      std::vector<std::string> keys = {key};
      primary_.del(keys, false, deleted, seq);
    } else {
      set(key, std::to_string(i));
    }
    if (i % 97 == 0) replicate();  // the stream lags behind, then catches up
  }
  replicate();
  EXPECT_EQ(backup_.size(), primary_.size());
  for (int k = 0; k < 500; ++k) {
    std::string key = "k" + std::to_string(k);
    EXPECT_EQ(backup_.peek(key), primary_.peek(key)) << key;
  }
}

TEST_F(ReplicatedStoreTest, SlotSnapshotReplacesTheBackupsSlot) {
  set("{s}a", "1");
  set("{s}b", "2");
  const uint16_t slot = cluster::keySlot("{s}");
  // The backup holds stale data for the slot (e.g. from a previous primary).
  Op stale = put("{s}stale", "x");
  backup_.applyReplicated(stale);

  applied_ = log_.lastSeq();  // pretend the individual puts were never sent
  primary_.logSlotSnapshot(slot);
  replicate();
  EXPECT_EQ(backup_.keysInSlot(slot), 2u);
  EXPECT_EQ(backup_.peek("{s}b"), "2");
  EXPECT_FALSE(backup_.contains("{s}stale"));
}

TEST_F(ReplicatedStoreTest, MigrationIntentIsLoggedBeforeTheStateChanges) {
  const uint16_t slot = cluster::keySlot("{m}");
  uint64_t seq = 0;
  ASSERT_TRUE(primary_.logMigration(slot, "10.0.0.9:7009", seq));
  EXPECT_GT(seq, 0u);
  // Not redirecting yet: the backup has not acknowledged.
  EXPECT_EQ(primary_.slotState(slot), SlotState::kOwned);
  replicate();
  EXPECT_EQ(backup_.slotState(slot), SlotState::kMigrating);
  ASSERT_TRUE(primary_.beginMigration(slot));
  EXPECT_EQ(primary_.slotState(slot), SlotState::kMigrating);

  log_.setAccepting(false);
  EXPECT_FALSE(primary_.logMigration(cluster::keySlot("{other}"), "x:1", seq));
}

TEST_F(ReplicatedStoreTest, MigratedKeysAreDeletedOnlyAfterTheBackupHasTheDeletion) {
  set("{m}a", "1");
  const uint16_t slot = cluster::keySlot("{m}");
  uint64_t seq = 0;
  ASSERT_TRUE(primary_.logMigration(slot, "t:1", seq));
  ASSERT_TRUE(primary_.beginMigration(slot));
  auto batch = primary_.takeMigrationBatch(slot, 10, 1 << 20);
  ASSERT_EQ(batch.size(), 1u);

  uint64_t del_seq = primary_.logMigrationDeletes(slot, batch);
  ASSERT_GT(del_seq, 0u);
  // Logged but not applied: the key is still here, frozen.
  EXPECT_TRUE(primary_.contains("{m}a"));
  replicate();
  EXPECT_FALSE(backup_.contains("{m}a"));
  primary_.finishMigrationBatch(slot, batch);
  EXPECT_FALSE(primary_.contains("{m}a"));
}

TEST_F(ReplicatedStoreTest, ReplicaRedirectsClientsButAppliesTheStream) {
  std::optional<std::string> value;
  EXPECT_EQ(backup_.get("k", false, value), Access::kMoved);
  uint64_t seq = 0;
  EXPECT_EQ(backup_.set("k", "v", true, seq), Access::kMoved);
  backup_.applyReplicated(put("k", "v"));
  EXPECT_EQ(backup_.peek("k"), "v");
}

TEST_F(ReplicatedStoreTest, ImportingStateIsReplicatedSoAPromotedBackupKeepsImporting) {
  const uint16_t slot = cluster::keySlot("{i}");
  Store target(16, SlotState::kNotOwned);
  target.setReplicationLog(&log_);
  ASSERT_TRUE(target.beginImport(slot, 7));
  // A newer migration id re-arms the import (resume after a failover)...
  EXPECT_TRUE(target.beginImport(slot, 8));
  // ...an older one does not.
  EXPECT_FALSE(target.beginImport(slot, 7));
  replicate();
  EXPECT_EQ(backup_.slotState(slot), SlotState::kImporting);
  backup_.setReplica(false);  // promoted
  backup_.applyOwnership(slot, false);
  EXPECT_EQ(backup_.slotState(slot), SlotState::kImporting);
}

}  // namespace
}  // namespace kv::replication

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "storage/key_value.h"

namespace kv::replication {

// One change to replicate, in the order the primary applied it.
struct Op {
  enum class Type : uint8_t {
    kPut,
    kDelete,
    kSlotSnapshot,   // replace a slot's contents (full sync of a new backup)
    kSlotMigrating,  // slot state changes, so a promoted backup can resume
    kSlotImporting,  //   a migration its primary had started
  };

  Type type = Type::kPut;
  uint64_t seq = 0;
  uint16_t slot = 0;              // snapshot and slot-state ops
  std::string key;                // put, delete
  std::string value;              // put
  std::vector<KeyValue> entries;  // snapshot
  std::string peer_addr;          // migrating: client address of the target
  uint64_t migration_id = 0;      // importing
};

// Replication progress, shared with the event loops so they can release
// client replies that were waiting for the backup's acknowledgement.
//
// Sequence numbers increase for the lifetime of the process, across backups:
// "failed" covers every seq that was appended but never acknowledged by the
// backup it was sent to.
class Progress {
 public:
  enum class Status { kAcked, kFailed, kPending };

  Status status(uint64_t seq) const {
    if (seq <= acked_.load(std::memory_order_acquire)) return Status::kAcked;
    if (seq <= failed_through_.load(std::memory_order_acquire)) return Status::kFailed;
    return Status::kPending;
  }

  // Called from any thread after progress changes. All listeners must be
  // added before replication starts; the list is read without locking.
  void addListener(std::function<void()> listener) { listeners_.push_back(std::move(listener)); }

 private:
  friend class ReplicationLog;
  void notify() const {
    for (const auto& listener : listeners_) listener();
  }

  std::atomic<uint64_t> acked_{0};
  std::atomic<uint64_t> failed_through_{0};
  std::vector<std::function<void()>> listeners_;
};

// The primary's ordered log of changes not yet acknowledged by its backup.
//
// append() is called while the Store holds the lock of the slot being
// changed, so for any one slot the log order equals the order in which the
// changes were applied. That is the only ordering the backup needs: changes
// to different slots touch different keys and commute.
class ReplicationLog {
 public:
  explicit ReplicationLog(Progress& progress) : progress_(progress) {}

  // Whether client writes are accepted. False while this node is not a
  // primary with a synced backup (strict mode: no single-copy writes).
  bool accepting() const { return accepting_.load(std::memory_order_acquire); }
  void setAccepting(bool accepting) { accepting_.store(accepting, std::memory_order_release); }

  // Assigns the next seq and queues the op. Returns the seq.
  uint64_t append(Op op);

  // Sender side: waits up to `timeout` for ops with seq > `after` and returns
  // at most `max` of them, oldest first.
  std::vector<std::shared_ptr<const Op>> waitForOps(uint64_t after, size_t max,
                                                    std::chrono::milliseconds timeout);

  // The backup has applied everything up to `seq`.
  void acknowledge(uint64_t seq);

  // Fails every op not yet acknowledged and empties the log (the backup is
  // gone, replaced or unresponsive). Clients waiting on those ops get errors.
  void failPending();

  // Blocks until `seq` is acknowledged (true) or failed / timed out (false).
  bool waitForAck(uint64_t seq, std::chrono::milliseconds timeout);

  // How long the oldest unacknowledged op has been waiting (zero if none).
  std::chrono::milliseconds oldestPendingAge() const;

  uint64_t lastSeq() const;
  Progress& progress() { return progress_; }

 private:
  struct Entry {
    std::shared_ptr<const Op> op;
    std::chrono::steady_clock::time_point appended;
  };

  Progress& progress_;
  std::atomic<bool> accepting_{false};
  mutable std::mutex mutex_;
  std::condition_variable changed_;
  std::deque<Entry> pending_;  // unacknowledged ops, increasing seq
  uint64_t next_seq_ = 1;
};

}  // namespace kv::replication

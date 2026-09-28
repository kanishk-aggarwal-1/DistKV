#include "replication/replication_log.h"

#include <algorithm>

namespace kv::replication {

uint64_t ReplicationLog::append(Op op) {
  std::lock_guard lock(mutex_);
  op.seq = next_seq_++;
  const uint64_t seq = op.seq;
  pending_.push_back({std::make_shared<const Op>(std::move(op)), std::chrono::steady_clock::now()});
  changed_.notify_all();
  return seq;
}

std::vector<std::shared_ptr<const Op>> ReplicationLog::waitForOps(uint64_t after, size_t max,
                                                                  std::chrono::milliseconds timeout) {
  std::unique_lock lock(mutex_);
  auto has_new = [&] { return !pending_.empty() && pending_.back().op->seq > after; };
  changed_.wait_for(lock, timeout, has_new);

  std::vector<std::shared_ptr<const Op>> ops;
  for (const Entry& entry : pending_) {
    if (entry.op->seq <= after) continue;
    if (ops.size() >= max) break;
    ops.push_back(entry.op);
  }
  return ops;
}

void ReplicationLog::acknowledge(uint64_t seq) {
  {
    std::lock_guard lock(mutex_);
    while (!pending_.empty() && pending_.front().op->seq <= seq) pending_.pop_front();
    uint64_t current = progress_.acked_.load();
    if (seq > current) progress_.acked_.store(seq, std::memory_order_release);
    changed_.notify_all();
  }
  progress_.notify();
}

void ReplicationLog::failPending() {
  {
    std::lock_guard lock(mutex_);
    if (!pending_.empty()) {
      progress_.failed_through_.store(next_seq_ - 1, std::memory_order_release);
      pending_.clear();
    }
    changed_.notify_all();
  }
  progress_.notify();
}

bool ReplicationLog::waitForAck(uint64_t seq, std::chrono::milliseconds timeout) {
  std::unique_lock lock(mutex_);
  changed_.wait_for(lock, timeout, [&] { return progress_.status(seq) != Progress::Status::kPending; });
  return progress_.status(seq) == Progress::Status::kAcked;
}

std::chrono::milliseconds ReplicationLog::oldestPendingAge() const {
  std::lock_guard lock(mutex_);
  if (pending_.empty()) return std::chrono::milliseconds(0);
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                               pending_.front().appended);
}

uint64_t ReplicationLog::lastSeq() const {
  std::lock_guard lock(mutex_);
  return next_seq_ - 1;
}

}  // namespace kv::replication

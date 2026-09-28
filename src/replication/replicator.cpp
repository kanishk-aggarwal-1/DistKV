#include "replication/replicator.h"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <random>

#include "cluster/rpc.h"
#include "distkv.grpc.pb.h"
#include "replication/ops_proto.h"

namespace kv::replication {

namespace {
constexpr size_t kMaxOpsPerBatch = 256;
constexpr auto kPollInterval = std::chrono::milliseconds(20);
constexpr auto kReconnectDelay = std::chrono::milliseconds(50);

uint64_t newLogId() {
  std::random_device rd;
  return (static_cast<uint64_t>(rd()) << 32) ^ rd() ^
         static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}
}  // namespace

Replicator::Replicator(ReplicationLog& log, std::string self_id,
                       std::chrono::milliseconds ack_timeout)
    : log_(log), self_id_(std::move(self_id)), ack_timeout_(ack_timeout) {}

Replicator::~Replicator() { stop(); }

void Replicator::start(const std::string& group_id, const cluster::NodeInfo& backup) {
  stop();
  std::lock_guard lock(mutex_);
  running_ = true;
  backup_id_ = backup.id;
  broken_ = false;
  thread_ = std::thread([this, group_id, backup, log_id = newLogId()] { run(group_id, backup, log_id); });
}

void Replicator::stop() {
  std::thread thread;
  {
    std::lock_guard lock(mutex_);
    running_ = false;
    backup_id_.clear();
    if (stream_ctx_ != nullptr) stream_ctx_->TryCancel();
    thread = std::move(thread_);
  }
  if (thread.joinable()) thread.join();
  log_.failPending();
}

bool Replicator::running() const {
  std::lock_guard lock(mutex_);
  return running_;
}

std::string Replicator::backupId() const {
  std::lock_guard lock(mutex_);
  return backup_id_;
}

bool Replicator::shouldRun() const {
  std::lock_guard lock(mutex_);
  return running_ && !broken_;
}

void Replicator::giveUp(const char* reason) {
  std::fprintf(stderr, "replication to %s broken: %s\n", backupId().c_str(), reason);
  broken_ = true;
  log_.setAccepting(false);
  log_.failPending();
}

void Replicator::run(std::string group_id, cluster::NodeInfo backup, uint64_t log_id) {
  while (shouldRun()) {
    runStream(group_id, backup, log_id);
    if (!shouldRun()) break;
    std::this_thread::sleep_for(kReconnectDelay);
  }
}

void Replicator::runStream(const std::string& group_id, const cluster::NodeInfo& backup,
                           uint64_t log_id) {
  auto stub = cluster::makeNodeStub(backup.grpc_addr);
  grpc::ClientContext ctx;
  {
    std::lock_guard lock(mutex_);
    if (!running_) return;
    stream_ctx_ = &ctx;
  }
  auto stream = stub->Replicate(&ctx);

  // Reader: the backup's cumulative acks.
  std::atomic<bool> stream_done{false};
  std::thread reader([&] {
    distkv::v1::ReplicationAck ack;
    while (stream->Read(&ack)) log_.acknowledge(ack.applied_seq());
    stream_done = true;
  });

  // Writer: everything in the log, in order. A new stream resends every
  // unacknowledged op; the backup skips seqs it has already applied.
  uint64_t sent = 0;
  bool leaving = false;  // we end the stream (stop or timeout), not the backup
  while (!stream_done) {
    if (!shouldRun()) {
      leaving = true;
      break;
    }
    if (log_.oldestPendingAge() > ack_timeout_) {
      giveUp("backup did not acknowledge in time");
      leaving = true;
      break;
    }
    auto ops = log_.waitForOps(sent, kMaxOpsPerBatch, kPollInterval);
    if (ops.empty()) continue;
    distkv::v1::ReplicationBatch batch;
    batch.set_group_id(group_id);
    batch.set_primary_id(self_id_);
    batch.set_log_id(log_id);
    for (const auto& op : ops) *batch.add_ops() = opToProto(*op);
    // A failed write means the stream is over; the reader will see the end.
    if (!stream->Write(batch)) break;
    sent = ops.back()->seq;
  }

  // Cancel only if we are the ones leaving. If the backup ended the stream,
  // Finish() must return *its* status: a FAILED_PRECONDITION there is the
  // fencing signal, which cancelling would turn into a plain CANCELLED.
  if (leaving) ctx.TryCancel();
  reader.join();
  grpc::Status status = stream->Finish();
  {
    std::lock_guard lock(mutex_);
    stream_ctx_ = nullptr;
  }
  // The backup refusing us is final: it follows a newer map in which we are
  // not its primary (it may have been promoted). Only a re-sync ordered by
  // the coordinator can restart replication.
  if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
    giveUp(status.error_message().c_str());
  }
}

}  // namespace kv::replication

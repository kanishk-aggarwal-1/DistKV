#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>

#include "protocol/resp_parser.h"

namespace kv::net {

// Per-connection protocol state that outlives a single command.
struct Session {
  // Set by ASKING; applies to the next command only (Redis Cluster semantics).
  bool asking = false;
  // Set by the command handler: the reply to the command just executed may
  // only be sent once replication seq `reply_seq` is acknowledged by the
  // backup (0 = no such requirement).
  uint64_t reply_seq = 0;
};

// Executes one command and appends its RESP reply to `out`.
using CommandHandlerFn =
    std::function<void(const resp::Command& cmd, Session& session, std::string& out)>;

// Decides when a reply held for a replication seq may be sent.
struct ReplyGate {
  enum class Status { kReady, kFailed, kWaiting };
  std::function<Status(uint64_t seq)> status;
  // Sent instead of the held reply if its write was never acknowledged.
  std::string failed_reply;
};

// One client connection. Owned by, and only ever touched from, a single
// event-loop thread, so it needs no locking.
//
// Pipelining: one read may contain many commands. They are all parsed and
// executed in order and their replies are appended to one output buffer,
// which is flushed with as few send() calls as possible.
//
// Held replies (replication): a write's reply waits until the backup has
// acknowledged the write. Commands behind it keep executing, but their
// replies queue behind the held one, so the client always receives replies
// in request order and pipelining keeps working.
//
// Backpressure: if a client sends faster than it reads replies, the pending
// output grows. Above kOutputHighWater we stop parsing and stop asking epoll
// for readability, so the kernel's receive buffer fills up and TCP flow
// control slows the client down.
class Connection {
 public:
  static constexpr size_t kOutputHighWater = 1024 * 1024;

  // `gate` may be null when nothing is replicated (standalone mode).
  Connection(int fd, const CommandHandlerFn& handler, const ReplyGate* gate);
  ~Connection();  // closes the socket

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Each returns false when the connection should be closed.
  bool onReadable();
  bool onWritable();
  // Replication made progress: release held replies that are now decided.
  bool onReplicationProgress();

  // The epoll events this connection currently wants.
  uint32_t wantedEvents() const;

  int fd() const { return fd_; }

  // The events currently registered with epoll; maintained by the event loop
  // to skip redundant epoll_ctl calls.
  uint32_t registered_events = 0;

 private:
  struct HeldReply {
    uint64_t seq;
    std::string reply;     // the reply that waits for `seq`
    std::string trailing;  // later replies queued behind it
  };

  // Alternates processInput() and flush() until the input is exhausted or
  // the socket stops accepting output. Returns false if the connection
  // should be closed.
  bool processAndFlush();
  // Parses and executes buffered commands until the input runs out or the
  // output reaches the high-water mark. Returns false on a protocol error.
  bool processInput();
  // Moves decided held replies to the output buffer, in order.
  void releaseHeldReplies();
  // Sends as much pending output as the socket accepts. Returns false on error.
  bool flush();
  size_t pendingOutput() const { return out_.size() - out_sent_ + held_bytes_; }

  int fd_;
  const CommandHandlerFn& handler_;
  const ReplyGate* gate_;
  Session session_;
  resp::Parser parser_;
  std::string in_;          // received bytes not yet consumed by the parser
  std::string out_;         // replies ready to send, not yet fully sent
  size_t out_sent_ = 0;     // bytes at the front of out_ already sent
  std::deque<HeldReply> held_;
  size_t held_bytes_ = 0;
};

}  // namespace kv::net

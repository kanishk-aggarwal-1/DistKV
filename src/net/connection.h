#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "protocol/resp_parser.h"

namespace kv::net {

// Per-connection protocol state that outlives a single command.
struct Session {
  // Set by ASKING; applies to the next command only (Redis Cluster semantics).
  bool asking = false;
};

// Executes one command and appends its RESP reply to `out`.
using CommandHandlerFn =
    std::function<void(const resp::Command& cmd, Session& session, std::string& out)>;

// One client connection. Owned by, and only ever touched from, a single
// event-loop thread, so it needs no locking.
//
// Pipelining: one read may contain many commands. They are all parsed and
// executed in order and their replies are appended to one output buffer,
// which is flushed with as few send() calls as possible.
//
// Backpressure: if a client sends faster than it reads replies, the pending
// output grows. Above kOutputHighWater we stop parsing and stop asking epoll
// for readability, so the kernel's receive buffer fills up and TCP flow
// control slows the client down.
class Connection {
 public:
  static constexpr size_t kOutputHighWater = 1024 * 1024;

  Connection(int fd, const CommandHandlerFn& handler);
  ~Connection();  // closes the socket

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  // Each returns false when the connection should be closed.
  bool onReadable();
  bool onWritable();

  // The epoll events this connection currently wants.
  uint32_t wantedEvents() const;

  int fd() const { return fd_; }

  // The events currently registered with epoll; maintained by the event loop
  // to skip redundant epoll_ctl calls.
  uint32_t registered_events = 0;

 private:
  // Alternates processInput() and flush() until the input is exhausted or
  // the socket stops accepting output. Returns false if the connection
  // should be closed.
  bool processAndFlush();
  // Parses and executes buffered commands until the input runs out or the
  // output reaches the high-water mark. Returns false on a protocol error.
  bool processInput();
  // Sends as much pending output as the socket accepts. Returns false on error.
  bool flush();
  size_t pendingOutput() const { return out_.size() - out_sent_; }

  int fd_;
  const CommandHandlerFn& handler_;
  Session session_;
  resp::Parser parser_;
  std::string in_;         // received bytes not yet consumed by the parser
  std::string out_;        // replies not yet fully sent
  size_t out_sent_ = 0;    // bytes at the front of out_ already sent
};

}  // namespace kv::net

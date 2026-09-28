#pragma once

#include <memory>
#include <unordered_map>

#include "net/connection.h"

namespace kv::net {

// A single-threaded epoll loop. It owns one listening socket (bound with
// SO_REUSEPORT alongside the other loops' sockets) plus every connection
// accepted on it. Nothing here is shared between threads except stop() and
// notifyReplicationProgress().
//
// epoll is used in level-triggered mode: a ready socket keeps being
// reported until it is drained, so a handler never has to read to EAGAIN
// and a missed wake-up cannot strand data in the kernel buffer.
class EventLoop {
 public:
  // Takes ownership of `listen_fd`. `handler` and `gate` must outlive the
  // loop; `gate` may be null (nothing is replicated).
  EventLoop(int listen_fd, const CommandHandlerFn& handler, const ReplyGate* gate);
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // Runs until stop() is called.
  void run();

  // Thread-safe; wakes the loop through an eventfd.
  void stop();

  // Thread-safe mailbox: the backup acknowledged (or replication failed) for
  // some writes; the loop wakes and releases the replies that were held.
  void notifyReplicationProgress();

 private:
  void acceptConnections();
  void handleConnectionEvent(int fd, uint32_t events);
  void onReplicationProgress();
  void updateInterest(Connection& conn);
  void closeConnection(int fd);

  int epoll_fd_;
  int listen_fd_;
  int wake_fd_;
  int progress_fd_;
  const CommandHandlerFn& handler_;
  const ReplyGate* gate_;
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

}  // namespace kv::net

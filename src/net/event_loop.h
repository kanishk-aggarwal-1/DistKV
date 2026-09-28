#pragma once

#include <memory>
#include <unordered_map>

#include "net/connection.h"

namespace kv::net {

// A single-threaded epoll loop. It owns one listening socket (bound with
// SO_REUSEPORT alongside the other loops' sockets) plus every connection
// accepted on it. Nothing here is shared between threads except stop().
//
// epoll is used in level-triggered mode: a ready socket keeps being
// reported until it is drained, so a handler never has to read to EAGAIN
// and a missed wake-up cannot strand data in the kernel buffer.
class EventLoop {
 public:
  // Takes ownership of `listen_fd`. `handler` must outlive the loop.
  EventLoop(int listen_fd, const CommandHandlerFn& handler);
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // Runs until stop() is called.
  void run();

  // Thread-safe; wakes the loop through an eventfd.
  void stop();

 private:
  void acceptConnections();
  void handleConnectionEvent(int fd, uint32_t events);
  void updateInterest(Connection& conn);
  void closeConnection(int fd);

  int epoll_fd_;
  int listen_fd_;
  int wake_fd_;
  const CommandHandlerFn& handler_;
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

}  // namespace kv::net

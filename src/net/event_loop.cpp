#include "net/event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <system_error>
#include <vector>

#include "net/socket_utils.h"

namespace kv::net {

namespace {

void addToEpoll(int epoll_fd, int fd, uint32_t events) {
  epoll_event ev{};
  ev.events = events;
  ev.data.fd = fd;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl(ADD)");
  }
}

}  // namespace

EventLoop::EventLoop(int listen_fd, const CommandHandlerFn& handler, const ReplyGate* gate)
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      listen_fd_(listen_fd),
      wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      progress_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      handler_(handler),
      gate_(gate) {
  if (epoll_fd_ < 0 || wake_fd_ < 0 || progress_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_create1/eventfd");
  }
  addToEpoll(epoll_fd_, listen_fd_, EPOLLIN);
  addToEpoll(epoll_fd_, wake_fd_, EPOLLIN);
  addToEpoll(epoll_fd_, progress_fd_, EPOLLIN);
}

EventLoop::~EventLoop() {
  connections_.clear();  // closes client sockets
  ::close(listen_fd_);
  ::close(wake_fd_);
  ::close(progress_fd_);
  ::close(epoll_fd_);
}

void EventLoop::stop() {
  uint64_t one = 1;
  // Can only fail if the counter would overflow, which cannot happen here.
  [[maybe_unused]] ssize_t n = ::write(wake_fd_, &one, sizeof(one));
}

void EventLoop::notifyReplicationProgress() {
  uint64_t one = 1;
  // eventfd counters add up, so many notifications before the loop wakes
  // collapse into one wake-up.
  [[maybe_unused]] ssize_t n = ::write(progress_fd_, &one, sizeof(one));
}

void EventLoop::onReplicationProgress() {
  uint64_t count = 0;
  [[maybe_unused]] ssize_t n = ::read(progress_fd_, &count, sizeof(count));
  // Collect first: closing a connection while iterating would invalidate the map.
  std::vector<int> to_close;
  for (auto& [fd, conn] : connections_) {
    if (conn->onReplicationProgress()) {
      updateInterest(*conn);
    } else {
      to_close.push_back(fd);
    }
  }
  for (int fd : to_close) closeConnection(fd);
}

void EventLoop::run() {
  std::array<epoll_event, 256> events;
  while (true) {
    int n = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      throw std::system_error(errno, std::generic_category(), "epoll_wait");
    }
    for (int i = 0; i < n; ++i) {
      int fd = events[i].data.fd;
      if (fd == wake_fd_) return;
      if (fd == progress_fd_) {
        onReplicationProgress();
        continue;
      }
      if (fd == listen_fd_) {
        acceptConnections();
      } else {
        handleConnectionEvent(fd, events[i].events);
      }
    }
  }
}

void EventLoop::acceptConnections() {
  while (true) {
    int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) continue;
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        std::fprintf(stderr, "accept4: %s\n", std::strerror(errno));
      }
      return;
    }
    setNoDelay(fd);
    auto conn = std::make_unique<Connection>(fd, handler_, gate_);
    conn->registered_events = conn->wantedEvents();
    addToEpoll(epoll_fd_, fd, conn->registered_events);
    connections_.emplace(fd, std::move(conn));
  }
}

void EventLoop::handleConnectionEvent(int fd, uint32_t events) {
  auto it = connections_.find(fd);
  if (it == connections_.end()) return;
  Connection& conn = *it->second;

  bool keep = true;
  if (events & (EPOLLERR | EPOLLHUP)) {
    keep = false;
  } else {
    if (events & EPOLLIN) keep = conn.onReadable();
    if (keep && (events & EPOLLOUT)) keep = conn.onWritable();
  }

  if (keep) {
    updateInterest(conn);
  } else {
    closeConnection(fd);
  }
}

void EventLoop::updateInterest(Connection& conn) {
  uint32_t wanted = conn.wantedEvents();
  if (wanted == conn.registered_events) return;
  epoll_event ev{};
  ev.events = wanted;
  ev.data.fd = conn.fd();
  if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd(), &ev) < 0) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl(MOD)");
  }
  conn.registered_events = wanted;
}

void EventLoop::closeConnection(int fd) {
  // Closing the fd removes it from the epoll set automatically.
  connections_.erase(fd);
}

}  // namespace kv::net

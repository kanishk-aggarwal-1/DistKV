#include "server/server.h"

#include <cstdio>
#include <exception>

#include "net/socket_utils.h"

namespace kv {

Server::Server(ServerConfig config)
    : config_(config),
      store_(config.stripes),
      handler_(store_),
      handler_fn_([this](const resp::Command& cmd, std::string& out) {
        handler_.execute(cmd, out);
      }) {
  if (config_.threads == 0) {
    config_.threads = std::thread::hardware_concurrency();
    if (config_.threads == 0) config_.threads = 1;
  }
}

Server::~Server() { stop(); }

void Server::start() {
  // Bind every socket before starting any thread so a bind failure is
  // reported to the caller. With port 0 the first socket picks a free port
  // and the rest join it.
  port_ = config_.port;
  for (unsigned i = 0; i < config_.threads; ++i) {
    int fd = net::createListenSocket(port_);
    if (i == 0) port_ = net::localPort(fd);
    loops_.push_back(std::make_unique<net::EventLoop>(fd, handler_fn_));
  }
  for (auto& loop : loops_) {
    threads_.emplace_back([loop = loop.get()] {
      try {
        loop->run();
      } catch (const std::exception& e) {
        std::fprintf(stderr, "event loop failed: %s\n", e.what());
        std::terminate();
      }
    });
  }
}

void Server::stop() {
  for (auto& loop : loops_) loop->stop();
  for (auto& thread : threads_) thread.join();
  threads_.clear();
  loops_.clear();
}

}  // namespace kv

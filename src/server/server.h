#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "net/connection.h"
#include "net/event_loop.h"
#include "server/command_handler.h"
#include "storage/store.h"

namespace kv {

struct ServerConfig {
  uint16_t port = 6380;
  unsigned threads = 0;   // event-loop threads; 0 means one per hardware thread
  size_t stripes = 256;   // lock stripes in the Store
};

// Threading model: N event-loop threads share one lock-striped Store.
// Each thread has its own epoll instance and its own SO_REUSEPORT listening
// socket, so the kernel balances new connections across threads and a
// connection is served by the same thread for its whole life.
class Server {
 public:
  explicit Server(ServerConfig config);
  ~Server();  // calls stop()

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds the listening sockets and starts the event-loop threads. When this
  // returns the server is accepting connections.
  void start();

  // Stops all event loops and joins their threads. Idempotent.
  void stop();

  // The port actually bound (useful when config.port is 0).
  uint16_t port() const { return port_; }

 private:
  ServerConfig config_;
  Store store_;
  CommandHandler handler_;
  net::CommandHandlerFn handler_fn_;
  std::vector<std::unique_ptr<net::EventLoop>> loops_;
  std::vector<std::thread> threads_;
  uint16_t port_ = 0;
};

}  // namespace kv

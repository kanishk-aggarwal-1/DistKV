#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cluster/cluster_state.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "server/command_handler.h"
#include "storage/store.h"

namespace grpc {
class Server;
}

namespace kv {

namespace cluster {
class NodeService;
}

struct ServerConfig {
  uint16_t port = 6380;
  unsigned threads = 0;   // event-loop threads; 0 means one per hardware thread
  size_t stripes = 256;   // lock stripes in the Store

  // Cluster mode. Without it the node serves every slot on its own.
  bool cluster = false;
  std::string node_id;                      // required in cluster mode
  std::string advertise_host = "127.0.0.1";  // how clients and peers reach this node
  uint16_t grpc_port = 0;                   // 0 picks a free port
};

// Threading model: N event-loop threads share one lock-striped Store.
// Each thread has its own epoll instance and its own SO_REUSEPORT listening
// socket, so the kernel balances new connections across threads and a
// connection is served by the same thread for its whole life.
//
// In cluster mode a gRPC server (with its own thread pool) additionally
// serves NodeService for the coordinator and peer nodes.
class Server {
 public:
  explicit Server(ServerConfig config);
  ~Server();  // calls stop()

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds the listening sockets and starts the event-loop threads (and the
  // gRPC server in cluster mode). When this returns the server is serving.
  void start();

  // Stops everything and joins the threads. Idempotent.
  void stop();

  // Ports actually bound (useful when the configured port is 0).
  uint16_t port() const { return port_; }
  uint16_t grpcPort() const { return grpc_port_; }

  std::string clientAddress() const;
  std::string grpcAddress() const;

  Store& store() { return store_; }
  const cluster::ClusterState* clusterState() const { return cluster_state_.get(); }

 private:
  ServerConfig config_;
  Store store_;
  std::unique_ptr<cluster::ClusterState> cluster_state_;  // null in standalone mode
  CommandHandler handler_;
  net::CommandHandlerFn handler_fn_;
  std::vector<std::unique_ptr<net::EventLoop>> loops_;
  std::vector<std::thread> threads_;
  std::unique_ptr<cluster::NodeService> node_service_;
  std::unique_ptr<grpc::Server> grpc_server_;
  uint16_t port_ = 0;
  uint16_t grpc_port_ = 0;
};

}  // namespace kv

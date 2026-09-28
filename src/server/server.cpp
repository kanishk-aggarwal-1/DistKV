#include "server/server.h"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <exception>
#include <stdexcept>

#include "cluster/node_service.h"
#include "cluster/rpc.h"
#include "net/socket_utils.h"

namespace kv {

namespace {

std::unique_ptr<cluster::ClusterState> makeClusterState(const ServerConfig& config) {
  if (!config.cluster) return nullptr;
  if (config.node_id.empty()) throw std::invalid_argument("cluster mode requires a node id");
  return std::make_unique<cluster::ClusterState>(config.node_id);
}

}  // namespace

Server::Server(ServerConfig config)
    : config_(std::move(config)),
      // A cluster node serves nothing until the coordinator assigns it slots.
      store_(config_.stripes, config_.cluster ? SlotState::kNotOwned : SlotState::kOwned),
      cluster_state_(makeClusterState(config_)),
      log_(progress_),
      handler_(store_, cluster_state_.get()),
      handler_fn_([this](const resp::Command& cmd, net::Session& session, std::string& out) {
        handler_.execute(cmd, session, out);
      }) {
  if (config_.threads == 0) {
    config_.threads = std::thread::hardware_concurrency();
    if (config_.threads == 0) config_.threads = 1;
  }
  if (cluster_state_) {
    // Every write is replicated; writes are refused until this node is a
    // primary with a synced backup (the log starts out not accepting).
    store_.setReplicationLog(&log_);
    gate_.status = [this](uint64_t seq) {
      switch (progress_.status(seq)) {
        case replication::Progress::Status::kAcked:
          return net::ReplyGate::Status::kReady;
        case replication::Progress::Status::kFailed:
          return net::ReplyGate::Status::kFailed;
        case replication::Progress::Status::kPending:
          break;
      }
      return net::ReplyGate::Status::kWaiting;
    };
    gate_.failed_reply = CommandHandler::kReplicationFailedReply;
  }
}

Server::~Server() { stop(); }

std::string Server::clientAddress() const {
  return config_.advertise_host + ":" + std::to_string(port_);
}

std::string Server::grpcAddress() const {
  return config_.advertise_host + ":" + std::to_string(grpc_port_);
}

void Server::start() {
  // Bind every socket before starting any thread so a bind failure is
  // reported to the caller. With port 0 the first socket picks a free port
  // and the rest join it.
  port_ = config_.port;
  const net::ReplyGate* gate = cluster_state_ ? &gate_ : nullptr;
  for (unsigned i = 0; i < config_.threads; ++i) {
    int fd = net::createListenSocket(port_);
    if (i == 0) port_ = net::localPort(fd);
    loops_.push_back(std::make_unique<net::EventLoop>(fd, handler_fn_, gate));
  }

  if (cluster_state_) {
    // Replication progress wakes every loop so held replies are released.
    for (auto& loop : loops_) {
      progress_.addListener([loop = loop.get()] { loop->notifyReplicationProgress(); });
    }

    int selected_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(config_.grpc_port),
                             grpc::InsecureServerCredentials(), &selected_port);
    builder.SetMaxReceiveMessageSize(cluster::kMaxMessageBytes);
    builder.SetMaxSendMessageSize(cluster::kMaxMessageBytes);
    // The node reports only its id and client address; the coordinator
    // already knows the gRPC address it used to reach the node.
    node_service_ = std::make_unique<cluster::NodeService>(
        cluster::NodeInfo{config_.node_id, clientAddress(), ""}, store_, *cluster_state_, log_,
        config_.replication_ack_timeout);
    builder.RegisterService(node_service_.get());
    grpc_server_ = builder.BuildAndStart();
    if (!grpc_server_ || selected_port == 0) {
      throw std::runtime_error("failed to start gRPC server on port " +
                               std::to_string(config_.grpc_port));
    }
    grpc_port_ = static_cast<uint16_t>(selected_port);
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

void Server::stopNodeService() {
  if (grpc_server_) {
    // Cancel in-flight calls (e.g. a peer's replication stream) instead of
    // waiting for them: a partition does not wait either.
    grpc_server_->Shutdown(std::chrono::system_clock::now());
    grpc_server_.reset();
  }
}

void Server::stop() {
  stopNodeService();
  // Stop replication before the loops go away: its progress notifications
  // point at them.
  if (node_service_) {
    node_service_->shutdown();
    node_service_.reset();
  }
  for (auto& loop : loops_) loop->stop();
  for (auto& thread : threads_) thread.join();
  threads_.clear();
  loops_.clear();
}

}  // namespace kv

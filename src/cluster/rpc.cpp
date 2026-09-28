#include "cluster/rpc.h"

#include <algorithm>

namespace kv::cluster {

std::shared_ptr<grpc::Channel> makeChannel(const std::string& addr) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(kMaxMessageBytes);
  args.SetMaxSendMessageSize(kMaxMessageBytes);
  // Nodes come and go (failures, restarts, spares). gRPC's default reconnect
  // backoff grows to 120 s, during which calls on the channel fail at once:
  // a node restarted at a previously failed address could not be re-added
  // for up to two minutes. Cap it at 1 s.
  args.SetInt("grpc.initial_reconnect_backoff_ms", 100);
  args.SetInt("grpc.min_reconnect_backoff_ms", 100);
  args.SetInt("grpc.max_reconnect_backoff_ms", 1000);
  // Traffic stays inside the cluster's private network (see DESIGN.md).
  return grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), args);
}

std::unique_ptr<distkv::v1::NodeService::Stub> makeNodeStub(const std::string& addr) {
  return distkv::v1::NodeService::NewStub(makeChannel(addr));
}

void setDeadline(grpc::ClientContext& ctx, std::chrono::milliseconds timeout) {
  ctx.set_deadline(std::chrono::system_clock::now() + timeout);
}

bool isRetryable(const grpc::Status& status) {
  switch (status.error_code()) {
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return true;
    default:
      return false;
  }
}

std::chrono::milliseconds retryBackoff(int attempt) {
  return std::chrono::milliseconds(std::min(50 << attempt, 2000));
}

}  // namespace kv::cluster

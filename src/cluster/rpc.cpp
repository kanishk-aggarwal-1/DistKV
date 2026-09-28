#include "cluster/rpc.h"

#include <algorithm>

namespace kv::cluster {

std::shared_ptr<grpc::Channel> makeChannel(const std::string& addr) {
  grpc::ChannelArguments args;
  args.SetMaxReceiveMessageSize(kMaxMessageBytes);
  args.SetMaxSendMessageSize(kMaxMessageBytes);
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

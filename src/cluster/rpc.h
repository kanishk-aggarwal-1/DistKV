#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <memory>
#include <string>

#include "distkv.grpc.pb.h"

// Small helpers shared by every gRPC client in the project.
namespace kv::cluster {

// Control-plane calls (config pushes, slot state changes) are tiny.
inline constexpr std::chrono::milliseconds kControlRpcTimeout{2000};
// Data calls carry up to ~1 MB of keys.
inline constexpr std::chrono::milliseconds kDataRpcTimeout{10000};
// Moving all keys of a batch of slots can take a while under load.
inline constexpr std::chrono::milliseconds kMigrateRpcTimeout{600000};
inline constexpr int kRpcAttempts = 5;

// Values can be up to 16 MB; allow messages comfortably above that.
inline constexpr int kMaxMessageBytes = 64 * 1024 * 1024;

std::shared_ptr<grpc::Channel> makeChannel(const std::string& addr);
std::unique_ptr<distkv::v1::NodeService::Stub> makeNodeStub(const std::string& addr);

void setDeadline(grpc::ClientContext& ctx, std::chrono::milliseconds timeout);

// Transient failures worth retrying (peer restarting, overloaded, timed out).
bool isRetryable(const grpc::Status& status);
std::chrono::milliseconds retryBackoff(int attempt);

}  // namespace kv::cluster

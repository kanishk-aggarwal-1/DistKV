#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "distkv.pb.h"

namespace kv::cluster {

struct NodeInfo {
  std::string id;
  std::string client_addr;  // host:port
  std::string grpc_addr;    // host:port
};

// In-memory form of distkv.v1.ClusterMap.
struct ClusterMap {
  uint64_t epoch = 0;
  std::vector<NodeInfo> nodes;
  std::vector<uint32_t> slot_owner;  // kNumSlots entries (index into nodes), or empty

  const NodeInfo* owner(uint16_t slot) const;
  const NodeInfo* findNode(std::string_view id) const;
  std::optional<uint32_t> indexOf(std::string_view id) const;

  distkv::v1::ClusterMap toProto() const;
  static ClusterMap fromProto(const distkv::v1::ClusterMap& proto);
};

NodeInfo nodeFromProto(const distkv::v1::Node& proto);
distkv::v1::Node nodeToProto(const NodeInfo& node);

// Splits "host:port". Returns false if there is no valid port.
bool splitHostPort(std::string_view addr, std::string& host, uint16_t& port);

}  // namespace kv::cluster

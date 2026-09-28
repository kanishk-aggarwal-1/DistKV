#include "cluster/cluster_map.h"

#include <charconv>

namespace kv::cluster {

const NodeInfo* ClusterMap::owner(uint16_t slot) const {
  if (slot >= slot_owner.size()) return nullptr;
  uint32_t index = slot_owner[slot];
  return index < nodes.size() ? &nodes[index] : nullptr;
}

const NodeInfo* ClusterMap::findNode(std::string_view id) const {
  for (const NodeInfo& node : nodes) {
    if (node.id == id) return &node;
  }
  return nullptr;
}

std::optional<uint32_t> ClusterMap::indexOf(std::string_view id) const {
  for (uint32_t i = 0; i < nodes.size(); ++i) {
    if (nodes[i].id == id) return i;
  }
  return std::nullopt;
}

distkv::v1::ClusterMap ClusterMap::toProto() const {
  distkv::v1::ClusterMap proto;
  proto.set_epoch(epoch);
  for (const NodeInfo& node : nodes) *proto.add_nodes() = nodeToProto(node);
  proto.mutable_slot_owner()->Add(slot_owner.begin(), slot_owner.end());
  return proto;
}

ClusterMap ClusterMap::fromProto(const distkv::v1::ClusterMap& proto) {
  ClusterMap map;
  map.epoch = proto.epoch();
  for (const auto& node : proto.nodes()) map.nodes.push_back(nodeFromProto(node));
  map.slot_owner.assign(proto.slot_owner().begin(), proto.slot_owner().end());
  return map;
}

NodeInfo nodeFromProto(const distkv::v1::Node& proto) {
  return NodeInfo{proto.id(), proto.client_addr(), proto.grpc_addr()};
}

distkv::v1::Node nodeToProto(const NodeInfo& node) {
  distkv::v1::Node proto;
  proto.set_id(node.id);
  proto.set_client_addr(node.client_addr);
  proto.set_grpc_addr(node.grpc_addr);
  return proto;
}

bool splitHostPort(std::string_view addr, std::string& host, uint16_t& port) {
  size_t colon = addr.rfind(':');
  if (colon == std::string_view::npos || colon == 0) return false;
  std::string_view port_text = addr.substr(colon + 1);
  auto [end, ec] = std::from_chars(port_text.data(), port_text.data() + port_text.size(), port);
  if (ec != std::errc() || end != port_text.data() + port_text.size()) return false;
  host = std::string(addr.substr(0, colon));
  return true;
}

}  // namespace kv::cluster

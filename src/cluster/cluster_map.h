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

// A replication group: the primary serves clients, the backup keeps a
// synchronous copy. Slots are assigned to groups.
struct GroupInfo {
  std::string id;
  std::string primary;       // node id
  std::string backup;        // node id; empty if none
  bool backup_ready = false; // the backup is fully synced

  bool hasReadyBackup() const { return !backup.empty() && backup_ready; }
};

// In-memory form of distkv.v1.ClusterMap.
struct ClusterMap {
  uint64_t epoch = 0;
  std::vector<NodeInfo> nodes;
  std::vector<GroupInfo> groups;
  std::vector<uint32_t> slot_owner;  // kNumSlots entries (index into groups), or empty
  std::vector<std::string> spares;

  const GroupInfo* groupOf(uint16_t slot) const;
  // The primary of the group owning `slot`.
  const NodeInfo* owner(uint16_t slot) const;
  const NodeInfo* findNode(std::string_view id) const;
  const GroupInfo* findGroup(std::string_view id) const;
  GroupInfo* findGroup(std::string_view id);
  std::optional<uint32_t> groupIndex(std::string_view id) const;
  // The group a node belongs to (as primary or backup), if any.
  const GroupInfo* groupOfNode(std::string_view node_id) const;

  distkv::v1::ClusterMap toProto() const;
  static ClusterMap fromProto(const distkv::v1::ClusterMap& proto);
};

NodeInfo nodeFromProto(const distkv::v1::Node& proto);
distkv::v1::Node nodeToProto(const NodeInfo& node);

// Splits "host:port". Returns false if there is no valid port.
bool splitHostPort(std::string_view addr, std::string& host, uint16_t& port);

}  // namespace kv::cluster

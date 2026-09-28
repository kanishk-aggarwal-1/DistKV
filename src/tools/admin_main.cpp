// distkv-admin: command-line client for the coordinator.
//
//   distkv-admin [--coordinator HOST:PORT] add-group PRIMARY_GRPC_ADDR BACKUP_GRPC_ADDR
//   distkv-admin [--coordinator HOST:PORT] remove-group GROUP_ID
//   distkv-admin [--coordinator HOST:PORT] add-spare NODE_GRPC_ADDR
//   distkv-admin [--coordinator HOST:PORT] rebalance
//   distkv-admin [--coordinator HOST:PORT] show

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "cluster/cluster_map.h"
#include "cluster/rpc.h"
#include "distkv.grpc.pb.h"

namespace {

void usage(const char* prog) {
  std::fprintf(stderr,
               "usage: %s [--coordinator HOST:PORT] COMMAND\n"
               "commands:\n"
               "  add-group PRIMARY BACKUP  add a replication group (nodes' gRPC addresses),\n"
               "                            sync its backup and rebalance\n"
               "  remove-group ID           move all slots off group ID and remove it\n"
               "  add-spare GRPC_ADDR       add a spare node to replace failed backups\n"
               "  rebalance                 move slots until they match the hash ring\n"
               "  show                      print the cluster map\n"
               "default coordinator: 127.0.0.1:9000\n",
               prog);
}

int fail(const grpc::Status& status) {
  std::fprintf(stderr, "error: %s\n", status.error_message().c_str());
  return 1;
}

void printChange(const distkv::v1::MembershipChange& change) {
  std::printf("epoch %llu: moved %u slots, %llu keys\n",
              static_cast<unsigned long long>(change.epoch()), change.slots_moved(),
              static_cast<unsigned long long>(change.keys_moved()));
}

void printMap(const kv::cluster::ClusterMap& map) {
  std::map<uint32_t, size_t> slots_per_group;
  for (uint32_t owner : map.slot_owner) ++slots_per_group[owner];
  std::printf("epoch %llu, %zu group(s), %zu node(s)\n",
              static_cast<unsigned long long>(map.epoch), map.groups.size(), map.nodes.size());
  std::printf("%-6s %-6s %-10s %-6s %s\n", "GROUP", "SLOTS", "PRIMARY", "BACKUP", "STATE");
  for (uint32_t i = 0; i < map.groups.size(); ++i) {
    const auto& g = map.groups[i];
    const char* backup_state = g.backup.empty() ? "(none)" : (g.backup_ready ? "ready" : "syncing");
    std::printf("%-6s %-6zu %-10s %-6s %s\n", g.id.c_str(), slots_per_group[i], g.primary.c_str(),
                g.backup.c_str(), backup_state);
  }
  std::printf("%-6s %-22s %s\n", "NODE", "CLIENT", "GRPC");
  for (const auto& node : map.nodes) {
    std::printf("%-6s %-22s %s\n", node.id.c_str(), node.client_addr.c_str(),
                node.grpc_addr.c_str());
  }
  if (!map.spares.empty()) {
    std::printf("spares:");
    for (const auto& id : map.spares) std::printf(" %s", id.c_str());
    std::printf("\n");
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::string coordinator = "127.0.0.1:9000";
  std::vector<std::string> args(argv + 1, argv + argc);
  if (args.size() >= 2 && args[0] == "--coordinator") {
    coordinator = args[1];
    args.erase(args.begin(), args.begin() + 2);
  }
  if (args.empty()) {
    usage(argv[0]);
    return 2;
  }

  auto stub = distkv::v1::CoordinatorService::NewStub(kv::cluster::makeChannel(coordinator));
  grpc::ClientContext ctx;
  // Membership changes wait for syncing and rebalancing to finish; reads do not.
  kv::cluster::setDeadline(ctx, args[0] == "show" ? std::chrono::milliseconds(10000)
                                                  : std::chrono::milliseconds(3600000));
  distkv::v1::MembershipChange change;
  grpc::Status status;

  if (args[0] == "add-group" && args.size() == 3) {
    distkv::v1::AddGroupRequest request;
    request.set_primary_grpc_addr(args[1]);
    request.set_backup_grpc_addr(args[2]);
    status = stub->AddGroup(&ctx, request, &change);
  } else if (args[0] == "remove-group" && args.size() == 2) {
    distkv::v1::RemoveGroupRequest request;
    request.set_id(args[1]);
    status = stub->RemoveGroup(&ctx, request, &change);
  } else if (args[0] == "add-spare" && args.size() == 2) {
    distkv::v1::AddSpareRequest request;
    request.set_grpc_addr(args[1]);
    status = stub->AddSpare(&ctx, request, &change);
  } else if (args[0] == "rebalance" && args.size() == 1) {
    status = stub->Rebalance(&ctx, distkv::v1::Empty(), &change);
  } else if (args[0] == "show" && args.size() == 1) {
    distkv::v1::ClusterMap map;
    status = stub->GetClusterMap(&ctx, distkv::v1::Empty(), &map);
    if (!status.ok()) return fail(status);
    printMap(kv::cluster::ClusterMap::fromProto(map));
    return 0;
  } else {
    usage(argv[0]);
    return 2;
  }
  if (!status.ok()) return fail(status);
  printChange(change);
  return 0;
}

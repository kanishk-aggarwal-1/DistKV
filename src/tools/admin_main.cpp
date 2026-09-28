// distkv-admin: command-line client for the coordinator.
//
//   distkv-admin [--coordinator HOST:PORT] add-node NODE_GRPC_ADDR
//   distkv-admin [--coordinator HOST:PORT] remove-node NODE_ID
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
               "  add-node GRPC_ADDR   add the node serving gRPC at GRPC_ADDR and rebalance\n"
               "  remove-node ID       move all slots off node ID and remove it\n"
               "  show                 print the cluster map\n"
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
  std::map<uint32_t, size_t> slots_per_node;
  for (uint32_t owner : map.slot_owner) ++slots_per_node[owner];
  std::printf("epoch %llu, %zu node(s)\n", static_cast<unsigned long long>(map.epoch),
              map.nodes.size());
  std::printf("%-12s %-22s %-22s %s\n", "ID", "CLIENT", "GRPC", "SLOTS");
  for (uint32_t i = 0; i < map.nodes.size(); ++i) {
    const auto& node = map.nodes[i];
    std::printf("%-12s %-22s %-22s %zu\n", node.id.c_str(), node.client_addr.c_str(),
                node.grpc_addr.c_str(), slots_per_node[i]);
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
  // Membership changes wait for the whole rebalance to finish; reads do not.
  kv::cluster::setDeadline(ctx, args[0] == "show" ? std::chrono::milliseconds(10000)
                                                  : std::chrono::milliseconds(3600000));

  if (args[0] == "add-node" && args.size() == 2) {
    distkv::v1::AddNodeRequest request;
    request.set_grpc_addr(args[1]);
    distkv::v1::MembershipChange change;
    grpc::Status status = stub->AddNode(&ctx, request, &change);
    if (!status.ok()) return fail(status);
    printChange(change);
  } else if (args[0] == "remove-node" && args.size() == 2) {
    distkv::v1::RemoveNodeRequest request;
    request.set_id(args[1]);
    distkv::v1::MembershipChange change;
    grpc::Status status = stub->RemoveNode(&ctx, request, &change);
    if (!status.ok()) return fail(status);
    printChange(change);
  } else if (args[0] == "show" && args.size() == 1) {
    distkv::v1::ClusterMap map;
    grpc::Status status = stub->GetClusterMap(&ctx, distkv::v1::Empty(), &map);
    if (!status.ok()) return fail(status);
    printMap(kv::cluster::ClusterMap::fromProto(map));
  } else {
    usage(argv[0]);
    return 2;
  }
  return 0;
}

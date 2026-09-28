// End-to-end tests of sharding and live migration: real nodes (RESP + gRPC)
// and a coordinator, all in one process, driven by cluster-aware clients.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "cluster/slot.h"
#include "coordinator/coordinator.h"
#include "server/server.h"
#include "support/resp_client.h"

namespace kv {
namespace {

using testing::ClusterClient;
using testing::Reply;

class LocalCluster {
 public:
  explicit LocalCluster(coordinator::CoordinatorConfig config) : coordinator_(config) {}

  // Starts a node and adds it to the cluster (blocks until rebalanced).
  Server& addNode(const std::string& id) {
    ServerConfig config;
    config.port = 0;
    config.grpc_port = 0;
    config.threads = 2;
    config.stripes = 64;
    config.cluster = true;
    config.node_id = id;
    auto node = std::make_unique<Server>(config);
    node->start();
    Server& ref = *node;
    nodes_[id] = std::move(node);

    distkv::v1::MembershipChange change;
    grpc::Status status = coordinator_.addNode(ref.grpcAddress(), &change);
    EXPECT_TRUE(status.ok()) << status.error_message();
    std::printf("added %s: epoch %llu, %u slots and %llu keys moved\n", id.c_str(),
                static_cast<unsigned long long>(change.epoch()), change.slots_moved(),
                static_cast<unsigned long long>(change.keys_moved()));
    return ref;
  }

  // Removes a node from the cluster; the process keeps running (as a real
  // node would until an operator stops it) so stale clients can still reach it.
  void removeNode(const std::string& id) {
    distkv::v1::MembershipChange change;
    grpc::Status status = coordinator_.removeNode(id, &change);
    EXPECT_TRUE(status.ok()) << status.error_message();
    std::printf("removed %s: epoch %llu, %u slots and %llu keys moved\n", id.c_str(),
                static_cast<unsigned long long>(change.epoch()), change.slots_moved(),
                static_cast<unsigned long long>(change.keys_moved()));
  }

  std::string seed() const { return nodes_.begin()->second->clientAddress(); }
  std::map<std::string, std::unique_ptr<Server>>& nodes() { return nodes_; }
  coordinator::Coordinator& coordinator() { return coordinator_; }

 private:
  coordinator::Coordinator coordinator_;
  std::map<std::string, std::unique_ptr<Server>> nodes_;
};

TEST(Cluster, KeysAreStoredOnlyOnTheirOwner) {
  LocalCluster cluster({});
  cluster.addNode("n1");
  cluster.addNode("n2");
  cluster.addNode("n3");

  ClusterClient client(cluster.seed());
  for (int i = 0; i < 2000; ++i) {
    std::string key = "key:" + std::to_string(i);
    ASSERT_EQ(client.execute({"SET", key, key}).str, "OK");
  }

  cluster::ClusterMap map = cluster.coordinator().map();
  for (int i = 0; i < 2000; ++i) {
    std::string key = "key:" + std::to_string(i);
    EXPECT_EQ(client.execute({"GET", key}).str, key);
    const std::string& owner = map.owner(cluster::keySlot(key))->id;
    for (auto& [id, node] : cluster.nodes()) {
      EXPECT_EQ(node->store().contains(key), id == owner) << key << " on " << id;
    }
  }
}

TEST(Cluster, WrongNodeAnswersMovedToTheOwner) {
  LocalCluster cluster({});
  cluster.addNode("n1");
  cluster.addNode("n2");
  cluster::ClusterMap map = cluster.coordinator().map();

  const std::string key = "foo";
  const uint16_t slot = cluster::keySlot(key);
  const cluster::NodeInfo* owner = map.owner(slot);
  for (auto& [id, node] : cluster.nodes()) {
    if (id == owner->id) continue;
    testing::RespClient conn(node->clientAddress());
    Reply reply = conn.command({"GET", key});
    EXPECT_EQ(reply.str, "MOVED " + std::to_string(slot) + " " + owner->client_addr);
  }
}

// The Phase 2 correctness test: no acknowledged write is lost, and no
// deleted key comes back, while nodes join and leave under load.
TEST(Cluster, NoAcknowledgedWriteIsLostWhileNodesJoinAndLeave) {
  // Small migration steps: many map changes while the writers run.
  LocalCluster cluster({.vnodes_per_node = 128, .slots_per_step = 64});
  cluster.addNode("n1");
  cluster.addNode("n2");
  cluster.addNode("n3");

  constexpr int kWriters = 4;
  constexpr int kKeysPerWriter = 3000;
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> operations{0};

  struct WriterResult {
    // Every key this writer touched and the state its last acknowledged
    // operation left it in (nullopt = deleted).
    std::map<std::string, std::optional<std::string>> expected;
    uint64_t moved = 0, ask = 0, tryagain = 0;
    std::string failure;
  };
  std::vector<WriterResult> results(kWriters);

  const std::string seed = cluster.seed();
  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      WriterResult& result = results[w];
      try {
        ClusterClient client(seed);
        std::mt19937 rng(static_cast<unsigned>(w));
        uint64_t seq = 0;
        while (!stop.load()) {
          // Each writer owns its keys, so its expected state is exact.
          std::string key = "w" + std::to_string(w) + ":k" + std::to_string(rng() % kKeysPerWriter);
          unsigned op = rng() % 10;
          if (op < 6) {
            std::string value = std::to_string(++seq);
            Reply reply = client.execute({"SET", key, value});
            if (reply.str != "OK") throw std::runtime_error("SET " + key + ": " + reply.str);
            result.expected[key] = value;
          } else if (op < 8) {
            Reply reply = client.execute({"DEL", key});
            if (reply.type != ':') throw std::runtime_error("DEL " + key + ": " + reply.str);
            auto it = result.expected.find(key);
            bool existed = it != result.expected.end() && it->second.has_value();
            if (reply.integer != (existed ? 1 : 0)) {
              throw std::runtime_error("DEL " + key + " returned " + std::to_string(reply.integer));
            }
            result.expected[key] = std::nullopt;
          } else {
            // Reads during migration must see the latest acknowledged write.
            Reply reply = client.execute({"GET", key});
            auto it = result.expected.find(key);
            std::optional<std::string> want =
                it == result.expected.end() ? std::nullopt : it->second;
            std::optional<std::string> got =
                reply.is_null ? std::nullopt : std::optional<std::string>(reply.str);
            if (reply.isError() || got != want) {
              throw std::runtime_error("GET " + key + " returned " +
                                       (reply.isError() ? reply.str : got.value_or("(nil)")) +
                                       ", expected " + want.value_or("(nil)"));
            }
          }
          operations.fetch_add(1);
        }
        result.moved = client.moved;
        result.ask = client.ask;
        result.tryagain = client.tryagain;
      } catch (const std::exception& e) {
        result.failure = e.what();
      }
    });
  }

  auto waitForOps = [&](uint64_t count) {
    uint64_t target = operations.load() + count;
    while (operations.load() < target) std::this_thread::sleep_for(std::chrono::milliseconds(5));
  };

  waitForOps(5000);  // let the writers fill the cluster
  cluster.addNode("n4");
  waitForOps(2000);
  cluster.removeNode("n2");
  waitForOps(2000);
  stop = true;
  for (auto& t : writers) t.join();

  uint64_t moved = 0, ask = 0, tryagain = 0;
  size_t expected_present = 0;
  for (const WriterResult& result : results) {
    ASSERT_TRUE(result.failure.empty()) << result.failure;
    moved += result.moved;
    ask += result.ask;
    tryagain += result.tryagain;
    for (const auto& [key, value] : result.expected) expected_present += value.has_value();
  }
  std::printf("%llu operations; redirects seen: %llu MOVED, %llu ASK, %llu TRYAGAIN\n",
              static_cast<unsigned long long>(operations.load()),
              static_cast<unsigned long long>(moved), static_cast<unsigned long long>(ask),
              static_cast<unsigned long long>(tryagain));
  // The writers really ran through the topology changes.
  EXPECT_GT(moved, 0u);

  // 1. Every acknowledged write is readable; every deleted key stays deleted.
  ClusterClient reader(cluster.nodes().at("n1")->clientAddress());
  for (const WriterResult& result : results) {
    for (const auto& [key, value] : result.expected) {
      Reply reply = reader.execute({"GET", key});
      ASSERT_FALSE(reply.isError()) << key << ": " << reply.str;
      if (value) {
        EXPECT_EQ(reply.str, *value) << key;
      } else {
        EXPECT_TRUE(reply.is_null) << key << " was deleted but reads " << reply.str;
      }
    }
  }

  // 2. Each live key is stored exactly once, on the node the final map says
  //    owns it, and nothing else is stored anywhere (no strays, no duplicates).
  cluster::ClusterMap map = cluster.coordinator().map();
  EXPECT_EQ(map.findNode("n2"), nullptr);
  size_t stored = 0;
  for (auto& [id, node] : cluster.nodes()) stored += node->store().size();
  EXPECT_EQ(stored, expected_present);
  EXPECT_EQ(cluster.nodes().at("n2")->store().size(), 0u);
  for (const WriterResult& result : results) {
    for (const auto& [key, value] : result.expected) {
      if (!value) continue;
      const std::string& owner = map.owner(cluster::keySlot(key))->id;
      EXPECT_TRUE(cluster.nodes().at(owner)->store().contains(key)) << key << " missing on " << owner;
    }
  }

  // 3. No slot was left mid-migration.
  for (auto& [id, node] : cluster.nodes()) {
    for (uint16_t slot = 0; slot < cluster::kNumSlots; ++slot) {
      const cluster::NodeInfo* owner = map.owner(slot);
      SlotState want = owner->id == id ? SlotState::kOwned : SlotState::kNotOwned;
      ASSERT_EQ(node->store().slotState(slot), want) << id << " slot " << slot;
    }
  }
}

}  // namespace
}  // namespace kv

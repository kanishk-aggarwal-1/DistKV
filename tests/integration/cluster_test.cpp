// End-to-end tests of sharding, replication and failover: real nodes (RESP +
// gRPC) and a coordinator, all in one process, driven by cluster-aware
// clients.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cluster/hash_ring.h"
#include "cluster/slot.h"
#include "coordinator/coordinator.h"
#include "server/server.h"
#include "support/resp_client.h"

namespace kv {
namespace {

using namespace std::chrono_literals;
using testing::ClusterClient;
using testing::Reply;
using Clock = std::chrono::steady_clock;

bool waitUntil(const std::function<bool()>& condition, std::chrono::milliseconds timeout) {
  auto deadline = Clock::now() + timeout;
  while (Clock::now() < deadline) {
    if (condition()) return true;
    std::this_thread::sleep_for(10ms);
  }
  return condition();
}

class LocalCluster {
 public:
  explicit LocalCluster(coordinator::CoordinatorConfig config = {}) : coordinator_(config) {}

  Server& startNode(const std::string& id) {
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
    return ref;
  }

  // Starts two nodes and adds them as a group (blocks until synced and rebalanced).
  void addGroup(const std::string& primary, const std::string& backup) {
    Server& p = startNode(primary);
    Server& b = startNode(backup);
    distkv::v1::MembershipChange change;
    grpc::Status status = coordinator_.addGroup(p.grpcAddress(), b.grpcAddress(), &change);
    ASSERT_TRUE(status.ok()) << status.error_message();
    std::printf("added group (%s, %s): epoch %llu, %u slots and %llu keys moved\n", primary.c_str(),
                backup.c_str(), static_cast<unsigned long long>(change.epoch()),
                change.slots_moved(), static_cast<unsigned long long>(change.keys_moved()));
  }

  void addSpare(const std::string& id) {
    Server& s = startNode(id);
    distkv::v1::MembershipChange change;
    grpc::Status status = coordinator_.addSpare(s.grpcAddress(), &change);
    ASSERT_TRUE(status.ok()) << status.error_message();
  }

  // Removes a group; its processes keep running (as they would until an
  // operator stops them) so stale clients can still reach them.
  void removeGroupOf(const std::string& node_id) {
    const cluster::ClusterMap map = coordinator_.map();  // keep the copy alive
    const cluster::GroupInfo* group = map.groupOfNode(node_id);
    ASSERT_NE(group, nullptr);
    std::string id = group->id;
    distkv::v1::MembershipChange change;
    grpc::Status status = coordinator_.removeGroup(id, &change);
    ASSERT_TRUE(status.ok()) << status.error_message();
    std::printf("removed group %s: epoch %llu, %u slots and %llu keys moved\n", id.c_str(),
                static_cast<unsigned long long>(change.epoch()), change.slots_moved(),
                static_cast<unsigned long long>(change.keys_moved()));
  }

  std::vector<std::string> clientAddresses() const {
    std::vector<std::string> addrs;
    for (const auto& [id, node] : nodes_) addrs.push_back(node->clientAddress());
    return addrs;
  }

  Server& node(const std::string& id) { return *nodes_.at(id); }
  std::map<std::string, std::unique_ptr<Server>>& nodes() { return nodes_; }
  coordinator::Coordinator& coordinator() { return coordinator_; }

 private:
  // Declared first so they are destroyed last: the coordinator (and its
  // heartbeat monitors) stops before the nodes, so tearing the test down
  // does not look like a string of node failures.
  std::map<std::string, std::unique_ptr<Server>> nodes_;
  coordinator::Coordinator coordinator_;
};

// Writers that keep issuing SET / DEL / GET on keys each of them owns, and
// track every state a key may legitimately be in:
//   - an acknowledged write leaves exactly one possible state;
//   - an ambiguous outcome (connection lost, or "not acknowledged by the
//     backup") adds the attempted state to the possibilities;
//   - a refused write ("not enough good replicas") changes nothing.
// A GET must always return one of the possible states.
class Workload {
 public:
  struct Stats {
    uint64_t ops = 0, moved = 0, ask = 0, tryagain = 0, refused = 0, ambiguous = 0;
  };

  Workload(std::vector<std::string> seeds, int writers, int keys_per_writer)
      : seeds_(std::move(seeds)), results_(writers), keys_per_writer_(keys_per_writer) {
    for (int w = 0; w < writers; ++w) threads_.emplace_back([this, w] { run(w); });
  }

  ~Workload() { stop(); }

  void waitForOps(uint64_t count) {
    uint64_t target = ops_.load() + count;
    while (ops_.load() < target && failures() == 0) std::this_thread::sleep_for(5ms);
  }

  void stop() {
    stop_ = true;
    for (auto& t : threads_) {
      if (t.joinable()) t.join();
    }
  }

  int failures() const {
    int n = 0;
    for (const auto& r : results_) n += !r.failure.empty() ? 1 : 0;
    return n;
  }

  // After stop(): checks every key through a fresh client. Returns the
  // number of keys that must exist.
  size_t verify(const std::vector<std::string>& seeds) {
    Stats total;
    size_t must_exist = 0;
    ClusterClient reader(seeds);
    for (const WriterResult& result : results_) {
      EXPECT_TRUE(result.failure.empty()) << result.failure;
      total.ops += result.stats.ops;
      total.moved += result.stats.moved;
      total.ask += result.stats.ask;
      total.tryagain += result.stats.tryagain;
      total.refused += result.stats.refused;
      total.ambiguous += result.stats.ambiguous;
      for (const auto& [key, possible] : result.keys) {
        Reply reply = reader.execute({"GET", key});
        EXPECT_FALSE(reply.isError() || reply.type == ClusterClient::kAmbiguous)
            << key << ": " << reply.str;
        std::optional<std::string> got =
            reply.is_null ? std::nullopt : std::optional<std::string>(reply.str);
        EXPECT_TRUE(possible.count(got)) << key << " reads " << got.value_or("(nil)")
                                         << ", which no acknowledged write left it as";
        if (possible.size() == 1 && possible.begin()->has_value()) ++must_exist;
      }
    }
    std::printf(
        "%llu operations; %llu MOVED, %llu ASK, %llu TRYAGAIN; %llu writes refused (no synced "
        "backup), %llu ambiguous outcomes\n",
        static_cast<unsigned long long>(total.ops), static_cast<unsigned long long>(total.moved),
        static_cast<unsigned long long>(total.ask), static_cast<unsigned long long>(total.tryagain),
        static_cast<unsigned long long>(total.refused),
        static_cast<unsigned long long>(total.ambiguous));
    stats_ = total;
    return must_exist;
  }

  const Stats& stats() const { return stats_; }

  // Keys whose final state is certain to be "present".
  std::vector<std::pair<std::string, std::string>> certainValues() const {
    std::vector<std::pair<std::string, std::string>> out;
    for (const WriterResult& result : results_) {
      for (const auto& [key, possible] : result.keys) {
        if (possible.size() == 1 && possible.begin()->has_value()) {
          out.emplace_back(key, **possible.begin());
        }
      }
    }
    return out;
  }

 private:
  using Possible = std::set<std::optional<std::string>>;

  struct WriterResult {
    std::map<std::string, Possible> keys;
    Stats stats;
    std::string failure;
  };

  static bool refused(const Reply& reply) {
    return reply.isErrorWithPrefix("NOREPLICAS Not enough good replicas");
  }
  static bool ambiguous(const Reply& reply) {
    return reply.type == ClusterClient::kAmbiguous ||
           reply.isErrorWithPrefix("NOREPLICAS Write not acknowledged");
  }

  void run(int w) {
    WriterResult& result = results_[w];
    try {
      ClusterClient client(seeds_);
      std::mt19937 rng(static_cast<unsigned>(w));
      uint64_t seq = 0;
      while (!stop_.load()) {
        std::string key = "w" + std::to_string(w) + ":k" + std::to_string(rng() % keys_per_writer_);
        Possible& possible = result.keys.try_emplace(key, Possible{std::nullopt}).first->second;
        unsigned op = rng() % 10;
        if (op < 6) {
          std::string value = std::to_string(++seq);
          Reply reply = client.execute({"SET", key, value});
          if (reply.str == "OK" && reply.type == '+') {
            possible = {value};
          } else if (ambiguous(reply)) {
            possible.insert(value);
            ++result.stats.ambiguous;
          } else if (refused(reply)) {
            ++result.stats.refused;
          } else {
            throw std::runtime_error("SET " + key + ": " + reply.str);
          }
        } else if (op < 8) {
          Reply reply = client.execute({"DEL", key});
          if (reply.type == ':') {
            // The count is only checkable when the previous state is certain.
            if (possible.size() == 1) {
              int64_t want = possible.begin()->has_value() ? 1 : 0;
              if (reply.integer != want) {
                throw std::runtime_error("DEL " + key + " returned " +
                                         std::to_string(reply.integer));
              }
            }
            possible = {std::nullopt};
          } else if (ambiguous(reply)) {
            possible.insert(std::nullopt);
            ++result.stats.ambiguous;
          } else if (refused(reply)) {
            ++result.stats.refused;
          } else {
            throw std::runtime_error("DEL " + key + ": " + reply.str);
          }
        } else {
          // Reads must never show a state no acknowledged write produced,
          // including during migrations and failovers.
          Reply reply = client.execute({"GET", key});
          if (reply.type == ClusterClient::kAmbiguous) continue;
          if (reply.isError()) throw std::runtime_error("GET " + key + ": " + reply.str);
          std::optional<std::string> got =
              reply.is_null ? std::nullopt : std::optional<std::string>(reply.str);
          if (!possible.count(got)) {
            throw std::runtime_error("GET " + key + " returned " + got.value_or("(nil)") +
                                     ", which no acknowledged write left it as");
          }
        }
        ++result.stats.ops;
        ops_.fetch_add(1);
      }
      result.stats.moved = client.moved;
      result.stats.ask = client.ask;
      result.stats.tryagain = client.tryagain;
    } catch (const std::exception& e) {
      result.failure = "writer " + std::to_string(w) + ": " + e.what();
    }
  }

  std::vector<std::string> seeds_;
  std::vector<WriterResult> results_;
  int keys_per_writer_;
  std::vector<std::thread> threads_;
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> ops_{0};
  Stats stats_;
};

// Every node's slot states match the map: primaries and backups own their
// group's slots, and nothing is left mid-migration.
void expectStableSlotStates(LocalCluster& cluster) {
  cluster::ClusterMap map = cluster.coordinator().map();
  for (const cluster::GroupInfo& group : map.groups) {
    uint32_t index = *map.groupIndex(group.id);
    for (const std::string& node_id : {group.primary, group.backup}) {
      if (node_id.empty()) continue;
      Store& store = cluster.node(node_id).store();
      for (uint16_t slot = 0; slot < cluster::kNumSlots; ++slot) {
        SlotState want = map.slot_owner[slot] == index ? SlotState::kOwned : SlotState::kNotOwned;
        ASSERT_EQ(store.slotState(slot), want) << node_id << " slot " << slot;
      }
    }
  }
}

// ---- Sharding ------------------------------------------------------------------

TEST(Cluster, KeysAreStoredOnTheirOwnerAndItsBackupOnly) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  cluster.addGroup("n3", "n4");
  cluster.addGroup("n5", "n6");

  ClusterClient client(cluster.clientAddresses());
  for (int i = 0; i < 2000; ++i) {
    std::string key = "key:" + std::to_string(i);
    ASSERT_EQ(client.execute({"SET", key, key}).str, "OK");
  }

  cluster::ClusterMap map = cluster.coordinator().map();
  for (int i = 0; i < 2000; ++i) {
    std::string key = "key:" + std::to_string(i);
    EXPECT_EQ(client.execute({"GET", key}).str, key);
    const cluster::GroupInfo* group = map.groupOf(cluster::keySlot(key));
    for (auto& [id, node] : cluster.nodes()) {
      bool expected = id == group->primary || id == group->backup;
      EXPECT_EQ(node->store().contains(key), expected) << key << " on " << id;
    }
  }
}

TEST(Cluster, PrimariesAndBackupsRedirectToTheOwner) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  cluster.addGroup("n3", "n4");
  cluster::ClusterMap map = cluster.coordinator().map();

  const std::string key = "foo";
  const uint16_t slot = cluster::keySlot(key);
  const cluster::NodeInfo* owner = map.owner(slot);
  for (auto& [id, node] : cluster.nodes()) {
    if (id == owner->id) continue;  // backups redirect too: they serve no clients
    testing::RespClient conn(node->clientAddress());
    Reply reply = conn.command({"GET", key});
    EXPECT_EQ(reply.str, "MOVED " + std::to_string(slot) + " " + owner->client_addr) << id;
  }
}

// No acknowledged write is lost, and no deleted key comes back, while groups
// join and leave under load (live slot migration, with every migration step
// replicated).
TEST(Cluster, NoAcknowledgedWriteIsLostWhileGroupsJoinAndLeave) {
  // Small migration steps: many map changes while the writers run.
  LocalCluster cluster({.vnodes_per_node = 128, .slots_per_step = 64});
  cluster.addGroup("n1", "n2");
  cluster.addGroup("n3", "n4");
  cluster.addGroup("n5", "n6");

  Workload workload(cluster.clientAddresses(), 4, 3000);
  workload.waitForOps(5000);  // let the writers fill the cluster
  cluster.addGroup("n7", "n8");
  workload.waitForOps(2000);
  cluster.removeGroupOf("n3");
  workload.waitForOps(2000);
  workload.stop();
  ASSERT_EQ(workload.failures(), 0);

  size_t must_exist = workload.verify(cluster.clientAddresses());
  EXPECT_GT(workload.stats().moved, 0u);
  EXPECT_EQ(workload.stats().ambiguous, 0u);  // no failures in this test

  // Each live key is stored on its owner's primary and backup, and nowhere
  // else: summed over primaries, stored keys equal the live keys.
  cluster::ClusterMap map = cluster.coordinator().map();
  EXPECT_EQ(map.groupOfNode("n3"), nullptr);
  size_t stored = 0;
  for (const cluster::GroupInfo& group : map.groups) {
    stored += cluster.node(group.primary).store().size();
    EXPECT_EQ(cluster.node(group.backup).store().size(), cluster.node(group.primary).store().size())
        << group.id;
  }
  EXPECT_EQ(stored, must_exist);
  EXPECT_EQ(cluster.node("n3").store().size(), 0u);
  EXPECT_EQ(cluster.node("n4").store().size(), 0u);
  expectStableSlotStates(cluster);
}

// ---- Replication -----------------------------------------------------------------

TEST(Replication, AcknowledgedWritesAreAlreadyOnTheBackup) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  ClusterClient client(cluster.clientAddresses());
  Store& backup = cluster.node("n2").store();
  for (int i = 0; i < 3000; ++i) {
    std::string key = "key:" + std::to_string(i);
    ASSERT_EQ(client.execute({"SET", key, "v" + std::to_string(i)}).str, "OK");
    // The reply was held until the backup acknowledged, so it is there now.
    ASSERT_EQ(backup.peek(key), "v" + std::to_string(i)) << key;
    if (i % 3 == 0) {
      ASSERT_EQ(client.execute({"DEL", key}).integer, 1);
      ASSERT_FALSE(backup.contains(key)) << key;
    }
  }
}

TEST(Replication, PipelinedWritesKeepTheirReplyOrder) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  testing::RespClient conn(cluster.node("n1").clientAddress());
  // Writes (held for the backup) interleaved with reads (not held).
  std::string batch;
  for (int i = 0; i < 500; ++i) {
    batch += testing::encodeCommand({"SET", "p" + std::to_string(i), std::to_string(i)});
    batch += testing::encodeCommand({"GET", "p" + std::to_string(i)});
    batch += testing::encodeCommand({"PING"});
  }
  conn.sendRaw(batch);
  for (int i = 0; i < 500; ++i) {
    ASSERT_EQ(conn.readReply().str, "OK") << i;
    ASSERT_EQ(conn.readReply().str, std::to_string(i)) << i;
    ASSERT_EQ(conn.readReply().str, "PONG") << i;
  }
}

TEST(Replication, WritesAreRefusedWhileTheBackupIsNotSynced) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  ClusterClient client(cluster.clientAddresses());
  ASSERT_EQ(client.execute({"SET", "before", "1"}).str, "OK");

  cluster.node("n2").stop();  // the backup dies; no spare to replace it
  ASSERT_TRUE(waitUntil([&] { return cluster.coordinator().map().groups[0].backup.empty(); }, 5s));
  ASSERT_TRUE(waitUntil([&] { return !cluster.node("n1").clusterState()->map()->groups[0].backup_ready; }, 5s));

  // Strict mode: no single-copy writes. Reads still work.
  Reply reply = client.execute({"SET", "after", "1"});
  EXPECT_TRUE(reply.isErrorWithPrefix("NOREPLICAS")) << reply.str;
  EXPECT_EQ(client.execute({"GET", "before"}).str, "1");

  // A spare becomes the new backup; after its full sync writes resume.
  cluster.addSpare("n3");
  ASSERT_TRUE(waitUntil([&] { return cluster.coordinator().map().groups[0].hasReadyBackup(); }, 10s));
  ASSERT_TRUE(waitUntil([&] { return client.execute({"SET", "after", "2"}).str == "OK"; }, 5s));
  EXPECT_EQ(cluster.node("n3").store().peek("before"), "1");  // synced by snapshot
  EXPECT_EQ(cluster.node("n3").store().peek("after"), "2");   // replicated
}

// ---- Failover ------------------------------------------------------------------------

TEST(Failover, PromotedBackupHasEveryAcknowledgedWrite) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  cluster.addGroup("n3", "n4");
  cluster.addSpare("n5");
  const std::vector<std::string> seeds = cluster.clientAddresses();

  Workload workload(seeds, 4, 2000);
  workload.waitForOps(5000);

  // A probe that keeps writing one key in a slot the victim owns, to time how
  // long writes to that group are unavailable.
  cluster::ClusterMap before = cluster.coordinator().map();
  std::string probe_key;
  for (int i = 0;; ++i) {
    probe_key = "probe:" + std::to_string(i);
    if (before.groupOf(cluster::keySlot(probe_key))->primary == "n1") break;
  }
  std::atomic<bool> killed{false};
  std::atomic<long long> unavailable_ms{-1};
  Clock::time_point kill_time;
  std::thread probe([&] {
    ClusterClient client(seeds);
    while (!killed) std::this_thread::sleep_for(1ms);
    while (unavailable_ms < 0) {
      Reply reply = client.execute({"SET", probe_key, "x"});
      if (reply.type == '+') {
        unavailable_ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - kill_time).count();
      } else {
        std::this_thread::sleep_for(1ms);
      }
    }
  });

  kill_time = Clock::now();
  cluster.node("n1").stop();  // the primary of group g1 crashes
  killed = true;

  // The backup is promoted and the spare becomes its new, synced backup.
  ASSERT_TRUE(waitUntil([&] {
    cluster::ClusterMap map = cluster.coordinator().map();
    const cluster::GroupInfo* g = map.findGroup(before.groupOfNode("n1")->id);
    return g->primary == "n2" && g->backup == "n5" && g->backup_ready;
  }, 20s));
  probe.join();
  std::printf("writes to the failed group resumed %lld ms after the primary stopped\n",
              unavailable_ms.load());

  workload.waitForOps(3000);
  workload.stop();
  ASSERT_EQ(workload.failures(), 0);
  size_t must_exist = workload.verify({cluster.node("n2").clientAddress()});
  EXPECT_GT(must_exist, 0u);

  // Every key whose last acknowledged state was "present" is on the new
  // primary and its new backup with that value.
  cluster::ClusterMap map = cluster.coordinator().map();
  for (const auto& [key, value] : workload.certainValues()) {
    const cluster::GroupInfo* group = map.groupOf(cluster::keySlot(key));
    EXPECT_EQ(cluster.node(group->primary).store().peek(key), value) << key;
    EXPECT_EQ(cluster.node(group->backup).store().peek(key), value) << key;
  }
  expectStableSlotStates(cluster);
}

// A source primary dies in the middle of a rebalance. Its promoted backup
// knows which slots were migrating (the state is replicated), the
// coordinator's reconciler finishes the interrupted rebalance under a newer
// migration id, and no acknowledged write is lost.
TEST(Failover, PrimaryDiesDuringMigration) {
  LocalCluster cluster({.vnodes_per_node = 128, .slots_per_step = 64});
  cluster.addGroup("n1", "n2");
  cluster.addGroup("n3", "n4");
  cluster.addSpare("n5");
  Server& p = cluster.startNode("n6");
  Server& b = cluster.startNode("n7");
  std::vector<std::string> seeds = cluster.clientAddresses();

  Workload workload(seeds, 4, 2000);
  workload.waitForOps(5000);

  // Add a third group; its rebalance migrates slots away from g1 and g2.
  const uint64_t epoch_before = cluster.coordinator().map().epoch;
  std::thread joiner([&] {
    distkv::v1::MembershipChange change;
    grpc::Status status = cluster.coordinator().addGroup(p.grpcAddress(), b.grpcAddress(), &change);
    std::printf("addGroup returned: %s\n", status.ok() ? "OK" : status.error_message().c_str());
  });
  // Kill g1's primary once a few migration steps have committed.
  ASSERT_TRUE(waitUntil([&] { return cluster.coordinator().map().epoch >= epoch_before + 6; }, 20s));
  cluster.node("n1").stop();
  joiner.join();

  // The coordinator promotes n2, gives it the spare, and finishes the
  // rebalance: every slot ends up where the hash ring says.
  ASSERT_TRUE(waitUntil([&] {
    cluster::ClusterMap map = cluster.coordinator().map();
    if (map.groups.size() != 3) return false;
    std::vector<std::string> ids;
    for (const auto& g : map.groups) {
      if (!g.hasReadyBackup()) return false;
      ids.push_back(g.id);
    }
    cluster::HashRing ring(ids, 128);
    for (uint16_t slot = 0; slot < cluster::kNumSlots; ++slot) {
      if (map.groups[map.slot_owner[slot]].id != ring.ownerOf(slot)) return false;
    }
    return true;
  }, 30s));
  cluster::ClusterMap map = cluster.coordinator().map();
  EXPECT_EQ(map.findGroup("g1")->primary, "n2");

  workload.waitForOps(3000);
  workload.stop();
  ASSERT_EQ(workload.failures(), 0);
  size_t must_exist = workload.verify({cluster.node("n2").clientAddress()});

  size_t stored = 0;
  for (const cluster::GroupInfo& group : map.groups) {
    stored += cluster.node(group.primary).store().size();
    EXPECT_EQ(cluster.node(group.backup).store().size(), cluster.node(group.primary).store().size())
        << group.id;
  }
  // Ambiguous outcomes may or may not have left a key behind, so the stored
  // count is at least the keys that must exist.
  EXPECT_GE(stored, must_exist);
  for (const auto& [key, value] : workload.certainValues()) {
    const cluster::GroupInfo* group = map.groupOf(cluster::keySlot(key));
    EXPECT_EQ(cluster.node(group->primary).store().peek(key), value) << key;
  }
  expectStableSlotStates(cluster);
}

// A primary cut off from the coordinator (but still reachable by clients)
// is replaced. From the moment its backup is promoted it must not
// acknowledge any write: the backup refuses its replication stream.
TEST(Failover, PartitionedPrimaryCannotAcknowledgeWritesAfterPromotion) {
  LocalCluster cluster;
  cluster.addGroup("n1", "n2");
  cluster.addSpare("n3");
  Server& old_primary = cluster.node("n1");

  // A client pinned to the old primary keeps writing throughout.
  std::atomic<bool> stop{false};
  std::vector<std::pair<std::string, std::string>> acked;
  std::mutex acked_mutex;
  std::thread writer([&] {
    testing::RespClient conn(old_primary.clientAddress());
    for (int i = 0; !stop; ++i) {
      std::string key = "fence:" + std::to_string(i);
      Reply reply = conn.command({"SET", key, "v"});
      if (reply.type == '+') {
        std::lock_guard lock(acked_mutex);
        acked.emplace_back(key, "v");
      }
      std::this_thread::sleep_for(1ms);
    }
  });

  std::this_thread::sleep_for(200ms);
  old_primary.stopNodeService();  // partition: the coordinator cannot reach n1

  // The coordinator promotes n2; wait until n2 itself knows.
  ASSERT_TRUE(waitUntil([&] {
    auto map = cluster.node("n2").clusterState()->map();
    return !map->groups.empty() && map->groups[0].primary == "n2";
  }, 10s));
  const auto promoted_at = Clock::now();

  // Writes sent to the old primary after the promotion are never acknowledged.
  // They must fail because the promoted backup refuses the old primary's
  // stream (epoch fencing), well before the 2 s ack timeout would kick in.
  testing::RespClient probe(old_primary.clientAddress());
  const auto probe_start = Clock::now();
  for (int i = 0; i < 20; ++i) {
    Reply reply = probe.command({"SET", "after-promotion:" + std::to_string(i), "x"});
    EXPECT_TRUE(reply.isErrorWithPrefix("NOREPLICAS")) << reply.str;
  }
  EXPECT_LT(Clock::now() - probe_start, 1000ms) << "fenced by the ack timeout, not by the backup";
  stop = true;
  writer.join();

  // Every write the old primary acknowledged (before the promotion) is on
  // the new primary.
  std::lock_guard lock(acked_mutex);
  ASSERT_FALSE(acked.empty());
  for (const auto& [key, value] : acked) {
    EXPECT_EQ(cluster.node("n2").store().peek(key), value) << key;
  }
  for (int i = 0; i < 20; ++i) {
    EXPECT_FALSE(cluster.node("n2").store().contains("after-promotion:" + std::to_string(i)));
  }
  std::printf("old primary acknowledged %zu writes before being fenced; checked %lld ms after "
              "promotion\n",
              acked.size(),
              static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                         Clock::now() - promoted_at)
                                         .count()));
}

}  // namespace
}  // namespace kv

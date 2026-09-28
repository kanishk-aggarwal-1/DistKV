#include "server/command_handler.h"

#include <gtest/gtest.h>

#include <string>

namespace kv {
namespace {

class CommandHandlerTest : public ::testing::Test {
 protected:
  std::string run(const resp::Command& cmd) {
    std::string out;
    handler_.execute(cmd, session_, out);
    return out;
  }

  Store store_{16};
  CommandHandler handler_{store_, nullptr};
  net::Session session_;
};

TEST_F(CommandHandlerTest, Ping) {
  EXPECT_EQ(run({"PING"}), "+PONG\r\n");
  EXPECT_EQ(run({"ping", "hello"}), "$5\r\nhello\r\n");
}

TEST_F(CommandHandlerTest, SetGetDel) {
  EXPECT_EQ(run({"GET", "k"}), "$-1\r\n");
  EXPECT_EQ(run({"SET", "k", "v1"}), "+OK\r\n");
  EXPECT_EQ(run({"GET", "k"}), "$2\r\nv1\r\n");
  EXPECT_EQ(run({"set", "k", "v2"}), "+OK\r\n");
  EXPECT_EQ(run({"get", "k"}), "$2\r\nv2\r\n");
  EXPECT_EQ(run({"DEL", "k", "missing"}), ":1\r\n");
  EXPECT_EQ(run({"GET", "k"}), "$-1\r\n");
}

TEST_F(CommandHandlerTest, CommandNamesAreCaseInsensitive) {
  EXPECT_EQ(run({"SeT", "k", "v"}), "+OK\r\n");
  EXPECT_EQ(run({"gEt", "k"}), "$1\r\nv\r\n");
}

TEST_F(CommandHandlerTest, WrongArity) {
  EXPECT_EQ(run({"GET"}), "-ERR wrong number of arguments for 'get' command\r\n");
  EXPECT_EQ(run({"SET", "k"}), "-ERR wrong number of arguments for 'set' command\r\n");
  EXPECT_EQ(run({"DEL"}), "-ERR wrong number of arguments for 'del' command\r\n");
  EXPECT_EQ(run({"PING", "a", "b"}), "-ERR wrong number of arguments for 'ping' command\r\n");
}

TEST_F(CommandHandlerTest, SetOptionsAreRejected) {
  EXPECT_EQ(run({"SET", "k", "v", "EX", "10"}), "-ERR syntax error\r\n");
}

TEST_F(CommandHandlerTest, UnknownCommand) {
  EXPECT_EQ(run({"FLUSHALL"}), "-ERR unknown command 'FLUSHALL'\r\n");
}

TEST_F(CommandHandlerTest, StandaloneModeServesEverySlotAndAllowsCrossSlotDel) {
  EXPECT_EQ(run({"SET", "foo", "1"}), "+OK\r\n");
  EXPECT_EQ(run({"SET", "bar", "2"}), "+OK\r\n");
  EXPECT_EQ(run({"DEL", "foo", "bar"}), ":2\r\n");
  EXPECT_EQ(run({"CLUSTER", "SLOTS"}), "-ERR This instance has cluster support disabled\r\n");
}

TEST_F(CommandHandlerTest, AskingIsAcceptedInStandaloneMode) {
  EXPECT_EQ(run({"ASKING"}), "+OK\r\n");
}

// A node "me" in a two-node cluster: slots < 8192 are mine, the rest belong to "other".
class ClusterCommandTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster::ClusterMap map;
    map.epoch = 1;
    map.nodes = {{"me", "10.0.0.1:7001", "10.0.0.1:17001"}, {"other", "10.0.0.2:7002", "10.0.0.2:17002"}};
    map.groups = {{"g1", "me", "", false}, {"g2", "other", "", false}};
    map.slot_owner.resize(cluster::kNumSlots);
    for (uint16_t s = 0; s < cluster::kNumSlots; ++s) {
      map.slot_owner[s] = s < 8192 ? 0 : 1;
      store_.applyOwnership(s, s < 8192);
    }
    ASSERT_TRUE(state_.apply(map));
  }

  std::string run(const resp::Command& cmd) {
    std::string out;
    handler_.execute(cmd, session_, out);
    return out;
  }

  // "bar" -> slot 5061 (mine), "foo" -> slot 12182 (other's).
  Store store_{16, SlotState::kNotOwned};
  cluster::ClusterState state_{"me"};
  CommandHandler handler_{store_, &state_};
  net::Session session_;
};

TEST_F(ClusterCommandTest, ServesOwnSlotsAndRedirectsOthersWithMoved) {
  EXPECT_EQ(run({"SET", "bar", "1"}), "+OK\r\n");
  EXPECT_EQ(run({"GET", "bar"}), "$1\r\n1\r\n");
  EXPECT_EQ(run({"SET", "foo", "1"}), "-MOVED 12182 10.0.0.2:7002\r\n");
  EXPECT_EQ(run({"GET", "foo"}), "-MOVED 12182 10.0.0.2:7002\r\n");
  EXPECT_EQ(run({"DEL", "foo"}), "-MOVED 12182 10.0.0.2:7002\r\n");
}

TEST_F(ClusterCommandTest, NoMapMeansClusterDown) {
  Store store(16, SlotState::kNotOwned);
  cluster::ClusterState empty("me");
  CommandHandler handler(store, &empty);
  std::string out;
  handler.execute({"GET", "foo"}, session_, out);
  EXPECT_EQ(out, "-CLUSTERDOWN Hash slot not served\r\n");
}

TEST_F(ClusterCommandTest, MigratingSlotAsksForAbsentKeys) {
  ASSERT_TRUE(store_.beginMigration(5061));
  state_.setMigrationTarget(5061, "10.0.0.2:7002");
  EXPECT_EQ(run({"GET", "bar"}), "-ASK 5061 10.0.0.2:7002\r\n");
}

TEST_F(ClusterCommandTest, AskingAppliesToExactlyOneCommand) {
  ASSERT_TRUE(store_.beginImport(12182, 1));
  EXPECT_EQ(run({"SET", "foo", "1"}), "-MOVED 12182 10.0.0.2:7002\r\n");
  EXPECT_EQ(run({"ASKING"}), "+OK\r\n");
  EXPECT_EQ(run({"SET", "foo", "1"}), "+OK\r\n");
  EXPECT_EQ(run({"GET", "foo"}), "-MOVED 12182 10.0.0.2:7002\r\n");
}

TEST_F(ClusterCommandTest, MultiKeyDelMustStayInOneSlot) {
  EXPECT_EQ(run({"DEL", "foo", "bar"}), "-CROSSSLOT Keys in request don't hash to the same slot\r\n");
  EXPECT_EQ(run({"SET", "{bar}a", "1"}), "+OK\r\n");  // hash tags keep related keys together
  EXPECT_EQ(run({"SET", "{bar}b", "1"}), "+OK\r\n");
  EXPECT_EQ(run({"DEL", "{bar}a", "{bar}b"}), ":2\r\n");
}

TEST_F(ClusterCommandTest, ClusterSlotsListsContiguousRanges) {
  EXPECT_EQ(run({"CLUSTER", "SLOTS"}),
            "*2\r\n"
            "*3\r\n:0\r\n:8191\r\n*3\r\n$8\r\n10.0.0.1\r\n:7001\r\n$2\r\nme\r\n"
            "*3\r\n:8192\r\n:16383\r\n*3\r\n$8\r\n10.0.0.2\r\n:7002\r\n$5\r\nother\r\n");
}

}  // namespace
}  // namespace kv

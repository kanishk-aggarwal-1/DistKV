// Runs a real Server in-process and talks to it over TCP.

#include "server/server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

namespace kv {
namespace {

// Minimal blocking client for tests.
class Client {
 public:
  explicit Client(uint16_t port) {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      ADD_FAILURE() << "connect failed";
    }
  }
  ~Client() { ::close(fd_); }

  void send(const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
      ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
      ASSERT_GT(n, 0);
      sent += static_cast<size_t>(n);
    }
  }

  // Reads exactly `n` bytes, or fewer if the server closes the connection.
  std::string recv(size_t n) {
    std::string data(n, '\0');
    size_t got = 0;
    while (got < n) {
      ssize_t r = ::recv(fd_, data.data() + got, n - got, 0);
      if (r <= 0) break;
      got += static_cast<size_t>(r);
    }
    data.resize(got);
    return data;
  }

  // True if the server has closed its side of the connection.
  bool closedByPeer() {
    char c;
    return ::recv(fd_, &c, 1, 0) == 0;
  }

 private:
  int fd_;
};

std::string encode(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& a : args) out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

class ServerTest : public ::testing::Test {
 protected:
  void SetUp() override { server_.start(); }

  Server server_{ServerConfig{.port = 0, .threads = 4, .stripes = 64}};
};

TEST_F(ServerTest, PingSetGetDel) {
  Client c(server_.port());
  c.send(encode({"PING"}));
  EXPECT_EQ(c.recv(7), "+PONG\r\n");
  c.send(encode({"SET", "k", "hello"}));
  EXPECT_EQ(c.recv(5), "+OK\r\n");
  c.send(encode({"GET", "k"}));
  EXPECT_EQ(c.recv(11), "$5\r\nhello\r\n");
  c.send(encode({"DEL", "k"}));
  EXPECT_EQ(c.recv(4), ":1\r\n");
  c.send(encode({"GET", "k"}));
  EXPECT_EQ(c.recv(5), "$-1\r\n");
}

TEST_F(ServerTest, PipelinedCommandsAnsweredInOrder) {
  constexpr int kCommands = 1000;
  std::string batch, expected;
  for (int i = 0; i < kCommands; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    batch += encode({"SET", key, value});
    batch += encode({"GET", key});
    expected += "+OK\r\n$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  }
  Client c(server_.port());
  c.send(batch);  // one large write containing 2000 commands
  EXPECT_EQ(c.recv(expected.size()), expected);
}

TEST_F(ServerTest, CommandSentOneByteAtATime) {
  Client c(server_.port());
  for (char ch : encode({"SET", "slow", "drip"})) {
    c.send(std::string(1, ch));
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  EXPECT_EQ(c.recv(5), "+OK\r\n");
}

TEST_F(ServerTest, LargeValueRoundTrip) {
  // Larger than the socket buffers and the output high-water mark, so the
  // reply is written across several writable events.
  std::string value(4 * 1024 * 1024, 'x');
  for (size_t i = 0; i < value.size(); i += 4096) value[i] = static_cast<char>('a' + i % 26);
  Client c(server_.port());
  c.send(encode({"SET", "big", value}));
  EXPECT_EQ(c.recv(5), "+OK\r\n");
  c.send(encode({"GET", "big"}));
  std::string expected = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  EXPECT_EQ(c.recv(expected.size()), expected);
}

TEST_F(ServerTest, ClientThatDoesNotReadIsThrottledNotKilled) {
  // Queue many GETs of a large value without reading replies, then read them
  // all. Backpressure must pause the connection, not drop it or lose replies.
  std::string value(64 * 1024, 'v');
  Client c(server_.port());
  c.send(encode({"SET", "v", value}));
  ASSERT_EQ(c.recv(5), "+OK\r\n");

  constexpr int kGets = 200;  // ~13 MB of replies, well above the high-water mark
  std::string one = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  std::thread writer([&] {
    std::string get = encode({"GET", "v"});
    for (int i = 0; i < kGets; ++i) c.send(get);
  });
  for (int i = 0; i < kGets; ++i) ASSERT_EQ(c.recv(one.size()), one) << "reply " << i;
  writer.join();
}

TEST_F(ServerTest, ProtocolErrorReturnsErrorAndCloses) {
  Client c(server_.port());
  c.send("*1\r\n+PING\r\n");
  std::string reply = c.recv(64);  // reads until the server closes
  EXPECT_EQ(reply.rfind("-ERR Protocol error", 0), 0u) << reply;
  EXPECT_TRUE(c.closedByPeer());
}

TEST_F(ServerTest, ManyConcurrentClients) {
  constexpr int kClients = 16;
  constexpr int kOps = 500;
  std::vector<std::thread> threads;
  for (int t = 0; t < kClients; ++t) {
    threads.emplace_back([this, t] {
      Client c(server_.port());
      for (int i = 0; i < kOps; ++i) {
        std::string key = "c" + std::to_string(t) + ":" + std::to_string(i);
        c.send(encode({"SET", key, key}));
        ASSERT_EQ(c.recv(5), "+OK\r\n");
        c.send(encode({"GET", key}));
        std::string expected = "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
        ASSERT_EQ(c.recv(expected.size()), expected);
      }
    });
  }
  for (auto& th : threads) th.join();
}

TEST(ServerLifecycle, StopIsIdempotent) {
  Server server(ServerConfig{.port = 0, .threads = 2, .stripes = 4});
  server.start();
  server.stop();
  server.stop();
}

}  // namespace
}  // namespace kv

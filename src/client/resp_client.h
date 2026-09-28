#pragma once

// Blocking RESP clients: a single-connection client and a
// cluster-aware client that follows MOVED / ASK and retries TRYAGAIN, like
// redis-cli -c or memtier --cluster-mode.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cluster/cluster_map.h"
#include "cluster/slot.h"

namespace kv::client {

struct Reply {
  char type = 0;  // '+', '-', ':', '$', '*'
  std::string str;             // simple string, error or bulk string
  int64_t integer = 0;
  bool is_null = false;        // $-1 or *-1
  std::vector<Reply> elements;

  bool isError() const { return type == '-'; }
  bool isErrorWithPrefix(std::string_view prefix) const {
    return isError() && str.compare(0, prefix.size(), prefix) == 0;
  }
};

inline std::string encodeCommand(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& a : args) out += "$" + std::to_string(a.size()) + "\r\n" + a + "\r\n";
  return out;
}

class RespClient {
 public:
  explicit RespClient(const std::string& addr) {
    std::string host;
    uint16_t port = 0;
    if (!cluster::splitHostPort(addr, host, port)) throw std::runtime_error("bad address " + addr);
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    ::inet_pton(AF_INET, host.c_str(), &sa.sin_addr);
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
      ::close(fd_);
      throw std::runtime_error("connect to " + addr + " failed");
    }
    int one = 1;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }
  ~RespClient() { ::close(fd_); }
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;

  void sendRaw(const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
      ssize_t n = ::send(fd_, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
      if (n <= 0) throw std::runtime_error("send failed");
      sent += static_cast<size_t>(n);
    }
  }

  Reply command(const std::vector<std::string>& args) {
    sendRaw(encodeCommand(args));
    return readReply();
  }

  Reply readReply() {
    std::string line = readLine();
    Reply reply;
    reply.type = line[0];
    std::string body = line.substr(1);
    switch (reply.type) {
      case '+':
      case '-':
        reply.str = body;
        break;
      case ':':
        reply.integer = std::stoll(body);
        break;
      case '$': {
        int64_t len = std::stoll(body);
        if (len < 0) {
          reply.is_null = true;
          break;
        }
        reply.str = readExactly(static_cast<size_t>(len) + 2).substr(0, static_cast<size_t>(len));
        break;
      }
      case '*': {
        int64_t count = std::stoll(body);
        if (count < 0) {
          reply.is_null = true;
          break;
        }
        for (int64_t i = 0; i < count; ++i) reply.elements.push_back(readReply());
        break;
      }
      default:
        throw std::runtime_error("bad reply type: " + line);
    }
    return reply;
  }

 private:
  std::string readLine() {
    while (true) {
      size_t eol = buffer_.find("\r\n");
      if (eol != std::string::npos) {
        std::string line = buffer_.substr(0, eol);
        buffer_.erase(0, eol + 2);
        return line;
      }
      fill();
    }
  }

  std::string readExactly(size_t n) {
    while (buffer_.size() < n) fill();
    std::string data = buffer_.substr(0, n);
    buffer_.erase(0, n);
    return data;
  }

  void fill() {
    char buf[16384];
    ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n <= 0) throw std::runtime_error("connection closed");
    buffer_.append(buf, static_cast<size_t>(n));
  }

  int fd_;
  std::string buffer_;
};

// Follows redirects the way a cluster-aware client does:
//   MOVED slot addr  -> remember the new owner of the slot and retry there
//   ASK slot addr    -> send ASKING + the command to addr, once
//   TRYAGAIN         -> wait briefly and retry
// and survives node failures:
//   cannot connect   -> nothing was sent: refresh the slot table from any
//                       seed and retry
//   connection lost after sending -> the outcome is unknown: returned as a
//                       reply of type kAmbiguous, never retried (retrying a
//                       DEL could report a wrong count)
class ClusterClient {
 public:
  static constexpr char kAmbiguous = '?';

  explicit ClusterClient(std::string seed_addr) : seeds_{std::move(seed_addr)} { refreshSlots(); }
  explicit ClusterClient(std::vector<std::string> seeds) : seeds_(std::move(seeds)) {
    refreshSlots();
  }

  Reply execute(const std::vector<std::string>& args) {
    const uint16_t slot = cluster::keySlot(args.at(1));
    std::string addr = slot_addr_[slot].empty() ? seeds_.front() : slot_addr_[slot];
    bool asking = false;
    for (int attempt = 0; attempt < 5000; ++attempt) {
      RespClient* conn = nullptr;
      try {
        conn = &connection(addr);
      } catch (const std::exception&) {
        // Could not connect: the node is down. Nothing was sent.
        ++connect_failures;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        refreshSlots();
        addr = slot_addr_[slot].empty() ? seeds_.front() : slot_addr_[slot];
        asking = false;
        continue;
      }
      Reply reply;
      try {
        if (asking) {
          conn->sendRaw(encodeCommand({"ASKING"}) + encodeCommand(args));
          Reply ok = conn->readReply();
          if (ok.isError()) throw std::runtime_error("ASKING failed: " + ok.str);
          reply = conn->readReply();
        } else {
          reply = conn->command(args);
        }
      } catch (const std::exception&) {
        connections_.erase(addr);
        Reply ambiguous;
        ambiguous.type = kAmbiguous;
        ambiguous.str = "connection to " + addr + " lost";
        return ambiguous;
      }
      asking = false;

      if (reply.isErrorWithPrefix("MOVED ")) {
        ++moved;
        addr = redirectAddress(reply.str);
        slot_addr_[slot] = addr;
      } else if (reply.isErrorWithPrefix("ASK ")) {
        ++ask;
        addr = redirectAddress(reply.str);
        asking = true;
      } else if (reply.isErrorWithPrefix("TRYAGAIN") || reply.isErrorWithPrefix("CLUSTERDOWN")) {
        ++tryagain;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      } else {
        return reply;
      }
    }
    throw std::runtime_error("too many redirects for key " + args.at(1));
  }

  // Rebuilds the slot table from the first seed that answers CLUSTER SLOTS.
  void refreshSlots() {
    for (const std::string& seed : seeds_) {
      try {
        Reply slots = connection(seed).command({"CLUSTER", "SLOTS"});
        if (slots.type != '*') continue;
        for (const Reply& range : slots.elements) {
          const Reply& node = range.elements.at(2);
          std::string addr =
              node.elements.at(0).str + ":" + std::to_string(node.elements.at(1).integer);
          for (int64_t s = range.elements.at(0).integer; s <= range.elements.at(1).integer; ++s) {
            slot_addr_[static_cast<size_t>(s)] = addr;
          }
        }
        return;
      } catch (const std::exception&) {
        connections_.erase(seed);
      }
    }
  }

  uint64_t moved = 0, ask = 0, tryagain = 0, connect_failures = 0;

 private:
  static std::string redirectAddress(const std::string& error) {
    return error.substr(error.rfind(' ') + 1);  // "MOVED <slot> <host:port>"
  }

  RespClient& connection(const std::string& addr) {
    auto& conn = connections_[addr];
    if (!conn) {
      try {
        conn = std::make_unique<RespClient>(addr);
      } catch (...) {
        connections_.erase(addr);
        throw;
      }
    }
    return *conn;
  }

  std::vector<std::string> seeds_;
  std::array<std::string, cluster::kNumSlots> slot_addr_;
  std::map<std::string, std::unique_ptr<RespClient>> connections_;
};

}  // namespace kv::client

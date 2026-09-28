#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

#include "server/server.h"

namespace {

void usage(const char* prog) {
  std::fprintf(stderr,
               "usage: %s [--port N] [--threads N] [--stripes N]\n"
               "          [--cluster --node-id ID [--grpc-port N] [--advertise-host HOST]]\n"
               "  --port            TCP port for clients (default 6380)\n"
               "  --threads         event-loop threads (default: hardware threads)\n"
               "  --stripes         lock stripes in the store (default 256)\n"
               "  --cluster         run as a cluster node controlled by a coordinator\n"
               "  --node-id         unique node id (cluster mode)\n"
               "  --grpc-port       port for the internal gRPC service (default: port + 10000)\n"
               "  --advertise-host  address clients and peers use to reach this node\n"
               "                    (default 127.0.0.1)\n",
               prog);
}

// Parses a whole non-negative decimal number; rejects "", "12x", "-1".
bool parseNumber(const char* text, unsigned long& value) {
  if (*text < '0' || *text > '9') return false;
  char* end = nullptr;
  errno = 0;
  value = std::strtoul(text, &end, 10);
  return errno == 0 && *end == '\0';
}

bool parseArgs(int argc, char** argv, kv::ServerConfig& config) {
  bool grpc_port_set = false;
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if (flag == "--cluster") {
      config.cluster = true;
      continue;
    }
    if (i + 1 >= argc) return false;
    const char* arg = argv[++i];
    if (flag == "--node-id") {
      config.node_id = arg;
      continue;
    }
    if (flag == "--advertise-host") {
      config.advertise_host = arg;
      continue;
    }
    unsigned long value = 0;
    if (!parseNumber(arg, value)) return false;
    if (flag == "--port" && value <= 65535) {
      config.port = static_cast<uint16_t>(value);
    } else if (flag == "--grpc-port" && value <= 65535) {
      config.grpc_port = static_cast<uint16_t>(value);
      grpc_port_set = true;
    } else if (flag == "--threads") {
      config.threads = static_cast<unsigned>(value);
    } else if (flag == "--stripes" && value > 0) {
      config.stripes = value;
    } else {
      return false;
    }
  }
  if (config.cluster && config.node_id.empty()) return false;
  if (config.cluster && !grpc_port_set) {
    if (config.port == 0 || config.port > 55535) return false;
    config.grpc_port = static_cast<uint16_t>(config.port + 10000);
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  kv::ServerConfig config;
  if (!parseArgs(argc, argv, config)) {
    usage(argv[0]);
    return 2;
  }

  // Block SIGINT/SIGTERM before any thread starts (threads inherit the
  // mask), then wait for them synchronously on the main thread. This avoids
  // doing work inside an async signal handler.
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  try {
    kv::Server server(config);
    server.start();
    std::printf("distkv listening on port %u\n", server.port());
    if (config.cluster) {
      std::printf("cluster node %s, gRPC on port %u\n", config.node_id.c_str(), server.grpcPort());
    }
    std::fflush(stdout);

    int sig = 0;
    sigwait(&signals, &sig);
    std::printf("received %s, shutting down\n", strsignal(sig));
    server.stop();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "fatal: %s\n", e.what());
    return 1;
  }
  return 0;
}

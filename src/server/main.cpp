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
               "  --port     TCP port to listen on (default 6380)\n"
               "  --threads  event-loop threads (default: hardware threads)\n"
               "  --stripes  lock stripes in the store (default 256)\n",
               prog);
}

bool parseArgs(int argc, char** argv, kv::ServerConfig& config) {
  for (int i = 1; i < argc; ++i) {
    std::string flag = argv[i];
    if (i + 1 >= argc) return false;
    unsigned long value = std::strtoul(argv[++i], nullptr, 10);
    if (flag == "--port" && value <= 65535) {
      config.port = static_cast<uint16_t>(value);
    } else if (flag == "--threads") {
      config.threads = static_cast<unsigned>(value);
    } else if (flag == "--stripes" && value > 0) {
      config.stripes = value;
    } else {
      return false;
    }
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

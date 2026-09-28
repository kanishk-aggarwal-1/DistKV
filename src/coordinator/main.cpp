#include <grpcpp/grpcpp.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "coordinator/coordinator.h"

namespace {

void usage(const char* prog) {
  std::fprintf(stderr,
               "usage: %s [--port N] [--vnodes N] [--slots-per-step N]\n"
               "          [--heartbeat-ms N] [--failure-timeout-ms N]\n"
               "  --port                gRPC port (default 9000)\n"
               "  --vnodes              virtual nodes per group on the hash ring (default 128)\n"
               "  --slots-per-step      slots migrated per step (default 256)\n"
               "  --heartbeat-ms        how often every node is pinged (default 100)\n"
               "  --failure-timeout-ms  silence after which a node is declared dead\n"
               "                        (default 1000)\n",
               prog);
}

bool parseNumber(const char* text, unsigned long& value) {
  if (*text < '0' || *text > '9') return false;
  char* end = nullptr;
  errno = 0;
  value = std::strtoul(text, &end, 10);
  return errno == 0 && *end == '\0';
}

}  // namespace

int main(int argc, char** argv) {
  unsigned long port = 9000;
  kv::coordinator::CoordinatorConfig config;
  for (int i = 1; i < argc; i += 2) {
    std::string flag = argv[i];
    unsigned long value = 0;
    if (i + 1 >= argc || !parseNumber(argv[i + 1], value)) {
      usage(argv[0]);
      return 2;
    }
    if (flag == "--port" && value <= 65535) {
      port = value;
    } else if (flag == "--vnodes" && value > 0) {
      config.vnodes_per_node = value;
    } else if (flag == "--slots-per-step" && value > 0) {
      config.slots_per_step = value;
    } else if (flag == "--heartbeat-ms" && value > 0) {
      config.heartbeat_interval = std::chrono::milliseconds(value);
    } else if (flag == "--failure-timeout-ms" && value > 0) {
      config.failure_timeout = std::chrono::milliseconds(value);
    } else {
      usage(argv[0]);
      return 2;
    }
  }

  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);

  kv::coordinator::Coordinator coordinator(config);
  kv::coordinator::CoordinatorService service(coordinator);
  grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort("0.0.0.0:" + std::to_string(port), grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server || selected_port == 0) {
    std::fprintf(stderr, "fatal: cannot listen on port %lu\n", port);
    return 1;
  }
  std::printf("distkv coordinator listening on port %d\n", selected_port);
  std::fflush(stdout);

  int sig = 0;
  sigwait(&signals, &sig);
  std::printf("received %s, shutting down\n", strsignal(sig));
  server->Shutdown();
  return 0;
}

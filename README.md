# DistKV

A sharded, replicated, in-memory key-value store written in C++20. It speaks
a subset of the Redis protocol (`PING`, `GET`, `SET`, `DEL`), so standard
tools such as `redis-cli` and `memtier_benchmark` work with it unchanged.

It's a portfolio project for distributed systems and systems programming.
Every major design decision, the alternatives considered and the known
weaknesses are written up in [docs/DESIGN.md](docs/DESIGN.md).

## Status

| Phase | Scope | Status |
|---|---|---|
| 1 | Single-node server: epoll event loops, RESP parser, lock-striped store | Done |
| 2 | Sharding with consistent hashing, request routing, key migration | Planned |
| 3 | Primary-backup replication, heartbeats, automatic failover | Planned |
| 4 | AWS deployment across 3 availability zones with Terraform | Planned |
| 5 | Benchmarks against Redis on EC2, failure testing | Planned |
| 6 | Final documentation | Planned |

## Architecture (Phase 1)

```
clients ──TCP──► SO_REUSEPORT listen sockets (one per thread)
                   │
                   ▼
        EventLoop thread 0..N-1 (epoll, level-triggered)
          read ─► RESP parser ─► command handler ─► reply buffer ─► send
                                      │
                                      ▼
                     Store: 256 stripes, each an unordered_map
                     guarded by its own std::shared_mutex
```

Each connection stays on one thread for its whole life, so pipelined replies
come back in order without any extra coordination. See
[docs/DESIGN.md](docs/DESIGN.md#phase-1-single-node-server) for why this
model was chosen over a single thread (like Redis) or a shared-nothing
thread-per-core design.

## Repository layout

```
src/protocol/   RESP parser and reply encoder (no I/O)
src/storage/    thread-safe lock-striped store
src/net/        epoll event loop, connections, sockets
src/server/     command dispatch, server wiring, main
tests/unit/     GoogleTest: parser, store, command handler
tests/integration/  in-process server over TCP; redis-cli end-to-end script
bench/          benchmark scripts and committed raw results
docker/         Linux development image
scripts/        dev container launcher, test runner
docs/           DESIGN.md
```

## Building and running

The server uses Linux-only APIs (`epoll`, `eventfd`, `SO_REUSEPORT`). On
Windows or macOS, use the dev container.
[docker/dev.Dockerfile](docker/dev.Dockerfile) pins the toolchain: GCC 13,
CMake, Ninja, Redis 7 and memtier_benchmark 2.5.1.

```powershell
# Windows PowerShell: open a shell in the dev container (builds the image on first use)
.\scripts\dev.ps1
```

Inside the container, or on any Linux machine with CMake ≥ 3.22, Ninja,
GCC 13+ and redis-tools:

```bash
cmake --preset release
cmake --build --preset release
./build/release/distkv-server --port 6380 --threads 4
```

```bash
redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
```

Server flags: `--port` (default 6380), `--threads` (default: one per
hardware thread), `--stripes` (default 256).

## Testing

```bash
scripts/check.sh              # debug, AddressSanitizer+UBSan and ThreadSanitizer builds
scripts/check.sh debug        # a single preset
```

Unit tests cover the parser, including input split at every byte offset, the
store under concurrent access, and command semantics. Integration tests run a
real server over TCP: pipelining, one byte at a time, large values,
backpressure from a client that doesn't read, concurrent clients, and
end-to-end checks with `redis-cli`. CI runs all three builds on every push.

## Benchmarks

Phase 1 results come from a laptop and aren't representative. They're
recorded with their caveats in
[docs/DESIGN.md](docs/DESIGN.md#local-benchmark-phase-1), and the raw output
is in [bench/results/phase1-local/](bench/results/phase1-local/). Proper
results from dedicated EC2 instances will be added in Phase 5.

```bash
bench/run_local.sh            # DistKV vs Redis with memtier_benchmark
```

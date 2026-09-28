# DistKV

A sharded, replicated, in-memory key-value store written in C++20. It speaks
a subset of the Redis protocol (`PING`, `GET`, `SET`, `DEL`, plus the Redis
Cluster routing commands `CLUSTER SLOTS` and `ASKING`), so standard tools
such as `redis-cli -c` and `memtier_benchmark --cluster-mode` work with it
unchanged.

It's a portfolio project for distributed systems and systems programming.
Every major design decision, the alternatives considered and the known
weaknesses are written up in [docs/DESIGN.md](docs/DESIGN.md).

## Status

| Phase | Scope | Status |
|---|---|---|
| 1 | Single-node server: epoll event loops, RESP parser, lock-striped store | Done |
| 2 | Sharding: consistent hashing with virtual nodes, MOVED/ASK routing, live slot migration | Done |
| 3 | Primary-backup replication, heartbeats, automatic failover | Planned |
| 4 | AWS deployment across 3 availability zones with Terraform | Planned |
| 5 | Benchmarks against Redis on EC2, failure testing | Planned |
| 6 | Final documentation | Planned |

## Architecture

```
                   distkv-admin ──gRPC──► distkv-coordinator
                                          (cluster map: epoch, nodes,
                                           owner of each of 16384 slots)
                                                  │ gRPC
                     ┌────────────────────────────┼───────────────────────────┐
                     ▼                            ▼                           ▼
              ┌─────────────┐              ┌─────────────┐             ┌─────────────┐
              │ node n1     │◄──gRPC──────►│ node n2     │◄───gRPC────►│ node n3     │
              │ RESP + gRPC │  migration   │ RESP + gRPC │             │ RESP + gRPC │
              └──────▲──────┘              └──────▲──────┘             └──────▲──────┘
                     └───────── RESP, with MOVED / ASK redirects ─────────────┘
                              cluster-aware clients (redis-cli -c, memtier)
```

- **Keys → slots:** a key's slot is `CRC16(key) mod 16384`, as in Redis
  Cluster, so standard cluster clients route correctly.
- **Slots → nodes:** a consistent-hash ring with 128 virtual nodes per node
  decides which node owns each slot. Adding or removing a node moves only
  about 1/N of the slots.
- **Live migration:**
  - The slot keeps serving while its keys are copied to the new owner.
  - Clients follow `ASK` for keys that have already moved.
  - Writes to keys in the batch being copied get `TRYAGAIN` for one round
    trip.
  - A test checks that no acknowledged write is lost while nodes join and
    leave under load.

Inside each node:

```
clients ──TCP──► SO_REUSEPORT listen sockets (one per event-loop thread)
                   │
        EventLoop threads (epoll): read ─► RESP parser ─► command handler ─► send
                                                             │
                              Store: 16384 slots (keys + slot state),
                              lock-striped with std::shared_mutex
                                                             ▲
        gRPC NodeService threads (coordinator commands, key import/export)
```

## Repository layout

```
src/protocol/      RESP parser and reply encoder (no I/O)
src/storage/       slot-partitioned, lock-striped store with migration support
src/net/           epoll event loop, connections, sockets
src/cluster/       slots, hash ring, cluster map, node gRPC service
src/coordinator/   coordinator (map owner, rebalancing) and its main
src/server/        command dispatch, server wiring, node main
src/tools/         distkv-admin CLI
proto/             gRPC service definitions
tests/unit/        GoogleTest: parser, store, slots, hash ring, commands
tests/integration/ in-process servers and clusters over TCP/gRPC; redis-cli scripts
bench/             benchmark scripts and committed raw results
docker/            Linux development image
scripts/           dev container launcher, test runner, local cluster
docs/              DESIGN.md
```

## Building

The server uses Linux-only APIs (`epoll`, `eventfd`, `SO_REUSEPORT`). On
Windows or macOS, use the dev container.
[docker/dev.Dockerfile](docker/dev.Dockerfile) pins the toolchain: GCC 13,
CMake, Ninja, gRPC 1.51, Redis 7 and memtier_benchmark 2.5.1.

```powershell
# Windows PowerShell: open a shell in the dev container (builds the image on first use)
.\scripts\dev.ps1
```

Inside the container, or on Ubuntu 24.04 with `build-essential cmake ninja-build
redis-tools libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc`:

```bash
cmake --preset release
cmake --build --preset release
```

## Running

A single node, serving every slot itself:

```bash
./build/release/distkv-server --port 6380 --threads 4
redis-cli -p 6380 SET hello world
```

A local cluster (a coordinator plus nodes on 127.0.0.1:7001, 7002, …):

```bash
scripts/local_cluster.sh start 3      # coordinator + 3 nodes
redis-cli -c -p 7001 SET hello world  # -c follows MOVED/ASK redirects
scripts/local_cluster.sh add 4        # start node 4 and rebalance onto it
scripts/local_cluster.sh remove 2     # move node 2's slots away, then stop it
scripts/local_cluster.sh show         # print the cluster map
scripts/local_cluster.sh stop
```

```bash
memtier_benchmark -s 127.0.0.1 -p 7001 --cluster-mode --protocol=redis
```

Node flags: `--port`, `--threads`, `--stripes`, and for cluster mode
`--cluster --node-id ID [--grpc-port N] [--advertise-host HOST]`.
Coordinator flags: `--port`, `--vnodes`, `--slots-per-step`.

## Testing

```bash
scripts/check.sh              # debug, AddressSanitizer+UBSan and ThreadSanitizer builds
scripts/check.sh debug        # a single preset
```

- **Unit tests:**
  - the parser, including input split at every byte offset;
  - the store under concurrent access, and its slot states and migration
    primitives;
  - CRC16 slot values checked against Redis;
  - hash ring balance and minimal movement;
  - command replies and redirects.
- **Integration tests:**
  - a real server over TCP: pipelining, one byte at a time, large values,
    backpressure, concurrent clients;
  - in-process clusters, including the main migration test: writers keep
    running SET, DEL and GET while nodes join and leave, then every
    acknowledged write is checked;
  - end-to-end checks with real processes and `redis-cli -c`.
- **CI** runs all three builds on every push.

## Benchmarks

Local results so far come from a laptop and aren't representative. They're
recorded with their caveats in [docs/DESIGN.md](docs/DESIGN.md), and the raw
output is in [bench/results/](bench/results/). Proper results from dedicated
EC2 instances will be added in Phase 5.

```bash
bench/run_local.sh            # DistKV vs Redis with memtier_benchmark
```

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
| 3 | Synchronous primary-backup replication, heartbeats, automatic failover with epoch fencing | Done |
| 4 | AWS deployment across 3 availability zones with Terraform | Built; scripts rehearsed locally on Docker stand-ins; first real AWS deployment pending |
| 5 | Benchmarks against Redis on EC2, failure testing | Planned |
| 6 | Final documentation | Planned |

## Architecture

```
                 distkv-admin ──gRPC──► distkv-coordinator
                                        (cluster map: epoch, groups, spares,
                                         owner of each of 16384 slots;
                                         heartbeats, failover, re-sync)
                                                │ gRPC
             ┌──────────────────────────────────┼──────────────────────────┐
             ▼                                  ▼                          ▼
   group g1                           group g2                         spare
 ┌──────────┐ replicate ┌──────────┐ ┌──────────┐     ┌──────────┐  ┌──────────┐
 │ n1       │──────────►│ n2       │ │ n3       │────►│ n4       │  │ n5       │
 │ primary  │◄──────────│ backup   │ │ primary  │◄────│ backup   │  │          │
 └────▲─────┘   acks    └──────────┘ └────▲─────┘     └──────────┘  └──────────┘
      └────────── RESP, with MOVED / ASK redirects ─┘
               cluster-aware clients (redis-cli -c, memtier)
```

- **Keys → slots:** a key's slot is `CRC16(key) mod 16384`, as in Redis
  Cluster, so standard cluster clients route correctly.
- **Slots → groups:** a consistent-hash ring with 128 virtual nodes per group
  decides which replication group owns each slot. Adding or removing a group
  moves only about 1/N of the slots.
- **Live migration:**
  - The slot keeps serving while its keys are copied to the new owner.
  - Clients follow `ASK` for keys that have already moved.
  - Writes to keys in the batch being copied get `TRYAGAIN` for one round
    trip.
- **Synchronous replication:**
  - Every write is streamed, in order, to the group's backup.
  - The client's reply is held until the backup acknowledges the write, so no
    acknowledged write is lost when a primary dies.
  - With no synced backup, writes are refused (`NOREPLICAS`); reads still work.
- **Failover:**
  - The coordinator pings every node every 100 ms and promotes the backup after
    1 s of silence.
  - A spare then becomes the new backup and is fully synced.
  - The promoted backup refuses its old primary's stream (epoch fencing), so a
    cut-off old primary can't acknowledge writes.
- **Tests:** they check that no acknowledged write is lost while groups join
  and leave, when a primary is killed under load, and when a primary dies in
  the middle of a migration.

Inside each node:

```
clients ──TCP──► SO_REUSEPORT listen sockets (one per event-loop thread)
                   │
        EventLoop threads (epoll): read ─► RESP parser ─► command handler ─► send
                                                             │
                              Store: 16384 slots (keys + slot state),
                              lock-striped with std::shared_mutex
                                                             ▲
        gRPC NodeService threads (coordinator commands, key import/export,
                                  applying the primary's replication stream)
        replicator thread (primary): replication log ─► backup; acks wake the
                                  event loops, which release held replies
```

## Repository layout

```
src/protocol/      RESP parser and reply encoder (no I/O)
src/storage/       slot-partitioned, lock-striped store with migration support
src/net/           epoll event loop, connections, sockets
src/cluster/       slots, hash ring, cluster map, node gRPC service
src/replication/   replication log, replicator (primary side), op encoding
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

A local cluster (a coordinator, replication groups and spares on
127.0.0.1; node I serves clients on port 7000+I):

```bash
scripts/local_cluster.sh start 3 1    # coordinator, 3 groups (n1..n6), 1 spare (n7)
redis-cli -c -p 7001 SET hello world  # -c follows MOVED/ASK redirects
scripts/local_cluster.sh add-group    # two more nodes as a new group; rebalance
scripts/local_cluster.sh remove-group g2
scripts/local_cluster.sh kill 1       # SIGKILL n1: its backup is promoted
scripts/local_cluster.sh show         # groups, slot counts, backups, spares
scripts/local_cluster.sh stop
```

```bash
memtier_benchmark -s 127.0.0.1 -p 7001 --cluster-mode --protocol=redis
```

Node flags: `--port`, `--threads`, `--stripes`, and for cluster mode
`--cluster --node-id ID [--grpc-port N] [--advertise-host HOST]
[--replication-timeout-ms N]`.
Coordinator flags: `--port`, `--vnodes`, `--slots-per-step`, `--heartbeat-ms`,
`--failure-timeout-ms`.

## Running on AWS

[deploy/](deploy/) creates a VPC across three availability zones:
- 3 replication groups, with each primary and its backup in different AZs;
- a spare node;
- a coordinator;
- a load generator that also builds the binaries.

Instances are Graviton `c7g`, costing **roughly $0.50/hour** for the whole
cluster. Traffic between AZs is billed on top. See
[docs/DESIGN.md](docs/DESIGN.md#phase-4-aws-deployment) for the estimate and
its caveats.

Prerequisites: an AWS account, `aws configure` done (on Windows or in the dev
container), and ideally an AWS Budgets alert. Everything below runs inside the
dev container (`.\scripts\dev.ps1`).

```bash
deploy/scripts/up.sh          # terraform apply: shows the plan and asks before creating anything
deploy/scripts/deploy.sh      # build on the load generator, copy binaries to every instance
deploy/scripts/start.sh       # start coordinator and nodes, form groups, add the spare
deploy/scripts/status.sh      # processes per instance + cluster map
deploy/scripts/ssh.sh loadgen # shell on the load generator (memtier, redis-cli)
deploy/scripts/kill-node.sh n1   # SIGKILL a node (failure testing)
deploy/scripts/stop.sh        # stop all processes, keep the instances
deploy/scripts/teardown.sh    # destroy everything and verify nothing tagged distkv is left
```

**Safety net:** every instance shuts itself down, and is then terminated, 4
hours after boot (`-var max_lifetime_hours=N` to change). A forgotten cluster
can't keep billing for long, but `teardown.sh` is still the way to finish.

**Rehearsing without AWS:** `deploy/local/rehearse.sh up` starts 9 Docker
containers that stand in for the instances. Run it from Git Bash or a Linux
shell on the host, not inside the dev container. Then
`deploy/local/rehearse.sh run deploy/scripts/deploy.sh` (and likewise
`start.sh`, `kill-node.sh n1`, …) runs the same scripts against them, and
`rehearse.sh down` removes everything.

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
  - the replication log, and a backup store converging to its primary;
  - command replies and redirects.
- **Integration tests:**
  - a real server over TCP: pipelining, one byte at a time, large values,
    backpressure, concurrent clients;
  - in-process clusters, where writers keep running SET, DEL and GET and every
    acknowledged write is checked afterwards:
    - while groups join and leave;
    - when a primary is killed;
    - when a primary dies mid-migration;
    - when a primary is cut off from the coordinator (fencing);
  - end-to-end checks with real processes and `redis-cli -c`, including a
    `SIGKILL` failover.
- **CI** runs all three builds on every push, plus an `infra` job:
  Terraform fmt, validate, and a plan against mocked AWS that checks the
  multi-AZ layout (`terraform test`), and shellcheck. The ThreadSanitizer build skips
  the three failure-injection tests, because gRPC isn't built with TSan (see
  DESIGN.md).

## Benchmarks

Local results so far come from a laptop and aren't representative. They're
recorded with their caveats in [docs/DESIGN.md](docs/DESIGN.md), and the raw
output is in [bench/results/](bench/results/). Proper results from dedicated
EC2 instances will be added in Phase 5.

```bash
bench/run_local.sh            # DistKV vs Redis with memtier_benchmark
```

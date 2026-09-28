# Design

This document records each significant design decision, the alternatives
considered, and why the chosen option won. It grows phase by phase.

---

## Phase 1: Single-node server

### Architecture

```
               ┌──────────────── distkv-server process ────────────────┐
               │                                                        │
 clients ──TCP─┼─► listen socket 0 ─► EventLoop 0 (thread) ─┐           │
  (kernel      │   listen socket 1 ─► EventLoop 1 (thread) ─┼─► Store   │
  SO_REUSEPORT │        ...                ...              │  (N lock  │
  balancing)   │   listen socket N ─► EventLoop N (thread) ─┘  stripes) │
               └────────────────────────────────────────────────────────┘

 Inside one EventLoop, per connection:
   read() ─► input buffer ─► resp::Parser ─► CommandHandler ─► output buffer ─► send()
```

Source layout:

| Directory | Responsibility | Depends on |
|---|---|---|
| `src/protocol` | RESP parser and reply encoder. Pure functions over bytes, with no I/O. | - |
| `src/storage` | `Store`: thread-safe key-value map | - |
| `src/net` | epoll loop, connections, sockets | protocol |
| `src/server` | command dispatch, `Server` wiring, `main` | all of the above |

### Decision: development environment — Docker container

The code targets Linux (`epoll`, `eventfd`, `SO_REUSEPORT`), but development
happens on Windows.

| Option | Why not / why |
|---|---|
| WSL2 + Ubuntu | Fast native loop, but depends on how each developer's machine is set up. |
| **Docker dev image** (chosen) | One `docker/dev.Dockerfile` pins the whole toolchain: GCC 13, CMake, Redis 7, memtier 2.5.1. Anyone can reproduce the build and the benchmark with the same image. |
| Remote Linux VM | Costs money and adds a network round trip to every edit. |

Consequences:
- `build/` is a named Docker volume, so compilation doesn't go through the slow Windows bind mount.
- TSan needs `vm.mmap_rnd_bits ≤ 28`, but the Docker Desktop kernel uses 32. Locally, `scripts/check.sh` runs the TSan tests with ASLR disabled (`setarch -R`, which needs `--security-opt seccomp=unconfined`). CI lowers the sysctl instead.
- Local benchmark numbers come from a VM on a laptop, so they only give a rough idea. The numbers that count are from Phase 5 on dedicated EC2 instances.

### Decision: threading model — event-loop threads plus a lock-striped shared store

| Option | Pros | Cons |
|---|---|---|
| A. One event-loop thread (Redis) | No locks, commands atomic for free, simplest design | Uses one core |
| **B. N event loops + lock-striped store** (chosen) | Uses all cores. Any thread serves any key. Pipelined replies stay in order automatically. Easy to reason about. | Hot keys serialize on their stripe's lock. Lock cache lines bounce between cores. The allocator is shared. |
| C. Shared-nothing, thread per core (Seastar, Dragonfly) | No locks on data, best scaling in theory | Most requests hop between threads. Pipelined replies must be put back in order. Needs cross-thread backpressure. When Phase 3 adds replication acks, a write goes through two layers of message passing. |

Why B: the project's hard problems are distributed ones (Phases 2–3). C would
add a second message-passing system inside every node, which makes
replication harder and the code less readable. B covers the concurrency
questions that matter, is easy to check with TSan, and keeps every client
connection on one thread. Phase 3 relies on that: a SET reply can wait for
the backup's ack without breaking reply order on that connection.

Details within B:

- **`SO_REUSEPORT` per thread rather than one thread that accepts and hands off connections.**
  Each loop owns its own listening socket and the kernel spreads new connections
  across them. There's no thread dedicated to accepting and no handoff queue.
  - Weakness: the kernel balances *connections*, not load. A few long-lived,
    very busy connections can land on the same thread. memtier opens many
    connections, so this doesn't show up in the benchmarks, but a
    proxy-heavy deployment would notice.
  - Also: any process running as the same user can bind the same port and
    receive some of the connections.
- **256 stripes by default** (`--stripes`). With many more stripes than
  threads, two threads rarely want the same lock unless they touch the same
  key. Each stripe is `alignas(64)` so neighbouring locks don't share a cache
  line. *(Phase 2 changed how a key's stripe is chosen: it's now based on
  the key's hash slot instead of the key's hash. See "Store reorganised by
  slot" below.)*
- **`std::shared_mutex` per stripe.** memtier's default mix, like most
  caching workloads, is read-heavy (1 SET : 10 GET), and a reader-writer lock
  lets GETs on the same stripe run in parallel. The cost is that
  `shared_mutex` is more expensive per operation than `std::mutex` when
  nothing contends. That trade-off hasn't been measured yet. It's easy to
  benchmark by swapping the type.
- **GET copies the value while holding the lock.** A reference would dangle as
  soon as the lock is released and another thread overwrites the value.
  Reference-counted values would avoid the copy but add complexity for values
  that are mostly small.

### Decision: event loop details

- **Level-triggered epoll.** A ready socket keeps being reported until it's
  drained, so a missed wake-up can't strand data in the kernel buffer. Each
  readable event does one `read()` of up to 16 KB, which lets other
  connections on the same thread take turns. Edge-triggered mode would save
  some `epoll_wait` calls but requires draining every socket until `EAGAIN`,
  which is a classic source of stalls.
- **Write immediately, register EPOLLOUT only when needed.** Replies are sent
  right after the commands are processed. EPOLLOUT is registered only when the
  kernel's send buffer is full. The loop remembers each connection's
  registered events and skips `epoll_ctl` calls that wouldn't change anything.
- **Backpressure.** If a client keeps sending but doesn't read its replies,
  pending output grows. Above 1 MB, the connection stops parsing and stops
  asking for EPOLLIN. The kernel's receive buffer then fills up, and TCP flow
  control slows the client down, with no data dropped.
  - A bug found while testing this: when the output drained completely during
    a flush, parsing only resumed on the next socket event. If the client had
    already sent everything, that event never came. `Connection::processAndFlush`
    now alternates parsing and flushing until the input is used up or the socket
    actually blocks. `ServerTest.ClientThatDoesNotReadIsThrottledNotKilled`
    covers this case.
- **Shutdown.** `main` blocks SIGINT and SIGTERM before any thread starts, then
  waits for them with `sigwait`. Each loop is woken through an `eventfd`. No
  work happens inside an async signal handler.
- **`TCP_NODELAY`** on every accepted socket so small replies aren't held back
  by Nagle's algorithm. **`MSG_NOSIGNAL`** so a client that has disconnected
  produces `EPIPE` instead of killing the process with SIGPIPE.

### Decision: RESP parser

- **Incremental, with state kept in the parser.** The parser remembers how many
  arguments are still expected and the length of the bulk string it's reading.
  Arguments that are already complete are copied out and their bytes dropped
  from the input buffer. So a 16 MB value arriving in 1,000 reads isn't scanned
  from the start each time; a parser that restarts from scratch on each read
  would be O(n²). The only rescanning is a partial header line, which is
  capped at 64 KB.
- **No I/O.** The parser takes bytes and returns `NeedMore`, `Command` or
  `Error` plus the number of bytes consumed. Tests feed it split and pipelined
  input at every byte offset, with no sockets involved.
- **Only arrays of bulk strings are accepted.** That's what redis-cli and
  memtier send. Redis's inline command format (plain text, for telnet) is out
  of scope, so this server rejects it.
- **Limits:** at most 1M arguments and 16 MB per bulk string (Redis allows
  512 MB). Header lines are capped at 64 KB. The argument vector's
  pre-allocation is capped at 16 entries, so a tiny header can't trigger a huge
  allocation.
- **Protocol errors close the connection** after sending `-ERR Protocol error: ...`,
  as Redis does. Once framing is lost there's no reliable way to find the
  start of the next command.

### Known limitations (Phase 1)

- A single command's arguments can add up to 1M × 16 MB, with no total
  limit. Redis has the same kind of gap, controlled by
  `client-query-buffer-limit`.
- If `accept4` fails with `EMFILE` (too many open files), the loop logs it
  and returns. Because epoll is level-triggered, it will retry immediately,
  so the loop spins until a file descriptor frees up.
- `Store::size()` isn't an atomic snapshot while writers are active.

### Local benchmark (Phase 1)

Raw memtier output is in `bench/results/phase1-local/`, with the exact setup
in `environment.txt`. The script is `bench/run_local.sh`. These runs were
recorded before the project was renamed, so the raw files label DistKV as
`kvstore-1t` and `kvstore-4t`.

**Setup:**
- Machine: Intel i7-1165G7 laptop (4 cores, 8 hyperthreads), WSL2 kernel 6.6, Docker.
- Pinning: the server on CPUs 0–3 (physical cores 0–1) and memtier on CPUs 4–7
  (the other two physical cores).
- Workload: 100 connections (4 threads × 25), 1:10 SET:GET, 32-byte values,
  100k keys prefilled and accessed at random. 20 s per run, 3 runs each.
- Redis 7.0.15 with persistence off and its default single thread.

Median of 3 runs, with the range across runs in brackets:

| Target | Pipeline | Ops/sec | p50 ms | p99 ms |
|---|---|---|---|---|
| Redis 7.0 | 1 | 70.1k [68.9k–74.8k] | 1.31 [1.27–1.32] | 3.41 [2.74–3.42] |
| DistKV, 1 thread | 1 | 88.9k [81.7k–91.7k] | 1.00 [0.98–1.08] | 2.54 [2.50–2.75] |
| DistKV, 4 threads | 1 | 117k [108k–169k] | 0.46 [0.44–0.47] | 10.0 [2.85–13.2] |
| Redis 7.0 | 16 | 781k [778k–846k] | 1.88 [1.82–1.91] | 4.29 [3.65–4.86] |
| DistKV, 1 thread | 16 | 781k [746k–835k] | 1.82 [1.73–1.89] | 4.99 [4.00–5.28] |
| DistKV, 4 threads | 16 | 1.19M [932k–1.20M] | 1.04 [1.03–1.05] | 6.75 [6.40–13.9] |

What the data supports:
- **Same core budget (1 thread vs Redis):** DistKV handled more requests per
  second without pipelining, and about the same with pipeline 16. This is
  *not* evidence that DistKV is faster than Redis in general. Redis does
  more work per command (stats, keyspace bookkeeping, a far richer command
  set) and runs with its default settings.
- **The 4-thread results are limited by the load generator, not the server.**
  memtier used 3.8 of its 4 CPUs (95%) in these runs, compared with about 1.3 in
  the others. The p99 numbers and the wide run-to-run spread are
  consistent with a saturated client. The server's four threads also share two
  physical cores. So these numbers don't show how DistKV scales with
  threads. Phase 5 runs the load generator on its own instance to measure that.
- A laptop running inside a VM is noisy: identical runs of DistKV with 4
  threads and pipeline 1 ranged from 108k to 169k ops/sec.
- These numbers come from the Phase 1 store. Phase 2 put slot hashing on
  every request (see "Phase 2 standalone check" below).

---

## Phase 2: Sharding

### Architecture

```
                         distkv-admin (CLI)
                               │ gRPC
                               ▼
                     ┌───────────────────┐   cluster map (epoch, nodes,
                     │ distkv-coordinator│   16384 slot owners)
                     └─────────┬─────────┘
             gRPC: ApplyClusterMap, SetMigrating/SetImporting, MigrateSlots
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │ node n1     │◄────►│ node n2     │◄────►│ node n3     │  gRPC ImportKeys
   │ RESP :7001  │      │ RESP :7002  │      │ RESP :7003  │  (source → target)
   │ gRPC :17001 │      │ gRPC :17002 │      │ gRPC :17003 │
   └──────▲──────┘      └──────▲──────┘      └──────▲──────┘
          └────────── RESP (MOVED / ASK redirects) ──┘
                 cluster-aware clients: redis-cli -c, memtier --cluster-mode
```

- **Keys → slots:** `CRC16(key) mod 16384`, with `{hash tags}`, exactly as
  in Redis Cluster ([src/cluster/slot.cpp](../src/cluster/slot.cpp)).
- **Slots → nodes:** a consistent-hash ring with virtual nodes decides which
  node owns each slot ([src/cluster/hash_ring.cpp](../src/cluster/hash_ring.cpp)).
- **The coordinator** holds the authoritative map and drives every change
  ([src/coordinator/coordinator.cpp](../src/coordinator/coordinator.cpp)).
- **Nodes** serve RESP to clients and a `NodeService` over gRPC to the
  coordinator and to each other
  ([src/cluster/node_service.cpp](../src/cluster/node_service.cpp)).

### Decision: node-to-node communication — gRPC

| Option | Trade-off |
|---|---|
| **gRPC** (chosen) | Industry standard, typed schemas, deadlines. Costs: a large dependency, and its own thread pool alongside our epoll threads. |
| RESP for internal traffic as well | No new dependency, and everything would stay single-threaded per connection. But request matching, timeouts and schemas would all be hand-written. |
| A custom binary protocol | A second protocol to write and test, with nothing gained at this scale. |

**Where gRPC comes from: Ubuntu 24.04's packages (gRPC 1.51, protobuf 3.21).**
- It installs in about a minute in CI and in the dev image.
- The alternative, building from source, takes 20–40 min per sanitizer build.
- The cost is covered in "ThreadSanitizer and an uninstrumented gRPC" below.

**How gRPC's threads meet the epoll threads: synchronous gRPC server.**
- Phase 2's RPCs are control-plane traffic (map pushes, slot state changes)
  and migration batches. None of them is on a client's request path.
- The handlers run on gRPC's threads and touch only the thread-safe `Store`
  and `ClusterState`. They never touch event-loop state.
- Phase 3 will add a mailbox per event loop (a queue plus an `eventfd`), so a
  client's write can wait for the backup's ack without blocking its thread.

### Decision: request routing — server-side redirects (Redis Cluster's MOVED/ASK)

| Option | Trade-off |
|---|---|
| Routing proxy | Works with every client, but adds a network round trip to every request, and the proxy becomes a bottleneck and a single point of failure. |
| **Redirects** (chosen) | No extra hop once a client's slot table is warm. Clients must be cluster-aware. |
| Nodes forward requests internally | Clients stay unaware of the cluster, but about (N−1)/N of requests take an extra hop between nodes. |

Why use Redis's slots on top of consistent hashing? The cluster-aware
clients (redis-cli -c, memtier --cluster-mode) hard-code `CRC16 mod 16384`
and discover the topology with `CLUSTER SLOTS`. I checked memtier 2.5.1's
source to confirm. A pure ring would need a custom client. So slots are the
routing table clients see, and the ring decides who owns each slot. Adding
or removing a node still moves only about 1/N of the slots.

Protocol additions beyond PING/GET/SET/DEL, all needed for routing:
`CLUSTER SLOTS`, `ASKING`, and the replies `MOVED`, `ASK`, `TRYAGAIN`,
`CROSSSLOT` and `CLUSTERDOWN`.

**Measured ring balance** (128 virtual nodes, from `HashRing.SpreadsSlotsEvenly`):
- 3 nodes: 94.4% – 110.7% of a fair share
- 4 nodes: 91.7% – 116.7%
- 6 nodes: 87.7% – 113.8%

More virtual nodes would even this out, at the cost of a larger ring to
search.

### Decision: membership — a single coordinator process

| Option | Trade-off |
|---|---|
| **Single coordinator** (chosen) | One writer of the map, so ordering is trivial and correctness is easy to argue. It's a single point of failure (see Phase 3). |
| Gossip with config epochs (Redis Cluster) | No single point of failure, but eventually consistent. Much harder to test, and it roughly doubles the size of Phases 2–3. |
| Static config plus an admin tool | Simplest, but can't do automatic failover in Phase 3. |

- Nodes are passive: they start with no slots and wait for the coordinator.
- `distkv-admin add-node <grpc addr>` asks the coordinator to fetch the node's
  id and client address from the node itself, then rebalance.
- Every map change increments the **epoch**, and nodes ignore older maps. A
  delayed or retried push therefore can't undo a newer one.

### Decision: live migration (Redis's MIGRATING/IMPORTING + ASK), with per-key fencing

Three options were considered:
- **Live migration** (chosen): the slot keeps serving throughout.
- **Freeze-copy-flip:** writes to a batch of slots pause for the whole copy.
- **Copy-then-catch-up:** snapshot plus a write log.

The coordinator moves the slots in steps of `slots_per_step` slots (256 by
default). For each step:

1. **Target → IMPORTING.** It serves those slots only to clients that send
   `ASKING` first. Everyone else gets `MOVED` to the current owner.
2. **Source → MIGRATING.**
   - Keys it still has are served normally.
   - An absent key gets `-ASK <slot> <target>`. That includes new keys, so the
     source only ever shrinks and the migration is guaranteed to finish.
   - A multi-key DEL that's split between the two nodes gets `TRYAGAIN`.
3. **The source moves the keys in batches** (at most 128 keys or 1 MB):
   1. Under the slot's lock, mark the batch's keys **in flight**. They can
      still be read, but writes and deletes to them get `-TRYAGAIN`.
   2. Send the batch to the target (`ImportKeys`) and wait for its ack.
   3. Under the lock, delete the batch.
4. **Commit.** The map gives the slots to the target and the epoch goes up.
   - The target gets the new map first, so the node the source's `MOVED`
     points at is already serving.
   - Then the source, which starts answering `MOVED` for those slots.
   - Then every other node.

**Why no acknowledged write is lost.** For any key, exactly one node serves
writes at any moment:
- While the key is on the source and not in flight, the source serves it. The
  target never sees a client write for it, because clients only reach the
  target through `ASK`, and the source sends `ASK` only for keys it doesn't
  have.
- While the key is in flight, nobody accepts writes to it. The source returns
  `TRYAGAIN` and the target isn't reachable for it. So the copy is exact.
- The source deletes the key only after the target has acknowledged it.
  After that, the target serves it, reached through `ASK`.

Slot state lives in the same structure as the keys, under the same lock
(see "Store reorganised by slot"). So "which state is this slot in?" and
"apply this write" are one atomic step. A migration can't start between a
request's state check and its write.

**Subtle bugs found while designing and testing this.** They're recorded here
because they're good interview material.
- **The version-checked delete I first proposed loses data.** The idea: copy
  the key, then delete it on the source only if it hadn't changed. But
  suppose a client deletes the key after the copy. The version check fails,
  and the key is now absent on the source. The next `GET` gets `ASK`, goes to
  the target, and **reads the deleted value**. Fixing that would need
  tombstones on the target. Freezing keys while they're in flight removes the
  race entirely.
- **An import that arrives late overwrites a newer write.** An `ImportKeys`
  call that timed out may still be applied later. By then the source may have
  retried, deleted the key, and a client may have written a newer value on the
  target. Every batch carries `(migration_id, seq)`, with a new `seq` on every
  attempt. The target ignores any batch at or below the last one it applied
  for that slot. Covered by `SlotStateTest.DelayedImportBatchCannotOverwriteNewerWrites`.
- **Movers run one at a time per node** (a mutex in `MigrateSlots`). If the
  coordinator retries after a timeout while the first call is still running,
  two movers could otherwise send the same keys in an overlapping order.
- **An `ASK` briefly had nowhere to point** (found with memtier). On commit,
  the source used to clear its migration targets when it installed the new
  map, *before* the store left the MIGRATING state. A request in that gap got
  `TRYAGAIN Slot is being reconfigured`. Migration targets are now kept; a
  target is only consulted while the slot is migrating.

**Cost of this design.**
- A key in the current batch rejects writes for one `ImportKeys` round trip.
- Clients reach keys already moved to the target through `ASK`, which is an
  extra round trip until the step commits.
- Measured on the laptop (local cluster, `scripts/local_cluster.sh`, 3 nodes):
  - adding a 4th node moved 3,754 slots in **247 ms** with no load, and in
    **409 ms** while memtier was running;
  - removing a node took 236 ms with no load;
  - those runs held only a few dozen to a few hundred keys, so these are
    control-plane costs, not data-copy costs.

**How clients experienced it.**
- **memtier** (`--cluster-mode --retry-on-error`): throughput fell from about
  45k to under 2k ops/sec for about 3 s during the join, with 0 errors.
  - The migration itself had finished after 0.4 s.
  - memtier logged 32 topology refreshes for 17 map changes. It re-fetches
    `CLUSTER SLOTS` and reconnects after a `MOVED`, so most of the dip is
    memtier recovering on the client side.
  - A brief server-side slowdown isn't ruled out: my attempt to probe
    server latency during the join produced no data.
  - Fewer, larger steps (`--slots-per-step`) mean fewer epoch changes for
    clients to react to.
- **The test client** in `cluster_test.cpp` kept running at full speed through
  joins and leaves.

### The no-keys-lost test

`Cluster.NoAcknowledgedWriteIsLostWhileNodesJoinAndLeave`
([tests/integration/cluster_test.cpp](../tests/integration/cluster_test.cpp)):

- **Setup:** 3 nodes and a coordinator running in the test process, talking
  over real TCP and gRPC. Migration steps are small (64 slots), so there are
  many map changes during the run.
- **Load:** 4 writer threads with cluster-aware clients. Each owns 3,000 keys,
  so its expected state is exact. The mix is 60% SET, 20% DEL (checking the
  deleted count), and 20% GET, which must return the latest acknowledged
  value **during** the migration.
- **Topology changes under load:** add n4, then remove n2.
- **Checks afterwards:**
  - every acknowledged write reads back;
  - every deleted key stays deleted;
  - every live key is stored exactly once, on the owner in the final map;
  - the removed node holds nothing;
  - no slot is left migrating or importing.
- **Results:**
  - 10 consecutive runs (debug build) passed.
  - Each run did 124k–138k operations and hit every redirect type:
    about 5.2k `MOVED`, 170–220 `ASK`, and 60–115 `TRYAGAIN`.
  - It also passes under ASan and TSan.

`redis_cli_cluster_test.sh` does the same kind of end-to-end check with real
processes and `redis-cli -c`.

### Store reorganised by slot

A slot's keys have to be listed for migration, and a slot's state has to be
checked atomically with each operation. So the `Store` is now an array of
16,384 slots, each holding its own key map and its state. Lock stripe = `slot
% stripes`, so every key of a slot shares one lock. Standalone mode (no
`--cluster`) simply starts with every slot owned.
- **Cost:** a CRC16 computation per request.
- **Cost:** all the keys of one slot share a lock.

### ThreadSanitizer and an uninstrumented gRPC

Ubuntu's gRPC isn't built with TSan, so TSan can't see the synchronization
inside it. The first TSan run of the cluster tests reported about 880 races.
I grouped them by the frame where the racing access happens. Every one was
inside gRPC, protobuf, or gRPC's buffer handling, and none was in `src/`.

- **The fix:** `TSAN_OPTIONS=ignore_noninstrumented_modules=1` in the TSan
  test preset. TSan then ignores accesses made from inside uninstrumented
  libraries, and 0 reports remain.
- **Why not a suppressions file:** a blanket `race:grpc` would also have hidden
  real races in our own gRPC handlers, whose stacks always contain gRPC frames
  further down.
- **Checked:** I confirmed with a deliberately racy program that TSan still
  catches races in instrumented code in this mode.
- **What remains uncovered:** races *inside* gRPC itself.

### Test harness fix: ports

The shell-based cluster test failed about 1 run in 4 under TSan with
"Address already in use". It picked random ports in 20000–50000, which
overlaps Linux's ephemeral range (32768–60999). Outgoing connections from
earlier runs sometimes held the chosen port. Test ports are now kept below
32768. After the change, 15 consecutive runs under TSan passed.

### Known limitations (Phase 2)

- **The coordinator is a single point of failure and keeps its state in
  memory.** If it crashes mid-migration, slots stay MIGRATING/IMPORTING and
  there's no automatic resume. A failed `add-node` leaves the node in the map,
  and retrying returns `ALREADY_EXISTS`. Recovery is manual (Redis needs
  `redis-cli --cluster fix` in the same situation). Phase 3 discusses this.
- **A node that dies mid-migration stalls it.** In-flight keys on the source
  keep answering `TRYAGAIN` until the target is back. There's no abort path,
  on purpose: after the target has accepted `ASK` writes, rolling back would
  lose them. A dead node's data is lost until Phase 3 adds replicas.
- **Map pushes to nodes outside a migration are best effort.** A node that
  misses one keeps redirecting to the previous owner, which redirects again.
  The next successful push brings it up to date.
- **Starting each migration batch scans the slot**, which is fine at thousands
  of keys per slot but not at millions.
- **gRPC runs without TLS or authentication.** This assumes the cluster's
  private network (the Phase 4 VPC and security groups).
- **Multi-key DEL must stay within one slot** (`CROSSSLOT`), as in Redis Cluster.

### Phase 2 standalone check

Phase 2 puts slot hashing and the per-slot store layout on every request,
including in standalone mode. To check the cost, I re-ran the Phase 1
benchmark setup (same script, same pinning) with the Phase 2 build, and ran
Redis again in the same session as a control. Raw results are in
`bench/results/phase2-standalone-check/`.

Median of 3 runs of 20 s:

| Target | Pipeline | Ops/sec | p50 ms | p99 ms |
|---|---|---|---|---|
| Redis 7.0 | 1 | 67.0k [66.1k–67.3k] | 1.42 [1.40–1.45] | 3.06 [3.04–3.26] |
| DistKV, 1 thread | 1 | 74.2k [72.3k–75.1k] | 1.20 [1.19–1.25] | 3.39 [3.15–3.58] |
| Redis 7.0 | 16 | 670k [619k–693k] | 2.26 [2.19–2.40] | 4.86 [4.54–5.86] |
| DistKV, 1 thread | 16 | 552k [537k–605k] | 2.69 [2.45–2.75] | 6.50 [5.89–6.72] |

**Relative to Redis, DistKV lost ground.** It went from 1.27× to 1.11× of
Redis's throughput at pipeline 1, and from 1.00× to 0.82× at pipeline 16.

**A store microbenchmark** (not committed): single thread, 100k keys, the
same 1:10 mix, 5M operations, 3 rounds.
- Phase 1 store: 435–460 ns per operation.
- Phase 2 store: 499–540 ns per operation.
- Computing the slot (CRC16) alone: 34–52 ns.
- So the new store costs about 60–95 ns more per operation. About half of
  that is the CRC16 and the rest is the per-slot layout: 16,384 small hash
  tables instead of 256 large ones, which means more cache misses.

**What this does and doesn't explain.** At about 550k ops/sec, 70 ns is
roughly 4% of the time per request, so the store explains only part of the
drop. The rest isn't explained yet. The laptop is noisy (the Redis control
alone varied by 12% across runs), so this will be re-measured on dedicated
EC2 instances in Phase 5 before any conclusion.

**Cheap options if it holds up there:**
- a faster CRC16 (processing several bytes per step instead of one);
- keeping one hash table per stripe and adding a slot → keys index only for
  migrating slots.

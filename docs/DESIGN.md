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
  `redis-cli --cluster fix` in the same situation). *Still true after Phase 3
  for a coordinator crash; see Phase 3's weaknesses.*
- **A node that dies mid-migration stalls it.** In-flight keys on the source
  keep answering `TRYAGAIN` until the target is back. There's no abort path,
  on purpose: after the target has accepted `ASK` writes, rolling back would
  lose them. A dead node's data is lost until Phase 3 adds replicas.
  *Resolved in Phase 3: the backup is promoted with the migration state, and
  the reconciler finishes the rebalance.*
- **Map pushes to nodes outside a migration are best effort.** A node that
  misses one keeps redirecting to the previous owner, which redirects again.
  The next successful push brings it up to date. *Phase 3: the heartbeat
  monitor re-sends the map to any node that reports an older epoch.*
- *Phase 3 replaced `add-node`/`remove-node` with `add-group`/`remove-group`/
  `add-spare`; the Phase 2 text above describes the original single-node
  membership.*
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

---

## Phase 3: Replication and failover

### Architecture

```
                      distkv-coordinator
          (map: groups, slot owners, spares; heartbeats every 100 ms;
           failover; reconciler: spare assignment, backup sync, rebalance)
                  │ Ping / ApplyClusterMap / StartReplication
     ┌────────────┼──────────────────────────────┬───────────────┐
     ▼            ▼                              ▼               ▼
 ┌────────┐  Replicate (gRPC  ┌────────┐     ┌────────┐     ┌────────┐
 │ n1     │  bidi stream,     │ n2     │     │ n3 ... │     │ n5     │
 │primary │─────────────────► │ backup │     │        │     │ spare  │
 │ of g1  │ ◄───────────────  │ of g1  │     │        │     │        │
 └────────┘  cumulative acks  └────────┘     └────────┘     └────────┘
```

- **A group** is a primary plus a backup. The ring from Phase 2 now assigns
  slots to *groups* rather than nodes.
- **Primaries** serve clients. **Backups** serve no clients: they answer `MOVED`
  and apply the primary's stream.
- **Spares** wait to replace a failed backup.
- **New code:** [src/replication/](../src/replication/),
  the roles and the backup side of the stream in
  [src/cluster/node_service.cpp](../src/cluster/node_service.cpp),
  and failure handling in
  [src/coordinator/coordinator.cpp](../src/coordinator/coordinator.cpp).

### The write path

1. An event loop runs `SET k v`. Under the slot's lock, the Store:
   - checks that the node accepts writes (it's a primary with a synced backup);
   - applies the write;
   - appends `Put(k, v)` to the **replication log**, which assigns it a seq.
2. The handler sets `Session::reply_seq = seq`. The connection keeps the
   reply in its **held** queue. Later pipelined commands still run, but their
   replies queue behind the held one.
3. The **replicator** thread streams the log to the backup in order, over one
   gRPC bidirectional stream.
4. The backup applies each batch in order and acknowledges the highest seq it
   has applied ("everything up to N").
5. The ack advances the shared `Progress`. That wakes every event loop through
   its **mailbox** (an `eventfd`), and each loop releases the held replies
   that are now acknowledged, in request order.

Reads aren't replicated. They're served straight from the primary's memory.

### Decisions

**1. Where backups live: a dedicated backup node per group** *(chosen)*
- Alternative: chained backups, where each node backs up its neighbour's slots.
  That leaves no hardware idle, but each node holds two kinds of data, and
  rebalancing would also have to move backup data.
- Dedicated pairs are Redis Cluster's model and the simplest to reason about.
- **Cost:** half the nodes do no client work.
- For Phase 4, six nodes means three groups, with each primary and backup in
  different availability zones.

**2. Transport: one ordered gRPC stream per group, with cumulative acks** *(chosen)*
- Alternative: one unary RPC per write. That pays per-call overhead on every
  write, and the backup would have to reorder concurrent calls anyway.
- **How order is guaranteed:** the seq is assigned under the slot's lock at
  the moment the write is applied. So the log's order for any slot is the
  order its writes were applied, which is the only order that matters.
  Different slots hold different keys, so their writes can be applied in any
  order.
- **Cost:** a single log mutex is taken on every write, making it a
  serialisation point across event-loop threads. Its critical section is a
  queue append.

**3. Replies while the ack is pending: hold them in order, keep executing** *(chosen)*
- Alternative: pause the connection until the ack arrives. That makes a
  pipelined client pay a full round trip per write.
- **Cost:** a later reply on the same connection can't be sent before an
  earlier write's ack, even if it's a read.
- `PipelinedWritesKeepTheirReplyOrder` interleaves 500 SET/GET/PING triples
  and checks the order.

**4. Strict writes: no single-copy writes** *(chosen)*
- A primary without a *synced* backup refuses writes with
  `-NOREPLICAS Not enough good replicas to write`, as Redis does with
  `min-replicas-to-write`. Reads are still served.
- Alternative: degrade and let the primary acknowledge writes alone. That's
  more available, but a write acknowledged in that window is lost if the
  primary then dies, which contradicts the stated guarantee.
- **Consequence I had to build for: a spare pool.**
  - After a failover, the promoted node has no backup, so its group refuses
    writes until a new backup is synced.
  - Without automation that would last until an operator acted. So the
    coordinator keeps **spares** (`distkv-admin add-spare`), and its
    reconciler assigns one and runs the full sync.
  - Without a spare, the group stays read-only until one is added.

**5. Failure detection: the coordinator pings every node** *(chosen)*
- A node is declared dead after `failure_timeout` of failed pings: 100 ms
  interval, 1 s timeout, both configurable.
- Pings go from the coordinator to the nodes, so nodes need no coordinator
  address. Each reply carries the node's map epoch, and the coordinator
  re-sends the map to any node behind the current epoch. That fixes the Phase 2
  gap where best-effort map pushes could be missed.

**6. Split-brain protection: epoch fencing, no leases** *(chosen)*
- **The fence:** a backup applies a batch only if its *own* current map names
  the sender as its group's primary. Once promoted, the backup refuses its old
  primary (`FAILED_PRECONDITION`).
- **Why that's enough for writes:** every write needs the backup's ack, so
  after the promotion the old primary cannot acknowledge any more writes.
  - When it's refused, its replicator gives up: waiting writes fail with
    `NOREPLICAS`, it stops accepting writes, and it reports
    `replication_broken` so the coordinator re-syncs.
  - `PartitionedPrimaryCannotAcknowledgeWritesAfterPromotion` cuts a primary
    off from the coordinator only, waits for the promotion, then checks that
    writes to the old primary fail fast (3–5 ms after promotion, in 10 of 10
    runs).
- **Promotion is atomic with batch application:** the backup applies each
  batch while holding a shared role lock, and a map change takes the lock
  exclusively. A batch already in flight is therefore applied either entirely
  before the promotion or not at all.
- **Alternative: leases.** A primary would stop serving once its lease from the
  coordinator expired, and the coordinator would wait out the lease before
  promoting. That also stops **stale reads** on a cut-off primary, which this
  design allows (see weaknesses). It costs failover time and relies on bounded
  clock drift. I didn't build it, to keep scope down.
- **Ack timeout (2 s):** if the backup doesn't acknowledge a write in time, for
  example because the primary can reach the coordinator but not its backup,
  the replicator gives up the same way. So a client never waits indefinitely.

**7. Syncing a new backup: slot snapshots inside the replication stream** *(chosen)*
- **How it works:** `StartReplication` starts a new log, then for each of the
  16,384 slots takes the slot's lock and appends a `SlotSnapshot`. The backup
  replaces the slot's contents with the snapshot.
- **Why ordering is consistent:** every write to a slot gets its seq under the
  same lock. So each earlier write is included in the snapshot, and each later
  write comes after it in the stream.
- **Alternatives:**
  - stop the world: block writes during the whole copy;
  - keep a version per key and merge snapshots with the stream.
- **Windowing:** the sync waits for the backup's ack every 256 slots, so the
  log never holds the whole dataset at once.
- In practice, strict mode means no writes arrive during a sync anyway. The
  scheme is correct either way, and the tests don't depend on that.

### Migration + replication (Phase 2 meets Phase 3)

A migration now writes on two groups, and either primary can fail halfway.
Three rules keep it correct:

- **The target acknowledges an `ImportKeys` batch only after its backup has
  it.** The source deletes its copy as soon as the target says OK.
- **The source logs its deletes of a migrated batch, waits for its own
  backup's ack, and only then applies them.** Until then the keys stay frozen
  and clients aren't sent to the target for them. So a promoted source backup
  can never hold a stale copy of a key that clients have since changed on the
  target.
- **Slot migration state is replicated** (`SlotMigrating`, `SlotImporting` ops),
  so a promoted backup keeps sending `ASK` for keys already moved.
  - The source logs its "migrating" intent and waits for the backup before it
    starts redirecting.
  - Without this, a failover could leave the promoted source treating the slot
    as normally owned, while some of its keys already live on the target.

After a failover, the aborted rebalance leaves `needs_rebalance` set. The
coordinator's reconciler then:
- gives the group a spare and syncs it;
- recomputes the moves from the ring;
- re-arms each import under a **newer migration id**. A newer id replaces an
  older import, and batches from the old one are then rejected.

`Failover.PrimaryDiesDuringMigration` kills a source primary while a new group
joins under load. It passed 6 of 6 runs, and every acknowledged write was
intact. Where the kill lands varies:
- during the post-commit map push to the dead node: 5 runs;
- during `SetMigrating`: 1 run;
- while keys were in flight inside `MigrateSlots`: never observed.

That last case rests on the argument above and on the unit tests of the
log-then-apply steps
(`MigratedKeysAreDeletedOnlyAfterTheBackupHasTheDeletion`,
`MigrationIntentIsLoggedBeforeTheStateChanges`), not on an integration run.

### Bugs found along the way

- **Fencing was really done by the 2 s timeout.** The replicator called
  `TryCancel()` before `Finish()`, so the backup's `FAILED_PRECONDITION` came
  back as `CANCELLED`. The replicator then kept reconnecting until the 2 s ack
  timeout fired.
  - The fencing test still passed, because it only checked that no write was
    acknowledged. A log line gave it away.
  - Fixed by cancelling only when the replicator itself is leaving. The test
    now also asserts that fenced writes fail in under 1 s.
- **Use-after-free in a test helper,** found by ASan: it held a pointer into a
  temporary copy of the map.
- **Found while designing, before coding:**
  - A promoted source could forget a migration that was in progress. Fixed by
    replicating the slot states.
  - A write on a cut-off primary could hang forever waiting for an ack. Fixed
    by the ack timeout.
  - Strict writes needed automatic recovery. Fixed by the spare pool.

### Measurements (laptop, in-process tests, debug build)

These come from test runs, not a benchmark. The Phase 5 numbers on EC2 will
replace them.

- **Failover** (`Failover.PromotedBackupHasEveryAcknowledgedWrite`, 10 runs):
  - writes to the failed group resumed **1,170–1,360 ms** after the primary
    stopped;
  - failure detection alone took 1,000–1,090 ms (the 1 s timeout plus up to
    one heartbeat interval);
  - the rest is promotion, the map push, assigning the spare, and the spare's
    full sync, which took about 100 ms with a few thousand keys. The sync time
    grows with the data size.
- **Fencing:** writes to a cut-off old primary fail 3–5 ms after the promotion
  (10 runs; one run took 23 ms).
- **Cost of replication, in the Phase 2 migration test:** about 61k–64k
  operations per run, against 124k–138k before replication. It's the same test
  shape, but the test runs for a fixed number of operations around the
  topology changes, not for a fixed time. So this is only a rough sign that
  each write now waits for an extra network round trip, not a throughput
  figure.

### ThreadSanitizer: three failure tests excluded

Killing or cutting off a node makes gRPC connect to dead peers. That produced
8 new TSan reports.
- **Where they happen:** `scripts/tsan_classify.sh` groups every racing access
  by its first frame outside the standard library. All 16 accesses are inside
  `libgrpc.so` (its executor threads and `ChannelArgs`), and none is in `src/`.
- **Why they slip through the Phase 2 setting:** the `std::string` code
  involved is compiled into our instrumented binary but called by gRPC, so
  `ignore_noninstrumented_modules` doesn't cover it.
- **Why not a suppressions file:**
  - TSan matches suppressions against *every* frame, including the stacks where
    threads were created. gRPC creates its executor threads and the server
    threads that run our handlers the same way (`grpc_core::Thread`).
  - So any rule that silences these reports would also silence real races in
    our NodeService handlers.
- **What I did instead:** the TSan test preset excludes the three tests that
  inject failures: `Failover.*` and
  `Replication.WritesAreRefusedWhileTheBackupIsNotSynced`.
  - They still run under the debug and ASan builds.
  - Replication streaming, held replies, and migration with replication all
    stay covered under TSan.
- **The proper fix is a TSan build of gRPC,** which Decision 4a traded away
  for CI time.

### Weaknesses, and what Raft would fix

- **The coordinator is a single point of failure.**
  - While it's down, nothing fails over, no spare is assigned, and membership
    can't change. The data path keeps working.
  - Its state is in memory only, so a restart forgets the cluster.
  - *Raft fix:* replicate the coordinator itself (3 or 5 instances) and commit
    every map change through the Raft log. A new leader continues with the same
    map and the same epochs. etcd and ZooKeeper play this role in real systems.
- **Unnecessary failovers when only the coordinator is cut off.** A healthy
  primary that can't reach the coordinator still gets replaced.
  - Fencing makes this safe: no split-brain writes.
  - But it isn't free: a failover, a re-sync, and a window of refused writes.
- **Stale reads from a cut-off primary.**
  - Until it receives the new map or its replicator is refused, an old primary
    keeps serving reads. Writes are fenced, reads aren't.
  - *Fixes:* leases (Decision 6), or Raft's leader leases or ReadIndex for
    linearizable reads.
- **Dirty reads of unacknowledged writes.**
  - The primary applies a write before the backup acknowledges it, so another
    client can read a value whose write then fails. If the primary dies, that
    value disappears, and the client that wrote it got an error reply.
  - Redis has the same behaviour with `WAIT`. Fixing it would mean applying
    writes only after the ack, which is more complex and makes reads of your
    own writes slower.
- **One backup means one failure per group.**
  - Losing the primary before a new backup has synced loses the group:
    the coordinator logs `group LOST` and its slots are unavailable.
  - Any second failure within that window loses the group's data, since
    everything is in memory and nothing is persisted.
  - *Raft fix:* 3 replicas per shard, with a write committed once a majority
    has it. That survives one failure *without* write downtime: the remaining
    two are still a majority. Strict primary-backup has to stop writes until
    the backup is replaced.
- **Strict writes cost availability.** A group refuses writes from the moment
  its backup fails until a spare is synced. Measured: about 1.2–1.4 s of
  unavailability after a primary failure, most of it detection.
- **Failure detection is a fixed timeout.** 1 s trades failover speed against
  false positives on a slow network, which a flapping connection could
  trigger. Phi-accrual detectors or Raft's randomized election timeouts adapt
  better.
- **Failed writes are ambiguous.** A reply of "not acknowledged; may or may not
  have been applied", or a dropped connection, leaves the outcome unknown, as
  with any networked store. The workload checker in the tests tracks both
  possible states for such keys.
- **A backup that falls behind is re-synced in full**, not caught up from a log
  position. That's simple, but expensive for large datasets. Raft's log
  matching plus snapshots would catch up incrementally.

---

## Phase 4: AWS deployment

### Layout

```
 us-east-1, VPC 10.42.0.0/16, one public subnet in each of the first
 3 AZs that offer the instance types

   AZ 1                    AZ 2                    AZ 3
   n1  g1 primary          n2  g1 backup           n4  g2 backup
   n6  g3 backup           n3  g2 primary          n5  g3 primary
   n7  spare
   coordinator
   loadgen
```

- **Placement rule** (`local.nodes` in
  [deploy/terraform/main.tf](../deploy/terraform/main.tf)): group *g*'s
  primary goes in AZ *g* mod 3 and its backup in the next AZ.
  - So every group survives the loss of any one AZ.
  - The three primaries are spread over all three AZs.
  - The spare goes to the next AZ in rotation.
- **Instances:**
  - 6 storage nodes (3 groups) and 1 spare, on `c7g.medium`;
  - the coordinator, on `c7g.medium`;
  - the load generator, on `c7g.xlarge`. It also builds the binaries.

### Decisions

| Decision | Chosen | Alternatives and why not |
|---|---|---|
| Layout | 3 groups + 1 spare + a dedicated coordinator + a load generator (9 instances) | Coordinator on the load generator: saves one instance, but a saturated load generator would delay heartbeats and could trigger false failovers during benchmarks. 2 groups: cheaper, but a less representative cluster. |
| Instance types | `c7g` (Graviton, fixed performance) | Burstable `t3`/`t4g`: they run out of CPU credits mid-benchmark, so results would depend on the credit balance. `c6i` (x86): about 2× the price per node. |
| Getting binaries onto instances | Build once on the load generator (same OS and CPU architecture as the nodes), copy over the private network | Cross-building for arm64 on the laptop needs QEMU emulation (slow with gRPC). A container registry adds ECR, Docker on every node and more cost. |
| Network | Public subnets; SSH only from the operator's IP (detected automatically); every other port only within the security group | Private subnets need a NAT gateway (hourly cost plus data) or VPC endpoints for SSM, just so instances can run `apt`. |
| Operations | Bash scripts over SSH, using an inventory JSON written by Terraform. Processes are started detached and **never restarted automatically**. | Ansible: more structure, but another tool to explain. systemd with auto-restart would bring killed nodes back, which defeats the failure tests. |

### Guards against surprise bills

1. **One-command teardown:** `deploy/scripts/teardown.sh` runs
   `terraform destroy`, then asks the AWS API whether any instance or VPC
   tagged `Project=distkv` is left, and fails loudly if so.
2. **Maximum lifetime:** every instance runs `shutdown -h +240` at boot and is
   set to *terminate* on shutdown. A forgotten cluster deletes itself after
   4 hours (`max_lifetime_hours`). What's left is the VPC, subnets and security
   group, which cost nothing, and the key pair; a later `teardown.sh` removes
   them.
3. **No NAT gateway, no load balancer, no Elastic IPs.**
4. **Recommended:** an AWS Budgets alert on the account.

**Estimated cost:** roughly $0.50 per hour for the whole cluster, from AWS's
published us-east-1 on-demand prices as I know them. Check against the
pricing page before relying on them:
- 8 × `c7g.medium` at about $0.036/h;
- 1 × `c7g.xlarge` at about $0.145/h;
- 9 public IPv4 addresses at $0.005/h each;
- about 150 GB of gp3 storage.

**Traffic between AZs costs about $0.01/GB in each direction.** Replication
and benchmark traffic cross AZs, so a long, high-rate benchmark can cost more
in transfer than in instances. Phase 5 will record the traffic actually
generated.

### Credentials and keys

- **AWS credentials** come from the operator's own `aws configure`.
  `scripts/dev.ps1` mounts `%USERPROFILE%\.aws` into the container, and they
  never enter the repository.
- **The SSH key** is generated by Terraform for this cluster only
  (`tls_private_key`).
  - It's written to `deploy/.generated/` (git-ignored) and is also held in
    the local Terraform state (`*.tfstate`, git-ignored).
  - The deploy script copies it to the load generator so it can distribute
    binaries over private IPs.
  - It grants access to these throwaway instances only.

### Verified without spending money

- `terraform validate`.
- `terraform test`
  ([deploy/terraform/tests/layout.tftest.hcl](../deploy/terraform/tests/layout.tftest.hcl))
  plans the configuration against **mocked AWS, HTTP, TLS and local
  providers** and asserts that:
  - a group's primary and backup are in different AZs;
  - the primaries cover three AZs;
  - SSH is limited to the operator's /32 and nothing is open to
    `0.0.0.0/0`;
  - every instance terminates on shutdown;
  - node counts follow the variables.
- **The boot script** is rendered for both roles and checked with `bash -n`.
- **shellcheck** is clean on every script.
- **CI** runs all of the above in an `infra` job.

### Local rehearsal of the deploy scripts

[deploy/local/rehearse.sh](../deploy/local/rehearse.sh) runs 9 Docker
containers as stand-ins for the EC2 instances, on a private network with the
same layout, roles and fake AZ labels. Each runs an SSH server with the
`ubuntu` user and has the directories the boot script creates. A generated
inventory points the **unmodified** deploy scripts at them;
`DISTKV_GEN_DIR` keeps it apart from a real deployment's inventory.

**Rehearsal run (all passed):**
- `deploy.sh` built on the "load generator" and distributed the binaries to 8
  hosts, in 37 s.
- `start.sh` formed 3 groups and added the spare. Every backup synced.
- From the load generator:
  - `redis-cli -c` wrote 300 keys;
  - memtier `--cluster-mode` discovered 3 primaries and ran without errors.
- `kill-node.sh n1` (SIGKILL):
  - g1's backup n2 was promoted and the spare n7 synced as its new backup;
  - all 300 keys were readable afterwards;
  - a new write succeeded.
- `status.sh` and `stop.sh` behaved as expected.

**Bugs this caught before any money was spent:**
- **In the rehearsal script itself:** `GROUPS` is a reserved bash variable
  (the user's group ids), so `GROUPS=3` was silently ignored. The first run
  started 237 containers before running out of addresses. They were all
  removed, and the variable renamed.
- **Cosmetic:** `distkv-admin show` had an unlabelled column.

**What the rehearsal cannot cover:** Terraform itself, the boot script's
package installs, the arm64 build, real AZs and network latency, and
instance termination. Those are verified on the first real `up.sh` run, which
is billable and needs the operator's go-ahead.

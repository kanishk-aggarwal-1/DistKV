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
  line.
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

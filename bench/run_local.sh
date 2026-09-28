#!/usr/bin/env bash
# Phase 1 local benchmark: DistKV vs Redis on the same machine, driven by
# memtier_benchmark. Servers and load generator are pinned to disjoint CPUs
# so they do not compete for cores.
#
# Usage (from the repo root, inside the dev container):
#   cmake --preset release && cmake --build --preset release
#   bench/run_local.sh
set -euo pipefail

SERVER_BIN=${SERVER_BIN:-build/release/distkv-server}
OUT=${OUT:-bench/results/local-$(date -u +%Y%m%dT%H%M%SZ)}
SERVER_CPUS=${SERVER_CPUS:-0-3}
CLIENT_CPUS=${CLIENT_CPUS:-4-7}
DURATION=${DURATION:-20}     # seconds per run
REPEATS=${REPEATS:-3}
KEYS=${KEYS:-100000}
PIPELINES=${PIPELINES:-"1 16"}
TARGETS=${TARGETS:-"redis distkv-1t distkv-4t"}
PORT=6390

mkdir -p "$OUT"

{
  echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_rev: $(git describe --always --dirty 2>/dev/null || echo unknown)"
  echo "kernel: $(uname -r)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)"
  echo "nproc: $(nproc)"
  echo "server_cpus: $SERVER_CPUS"
  echo "client_cpus: $CLIENT_CPUS"
  echo "redis: $(redis-server --version)"
  echo "memtier: $(memtier_benchmark --version | head -1)"
  echo "memtier_args: -t 4 -c 25 --ratio=1:10 --data-size=32 --key-pattern=R:R --key-maximum=$KEYS --test-time=$DURATION"
} > "$OUT/environment.txt"

start_target() {
  case "$1" in
    redis)      taskset -c "$SERVER_CPUS" redis-server --port "$PORT" --save "" --appendonly no > "$OUT/$1.log" 2>&1 & ;;
    distkv-1t) taskset -c "$SERVER_CPUS" "$SERVER_BIN" --port "$PORT" --threads 1 > "$OUT/$1.log" 2>&1 & ;;
    distkv-4t) taskset -c "$SERVER_CPUS" "$SERVER_BIN" --port "$PORT" --threads 4 > "$OUT/$1.log" 2>&1 & ;;
    *) echo "unknown target $1" >&2; exit 1 ;;
  esac
  SERVER_PID=$!
  for _ in $(seq 50); do
    [[ "$(redis-cli -p "$PORT" PING 2>/dev/null)" == "PONG" ]] && return
    sleep 0.1
  done
  echo "$1 did not start" >&2
  exit 1
}

stop_target() {
  kill "$SERVER_PID"
  wait "$SERVER_PID" 2>/dev/null || true
}

memtier() {
  taskset -c "$CLIENT_CPUS" memtier_benchmark -s 127.0.0.1 -p "$PORT" --protocol=redis \
    --data-size=32 --key-maximum="$KEYS" --hide-histogram "$@"
}

for target in $TARGETS; do
  echo "=== $target ==="
  start_target "$target"
  # Prefill every key once so GETs hit.
  memtier -t 1 -c 1 --ratio=1:0 --key-pattern=P:P --requests=allkeys > /dev/null 2>&1
  for pipeline in $PIPELINES; do
    for run in $(seq "$REPEATS"); do
      name="$target-p$pipeline-r$run"
      echo "  $name"
      memtier -t 4 -c 25 --ratio=1:10 --key-pattern=R:R --pipeline="$pipeline" \
        --test-time="$DURATION" --distinct-client-seed \
        --json-out-file="$OUT/$name.json" > "$OUT/$name.txt" 2>&1
    done
  done
  stop_target
done

# Summary from memtier's "Totals" row:
# Type Ops/sec Hits/sec Misses/sec Avg.Latency p50 p99 p99.9 KB/sec
# The average is left out: in test runs memtier occasionally reported an
# average far above p99.9 (e.g. 171 ms vs 3.4 ms, against Redis too), which
# cannot be right for the run, so only percentiles are summarised.
echo "target,pipeline,run,ops_per_sec,p50_ms,p99_ms,p999_ms" > "$OUT/summary.csv"
for f in "$OUT"/*-p*-r*.txt; do
  base=$(basename "$f" .txt)
  target=${base%-p*}
  rest=${base##*-p}
  pipeline=${rest%-r*}
  run=${rest##*-r}
  awk -v t="$target" -v p="$pipeline" -v r="$run" \
    '$1 == "Totals" { printf "%s,%s,%s,%s,%s,%s,%s\n", t, p, r, $2, $6, $7, $8 }' "$f" \
    >> "$OUT/summary.csv"
done
echo "results in $OUT"
cat "$OUT/summary.csv"

#!/usr/bin/env bash
# Phase 5 benchmark: DistKV vs Redis on identical instances, driven by
# memtier_benchmark on the load generator. Uses the same inventory as the
# deploy scripts, so it also runs against the local rehearsal cluster.
#
#   bench/aws/run_benchmarks.sh [TARGET...]
#
# Targets (default: all four, in this order):
#   redis-single     one Redis on n1, persistence off
#   distkv-single    one standalone DistKV on n1
#   redis-cluster    Redis Cluster: primaries on the group primaries' hosts,
#                    one replica each on the group backups' hosts
#   distkv-cluster   the DistKV cluster (deploy/scripts/start.sh layout)
#
# Environment: OUT (results directory), DURATION (s per run, default 60),
#              REPEATS (default 3), PIPELINES (default "1 16"), KEYS (100000).
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/../../deploy/scripts/common.sh"
require_inventory

TARGETS=("$@")
(( ${#TARGETS[@]} )) || TARGETS=(redis-single distkv-single redis-cluster distkv-cluster)
DURATION=${DURATION:-60}
REPEATS=${REPEATS:-3}
PIPELINES=${PIPELINES:-"1 16"}
KEYS=${KEYS:-100000}
OUT=${OUT:-$REPO_DIR/bench/results/aws-$(date -u +%Y%m%dT%H%M%SZ)}
REDIS_PORT=6379

LOADGEN=$(public_ip loadgen)
mkdir -p "$OUT"

primaries() { inv '.nodes[] | select(.role == "primary") | .id'; }
backup_of_group() { inv ".nodes[] | select(.group == $1 and .role == \"backup\") | .id"; }
group_nodes() { inv '.nodes[] | select(.role != "spare") | .id'; }

# ---- Environment record ----------------------------------------------------------
{
  echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_rev: $(git -C "$REPO_DIR" describe --always --dirty 2> /dev/null || echo unknown)"
  echo "region: $(inv .region)"
  echo "node_instance_type: $(inv .node_instance_type)"
  echo "loadgen_instance_type: $(inv .loadgen_instance_type)"
  echo "node_cpus: $(remote "$(public_ip n1)" nproc)"
  echo "loadgen_cpus: $(remote "$LOADGEN" nproc)"
  echo "kernel: $(remote "$(public_ip n1)" uname -r)"
  echo "redis: $(remote "$(public_ip n1)" redis-server --version)"
  echo "memtier: $(remote "$LOADGEN" memtier_benchmark --version | head -1)"
  echo "memtier_args: -t 4 -c 25 --ratio=1:10 --data-size=32 --key-pattern=R:R --key-maximum=$KEYS --test-time=$DURATION --distinct-client-seed"
  echo "placement:"
  inv '.nodes[] | "  \(.id) \(.role) group=\(.group) az=\(.az)"'
} > "$OUT/environment.txt"

# ---- Server control ----------------------------------------------------------------

stop_everything() {
  bash "$REPO_DIR/deploy/scripts/stop.sh" > /dev/null
  for id in $(inv '.nodes[].id'); do
    remote "$(public_ip "$id")" "redis-cli -p $REDIS_PORT shutdown nosave > /dev/null 2>&1 || true;
      rm -f $REMOTE_DIR/nodes-$REDIS_PORT.conf $REMOTE_DIR/dump.rdb"
  done
}

start_redis() {  # start_redis NODE_ID [cluster]
  local extra=""
  [[ ${2:-} == cluster ]] && extra="--cluster-enabled yes --cluster-config-file nodes-$REDIS_PORT.conf"
  remote "$(public_ip "$1")" "cd $REMOTE_DIR && redis-server --port $REDIS_PORT --bind 0.0.0.0 \
    --protected-mode no --save '' --appendonly no --daemonize yes --dir $REMOTE_DIR \
    --pidfile $REMOTE_DIR/redis.pid --logfile $REMOTE_DIR/logs/redis.log $extra"
  for _ in $(seq 50); do
    remote "$(public_ip "$1")" redis-cli -p "$REDIS_PORT" PING > /dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "redis on $1 did not start" >&2
  exit 1
}

form_redis_cluster() {
  local masters=() p
  for p in $(primaries); do masters+=("$(private_ip "$p"):$REDIS_PORT"); done
  remote "$LOADGEN" redis-cli --cluster create "${masters[@]}" --cluster-yes > /dev/null
  # One replica per primary, on the same host as that group's DistKV backup,
  # so both systems replicate across the same AZ pairs.
  local g=0 id
  for p in $(primaries); do
    g=$((g + 1))
    id=$(remote "$LOADGEN" redis-cli -h "$(private_ip "$p")" -p "$REDIS_PORT" CLUSTER MYID)
    remote "$LOADGEN" redis-cli --cluster add-node "$(private_ip "$(backup_of_group $g)"):$REDIS_PORT" \
      "$(private_ip "$p"):$REDIS_PORT" --cluster-slave --cluster-master-id "$id" > /dev/null
  done
  for _ in $(seq 100); do
    if remote "$LOADGEN" "redis-cli -h $(private_ip n1) -p $REDIS_PORT CLUSTER INFO | grep -q cluster_state:ok" &&
       [[ $(remote "$LOADGEN" "redis-cli -h $(private_ip n1) -p $REDIS_PORT CLUSTER NODES | grep -c slave") -eq $(primaries | wc -l) ]]; then
      return 0
    fi
    sleep 0.5
  done
  echo "Redis Cluster did not become healthy" >&2
  exit 1
}

# ---- memtier ------------------------------------------------------------------------

memtier() {  # memtier HOST PORT CLUSTER_FLAG ARGS...
  local host=$1 port=$2 cluster=$3
  shift 3
  # $cluster is either empty or --cluster-mode; unquoted so empty vanishes.
  # shellcheck disable=SC2086
  remote "$LOADGEN" memtier_benchmark -s "$host" -p "$port" $cluster --protocol=redis \
    --data-size=32 --key-maximum="$KEYS" --hide-histogram "$@"
}

run_target() {  # run_target NAME HOST PORT CLUSTER_FLAG
  local name=$1 host=$2 port=$3 cluster=$4 pipeline run
  echo "== $name"
  # Prefill every key once so GETs hit.
  memtier "$host" "$port" "$cluster" -t 1 -c 1 --ratio=1:0 --key-pattern=P:P --requests=allkeys > /dev/null 2>&1
  remote "$LOADGEN" "mkdir -p $REMOTE_DIR/results && rm -f $REMOTE_DIR/results/$name-*"
  for pipeline in $PIPELINES; do
    for run in $(seq "$REPEATS"); do
      echo "   pipeline $pipeline, run $run"
      memtier "$host" "$port" "$cluster" -t 4 -c 25 --ratio=1:10 --key-pattern=R:R \
        --pipeline="$pipeline" --test-time="$DURATION" --distinct-client-seed \
        --json-out-file="$REMOTE_DIR/results/$name-p$pipeline-r$run.json" \
        > "$OUT/$name-p$pipeline-r$run.txt" 2>&1
    done
  done
  scp "${ssh_opts[@]}" -i "$SSH_KEY" -q "ubuntu@$LOADGEN:$REMOTE_DIR/results/$name-*.json" "$OUT/"
}

# ---- Main ------------------------------------------------------------------------------

stop_everything
for target in "${TARGETS[@]}"; do
  case "$target" in
    redis-single)
      start_redis n1
      run_target redis-single "$(private_ip n1)" "$REDIS_PORT" ""
      ;;
    distkv-single)
      start_process "$(public_ip n1)" n1 "$REMOTE_DIR/bin/distkv-server --port $CLIENT_PORT"
      sleep 1
      run_target distkv-single "$(private_ip n1)" "$CLIENT_PORT" ""
      ;;
    redis-cluster)
      for id in $(group_nodes); do start_redis "$id" cluster; done
      form_redis_cluster
      run_target redis-cluster "$(private_ip n1)" "$REDIS_PORT" --cluster-mode
      ;;
    distkv-cluster)
      bash "$REPO_DIR/deploy/scripts/start.sh" > "$OUT/distkv-cluster-start.txt"
      run_target distkv-cluster "$(private_ip n1)" "$CLIENT_PORT" --cluster-mode
      ;;
    *)
      echo "unknown target $target" >&2
      exit 2
      ;;
  esac
  stop_everything
done

python3 "$REPO_DIR/bench/summarize.py" "$OUT"
echo "results in $OUT"

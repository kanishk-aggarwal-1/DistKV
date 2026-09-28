#!/usr/bin/env bash
# Runs a local DistKV cluster on 127.0.0.1: one coordinator plus replication
# groups (a primary and a backup each) and optional spares.
#
#   scripts/local_cluster.sh start [G] [S]   coordinator, G groups (default 3), S spares (default 1)
#   scripts/local_cluster.sh add-group       start two more nodes and add them as a group
#   scripts/local_cluster.sh add-spare       start one more node as a spare
#   scripts/local_cluster.sh remove-group ID migrate group ID's slots away, then stop its nodes
#   scripts/local_cluster.sh kill I          SIGKILL node I (failure testing)
#   scripts/local_cluster.sh show            print the cluster map
#   scripts/local_cluster.sh stop            stop everything
#
# Nodes are numbered 1, 2, 3, ... in start order; group g gets nodes 2g-1
# (primary) and 2g (backup). Node I listens for clients on BASE_PORT+I and
# for gRPC on BASE_PORT+I+10000.
# Environment: BIN_DIR (default build/release), BASE_PORT (default 7000),
#              RUN_DIR (default /tmp/distkv-cluster), THREADS (default 2).
set -euo pipefail

BIN_DIR=${BIN_DIR:-build/release}
BASE_PORT=${BASE_PORT:-7000}
RUN_DIR=${RUN_DIR:-/tmp/distkv-cluster}
THREADS=${THREADS:-2}
COORD_PORT=$((BASE_PORT + 2000))
COORD="127.0.0.1:$COORD_PORT"

admin() { "$BIN_DIR/distkv-admin" --coordinator "$COORD" "$@"; }

wait_until() {  # wait_until DESCRIPTION PID COMMAND...
  local what=$1 pid=$2; shift 2
  for _ in $(seq 100); do
    if "$@" > /dev/null 2>&1; then return 0; fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "$what exited during startup (see $RUN_DIR)" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "timed out waiting for $what" >&2
  exit 1
}

next_node() {  # prints the next unused node number
  local i=1
  while [[ -f "$RUN_DIR/n$i.pid" || -f "$RUN_DIR/n$i.log" ]]; do i=$((i + 1)); done
  echo "$i"
}

start_node() {  # start_node I: starts node I and waits until it answers
  local i=$1 port=$((BASE_PORT + $1))
  "$BIN_DIR/distkv-server" --cluster --node-id "n$i" --port "$port" \
    --grpc-port $((port + 10000)) --threads "$THREADS" > "$RUN_DIR/n$i.log" 2>&1 &
  echo $! > "$RUN_DIR/n$i.pid"
  wait_until "node n$i" "$!" redis-cli -p "$port" PING
}

grpc_addr() { echo "127.0.0.1:$((BASE_PORT + $1 + 10000))"; }

add_group() {
  local p b
  p=$(next_node); start_node "$p"
  b=$(next_node); start_node "$b"
  admin add-group "$(grpc_addr "$p")" "$(grpc_addr "$b")"
}

add_spare() {
  local s
  s=$(next_node); start_node "$s"
  admin add-spare "$(grpc_addr "$s")"
}

stop_pid_file() {
  local file=$1 signal=${2:-TERM}
  [[ -f "$file" ]] || return 0
  local pid
  pid=$(cat "$file")
  kill "-$signal" "$pid" 2>/dev/null || true
  # Wait for the process to exit so ports are free for the next run.
  for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  rm -f "$file"
}

case "${1:-}" in
  start)
    groups=${2:-3}
    spares=${3:-1}
    mkdir -p "$RUN_DIR"
    rm -f "$RUN_DIR"/*.log
    "$BIN_DIR/distkv-coordinator" --port "$COORD_PORT" > "$RUN_DIR/coordinator.log" 2>&1 &
    echo $! > "$RUN_DIR/coordinator.pid"
    wait_until "coordinator" "$!" admin show
    for _ in $(seq "$groups"); do add_group > /dev/null; done
    for _ in $(seq "$spares"); do add_spare > /dev/null; done
    admin show
    ;;
  add-group)
    add_group
    ;;
  add-spare)
    add_spare
    ;;
  remove-group)
    group=${2:?group id}
    members=$(admin show | awk -v g="$group" '$1 == g { print $3, $4 }')
    admin remove-group "$group"
    for id in $members; do stop_pid_file "$RUN_DIR/$id.pid"; done
    ;;
  kill)
    stop_pid_file "$RUN_DIR/n${2:?node number}.pid" KILL
    ;;
  show)
    admin show
    ;;
  stop)
    stop_pid_file "$RUN_DIR/coordinator.pid"
    for file in "$RUN_DIR"/n*.pid; do stop_pid_file "$file"; done
    ;;
  *)
    sed -n '2,16p' "$0"
    exit 2
    ;;
esac

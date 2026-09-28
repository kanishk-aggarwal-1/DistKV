#!/usr/bin/env bash
# Runs a local DistKV cluster on 127.0.0.1: one coordinator plus N nodes.
#
#   scripts/local_cluster.sh start [N]     start a coordinator and nodes 1..N (default 3)
#   scripts/local_cluster.sh add I         start node I and add it to the cluster
#   scripts/local_cluster.sh remove I      migrate node I's slots away, then stop it
#   scripts/local_cluster.sh show          print the cluster map
#   scripts/local_cluster.sh stop          stop everything
#
# Node I listens for clients on BASE_PORT+I and for gRPC on BASE_PORT+I+10000.
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

start_node() {
  local i=$1 port=$((BASE_PORT + $1))
  "$BIN_DIR/distkv-server" --cluster --node-id "n$i" --port "$port" \
    --grpc-port $((port + 10000)) --threads "$THREADS" > "$RUN_DIR/n$i.log" 2>&1 &
  echo $! > "$RUN_DIR/n$i.pid"
  wait_until "node n$i" "$!" redis-cli -p "$port" PING
  admin add-node "127.0.0.1:$((port + 10000))"
}

stop_pid_file() {
  local file=$1
  [[ -f "$file" ]] || return 0
  local pid
  pid=$(cat "$file")
  kill "$pid" 2>/dev/null || true
  # Wait for the process to exit so ports are free for the next run.
  for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  rm -f "$file"
}

case "${1:-}" in
  start)
    count=${2:-3}
    mkdir -p "$RUN_DIR"
    "$BIN_DIR/distkv-coordinator" --port "$COORD_PORT" > "$RUN_DIR/coordinator.log" 2>&1 &
    echo $! > "$RUN_DIR/coordinator.pid"
    wait_until "coordinator" "$!" admin show
    for i in $(seq "$count"); do start_node "$i"; done
    admin show
    ;;
  add)
    start_node "${2:?node number}"
    ;;
  remove)
    admin remove-node "n${2:?node number}"
    stop_pid_file "$RUN_DIR/n$2.pid"
    ;;
  show)
    admin show
    ;;
  stop)
    for file in "$RUN_DIR"/n*.pid; do stop_pid_file "$file"; done
    stop_pid_file "$RUN_DIR/coordinator.pid"
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac

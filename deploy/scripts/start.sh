#!/usr/bin/env bash
# Starts the coordinator and every node, then forms the cluster: groups as
# laid out by Terraform (primary and backup in different AZs) plus spares.
#
#   deploy/scripts/start.sh
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

# start_process HOST NAME COMMAND: runs COMMAND detached on HOST, with its log
# in ~/distkv/logs/NAME.log and its pid in ~/distkv/NAME.pid. No automatic
# restart: a killed process stays dead, as the failure tests require.
start_process() {
  local ip=$1 name=$2 cmd=$3
  remote "$ip" "cd $REMOTE_DIR && if [ -f $name.pid ] && kill -0 \$(cat $name.pid) 2>/dev/null; then
      echo '   $name already running'; exit 0; fi
    nohup setsid $cmd > logs/$name.log 2>&1 < /dev/null & echo \$! > $name.pid"
}

echo "== coordinator"
start_process "$(public_ip coordinator)" coordinator \
  "$REMOTE_DIR/bin/distkv-coordinator --port $COORD_PORT"

echo "== nodes"
for id in $(node_ids); do
  start_process "$(public_ip "$id")" "$id" \
    "$REMOTE_DIR/bin/distkv-server --cluster --node-id $id --port $CLIENT_PORT \
     --grpc-port $NODE_GRPC_PORT --advertise-host $(private_ip "$id")"
done
for id in $(node_ids); do
  ip=$(public_ip "$id")
  for attempt in $(seq 50); do
    if remote "$ip" redis-cli -p "$CLIENT_PORT" PING > /dev/null 2>&1; then break; fi
    if (( attempt == 50 )); then
      echo "node $id did not start; log:" >&2
      remote "$ip" tail -20 "$REMOTE_DIR/logs/$id.log" >&2
      exit 1
    fi
    sleep 0.2
  done
done

echo "== forming the cluster"
groups=$(inv '.groups')
for g in $(seq "$groups"); do
  primary=$(inv ".nodes[] | select(.group == $g and .role == \"primary\") | .id")
  backup=$(inv ".nodes[] | select(.group == $g and .role == \"backup\") | .id")
  echo "   group $g: $primary (primary) + $backup (backup)"
  admin add-group "$(private_ip "$primary"):$NODE_GRPC_PORT" "$(private_ip "$backup"):$NODE_GRPC_PORT"
done
for id in $(inv '.nodes[] | select(.role == "spare") | .id'); do
  echo "   spare $id"
  admin add-spare "$(private_ip "$id"):$NODE_GRPC_PORT"
done

echo
admin show

#!/usr/bin/env bash
# End-to-end cluster test with real processes and the real redis-cli in
# cluster mode (-c, which follows MOVED and ASK redirects): groups join and
# leave, and a primary is killed with SIGKILL.
# Usage: redis_cli_cluster_test.sh <directory containing the distkv binaries>
set -euo pipefail

export BIN_DIR=${1:?usage: $0 <bin dir>}
# Client ports 10000-11999, coordinator +2000, gRPC +10000: all below Linux's
# ephemeral range (32768+), so outgoing connections can never be holding them.
export BASE_PORT=$((10000 + RANDOM % 1900))
export RUN_DIR
RUN_DIR=$(mktemp -d)
CLUSTER="$(dirname "$0")/../../scripts/local_cluster.sh"
cleanup() {
  local status=$?
  if (( status != 0 )); then
    for log in "$RUN_DIR"/*.log; do echo "== $log"; tail -20 "$log"; done
  fi
  bash "$CLUSTER" stop
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

failures=0
check() {  # check DESCRIPTION EXPECTED ACTUAL
  if [[ "$2" == "$3" ]]; then
    echo "ok:   $1"
  else
    echo "FAIL: $1: expected [$2] got [$3]"
    failures=$((failures + 1))
  fi
}

port() { echo $((BASE_PORT + $1)); }
cli() { local node=$1; shift; redis-cli -c -p "$(port "$node")" "$@"; }

# Nodes 1+2 = group g1, 3+4 = g2, 5 = spare.
bash "$CLUSTER" start 2 1 > /dev/null

KEYS=500
for i in $(seq $KEYS); do cli 1 SET "key:$i" "value:$i" > /dev/null; done

all_readable() {  # all_readable NODE: number of keys NOT readable through NODE
  local bad=0
  for i in $(seq $KEYS); do
    [[ "$(cli "$1" GET "key:$i")" == "value:$i" ]] || bad=$((bad + 1))
  done
  echo "$bad"
}

check "all keys readable via node 3" 0 "$(all_readable 3)"

# Raw (non -c) clients: every node except the owner's primary answers MOVED,
# backups and spares included.
moved=0
for n in 1 2 3 4 5; do
  if redis-cli -p "$(port $n)" GET foo | grep -q '^MOVED 12182 127.0.0.1:'; then moved=$((moved + 1)); fi
done
check "4 of 5 nodes redirect 'foo' with MOVED 12182" 4 "$moved"

check "CLUSTER SLOTS covers all 16384 slots" 16384 \
  "$(redis-cli -p "$(port 1)" CLUSTER SLOTS | awk 'NR % 5 == 1 { start = $1 } NR % 5 == 2 { total += $1 - start + 1 } END { print total }')"

bash "$CLUSTER" add-group > /dev/null   # nodes 6+7 = g3
check "all keys readable after group g3 joined" 0 "$(all_readable 6)"

bash "$CLUSTER" remove-group g1 > /dev/null
check "all keys readable after group g1 left" 0 "$(all_readable 3)"
check "g1 is gone from the map" "" "$(bash "$CLUSTER" show | awk '$1 == "g1"')"

# Failover: SIGKILL the primary of g2 (node 3). Its backup (node 4) must take
# over with every key, and the spare (node 5) must become the new backup.
bash "$CLUSTER" kill 3
promoted=""
for _ in $(seq 100); do
  if bash "$CLUSTER" show | awk '$1 == "g2" && $3 == "n4" && $4 == "n5" && $5 == "ready"' | grep -q .; then
    promoted=yes
    break
  fi
  sleep 0.1
done
check "g2: backup n4 promoted, spare n5 synced as new backup" yes "$promoted"
check "all keys readable after the primary was killed" 0 "$(all_readable 4)"
check "writes succeed after failover" OK "$(cli 4 SET after-failover 1)"

check "DEL through a redirect" 1 "$(cli 6 DEL key:1)"
check "deleted key stays deleted" "" "$(cli 4 GET key:1)"

if (( failures > 0 )); then
  echo "$failures failure(s)"
  exit 1
fi
echo "all redis-cli cluster checks passed"

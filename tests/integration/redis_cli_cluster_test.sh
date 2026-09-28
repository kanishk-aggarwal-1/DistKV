#!/usr/bin/env bash
# End-to-end cluster test with real processes and the real redis-cli in
# cluster mode (-c, which follows MOVED and ASK redirects).
# Usage: redis_cli_cluster_test.sh <directory containing the distkv binaries>
set -euo pipefail

export BIN_DIR=${1:?usage: $0 <bin dir>}
# Client ports 10000-11999, coordinator +2000, gRPC +10000: all below Linux's
# ephemeral range (32768+), so outgoing connections can never be holding them.
export BASE_PORT=$((10000 + RANDOM % 2000))
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

bash "$CLUSTER" start 3 > /dev/null

KEYS=500
for i in $(seq $KEYS); do cli 1 SET "key:$i" "value:$i" > /dev/null; done

all_readable() {  # all_readable NODE: every key readable through NODE
  local bad=0
  for i in $(seq $KEYS); do
    [[ "$(cli "$1" GET "key:$i")" == "value:$i" ]] || bad=$((bad + 1))
  done
  echo "$bad"
}

check "all keys readable via node 2" 0 "$(all_readable 2)"

# A raw (non -c) client is redirected to the owner.
reply=$(redis-cli -p "$(port 1)" GET foo; redis-cli -p "$(port 2)" GET foo; redis-cli -p "$(port 3)" GET foo)
check "one node owns 'foo', the others answer MOVED 12182" 2 "$(grep -c '^MOVED 12182 127.0.0.1:' <<< "$reply")"

check "CLUSTER SLOTS covers all 16384 slots" 16384 \
  "$(redis-cli -p "$(port 1)" CLUSTER SLOTS | awk 'NR % 5 == 1 { start = $1 } NR % 5 == 2 { total += $1 - start + 1 } END { print total }')"

bash "$CLUSTER" add 4 > /dev/null
check "all keys readable after node 4 joined" 0 "$(all_readable 4)"

bash "$CLUSTER" remove 2 > /dev/null
check "all keys readable after node 2 left" 0 "$(all_readable 3)"
check "node 2 is gone from the map" "" "$(bash "$CLUSTER" show | awk '$1 == "n2"')"

check "DEL through a redirect" 1 "$(cli 1 DEL key:1)"
check "deleted key stays deleted" "" "$(cli 3 GET key:1)"

if (( failures > 0 )); then
  echo "$failures failure(s)"
  exit 1
fi
echo "all redis-cli cluster checks passed"

#!/usr/bin/env bash
# End-to-end test: starts distkv-server and drives it with the real redis-cli.
# Usage: redis_cli_test.sh <path-to-distkv-server>
set -euo pipefail

SERVER_BIN=${1:?usage: $0 <distkv-server>}
PORT=${KV_TEST_PORT:-$((20000 + RANDOM % 20000))}

"$SERVER_BIN" --port "$PORT" --threads 2 > /dev/null &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT

cli() { redis-cli -p "$PORT" "$@"; }

for _ in $(seq 50); do
  if [[ "$(cli PING 2>/dev/null)" == "PONG" ]]; then break; fi
  sleep 0.1
done

failures=0
expect() {
  local expected=$1; shift
  local actual
  actual=$(cli "$@" 2>&1)
  if [[ "$actual" == "$expected" ]]; then
    echo "ok:   $*"
  else
    echo "FAIL: $*  expected [$expected] got [$actual]"
    failures=$((failures + 1))
  fi
}

expect "PONG"  PING
expect "hello" PING hello
expect ""      GET missing
expect "OK"    SET greeting "hello world"
expect "hello world" GET greeting
expect "OK"    set greeting replaced
expect "replaced" get greeting
expect "1"     DEL greeting
expect "0"     DEL greeting
expect ""      GET greeting
expect "OK"    SET a 1
expect "OK"    SET b 2
expect "2"     DEL a b c
expect "ERR wrong number of arguments for 'get' command" GET
expect "ERR unknown command 'FLUSHALL'" FLUSHALL

# Pipelining: 10k RESP-encoded SETs written in one stream over a raw socket.
# (redis-cli --pipe is not used because it relies on ECHO, which is out of scope.)
exec 3<>"/dev/tcp/127.0.0.1/$PORT"
seq 1 10000 | awk '{ k = "pipe:" $1; printf "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(k), k, length($1), $1 }' >&3
ok_replies=$(head -c $((10000 * 5)) <&3 | grep -c "+OK" || true)
exec 3>&-
if [[ "$ok_replies" == "10000" ]]; then
  echo "ok:   10000 pipelined SETs"
else
  echo "FAIL: pipelined SETs, got $ok_replies OK replies"
  failures=$((failures + 1))
fi
expect "9999" GET pipe:9999

# Server shuts down cleanly on SIGTERM.
kill -TERM "$SERVER_PID"
if wait "$SERVER_PID"; then
  echo "ok:   clean shutdown on SIGTERM"
else
  echo "FAIL: server exited with status $? on SIGTERM"
  failures=$((failures + 1))
fi
trap - EXIT

if (( failures > 0 )); then
  echo "$failures failure(s)"
  exit 1
fi
echo "all redis-cli checks passed"

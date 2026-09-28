#!/usr/bin/env bash
# Phase 5 failure test: kills a primary under load, several times, and checks
# that no acknowledged write was lost. Uses the same inventory as the deploy
# scripts, so it also runs against the local rehearsal cluster.
#
#   bench/aws/run_failover_test.sh
#
# Each round, on the load generator:
#   - memtier keeps a background load on the whole cluster;
#   - distkv-verifier writes sequenced keys, kills the victim (the current
#     primary of the next group, round-robin) KILL_AFTER seconds in via SSH,
#     and afterwards reads back every acknowledged key.
# Then the round waits until the group has a synced backup again (the spare),
# restarts the killed node as the new spare, and moves on.
#
# Environment: OUT, KILLS (default 5), DURATION (s per round, default 30),
#              KILL_AFTER (s, default 10), WRITERS (default 8).
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/../../deploy/scripts/common.sh"
require_inventory

KILLS=${KILLS:-5}
DURATION=${DURATION:-30}
KILL_AFTER=${KILL_AFTER:-10}
WRITERS=${WRITERS:-8}
OUT=${OUT:-$REPO_DIR/bench/results/aws-failover-$(date -u +%Y%m%dT%H%M%SZ)}
LOADGEN=$(public_ip loadgen)
mkdir -p "$OUT"

{
  echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_rev: $(git -C "$REPO_DIR" describe --always --dirty 2> /dev/null || echo unknown)"
  echo "node_instance_type: $(inv .node_instance_type)"
  echo "loadgen_instance_type: $(inv .loadgen_instance_type)"
  echo "kills: $KILLS, duration: ${DURATION}s, kill_after: ${KILL_AFTER}s, writers: $WRITERS"
  echo "coordinator: heartbeat 100 ms, failure timeout 1000 ms (defaults)"
  echo "background load: memtier -t 2 -c 10 --ratio=1:1 --cluster-mode"
} > "$OUT/environment.txt"

# Fresh cluster.
bash "$REPO_DIR/deploy/scripts/stop.sh" > /dev/null
bash "$REPO_DIR/deploy/scripts/start.sh" > "$OUT/start.txt"

# group_row GROUP_INDEX: that group's row of `distkv-admin show`
# (GROUP SLOTS PRIMARY BACKUP STATE).
group_row() { admin show | awk -v g="g$1" '$1 == g'; }

groups=$(inv '.groups')
for round in $(seq "$KILLS"); do
  g=$(( (round - 1) % groups + 1 ))
  victim=$(group_row "$g" | awk '{print $3}')
  victim_ip=$(private_ip "$victim")
  seeds=$(inv '[.nodes[] | .private_ip + ":7000"] | join(",")')
  echo "== round $round: killing $victim (primary of g$g, $victim_ip)"

  # Background load for the whole round (errors during failover are expected
  # and do not stop memtier).
  remote "$LOADGEN" "nohup memtier_benchmark -s $(private_ip n1) -p $CLIENT_PORT --cluster-mode \
    --protocol=redis -t 2 -c 10 --ratio=1:1 --data-size=32 --key-maximum=100000 \
    --test-time=$DURATION --hide-histogram > $REMOTE_DIR/logs/bg-memtier-$round.log 2>&1 &"

  # Open an SSH connection to the victim now and reuse it for the kill, so the
  # kill takes milliseconds instead of a fresh handshake (which would widen
  # the window in which the kill happened, and so the failover measurement).
  ctl="/tmp/distkv-kill-$victim"
  remote "$LOADGEN" "ssh -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR -i .ssh/cluster_key \
    -o ControlMaster=yes -o ControlPath=$ctl -o ControlPersist=300 -fN ubuntu@$victim_ip"
  # \\\$ survives two shells: this one and the load generator's (which sees \$
  # inside double quotes), so $(cat ...) is only expanded on the victim.
  kill_cmd="ssh -o ControlPath=$ctl -o LogLevel=ERROR ubuntu@$victim_ip 'kill -9 \\\$(cat $REMOTE_DIR/$victim.pid)'"
  set +e
  remote "$LOADGEN" "$REMOTE_DIR/bin/distkv-verifier --seeds $seeds --duration $DURATION \
    --writers $WRITERS --prefix r$round --victim $victim_ip:$CLIENT_PORT --kill-after $KILL_AFTER \
    --kill-cmd \"$kill_cmd\" --json $REMOTE_DIR/results/failover-$round.json \
    --timeline $REMOTE_DIR/results/failover-$round.csv" | tee "$OUT/failover-$round.txt"
  verifier_status=${PIPESTATUS[0]}
  set -e
  remote "$LOADGEN" "ssh -o ControlPath=$ctl -O exit ubuntu@$victim_ip 2> /dev/null || true"
  scp "${ssh_opts[@]}" -i "$SSH_KEY" -q \
    "ubuntu@$LOADGEN:$REMOTE_DIR/results/failover-$round.json" \
    "ubuntu@$LOADGEN:$REMOTE_DIR/results/failover-$round.csv" "$OUT/"
  if (( verifier_status != 0 )); then
    echo "round $round: verifier reported LOST acknowledged writes (exit $verifier_status)" >&2
  fi

  # Wait for the group to have a synced backup again (promotion + spare sync).
  for _ in $(seq 120); do
    row=$(group_row "$g")
    [[ $(awk '{print $5}' <<< "$row") == ready && $(awk '{print $3}' <<< "$row") != "$victim" ]] && break
    sleep 0.5
  done
  echo "   g$g now: $(group_row "$g")"

  # The killed node comes back as the new spare for the next round.
  start_process "$(public_ip "$victim")" "$victim" \
    "$REMOTE_DIR/bin/distkv-server --cluster --node-id $victim --port $CLIENT_PORT \
     --grpc-port $NODE_GRPC_PORT --advertise-host $victim_ip"
  for _ in $(seq 50); do
    remote "$(public_ip "$victim")" redis-cli -p "$CLIENT_PORT" PING > /dev/null 2>&1 && break
    sleep 0.2
  done
  admin add-spare "$victim_ip:$NODE_GRPC_PORT" > /dev/null
  sleep 2
done

# The coordinator's log has the detection / promotion / sync timeline.
remote "$(public_ip coordinator)" cat "$REMOTE_DIR/logs/coordinator.log" > "$OUT/coordinator.log"
bash "$REPO_DIR/deploy/scripts/stop.sh" > /dev/null
python3 "$REPO_DIR/bench/summarize.py" "$OUT"
echo "results in $OUT"

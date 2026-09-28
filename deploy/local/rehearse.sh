#!/usr/bin/env bash
# Rehearses the AWS deploy scripts against Docker containers instead of EC2
# instances, at no cost. Each container stands in for one instance (SSH
# server, `ubuntu` user, DistKV directories); a generated inventory points
# the unmodified deploy/scripts at them.
#
# Run on the host (it drives Docker):
#   deploy/local/rehearse.sh up                      start 9 fake instances
#   deploy/local/rehearse.sh run SCRIPT [ARGS...]    run a deploy script against them
#   deploy/local/rehearse.sh down                    remove them
#
# What it does NOT cover: Terraform, cloud-init package installs, the arm64
# build, real availability zones and real network latency.
set -euo pipefail

cd "$(dirname "$0")/../.."
REPO=$(pwd -W 2> /dev/null || pwd)  # Windows path under Git Bash, for docker -v
export MSYS_NO_PATHCONV=1
GEN=deploy/.generated-local
NETWORK=distkv-rehearsal
SUBNET_PREFIX=172.30.0
IMAGE=distkv-fakehost
# Not GROUPS: bash reserves that name (the current user's group ids).
NUM_GROUPS=3
NUM_SPARES=1

# name -> IP: coordinator .10, loadgen .11, node nI -> .(20+I)
hosts() {
  echo "coordinator $SUBNET_PREFIX.10"
  echo "loadgen $SUBNET_PREFIX.11"
  for i in $(seq $((NUM_GROUPS * 2 + NUM_SPARES))); do echo "n$i $SUBNET_PREFIX.$((20 + i))"; done
}

write_inventory() {
  local nodes=() i group role az
  for i in $(seq 0 $((NUM_GROUPS * 2 + NUM_SPARES - 1))); do
    # Same placement rule as deploy/terraform/main.tf, with fake AZ labels.
    if (( i < NUM_GROUPS * 2 )); then
      group=$((i / 2 + 1))
      if (( i % 2 == 0 )); then role=primary; else role=backup; fi
      az=$(( (i / 2 + i % 2) % 3 ))
    else
      group=0; role=spare; az=$((i % 3))
    fi
    local ip="$SUBNET_PREFIX.$((21 + i))"
    nodes+=("{\"id\":\"n$((i + 1))\",\"role\":\"$role\",\"group\":$group,\"public_ip\":\"$ip\",\"private_ip\":\"$ip\",\"az\":\"local-$az\"}")
  done
  local joined
  joined=$(IFS=,; echo "${nodes[*]}")
  cat > "$GEN/inventory.json" <<EOF
{"region":"local","ssh_user":"ubuntu","node_instance_type":"docker (rehearsal)","loadgen_instance_type":"docker (rehearsal)","groups":$NUM_GROUPS,"spares":$NUM_SPARES,
 "coordinator":{"public_ip":"$SUBNET_PREFIX.10","private_ip":"$SUBNET_PREFIX.10","az":"local-0"},
 "loadgen":{"public_ip":"$SUBNET_PREFIX.11","private_ip":"$SUBNET_PREFIX.11","az":"local-0"},
 "nodes":[$joined]}
EOF
}

case "${1:-}" in
  up)
    docker build -q -f deploy/local/fakehost.Dockerfile -t "$IMAGE" deploy/local > /dev/null
    mkdir -p "$GEN"
    if [[ ! -f "$GEN/id_ed25519" ]]; then
      docker run --rm -v "$REPO/$GEN:/gen" distkv-dev \
        ssh-keygen -q -t ed25519 -N '' -C distkv-rehearsal -f /gen/id_ed25519
    fi
    docker network inspect "$NETWORK" > /dev/null 2>&1 ||
      docker network create --subnet "$SUBNET_PREFIX.0/24" "$NETWORK" > /dev/null
    while read -r name ip; do
      docker rm -f "distkv-rh-$name" > /dev/null 2>&1 || true
      docker run -d --name "distkv-rh-$name" --hostname "$name" --network "$NETWORK" --ip "$ip" \
        -v "$REPO/$GEN:/keys:ro" "$IMAGE" > /dev/null
      echo "started $name ($ip)"
    done < <(hosts)
    write_inventory
    echo "inventory: $GEN/inventory.json"
    ;;
  run)
    shift
    script=${1:?usage: $0 run SCRIPT [ARGS...]}
    shift
    docker run --rm -i --network "$NETWORK" -v "$REPO:/work" -w /work \
      -e DISTKV_GEN_DIR=/work/$GEN distkv-dev bash "$script" "$@"
    ;;
  down)
    while read -r name _; do docker rm -f "distkv-rh-$name" > /dev/null 2>&1 || true; done < <(hosts)
    docker network rm "$NETWORK" > /dev/null 2>&1 || true
    echo "rehearsal cluster removed"
    ;;
  *)
    sed -n '2,13p' "$0"
    exit 2
    ;;
esac

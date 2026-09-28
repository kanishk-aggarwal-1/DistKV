#!/usr/bin/env bash
# Shared helpers for the deploy scripts. Sourced, not executed.
#
# Reads deploy/.generated/inventory.json, written by Terraform.
# The variables below are used by the scripts that source this file.
# shellcheck disable=SC2034

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "$DEPLOY_DIR/.." && pwd)"
TF_DIR="$DEPLOY_DIR/terraform"
# DISTKV_GEN_DIR lets the local rehearsal (deploy/local/) use its own
# inventory and key without touching a real deployment's.
GEN_DIR="${DISTKV_GEN_DIR:-$DEPLOY_DIR/.generated}"
INVENTORY="$GEN_DIR/inventory.json"

# Ports used on the instances.
CLIENT_PORT=7000
NODE_GRPC_PORT=17000
COORD_PORT=9000
REMOTE_DIR=/home/ubuntu/distkv

require_inventory() {
  if [[ ! -f "$INVENTORY" ]]; then
    echo "no inventory at $INVENTORY: run deploy/scripts/up.sh first" >&2
    exit 1
  fi
  # A bind-mounted Windows folder cannot hold 0600 permissions, which ssh
  # insists on, so use a private copy of the key.
  SSH_KEY=/tmp/distkv_ssh_key
  install -m 600 "$GEN_DIR/id_ed25519" "$SSH_KEY"
}

inv() { jq -r "$1" "$INVENTORY"; }

ssh_opts=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/tmp/distkv_known_hosts
          -o ConnectTimeout=10 -o LogLevel=ERROR -o ServerAliveInterval=15)

# remote HOST_PUBLIC_IP COMMAND... : runs a command on an instance.
remote() {
  local host=$1
  shift
  ssh "${ssh_opts[@]}" -i "$SSH_KEY" "ubuntu@$host" "$@"
}

# Addresses by name: "coordinator", "loadgen", or a node id such as "n3".
public_ip() {
  case "$1" in
    coordinator | loadgen) inv ".$1.public_ip" ;;
    *) inv ".nodes[] | select(.id == \"$1\") | .public_ip" ;;
  esac
}

private_ip() {
  case "$1" in
    coordinator | loadgen) inv ".$1.private_ip" ;;
    *) inv ".nodes[] | select(.id == \"$1\") | .private_ip" ;;
  esac
}

node_ids() { inv '.nodes[].id'; }
all_hosts() { echo coordinator; echo loadgen; node_ids; }

# admin ARGS... : runs distkv-admin on the coordinator host.
admin() {
  remote "$(public_ip coordinator)" "$REMOTE_DIR/bin/distkv-admin" --coordinator "127.0.0.1:$COORD_PORT" "$@"
}

now_ms() { date +%s%3N; }

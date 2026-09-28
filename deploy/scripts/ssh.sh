#!/usr/bin/env bash
# Opens a shell (or runs a command) on an instance.
#
#   deploy/scripts/ssh.sh coordinator|loadgen|NODE_ID [COMMAND...]
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

name=${1:?usage: $0 coordinator|loadgen|NODE_ID [COMMAND...]}
shift
ip=$(public_ip "$name")
[[ -n "$ip" ]] || { echo "unknown instance $name" >&2; exit 1; }
exec ssh "${ssh_opts[@]}" -i "$SSH_KEY" -t "ubuntu@$ip" "$@"

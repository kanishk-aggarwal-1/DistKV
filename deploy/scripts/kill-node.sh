#!/usr/bin/env bash
# Kills a node's process, for failure testing. Prints the kill time (ms since
# the epoch, from the node's own clock) so failover time can be measured.
#
#   deploy/scripts/kill-node.sh NODE_ID [SIGNAL]    (default SIGKILL)
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

id=${1:?usage: $0 NODE_ID [SIGNAL]}
signal=${2:-KILL}
ip=$(public_ip "$id")
[[ -n "$ip" ]] || { echo "unknown node $id" >&2; exit 1; }

remote "$ip" "cd $REMOTE_DIR && pid=\$(cat $id.pid) && date +%s%3N && kill -$signal \$pid && rm -f $id.pid" |
  { read -r killed_at; echo "killed $id with SIG$signal at $killed_at ms"; }

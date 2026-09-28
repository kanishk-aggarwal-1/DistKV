#!/usr/bin/env bash
# Shows the cluster map and which DistKV processes are running where.
#
#   deploy/scripts/status.sh
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

printf '%-12s %-12s %-16s %-16s %s\n' NAME AZ PUBLIC PRIVATE PROCESS
for host in coordinator $(node_ids); do
  if [[ $host == coordinator ]]; then az=$(inv '.coordinator.az'); else az=$(inv ".nodes[] | select(.id == \"$host\") | .az"); fi
  state=$(remote "$(public_ip "$host")" "cd $REMOTE_DIR && if [ -f $host.pid ] && kill -0 \$(cat $host.pid) 2>/dev/null; then echo running; else echo stopped; fi" 2> /dev/null || echo unreachable)
  printf '%-12s %-12s %-16s %-16s %s\n' "$host" "$az" "$(public_ip "$host")" "$(private_ip "$host")" "$state"
done
echo
admin show 2> /dev/null || echo "(coordinator not reachable)"

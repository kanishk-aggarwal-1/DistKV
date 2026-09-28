#!/usr/bin/env bash
# Stops every DistKV process (the instances keep running). The coordinator
# stops first, so the nodes going away is not mistaken for failures.
#
#   deploy/scripts/stop.sh
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

stop_process() {
  local ip=$1 name=$2
  remote "$ip" "cd $REMOTE_DIR && if [ -f $name.pid ]; then
      pid=\$(cat $name.pid); kill \$pid 2>/dev/null || true
      for i in \$(seq 50); do kill -0 \$pid 2>/dev/null || break; sleep 0.1; done
      rm -f $name.pid; fi"
}

stop_process "$(public_ip coordinator)" coordinator
echo "stopped coordinator"
for id in $(node_ids); do
  stop_process "$(public_ip "$id")" "$id"
  echo "stopped $id"
done

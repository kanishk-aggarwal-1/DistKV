#!/usr/bin/env bash
# Stops every DistKV process (the instances keep running). The coordinator
# stops first, so the nodes going away is not mistaken for failures.
#
#   deploy/scripts/stop.sh
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"
require_inventory

stop_process "$(public_ip coordinator)" coordinator
echo "stopped coordinator"
for id in $(node_ids); do
  stop_process "$(public_ip "$id")" "$id"
  echo "stopped $id"
done

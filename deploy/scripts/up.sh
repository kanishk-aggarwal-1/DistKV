#!/usr/bin/env bash
# Creates the AWS infrastructure (VPC, subnets in 3 AZs, security group,
# instances). Terraform shows the plan and asks for confirmation before it
# creates anything billable.
#
#   deploy/scripts/up.sh [-var 'groups=3' ...]
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"

aws sts get-caller-identity --query 'Account' --output text > /dev/null || {
  echo "AWS credentials not configured: run 'aws configure' first" >&2
  exit 1
}

mkdir -p "$GEN_DIR"
terraform -chdir="$TF_DIR" init -input=false
terraform -chdir="$TF_DIR" apply "$@"

echo
echo "Infrastructure is up. Instances shut themselves down (and terminate)"
echo "after the max lifetime; run deploy/scripts/teardown.sh when done."
echo "Next: deploy/scripts/deploy.sh && deploy/scripts/start.sh"

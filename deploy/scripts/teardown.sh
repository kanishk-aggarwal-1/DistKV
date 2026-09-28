#!/usr/bin/env bash
# Destroys everything Terraform created, then checks with the AWS API that no
# instance tagged Project=distkv is left, so nothing keeps billing.
#
#   deploy/scripts/teardown.sh
set -euo pipefail
# shellcheck source=deploy/scripts/common.sh
source "$(dirname "$0")/common.sh"

project=distkv
region=$(jq -r '.region' "$INVENTORY" 2> /dev/null || echo us-east-1)

terraform -chdir="$TF_DIR" init -input=false > /dev/null
terraform -chdir="$TF_DIR" destroy -auto-approve -input=false

echo "== checking for leftover resources tagged Project=$project in $region"
leftover=$(aws ec2 describe-instances --region "$region" \
  --filters "Name=tag:Project,Values=$project" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text)
vpcs=$(aws ec2 describe-vpcs --region "$region" --filters "Name=tag:Project,Values=$project" \
  --query 'Vpcs[].VpcId' --output text)

if [[ -n "$leftover" || -n "$vpcs" ]]; then
  echo "WARNING: still present: instances [$leftover] vpcs [$vpcs]" >&2
  echo "Check the EC2 console (region $region) and delete them by hand." >&2
  exit 1
fi
rm -f "$INVENTORY"
echo "All DistKV resources are gone."

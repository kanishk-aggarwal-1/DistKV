#!/usr/bin/env bash
# Does what cloud-init and the Terraform boot script do on a real instance:
# authorise the cluster key, create the DistKV directories, mark the host as
# ready, then run sshd in the foreground.
set -euo pipefail

install -d -o ubuntu -g ubuntu -m 700 /home/ubuntu/.ssh
install -o ubuntu -g ubuntu -m 600 /keys/id_ed25519.pub /home/ubuntu/.ssh/authorized_keys
install -d -o ubuntu -g ubuntu /home/ubuntu/distkv /home/ubuntu/distkv/bin /home/ubuntu/distkv/logs
touch /var/lib/distkv-ready

ssh-keygen -A > /dev/null
exec /usr/sbin/sshd -D -e

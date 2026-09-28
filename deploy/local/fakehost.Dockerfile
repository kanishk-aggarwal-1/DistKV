# A stand-in for an EC2 instance, for rehearsing the deploy scripts locally:
# the dev image (toolchain, runtime libraries, redis-cli, memtier) plus an SSH
# server for the `ubuntu` user, like the Ubuntu AMI.
FROM distkv-dev

RUN apt-get update && apt-get install -y --no-install-recommends openssh-server \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd \
    # The image's ubuntu user has a locked password; unlock it for key-only login.
    && usermod -p '*' ubuntu

COPY fakehost-entrypoint.sh /usr/local/bin/fakehost-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/fakehost-entrypoint.sh"]

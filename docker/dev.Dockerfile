# Development image: compiler toolchain, gRPC/protobuf, Redis (for redis-cli and the
# baseline server), memtier_benchmark, and the AWS deployment tools (Terraform,
# AWS CLI, SSH, shellcheck).
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates gdb \
        redis-server redis-tools \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
        autoconf automake libtool pkg-config libevent-dev libssl-dev zlib1g-dev libpcre3-dev \
        curl unzip jq openssh-client rsync shellcheck python3 \
    && rm -rf /var/lib/apt/lists/*

# memtier_benchmark is not packaged for Ubuntu 24.04; build a pinned release.
ARG MEMTIER_VERSION=2.5.1
RUN git clone --depth 1 --branch ${MEMTIER_VERSION} https://github.com/RedisLabs/memtier_benchmark.git /tmp/memtier \
    && cd /tmp/memtier && autoreconf -ivf && ./configure && make -j"$(nproc)" && make install \
    && rm -rf /tmp/memtier

# Terraform and AWS CLI v2, pinned.
ARG TERRAFORM_VERSION=1.16.4
ARG AWSCLI_VERSION=2.37.5
RUN curl -fsSL -o /tmp/terraform.zip \
        https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip \
    && unzip -q /tmp/terraform.zip -d /usr/local/bin && rm /tmp/terraform.zip \
    && curl -fsSL -o /tmp/awscli.zip \
        https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWSCLI_VERSION}.zip \
    && unzip -q /tmp/awscli.zip -d /tmp && /tmp/aws/install && rm -rf /tmp/aws /tmp/awscli.zip

WORKDIR /work

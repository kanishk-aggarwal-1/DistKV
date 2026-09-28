# Development image: compiler toolchain, gRPC/protobuf, Redis (for redis-cli and the
# baseline server), and memtier_benchmark.
FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates gdb \
        redis-server redis-tools \
        libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
        autoconf automake libtool pkg-config libevent-dev libssl-dev zlib1g-dev libpcre3-dev \
    && rm -rf /var/lib/apt/lists/*

# memtier_benchmark is not packaged for Ubuntu 24.04; build a pinned release.
ARG MEMTIER_VERSION=2.5.1
RUN git clone --depth 1 --branch ${MEMTIER_VERSION} https://github.com/RedisLabs/memtier_benchmark.git /tmp/memtier \
    && cd /tmp/memtier && autoreconf -ivf && ./configure && make -j"$(nproc)" && make install \
    && rm -rf /tmp/memtier

WORKDIR /work

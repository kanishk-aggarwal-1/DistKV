#!/usr/bin/env bash
# Builds and tests the given presets (default: debug asan tsan). Run inside
# the dev container or on any Linux host.
#
# TSan cannot start when the kernel uses 32 bits of mmap randomisation
# (vm.mmap_rnd_bits=32, the default on recent kernels, including WSL2). CI
# lowers that sysctl; here we run the TSan tests with ASLR disabled instead,
# which inside Docker needs `--security-opt seccomp=unconfined`.
set -euo pipefail

presets=("$@")
(( ${#presets[@]} )) || presets=(debug asan tsan)

for preset in "${presets[@]}"; do
  echo "=== $preset ==="
  cmake --preset "$preset" > /dev/null
  cmake --build --preset "$preset"
  if [[ "$preset" == tsan && "$(cat /proc/sys/vm/mmap_rnd_bits 2>/dev/null || echo 0)" -gt 28 ]]; then
    setarch "$(uname -m)" -R ctest --preset "$preset" --timeout 300
  else
    ctest --preset "$preset" --timeout 300
  fi
done

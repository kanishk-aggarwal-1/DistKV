# Runs a command inside the Linux dev container (see docker/dev.Dockerfile).
#   .\scripts\dev.ps1                          # interactive shell
#   .\scripts\dev.ps1 cmake --preset debug     # one-off command
#
# The source tree is bind-mounted at /work. build/ is a named Docker volume so
# compilation happens on the Linux filesystem instead of across the slower
# Windows bind mount.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

docker image inspect distkv-dev *> $null
if ($LASTEXITCODE -ne 0) {
    docker build -f "$root/docker/dev.Dockerfile" -t distkv-dev "$root/docker"
}

$tty = if ($args.Count -eq 0) { "-it" } else { "-i" }
$cmd = if ($args.Count -eq 0) { @("bash") } else { $args }
# seccomp=unconfined lets scripts/check.sh disable ASLR for the TSan tests
# (see docs/DESIGN.md). This is a local dev container only.
docker run --rm $tty `
    --security-opt seccomp=unconfined `
    -v "${root}:/work" `
    -v distkv-build:/work/build `
    -w /work `
    distkv-dev @cmd
exit $LASTEXITCODE

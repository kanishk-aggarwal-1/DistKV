# Runs a command inside the Linux dev container (see docker/dev.Dockerfile).
#   .\scripts\dev.ps1                          # interactive shell
#   .\scripts\dev.ps1 cmake --preset debug     # one-off command
#
# The source tree is bind-mounted at /work. build/ is a named Docker volume so
# compilation happens on the Linux filesystem instead of across the slower
# Windows bind mount.
#
# Your AWS CLI configuration (%USERPROFILE%\.aws) is mounted at /root/.aws so
# Terraform and the deploy scripts use your credentials; run `aws configure`
# (on Windows or inside the container) to set them up. They never enter the
# repository.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

docker image inspect distkv-dev *> $null
if ($LASTEXITCODE -ne 0) {
    docker build -f "$root/docker/dev.Dockerfile" -t distkv-dev "$root/docker"
}

$awsDir = Join-Path $env:USERPROFILE ".aws"
New-Item -ItemType Directory -Force $awsDir | Out-Null

$tty = if ($args.Count -eq 0) { "-it" } else { "-i" }
$cmd = if ($args.Count -eq 0) { @("bash") } else { $args }
# seccomp=unconfined lets scripts/check.sh disable ASLR for the TSan tests
# (see docs/DESIGN.md). This is a local dev container only.
docker run --rm $tty `
    --security-opt seccomp=unconfined `
    -v "${root}:/work" `
    -v distkv-build:/work/build `
    -v "${awsDir}:/root/.aws" `
    -w /work `
    distkv-dev @cmd
exit $LASTEXITCODE

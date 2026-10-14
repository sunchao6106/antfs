#!/usr/bin/env bash
# 在 macOS 宿主机执行,把 cargo 命令转到 colima Linux VM 里跑。
# - 源码通过 virtiofs 挂载在 VM 的同路径
# - cargo target 放 VM 原生 ext4(避免 virtiofs 慢/O_DIRECT 问题)
set -euo pipefail

# 脚本随仓库走(勿硬编码机器路径:colima 以同路径 virtiofs 挂载宿主仓库)。
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VM_TARGET_DIR="/var/tmp/antfs-target"

colima ssh -- sh -c "
  . \$HOME/.cargo/env
  export CARGO_TARGET_DIR='${VM_TARGET_DIR}'
  cd '${PROJECT_DIR}'
  cargo $*
"

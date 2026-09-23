#!/usr/bin/env bash
# 把 release 二进制打成发布资产:`dist/antfs-<version>-<target>.tar.gz` + 追写 `dist/SHA256SUMS`。
#
# 用法:scripts/release/package_binaries.sh <version> <target> [bin-dir]
#   version  不含前导 v,如 0.1.0
#   target   Rust target triple,如 x86_64-unknown-linux-gnu
#   bin-dir  可选,二进制所在目录;默认 target/<target>/release,没有再退回 target/release
set -euo pipefail

version=${1:?usage: package_binaries.sh <version> <target> [bin-dir]}
target=${2:?usage: package_binaries.sh <version> <target> [bin-dir]}
bin_dir=${3:-}

bins=(antfs-master antfs-store antfs-frontend antfs-admin antfs-devcluster)

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ -z $bin_dir ]]; then
  if [[ -d $repo_root/target/$target/release ]]; then
    bin_dir=$repo_root/target/$target/release
  else
    bin_dir=$repo_root/target/release
  fi
fi

dist=$repo_root/dist
name=antfs-$version-$target
stage=$dist/$name
rm -rf "$stage"
mkdir -p "$stage"

for bin in "${bins[@]}"; do
  if [[ ! -f $bin_dir/$bin ]]; then
    echo "package_binaries: missing binary $bin_dir/$bin" >&2
    exit 1
  fi
  install -m 0755 "$bin_dir/$bin" "$stage/$bin"
done
install -m 0644 "$repo_root/LICENSE" "$stage/LICENSE"
install -m 0644 "$repo_root/README.md" "$stage/README.md"

tarball=$name.tar.gz
tar -C "$dist" -czf "$dist/$tarball" "$name"
rm -rf "$stage"

# 校验和:两个平台都能跑出同一格式(`<hash>  <file>`),Linux 的 `sha256sum -c` 可直接校验。
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$dist" && sha256sum "$tarball" >> SHA256SUMS)
else
  (cd "$dist" && shasum -a 256 "$tarball" >> SHA256SUMS)
fi

echo "package_binaries: packed $dist/$tarball"

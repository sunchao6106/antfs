#!/usr/bin/env bash
# 创建或更新 GitHub Release:正文用 GitHub 自动生成的 release notes,资产覆盖上传。
#
# 正文首行带 managed 标记:重跑本脚本会刷新正文;若有人手工改过正文(把标记删了),则保留不动。
#
# 用法:scripts/release/create_or_update_release.sh <tag> [asset...]
#   需要 `gh` 已登录或 GH_TOKEN 已设;`git describe` 需要完整 tag 历史(fetch-depth: 0)。
set -euo pipefail

tag=${1:?usage: create_or_update_release.sh <tag> [asset...]}
shift
assets=("$@")

repo=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY 未设置(本地跑请显式导出)}
marker='<!-- antfs-release-notes: managed; 正文由 CI 生成,手工修改请删掉本标记 -->'

# 基线 = 目标 tag 之前最近的一个 v* tag;没有就让 GitHub 用默认基线。
previous=$(git describe --tags --abbrev=0 --match 'v*' "$tag^" 2>/dev/null || true)
if [[ -n $previous ]]; then
  echo "create_or_update_release: baseline $previous"
  notes=$(gh api --method POST "repos/$repo/releases/generate-notes" \
    -f tag_name="$tag" -f previous_tag_name="$previous" --jq .body)
else
  notes=$(gh api --method POST "repos/$repo/releases/generate-notes" \
    -f tag_name="$tag" --jq .body)
fi
body=$(printf '%s\n\n%s' "$marker" "$notes")

# 带 `-` 的 tag(0.1.0-rc.1)按预发布处理,不抢占 latest。
prerelease=()
[[ $tag == *-* ]] && prerelease=(--prerelease)

if gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
  current=$(gh release view "$tag" --repo "$repo" --json body --jq .body)
  if [[ $current == *"$marker"* ]]; then
    gh release edit "$tag" --repo "$repo" --notes "$body" "${prerelease[@]}" >/dev/null
    echo "create_or_update_release: refreshed notes for $tag"
  else
    echo "create_or_update_release: $tag 正文被人工改过,保留不动"
  fi
else
  gh release create "$tag" --repo "$repo" --title "$tag" --notes "$body" \
    --verify-tag "${prerelease[@]}"
  echo "create_or_update_release: created $tag"
fi

if (( ${#assets[@]} )); then
  gh release upload "$tag" --repo "$repo" --clobber "${assets[@]}"
  echo "create_or_update_release: uploaded ${#assets[@]} asset(s) to $tag"
fi

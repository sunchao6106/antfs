# Proposal: router-read-topology-refresh — 读拓扑的 TTL 与全败驱逐

## Why

router.md §7 老待办:读路由的 `topology` 缓存(`pid → 副本列表`)**无 TTL、无失效** —— 副本
搬迁/掉盘后 router 拿着旧列表把 R=1 的全部机会浪费在已消失的副本上,调用方看到的是"读失败",
真相是"router 只认识旧世界"。写路径有周期刷新(`partition_refresh_secs`)与发号回填,
读路径只在 miss 时查一次 master。

## What Changes

| 位置 | 改动 |
|---|---|
| `crates/common/router/src/lib.rs` | topology 条目 `(Vec<ReplicaLoc>, Instant)`;**TTL = `partition_refresh_secs`**(语义对齐:0=关;到期读先经 master `LookupPartition` 重拉,拉失败保留旧表可用性优先);R=1 读**全部副本失败**且失败含不可达类(非纯 NotFound)→ 驱逐该 pid 条目 + 立即重拉 + **重试读一次** |
| metrics | `router_topology_refreshes_total{reason=ttl\|evict}`(§7 双表同步) |
| 测试 | 条目纯语义单测(TTL 决策/驱逐条件分类);e2e:metric 断言(reason=ttl 经分区表步进、reason=evict 经全员不可达分区) |
| 文档 | router.md §7 第 208 条勾销 + 监控表行;DEVLOG 步 85 |

## Impact

- 无新配置(TTL 复用 `partition_refresh_secs`,读写共用同一个"拓扑新鲜度"旋钮);行为:
  陈旧拓扑的读从"必然失败"变"一次重拉内自愈"(有界:单请求至多一次 evict-重拉);
- TTL 语义与后台刷新一致(0=关),默认 30s 不多刷 master。

## Out of Scope

- 副本级健康缓存/熔断(已有 `up` 位与延迟 EWMA,另维度)。
- 读路径重试策略配置化(内建一次,保守)。

# Proposal: router-id-prefetch — 号段后台预取(边界 RPC 尖峰平滑)

## Why

router.md §7 老待办:号段按需取段 —— 段尾几次发号必然撞上一次同步 master 往返
(`sync_segments`),号段边界出现 RPC 尖峰;高频写场景表现为周期性 p99 抖动。

## What Changes

| 位置 | 改动 |
|---|---|
| `Segment` | 增 `start` 字段(段长记忆 → 低水位判定分母) |
| `IdCache` | `low_water_pid()` 探针(剩余 < ¼ 段长或 ≤ 2 号);`extend_segment` 回填(续段无缝/落空丢弃/无段直插) |
| `Router::allocate_one_inner` | 命中取号后低水位探针 → `spawn_prefetch(pid)`(单飞;同区一拍一次,std 锁布防;字段 Arc 副本进任务) |
| metrics | `router_id_prefetch_errors_total`(失败计数;成功不打 —— 用耗尽次数下降体现) |
| 测试 | lib 单测(探针/回填三态);e2e:4 号极小段连续 32 号 + 预取零错误 |
| 文档 | router.md §7 勾销 + 监控行;DEVLOG 步 86 |

## Impact

- **正确性零依赖**:耗尽的同步 `sync_segments` 原样保留;预取失败静默负计数(下一探针再多试)。
- 续段语义:**master 发号连续**(`AllocateObjectId` 抬水位切发 ⇒ 预取段 `start` 恰为本段
  `end`),恰对上 → 零空洞续;对不上(并发耗尽/507 摘除竞态)→ 保守丢弃,不多造空洞。
- 副收益:预取响应顺带刷新该分区读拓扑条目(`segment_from_response` 复用)。
- 无新配置(水位/heuristic 内建,§ 设计记档)。

## Out of Scope

- 双 buffer 预取(prefetch 到下一个完整段对;当前续段形态已覆盖连续场景)。
- 跨分区并行预取(逐探针逐区单飞,已自然并行)。

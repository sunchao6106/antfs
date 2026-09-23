## Why

批 1 补齐了观测与开关,但两个重活还没做,它们是 antfs 目前最明显的运维缺口:

1. **均衡**:分区/盘分布靠选盘时的一次性决策,之后只能靠手动 `MoveReplica` 一个个搬;没有"先评估再执行"的计划式均衡(对标 Ceph balancer `eval → optimize → execute` 与 SeaweedFS `volume_balance`)。
2. **一致性巡检**:三副本靠拉复制收敛,但没有任何"副本之间是否一致"的检查手段(对标 Ceph scrub / FastDFS `sync_check`);且 blob 的 `decode_record` 遇 CRC 错直接 `Err(Corrupt)`,一条坏记录会让该副本的复制/索引重建整体失败,运维甚至无法知道坏在哪个段。

本 change 只做设计定稿与任务拆解,**不实现**(留待批 2)。

## What Changes

### A. 计划式均衡(eval → start → cancel → state)

- **计划持久化**:`MasterState.balance: Option<BalancePlan>`(新类型 `BalancePlan`/`BalanceMove`/`BalanceStatus`,`#[serde(default)]` 兼容);`MasterCommand::SetBalancePlan`/`SetBalanceStatus`(枚举只追加,JSON 标签式序列化兼容)。计划是 leader 用软状态算出的**数据**,经 Raft 写入,apply 只存不算(与 `CreatePartition { replicas }` 同构),可跨 leader 切换存活。
- **planner**(新 `crates/master/src/balance.rs`,纯函数 + 单测/proptest):输入 partitions/moving/decommissioning + 每盘 (capacity, free, 副本数) + 批 1 上报的每分区真实占用;输出"把副本从 A 迁到 B"的 move 列表。判据:每盘副本数与占用双维度倾斜(复用 `select_disks` 做落点校验、`classify_partition` 剔除欠副本分区、`has_spare_disk` 标注 infeasible);**限制每分区同时只 1 个 in-flight move**(避免临时 RF+1 放大)。
- **执行器**:挂在 `spawn_reconciler` 内(天然获得 leader-only + grace + 维护模式三重门),受新配置 `balance_max_in_flight` 限并发;进度**从状态派生**(done = 源已不在 placement;in-flight = `moving` 含该对;pending = 目标不存在且不在 moving;infeasible = 复核失败),不额外存字段。
- **RPC**:`EvalBalance`(评估并落 PREVIEW 计划)、`StartBalance(plan_id)`、`CancelBalance`(只清计划,在途 move 自然收敛)、`GetBalanceState`(计划 + 逐 move 进度);admin 路由 `GET /api/balance` + `POST /api/balance/eval|start|cancel`;UI 预览表格(from→to、状态、失败原因)+ 确认/取消 + 进度。
- **已知坑(必须处理)**:
  - `MasterCommand::MoveReplica` 的 apply 只在"目标副本尚不存在"时把 `(pid, from)` 记入 `moving`;**重复投递目标已存在的 move 会静默无效**(不标 moving、源永不删)→ planner/executor 必须自己判断"目标已在 placement 中"并视作已受理,不能靠重试。
  - 无 move 超时:新副本长期不 up 会永久占住并发预算且分区长期 RF+1 → 需要 stuck 判据(超过 N 个 tick 标 failed 并跳过)。
  - 取消语义弱:`moving` 只记源不记目标,无法精确回滚 → MVP 只清计划 + 让它自然收敛。
- **新指标**:`master_balance_moves_total`、`master_balance_pending`、`master_balance_failed_total`;配置 `balance_enabled`(默认 false)、`balance_max_in_flight`(默认 1)、`balance_skew_threshold`、`balance_max_moves`。

### B. 副本一致性 scrub(逻辑比对 + 本地 verify)

- **逻辑一致性比对(admin 编排)**:admin 对指定分区 fan-out 到各副本,拉元数据流比对 key/flags + 各副本摘要(记录数/字节/段清单);完全复用现有 `GET /partitions/{pid}/replica-metadata` 与 `HttpPeer`/`MetadataEntry`(无需新 blob API)。发现"漏写/落后";**注意**:数据面没有 quorum 写,三副本靠拉复制收敛,必须在 `replication_enabled` 下才有意义;扫描期并发写会产生假不一致 → 报告需带各副本 log 末尾 token/bytes_read 以判定可比性,并支持"重跑再判定"。
- **本地完整性校验**(新 blob API `verify() -> VerifyReport`):逐记录读并验 CRC32 + 索引↔log 对拍,报告坏记录位置(段/offset);内部复用现有私有扫描(`for_each_record` / `scan_segment` / `rebuild_index`)。动机:一条坏记录当前会让整个副本的复制/重建失败而无任何定位手段。代价是读全量盘 → 需限流/错峰(`scrub_max_batch_bytes`、可选后台周期 `scrub_interval_secs`)。
- **store 端点**:`GET /partitions/{pid}/scrub-report`(摘要)、`GET /partitions/{pid}/verify`(本地完整性,只读)、可选 `GET /partitions/{pid}/entries?from_key=&limit=`(逐 key 对拍,blob 需新增按键有序区间遍历,索引层 `SkipMap` + 各封存段 mmap 已有序,属增量工作)。
- **admin 路由**:`POST /api/partitions/{id}/scrub`(逐副本比对 + 报告,仿 compact 的 fan-out 聚合)、`GET /api/scrub`(最近结果);指标 `store_server_scrub_runs_total`/`scrub_divergent_keys`/`scrub_bad_records`。
- **明确不做**:段级字节 CRC 跨副本比对 —— 副本各自 append 顺序不同 + compaction 会重写段与 offset,段字节天然不等,**语义上不成立**(段级校验只作本地自检)。

## Capabilities

### New Capabilities
- `cluster-balance`:计划式分区均衡(eval → start → cancel → progress)。
- `replica-scrub`:副本逻辑一致性比对 + 本地完整性校验。

### Modified Capabilities
<!-- MoveReplica 从"手工基元"变为可被计划驱动;ListPartitions 可能扩展 move 进度视图。 -->

## Impact

- **crate**:`master`(balance.rs 新模块、状态机 2 命令、4 RPC、reconciler 执行器、指标/配置)、`blob`(verify 与可选按键区间遍历)、`store`(scrub/verify 端点、指标/配置)、`admin`(balance 与 scrub 路由、fan-out 聚合)、`ui`(均衡预览/进度、scrub 报告)。
- **风险排序**:均衡的最大风险是 move 幂等陷阱与无超时;scrub 的最大风险是"扫描期并发写导致假不一致"与深扫 IO 放大。两者都必须有限流/可重跑/明确的不可比标记。

## Non-goals

- 不做自动持续均衡(计划由运维确认后执行);不做跨集群/跨 DC 的容量规划。
- 不做数据体内容对拍(逻辑一致 ≠ 字节一致);不做自动修复(scrub 只报告,修复走既有的副本重建/再复制路径)。
- 不引入 MVCC/快照读(blob 无 checkpoint 概念),因此 scrub 报告只保证"在各自 token 处可比"。

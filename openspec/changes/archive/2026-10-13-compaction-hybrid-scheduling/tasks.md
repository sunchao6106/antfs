## 1. 配置层：新增策略枚举与配置项

- [x] 1.1 在 `crates/store/src/config.rs` 新增 `CompactionPolicyKind` 枚举（`Incremental` / `CostBenefit` / `Hybrid`），实现 `Deserialize`（小写 kebab 或 snake 均可），默认 `Hybrid`
- [x] 1.2 在 `StoreServerConfig` 新增字段 `compaction_policy: CompactionPolicyKind`（默认 `Hybrid`）、`compaction_policy_switch_days: u32`（默认 6，范围 1–365）、`compaction_min_log_segment_count_to_reclaim: usize`（默认 1，范围 ≥1）
- [x] 1.3 在 `StoreServerConfig::validate()` 里校验新字段范围，非法值报描述性错误
- [x] 1.4 补单测：默认值测试 + 非法策略名 / 非法范围拒绝测试

## 2. 状态持久化：COMPACT_POLICY_STATE 读写

- [x] 2.1 在 `crates/store/src/worker.rs`（或新建 `compact_state.rs`）实现 `read_compact_policy_state(dir) -> Option<i64>` 和 `write_compact_policy_state(dir, epoch_secs) -> Result<()>`，JSON 格式 `{"last_compact_all_epoch_secs": <i64>}`
- [x] 2.2 写路径崩溃安全：tmp → write → fsync → rename（对齐引擎的标记式写法）
- [x] 2.3 读路径容错：文件缺失 / 空 / JSON 解析失败均返回 `None`（等价于"从未做过 CompactAll"）
- [x] 2.4 补单测：写后读回 round-trip、缺失文件返回 None、损坏文件返回 None、进程不同时刻写覆盖

## 3. Worker 策略分发：compact_all 改造

- [x] 3.1 在 `worker.rs` 新增 `fn should_compact_all(policy: CompactionPolicyKind, switch_days: u32, last_compact_all: Option<i64>, now: i64) -> bool` 纯函数：hybrid 模式下判定本轮是否走 CompactAll
- [x] 3.2 改造 `compact_all` 函数签名，接收 `CompactionPolicyKind` + `compaction_min_log_segment_count_to_reclaim` + `compaction_policy_switch_days` 参数（或直接接收 `&StoreServerConfig`）
- [x] 3.3 在 `compact_all` 的逐副本循环内，按策略分发：`Incremental` → `compact_if_needed`；`CostBenefit` → `compact_cost_benefit(policy, min_reclaim)`；`Hybrid` → 按 `should_compact_all` 决定走 `compact()` 还是 `compact_cost_benefit`
- [x] 3.4 `Hybrid` 模式下 CompactAll 完成后调用 `write_compact_policy_state` 更新时间戳
- [x] 3.5 `spawn_compaction_loop` 启动时（首轮前）为每个副本读取 `COMPACT_POLICY_STATE`，得到 `HashMap<PartitionId, Option<i64>>`；新建副本初始时视为 None
- [x] 3.6 补 `should_compact_all` 纯函数的单测：未过期 → false、已过期 → true、None（首次）→ true、非 hybrid 策略 → false

## 4. 指标

- [x] 4.1 在 `crates/store/src/metrics.rs` 的 `StoreServerMetrics` 新增 `compaction_policy_used: IntCounterVec`（label `policy`：`incremental` / `cost_benefit` / `compact_all`）
- [x] 4.2 在 `compact_all` 的每次实际执行 compact 后，按实际走的策略路径 `.inc()` 对应 label
- [x] 4.3 补单测：执行一次后对应 counter 为 1

## 5. 集成与文档

- [x] 5.1 更新 `docs/design/store.md` §10.2 配置项清单表：新增三个配置项行
- [x] 5.2 更新 `docs/design/store.md` 监控项清单表：新增 `store_server_compaction_policy_used` 行
- [x] 5.3 更新 `docs/design/cluster.md` §9 的配置段表与指标段表（与 store.md 对齐）
- [x] 5.4 `devcluster.toml.example` 补上新配置项（注释状态，不改默认行为）

## 6. 端到端测试

- [x] 6.1 新建 `crates/store/tests/compaction_hybrid.rs`：hybrid 模式下首轮触发 CompactAll（无状态文件）、写回状态文件后下一轮走 cost-benefit、手动删状态文件后再次触发 CompactAll
- [x] 6.2 同文件补策略切换测试：配 `incremental` → 走 `compact_if_needed`、配 `cost-benefit` → 走 `compact_cost_benefit`
- [x] 6.3 回归：确认现有 `compaction_async.rs`、`compaction_replication.rs` 测试仍通过（不改引擎层行为）
- [x] 6.4 `cargo test` 全绿 + `cargo clippy` 无告警

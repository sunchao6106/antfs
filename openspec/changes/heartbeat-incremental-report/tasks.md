# tasks — heartbeat-incremental-report

## 1. 协议与摘要
- [x] 1.1 proto:`NodeBeat.replica_digest`/`removed_replicas`、`MasterDirective.resend_full_report`/`delta_report_supported`;重新 codegen。
- [x] 1.2 `master::digest`:FNV-1a 单条哈希 + XOR 合成 + 全集口径;单测(稳定/逐字段敏感/顺序无关/集合变化)。

## 2. master 软状态索引
- [x] 2.1 `soft.rs`:`node_index` 反查;`record_replicas(node, entries, removed, full_report)`;全量替换 O(节点)。
- [x] 2.2 `node_replicas(node)` 节点视图 + `node_digest`;单测索引一致性(全量/增量/removal/摘要口径)。

## 3. master 每拍 O(节点) 化
- [x] 3.1 `store.rs`:`placement_epoch`,apply 对 placement 变更命令 bump(发号不 bump);装快照也 bump。
- [x] 3.2 `reconcile.rs`:`plan_transitions` 走节点 pid 集;`server.rs` `plan_drops` 用 `soft.holds`;单测"应建未建仍下发"。
- [x] 3.3 `server.rs` handler 重排:digest 校验 + `resend_full_report` + `delta_report_supported` + 下行增量(连接级 last_sent)。
- [x] 3.4 `metrics.rs`:`master_heartbeat_resend_requests_total` / `master_heartbeat_full_reports_total`。

## 4. store 增量上报
- [x] 4.1 `heartbeat.rs`:已上报快照 + `diff_report` 差分、全集 digest、握手/重发 AtomicBool 开关、首拍恒全量。
- [x] 4.2 单测:全量/增量差分、removal 检测、状态与 lag 变化敏感性。

## 5. 测试与文档
- [x] 5.1 集成测试 `crates/master/tests/heartbeat_delta.rs`:增量合并 / 显式摘除 / 摘要造假自愈 / 增量下行 / 重连全量下行。
- [x] 5.2 `cargo test --workspace` 全绿 + clippy 0 告警 + fmt 干净。
- [x] 5.3 `docs/design/cluster.md` §7.1 心跳口径 + §7.7 指标表更新;DEVLOG 步骤 35。

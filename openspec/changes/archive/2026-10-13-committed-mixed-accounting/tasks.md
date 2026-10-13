## 1. committed 累加逻辑改造

- [x] 1.1 在 `crates/master/src/server.rs` 的 `placement_usage()` 中，调用 `soft.used_bytes_by_partition()` 获取 `used_map: BTreeMap<PartitionId, u64>`
- [x] 1.2 遍历 partition table 累加 committed 时，按 `partition.state` 分流：`ReadWrite` → 用 `effective_capacity(partition, config)`；`ReadOnly | Sealed` → 若 `used_map.get(&pid)` 存在且 > 0 则用该值，否则退回 `effective_capacity`
- [x] 1.3 补单测：RO 分区用 used_bytes 累加、SEALED 用 used_bytes 累加、RW 仍用 capacity_bytes、混合盘精确累加、used_bytes 缺失退回 capacity_bytes
- [x] 1.4 ~~proptest 不变量~~ 取消：committed 累加是简单分支+加法，不变量已被 mixed_committed_accounting 和 fallback 两个单测精确覆盖，proptest 无额外价值

## 2. 验证

- [x] 2.1 `cargo test -p antfs-master` 全绿
- [x] 2.2 `cargo clippy -p antfs-master --all-targets -- -D warnings` 无告警
- [x] 2.3 `cargo test` 全绿

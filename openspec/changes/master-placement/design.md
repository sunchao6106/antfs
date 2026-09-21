# master-placement 设计(M2)

承 [`docs/design/cluster.md` §7.3](../../../docs/design/cluster.md)。M2a 定案如下;M2b/M2c 列边界。

## 1. 放置在哪算(确定性铁律)

选盘依赖**软状态**(各盘实时 `free_bytes`,心跳上报,各节点不一致)→ **绝不能在 `apply` 里算**(apply 必须跨节点确定,§master-node design 2)。

- **leader** 在提案前用软状态跑纯函数 `select_disks(...)` 得到具体 `Vec<ReplicaPlacement>`;
- 把选定副本**放进 `CreatePartition` 命令**提交 Raft;
- `apply` 只做确定性动作:分配 `next_partition_id` + 写入 `Partitions[id] = PartitionInfo{ state: RW, replicas }`。

## 2. CreatePartition 命令

```
MasterCommand::CreatePartition { replication_factor: u8, replicas: Vec<ReplicaPlacement> }
  → apply: id = next_partition_id; next_partition_id += 1;
           partitions.insert(id, PartitionInfo{ replication_factor, state: ReadWrite, replicas });
  → MasterResponse::PartitionId(id)
```
- 幂等性:`CreatePartition` 天然产生新 id,不重放安全由 client 请求去重承担(M2b:请求侧幂等留待需要时);M2a 只保证 apply 确定 + id 单调不漏。

## 3. 容量感知选盘(纯函数,§7.3)

`select_disks(nodes, usage, rf, partition_cap, near_full_ratio) -> Result<Vec<ReplicaPlacement>, PlacementError>`

输入:
- `nodes`: hard state 的节点身份 + 盘规格(容量/tag)。
- `usage`: 软状态 `(node,disk) -> free_bytes`(心跳)。缺失余量的盘视为不可用(未上报=不放)。

规则(对齐 §7.3):
1. **候选** = 满足 `free_bytes ≥ partition_cap` 且 `free_bytes/capacity > (1 - near_full_ratio)`(即未将满)的盘。
2. **故障域打散**:优先每块选自不同 `rack`(退化到不同 `node`);同一 partition 的 N 副本不落同一 node。
3. **余量相近**(减少大盘被小盘拖累的浪费):候选按 `free_bytes` 排序,取一组彼此相近的;M2a 用「排序后贪心选故障域不同、余量最接近上界」的确定式规则。
4. **不足** → `PlacementError::InsufficientDisks { needed, available }`(不静默降级 rf)。
5. 返回的 `ReplicaPlacement.target_state` = `"ACTIVE"`(Q4:M1/M2 不区分 leader/standby)。

**纯函数**:无 IO、无时钟、无随机 → 属性测试友好(打散、阈值、跳过将满、不足报错、确定性)。

## 4. 配置 / 监控(CLAUDE.md §7,落地同步 cluster.md 表)

- `master.partition_capacity_bytes`(u64,默认 100 GiB=107374182400,≥1):分区容量上界,选盘阈值。
- `master.disk_near_full_ratio`(f64,默认 0.95,(0,1]):盘用量超此比例视为将满,不放新分区(对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage`)。
- 指标:`master_partitions_created_total`(counter)、`master_partition_create_failed_total`(counter,标签 `reason`)。

## 5. 验证(M2a)

- **单测/属性测试**(纯函数):① rf 块盘落不同故障域;② 全部候选 `free ≥ cap` 且未将满;③ 将满盘被跳过;④ 候选不足返回 `InsufficientDisks`;⑤ 同输入多次调用结果一致(确定性)。
- `CreatePartition` apply:id 单调、`Partitions` 正确写入、跨重放确定(复用 M1 崩溃恢复测试模式)。
- `cargo test` 绿 + `cargo clippy` 无告警。

## 6. M2b / M2c(边界,后续)

- **M2b**:`AssignPartition`(只回 RW 分区)、`LookupPartition`(副本位置 + §7.2 状态门控)、`RoutingIndex` 软状态由心跳重建;gRPC + 3 节点集成测试(建分区→跨机架→Assign/Lookup)。
- **M2c**:Q5 自动扩容——后台监测可写容量/分区水位,自动 `CreatePartition`;**护栏**:防抖(水位滞回)、限速(单位时间建分区上限)、总量上限。单测覆盖抖动不触发过量建分区。

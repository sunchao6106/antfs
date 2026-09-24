# design — heartbeat-incremental-report

## 协议

```
message NodeBeat {
  ...
  uint64 replica_digest = 7;          // 覆盖全部持有副本(含未随拍上报的)的 XOR 摘要
  repeated uint32 removed_replicas = 8; // 增量模式下本拍摘除的副本(显式删除)
}
message MasterDirective {
  ...
  bool resend_full_report = 5;   // 摘要不符 → 下一拍全量重发
  bool delta_report_supported = 6; // 握手:master 理解增量心跳
}
```

`full_report=false` 时 `replicas` 只含变化项;**缺项不代表没有**,所以删除必须显式走
`removed_replicas`(DROPPED/OFFLINE 摘除后本地不再持有)。摘要永远覆盖**全集**(口径同
SeaweedFS digest:只覆盖上报项的话 master 无从判断应用增量后是否与节点一致)。

## 摘要口径(`master::digest`)

FNV-1a(手写,不引依赖;`DefaultHasher` 不承诺跨构建稳定,store/master 是独立进程)逐条哈希
`(pid, disk_id, current_state, lag_bytes, used_bytes)`,XOR 合成(顺序无关)。store 对本地全
集算,master 对软状态该节点全集算;不一致 → `resend_full_report` → 下一拍全量 → 收敛。

## master 每拍 O(节点) 化

1. **软状态节点索引**:`Inner.node_index: HashMap<NodeId, HashSet<PartitionId>>`。全量替换经索引
   删旧(O(节点))代替全表 retain;增量合并/removal 同步维护索引。全表读(active/errored/used_bytes
   汇总)保留给 reconciler(5s)与 RPC,不进每拍路径。
2. **placement 派生索引**:`plan_transitions` 要覆盖"placement 有、节点没上报"(丢副本要令其
   重建),需 placement 侧 node→pids。不进 Raft 状态:`RedbStateMachine` 持
   `placement_epoch: Arc<AtomicU64>`,apply 中改动 `partitions` 成员的命令 bump(发号等高频命令
   不 bump);handler 比对 epoch,过期才在 sm 读锁下全量重建一次。稳态零重建,变更后首拍摊还一次。
3. **下行增量**:连接级 `last_sent: HashMap<pid, (state, tombstones_allowed)>`;flag 依赖其他节点
   追平态,每拍仍需对节点持有分区重算(O(节点)×RF),但只下发挥发差分。`last_sent` 按当前持有
   retain,副本离开后重建句柄能重新收到状态。store 端 `set_partition_state` 本就是逐条合并。

## store 增量上报

`ReportState { sent: HashMap<pid, ReplicaBeat>, ... }`:每拍全量收集(本就 O(节点)×O(1),
metrics 同源)→ 算全集 digest → 增量模式发差分 + removed,否则全量。首拍恒全量(重连 = 新任务
= 全量,覆盖新 leader 软状态重建)。

**不量化** `used_bytes`/`lag_bytes`:冷分区逐拍恒定(`valid_data_size` 增量计数、lag 缓存),
热分区每拍变化是正比于真实写入的必要上报;`lag==0` 精确性是墓碑闸门判据。

## 兼容(§3.4)

- 新 master + 旧 store:旧 store 恒全量,digest 校验通过;下行增量对旧 store 成立(合并语义)。
- 旧 master + 新 store:收不到 `delta_report_supported`,永远全量 —— 避免旧 master 把增量 beat
  当全集规划导致 transition 风暴。

## 风险与对策

- **摘要哈希双方口径漂移**:单一 `master::digest` 模块,store 经 `antfs_master` 依赖复用;单测锁死。
- **resend 死循环**(全量后仍不符):已是全量仍不符时 master 只 warn 不再连环要求(仍会置标志,
  但全量响应后 digest 必然收敛,除非哈希 bug —— warn 日志可观测)。
- **丢副本重建回归**:`plan_transitions` 数据源从 beat 内容换成软状态节点视图 + placement 索引,
  单测覆盖"应建未建仍下发"。

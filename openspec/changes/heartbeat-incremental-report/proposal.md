## Why

store 每 3s 全量心跳(所有盘 + 所有副本状态),master 每拍处理是 **O(全集群)**:
`SoftState::record_replicas` 用 `retain` 扫全集群副本表做整体替换,`plan_transitions` 遍历全集群
partition×replica 过滤该节点,下行 `partition_states` 每拍全量重发。节点数 × 分区数上去后,
心跳处理会在 master(单写锁 + 单读锁路径)上平方增长,先于网络带宽成为瓶颈。

而稳态下心跳携带的信息熵趋近于零:停写且无 compaction 的分区,其 `used_bytes`(增量计数器)、
`lag_bytes`(追平后恒 0)、`current_state` 逐拍完全相同 —— 全量机制按信息论上限的数千倍付成本。

参照 SeaweedFS(`weed/server/volume_grpc_client_to_master.go` + `weed/storage/store.go::CollectHeartbeat`):
**周期 tick 保留**(保活 + 下行通道),但每拍只带**增量 + 全量摘要**;master 摘要不符回
`ResendFullVolumeList` 自愈;`VolumeDigestSupported` 握手防新旧混部。

## What Changes

- **proto(纯增量字段,守 §3.4)**:`NodeBeat` 加 `replica_digest`(覆盖全部持有副本的 XOR 摘要)与
  `removed_replicas`(增量模式下显式上报摘除);`MasterDirective` 加 `resend_full_report`(摘要
  不符 → 下一拍全量)与 `delta_report_supported`(版本握手)。
- **store 增量上报**:维护"已上报"快照,稳态每拍只发变化项 + 摘除项 + 摘要;首拍/被要求重发时全量。
  收到 `delta_report_supported` 才切增量(旧 master 会把增量 beat 当全集规划 → transition 风暴)。
- **master 每拍 O(节点) 化**:
  - 软状态加 `node -> partitions` 反查索引,全量替换/增量合并/removal 都 O(节点副本),消除全表 retain;
  - placement 派生 `node -> pids` 索引(epoch 懒重建:只有 placement 变更命令 bump,稳态零重建),
    `plan_transitions` 不再扫全集群;
  - 下行 `partition_states` 改增量下发(连接级 last_sent 比对,store 端本就是逐条合并语义)。
- **摘要自愈**:master 每拍按软状态重算 digest 比对,不符置 `resend_full_report`;首拍/重连恒全量,
  天然覆盖 leader 切换后软状态重建。

## Capabilities

### Modified Capabilities
- `store-server-heartbeat`: 心跳从"每拍全量"改为"增量 + 全量摘要 + 不符重发自愈";判死、指令时延、
  墓碑闸门(`lag==0` 精确性)语义不变。

## Impact

- **proto**:`master.proto` 新增 4 个字段(§3.4 兼容:新 master+旧 store、旧 master+新 store 均安全)。
- **master**:`soft.rs`(节点索引)、`digest.rs`(新,共享哈希口径)、`store.rs`(placement_epoch)、
  `reconcile.rs`(plan_transitions 索引化)、`server.rs`(handler 重排 + 下行增量)、`metrics.rs`。
- **store**:`heartbeat.rs`(ReportState + 增量计算 + 握手/重发开关)。
- **不做量化**:`used_bytes`/`lag_bytes` 逐拍精确上报 —— 冷分区值恒定,热分区变化正比于真实写入;
  `lag==0` 是墓碑回收闸门的判据,不能模糊。

## Non-goals

- 不改判死窗口(`heartbeat_timeout_secs` + 迟滞)与心跳周期(3s)。
- 不动 DiskBeat/statvfs 路径(盘数量级小,每拍全量带盘信息代价可忽略)。
- reconciler 周期全扫(5s)保留 —— 它不在每拍热路径。

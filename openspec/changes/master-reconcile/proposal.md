## Why

M1/M2 让 master 会选主、注册节点、放置分区。但放置只是**应然**(ideal placement):副本被放下后没人驱动它们从 `BOOTSTRAP` 追平到 `ACTIVE`,节点挂掉后也没人补副本恢复冗余。M3 补上 master 的**调谐循环**(reconciler,承 [`docs/design/cluster.md` §7.3](../../../docs/design/cluster.md)):让**实然**(current)向**应然**(target)收敛。

调谐全部在 master 侧,**不依赖 store server**:用心跳的存在/缺席判活、用心跳携带的副本 `current_state` 驱动转移下发。store server 后续 change 才真正执行转移与拉复制;M3 只做 master 的**决策与下发**(D1:master 只决策,不搬数据)。

## What Changes

- **副本状态上报**:`NodeBeat` 扩 `replicas`(每副本 partition_id / disk_id / current_state / lag_bytes);master 维护软状态 `ReplicaCurrent`。
- **转移下发(反应式)**:节点心跳上报各副本 current_state,master 对比 placement 的 `target_state`,把不一致的副本回以 `MasterDirective.transitions`(partition/disk/target)。
- **失联检测 + 再复制(主动式,leader-only 后台 loop)**:节点心跳超时视为 down;其承载的分区若存活副本 < RF,master 选一块新盘(排除已有节点)`AddReplica`(target=ACTIVE,经 BOOTSTRAP 追平)补回冗余。
- **新 Raft 命令** `AddReplica { partition_id, replica }`:确定性 apply(向 placement 追加副本)。
- **新配置** `master.reconcile_interval_secs`;**新指标** `master_transitions_issued_total` / `master_rereplications_total`。

**M3 不做**:副本转移与拉复制的**执行**(store server change);decommission / 删除死副本(M4);EC(M4)。

## Capabilities

### New Capabilities

- `master-reconcile`: 副本状态调谐——current↔target 转移下发、节点失联检测、再复制补副本(`AddReplica`)、`ReplicaCurrent` 软状态心跳重建。

### Modified Capabilities

<!-- 复用 master-raft-core / master-control-plane / master-placement;本 change 新增 reconcile 能力。 -->

## Impact

- **crate**:`crates/master` 新增 `reconcile.rs`(纯决策);`types.rs` 扩 `AddReplica`;`soft.rs` 加 `ReplicaCurrent`;`server.rs` 心跳处理 + 后台 reconciler loop;`proto` 扩 `ReplicaBeat`/`Transition`/`Task`。
- **决策**:转移=反应式(心跳同步返回),失联再复制=主动式(后台 loop);均 leader-only。选盘复用 M2 `place_one_partition`/`select_disks`,再复制排除已在该 partition 的节点(故障域打散)。
- **保留契约**:master 只决策不搬数据(D1);apply 确定(AddReplica 的具体副本在 leader 侧选好再进 Raft,§master-placement design 1);Raft 边界不变(Q2)。

## Non-goals

- 不实现 store server / 转移执行 / 拉复制。
- 不实现 decommission / 删死副本 / EC(M4)。
- 不改动 M1/M2 已定契约。

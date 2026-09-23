# store-replication-cursor-revalidate

拉复制游标的重定位(对齐 Ambry `revalidateStoreFindToken`):**两层身份**(段身份 = 位置 + 代次、副本 incarnation)
+ 位置 / 身份双重校验 + 追平判据收紧 + 取段按段身份寻址 + 墓碑回收闸门。

> 各条 Requirement / Scenario 后面附**真实存在的测试**;没有可测用例的条目会显式写明"范围约束"。
> 线上尚未启用,**不做兼容层**:所有 SHALL 都只按新格式实现,不为旧格式 / 旧节点留分支。

## ADDED Requirements

### Requirement: 段身份 = 位置 + 代次

每个 log 段 SHALL 具有一个身份 = **位置序号 + 代次**(`LogSegmentName`),文件名 SHALL 为
`{position}_{generation}_log.seg`,段头 SHALL 自述 generation(**v3** 新增字段),打开时 SHALL 校验段头自述的
代次与文件名一致,不一致即拒绝打开。同一 log 目录内代次 SHALL **单调递增**(滚动新段 = 当前最大代次 + 1);
全库 `compact` 建立的新 log SHALL 接着**旧 log 的最大代次**往后取(段位置从 0 重来,但代次接着涨)。故
**段位置 MAY 被复用,段身份 SHALL NOT 被复用**。索引值 SHALL 保持 21 B 定长布局不变(代次不进索引,只活在
段名与复制游标里)。

测试:`crates/blob/src/log.rs::generation_is_monotonic_and_never_reused`、
`crates/blob/src/log.rs::open_rejects_generation_mismatch_between_header_and_name`、
`crates/blob/src/blob.rs::full_compact_changes_identity_and_revalidates_old_cursor`。

#### Scenario: 滚动换代次且身份不复用

- **WHEN** 活跃段写满、log 滚动到新段
- **THEN** 新段的代次 SHALL 严格大于本目录此前所有段的代次,段身份 SHALL NOT 与任何已用过的 (位置, 代次) 重合

#### Scenario: 位置复用被代次识破

- **WHEN** 全库 `compact` 后新 log 从位置 0 重新建段,而复制侧手里还持有旧 log 位置 0 的游标
- **THEN** 该游标 SHALL 因**代次不同**被判作废(而不是"位置还在 ⇒ 合法"),重扫后数据 SHALL 补齐

### Requirement: 副本 incarnation 的产生与落盘

每个副本 SHALL 具有一个**副本级身份**(incarnation),落盘在 `{store_dir}/LOG_INCARNATION`。文件缺失时
SHALL 用**本机当前时刻(纳秒)**兜底(并保证 ≥ 1,**0 保留给"未分配"**)且**持久化**,使后续打开读到同一个值。
兜底值 SHALL NOT 取自段头(尤其 `created_ns`)—— 物理拷贝来的段诞生在对端,那是**对端**的身份,用它会与
对端撞号,使"重建 ⇒ 身份必换"在申领未成功时失效。采纳 master 颁发的权威值时 SHALL 改写该文件并 **fsync**;
与本地现值不同即 SHALL 视为"此前的游标作废"(保守方向)。

测试:`crates/blob/src/blob.rs::incarnation_is_persisted_and_adoptable`、
`crates/blob/src/blob.rs::bootstrapped_replica_identity_is_not_inherited_from_the_peer`、
`crates/blob/src/blob.rs::recreated_replica_gets_a_new_incarnation_file`、
`crates/blob/src/blob.rs::missing_incarnation_file_is_healed`。

#### Scenario: 重建副本必换 incarnation

- **WHEN** 已存在副本的目录被删除后重新建库
- **THEN** 新副本的 incarnation SHALL 与旧副本不同

#### Scenario: 身份文件缺失时自愈

- **WHEN** 打开一个没有 `LOG_INCARNATION` 的副本目录(物理 bootstrap 只拷了段)
- **THEN** SHALL 取一个 ≥ 1 的**本机**值并写盘,再次打开 SHALL 读到同一个值;该值 SHALL NOT 等于对端的
  incarnation(拷来的段头里带着对端的诞生时刻,不得沿用)

### Requirement: 从零建副本时向 master 申领 incarnation

master SHALL 提供 `AssignReplicaIncarnation(partition_id, node_id)`,该 RPC SHALL **仅在 leader 上生效**且
SHALL 经 **Raft 提交**;**apply 侧** SHALL 发放一个**全新、更大的**号(不是取回旧值),`0` 保留给"未分配"。
store SHALL 在两条路径上申领:①`ensure_replica_with_origin` 报告**盘上原本没有**这份副本(从零建出)时
——OFFLINE 摘除后挂回、进程重启重开、以及幂等心跳重复下发 SHALL NOT 申领(判据取盘上状态,不取内存句柄);②物理 `install_bootstrap` 成功(`bootstrap.segments > 0`)之后,在**新句柄**上采纳。
申领失败(网络不通 / 非 leader / 该副本不在 placement)SHALL NOT 影响建副本与对外服务 —— 只告警并**保留本地
兜底值**。

测试:`crates/store/tests/incarnation_and_gate.rs::creating_a_replica_claims_its_incarnation_from_master`、
`crates/store/tests/incarnation_and_gate.rs::repeated_active_directives_do_not_re_claim_the_incarnation`、
`crates/store/tests/incarnation_and_gate.rs::offline_then_active_keeps_the_incarnation`、
`crates/master/tests/replica_incarnation.rs::assign_replica_incarnation_replaces_with_a_bigger_value`、
`crates/master/tests/replica_incarnation.rs::lookup_partition_incarnation_matches_state_machine`、
`crates/master/tests/replica_incarnation.rs::three_node_cluster_agrees_on_incarnations`。

#### Scenario: 每次申领都换新号

- **WHEN** 对同一个 `(partition, node)` 连续两次调用 `AssignReplicaIncarnation`
- **THEN** 第二次的号 SHALL 严格大于第一次,且 placement 里的 incarnation SHALL 被换成新值(不是取回旧值)

#### Scenario: 幂等心跳不重复申领

- **WHEN** master 把同一份副本的 `ACTIVE` 转移指令重复下发,而本地**早就持有**这份副本
- **THEN** SHALL NOT 再次申领身份(否则身份每拍换新,对端手里的游标被反复作废)

#### Scenario: 摘除后挂回不换身份

- **WHEN** 该副本收到 `OFFLINE`(只摘内存句柄、盘上数据与身份文件保留)后又收到 `ACTIVE`
- **THEN** SHALL NOT 申领新号(那不是"从零建副本"),本地身份与盘上身份文件 SHALL 保持原值

#### Scenario: 申领失败不影响建副本

- **WHEN** 申领 RPC 失败(master 不可达 / 非 leader / 拒发)
- **THEN** 副本 SHALL 照常建出并对外服务,身份 SHALL 保持本地兜底值

### Requirement: 游标重定位(段身份 + 副本 incarnation 两道)

`FindToken::Offset` SHALL 携带**段身份**(位置 + 代次)+ 段内偏移。`find_entries_since` SHALL 在开始扫描前
revalidate:段不在段表 ⇒ `segment_missing`;位置上有段但**代次不同** ⇒ `segment_generation_mismatch`;偏移越过
该段数据区 ⇒ `offset_out_of_range`。三种情形 SHALL **重置为从头重扫**(而非返回空批),并经 `FindInfo.token_reset`
如实报告;扫描路径 SHALL NOT 把"段不存在"静默视为"已越过"。

元数据端点(`GET /partitions/{pid}/replica-metadata`)SHALL 另外校验游标声明的 **incarnation** 与本副本的
incarnation:不符 ⇒ 从头重扫并报告 `incarnation_mismatch`。请求参数 `pos` / `gen` / `off` / `incarnation`
**四件套齐全才可信**:`pos` / `gen` / `off` 任一非空却缺 `incarnation`,或声明的 incarnation 与本副本不符 ⇒
一律按不可信处理(从头重扫并报 `incarnation_mismatch`);`pos` / `gen` / `off` 三件套残缺 ⇒ 游标按 `Uninitialized`
处理(也是从头重扫,但 SHALL NOT 报重定位原因);四件套全空 = 全新拉取,SHALL NOT 算作重定位。

测试:`crates/blob/src/blob.rs::revalidate_reports_reasons_and_keeps_valid_cursors`(三条位置类理由与理由短名)、
`crates/blob/src/blob.rs::reused_segment_positions_are_detected_by_generation`、
`crates/store/tests/compaction_replication.rs::peer_replica_recreated_resets_session_and_keeps_data`。

#### Scenario: 段已不存在

- **WHEN** 用一个指向已删段的游标请求增量元数据
- **THEN** SHALL 从头返回全部条目,并报告 `segment_missing`

#### Scenario: 同位置、不同代次

- **WHEN** 游标指向的位置上有段,但代次与该游标声明的不符(段被 compaction 重写)
- **THEN** SHALL 从头返回全部条目,并报告 `segment_generation_mismatch`

#### Scenario: 偏移越过数据区

- **WHEN** 游标的段内偏移大于该段的数据区末端
- **THEN** SHALL 从头返回全部条目,并报告 `offset_out_of_range`

#### Scenario: 副本换代(重建 / 换盘)

- **WHEN** 游标声明的 incarnation 与本副本的 incarnation 不一致
- **THEN** SHALL 从头返回全部条目,并报告 `incarnation_mismatch`

#### Scenario: 合法游标不重定位

- **WHEN** 游标的段身份能在当前段表里认出、偏移也在数据区内,且 incarnation 相符
- **THEN** SHALL NOT 重定位,只返回该游标之后的增量

### Requirement: 取段端点按段身份寻址,并据此丢弃混合拷贝

`GET /partitions/{pid}/segments/{position}_{generation}` SHALL 按**段身份**寻址:对端认不出该段身份 ⇒ **404**;
名字畸形(缺代次 / 非数字)⇒ **400**。`GET /partitions/{pid}/segments` SHALL 返回
`{incarnation, segments: [{position, generation, data_end}]}` —— 清单 SHALL 带上对端 incarnation 与每段的代次。

物理 bootstrap SHALL 在**每条**下列判据成立时**丢弃整份拷贝**并回落**纯逻辑拉取**(幂等、必然收敛),
SHALL NOT 装入跨代次的混合快照:①取某段 404 / 出错;②取回字节**自述的代次**与清单不符;③拷完再列清单时
对端 incarnation 变了。

测试:`crates/store/tests/data_plane.rs::segment_endpoint_addresses_segments_by_identity`(200/404/400)、
`crates/store/tests/compaction_replication.rs::physical_bootstrap_discards_copy_when_peer_rewrites_midway`。

#### Scenario: 清单里的段身份可取,别的代次不可取

- **WHEN** 按 `/segments` 清单里的 `{position}_{generation}` 取段
- **THEN** SHALL 返回 200,且字节自述的代次与清单一致;同位置、**别的**代次 SHALL 返回 404;畸形名 SHALL 返回 400

#### Scenario: 拷段途中对端整库重写

- **WHEN** 分段拷贝期间对端重写了 log(段身份变或副本换代)
- **THEN** SHALL 丢弃这份拷贝(`bootstrap.segments == 0`)并回落纯逻辑拉取,最终与对端逐 key 一致

### Requirement: 无进展兜底

同一 `(partition, peer)` 的续拉会话(`PullSession`)在**连续两轮**"游标未推进且未 apply 任何条目"时 SHALL 丢弃
本地游标,下一轮从起点重拉,并记录 `no_progress`。会话状态(游标 + 对端 incarnation + 无进展计数)SHALL 由
**生产复制 loop 与集成测试共用同一份实现**。

测试:`crates/store/src/replication.rs::pull_session_advances_on_reset_incarnation_change_and_stall`。

#### Scenario: 连续无进展后从头重拉

- **WHEN** 连续两轮拿到相同游标且没有可 apply 的条目
- **THEN** SHALL 丢弃游标并记 `no_progress`,下一轮从 log 起点重新拉取

### Requirement: 墓碑回收闸门

`PartitionStateInfo` SHALL 携带 `tombstones_reclaim_allowed`,其语义 SHALL 为"该分区**所有副本**都已追平"
(单个副本已追平 = 节点存活 **且** 上报 `current_state == "ACTIVE"` **且** `lag_bytes == 0`;缺上报 / 节点 down 均
算未追平)。placement 为空时 SHALL 返回 `false`(看不见 ≠ 已传播)。store SHALL 把该值落到
`ReplicaHandle.tombstones_reclaim_allowed`(**初值 `false`**)并喂给 `CompactPolicy.tombstones_allowed`:为 `false` 时
墓碑 SHALL **一律照搬**(连已过保留期的也不回收)。**单机模式**(未接 master,无其他副本可滞后)SHALL 恒放行。
定时 compaction 与手动 compact SHALL 共用同一判据。

测试:`crates/master/src/reconcile.rs::tombstones_reclaim_only_when_every_replica_caught_up`、
`crates/master/src/reconcile.rs::tombstones_reclaim_denied_without_replicas`、
`crates/master/tests/tombstone_gate.rs::tombstones_reclaim_gate_follows_replica_catch_up`、
`crates/store/tests/incarnation_and_gate.rs::tombstone_gate_opens_when_master_allows`、
`crates/store/tests/incarnation_and_gate.rs::tombstone_gate_closes_when_master_says_not_allowed`。

#### Scenario: 还有副本没追平时不放行

- **WHEN** 某分区有一个副本处于 `BOOTSTRAP` / 上报 `ERROR` / 仍有滞后字节 / 本拍未上报
- **THEN** 闸门 SHALL 为 `false`,该分区的墓碑 SHALL NOT 被回收

#### Scenario: 未得到 master 放行前不回收

- **WHEN** store 刚建出一份副本、还没收到任何 master 放行
- **THEN** 句柄上的闸门 SHALL 是 `false`(初值 = 没证据就不回收)

#### Scenario: master 放行后回收

- **WHEN** master 下发 `tombstones_reclaim_allowed: true`
- **THEN** 句柄上的闸门 SHALL 变 `true`,后续 compaction SHALL 按保留期回收墓碑

### Requirement: 游标重定位指标

SHALL 暴露 `store_server_replication_cursor_resets_total{reason}`,其 `reason` 取值集合 SHALL 恰为
`incarnation_mismatch` / `segment_generation_mismatch` / `segment_missing` / `offset_out_of_range` / `no_progress`;
每次游标被重定位 SHALL 按原因各计一次。

测试:`crates/blob/src/blob.rs::revalidate_reports_reasons_and_keeps_valid_cursors`(断言四个理由短名)、
`crates/store/src/replication.rs::pull_session_advances_on_reset_incarnation_change_and_stall`(断言会话返回的标签)、
`crates/store/tests/compaction_replication.rs::background_replicator_recovers_after_peer_compact`(真实 loop 上读被测
节点自己的 `Registry`)。

#### Scenario: 重定位按原因打点

- **WHEN** 一轮拉复制中游标被引擎或服务层判为不可用
- **THEN** `store_server_replication_cursor_resets_total` SHALL 以对应 `reason` 增 1

### Requirement: 不提供新旧格式兼容层

线上尚未启用,不存在新旧节点并存,故实现 SHALL NOT 引入任何回滚 / 兼容分支(不认旧裸数组清单、不加可选字段
回落、不保留旧段名 / 旧段头解析)。

**范围约束性条目,无独立可测用例** —— 它是"没有这些代码路径"这条否定性事实:上列各条测试都在新格式上跑,旧格式
本就无人构造。

## MODIFIED Requirements

### Requirement: 追平判据

追平判据(`caught_up_with_peer`)SHALL 在原有条件(对端无该分区副本;或本轮无待 apply 条目)之外,增加**前置条件**:
本轮**没有** `cursor_reset_reason`。`sync_round` SHALL 接收上一次见到的对端 incarnation,并在**对端 incarnation
发生变化的那一轮** SHALL NOT 判追平、SHALL NOT 置 `ACTIVE`(那轮"末端一致"是在作废的坐标系上算出来的)。与
peer 游标位置的比较(`BlobStore::has_content_after`)SHALL **只比位置**:peer 游标的段身份属于**对端**坐标系,本地
无从校验;拿它进 `find_entries_since` 会被重定位判定误伤。

测试:`crates/store/tests/compaction_replication.rs::batched_drain_across_peer_rewrite_is_never_false_caught_up`、
`crates/store/tests/compaction_replication.rs::catchup_survives_peer_full_compact_between_rounds`、
`crates/store/tests/compaction_replication.rs::empty_peer_never_yields_false_catchup`、
`crates/store/tests/compaction_replication.rs::local_compaction_mid_drain_keeps_the_peer_cursor_usable`、
`crates/store/src/replication.rs::peer_full_compaction_between_rounds_is_revalidated_and_converges`。

#### Scenario: 重定位那轮不转正

- **WHEN** 一次排空过程中对端重写了 log(批间发生),本轮拿到重定位信号
- **THEN** 该副本 SHALL 保持 `BOOTSTRAP`,不得转 `ACTIVE`;下一轮身份稳定且真正追平后 SHALL 正常转 `ACTIVE`

#### Scenario: 对端换代那轮不判追平

- **WHEN** 本轮发现对端 incarnation 与上次不同(副本被重建 / 换盘)
- **THEN** SHALL NOT 判追平、SHALL NOT 置 `ACTIVE`,并记一次 `incarnation_mismatch`;后续轮次从对端给的新游标继续

#### Scenario: 已追平后对端重写

- **WHEN** 副本已 `ACTIVE`,对端随后全库 compact 并写入新数据
- **THEN** 后续轮次 SHALL 通过重定位把新数据补齐,本地已有数据 SHALL NOT 丢失,`ACTIVE` SHALL NOT 回退

#### Scenario: 对端变空而本地有数据

- **WHEN** 对端副本被重建为空,本地仍持有对端没有的数据
- **THEN** SHALL NOT 判追平(保守留在 `BOOTSTRAP`),本地数据 SHALL NOT 被删除

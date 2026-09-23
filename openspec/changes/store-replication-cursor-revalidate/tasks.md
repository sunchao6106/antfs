# tasks — store-replication-cursor-revalidate

## 1. 变更文档
- [x] 1.1 proposal / spec delta(`specs/store-replication-cursor-revalidate/spec.md`)。

## 2. 第一层身份:段身份 = 位置 + 代次(`crates/blob`)
- [x] 2.1 `types.rs`:`LogSegmentName{position, generation}`;文件名 `{position}_{generation}_log.seg`(`filename`)
      与反向解析(`parse`);`LogOffset` 保持 21 B 定长打包(**代次不进索引**)。
- [x] 2.2 `log_segment.rs`:段头升到 **v3**,新增自述 `generation` 字段;`LogSegment::generation()`;
      `peek_generation(&[u8])` 供取段方核字节自述代次;打开时校验段头与文件名一致(不符即 `Corrupt`)。
- [x] 2.3 `log.rs`:`Log::create(.., first_generation)`;滚动新段代次 = `max(gen) + 1`;
      **全库 compact 的 `first_generation` = 旧 log 的最大代次 + 1**(段位置复用靠代次识破);
      `generation_of(position)` / `max_generation()` / `sealed_segment_names()`;`read_segment_prefix(LogSegmentName)`
      按**段身份**(位置 + 代次)寻址(认不出即报错)。
- [x] 2.4 `blob.rs`:`BlobStore::sealed_segments()` 回 `(LogSegmentName, data_end)`;
      `read_segment_prefix(LogSegmentName)`;全库 `compact` 走"新首段代次 = 旧最大代次 + 1"。

## 3. 第二层身份:副本 incarnation(`crates/blob` + `crates/store`)
- [x] 3.1 `blob.rs`:常量 `LOG_INCARNATION_FILE`(u64 文本);读 / 写(写后 **fsync**);`BlobStore::incarnation()`
      访问器;`BlobStore::adopt_incarnation()`(值变化才改写并落盘 + `info` 日志)。
- [x] 3.2 `blob.rs::ensure_incarnation`:文件缺失 ⇒ 用**本机当前时刻**(纳秒,≥ 1)兜底并落盘
      (物理 bootstrap 只拷段、换盘后重来都走这条路)。**刻意不从段头取 `created_ns`** —— 拷来的段诞生在
      对端,那是**对端**的兜底身份,用它会与对端撞号,"重建 ⇒ 身份必换"会失效;为此删掉了无人再用的
      `Log::first_created_ns`。
- [x] 3.3 `store/src/replica.rs`:`install_bootstrap` 会整个换掉本地句柄,故采纳身份必须在**新句柄**上做
      (调用点在复制 loop 里,见 §5.10)。

## 4. master 侧:身份发放与墓碑闸门(`crates/master`)
- [x] 4.1 `proto/master.proto`:`rpc AssignReplicaIncarnation` + `AssignIncarnationRequest{version, cluster_secret,
      partition_id, node_id}` / `AssignIncarnationResponse{ok, incarnation, leader_addr, error}`;`ReplicaPlacement`
      线格式增 `incarnation`;`PartitionStateInfo` 增 `tombstones_reclaim_allowed`。
- [x] 4.2 `types.rs`:`ReplicaPlacement.incarnation`(`0` = 未分配)+ `MasterState.next_incarnation`(从 1 起,
      `#[serde(default)]` 兜旧快照);`assign_incarnation()` **只在 apply 侧调用**(号从已提交状态取)。
- [x] 4.3 `types.rs`:`MasterCommand::AssignReplicaIncarnation` / `MasterResponse::Incarnation`;
      apply 语义 = 该 `(partition, node)` 在 placement 里就**换一个更大的新号**(不在则回 `incarnation: None`,
      状态不变);建 / 加副本、迁移补副本也各自发新号(与 `incarnation` 提案字段解耦)。
- [x] 4.4 `server.rs`:`assign_replica_incarnation` 实现 —— **leader-only**(非 leader 回 `leader_addr` + 错误)
      + `client_write` 经 **Raft 提交**;`reconcile::tombstones_reclaim_allowed`(单副本判据 = 存活 且 `ACTIVE`
      且 `lag == 0`;空 placement 不放行)+ `plan_partition_states` 里逐分区算出闸门随指令下发。

## 5. 引擎侧与复制侧重定位(`crates/blob` + `crates/store`)
- [x] 5.1 `blob.rs`:`FindToken::{Uninitialized, Offset{segment, offset}}` + `at` / `offset()` / `segment_name()`;
      `TokenResetReason{SegmentMissing, SegmentReplaced, OffsetOutOfRange, IncarnationMismatch}` + `as_str()`
      (`segment_missing` / `segment_generation_mismatch` / `offset_out_of_range` / `incarnation_mismatch`)。
- [x] 5.2 `blob.rs`:`FindInfo.token_reset`;`find_entries_since` 入口 `revalidate_token`(段存在性 + **代次一致**
      + 偏移边界),重定位后从起点重扫;`scan_candidates` 对"段不存在"改为报错(不再静默当已越过);
      `bytes_read_to` 适配新游标。
- [x] 5.3 `blob.rs`:`has_content_after(LogOffset)` —— 本地探测**纯位置**比较,不校验段身份 / 不触发重定位。
- [x] 5.4 `store/replication.rs`:`MetadataResponse{token_position, token_generation, token_offset, log_incarnation,
      token_reset}` + `from_find(info, log_incarnation)`;`token_from(pos, gen, off)`(三件套不全 ⇒ `Uninitialized`);
      `HttpPeer::with_incarnation`(请求带 `&incarnation=`)并在每次响应后自我学习 `log_incarnation`。
- [x] 5.5 `store/server.rs`:`replica_metadata` 服务层校验 —— 游标声明缺 incarnation 而其余非空、或声明值与
      本副本不符 ⇒ 从头重扫 + `IncarnationMismatch`(`MetaParams{pos, gen, off, incarnation, max_bytes}`)。
- [x] 5.6 `store/replication.rs`:`SealedListing{incarnation, segments}` + `SegmentInfo{position, generation,
      data_end}`;`bootstrap_from_peer` 三条丢弃判据(取段失败 / 段头自述代次不符 / 拷完 incarnation 变了)⇒
      丢弃拷贝回落纯逻辑拉取;续拉游标 = 最高封存段末尾并绑定 peer 的段身份。
- [x] 5.7 `store/server.rs`:取段端点改为 `GET /partitions/{pid}/segments/{name}`,路径参数 = **段身份**
      `{position}_{generation}`(解析失败 400、认不出 404、未建副本 404)。
- [x] 5.8 `store/replication.rs`:`SyncOutcome{cursor_reset_reason, peer_incarnation}`;`caught_up_with_peer` 前置
      "本轮没有 `cursor_reset_reason`";`sync_round` 增 `prior_incarnation`,`对端 incarnation 变了那一轮绝不判
      ACTIVE`;`local_has_nothing_past` 只比位置。
- [x] 5.9 `store/replication.rs`:`PullSession{token, incarnation, stalled, resets}` + `prior_token` /
      `prior_incarnation` / `advance`(重定位原因打标 + 对端换代记 `incarnation_mismatch` + 连续两轮无进展
      记 `no_progress` 并丢游标);生产 loop 每 `(pid, peer)` 持一个会话。
- [x] 5.10 `store/heartbeat.rs::claim_incarnation` —— 复用同一个 `MasterClient` 调
      `AssignReplicaIncarnation`,成功则 `adopt_incarnation`,失败只 warn 并保留本地兜底值;
      调用点:①心跳里 `ensure_replica`(仅当**本地本来没有**该副本)②物理 `install_bootstrap` 成功
      (`bootstrap.segments > 0`)后的**新句柄**。
- [x] 5.11 `store/metrics.rs`:`store_server_replication_cursor_resets_total{reason}`(生产 loop 与会话共用)。

## 6. 墓碑回收闸门(`crates/store`)
- [x] 6.1 `replica.rs`:句柄字段 `tombstones_reclaim_allowed`(**初值 `false`**,= 没证据就不回收)、
      `set_tombstones_reclaim_allowed`(幂等)、`tombstones_reclaim_allowed()`。
- [x] 6.2 `heartbeat.rs`:随 `partition_states` 吸收闸门(`replicas.set_tombstones_reclaim_allowed`);
      本地没该副本时忽略(下拍还会重发)。
- [x] 6.3 `worker.rs`:`tombstones_allowed(standalone, handle)` —— 单机模式恒放行,集群模式看句柄;
      `policy_from(.., tombstones_allowed)` 填入 `CompactPolicy.tombstones_allowed`。
- [x] 6.4 `blob.rs`:`CompactPolicy.tombstones_allowed` + `with_tombstones_allowed`;`is_reclaimable` 在闸门关闭时
      **墓碑一律照搬**(连过保留期的也不回收);`preserve_legacy` 置 `false`。
- [x] 6.5 `server.rs`:手动 compact 走 `manual_compact_policy(tombstones_allowed)` —— 与定时 compaction 共用同一判据
      (运维强制不影响"墓碑要不要回收")。

## 7. 兼容层(不做)
- [x] 7.1 **不做**任何新旧格式 / 旧节点兼容分支:线上未启用,不存在新旧节点并存。不认旧裸数组清单、
      不加可选字段回落、不保留旧段名 / 旧段头解析;(初版设计里的"坐标系身份文件"、盘级计数器、
      以首段 `created_ns` 充当副本身份,均已按最终方案替换 —— 见 `docs/DEVLOG.md` 步骤 21 的"被否决的方案")。

## 8. 测试
- [x] 8.1 单测(`crates/blob/src/blob.rs`):incarnation 落盘 / 采纳 / 重建换号 / 缺文件自愈;
      **bootstrap 拷来的段不得把对端身份带过来**(`bootstrapped_replica_identity_is_not_inherited_from_the_peer`,
      在"用首段 `created_ns` 兜底"的旧实现上必红);
      revalidate 三分支与理由短名;合法游标不重定位;全库 compact 换段身份(代次接续)且旧游标必被重定位;
      "位置复用被代次识破"(钉住设计前提)。
- [x] 8.2 单测(`crates/blob/src/log.rs`):代次单调且永不复用;段头与文件名代次不符即拒绝打开。
- [x] 8.3 单测(`crates/store/src/replication.rs`):`local_has_nothing_past` 的空分区 / 滞后本地;
      peer 无副本只在"本轮没拉到东西"时算追平;`PullSession::advance` 三态(重定位 / 对端换代 / 无进展丢游标);
      两轮之间对端全库 compact ⇒ 重定位 + 新数据补齐 + 那轮不判追平。
- [x] 8.4 单测(`crates/store/src/worker.rs::tombstone_reclamation_needs_the_master_gate`):闸门关时墓碑照搬 /
      放行后真的回收 / 重复置位幂等 / 单机模式(未接 master)恒放行。
- [x] 8.5 单测(`crates/master/src/reconcile.rs`):全副本追平才放行(未追平 / ERROR / 有滞后 / 未上报都不放行);
      空 placement 拒绝。
- [x] 8.6 集成(**边复制边 compaction 矩阵**,`crates/store/tests/compaction_replication.rs`,**9 场景**,真 HTTP):
      ①追赶中 + 对端全库 compact ②追赶中 + 对端增量 compaction ③对端副本 drop 后重建(身份必变大、新数据拉到、
      本地不丢、ACTIVE 不回退)④已追平后 compact + delete 传播 ⑤对端变空而本地有数据(绝不误判追平)
      ⑥真实后台复制 loop 恢复 + 指标断言(读被测节点自己的 `Registry`)
      ⑦**一次排空过程中的批间重写**(`sync_round` 走真链路 + 限批 PeerSource,断言**那一轮绝不转 ACTIVE**)
      ⑧**物理 bootstrap 拷段途中对端整库重写**(丢弃拷贝 `bootstrap.segments == 0` + 回落逻辑拉取后逐 key 一致)
      ⑨**本端(接收方)在追赶途中做全库 compaction**:同一对端游标继续用、不得触发重定位、不得少拉,
      `max_segment_generation` 接续、最终逐 key 一致。
- [x] 8.7 集成(`crates/store/tests/incarnation_and_gate.rs`,**5 场景**):建副本申领身份并落盘
      `partition_0/LOG_INCARNATION`;重复 `ACTIVE` 指令**不重复申领**;`OFFLINE` 挂回**不换身份**;
      闸门关;闸门开。配套单测 `crates/store/src/replica.rs::ensure_replica_origin_reports_only_from_scratch_creations`
      (申领判据落在**盘上**:从零建 true;内存命中 / OFFLINE 挂回 false;DROPPED 后重建 true)。
- [x] 8.8 集成(`crates/store/tests/data_plane.rs::segment_endpoint_addresses_segments_by_identity`):
      清单带 `incarnation` 与每段 `{position, generation, data_end}`;按段身份 200 且字节自述代次一致;
      同位置别的代次 404;畸形名 400;不存在的段 / 未建副本 404。
- [x] 8.9 集成(`crates/master/tests/replica_incarnation.rs`,5 场景):首建 / 再加副本 / 迁移迁入各发新号且
      全局严格递增、互不相同、≥ 1;`AssignReplicaIncarnation` 每次换一个更大的新号并落进 placement;
      不在 placement 的 `(pid, node)` 拒发(`incarnation == 0`)且**不烧号**;
      `LookupPartition` 报的 incarnation 与状态机逐副本一致;apply 确定性(同命令序列同结果,
      含 `next_incarnation`);三节点经 Raft 对 incarnation 完全一致。
- [x] 8.10 集成(`crates/master/tests/tombstone_gate.rs::tombstones_reclaim_gate_follows_replica_catch_up`):
      闸门随副本上报(ACTIVE + lag == 0)翻转换向。
- [x] 8.11 测试夹具:`crates/store/tests/common/mod.rs` 的 `FakeMaster` 支持 `AssignReplicaIncarnation`;
      既有 `ReplicaPlacement` 构造点补 `incarnation` 字段(`crates/master/tests/{balance,reconcile,blob_ids}.rs`、
      `crates/frontend/tests/rest.rs`、`crates/common/router/tests/e2e.rs`、`crates/store/tests/replica_drop.rs` 等)。
- [x] 8.12 回归:既有 `replication.rs` / `replica_state.rs` / `bootstrap` / `data_plane.rs` 场景全绿;
      `cargo test -p antfs-blob -p antfs-store -p antfs-master` 全绿。

## 9. 文档
- [x] 9.1 `store/src/replication.rs` 模块头:两层身份 + 追平判据收紧 + 已知边界(整段重写)。
- [x] 9.2 `docs/design/store.md`:§3 + 新增 §9.2(判定表 / 不变量 / 观测 / 覆盖测试)。
- [x] 9.3 `docs/design/cluster.md`:S3a 加固段 + 监控项清单补 `store_server_replication_cursor_resets_total`。
- [x] 9.4 `docs/DEVLOG.md` 步骤 21(按最终实现重写,含被否决方案的否掉理由)。

- [x] 9.5 清掉两处与最终实现脱节的**代码注释**(不改行为):`crates/store/src/replication.rs`
      模块头"旧版本 peer 不回身份"的滚动升级边界说明(与"不做兼容层"不一致,已改为"无新旧节点并存 ⇒ 不做兼容分支");
      `crates/blob/src/blob.rs::find_entries_since` 文档里"身份由服务层贴回,见 `FindToken::epoch`"
      (`FindToken` 已无该字段,已改为"游标自带段身份;incarnation 由服务层/会话另持")。
      同族顺手统一:`crates/blob/src/log.rs::open` 的注释(不再提 `LOG_EPOCH`)、`blob.rs` 测试模块的段标题、
      单测名 `pull_session_advances_on_reset_epoch_change_and_stall` → `..._incarnation_change_...`。

## 后续(不在本 change)
- [ ] 真集群 / 多副本环境下跑一遍迁移 + compaction 的端到端验证(本 change 的集成用例都是进程内 + 双节点 HTTP)。

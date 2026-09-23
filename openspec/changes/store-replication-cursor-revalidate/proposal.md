## Why

拉复制游标(`FindToken`)记录的是**对端 log 的绝对位置**(段 + 段内偏移),而 compaction 会**改写坐标系**:
全库 `compact` 把存活记录重写进新 log、段位置从 0 重来;增量 compaction 把记录搬到更高的段、删掉旧段;
副本**重建 / 换盘**更是"换了一整个副本"。旧实现把"游标已作废"误判成"已追上",于是有一条**静默丢数据**链:

1. `scan_candidates` 把"游标所在段不存在"静默当成"已越过"(`pos < s.segment { continue }`)⇒ 返回**空批 + 原游标**;
2. `replicate_from_peer` 把"拿到空条目批"当成"对端末端已到";
3. `caught_up_with_peer` 再拿同一个陈旧游标探本地库,也空 ⇒ **判追平** ⇒ 置 `ACTIVE`。

`ACTIVE` 是 master 侧删源闸门的输入("搬迁 / 下线要等继任者已 `ACTIVE`",`reconcile::plan_move` /
`drop_confirmed`):继任者实际缺数据却转正 ⇒ 源副本被摘除 + 下发 `DROPPED` 删源数据 ⇒ **丢数据且无声**。

Ambry 原版靠 `PersistentIndex.findEntriesSince` 的第一步 `revalidateStoreFindToken` 挡住:游标段名认不出
(compaction 换 generation 后旧段名必然消失)⇒ `new StoreFindToken()` 从头重扫 —— **方向是多拉(安全)**,
绝不把空批当"已追上"。本 change 补上这一层,并把身份做成**两层**:段身份识别"同一副本内 log 被 compaction
重写",副本 incarnation 识别"对面整个换了一代副本"。

## What Changes

- **第一层身份:段身份 = 位置 + 代次**(`LogSegmentName`;段名 `{position}_{generation}_log.seg`,段头 **v3**
  自述 generation)。代次在同一 log 目录内**单调递增**(滚动 = `max(gen)+1`),**全库 compact 接着旧 log 的最大
  代次往后取** —— 段位置可以复用,段身份永不复用(位置复用靠代次识破)。校验在**引擎**
  (`find_entries_since` 入口 `revalidate_token`),理由短名 `segment_missing` / `segment_generation_mismatch` /
  `offset_out_of_range`。**索引值 21 B 定长布局不变**(代次不进索引,只活在段名与游标里)。
- **第二层身份:副本 incarnation**(master 发放,落盘 `{store_dir}/LOG_INCARNATION`;文件缺失时用**本机
  当前时刻**兜底并落盘 —— **不取段头 `created_ns`**:拷来的段诞生在对端,那是**对端**的身份,用它会与对端
  撞号)。识别"**对面整个换了一代副本**"(重建 / 换盘)。核对放**服务层**
  (`GET /partitions/{pid}/replica-metadata`)—— 引擎看不到副本级身份。理由短名 `incarnation_mismatch`。
- **游标**:`FindToken::{Uninitialized, Offset{segment: LogSegmentName, offset: u64}}`;请求参数
  `pos` / `gen` / `off` / `incarnation` **四件套齐全才可信** —— `pos`/`gen`/`off` 残缺即按"从起点重扫"处理
  (方向是多拉,安全);有游标却缺 `incarnation`、或 incarnation 不符 ⇒ 从头重扫并报 `token_reset`。
- **判据收紧**:`caught_up_with_peer` 前置"本轮没有 `cursor_reset_reason`";`sync_round` 增 `prior_incarnation`,
  **对端 incarnation 变了那一轮绝不判 `ACTIVE`**。本地探测 `BlobStore::has_content_after` 是**纯位置**比较
  (peer 游标的段身份属于对端坐标系,本地无从校验)。会话状态机 `replication::PullSession`(游标 + 对端身份 +
  无进展计数)由**生产 loop 与集成测试共用同一份**;`incarnation_mismatch` + 连续两轮无进展 ⇒ `no_progress`
  丢弃游标重拉(防永远卡死在 `BOOTSTRAP`)。
- **取段 404 化**:`GET /partitions/{pid}/segments/{position}_{generation}` 按**段身份**寻址 —— 对端认不出该段 ⇒
  **404**、名字畸形 ⇒ **400**;`/segments` 响应 = `{incarnation, segments:[{position, generation, data_end}]}`。
  物理 bootstrap 三条丢弃判据(丢弃后回落**纯逻辑拉取**,幂等、必然收敛,**绝不装混合快照**):①取段 404/出错;
  ②段头自述代次与清单不符;③拷完再列清单 incarnation 变了。
- **从零建副本时申领身份**:store 心跳 `ensure_replica`(**本地本来没有**该副本才算)与物理 `install_bootstrap`
  成功(`bootstrap.segments > 0`)之后调 RPC `AssignReplicaIncarnation(partition_id, node_id)`;master **每次发
  一个全新更大的号**(不是取回旧值),leader-only + 经 Raft 提交,`next_incarnation` 由 **apply 发放**并落进
  `ReplicaPlacement.incarnation`(`0` = 未分配)。**申领失败绝不影响建副本**(warn + 保留本地兜底值);
  **幂等心跳不重复申领**(否则身份每拍换新,对端手里的游标被反复作废)。
- **墓碑回收闸门**(同一批改动里的配套项):`PartitionStateInfo.tombstones_reclaim_allowed`(master 判据 = 该分区
  **所有副本都 `ACTIVE` 且 `lag == 0`**;空 placement 保守不放行)→ store `ReplicaHandle.tombstones_reclaim_allowed`
  (初值 `false`)→ `CompactPolicy.tombstones_allowed`。关着时墓碑**一律照搬**(连过保留期的也不回收):还有副本
  没追平就收墓碑,那份副本永远学不到这条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。**单机模式
  (未接 master)恒放行**(没有别的副本可滞后);定时 compaction 与手动 compact 共用同一判据。
- **观测**:`store_server_replication_cursor_resets_total{reason ∈ incarnation_mismatch, segment_generation_mismatch,
  segment_missing, offset_out_of_range, no_progress}`;非零是正常现象,持续增长才值得查。
- **不做兼容层**:线上尚未启用,不存在新旧节点并存 ⇒ 不写任何回滚 / 兼容分支(旧格式清单、可选字段都不加)。

## Capabilities

### Modified Capabilities

- `store-server-replication`(S3a):两层游标重定位 + 追平判据收紧 + 取段按段身份寻址 + incarnation 申领。
- `blob`(store 设计 §3 / §9.2):`LogSegmentName` 的段身份与代次生成规则、`find_entries_since` 的 revalidate 语义、
  `LOG_INCARNATION` 与 `CompactPolicy.tombstones_allowed`。
- `master-placement`(§7.2):`ReplicaPlacement.incarnation` 的发放规则(`AssignReplicaIncarnation`)与墓碑回收闸门。

## Impact

- **crate**:`blob`(段身份 / 代次、revalidate、incarnation 文件、墓碑闸门字段)· `store`(取段端点、清单响应、
  `HttpPeer`、`PullSession`、追平判据、申领 RPC、指标)· `master`(incarnation 发放 + RPC + proto、
  `tombstones_reclaim_allowed`)· 集成 / 单测。
- **磁盘格式(§3.4)**:记录与索引布局**零变更**(索引值仍 21 B);段文件**改名**为 `{position}_{generation}_log.seg`、
  段头升到 **v3**(多一个自述代次字段);副本目录**新增** `LOG_INCARNATION`(一个 u64 文本文件)。线上未启用,
  故不为旧段名 / 旧段头留兼容分支。
- **数据安全(§3.1)**:改动只让 `ACTIVE` **更难**成立(保守方向),不触及写路径与 fsync 边界;`ACTIVE` 是 master
  删源闸门的输入,收紧它就是**减少丢数据风险**。墓碑闸门同理:不放行只会让墓碑多留一拍(追平后自然放行)。
- **被否决的方案**:盘级计数器、以首段 `created_ns` 充当副本身份、滚动升级兼容层 —— 三者各自为什么否掉,见
  `docs/DEVLOG.md` 步骤 21。

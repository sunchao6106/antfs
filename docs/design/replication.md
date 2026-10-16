# replication 设计:副本间异步拉式同步

本文固化 **副本之间怎么互相追平**的机制(对标 Ambry `ambry-replication` 模块)。**状态:已实现** —— 复制角色落在
store server 内(`crates/store/src/replication.rs`(循环 + 后台驱动)/ `replica.rs`(本地副本管理与 bootstrap 装载),两阶段拉取 + 游标重校验 + 封存段物理
bootstrap;本地 flush/compaction 维护 worker(`worker.rs`)与复制无关),机制细化与测试证据见 [cluster.md §9.1](./cluster.md) 的 S3a/S3b 小节与 [store.md](./store.md) §9;
配置/监控两张清单也在 cluster.md §9(避免双处维护漂移)。本文保留最初的设计推理与 Ambry 映射,落地差异在下文各节标注。
跨模块角色划分、传输选型见 [cluster.md](./cluster.md)(本文是其 [D4](./cluster.md) 数据面的展开)。

## 0. 先厘清:写路径 ≠ 同步路径

客户端 PUT 走 frontend Router **同步写到 W 个副本(多数派 quorum)即返回**;剩下的副本、以及当时没写到的副本,靠**本文这套独立、异步的拉式反熵复制**追平,**不是写路径 push 过去的**。
收敛保证来自:**每个副本各自、周期性地从每个 peer 拉**——不论当初哪 W 个副本收到原始写,最终全体一致。

## 1. 模型:拉,不是推

每台节点按远端节点分组跑一批 replica 线程(Ambry `ReplicaThread`,由 `ReplicationManager`/`ReplicationEngine` 拉起)。
本机每个 replica 持有一组对端信息(Ambry `RemoteReplicaInfo`)= 同一 partition 在其它节点上的副本。线程周期性**主动拉**每个对端的新数据。

## 2. 每轮同步 = 两阶段

对某远端节点、每个 partition:

### 阶段 1 —— 换元数据(Ambry `exchangeMetadata`)
1. 本机发 `ReplicaMetadataRequest`,带一个**每对端一个的 `FindToken`**(游标:"我上次读到你 log 的哪")。
2. 远端 `store.findEntriesSince(token, maxSize)` → 返回该 token 之后一批 `MessageInfo`(key + put/delete 标志 + size)+ 新 token + 本批读过的字节数(**lag 由拉取方**对账本地游标自算,peer 不回)。
3. 本机逐条对账本地索引(`replicate_from_peer` 循环内 `local.stat(&key)`:非墓碑且本地无 → 需取数据;引擎另有 `find_missing_keys` 批量原语,loop 未用)→ 得到**本地缺失 key 集合**。

### 阶段 2 —— 补数据(Ambry `fixMissingStoreKeys`)
1. 仅对缺失 key 发 `GetRequest` → 远端回 `GetResponse` 带 blob 字节。
2. 逐条写本地 store:`apply_replicated(rec)` —— 存活记录 → 写整记录(数据体即阶段 2 拉到的);墓碑 → 只写状态(墓碑恒胜,不拉数据体)。
3. 写完**推进并持久化 token**,下轮从新位置续拉。

## 3. 两个"永不丢、可重来"的关键

- **幂等 apply(墓碑恒胜)**:合并规则只有一条 —— 本地缺失 → 应用(peer 存活记录需把数据体一起拉过来);本地存活 + peer 墓碑 → 覆盖为墓碑;本地墓碑 → 一律 `Skipped`;两边都存活 → `Skipped`(对象不可变 ⇒ 同 key 不会有两次 PUT;peer 的 PUT 永不覆盖本地 PUT)。于是**重复拉、乱序拉都安全**,反复补齐最终收敛(状态沿 `无 → 存活 → 墓碑` 单向推进)。合并无需定序字段,也就没有版本竞态类错误。
- **Token 会话(限定现实)**:游标收在复制会话 `PullSession`(进程内存;`replication.rs`)。Ambry 由
  `ReplicaTokenPersistor`/`DiskTokenPersistor` 落盘,antfs **当前不落盘**——重启即新会话、从
  `Uninitialized` 安全重扫(幂等 apply 兜底,代价是重启后多一轮全量扫描)。token 分两段:近期走 **journal**(offset
  有序游标,快),落后过多退回**索引全扫 bootstrap**。

## 4. leader-based 跨 DC 优化

- **同 DC(intra-colo)**:所有副本直接互拉。
- **跨 DC(cross-colo)**:开启后**只有该 partition 的 leader 副本跨 DC 拉**一份,本 DC 的 standby 再从本地 leader 拉 → 避免同一 blob 被 N 副本各走一遍 WAN(Ambry `leaderBasedReplicationAdmin.isLeaderPair`)。
- **落地差异(当前)**:单 DC 形态,`LEADER` 状态折叠进 `ACTIVE`(见 cluster.md §7.2 Q4);本节属后续扩展,未实现。

## 5. 传输映射(承 cluster.md D2)

| 阶段 | 平面 | 传输(设计稿) | 实际落地 |
|---|---|---|---|
| 阶段 1 换元数据(ReplicaMetadata) | 控制面 | **gRPC**(`tonic`+`prost`,proto 带版本) | **store-server HTTP(axum) `GET .../replica-metadata`**(见 cluster.md §9.1 S3a「传输(细化 D2)」:不为此新起 gRPC server) |
| 阶段 2 拉 blob 字节 | 数据面 | 裸 HTTP GET(Wire 流式) | 实际**整块缓冲**(`Bytes` → `Vec<u8>`;单 blob 受 `store_server.max_body_bytes` 上界约束,大文件复合对象在写侧即被分片)。真逐块流式与 store 侧流式 GET 同档物,见 frontend.md §11 待办 |

## 6. 对 antfs 的映射与落地状态

- **已实现(store 侧地基)**:`find_entries_since` / `FindToken` / `journal` / `find_missing_keys` / `is_key_deleted` / `apply_replicated`(墓碑恒胜)。
- **已实现(复制角色)**:`ReplicationManager` 等价物 = store 内 `replication.rs`(`replicate_from_peer` 循环 + `spawn_replicator` 后台驱动 + `PullSession` 拉取会话)+ `replica.rs`(本地 `ReplicaManager`/`ensure_replica`/`install_bootstrap`);`RemoteReplicaInfo` 等价物已落地(token 会话为进程内存,不做落盘,见 §3)。封存段物理 bootstrap(封存段整段拷贝 + 空副本首次先物拷后逻辑补尾)见 cluster.md §9.1 S3b。
- **Token 身份(超出设计稿的加固)**:`FindToken` 自带**段身份**(位置+代次);**对端副本 incarnation** 由拉取会话另持、随每轮 metadata 请求带上校验(不在 FindToken 内)——游标认不出对端坐标系或 incarnation 更替即重置重扫( 防 compaction 改写坐标系后误判追平,见 S3a 游标重校验);会话重启即重置(安全但重扫),持久化留后续小项。

## 7. 配置项 / 监控项清单

**已在 cluster.md §9.1 的 S3 复制相关小节成表维护**(`replication.*` 配置键与 `store_server_replication_*` 指标):
复制拉周期、并发、bootstrap 开关等配置与指标逐项见 [cluster.md §9](./cluster.md);本文不再重复维护第二份,防漂移。

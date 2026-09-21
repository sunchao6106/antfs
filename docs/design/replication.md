# replication 设计:副本间异步拉式同步

本文固化 **副本之间怎么互相追平**的机制(对标 Ambry `ambry-replication` 模块)。**状态:选型/原型已理清,实现规划中**
——store 侧原语已落地([`store`](./store.md) 的 `find_entries_since`/`FindToken`/journal/`find_missing_keys`),复制角色本身尚未实现。
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
2. 远端 `store.findEntriesSince(token, maxSize)` → 返回该 token 之后一批 `MessageInfo`(key + put/delete/ttl/undelete 标志)+ 新 token + 落后字节数(lag)。
3. 本机把这批 key 与自己索引对账(Ambry `getMissingStoreMessages` = 本项目 `find_missing_keys`)→ 得到**本地缺失 key 集合**。

### 阶段 2 —— 补数据(Ambry `fixMissingStoreKeys`)
1. 仅对缺失 key 发 `GetRequest` → 远端回 `GetResponse` 带 blob 字节。
2. 逐条写本地 store:PUT 数据 → `applyPut`→`store.put`;纯元数据操作 → `applyTtlUpdate`/`applyDelete`/`applyUndelete`(不用拉 blob 体)。
3. 写完**推进并持久化 token**,下轮从新位置续拉。

## 3. 两个"永不丢、可重来"的关键

- **幂等 apply**:apply* 吞掉 `IDDeleted`/`AlreadyUpdated`/`IDUndeleted`/`LifeVersionConflict` 这类"已是该状态"的错误 → **重复拉、乱序拉都安全**,反复补齐最终收敛。合法性按"操作发生时"判(非当前时刻),故过期 TTL/迟到更新也照收。
- **Token 持久化**:`FindToken` 落盘(Ambry `ReplicaTokenPersistor`/`DiskTokenPersistor`),重启从断点续拉。token 分两段:近期走 **journal**(offset 有序游标,快),落后过多退回**索引全扫 bootstrap**。

## 4. leader-based 跨 DC 优化

- **同 DC(intra-colo)**:所有副本直接互拉。
- **跨 DC(cross-colo)**:开启后**只有该 partition 的 leader 副本跨 DC 拉**一份,本 DC 的 standby 再从本地 leader 拉 → 避免同一 blob 被 N 副本各走一遍 WAN(Ambry `leaderBasedReplicationAdmin.isLeaderPair`)。

## 5. 传输映射(承 cluster.md D2)

| 阶段 | 平面 | 传输 |
|---|---|---|
| 阶段 1 换元数据(ReplicaMetadata) | 控制面 | **gRPC**(`tonic`+`prost`,proto 带版本) |
| 阶段 2 拉 blob 字节 | 数据面 | **裸 HTTP 流式**(`Bytes`,不整缓进内存) |

## 6. 对 ambry-rs 的映射与缺口

- **已就位(store 侧地基)**:`find_entries_since` / `FindToken` / `journal` / `find_missing_keys` / `is_key_deleted` / `update_ttl` —— 即未来复制线程要调的 store 原语。
- **待实现(复制角色)**:`ReplicaThread` 等价物、`RemoteReplicaInfo`、token 持久化器、两阶段 RPC 端点、leader-based admin。归 server 内复制角色(cluster.md §1)。
- **已知缺口**:当前 `FindToken` 是**简化版**(仅 offset 游标),Ambry 全保真 token 还含 `sessionId`/`incarnationId`/`resetKey`(用于检测远端 store 重建/日志重置、避免误读旧 offset)。做真复制层时须补齐,否则对端重建后续拉会读错位置。

## 7. 配置项 / 监控项清单

> 复制角色尚未实现,**待落地时按 [CLAUDE.md §7](../../CLAUDE.md) 补两张表**(拉周期、每轮 maxSize、并发线程数、token 持久化间隔;复制 lag、每轮补齐字节/条数、metadata/get RPC 时延等)。此处占位,实现 PR 必须同步填。

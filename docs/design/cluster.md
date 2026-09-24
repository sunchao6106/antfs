# cluster 设计:集群/服务层角色与传输选型

本文固化 **store 之上的集群/服务层**的角色划分与关键选型。**状态:master / store server / admin / router SDK / frontend
均已落地**(见 §7/§9/§10/§11 与 [`router.md`](./router.md) / [`frontend.md`](./frontend.md));S3 网关仍是远景。
选型依据来自对 Ambry / SeaweedFS / CubeFS 三套系统的源码调研。

## 1. 角色划分

```
  app + router(`crates/common/router`)──gRPC GetBlobIds / LookupPartition──▶ master
       │  ──HTTP 数据面(put W=多数派 / get R=1)──▶ store server  ← 直连,无中间跳
       ▼
  frontend(HTTP REST;内嵌同一个 router)──▶ master / store server
       │  (远景:同层加 S3 协议翻译 + auth/quota)

  master ◀─心跳/放置(gRPC)─▶ store server ◀─拉复制(gRPC ReplicaMetadata + HTTP 拉 blob)─▶ 其它 store server
   集群大脑:partition→replica 放置、节点状态机、故障处置(取代 Ambry 的 Helix+ZK)
```

| 角色 | 职责 | 状态 |
|---|---|---|
| **store server**(数据节点) | 承载 [`store`](./store.md) 引擎(挂 N disk、每 disk 一堆 BlobStore),服务 put/get,跑副本复制 | 已实现(见 §9) |
| **master** | partition→replica 放置、节点/副本状态机、故障处置、**发号(blobId 号段)**;**内建 Raft**(自包含,不依赖外部协调器) | 已实现(见 §7) |
| **router**(客户端 SDK / 通用组件) | Router 逻辑做成**库**(`crates/common/router`):号段缓存、选分区、拼 blobId、直连 store server 做 W/R、切副本;**frontend 与压测二进制共用同一份实现** | 已实现(见 [`router.md`](./router.md)) |
| **frontend**(HTTP REST 入口) | `POST/GET/HEAD/DELETE /blobs`、写入时定 TTL(`x-ambry-ttl-ms`)、分片拼装、`/metrics`;**内嵌同一 router SDK**,不重复实现 W/R | 已实现(见 [`frontend.md`](./frontend.md)) |
| **S3 网关**(远景) | S3 协议翻译 + auth/quota;与 frontend 同层,复用同一 SDK | 规划 |
| **scheduler**(待定 P1) | 集群级 compaction 编排 / 再复制 / 均衡 / EC 调度 | 待定(见 §4) |
| **admin web**(独立服务) | 运维操作(经 master admin gRPC)+ 控制面状态 dashboard(拓扑/节点/盘/分区/副本);指标时序复用 Prometheus+Grafana;需 operator 鉴权 | 已实现(见 §11) |
| **EC 冷层**(D5) | 老数据纠删码降本 | 已锁(见 §5) |

> **frontend 与 router 并存(修正原「client SDK 取代 frontend」决策)**:两者**共用同一份 router 实现**
> (frontend 依赖 `crates/common/router`)。直连模式适合**内部可信服务**(少一跳、低延迟);frontend 提供 **HTTP/REST 入口**,
> 给非 Rust 客户端、`curl`、通用压测工具使用,并且是将来 **ACL / 配额 / 多租户的唯一落点** —— blobId 是 64 位、无
> account/container 位(D7 布局永久冻结),租户信息只能挂网关层(与 Ambry `AccountService` 同位置)。

> **接入模式(并存,复用同一套 SDK / blobId / 数据面)**:
> - **① 直连模式(SeaweedFS 式)**:app 内嵌 router SDK,直接 gRPC 问 master(发号/查副本)+ HTTP 直连 store server 读写,
>   **无中间跳**,低延迟高吞吐;适合**内部可信服务**。blobId 即取件凭证。
> - **② REST 网关模式**:app / `curl` 走 **frontend**(内嵌同一 SDK),由它取号段、写副本、拼接分片。
> - **③ S3 网关模式(远景)**:标准 S3 客户端走网关做协议翻译 + auth/quota。
> - 关系:三种模式底层 blobId、发号、W/R 数据面**完全一致**,非互斥,可同集群并存。

## 2. 已定选型(locked)

### D1. master 内建 Raft,取代 Helix/ZK
Ambry 依赖 Apache Helix + ZooKeeper、SDFS(网易 NOS)依赖 MySQL + ZooKeeper 做集群协调/元数据,**均重依赖外部组件,运维重**。
SeaweedFS/CubeFS 则**内建 Raft master**,自包含。故本项目 master 用 Rust 的 **[`openraft`](https://docs.rs/openraft)** 自建
(对标 SeaweedFS 的 `hashicorp/raft`、CubeFS 的 `tiglabs/raft`),只管**集群元数据**(partition→replica 放置、节点状态机),
不介入数据副本一致性(见 D4),**不外挂 ZK/MySQL**。

### D2. 传输:控制面 gRPC + 数据面 HTTP 流式
三条路线里裁决(取 SeaweedFS 实际做法):

| 平面 | 选择 | 依据 |
|---|---|---|
| **控制面**(心跳/assign/lookup/状态转移/复制的 ReplicaMetadata)| **gRPC**(`tonic` + `prost`) | 结构化、schema 演进、流式(心跳 stream);codegen + 前后兼容 |
| **数据面**(put/get、复制拉 blob 字节)| **裸 HTTP 流式**(server `axum`/`hyper`,client `hyper` + 连接池) | blob 是不透明字节:HTTP 零拷贝流式(`Content-Length`/range),不进 protobuf 编解码;小文件高 QPS 用 HTTP/1.1 + 连接池起步,HTTP/2 后续可选 |

- **明确否决**:全自研二进制 TCP 协议(Ambry 旧路 / CubeFS)——绿地 Rust 无理由再造二进制协议,零生态红利。
- blob body 全程 `Bytes`/流式,**不整缓进内存**(CLAUDE.md §5.3)。
- gRPC proto 与磁盘格式一样**带版本字段**,保前后兼容(§3.4 同理)。
- 复制天然分裂:拉模型第一步"自 token 起有啥新的"走 gRPC(ReplicaMetadata),第二步"拉缺失 blob"走 HTTP。

### D3. 对象寻址:blobId 自带定位,无中心每对象元数据
承 Ambry 与 SeaweedFS 的交集:**blobId 自带 partition**(SeaweedFS 的 fid 自带 volume 同理),master 只做 **partition 级**(粗粒度)映射,**不存每个小文件的元数据** —— 这是 10¹¹ 小文件可扩展的关键。**不引入** CubeFS metanode 那套 POSIX 命名空间(除非将来要 FS 语义,那是网关层的事)。

### D4. 一致性分级:数据异步 + 拉复制;Raft 只用于 master
- **数据副本**:Ambry 式**异步多副本 quorum 写 + 拉式 anti-entropy**(便宜、抗规模)。**不**学 CubeFS 每 data partition 一个 Raft 组(强一致但每分区开销大,不适合海量小文件)。
- **强一致**只用在 **master 的集群元数据**(D1 的 Raft)。

## 3. 横向调研摘要(四方 + SDFS)

| 维度 | Ambry | SeaweedFS | CubeFS | SDFS(网易 NOS) |
|---|---|---|---|---|
| 集群大脑 | 外部 Helix+ZK | 内建 Raft master | 内建 Raft master | **MDS(Java)+ ZK + MySQL** |
| master 元数据存哪 | ZK(ideal state) | **Raft 仅 MaxVolumeId** | Raft + RocksDB(全量) | **MySQL + 缓存**(桶/拓扑),ZK(实时状态) |
| master HA | Helix controller 选主 | Raft | Raft | **ZK 主备 + standby 同步** |
| 每对象元数据 | 无(blobId 自带 partition) | 无(fid 自带 volume) | metanode 存 POSIX 命名空间 | 无(**docid 自带定位**) |
| 副本/冗余 | 异步 quorum + 拉式 anti-entropy | 副本 + 冷 EC | 每分区 Raft 强一致 | **磁盘对镜像 + 热备/恢复** |
| 传输 | 自研 TCP + HTTP2 | **gRPC + HTTP(数据)** | 自研 TCP 包 + HTTP(master) | **Thrift** |
| 存储节点语言 | Java | Go | Go | **C++** |
| 后台任务 | compaction 内嵌 store | master 触发 vacuum | scheduler 独立节点 | MDS 驱动整盘恢复/热备 |
| 单机盘识别 | **纯声明**:HardwareLayout.json 写死 mountPath+容量,从不 statfs | **声明目录 + 运行时探测**:`-dir/-max/-disk` 给目录,`statfs` 探真实容量、`-max 0` 自动定槽、`vol_dir.uuid` 认盘、扫 `.dat/.idx` 自恢复、低空间转 RO | 目录声明 + 探测(未逐源码核实) | MDS 登记盘对(闭源,细节未证实) |

**采纳映射**:
- master→**内建 Raft**(取 SeaweedFS/CubeFS);**反面教材** = Ambry(Helix+ZK)、SDFS(MySQL+ZK)重依赖外部组件 → 强化 D1 选 openraft。
- Raft hard state→**最小化**(取 SeaweedFS 仅 MaxVolumeId 的精神);**反面** = CubeFS 全量塞 raft+RocksDB。
- 传输→**gRPC 控制 + HTTP 数据**(取 SeaweedFS);Thrift(SDFS)已过时,自研 TCP(Ambry/CubeFS)无生态红利。
- 寻址→**id 自带定位**(四家全票:blobId/fid/docid + Ambry)→ D3 锁死。
- 数据一致性→**异步 + 拉**(取 Ambry);disk-pair(SDFS)/每分区 Raft(CubeFS)均不采纳,但 SDFS 的"热备盘 + 整盘恢复"运维思路可借鉴。

## 4. 待定(parked,未锁)

- **P1. 独立 scheduler 角色**:把 compaction/修复/均衡/GC 独立成角色/节点(CubeFS blobstore `scheduler` 蓝本;呼应 store.md 路线图"后台调度器移出 store 层")。EC 转换/修复也归它(见 D5)。

## 5. EC 冷层(封存段级,已锁 = D5)

热数据多副本、冷数据纠删码降本。**编码单元 = 封存段(sealed `LogSegment`)**,**不做 blob 级 EC**
(50–100KB 小对象逐个 EC → 分片数爆炸/padding 浪费/修复慢,是反模式;段是 GB 级、天然对齐 stripe)。
blob 的"转 EC" = 其所在封存段转 EC,**对上层透明,blobId 定位不变**。

### 5.1 状态机(每个封存段)
```
active(副本) ──写满 rollover──▶ sealed(副本) ──静默 ≥ ec.convert_delay_secs 且 ec.auto_convert=on──▶
   [scheduler 后台转换] ──▶ EC(k 数据 + m 校验分片,散布不同节点/盘) ──分片丢失──▶ repair(读任意 k 个重构)
```
- 触发看"冷":封存后静默满 `convert_delay`;开关看 `ec.auto_convert`。

### 5.2 转换流程(崩溃安全,守 §3 持久性红线)
1. scheduler 扫"封存 ≥ delay 且未 EC 且 ≥ min_segment_bytes"的段;
2. 读该段一份完整副本 → Reed-Solomon 编码 k 数据 + m 校验分片 → 散写到不同故障域 → **fsync + 校验可解码**;
3. **确认 EC 分片落盘且可重构后**,才按 `ec.keep_replica_after_ec` 删多余副本;
4. 全程 tmp→fsync→rename + commit 标记,中途崩溃可回滚/续做(对齐 compaction 的标记式修复)。
> 铁律:任何时刻数据的冗余度不得低于安全线——**先落够 EC 冗余、再删副本**,绝不先删后编。

### 5.3 读 / 修复
- 段已 EC:有存活整副本(keep_replica=1)直读;否则读任意 k 个分片在线重构。
- 分片丢失:后台 repair 读 k 个重构缺失分片,补回冗余。

### 5.4 归属
转换/修复是后台重活 → 归 **P1 scheduler 角色**(P1 的节点独立性仍待锁;在此之前可先落成 server 内后台 worker)。**不塞进 store 热路径**。

### 5.5 配置项清单(遵 CLAUDE.md §7)
| key | 类型 | 默认 | 取值/校验 | 含义 |
|---|---|---|---|---|
| `ec.auto_convert` | bool | false | — | 是否后台自动把够格封存段转 EC |
| `ec.convert_delay_secs` | u64 | 604800(7d) | ≥1 | 封存后静默多久才够格转 |
| `ec.data_shards`(k) | u8 | 4 | ≥1 | 数据分片数 |
| `ec.parity_shards`(m) | u8 | 2 | ≥1 | 校验分片数(容忍 m 个丢失) |
| `ec.min_segment_bytes` | u64 | 段容量 | ≥1 | 小于此不值得转 |
| `ec.keep_replica_after_ec` | u8 | 0 | 0/1 | 转后是否留 1 份整副本兜底热读 |

## 6. 目标 TODO(实现必须紧跟)

> **约束**:集群/服务层的任何实现 **必须**符合下列已锁目标(D1–D5);偏离需先在本文改决策 + 记 `openspec/changes/`,不得在代码里"就地另选"。P1 未锁,实现前先定。

**已锁目标(binding)**:
- [ ] **D1 master 内建 Raft(`openraft`)取代 Helix/ZK** —— 只管集群元数据(partition→replica 放置、节点/副本状态机),不碰数据副本一致性。
- [ ] **D2 传输分平面**:控制面 **gRPC(`tonic`+`prost`,proto 带版本)**;数据面 **裸 HTTP 流式(`axum`/`hyper`,blob 走 `Bytes` 不整缓)**。**禁**自研二进制 TCP 协议。
- [ ] **D3 blobId 自带 partition 定位,无中心每对象元数据** —— master 仅做 partition 级映射;不引入 POSIX 命名空间(除非网关层显式需要)。
- [ ] **D4 一致性分级**:数据副本 = 异步 quorum 写 + 拉式 anti-entropy;**强一致(Raft)只用于 master 集群元数据**,不做每 data-partition Raft。
- [ ] **D5 EC 冷层 = 封存段级**:热副本 / 冷 EC,`ec.auto_convert` 后台转换;**编码单元是封存段,不做 blob 级 EC**;先落够 EC 冗余再删副本(见 §5)。

**待锁目标(P,实现前先定)**:
- [ ] **P1 独立 scheduler 角色**(compaction/修复/均衡/GC + EC 转换/修复)。

## 7. master 细化设计(D1 展开,探索产出;实现前随 change proposal 定案)

> 状态:**探索中**,尚未锁进 §6 TODO。落地前会走 OpenSpec change(`master-node`)再定案。

### 7.1 Hard state(进 Raft)vs Soft state(心跳重建)

原则(取 SeaweedFS 精神):**只有"必须一致 + 需 failover 存活"的进 Raft;liveness/负载/实际副本状态全是心跳重建的软状态**。规模估算(7.5PB / 100GB 分区 ×3 副本 ≈ 22 万副本记录 ×~100B ≈ **~20MB**)→ 内存放得下、快照几十 MB,**用嵌入式 KV(`redb`)即可,不上 RocksDB**。
**HARD(Raft 状态机,`redb` 持久 + 快照)**:
```
IdAllocator   { next_partition_id: u64 }                         // 单调,类 SeaweedFS MaxVolumeId
Nodes         map<NodeId, { dc, rack, addr, disks: map<DiskId,   // 自注册后固化的身份+容量
                  { capacity_bytes }>, registered_at }>
Partitions    map<PartitionId, {
                  replication_factor,
                  state: RW | RO | SEALED,                       // 分区级写入闸门
                  replicas: [ { node_id, disk_id, target_state, incarnation } ]  // ← ideal placement(应然)
              }>
Policy        { default_replication_factor, placement_constraints }
```
> 上表之外的 hard state 记账(**均为新增字段 + `serde(default)`,旧快照/旧持久态可读**,§3.4):
> `decommissioning`(下线中节点)、`moving` + `move_targets`(迁移中的源与继任者)、`pending_drop`(placement 已移除、待源节点确认后删目录的副本)、`maintenance` / `pause_autogrow`(维护模式)。
**SOFT(内存,心跳重建;新 leader 上任后一个心跳周期重建)**:
```
Liveness       map<NodeId, { last_beat, up }>
DiskUsage      map<(NodeId,DiskId), { capacity_bytes, free_bytes, health }>       // health 空=探测未启用
ReplicaCurrent map<(PartitionId,NodeId), { disk_id, current_state, lag_bytes, used_bytes }>  // ← 实然
RoutingIndex   map<PartitionId, [ 存活副本位置 ]>                                 // 供 LookupPartition
```
> `ReplicaCurrent` 按节点**整体替换**(心跳是全量上报 `full_report=true`):本轮没列出的 `(pid, node)` 即"该节点已不再持有该副本"(DROPPED 后删除的场景),据此清理软状态。

### 7.2 副本状态机(忠实照搬 Ambry `ReplicaState`)

```
                   ┌───────────────────────────────┐
   DROPPED ◀── OFFLINE ◀────────────── INACTIVE ◀───┐
                   │                        ▲        │
                   ▼                        │        │
               BOOTSTRAP ─────▶ STANDBY ◀──▶ LEADER ─┘
              (追平中)         (正常服务)   (≤1/分区/DC)
                   └────── ERROR(异常,触发再复制)◀── 任意态
```

**状态语义 + frontend 路由门控**(状态直接决定前台把哪些请求路由给该副本,承 Ambry):

| 状态 | 含义 | 可路由请求 |
|---|---|---|
| OFFLINE | 初始/已停,不服务 | 无 |
| BOOTSTRAP | OFFLINE→STANDBY 中间态,追平 peers | GET / DELETE(**不含 PUT**) |
| STANDBY | 完全可用 | 全部 |
| LEADER | 完全可用;**当前与 STANDBY 无差别**,预留将来跨 DC 复制(≤1/分区/DC) | 全部 |
| INACTIVE | STANDBY→OFFLINE 中间态(下线中,等 peers 追平自己) | **仅 GET** |
| ERROR | 副本异常(盘坏等) | 无 → 触发再复制 |
| DROPPED | 退役终态 | 无 |

- **target_state** 由 master 决定(hard,placement 的一部分);**current_state** 由节点心跳上报(soft)。
- MVP 单 DC 可把 LEADER 折叠进 STANDBY(叫 ACTIVE);多 DC 再启用 LEADER 做跨 DC 拉复制。

### 7.3 调谐循环(reconciler)

```
每个心跳/周期:  对每个 (partition, replica):
    if current_state != target_state:  下发 MasterDirective.transition(target_state)
    if 节点失联(last_beat 超时):        标 ERROR → 选新盘 → 追加 replica(target=BOOTSTRAP)→ 产"再复制任务"
    if 磁盘将满/不均衡:                  产"再平衡任务"(P1 scheduler 执行)
    if 可写容量/可写分区 < 水位(Q5):    自动 CreatePartitions(rack-aware+容量感知放置)——需防抖/限速/上限
每个周期:       盘将满(任一副本所在盘用量 > 阈值)→ 该 RW 分区 SetPartitionState(RO)(只置不回)
每个心跳:       待删源副本(确认齐:placement 已移除 + 继任者 ACTIVE)→ 向源节点下发 transition(DROPPED)
```
master **只决策、只下发任务**,不亲自搬数据(D1);执行在 server/scheduler(P1)。

**维护模式**(§7.4 `SetMaintenance`):`ClusterState.maintenance=true` 时本循环跳过**全部自动动作**(再复制、evacuation 的"先补/后删"、迁移后删),只保留观测刷新与心跳里的指令下发;`pause_autogrow=true` 时另停 Q5 自动扩容。手动 ops 走 RPC,不受影响(语义细则见 §7.4)。

**例外(维护期照样生效)**:① **盘将满转 RO**(§7.3)是止损而非恢复动作,维护窗口换盘时正是要它先兜住写入;② 心跳里的指令下发(§7.4)。

**迁移完成后自动删源目录(承 §3.1 "先补后删"的收尾)**:
- `MoveReplica` / evacuation 在 hard placement 里**移除**源副本时,同一条命令顺带写下记账:`move_targets[(pid, 源)] = 目标节点`(仅 move)与 `pending_drop += (pid, 源)`(单条 log 原子,不会出现"placement 移除了却不删数据"的中间态)。
- master 随后通过**心跳**给源节点下发 `Transition{ target_state: "DROPPED" }`(**三条确认全满足**才发):
  1. hard placement 已不含该副本;
  2. **继任者确认**:有 `move_targets` 记账时要求其**目标节点已上报 `ACTIVE`**(= 新副本追平,转移成功);无记账(evacuation 的"后删")退化为"该分区其余副本数 ≥ RF 且无一 `ERROR`";
  3. 源节点**存活且仍上报该副本**(否则无需删)。
- **不拖延**:确认只看"目标 ACTIVE + 源已移除",**其余副本临时失联不阻挡删除**(转移已由 hard placement 记账,源上那份不再是唯一来源);每轮心跳**重发**直到 store 不再上报该副本(幂等)。
- **记账清理**:源节点不再上报该副本后,由 reconciler 写 `ConfirmDrop` 清 `pending_drop` / `move_targets`(仍是 leader 提交,`master_pending_drops` 随之归零)。
  - 源节点长期不回(心跳判 down)时**不清理**:`pending_drop` 会一直留着(`master_pending_drops` > 0),数据也留在源盘上 —— **保守优先,绝不误删**。
- **安全下界**:全仓只有 reconciler 写 `RemoveReplica`,**没有运维 RPC 能直接移除副本** → "移除 + 继任者已 ACTIVE + 源仍持有"三条齐了才删;拿不准一律不删(最坏只是源盘上多留一份数据)。

**计划式均衡(对标 Ceph balancer 的 eval → optimize → execute)**:
- **语义**:均衡是**显式运维动作**,不是后台自动行为 —— `EvalBalance` 算出计划(status `PREVIEW`)落 Raft,运维确认后 `StartBalance` 置 `RUNNING`,由 reconciler 内的**均衡执行器**(维护模式 skip 之后,与再复制/evacuation 并列)按并发上限逐条推进;`CancelBalance` 清计划。计划只存数据(谁迁到哪 + 判败记录),**进度由 `partitions` + `moving` + `failed` 派生**(`GetBalanceState` 返回时现算),不冗余持久化;计划进 Raft 快照,**可跨切主存活**。
- **planner 判据**(`crates/master/src/balance.rs`,纯函数):对每块"合格盘"(节点存活、未下线、有容量上报)统计 ① 承载副本数 ② 用量比 `used/capacity`。**源盘** = 副本数或用量比 > 均值 × `balance_skew_threshold`;**目标盘** = 低于均值(同一阈值)且通过 `select_disks` 落地校验(盘 HEALTHY、`free ≥ partition_capacity_bytes`、未将满;节点存活、未下线、未持有该分区副本)。**改善护栏(防振荡)**:仅当"源副本数 ≥ 目标 + 2 或源用量比 > 目标 × 阈值"才产出该 move —— 副本数不可整除均分时(如 9 副本 4 盘收敛到 2,2,3,2)不会来回搬。硬约束:**同一分区一份计划至多 1 条 move**(避免临时 RF+1 叠加)、`moving` 在途分区跳过、找不到合格落点的分区**不进计划**、超过 `balance_max_moves` 截断;同一源盘上按分区实际占用(心跳 `used_bytes`,缺失按 0)从大到小挑,每产一条就**模拟**转移再算下一条。
- **执行器**:仅推进 `RUNNING` 计划;并发预算 = `max_in_flight` − 在途数(在途 = 派生为 in_flight 的本计划 move:目标已补副本或已记 `moving`,等 `plan_move` 摘源)。投递前再校验(源仍持有 / 目标存活未下线未持有 / 目标盘仍可容纳,评估时选的盘失效则在目标节点上重选),落点失效 → `FailBalanceMove` 判败不重试。**幂等**:`MoveReplica` 的 apply 只在目标副本尚不存在时才追加并记 `moving`,故"目标已持有"视为**已受理**,绝不重投。**stuck 判据**:在途超 `balance_move_timeout_secs` 未摘源 → 判败释放预算(同一落点重试大概率再卡;重搬由重新 eval 决策)。全部 move 进入 done/failed → 置 `DONE`。
- **不回滚**:`CancelBalance` 只清计划;已受理的在途 move 由既有"后删 + DROPPED + 记账清理"管线自然收敛(等价于一次手动 `MoveReplica` 走完,守 §3.1 先补后删)。计划被替换/清除后执行器不再推进旧计划。

**启动/切主稳定期(防再复制风暴)**:软状态(节点存活)靠 store 心跳**从零重建**、初始为空(不像 Ambry/SDFS 把存活放 ZK)。故 leader 上任后先静默 `reconcile_startup_grace_secs`(默认 15s),期内不做失败驱动的再复制——否则重启/切主瞬间会误判全体失联、给每个分区触发补副本(RF=节点数时更是永远补不上、刷屏)。数据面读写不受影响(D2/D3 解耦):稳定期只推迟"补副本"这一后台修复,store 节点照常服务。对齐 CubeFS「加载即 `ReportTime=now` 隐式宽限」的效果。

**容量感知放置(重要,解决"固定容量容器 vs 磁盘余量"矛盾)**:
- partition 是**段级懒增长 + 定容上界**(不预留整块;对齐 store.md"定容上界 + 写满滚动新 partition"),空间不浪费——长到上界或盘满即封存(RO)。
- **副本尺寸受最小盘拖累**:一个 partition 的 N 副本装同一批对象,有效容量 = 各副本盘余量的**最小值**;若余量悬殊,大盘空间被小盘拖累浪费。
- 故放置(`CreatePartitions` / 失联补副本)选盘约束:**从心跳 `DiskUsage.free_bytes`(§7.1 软状态)里,选余量 ≥ partition 上界、且彼此相近的 N 块盘**,并按故障域(dc/rack)打散。
- **盘将满 → 其上活跃 partition 转 RO、不再放新 partition**(对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage≈95%`;本项目 `disk_near_full_ratio` 默认取更保守的 **0.8**);写流量由 `AssignPartition` 只返回 RW 分区自动导流。
  - 转 RO 由 reconciler 每周期自动执行:某 RW 分区的**任一副本所在盘**用量 > `disk_near_full_ratio` → 提交 `SetPartitionState(RO)` + `warn` 日志。
  - **只置不回**:空间释放后**不自动恢复 RW**(盘余量抖动就自动切回会把写流量又引回将满的盘);恢复由运维走 `POST /api/partitions/{id}/state` 显式设定。因为只在当前为 RW 时写,天然幂等,不会每 tick 空转 Raft。
  - 该规则是**止损**(不是"恢复动作"),故在维护模式(`maintenance=true`)下**仍然生效**,见 §7.3 维护模式段。
- **盘健康避让**:store 主动探测上报 `DiskHealth`(§9),master 把**非空且 ≠ `HEALTHY`** 的盘从新副本候选里剔除(再复制 / migration 选盘 / 建分区一视同仁)。
  - **空串 = 该盘未启用主动探测**(store `disk_health_*` 默认关)→ **视为可用**:否则未开探测的集群会被整体判为不可放置。
  - 即"只避让明确探测出来的坏盘",不把"没探测"当"不健康"。
- 尾部搁浅余量(< 1 个 partition 上界,100GB/16TB ≈ 0.6%)可忽略;**不做变尺寸尾 partition**(不值当)。

### 7.4 gRPC 接口(D2;实际契约以 [`crates/master/proto/master.proto`](../../crates/master/proto/master.proto) 为准)

```proto
service Master {
  // 数据服务器 ↔ master:双向流心跳(承 SeaweedFS SendHeartbeat)。
  // 上行:活着 + 我持有的副本及其 current_state/lag + 各盘余量;下行:状态转移指令 + 任务。
  rpc Heartbeat(stream NodeBeat) returns (stream MasterDirective);

  // 写路径:申请一段 blobId(objectId 号段)= 可写分区 + 号段 + 副本位置。
  // leader-only + 准入密钥;水位经 Raft 抬升,只发已提交水位以内、永不复用(blobid-master-assign D1–D4)。
  // count=0 → 用 master.id_alloc_segment_size;partition_id 缺省(optional)= 由 master 按剩余容量加权挑(RW,D10)。
  rpc GetBlobIds(GetBlobIdsRequest) returns (GetBlobIdsResponse);

  // 读路由:某 partition 的存活副本位置 + 状态(据此按 §7.2 门控选副本);
  // 每个 `ReplicaLoc` 带该副本的 `incarnation` 身份(0 = 未分配,权威值由 AssignReplicaIncarnation 发放)。
  rpc LookupPartition(LookupRequest) returns (PartitionLocations);
  // 取一个可写(RW)分区(不带号;发号走 GetBlobIds)。
  rpc AssignPartition(AssignRequest) returns (AssignResponse);

  // 管理面。
  rpc CreatePartitions(CreateRequest) returns (CreateResponse);
  rpc ListNodes(Empty) returns (NodeList);
  // 控制面全量视图(admin dashboard):hard placement + soft current;`ListPartitionsRequest.state`
  // 空=全部,否则按 RW/RO/SEALED 过滤(过滤先于分页,`total` 是过滤后总数;非法 state 串返回
  // INVALID_ARGUMENT,不静默当"全部")。
  rpc ListPartitions(ListPartitionsRequest) returns (ListPartitionsResponse);
  rpc DecommissionNode(NodeRef) returns (Ack);
  rpc GetClusterState(Empty) returns (ClusterState);

  // 指标导出:任何节点(含 follower)可答,返回本节点 Registry 的 Prometheus 文本。
  rpc GetMetrics(MetricsRequest) returns (MetricsResponse);
  // 手动立即补一个副本(不等 reconciler 周期);返回新副本落点。
  rpc RepairPartition(RepairPartitionRequest) returns (RepairPartitionResponse);
  // 副本迁移:add-before-remove(守 §3.1)——先在目标补新副本并把源记入 `moving` 集合
  // (hard state,`PartitionEntry.moving_out_nodes` 可见),新副本补上且健康副本(不含迁出源)
  // ≥ RF 后由 reconciler 移除迁出源;to_node_id=0 = 自动选目标。
  rpc MoveReplica(MoveReplicaRequest) returns (MoveReplicaResponse);
  // 副本身份(incarnation):store 每次**从零建副本目录**时向 leader 申领一个**全新身份**
  // (每次发一个更大的新号,不是取回旧号;leader-only + 经 Raft 提交,号由 apply 发放)。
  // 用途:重建 / 换盘后的副本不得沿用旧身份,否则拉取方手里的旧复制游标会"看似合法"(见 §9.1 S3a)。
  rpc AssignReplicaIncarnation(AssignIncarnationRequest) returns (AssignIncarnationResponse);
  // 维护模式开关(对标 Ceph `osd set/unset noout|norecover`):`paused` 暂停自动动作,
  // `pause_autogrow` 是否连带暂停自动扩容(默认 false)。仅 leader 受理,以 Raft 命令
  // `SetMaintenance` 提交(切主后仍生效);响应 `ok=true` ⟺ 已提交的 `ClusterState.maintenance`
  // == `paused`。非 leader 回 `leader_addr` 供重定向。
  rpc SetMaintenance(SetMaintenanceRequest) returns (SetMaintenanceResponse);

  // 计划式均衡(对标 Ceph balancer;语义细则见 §7.3「计划式均衡」段):
  // eval 出计划(PREVIEW,写 Raft 可跨切主)→ start 确认执行(RUNNING)→ cancel 清计划(不回滚在途)。
  // 均为 leader-only 写路径(准入密钥 + 非 leader 回 leader_addr);GetBalanceState 只读,任何节点可答。
  rpc EvalBalance(EvalBalanceRequest) returns (EvalBalanceResponse);     // max_moves=0 用配置默认;新 eval 整体替换旧计划;无倾斜也 ok=true + 空 moves
  rpc StartBalance(StartBalanceRequest) returns (StartBalanceResponse);  // plan_id 必须是当前 PREVIEW 计划,否则 ok=false(防陈旧确认)
  rpc CancelBalance(CancelBalanceRequest) returns (CancelBalanceResponse);
  rpc GetBalanceState(BalanceStateRequest) returns (BalanceStateResponse); // present + 计划 + 逐 move 派生进度(pending/in_flight/done/failed)
}

message NodeBeat {
  string node_id = 1; string dc = 2; string rack = 3; string addr = 4;
  repeated DiskBeat disks = 5;      // disk_id, free_bytes, load
  repeated ReplicaBeat replicas = 6; // partition_id, disk_id, current_state, lag_bytes, used_bytes, sealed
  bool full_report = 7;             // true=全量(注册/重连), false=增量(承 SeaweedFS Incremental)
}
message MasterDirective {
  repeated Transition transitions = 1; // partition_id, disk_id, target_state
  repeated Task tasks = 2;             // 再复制 / 再平衡 / EC 转换(from,to_hint)
  repeated PartitionStateInfo partition_states = 3; // partition_id, state(RW/RO/SEALED), tombstones_reclaim_allowed
}
```
- master↔master 的 Raft 内部 RPC 走 openraft 自己的网络层(可另起一个 gRPC `RaftInternal` 服务),与上面控制面服务分开。
- proto **带版本字段**(D2 要求)。
- **`partition_states`(已实现)**:每拍**全量**下发该节点承载副本的分区可写态(`RW`/`RO`/`SEALED`)**+ 墓碑回收闸门 `tombstones_reclaim_allowed`**(master 权威:该分区的**所有副本都已追平** —— 各副本上报 `ACTIVE` 且 `lag_bytes == 0`;缺上报 / 节点 down / 空 placement 都算未追平)。前者供 store 侧挑本地维护(定时 compaction)的候选集,后者是 compaction 的**判活口径**闸门 —— store 不自行从"能不能写"反推可写态、也不自行推断删除是否已传播,只认 master 的权威值(见 §9.1)。与 `Transition.target_state`(副本实然态 `ACTIVE`/`BOOTSTRAP`/`DROPPED`)是两码事。
- **`ListPartitions` / `PartitionEntry.write_weight`(已实现)**:控制面全量视图的 `PartitionEntry` 多带一个**写权重**(`uint32`,千分比 `0..=1000`)= 该分区**剩余容量占容量上限的比例** —— `1000` = 完全空,越接近 `1` 越满,**下限 1**,`0` = 未下发 / 无意见 → 客户端**按均权处理**(故 `0` 永不表示"零分配")。它是给 **router(客户端)** 用的:发号时按各可写分区的**相对**权重挑分区(平滑加权轮转),于是"越空的分区拿到越多号"。这**与 master 自己在 `partition_id` 缺省时加权挑分区共用同一个定义**(`capacity_weight_permille`),不让集群里出现两套自相矛盾的容量权重。权重取自心跳软状态(每分区各副本 `used_bytes` 的**最大值**,RF 不重复计数),**不进 Raft / apply**(切主后由心跳重建);**容量 policy 只活在 master**,客户端只用相对值(见 [`router.md` §2](./router.md) 与 [`openspec/changes/router-weighted-partition-alloc`](../../openspec/changes/router-weighted-partition-alloc/proposal.md))。
- **`AssignReplicaIncarnation`(已实现)**:store **从零建副本目录**时申领副本身份。四条语义要点:① master 每次发**全新的更大号**(不是取回旧号),号由 apply 发放并已写进 placement,故 `LookupPartition` 里 `ReplicaLoc.incarnation` 看到的是同一个值;② leader-only + 准入密钥,非 leader 回 `leader_addr`(store 侧**不代转**,只记 `warn` 并保留本地兜底值 = 建库/装载时取的**本机时刻**);③ 副本不在 placement → `ok=false`,不发号也不改状态;④ **幂等空转不重领** —— 判据是"**盘上原本没有**这份副本"(而不是"内存里没有句柄"):OFFLINE 摘除后挂回、进程重启重开都不申领(否则身份每换一次,别的节点手里的复制游标就被作废一次、白跑一轮全量)。
- **`Transition.target_state` 取值**:placement 的目标态(现阶段统一 `ACTIVE`)或 **`DROPPED`**(删源副本 —— 迁移/下线完成后让源节点删掉本地副本/目录;**proto 不新增字段**,复用既有 `target_state`)。下发 DROPPED 的**三条确认**(placement 已移除 + 继任者已 `ACTIVE`(无 move 记账时退化为"其余副本 ≥ RF 且无 ERROR")+ 源仍上报该副本)与"保守性/不拖延/幂等重发"细则见 §7.3;拿不准一律不发。
- **维护模式语义**(`SetMaintenance`;`ClusterState.maintenance`/`pause_autogrow` 对外可见):
  - **停**(`paused=true`):reconciler 不再执行**自动动作** —— 失联/`ERROR` 触发的再复制、下线 evacuation 的"先补"与"后删"(节点已标 `decommissioning`,但副本停在原地不搬)、`MoveReplica` 的"后删"(中间态停在"源仍持有副本",**不丢数据**,守 §3.1);`pause_autogrow=true` 时另停 Q5 自动扩容(默认 false = 维护不连带停扩容)。
  - **不停**:心跳与软状态(存活/容量/副本实然态)照常重建;心跳里的 `plan_transitions` 指令下发照常(**否则 move 的新副本永不落地**);全部观测打点照常刷新(reconciler 每周期仍更新 `master_maintenance`/`master_under_replicated_partitions` 等 gauge,欠副本数不会因停自动动作而失真);数据面读写完全不受影响。
  - **手动 ops 不受阻**:`RepairPartition` / `MoveReplica` / `SetPartitionState` / `DecommissionNode` 照常受理 —— 维护期正是要"人工说了算"。
  - **动机**:维护窗口(换盘/升级/大批迁移)里避免自动动作与人工操作互相打架(刚补的副本又被搬走、反复触发再复制),对齐 Ceph `noout` + `norecover` 的语义。

### 7.5 关键流程时序

```
① 自注册+心跳:  SN 首心跳(full_report)→ master 固化 Nodes(raft)→ 重建 Liveness/RoutingIndex(soft)
② 建分区:       admin CreatePartitions → 分配 id(raft 单调)→ 故障域选盘置 N 副本(写 raft placement,target=BOOTSTRAP)
                 → 心跳下发 transition → 副本 BOOTSTRAP 追平 → STANDBY
③ 读:           client/frontend LookupPartition → 按副本 current_state 门控(§7.2)选一个 → 直连该 SN 走 HTTP 读(R=1,首个成功即返回)
④ 写:           client/frontend ListPartitions(RW + 写权重)+ 对每个分区 GetBlobIds(各一小批 objectId 号段)
                 → 本地按各分区**写权重**加权轮转(SWRR)取号 → 向该号所属分区副本并行 HTTP 写,收 W=副本数/2+1 个 ack 即返回(§10)
⑤ 节点失联:     心跳超时 → 相关副本标 ERROR → reconciler 选新盘补副本(target=BOOTSTRAP)→ 产再复制任务 → scheduler 执行
```

### 7.6 master 实现 TODO + 待拍决策(探索产出,落地前定案)

> 状态:**探索中**,未锁进 §6。落地走 OpenSpec change(`master-node`)时逐条定案。

**分期 TODO(每期可独立验收)**:
- [x] **M1 骨架**:openraft 起 3 节点 + 选主;节点自注册(心跳→raft 成员表);单调 partition_id;软状态(存活/容量)内存;gRPC `Heartbeat(stream)`/`RegisterNode`/`ListNodes`。**不做放置**。**(已实现:`crates/master`,redb 后端过 openraft `Suite::test_all` + 崩溃恢复属性测试 + 3 节点集成测试;配置/监控见 §7.7)**
- [x] **M2 放置**:`CreatePartitions`(分配 id + 故障域 rack-aware 选盘 + 写 raft placement)+ **容量感知选盘(余量 ≥ partition 上界且相近的 N 块盘;盘将满转 RO、不再放新,见 §7.3)** + `LookupPartition` + 路由索引心跳重建 + `AssignPartition`(取可写分区)。**(已实现:`crates/master`——M2a 选盘纯函数 + `CreatePartition` 确定性 apply;M2b `CreatePartitions`/`LookupPartition`/`AssignPartition` gRPC(路由由 hard-state placement + 软状态 liveness 直接联表,未单设 RoutingIndex 缓存);M2c Q5 自动扩容 loop + 防抖/限速/上限护栏;过属性测试 + 3 节点集成测试。见 `openspec/changes/master-placement/`)**
- [x] **M3 调谐**:副本状态机(BOOTSTRAP→STANDBY/LEADER,§7.2)+ 心跳失联检测 → 产再复制任务。**(已实现:`crates/master`——`reconcile.rs` 纯决策 `plan_transitions`/`plan_rereplication`;心跳反应式下发转移(current≠target)、leader-only 后台 loop 失联检测 → `AddReplica` 再复制补副本;过属性/单测 + 集成测试(转移下发 + 失联再复制)。见 `openspec/changes/master-reconcile/`。转移/拉复制的**执行**待 store server。)**
- [ ] **M4 再平衡/EC 钩子**:容量均衡、decommission、对接 D5 EC 转换决策。**(decommission 已实现:`crates/master`——`decommissioning` hard state + `DecommissionNode`/`RemoveReplica` 命令 + `plan_evacuation` 先补后删 evacuation(守 §3.1),过单测 + 集成测试(103 下线→迁到 104→移除 103)。见 `openspec/changes/master-decommission/`。容量再平衡已实现为**计划式均衡**(§7.3/§7.4:`balance.rs` planner + `EvalBalance`/`StartBalance`/`CancelBalance`/`GetBalanceState` + reconciler 内执行器,计划进 Raft 可跨切主),过单测 + 集成测试(eval/start/cancel/stuck 判败/并发上限)。D5 EC 转换钩子**推迟**:EC 依赖 store server + 封存段语义(§5)。)**

**待拍决策(实现前确认)**:
- [x] **Q1 节点注册 / 单机盘识别**:**自注册**——节点启动后心跳自报(节点 + 盘 + `statfs` 探到的**真实容量**),master 固化(不采 Ambry 写死配置)。⚠ **准入**:自注册需节点身份校验(防乱入),这是安全点。
  - 证据(源码核实):Ambry `DiskId.getRawCapacityInBytes()` 来自 HardwareLayout 配置、从不探盘,可用空间靠进程内 `decrease/increaseAvailableSpaceInBytes` 记账;SeaweedFS `DiskLocation.CheckDiskSpace` 周期 `statfs` 上报真实 total/free 供均衡,`-max 0` = 空闲空间÷volume 大小自动定槽,`vol_dir.uuid`+`os.SameFile` 认盘去重,`loadExistingVolumes` 扫目录自恢复中断的 compaction 也一并回滚/前滚。
  - 倾向:取 SeaweedFS「配置给目录列表 → 节点 `statfs` 探真实容量 + 扫目录自恢复 + 健康探测上报 master」,不采 Ambry 把容量写死配置的做法(详见 §3「单机盘识别」行)。
- [x] **Q2 Raft 边界**:锁在 §7.1 hard-state(id/Nodes/Partitions/Policy),其余全软(心跳重建)。
- [x] **Q3 Raft 存储后端**:**现阶段用 `redb`**(纯 Rust 嵌入式、崩溃安全);**不用 RocksDB**。
- [x] **Q4 LEADER/STANDBY**:**副本之间不区分**(统一 `ACTIVE`,不选 leader);多 DC 若需跨 DC 省带宽再议。
- [x] **Q5 partition 分配触发**:**自动扩容**——master 监测可写容量/分区水位,**自动建新分区**(rack-aware + 容量感知放置,§7.3);`CreatePartitions` 仍作机制供 admin 手动调。⚠ 需**防抖/限速 + 上限**(避免水位抖动导致过量建分区)。
- [x] **Q6 admin 入口**:**做 admin web**(独立 `admin` 服务,对齐 CubeFS `console`/SeaweedFS `weed admin`)——(a) **运维操作**:经 master admin gRPC(§7.4)建分区/decommission/触发 compaction/开关复制/触发 EC/标 RO 等;(b) **控制面状态 dashboard**:拓扑 + 节点/盘/分区/副本 状态机态/容量/健康(admin web 自建);(c) **指标时序**复用 **Prometheus + Grafana**(metric crate 已导出 /metrics),内嵌/外链,**不自造时序图表**;(d) 需 **operator 鉴权**。gRPC/CLI 仍保留作底层入口。
- [x] **Assign 交互对齐 SeaweedFS**:master 发 id + 位置,client 直连存储节点走 HTTP 写(D2/D4);master 不碰数据字节。
- [x] **Q7 blobId 的对象标识**:blobId = 64bit = `partitionId`(高32) + `objectId`(低32);**objectId = master 按 partition 单调号段发号**(号段批处理不进 per-object 写路径,Raft 水位持久化容忍空洞永不复用)。见 §8 + `openspec/changes/blobid-master-assign`。

### 7.7 M1 配置项清单 / 监控项清单(CLAUDE.md §7 必须项)

> 与 `crates/master` 的 `impl Default`+`Validate`(配置)、指标注册处(监控)逐项对齐;改代码即改表。

**配置项清单**(`MasterConfig`,TOML `[master]` 表下;加载用 `Loader::load_table("master")`,**同时兼容顶层 flat key**,见 common.md §配置加载):

| key | 类型 | 默认值 | 取值/校验 | 含义/影响 |
|---|---|---|---|---|
| `master.node_id` | u64 | 1 | ≥1 | 本节点在 raft 集群里的稳定 id(跨重启不变) |
| `master.raft_addr` | String | `127.0.0.1:7100` | 非空 | `RaftInternal` gRPC 监听地址 |
| `master.grpc_addr` | String | `127.0.0.1:7101` | 非空 | 控制面 `Master` gRPC 监听地址 |
| `master.data_dir` | String | `master-data` | 非空 | redb 目录(Raft log + 状态机快照) |
| `master.initial_members` | Vec\<u64\> | `[]` | — | 初始成员集(引导);空=由外部 `initialize` 引导 |
| `master.cluster_secret` | String | `""` | — | 准入密钥(Q1);空=关校验(仅本地测试) |
| `master.heartbeat_timeout_secs` | u64 | 15 | ≥1 | 单窗口判活阈值:超过则软状态标该节点**过期**。**语义变更(D5)**:它不再是唯一判活判据 —— 过期还要**连续** `node_down_debounce_ticks` 拍才判死,有效窗口 ≈ 本值 + 迟滞拍数 × `reconcile_interval_secs` |
| `master.node_down_debounce_ticks` | u64 | 3 | ≥0(**0 = 关迟滞**,保持改造前行为) | **判活迟滞(D5)**:节点判 down 需连续 K 个 reconcile tick 都观察到心跳过期(有效窗口 15 + 3×5 ≈ 30s)。为什么需要:原先单窗口判活 + store 每 3s 一拍 ⇒ 一次 GC/调度停顿、一次 `statvfs` 卡顿就能跨过阈值,**一次抖动 = 判死 = 全节点再复制**(几万个分区一起搬)。连击**单调累积**:长期 down 不会被清零,收到心跳才清零 |
| `master.rereplication_max_per_tick` | u64 | 4 | ≥1 | **再复制每 tick 预算(D6)**:每 tick 最多新发起这么多个欠副本分区的补副本(按 `PartitionId` 确定性取前 K),其余下 tick 继续,被压下的量见 `master_rereplication_deferred`。限的是"每 tick **数量**",**不是** `balance_max_in_flight` 那种并发信号量 |
| `master.disk_evac_enabled` | bool | **false** | — | **坏盘撤离总开关(D4)**。默认关(这是新写的**自动搬迁动作**,与该类动作的保守惯例一致,同 `auto_grow_enabled`)。关着时坏盘只被"不再接新副本"避开,**不做任何自动搬迁** |
| `master.disk_evac_min_bad_secs` | u64 | 300 | ≥1 | **持续证据门槛**:某盘为 `UNAVAILABLE` 需**持续**这么久才允许撤离它上面的副本。为什么单轮故障枚举不够 —— 一次抖动就搬 GB 级数据,代价不对称(判坏 3 轮/30s,撤离默认 300s)。**健康值一变就重置计时**,故反复跳动的盘永远攒不满 |
| `master.disk_evac_max_in_flight` | u64 | 1 | ≥1 | **在途撤离预算**:同时在途的撤离分区数上限。在途 = "已为它开过撤离,且坏盘上那份副本还在硬放置里"。撤离是真搬 GB 数据,风格对齐 `balance_max_in_flight = 1` |
| `master.partition_capacity_bytes` | u64 | 100 GiB | ≥1 | 分区容量上界;选盘时盘可用容量须 ≥ 它(M2,§7.3) |
| `master.disk_near_full_ratio` | f64 | 0.8 | (0,1] | 盘用量超此比例:① 不再放新分区/新副本(选盘跳过);② 其上 RW 分区被 reconciler 自动转 RO(**只置不回**,恢复靠运维)(本项目默认 0.8,较 Ambry ≈95% 更保守) |
| `master.auto_grow_enabled` | bool | false | — | Q5 自动扩容开关(默认关,安全) |
| `master.auto_grow_min_writable` | u64 | 4 | — | 低水位:RW 分区数低于它就补建 |
| `master.auto_grow_max_partitions` | u64 | 100000 | — | 分区总量上限(护栏) |
| `master.auto_grow_interval_secs` | u64 | 30 | ≥1 | 扩容检查周期,兼作限速节拍 |
| `master.auto_grow_batch` | u64 | 4 | ≥1 | 单周期建分区上限(限速护栏) |
| `master.reconcile_interval_secs` | u64 | 5 | ≥1 | 调谐/失联检测周期(M3) |
| `master.reconcile_startup_grace_secs` | u64 | 15 | ≥0 | 成为 leader 后调谐稳定期:期内不做失败驱动再复制,等心跳重建软状态(防重启/切主再复制风暴;0=关) |
| `master.id_alloc_segment_size` | u32 | 1024 | ≥1 | **号段大小 N**:client 单次经 `GetBlobIds` 申请的 objectId 个数(`count=0` 时取此值);越大越省 RPC、client 崩溃留下的空洞越大(空洞无害) |
| `master.id_alloc_raft_batch_multiple` | u32 | 2 | ≥1 | **水位批次倍数**:master 一次向 Raft 预留 `N × 该值` 个号,再由 leader 切成 N 大小的号段发出;越大 Raft 提交越省、切主留下的空洞越大 |
| `master.balance_max_in_flight` | u32 | 1 | ≥1 | 计划式均衡:同时在途搬迁数上限(评估时快照进计划,`StartBalance` 后按它节流;对齐 Ceph balancer 的慢速并发) |
| `master.balance_max_moves` | u32 | 100 | ≥1 | 计划式均衡:一份计划最多包含的搬迁条数(`EvalBalanceRequest.max_moves=0` 时取此值) |
| `master.balance_skew_threshold` | f64 | 1.0 | >0 | 计划式均衡:倾斜阈值——每盘副本数/用量比**超过均值 × 该值**才认为该盘倾斜(1.0 = 严格高于均值;另有"副本数差 ≥ 2 或用量比显著更好"的改善护栏防抖,见 §7.3) |
| `master.balance_move_timeout_secs` | u64 | 300 | ≥1 | 计划式均衡:单条在途搬迁的超时;超时未摘源 → 判败(`FailBalanceMove`)释放并发预算,不再重试该条(重搬由重新 eval 决策) |

> openraft 内部的 `heartbeat_interval`/`election_timeout`(M1 内置 250ms / 500–1000ms 常量)尚未暴露为配置项,M2 再提升为 `[master.raft]` 子表。

**监控项清单**(`MasterMetrics`,接入进程的 `Registry`):

| 指标名 | 类型 | 标签 | 单位 | 含义 |
|---|---|---|---|---|
| `master_raft_term` | gauge | — | — | 当前 raft term |
| `master_is_leader` | gauge | — | 0/1 | 本节点是否为 leader |
| `master_applied_commands_total` | counter | — | 条 | 状态机 apply 的命令数 |
| `master_registered_nodes` | gauge | — | 个 | 已注册节点数(hard state) |
| `master_heartbeats_total` | counter | — | 次 | 收到的心跳数 |
| `master_admission_rejected_total` | counter | — | 次 | 被准入校验拒绝的请求数 |
| `master_partitions_created_total` | counter | — | 个 | 成功创建的分区数(M2) |
| `master_partition_create_failed_total` | counter | — | 次 | 建分区失败数(M2) |
| `master_transitions_issued_total` | counter | — | 次 | 下发的副本状态转移指令数(M3) |
| `master_rereplications_total` | counter | — | 次 | 触发的再复制次数(M3) |
| `master_replicas_removed_total` | counter | — | 次 | 下线迁移移除的副本数(M4) |
| `master_repairs_triggered_total` | counter | — | 次 | 手动 RepairPartition 补副本请求数(进入 RPC 即计,含失败) |
| `master_replicas_moved_total` | counter | — | 次 | 副本迁移(MoveReplica)完成后移除的迁出源副本数 |
| `master_under_replicated_partitions` | gauge | — | 个 | 欠副本(健康副本数 < RF)的分区数(reconciler 周期刷新,仅 leader) |
| `master_assign_requests_total` | counter | — | 次 | AssignPartition 请求总数(含失败;对标 SeaweedFS pick_for_write) |
| `master_assign_failures_total` | counter | — | 次 | AssignPartition 失败数(无可写分区等 ok=false 或错误返回) |
| `master_lookup_requests_total` | counter | — | 次 | LookupPartition 读路由查询数 |
| `master_partitions_rw` | gauge | — | 个 | RW 态分区数(reconciler 周期刷新,仅 leader) |
| `master_partitions_ro` | gauge | — | 个 | RO 态分区数(同上) |
| `master_partitions_sealed` | gauge | — | 个 | SEALED 态分区数(同上) |
| `master_writable_capacity_bytes` | gauge | — | 字节 | 可写容量上界 = RW 分区数 × `partition_capacity_bytes` |
| `master_nodes_up` | gauge | — | 个 | 心跳判活的节点数 |
| `master_cluster_free_bytes` | gauge | — | 字节 | 集群磁盘余量合计(心跳上报软状态汇总) |
| `master_cluster_used_bytes` | gauge | — | 字节 | 集群磁盘已用 = Σcapacity − Σfree |
| `master_max_replica_lag_bytes` | gauge | — | 字节 | 所有副本上报 lag_bytes 的最大值 |
| `master_majority_replicas_down_partitions` | gauge | — | 个 | 健康副本数(存活且未上报 ERROR)≤ RF/2 的分区数(对齐 Ambry isMajorityReplicasDown) |
| `master_maintenance` | gauge | — | 0/1 | 是否处于维护模式(1=自动调谐已暂停;reconciler 周期 set,仅 leader,切主后跟随 hard state;对齐 Ceph `noout`/`norecover` 状态) |
| `master_writable_used_bytes` | gauge | — | 字节 | 可写(RW)分区的实际占用 = Σ_over_RW `max(各副本上报 used_bytes)`(**口径:每分区取副本最大值,RF 副本不重复计**;RO/SEALED 不计入;对齐 Ceph `ceph df` 的 pool `STORED`) |
| `master_writable_free_bytes` | gauge | — | 字节 | 可写余量 = `master_writable_capacity_bytes` − `master_writable_used_bytes`(饱和减;对齐 Ceph `ceph df` 的 pool `MAX AVAIL`) |
| `master_replicas_by_disk` | gauge | `node_id`, `disk_id` | 个 | 每盘承载的副本数(hard placement 口径:已注册盘 ∪ placement 涉及的盘,含 0 副本的盘;对齐 Ceph `osd df` 的每 OSD 承载量) |
| `master_disk_skew` | gauge | — | 个 | 每盘副本数的 max − min(无盘 = 0;越小越均衡,均衡器目标信号;对齐 Ceph balancer 的 OSD 用量方差类指标) |
| `master_unhealthy_disks` | gauge | — | 块 | 主动探测报出不健康的盘数(`health` 非空且 ≠ `HEALTHY`;**探测未启用的盘(空串)不计**;reconciler 周期刷新,仅 leader) |
| `master_partitions_forced_ro_total` | counter | — | 个 | 因副本所在盘将满被自动置 RO 的分区数(**只置不回**,恢复靠运维;对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage`) |
| `master_pending_drops` | gauge | — | 对 | 待删源副本对数(`pending_drop`:placement 已移除、等源节点确认已删后清;源节点长期不回则该记账保留、gauge > 0;reconciler 周期刷新,仅 leader) |
| `master_dropped_directives_total` | counter | — | 次 | 下发的 `DROPPED` 指令数(迁移/下线完成后删源副本/目录;每轮心跳重发会重复计数) |
| `master_balance_pending` | gauge | — | 条 | 当前均衡计划内 pending(未受理)的搬迁数(无计划 = 0;reconciler 周期刷新,仅 leader;对齐 Ceph balancer 的 plan 进度观测) |
| `master_balance_in_flight` | gauge | — | 条 | 当前均衡计划内在途(已受理待摘源)的搬迁数(口径同上;≤ `balance_max_in_flight`) |
| `master_balance_moves_total` | counter | — | 条 | 均衡计划内被观测到进入 done 的搬迁数(切主后对已在终态的条目不重复计) |
| `master_balance_failed_total` | counter | — | 条 | 均衡计划内被观测到判败(在途超时/落点失效)的搬迁数(口径同上) |
| `master_blob_ids_requests_total` | counter | — | 次 | `GetBlobIds` 发号请求数(含失败) |
| `master_blob_ids_allocated_total` | counter | — | 个 | 已发给 client 的 blobId 个数(号段切分后实发数之和) |
| `master_blob_ids_failures_total` | counter | — | 次 | 发号失败数(非 leader / 无可写分区 / objectId 耗尽 / raft 提交失败) |
| `master_id_alloc_raft_commits_total` | counter | — | 次 | 为抬水位而做的 Raft 提交次数(摊薄效果 = 发放数 / 该值 ≈ 号段 × 倍数) |
| `master_id_alloc_holes_total` | counter | — | 个 | 换号段时未发完而被丢弃的号数(即"空洞";**无害**,仅作观测) |
| `master_node_down_total` | counter | — | 次 | 判定节点 down 的次数(**迟滞后**才计;连击达标那一刻记一次,持续 down 不每拍重复计)。便于确认判活没被一次心跳抖动污染;`node_down_debounce_ticks = 0` 时退化为"每拍超时即计",会持续增长(D5) |
| `master_rereplication_deferred` | gauge | — | 个 | 因每 tick 预算被推后到下轮的欠副本分区数。它让"我们正在**限速**"可见 —— 否则运维看到的现象是"系统不动了",而不是"在按预算补齐"(D6) |
| `master_disk_evac_planned_total` | counter | `result` | 次 | 坏盘撤离计划生成(`planned`)/ 被预算挡住或无法满足(`unsatisfiable`)的次数。**持续 `unsatisfiable` = 有坏盘要搬但没地方搬(需加节点/盘)或预算太小**(D4) |
| `master_disk_evacuating` | gauge | — | 个 | 在途撤离分区数(坏盘上那份副本还在硬放置里;受 `disk_evac_max_in_flight` 约束)。**注意它比硬放置晚一拍收敛**(本 tick 动作之前刷新)(D4) |

## 8. blobId 结构(D3 的落地)

blobId = store 的 `StoreKey`,**定长 64bit(8B)**:高 32bit `partitionId` + 低 32bit `objectId`。store 只当它是不透明 key,**解析字段是 router SDK / master 层的事**。

```
64bit blobId (8B):
┌────────────────────────┬────────────────────────┐
│ partitionId (高 32bit)  │ objectId (低 32bit)     │
│ 自带定位(D3),永不复用 │ partition 内单调递增    │
└────────────────────────┴────────────────────────┘
```

**已定**:
- `partitionId`(高 32bit,自带定位 D3);一旦分配**永不复用**(对齐 Ambry:blob id 永久占用);上限 42.9 亿 partition。
- `objectId`(低 32bit):**master 按 partition 单调号段发号**——号段两层批处理(master→client 号段 + master→Raft 水位块)+ Raft 水位持久化(多数派 fsync)+ 容忍空洞永不复用;见 `openspec/changes/blobid-master-assign`。上限 42.9 亿/partition,平均 16–64KB 文件配合定容远够(依据见该 change D8)。
- **不含 version 字段**:64bit 全给 partition+object,blobId 布局**永久冻结**(显式偏离 §3.4,理由见 blobid-master-assign D7)。
- **不编 account/container / dc**:64bit 无余量;多租户 / 多 DC 信息推到**网关层 / 独立元数据**(等接 S3/多 DC 时在 blobId 之外解决,不再靠 version 升级)。
- store 侧:`StoreKey`(8B)= 整个 blobId,不解析内部字段;较原 32B 方案索引每条省 24B(10¹¹ 省 ~2.4TB)。

**已知取舍**:
- objectId 单调递增 → blobId 可枚举;MVP 无 ACL + 数据口直连靠**网络隔离 / 数据口鉴权 / 网关 ACL** 兜底,不靠 blobId 不可猜性(blobid-master-assign D6)。

## 9. store server(数据节点)角色

store 引擎(BlobStore)的**多副本编排 + 网络 + 复制外壳**。store 保持纯单副本引擎,store server 负责按 master 指令管一台机上所有副本的生死、发心跳、跑数据面。对齐 Ambry `StorageManager → DiskManager → BlobStore`。

**层次**:
```
store server(一个进程 / 一台数据节点)
  └─ 挂载 N 个 disk(每盘一个 DiskManager,持有该盘目录 + 容量)
       └─ 每 disk 建一堆 BlobStore(每个 = 一个 partition-replica,store crate)
```

**组件**:
| 组件 | 职责 |
|---|---|
| DiskManager ×每盘 | 持有盘目录、该盘上的 BlobStore 集、盘容量/健康 |
| ReplicaLifecycle | master 指令(target_state)→ store 操作(见下表)|
| HeartbeatClient | gRPC 流:报活 + 各盘 `available_capacity`(store 自省)+ 各副本 current_state/lag |
| DataHttpServer | HTTP 数据面:put/get/replicate 路由到对应 BlobStore(D2) |
| ReplicationPuller | STANDBY 副本从 peer 拉式 anti-entropy;BOOTSTRAP 拉全量(D4) |
| LocalWorker | 本地常驻:周期 flush + 单店 compaction(死占比阈值) |

**指令 → store 操作映射(胶水核心)**:
| target / 分区态 | store server 动作 | 上报 current |
|---|---|---|
| OFFLINE→BOOTSTRAP | `BlobStore::create/open` + 拉全量(从 peer) | BOOTSTRAP |
| BOOTSTRAP→STANDBY | 开放全部请求(收 PUT)+ 继续拉复制 | STANDBY |
| STANDBY↔LEADER | MVP 折叠 ACTIVE,no-op(多 DC 才用) | ACTIVE |
| STANDBY→INACTIVE | 停写、只 GET、等 peer 追平 | INACTIVE |
| INACTIVE→OFFLINE | 停服务(仍持数据) | OFFLINE |
| OFFLINE→DROPPED | `BlobStore::close` + 删目录回收 | DROPPED |
| 分区 RW→RO/SEALED | 该 replica 只读,拒 PUT | — |
| task 再复制 | = OFFLINE→BOOTSTRAP(选空盘建店 + 拉全量) | — |
| task EC 转换(D5) | 交 P1 scheduler / 本地 worker 做段级 EC | — |

**本地 worker vs P1 scheduler 分工(澄清)**:
- **本地 worker(store server 常驻,永远要有)**:flush / 单店 compaction —— 都是"单 BlobStore 内、本地、轻"(对齐 Ambry CompactionManager 跑在 StorageManager 里)。
- **P1 独立 scheduler(集群级、跨节点、重)**:再复制编排 / 跨节点再平衡 / EC 转换调度(master 决策 + 跨节点搬数据)。
- → compaction/flush 是本地事,**不等 P1**。

**store 连接点**:
- 生命周期:`create/open/close`(已成)+ **需补 `drop`(删目录回收)**。
- 容量:store 的 `available_capacity`/`is_full` → 心跳上报 master → 喂 §7.3 容量感知放置(闭环)。
- 读写/后台:`put/get`/`flush`/`compact_if_needed`(已成)直接被编排,**store 基本不改**。

**待拍/开放**:
- [x] **N1 命名**:`store server`(数据节点)。
- [x] **N2 拉复制归属**:**内嵌 store server**(对齐 Ambry ReplicationManager,同进程贴本地副本;用 N4 每盘池限流)。
- [x] **N3 bootstrap 拉全量方式**:**混合,全部实现(不推后,MTTR 优先)**——封存段(不可变)走**物理流式拷贝**(快;对齐 Ambry `FileCopyBasedReplication` / SeaweedFS 卷拷;disk-aware 每盘限并发,呼应 N4);活跃段尾巴走**逻辑 catch-up**(token,复用拉复制)补拷贝期间的新写。协议需含 FileCopy 类消息(§7.4)。
- [x] **N4 本地并发模型**:**每盘一个小线程池**(池大小按盘 IO 能力定;同盘上的 BlobStore 共享;贴合 §9 DiskManager + Ambry DiskIOScheduler)。异步网络层(axum/tonic)通过该池 offload 阻塞的 store 调用,不卡 tokio reactor;并发被限制在硬件可承受范围。衔接 store D4 P4。
  **⚠️ 已落地(store 侧,地基性变更)**:每盘自建 worker 线程 + 有界队列 + "短暂排队后 503"的准入语义;
  配置 `store_server.data_dirs[].pool_size` / `pool_default_size` / `pool_queue_size` / `pool_acquire_timeout_ms`;
  **引擎 IO 仍同步阻塞,未引入 async/AIO**(与 `io-index-design` D1 一致)。详见
  [`store.md` §12.11](./store.md)(含测试证据表与残留风险清单)与 `openspec/changes/store-disk-isolation/`。
  **⚠️ 配套已落地(store 侧 D2/D3)**:每盘**可服务性闸门**(盘被判定不可服务 ⇒ 该盘上所有 `/partitions/**`
  请求快失败 `503 + Retry-After`,含拉复制读/`replica-metadata`/`segments`;`/metrics` 与 `/admin/**` **永不被挡**)
  + **IO 看门狗**(闭包内 syscall 计时,不受探测开关约束)。详见 [`store.md` §12.12](./store.md)。
  **master 侧 D4/D5/D6 仍待续**(见下面「待续」)。
- [ ] **store 需补 `drop`**:关闭 + 删 partition-replica 目录、回收磁盘。

### 9.1 实现状态 + 配置/监控清单(CLAUDE.md §7 必须项)

**S1(数据节点骨架 + 数据面)已实现**(`crates/store`,见 `openspec/changes/store-server/`):`DiskManager`(`statfs` 探真实容量,Q1)、`ReplicaManager`(按 partition 管 `BlobStore`,幂等)、`DataHttpServer`(axum `PUT`/`GET`,同步引擎经 `spawn_blocking` offload)。过集成测试(put/get 往返 + 409/404/400)+ 单测(statfs、ensure_replica 幂等)。**不接 master / 不复制 / 不含 EC(暂不解锁)**。

**S2(接入集群 + 磁盘监控)已实现**(见 `openspec/changes/store-server-heartbeat/`):`RegisterNode` 启动注册 + 双向流心跳(上报盘 `statfs` 余量 + 各 replica `current_state` + 盘 `health`)+ ReplicaLifecycle(消费 `MasterDirective.transitions`:ACTIVE/BOOTSTRAP→建 replica,OFFLINE/DROPPED→关);**磁盘监控照搬 Ambry 两级**——被动(`store` 引擎连续 IO-error 自关 + hook → replica `current_state=ERROR` → 心跳上报 → **master reconcile 再复制**;master `plan_rereplication` 已纳入 ERROR 副本)、主动(`DiskHealthCheck` 周期 write/read/verify/delete + 超时,默认关,随心跳上报;**每盘至多一发在飞探测**——超时后不再叠加新探测、改收它的迟到结果,见 [store.md §12.11](./store.md))。过集成测试(注册→放置指令→建 replica→HTTP put/get)+ 单测(errored 再复制、健康探测、lifecycle)。

- 数据面端点(`crates/store`):`PUT`/`GET`(S1)+ `DELETE`(变更类,见 `openspec/changes/store-server-mutations/`,承 store 引擎语义 + 状态码映射;**删除不可逆**,见 `openspec/changes/simplify-delete-model/`)+ `GET .../replica-metadata` 与 blob 读 `?replication=1`(拉复制,见下 S3a)+ `GET /metrics`(Prometheus 文本导出,无鉴权)+ `POST /admin/partitions/{pid}/compact`(手动触发该分区本地副本全库 compact,对齐 Ambry `TriggerCompaction`;响应 JSON `{live_bytes, dead_bytes, segments_before, segments_after}`,无本地副本 → 404)。
  - **`PUT ...?expiration_ms=<i64>`**(见 `blob-io-end-to-end`):建对象时**即带 TTL**(分片上传的孤儿回收需要"出生即带 TTL")。
  - **`GET .../blobs/{key}/metadata`**(见 `blob-io-end-to-end`):只读元数据**不读数据体**的 JSON(`BlobStore::stat`),供 frontend 的 `HEAD` 用;已删/已过期也返回 200 + 标志(元数据可读 ≠ 数据体可读),从未写过 → 404。
  - **流式 PUT(见 `blob-io-end-to-end`)**:带 `Content-Length` 时:≤ 1 MiB(常量 `INLINE_BODY_MAX`)走**内存路径**
    (`Bytes` → 引擎整块写);> 1 MiB 才 body 边收边落**该副本所在盘的暂存目录**(`<disk>/tmp/`,内存 O(64KiB))
    再交引擎 `BlobStore::put_from_reader` 按已知长度顺序写。无 `Content-Length`(chunked)回落原缓冲路径。
    暂存文件成功/失败都删,崩溃遗留由 `DiskManager::mount` 清理。**为什么分档**:每个小对象都落一个暂存文件会多出
    `create/write/close/unlink` 的元数据开销,实测把 PUT p50 从 ~1ms 拖到 ~9ms(release,4 KiB × 并发 32)。
  - **写满 → `507 Insufficient Storage`**(此前落 500):压测/运维据此看出"该加分区了"(`map_mut_err`)。
  - **"单条记录放不进一个段" → `400`**(`Error::BadArgument`):body 超过该分区 `segment_size_bytes` 时,引擎拒绝
    在**单条记录**里落盘 —— 这是客户端可控的问题(该分片/该调 segment 大小),不能混进 500。注意由此推出的配置约束:
    `store_server.max_body_bytes` 应 ≤ `segment_size_bytes`(否则会出现"大小检查放行、引擎却存不下"的 400)。
  - **暂存文件在请求被取消时也要清掉**:store 侧用 RAII 守卫(`StagedFile` 的 `Drop`)删除 —— 客户端断开/上游 abort 会
    直接 drop 掉 handler future,"`.await` 之后手写 `remove_file`"那条路径根本不执行(实测一次 2 MiB × 3000 次压测
    漏出 1600+ 个暂存文件);`DiskManager::mount` 仍会清理崩溃遗留。
- **一致性巡检素材(Wave3,只读,豁免白名单内)**:`GET /partitions/{pid}/scrub-report`(副本摘要,只走索引/mmap 不读数据体:`live_records`/`tombstones`/`live_bytes`/`sealed_segments`/段清单/log 末端游标 + **带伤启动计数 `damaged_records`/`abandoned_segments`**);`GET /partitions/{pid}/verify?max_bytes=`(本地完整性校验:逐记录验 CRC 但**坏记录记下继续**,回 `records`/`bad_records`/`first_bad_offset`/`index_mismatch`/`truncated`,缺省上限用配置 `scrub_max_batch_bytes`)。无本地副本 → 404。**记录损坏不再让副本起不来**:`open` 容错跳过坏记录(不删不改盘上字节)、记 ERROR 日志 + `store_open_bad_records_total`/`store_open_abandoned_segments_total`,并把这笔账经 `scrub-report` 的 `damaged_records`/`abandoned_segments` 暴露给巡检(健康副本恒为 0,故带伤副本在一致性巡检里直接表现为差异)。语义与残留边界见 [`store.md` §12.10](./store.md)。
- **运维端点 `POST|GET /admin/replication-control`(A7)**:运行期暂停/恢复**本节点主动拉复制**。`POST` body `{paused}` → 200 `{paused}`;`GET` 回当前状态。只影响本节点向 peer 拉取(别人来拉本节点完全不受影响),与请求静默正交;**不持久化**(重启即恢复拉取),也不动已积累的复制游标(恢复后从原游标续拉)。对应 gauge `store_server_replication_paused`。
- **异步 compact 台账(A3)**:`POST /admin/partitions/{pid}/compact?async=1` 立即回 200 `{job_id}`,后台在阻塞池跑 `BlobStore::compact()`(不带 `async` 走原同步路径,响应体不变);`GET /admin/compactions` 回最近 32 条任务(最新在前,超出逐条淘汰最旧),`GET /admin/compactions/{job_id}` 回单条,未知/已淘汰 → 404。任务 JSON:`{job_id, partition_id, state: running|done|failed, started_at_ms, finished_at_ms?, result?{live_bytes,dead_bytes,segments_before,segments_after}, error?}`(未完成字段**不出现在 JSON 里**)。台账是**进程内内存态**(重启即丢),只服务"这次 compaction 跑完没";compaction 本身幂等,崩溃由引擎侧 `COMPACT_COMMIT` 收敛,与台账无关。
- **运维端点 `POST|GET /admin/request-control`(批 1)**:节点级/分区级**请求静默**(对标 Ambry `RequestControl` / `DataNodeRequestControl`)。`POST` body `{op, partition_id?, enable, ttl_secs?}`(`op` = `DataOp::as_str` 真值:put/get/delete;未知 op → 400,本地无该副本 → 404;`ttl_secs` 缺省用 `store_server.request_control_default_ttl_secs`,`0` = 立即到期),`GET` 回当前禁用集(已过期条目按"视为启用"剔除);命中禁用的数据面请求 → **503**(+ 说明文本,有剩余时长时带 `Retry-After`)。**豁免清单**:`?replication=1` 的 blob 读、`GET .../replica-metadata`、`GET .../segments*`、`GET /metrics`、**任何 `/admin/**`(含本端点自身)** 永不被禁——禁掉前四类会让副本停更(踩持久性红线),禁掉最后一类会做出"无法远程恢复"的节点。状态只在内存、不持久化(重启即全开),过期靠惰性判定。语义与偏离详见 [`store.md` §12](./store.md)。

**S3a(拉复制逻辑核心)已实现**(见 `openspec/changes/store-server-replication/`):store 引擎新增**忠实 apply 原语** `BlobStore::apply_replicated`(**墓碑恒胜**:peer 墓碑覆盖本地 PUT,peer PUT 永不覆盖本地 PUT/墓碑;只增不删幂等,原样保留 peer 的 flags/expiration,守 §3.1;防副本发散);store-server 新增 `replica-metadata` HTTP 端点(step-1,`find_entries_since`)+ blob 读 `?replication=1`(step-2,含**已过期**活记录的数据体;墓碑只写状态、不再传数据体,见 `simplify-delete-model`)+ `replication.rs`(`PeerSource` trait + `replicate_from_peer` 收敛循环 + `HttpPeer` 阻塞 adapter);后台复制器经 master `LookupPartition` 发现 peer(排除自身、按 `up` 过滤),对每个本地 replica 的每个 peer 周期拉一轮;BOOTSTRAP 从 `token=Uninitialized` 全量逻辑回放追平。过单测(apply 四态 + 收敛/幂等/bootstrap)+ 集成测试(两 store-server 真 HTTP 收敛 + delete 传播)。
  - **N3 分期(文档化,非代码就地另选,见该 change design.md D-A)**:S3a 只落**逻辑拉复制**(正确收敛的完备核,含 bootstrap 全量逻辑回放);**S3b** 再叠加封存段**物理流式拷贝**(MTTR 优化)。N3 最终形态(混合)不变。

**S3a 加固:复制游标重定位已实现**(见 `openspec/changes/store-replication-cursor-revalidate/`):游标是对端 log 坐标系的绝对位置,而 compaction 会改写坐标系 —— 全库 compact 段位置从 0 重来、增量 compaction 把记录搬到更高位置的新代次段。若把"拿陈旧游标问对端、对端返回空批"当成"已追上",就会在**作废的坐标系**上判追平 → 副本带着缺失数据转 `ACTIVE` → master 删源闸门误成立 → **静默丢数据**。现按 Ambry `revalidateStoreFindToken` 补齐两道:
  - **两层身份**:**段身份** = `LogSegmentName`(位置 + 代次;段名 `{position}_{generation}_log.seg`,段头也**自述**同一个代次)。代次在**同一 log 目录内单调递增**(每滚一段 +1),全库 compact 接着旧 log 的**最大代次**往后取 ⇒ 段身份**永不复用**;凡"整体重写 log、段位置从头开始"的操作(全库 compact)、滚动、增量 compaction 建新代次段,都会让旧游标认不出自己的段(段没了,**或位置还在、代次不同**)。**副本身份** = `LOG_INCARNATION`(副本目录里的 u64 文本),由 master 在**从零建这份副本**时发放(见 §7.4 `AssignReplicaIncarnation`),副本重建 / **换盘后重来**必然换新号;同一份副本的全库/增量 compaction **不换** incarnation。身份文件缺失(物理 bootstrap 只拷段;或单机模式没有 master)时由 `BlobStore::open` 用**本机当前时刻**兜底并落盘 —— **刻意不从段头取 `created_ns`**(拷贝来的段诞生在对端,那个时刻正是对端自己的兜底身份,拿它当本副本身份会在"申领没成功"时与对端撞号,使"重建 ⇒ 身份必换"失效)。
  - **重定位**:对端认不出游标(段没了 / **代次不符** / 偏移越界 / **incarnation 不符** / 四件套 `pos`+`gen`+`off`+`incarnation` 残缺)**或**本轮发现对端 incarnation 变化 ⇒ 一律**重置为从头重扫**(方向是多拉,安全),并在元数据响应里报 `token_reset`;**那一轮不得判追平**。会话状态(游标 + 对端 incarnation + 无进展计数)收在 `replication::PullSession`,生产 loop 与集成测试共用同一份;连续两轮"游标没动且没 apply" ⇒ 丢弃游标从头重拉(防永远卡 `BOOTSTRAP`)。
  - **判据前置**:转 `ACTIVE` 的判据 `caught_up_with_peer` **先看 `cursor_reset_reason` 是否为空**(非空直接不算追平 —— 那种"末端一致"是在刚作废的坐标系上算出来的),再看"peer 无该副本"与本地末端。**本地末端探测是纯位置的** `BlobStore::has_content_after`(不校验段身份、不触发重定位)—— peer 游标的段身份属**对端坐标系**(本地段位置/代次与对端并不一一对应),拿它进 `find_entries_since` 会被重定位判定误伤。
  - **传输(细化 D2,见 design.md D-B)**:node↔node 复制元数据走 store-server 现有 **HTTP(axum)**(非新起 gRPC server);master↔node 控制面仍 gRPC。

**S3b(封存段物理拷贝,N3 混合完成)已实现**(见 `openspec/changes/store-bootstrap-copy/`):`blob` 引擎加封存段只读拷贝原语(`sealed_segments`/`read_segment_prefix`/`LogSegment::raw_prefix`)+ `open_rebuild_index_with`;数据节点加发送端点(`GET .../segments`、`GET .../segments/{position}_{generation}`)+ 接收装载 `ReplicaManager::install_bootstrap`(段文件 `tmp→fsync→rename` 崩溃安全 + 重建打开);复制器对**空**副本首次先物理拷贝封存段(不可变),续拉游标 = 最高封存段末尾(**并绑定 peer 的 incarnation**),再逻辑补活跃尾;无 peer/无封存段回落纯逻辑。过单测(封存段枚举/拷贝重建)+ 集成测试(多封存段 + 活跃尾混合 bootstrap,真 HTTP 收敛)。
  - **清单与取段的形态**:`GET .../segments` 回 `{incarnation, segments:[{position, generation, data_end}]}`(每段带**代次**,清单连同**本副本 incarnation** 一起回来 —— bootstrap 后的初始游标据此绑定副本身份);取段是 `GET .../segments/{position}_{generation}`,路径参数就是**段身份**:认不出该段 → **404**,畸形名(位置/代次解析不出)→ **400**。
  - **与 compact 的竞态(已加固)**:分段拷贝**前后各列一次清单**,任一条成立即**丢弃整份拷贝**、回落纯逻辑拉取:① **取段 404 / 出错**(段身份已变,或网络失败);② 取回的字节**段头自述代次**与清单不符(`peek_generation` 再核一道,防"不发 404 的换段路径");③ 拷完再列清单发现对端 **incarnation 变了**(整份副本被重建)。否则"同位置、新代次"的字节会被当成清单里那一段装进去 —— 逐段取来的字节横跨两代副本,拼出的是**混合快照**(还可能带着对端已删的活记录)。丢弃重来是幂等的、必然收敛。
 - **已落地(2026-09-24)**:store 侧 D1 每盘有界池 / D2 健康闸门 / D3 IO 看门狗
  (见 [store.md §12.11](./store.md)(线程模型)与 [§12.12](./store.md)(闸门 + 看门狗,含测试证据表与残留风险清单));
  **master 侧 D4 坏盘撤离 / D5 判活迟滞 / D6 每 tick 再复制预算**(配置与监控见下面两张表/§7.7)。
- 待续:`DROPPED` 删目录回收;admin「磁盘指标」页接入真队列深度与健康状态;
  全部阈值的**压测标定**(默认值都是经验初值)。
- **EC 仍锁定(暂不解锁)**。
 - **已补齐(见 `blob-io-end-to-end`)**:router SDK(router 库 + CLI)、frontend(REST 网关)、master 发号 `GetBlobIds`、
   流式 put(网络流式 + 落盘暂存 + 引擎 `put_from_reader`)、`.../metadata` 端点、`StoreFull → 507`、`PUT ?expiration_ms=`。

**S4(本地维护 worker)已实现**(见 `openspec/changes/store-local-worker/`):数据节点常驻后台 `worker.rs`,对本节点每个本地 replica 分**两条**独立 loop 驱动 `blob` 引擎的 **flush**(推进持久化边界,缩小 W=2 page-cache 掉电窗口)、**compaction**(增量压缩);同步引擎调用经 `spawn_blocking` offload。复用引擎既有崩溃安全实现(不改写路径语义),单店失败只记 warn 不拖垮全局。过单测(flush 后重开可读 / compaction 回收死数据)。

**compaction 策略已升级**(见 `openspec/changes/store-compaction-policy/`):候选集与回收口径都不再是"无脑扫一遍"——

- **候选集**:按 master 下发的分区可写态挑 replica(`MasterDirective.partition_states`)—— `SEALED` 永不入选;`RO` 总是入选;`RW` 由 `compaction_include_rw` 决定(默认纳入,且**只压其封存段**,活跃段照写);状态未知(未接 master 的单机模式)按可压处理;未追平(`BOOTSTRAP`)的副本跳过(压了也要从 peer 拉回来,还会让复制游标失效)。
- **回收口径**(引擎 `CompactPolicy`):**被取代的旧记录 + TTL 已过期对象 + 墓碑已过保留期**三类字节真正释放。墓碑的删除时刻复用索引条目 `expiration_ms` 槽位存放(磁盘布局不变;旧数据 `-1` = 删除时间未知 ⇒ 永不自动回收)。
- **墓碑回收闸门**(`CompactPolicy.tombstones_allowed`):过保留期**只说明**"删除已足够久",还要过 master 这道闸门 —— 判据 = 该分区**所有副本都已追平**(各副本上报 `ACTIVE` 且 `lag_bytes == 0`;缺上报 / 节点 down / 空 placement 都算未追平),随心跳 `MasterDirective.partition_states` **每拍全量**下发,store 逐条落到对应副本句柄上。闸门关闭时墓碑**一律照搬**(连已过保留期的也留):还有副本没追平时一收,那份滞后副本就永远学不到这条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。**单机模式**(未接 master)没有别的副本可滞后,恒放行;句柄初值 `false`(没拿到权威放行前不回收)。
- **闸门**:partition 整体可回收占比 ≥ `compaction_dead_ratio` 才动手,再对达阈的非活跃段按同一阈值选段。
- **调度**:「首跑钟点 + 周期」—— 首次对齐本地 `compaction_start_time`,之后每 `compaction_interval_secs` 一次(默认 `01:00` + 86400s = 每天凌晨一次,跑在低峰)。
- **手动 compact**(`POST /admin/partitions/{pid}/compact`):不受状态/阈值闸门约束(运维强制),但回收口径与定时一致(仍守墓碑保留期)。

**配置项清单**(`StoreServerConfig`,TOML `[store_server]` 表下;加载用 `Loader::load_table("store_server")`,**同时兼容顶层 flat key**):

| key | 类型 | 默认值 | 取值/校验 | 含义 |
|---|---|---|---|---|
| `store_server.node_id` | u64 | 1 | ≥1 | 数据节点 id(与 master 一致) |
| `store_server.data_dirs` | Vec\<String\> | `[]` | 非空 | 挂载盘目录(每个当一块盘,`statfs` 探容量) |
| `store_server.http_addr` | String | `127.0.0.1:7200` | 非空 | 数据面 HTTP 监听地址 |
| `store_server.partition_capacity_bytes` | u64 | 100 GiB | ≥1 | 每个分区副本 `BlobStore` 容量上界(对齐 master) |
| `store_server.master_addr` | String | `""` | — | master 控制面 gRPC 地址;空=不接 master(S1 独立)(S2) |
| `store_server.cluster_secret` | String | `""` | — | 集群准入密钥(与 master 一致);空=关校验(S2) |
| `store_server.heartbeat_interval_secs` | u64 | 3 | ≥1 | 心跳周期(S2) |
| `store_server.disk_health_enabled` | bool | false | — | 主动磁盘探测开关(默认关,对齐 Ambry)(S2) |
| `store_server.disk_health_interval_secs` | u64 | 60 | ≥1 | 主动探测周期(S2) |
| `store_server.disk_health_timeout_secs` | u64 | 10 | ≥1 | 主动探测单次读写超时(抓盘 hang)(S2) |
| `store_server.replication_enabled` | bool | false | — | 拉复制开关(默认关);需 `master_addr` 发现 peer(S3a) |
| `store_server.replication_interval_secs` | u64 | 5 | ≥1 | 拉复制周期(S3a) |
| `store_server.replication_max_batch_bytes` | u64 | 4 MiB | ≥1 | 单次向 peer 拉元数据的批字节上限(限流)(S3a) |
| `store_server.bootstrap_copy_enabled` | bool | true | — | 封存段物理拷贝加速 bootstrap(S3b);随 `replication_enabled` 生效 |
| `store_server.local_worker_enabled` | bool | true | — | 本地维护 worker 开关(S4:周期 flush/compaction) |
| `store_server.flush_interval_secs` | u64 | 30 | ≥1 | 本地 worker flush 周期(推进持久化边界)(S4) |
| `store_server.compaction_interval_secs` | u64 | 86400 | ≥1 | 本地 worker **compaction 周期**:跑一次之后隔多久再跑(默认一天一次)(S4) |
| `store_server.compaction_start_time` | String | `"01:00"` | 严格 `HH:MM`(本地时间) | compaction **首跑钟点**:首次对齐到下一个该钟点(今天没过即今天,过了即明天),之后每 `compaction_interval_secs` 一次(S4) |
| `store_server.compaction_dead_ratio` | f64 | 0.3 | (0,1] | **可回收**字节占比 ≥ 此值才压缩(partition 级闸门与段级选择同一阈值);可回收 = 被取代的旧记录 + TTL 过期对象 + 墓碑过保留期(S4) |
| `store_server.compaction_include_rw` | bool | true | — | 是否把 **RW 分区**也纳入候选集(仅压其封存段);`SEALED` 永不入选,`false` = 只压 RO(S4) |
| `store_server.compaction_tombstone_retention_secs` | u64 | 86400 | ≥0 | **墓碑保留期**:`delete` 后满此时长才允许回收(`0` = 立即允许);应大于副本可能滞后的上界,防副本修复复活已删对象(S4) |
| `store_server.max_body_bytes` | u64 | 2097152(2 MiB) | ≥1 | 数据面单请求 body 上限(挂在数据面路由的 `DefaultBodyLimit`;**流式 PUT 另按 `Content-Length` 前置校验,超限 413 且不读 body**)。默认与 axum 的 `DefaultBodyLimit` 默认值一致(§5.1 不擅改默认),要收真正的大 blob 须显式调大 |
| `store_server.request_control_default_ttl_secs` | u64 | 1800 | ≥0 | `/admin/request-control` 请求未传 `ttl_secs` 时的默认静默时长(秒);**0 = 不设默认时长(即留空即永久禁用)**(批 1) |
| `store_server.scrub_max_batch_bytes` | u64 | 67108864 | ≥1 | `GET /partitions/{pid}/verify` 缺省的单次扫描读取字节上限(深扫读全量数据体,限流防打爆盘;到点响应 `truncated=true`)(Wave3) |
| `store_server.data_dirs` | Array\<String \| Table\> | (必填) | 至少一项;basename 必须唯一;空目录/每盘池 <1 拒 | 盘声明:裸目录或 `[[store_server.data_dirs]]` + `dir`/可选 `pool_size`(**地基性变更**见 store.md §12.11) |
| `store_server.pool_default_size` | u32 | 16 | ≥1 | **每盘**默认池大小(worker 线程数):没给 `pool_size` 的盘用它。每个 worker = 1 条 OS 线程 + 1 次在飞 IO;上线前按盘类型标定 |
| `store_server.pool_queue_size` | u32 | 32 | ≥0(0 = 不排队) | 每盘池排队容量;池满时短暂排队,超 `pool_acquire_timeout_ms` 即 **503 + `Retry-After`** |
| `store_server.pool_acquire_timeout_ms` | u64 | 200 | ≥0(0 = 不等) | 准入等待上限(等空位,不等这次 IO);见 store.md §12.11 |
| `store_server.staging_dir` | String | `""` | 空 = 默认;**非空不得带前后空格** | **流式 put 的暂存目录**:空 = 各数据盘的 `tmp/` 子目录(逐盘,既有默认);非空 = 本节点所有盘的暂存文件都落该目录(要把大对象中转从数据盘挪开时用)。**两条代价**:该目录成为单点(它所在的盘坏 ⇒ 全节点大对象上传一起失败,小对象走内存路径不受影响)、空间口径变为"在途大对象数 × 单对象大小"且不进任何容量上报。**别指 tmpfs**(吃内存)。目录由启动挂载创建 + 清遗留 `*.tmp`(见 store.md §12.11) |
| `store_server.disk_watchdog_ms` | u64 | 30000 | ≥1 | **IO 看门狗**:单次引擎 IO 的**系统调用**时长软上限(闭包内测,**不含**池排队);超时 ⇒ 记一次 `IO_TIMEOUT` 坏观测,再由迟滞决定是否摘盘。必须远大于 p99.9 与 compaction 大 IO 时长;**不受 `disk_health_enabled` 约束**(探测可关,看门狗不可关)(store.md §12.12 D3) |
| `store_server.disk_degraded_io_ms` | u64 | **0(关)** | ≥0 | **DEGRADED 阈值**(毫秒):闭包内 ≥ 本值即记一次"慢 IO"。关着时整档不生效(也零开销:连计数都不读)。开着时:该盘仍服务、master 不再放新副本、不触发撤离 |
| `store_server.disk_degraded_ratio` | f64 | 0.1 | (0,1] | 一轮"慢 IO"占比 ≥ 本值即算该轮劣化;连续多轮(走 `disk_health_bad_rounds`/`bad_secs`)才报 `DEGRADED` |
| `store_server.disk_watchdog_ms_by_op` | Table\<op→u64\> | 空 | key 必须是已知池上 op;值 ≥1 | **按 op 覆盖**看门狗门槛(毫秒):给 compaction/flush 这类拷整段的大 IO 单开一档,避免它们的长耗时被误判成盘超期。覆盖值可小于默认。拼错的 key 当场拒(否则门槛静默失效) |
| `store_server.disk_watchdog_escalate_secs` | u64 | 0 | 0,或 ≥ `disk_watchdog_ms` 的秒数 | **逃生舱**:某盘池全部 worker 被超期 IO 占住且持续此秒数 ⇒ `process::abort()` 交 supervisor 重启。**默认 0 = 永不升级**:antfs 单进程多盘,自杀会带走健康盘上的服务,误杀 = 自造全节点故障(D3) |
| `store_server.disk_health_bad_rounds` | u64 | 3 | ≥1 | 迟滞**进入**判据一:连续 N 轮坏 ⇒ 该盘 `UNAVAILABLE`(D2) |
| `store_server.disk_health_bad_secs` | u64 | 30 | ≥1 | 迟滞**进入**判据二(与上者**取先到**):坏态持续 ≥ T 秒也置 `UNAVAILABLE`。防"探测周期调大后隔离太慢"(D2) |
| `store_server.disk_health_good_rounds` | u64 | 3 | ≥1 | 迟滞**退出**判据一:连续 M 轮好才考虑复位(D2) |
| `store_server.disk_health_recover_stable_secs` | u64 | 120 | ≥0(0 = 只按轮数) | 迟滞**退出**判据二:从进入连续好态起还要**稳定满 T2 秒**。复位比进入更严是刻意的(D2) |

> store 引擎自身配置(段大小等)复用 `[store]` 段(见 store.md §10);`start_with` 可注入(测试/小部署)。

**监控项清单**(`StoreServerMetrics`):

| 指标名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `store_server_put_total` | counter | 次 | blob PUT 请求数 |
| `store_server_get_total` | counter | 次 | blob GET 请求数 |
| `store_server_put_bytes_total` | counter | 字节 | 写入字节数 |
| `store_server_get_bytes_total` | counter | 字节 | 读取字节数 |
| `store_server_replicas` | gauge | 个 | 本地 partition-replica 数 |
| `store_server_errors_total` | counter | 次 | 数据面错误数,label `kind`(`io` / `unavailable` / `join_error`) |
| `store_server_request_duration_seconds` | histogram | 秒 | 数据面请求延迟(handler 入口 → 引擎完成,含 spawn_blocking 排队),label `op`(`put`/`get`/`delete`/`replica_metadata`/`segments`) |
| `store_server_in_flight_requests` | gauge | 个 | 在途数据面请求数,label `op` |
| `store_server_manual_compactions_total` | counter | 次 | 手动 compact 端点(`POST /admin/partitions/{pid}/compact`)实际触发次数 |
| `store_server_heartbeats_total` | counter | 次 | 收到的心跳指令数(S2) |
| `store_server_replication_rounds_total` | counter | 次 | 拉复制轮次(S3a) |
| `store_server_replication_applied_total` | counter | 条 | 从 peer apply 的记录数(S3a) |
| `store_server_replication_bytes_total` | counter | 字节 | 从 peer apply 的字节数(S3a) |
| `store_server_replication_errors_total` | counter | 次 | 拉复制错误数(S3a) |
| `store_server_replication_cursor_resets_total` | counter | 次 | 复制游标**重定位**次数,label `reason` = `incarnation_mismatch` / `segment_generation_mismatch` / `segment_missing` / `offset_out_of_range` / `no_progress`(**非零正常**,持续增长才值得查)(S3a 加固) |
| `store_server_bootstrap_segments_total` | counter | 个 | bootstrap 物理拷贝的封存段数(S3b) |
| `store_server_bootstrap_bytes_total` | counter | 字节 | bootstrap 物理拷贝字节数(S3b) |
| `store_server_flushes_total` | counter | 次 | 本地 worker flush 次数(S4) |
| `store_server_compactions_total` | counter | 次 | 本地 worker 发生压缩的 replica 次数(S4) |
| `store_server_blob_request_duration_seconds` | histogram | 秒 | **成功** blob 请求延迟,label `op`(put/get)× `size`(small/medium/large,分桶边界 50 KiB / 1 MiB);错误/早退路径不 observe(批 1) |
| `store_server_rejected_requests_total` | counter | 次 | 被**闸门**拒掉的请求数(在进引擎之前),label `op` × `reason`(`disabled` = RequestControl 人工静默 / **`disk_unavailable` = 盘被隔离**;三者口径区别见 store.md §12.12「四」)(批 1 + D2) |
| `store_server_replication_paused` | gauge | — | 拉复制是否被运维暂停(`/admin/replication-control`;1 = 暂停本节点主动拉,0 = 正常)(A7) |
| `store_server_scrub_reports_total` | counter | 次 | `GET /partitions/{pid}/scrub-report` 调用次数(Wave3) |
| `store_server_verify_runs_total` | counter | 次 | `GET /partitions/{pid}/verify` 调用次数(Wave3) |
| `store_server_verify_bad_records_total` | counter | 条 | 本地校验发现的坏记录总数(Wave3) |
| `store_server_verify_duration_seconds` | histogram | 秒 | 单次本地校验耗时(Wave3) |
| `store_server_compaction_jobs_total` | counter | 次 | 后台 compact 任务按状态计数,label `state`(`running` / `done` / `failed`;提交即计 `running`,终态再计一次)(A3) |
| `store_server_replicas_dropped_total` | counter | 个 | 按 master `DROPPED` 指令删掉的本地副本数(A3 同期) |
| `store_server_disk_requests_total` | counter | 次 | 落在该盘上的数据面请求数(`rate()` = 该盘 IOPS;含随后被拒的请求,见 store.md §12.11),label `disk_id` × `op`(盘 hang 隔离批) |
| `store_server_disk_bytes_total` | counter | 字节 | 该盘上**成功**请求的字节数(`rate()` = 该盘带宽),label `disk_id` × `op`(同上) |
| `store_server_disk_request_duration_seconds` | histogram | 秒 | 该盘上请求的**端到端**延迟(含阻塞池排队),label `disk_id` × `op`(同上) |
| `store_server_disk_in_flight_requests` | gauge | 个 | 该盘在途请求数;UI 上作为**排队深度代偿**(真队列深度随每盘池落地),label `disk_id` × `op`(同上) |
| `store_server_disk_slow_requests_total` | counter | 次 | 端到端超过 `disk_health_timeout_secs` 的请求数(**超时计数**),label `disk_id` × `op`(同上) |
| `store_server_disk_probe_stuck` | gauge | 0/1 | 该盘是否有一发探测卡在盘里未归(`disk_health_enabled` 关闭时恒 0),label `disk_id`(同上) |
| `store_server_disk_probe_skipped_total` | counter | 轮 | 因上一发探测未归而跳过的探测轮次数(× 周期 ≈ 卡住时长),label `disk_id`(同上) |
| `store_server_disk_pool_workers` | gauge | 个 | 每盘池的 worker 线程数(启动时定死),label `disk_id`(**地基性变更**见 store.md §12.11) |
| `store_server_disk_pool_in_flight` | gauge | 个 | 每盘池**已准入未完成**的任务数(含排队中),label `disk_id`(同上) |
| `store_server_disk_pool_queued` | gauge | 个 | 每盘池**已准入但未被 worker 取走**的任务数 = **真队列深度**(与在飞代偿口径不同),label `disk_id`(同上) |
| `store_server_disk_pool_rejected_total` | counter | 次 | 每盘池拒绝的提交数,label `disk_id` × `op` × `reason`(`saturated` 池满超时 / `job_panicked` 任务 panic / `unknown_disk` / `closed`);**持续非零 = 该盘供不上,该调池或查盘**(同上) |
| `store_server_disk_io_duration_seconds` | histogram | 秒 | **闭包内**测得的单次 IO 时长(syscall,**不含**池排队)—— "这盘自己快不快"的唯一正确口径(与 `disk_request_duration_seconds` 的端到端口径**刻意区分**)。桶铺到 120s(看门狗 deadline 就是要看的长尾),label `disk_id` × `op`(store.md §12.12 D3) |
| `store_server_disk_io_watchdog_trips_total` | counter | 次 | 看门狗判定超期的次数(一次超期 = **一次坏观测**,是否摘盘由迟滞决定),label `disk_id`(同上) |
| `store_server_disk_health_state` | gauge | 0/1 | 该盘当前**上报状态**,label `disk_id` × `state`(`HEALTHY`/**`DEGRADED`**/`UNAVAILABLE`/`UNKNOWN`/`MOUNT_NOT_ACCESSIBLE`/`WRITE_TIMEOUT`/`WRITE_ERROR`/`READ_TIMEOUT`/`READ_ERROR`/`READ_MISMATCH`/`DELETE_ERROR`/`IO_TIMEOUT`)。**整族每次摆平**(当前值 1、其余 0);`UNKNOWN` = 从未观测(默认关探测的部署里每块盘都是它)(同上 D2) |
| `store_server_disk_health_transitions_total` | counter | 次 | 可服务性**迁移**次数,label `disk_id` × `from` × `to`;**只有"可服务性变了"才计**(原始判定变化不算);频繁迁移 = 阈值定得太紧(同上) |
| `store_server_disk_worker_skipped_total` | counter | 次 | 因盘不可服务而被跳过的后台工作,label `disk_id` × `task`(`flush`/`compaction`/`replication`)—— 让"坏盘被跳过"**可见**(同上) |

### 9.2 部署前提:设备级 fail-fast(D7,**不是代码,是上线检查项**)

> **本节与 §9.4 / §9.5 是同一批"上线前必须做"的动作**:本节是设备层前提,§9.4 是数字标定,§9.5 是逐条
> 真机核对清单(含故障注入验收)。

> **为什么单列一节**:上面 D2/D3 的进入判据来自"探测失败"与"IO 超期",而 D1/D3 的最终逃生舱是进程自杀。
> **如果设备层永远 hang**,那么:引擎自关(`io_error_count_to_trigger_shutdown`)**永远等不到错误**
> (被动链路全哑);看门狗**拿不到 errno**,只能超时隔离(而隔离的代价是该盘请求全部 503、线程**仍然泄漏**);
> 整条阶梯就退化成"把盘摘掉但永远补不回来 + 线程继续漏"。**内核不给超时,用户态做什么都是在猜。**

| 层 | 配置 | 要点 |
|---|---|---|
| SCSI | `/sys/class/scsi_device/*/device/timeout` | 设 30s,让 IO 以 **EIO** 返回而非永久 D 状态(默认值随内核/发行版浮动,须显式落盘) |
| NVMe | `nvme_core.io_timeout = 30`、`nvme_core.max_retries = 5` | 同上;重试次数决定"多久才放弃" |
| dm-multipath | `no_path_retry = N` | **绝不用 `queue_if_no_path`** —— 那正是"无界排队 = 永久 hang"的教科书例子,与"池满快失败"的设计精神正好相反 |
| NFS(若把 `data_dir` 放网络盘) | `timeo=` / `retrans=` / 必要时 `soft` | 网络盘的 hang 语义与本地盘不同,必须显式设超时 |
| antfs 引擎 | `[store] io_error_count_to_trigger_shutdown = 3..10` | **建议部署显式打开**(默认 0 = 关,§5.1 不擅改默认)。打开后"设备层 EIO → 连续错误 → 引擎自关 → hook → replica `ERROR` → 心跳上报 → master 再复制"这条**已有**闭环才真正工作 |
| 进程守护 | `systemd Restart=always` / `docker-compose restart: always` | D3 的逃生舱(`disk_watchdog_escalate_secs`,**默认关**)一旦开启,靠它把自杀的进程拉回来 |

代码侧只做一件便宜的事:**启动自检** —— `store_server.disk_health_enabled == false` **或**
`[store] io_error_count_to_trigger_shutdown == 0` 时记一条 `tracing::warn!`,把两条被动输入的当前状态
一起说清楚("此时只有 IO 看门狗能产出 `UNAVAILABLE`");**不改任何默认值**(CLAUDE.md §5.1)。

### 9.3 慢盘:客户端侧的延迟感知读路由(**已落地**)

> **一句话**:读恒为 R=1(第一个成功即返回),故"先试哪个副本"直接决定读延迟 —— 于是让 router
> **先试快的那个**。这是慢盘处置里**收益/代价比最高**的一条,也是唯一不需要改服务端的一条。

- **做什么**:router 记录每个副本最近若干次请求的耗时(指数移动平均,`alpha = 0.2` **写死不做配置**),
  读路径(`get` / `get_file` / `stat`)的副本顺序变为 **`up` 优先 → 观测延迟升序 → master 原序**。
- **为什么敢默认开着**:它**只改试副本的顺序,不改候选集** —— 读的候选、写的 W 多数派语义、
  冲突解决全都不变,**不可能引入新的正确性问题**;而没有测量数据时它退化为 **恒等变换**
  (全都没测 ⇒ 所有键相等 ⇒ 保持 master 顺序,即与关掉时完全一致)。
- **未测副本的先验**:按**已测副本的均值**参与排序 —— 既不奖励也不惩罚刚出现的副本。
- **`up = false` 的副本永远排最后**,哪怕它历史最快(master 已经说它不健康,先撞它只会白等一次超时)。
- **关掉它**(`router.latency_aware_routing = false`)的场景:排查"为什么读打到了某个副本"、
  或对照压测时想要确定性顺序。
- **诚实边界**:顺序变化会改变"你**先**看到哪个副本的状态"。对象不可变 + 删除是墓碑 ⇒ 滞后副本可能
  暂时读不到(→ 404 → 试下一个)或暂时看到已删对象仍活着 —— **这个风险在任意顺序下都存在**,
  本节只改变"先撞上哪一个",不引入新的语义类别。
- **代码**:`crates/common/router/src/lib.rs`(`LatencyTable` / `order_by_latency`),单测 6 条
  (快者优先、无数据恒等、关闭恒等、down 永远最后、未测用均值先验、EWMA 平滑与量级)。

### 9.4 阈值标定手册(**本轮不做:12 个数字仍是经验值,手册留作真机时用**)

> # ⚠️ 本节的默认值全部是**经验初值**,尚未标定
>
> 盘故障处置这条链上一共有 **12 个**数字(见下表),它们都取自
> `openspec/changes/store-disk-isolation/design.md` 的【提案】值 —— 实现时**没有**"顺手定一个",
> 但也**没有**经过压测。CLAUDE.md §5.1 要求"不擅自改默认值",同样要求**别把经验值当成标定过的值**:
> 下面给出每项的**测量步骤与判据**,标定完成后再回写代码 `impl Default` **和**两张配置表。
>
> **防静默变更的闸门已经就位**:每个数字都有单测钉着,改默认值必须显式改测试 ——
> store 侧 `config::tests::{pool_defaults_match_documented_values, disk_health_defaults_match_documented_proposal}`;
> master 侧 `config::tests::disk_fault_defaults_are_pinned_pending_calibration`。
>
> **本节只写方法与判据,不写数字。** 数字必须来自真实机器上的真实负载 —— 从别处抄一组数字填进来,
> 比留着"未标定"更危险。

**共同前提**:所有测量都用**闭包内**口径的 `store_server_disk_io_duration_seconds`(syscall 时长,
**不含**池排队)与 `store_server_disk_pool_*`(池水位)。**绝不要**用端到端的
`store_server_disk_request_duration_seconds` 去定任何隔离阈值 —— 它含排队,会把"池忙"读成"盘慢"
(这是整个设计里最容易走偏的一处,见 [`store.md` §12.12](./store.md) 五)。

| # | 旋钮 | 要测什么 | 判据(怎么改) |
|---|---|---|---|
| 1 | `store.disk_watchdog_ms` | **健康盘的 p99.9**(闭包内):`histogram_quantile(0.999, sum by (le,disk_id) (rate(store_server_disk_io_duration_seconds_bucket[1h])))` | 取**全集群健康盘 p99.9 的最大值 × 5~10**;同时必须 **远大于**一次全库 compaction 的单次大 IO 时长(见 #2)。**偏小 = 把健康盘判死**;偏大 = 坏死盘迟迟不被隔离 |
| 2 | `store.disk_watchdog_ms_by_op` | 跑**一次全库 compaction / flush**,看 `store_server_disk_io_duration_seconds{op=~"compaction\|flush"}` 的**最大值** | 给这几个 op 单开一档,取"实测最大值 × 3~5"。**已实现**(按 op 分档);不配就是全部用 `disk_watchdog_ms`,那时大 IO 的长耗时会误报 —— 大段/慢盘的部署**必须**配 |
| 3 | `store.disk_health_bad_rounds` / `bad_secs` | 隔离延迟目标 vs 探测周期:最坏 ≈ `min(bad_rounds × interval, bad_secs + interval)` | 先定"一块真死的盘多久必须被隔离"(建议 ≤60s),再反解两个数;**两者取先到**,所以两个都要满足 |
| 4 | `store.disk_health_good_rounds` / `recover_stable_secs` | 复位后盘是否再次被压垮:看 `store_server_disk_health_transitions_total{to="HEALTHY"}` 之后 `to="UNAVAILABLE"` 的**频繁往返** | **迁移频繁 = 阈值太松**;目标是"复位后不再回头"。宁可调长 `recover_stable_secs` |
| 4b | `store.disk_degraded_io_ms` / `disk_degraded_ratio` | 健康盘 p99 与 p99.9 的**比值**,以及"慢 IO 占比"的分布:`histogram_quantile(0.99, ...)` 对比 `histogram_quantile(0.999, ...)` | 阈值取 **p99 的 2~3 倍**(远高于绝大多数 IO、又低于真出问题的量级);占比从 0.1 起试。**先只开观测**(把该盘报成 `DEGRADED` 只影响"不再放新副本",几乎零代价)跑一周期,看**误报率**:若健康盘频繁进出 `DEGRADED`,调大阈值或调小占比 |
| 5 | `store.pool_default_size`(每盘可覆盖) | 稳态下 `store_server_disk_pool_queued` 与 `store_server_disk_io_duration_seconds` 的分位 | **持续排队(queued 长期 > 0)= 池不够**;同时看该盘 RT 分位**有没有随池变大而恶化**(恶化说明盘已饱和,加池无用,该加盘) |
| 6 | `store.pool_queue_size` / `pool_acquire_timeout_ms` | `store_server_disk_pool_rejected_total{reason="saturated"}` 的速率 vs 上游 `Retry-After` 后的成功重试率 | 拒绝**偶发**且重试立刻成功 = 排队位/等待时长合适;拒绝**持续** = 要么加池(#5)、要么这块盘真不行 |
| 7 | `store.disk_watchdog_escalate_secs` | 有没有出现"某盘池 100% 被超期 IO 占住且**永不恢复**":`store_server_disk_pool_in_flight == workers` 且 `io_watchdog_trips_total` 持续增长 | **默认 0 先跑一个完整周期**。确认过"真出现过永不恢复"再开,且取值 ≥ 600:单进程多盘,误杀一次 = 自造一次全节点故障 |
| 8 | `master.node_down_debounce_ticks` | `master_node_down_total` 的增长模式,叠加"判死之后实际搬了多少数据" | 目标是**只对真死计一次**。若运维看到"节点抖一下就判死",加拍数;若真死要 1 分钟才发现,减拍数 |
| 9 | `master.rereplication_max_per_tick` | `master_rereplication_deferred`(被压下的量)+ 再复制期间集群的**网络/盘写**水位 | `deferred` 长期 > 0 = 限速在起作用(正常);**若它持续不降**= 预算太小,欠副本永远追不上;若再复制把带宽打满 = 预算太大 |
| 10 | `master.disk_evac_min_bad_secs` | 一次真故障的 `UNAVAILABLE` 持续时长 vs 一次抖动的持续时长 | 门槛要落在两者**之间**:明显大于抖动、明显小于真故障的平均修复时间。**先只开观测**(`disk_evac_enabled = false`)看一周期这两个分布,再决定 |
| 11 | `master.disk_evac_max_in_flight` | 撤离期间集群的写入延迟与 `master_disk_evacuating` | 撤离是真搬 GB 数据:从 1 起,只有当"写入延迟没被抬起来"时才往上加 |
| 12 | `store.disk_health_interval_secs` / `timeout_secs` | 探测是**逐盘串行**的:`盘数 × timeout` 必须 < `interval`,否则轮次追尾(Burst) | 大节点(几十块盘)必须显式调 `interval`,或用 `磁盘指标` 页的 `probe_skipped_total` 看有没有追尾迹象 |

**标定完成后的收口动作(三步,缺一不可)**:
1. 改代码 `impl Default`(store: `crates/store/src/config.rs`;master: `crates/master/src/config.rs`);
2. 改**两张文档表**([`store.md` §12.12](./store.md) 八 与本节所属的 §9 / §7.7),并把"经验初值"的标注去掉、写上标定日期与机器/负载描述;
3. 改**钉住默认值的单测**(见本节开头的清单),否则下一个人会以为默认值还是经验值。

### 9.5 上线前真机核对清单(**本轮不做:按决策移交,清单留作真机时用**)

> ⚠️ **本节条目一条都没在真机上执行过,不声称任何已验证。** 代码侧验收用的是**合成故障注入**
> (`StoreServer::force_disk_unavailable` 喂的是与生产**完全相同**的迟滞状态机),它**造不出真 `D` 状态**、
> 也不经过内核 fail-fast —— 也就是说"盘故障处置这条链在真实硬件上长什么样"目前**只有合成证据**。
> 2026-09-24 的决定是:**这一批真机动作本轮不做**(见 `docs/DEVLOG.md` 步骤 34),清单原地留存 ——
> 将来上真机时逐条做,做完打勾并写明日期 / 机型 / 负载。

#### A. 逐机部署前置(不是代码;不做则 §9.2 那条链是空响)

- [ ] 按 §9.2 逐机落盘设备级超时:SCSI `timeout=30s`、NVMe `nvme_core.io_timeout=30` + `max_retries=5`、
      dm-multipath `no_path_retry=N`(**禁用 `queue_if_no_path`**);`data_dir` 在网络盘时补 NFS `timeo`/`retrans`。
- [ ] `[store] io_error_count_to_trigger_shutdown` 显式打开(建议 **3~10**;默认 0 = 关)。不开的话
      "EIO → 引擎自关 → replica `ERROR` → 心跳上报 → master 再复制"这条闭环根本不启动。
- [ ] 进程守护:`systemd Restart=always`(容器则 `restart: always`)—— 逃生舱 `abort()` 生效的前提。
- [ ] `store_server.data_dirs` 逐机核对:每块盘一条;异构盘写 `pool_size`,其余盘按"留给未指定盘的预算"
      均分(口径见 [`store.md` §12.11](./store.md))。
- [ ] 线程预算:`store_server.pool_default_size`(默认 16)× 盘数 = 每节点线程数,按机型核。
- [ ] `store_server.staging_dir` 决策(流式 put 暂存):不指定 = 各盘 `tmp/`;指定 = **单点**且**不进容量上报**,
      **别指 tmpfs**(大文件暂存会吃内存)。
- [ ] 观测通道可达:`/metrics` 与所有 `/admin/**` **不在健康闸门的拒绝名单里** —— 确认 LB / 防火墙也不挡,
      这是坏盘期间唯一的远程手段。

#### B. 真机故障注入验收

| 注入 | 手段 | 通过判据 |
|---|---|---|
| **单盘 IO hang** | `dm-delay` 拉长单盘全路径延迟 / cgroup `io.latency` 限速 / 物理拔盘 | **本项就是最初那个问题的验收**:同进程内**其他盘的 P50/P99/IOPS 与无故障基线一致**(不是"略降") |
| 单盘 EIO | `dm-error` / 写坏块 | 数据面当场 503 + `store_server_rejected_requests_total{reason="disk_unavailable"}`;`/metrics`、`/admin` 仍可达 |
| 慢盘 | 单盘限速到 p99 的数倍 | `store_server_disk_io_watchdog_trips_total` 增长 → 迟滞判 `UNAVAILABLE`;恢复后 `store_server_disk_health_transitions_total{to="HEALTHY"}` 不来回抖 |
| 池满 | 小池 + 高并发打单盘 | `store_server_disk_pool_queued` 贴顶、`store_server_disk_pool_rejected_total{reason="saturated"}` 与上游重试成功率的关系;503 带 `Retry-After`,客户端能退避 |
| 逃生舱 | 打开 `store_server.disk_watchdog_escalate_secs` | 全池被超期 IO 占死到点后确实 `abort()`,并被守护拉起(**先只观察一个完整周期,取值 ≥ 600**) |
| 坏盘撤离 | `master.disk_evac_enabled = true` | 副本确实从坏盘搬走;看 `master_disk_evacuating` 与**写入延迟**有没有被抬起 |
| 节点抖动 | 掐心跳 / 短暂网络隔离 | 真实抖动分布下 `master_node_down_total` **只对真死计一次** |
| 掉电 / 内核 panic | 硬断电 | 重启后数据面可用(崩溃恢复),不出现"某个分区打不开" |
| 再复制预算 | 一次打掉多盘副本 | `master_rereplication_deferred` 的长期水位可接受(持续不降 = 预算太小) |

#### C. 标定与观测接线

- [ ] 按 §9.4 把 **12 个数字**在真机 + 真实负载上跑出来(含"跑一次全库 compaction 取峰值"那条),再按
      §9.4 末尾的**三步收口**改代码、改文档表、改钉住默认值的单测。
- [ ] 告警规则接线(全名见 §7.7 与 [`store.md` §12.12](./store.md)):
      `store_server_rejected_requests_total{reason="disk_unavailable"}` > 0、
      `store_server_disk_io_watchdog_trips_total` 增速、`store_server_disk_pool_queued` 贴顶、
      `store_server_disk_pool_rejected_total`、`store_server_disk_health_state` /
      `store_server_disk_health_transitions_total`、`store_server_disk_probe_skipped_total`(探测追尾)、
      `master_rereplication_deferred` 持续不降、`master_disk_evacuating` 长期 > 0。
- [ ] admin「主机磁盘」页逐盘数字与 `fio` / `iostat` **对账**(iops、rt、带宽、队列深度、p50/p99);
      注意页面上的 **p99 只给下界**(只有分位点层,不上传 `le` 桶)。
- [ ] 慢盘收益量化:按 §9.3 用 `router.latency_aware_routing = true / false` 做对照压测,把"读路由到底省了多少"
      写回 §9.3(目前只有单测与构造性论证)。

#### D. 上线前对齐的"不做"(别当成 bug 上报)

- 引擎 IO 仍是**同步阻塞** `std::fs` + 页缓存(不迁 AIO / io_uring);**已经进 syscall 的那次 IO 无法取消** ——
  "快速失败"只对排队与后续请求成立。
- 盘恢复后**不自动回填副本**(只置状态,交均衡 / 运维);**不自动换硬件**。
- 单进程多盘下 `abort()` 会**带走同进程的健康盘** —— 所以逃生舱默认关、门槛极高(见 §9.4 第 7 行)。
- `ensure_replica` 建目录与流式 put 暂存的 `unlink` 仍走**进程级**池(D1 只覆盖数据面 IO);
  `DROPPED` 副本的目录不回收 —— 完整清单见 [`store.md` §12.12](./store.md) 的"残留风险"。

#### E. 三处"曾只有结构性论证"的缺口(现状见下 —— 只剩一小块没有端到端断言)

1. ~~盘恢复后不自动回填副本~~ **已钉住**:并入 `bad_disk_evacuation_copies_first_and_removes_only_after_successor_is_active`
   的第 ⑤ 阶段 —— 盘改回健康后跑 4 个调谐周期,断言放置**一动不动**、撤离计划计数不涨;
2. ~~复制游标"段被换掉即失效"~~ **已端到端覆盖**:`crates/store/tests/compaction_replication.rs` 的三个用例
   (`catchup_survives_peer_full_compact_between_rounds` / `..._incremental_compaction` /
   `peer_replica_recreated_resets_session_and_keeps_data`)断言"对端重写 log ⇒ 陈旧游标被**作废**(而不是返回空批、
   更不是报追上)⇒ 重写后的数据必须补回来"。**仍只有结构性论证的那一小块**:坏盘被后台 worker 跳过时
   "不碰 prior token"(跳过点在 `sessions` 之前),见 openspec 8.2;
3. ~~`UNKNOWN` 参与撤离判据~~ **已核对**:`pick_evicted_disks` 只认 `UNAVAILABLE` 且需持续
   ≥ `disk_evac_min_bad_secs`,`UNKNOWN` **不参与**;默认关探测时 `UNAVAILABLE` 只能由看门狗迟滞产出,
   该链路每一环都有用例(store 侧 `repeated_watchdog_trips_take_the_disk_out_via_hysteresis` /
   `disk_beats_report_unknown_and_unavailable_never_a_blank_string`,master 侧撤离全链路用例)。
   **仍待真机**:物理触发(见 B 组)。

## 10. 数据面读写 + quorum(D4 展开)

**W = 副本数/2+1(多数派)/ R=1,靠"不可变 + 拉复制"收敛**(RF=3 时 W=2,对齐 Ambry `routerPutSuccessTarget=2`;
读对齐 `routerGetSuccessTarget=1`)。Router 逻辑在 **router SDK**(`crates/common/router`),**frontend 内嵌同一份实现**,两者都直连 store server。

**PUT(W 多数派)**:
```
client/frontend: GetBlobIds 向 master 要号段 → 得 P + blobId(partitionId<<32|objectId)
  → 并行 HTTP PUT 到 P 的全部 N 副本(store server)
  → 收 W = N/2+1 个 ack 即返回成功(其余请求取消);第 3 副本靠拉复制补
```
- **W 由实际副本数实算,不做配置项**(见 `blob-open-end-to-end` D2):RF=3 → 2、RF=1 → 1、RF=5 → 3;
  副本数取**放置全量** ⇒ 掉 1 个节点仍写得进、掉 2 个写不进(少数派不接受写)。
- 分片(大对象):`POST /stitch` 拼装,描述符当 blob 存(frontend 侧,无状态)。

**GET(R=1,不可变红利)**:
```
client/frontend: 解析 partitionId → LookupPartition(master,可缓存;副本按 up 排序)
  → HTTP GET 第一个副本(§7.2 状态门控);200 即返回,失败/404 → 换下一个
```
> 关键:blob 不可变 → **不需要 R+W>N**。W=2 保证 ≥2 份有,GET 试几个必命中,第 3 份复制补。省掉 quorum 读。

**变更(仅 `DELETE`)—— 唯一最终一致窗口**:并行写墓碑 → **W 多数派** ack;冲突由**墓碑恒胜**收敛(store 已实现,无需定序字段);拉复制传播。⚠ 未追上的副本短暂可能返回旧态(接受,最终一致)。

**拉复制(anti-entropy)收敛**:
```
每 STANDBY 副本 对每 peer:周期发 ReplicaMetadataRequest(token=log offset 游标)
  → peer 返 token 后的新记录元数据 → 算缺失 key → GetRequest 拉 → apply(墓碑恒胜) → 推进 token
```
- 复用 store 已有 `FindToken`(每-peer 游标:**段身份**(位置 + 代次)+ 段内偏移;副本 incarnation 由复制会话另持并随请求带上,见 §9.1 S3a)。**bootstrap 拉全量 = 混合(§9 N3,全部实现)**:封存段物理流式拷贝(快,MTTR 优先)+ 活跃段尾巴 token 逻辑补;非纯 token=0 逻辑回放。
- **S3a 落地(见 §9.1 + `store-server-replication`)**:step-1 ReplicaMetadata 与 step-2 拉 blob **均走 store-server 现有 HTTP(axum)**(细化 D2:node↔node 复制走 HTTP,master↔node 仍 gRPC);写入用 store `apply_replicated`(**墓碑恒胜**)。物理段拷贝(N3 的 MTTR 优化)分期到 S3b。

**参数(默认,已定)**:
| 参数 | 默认 | 说明 |
|---|---|---|
| put 并行度 | = RF | 并行写全部副本 |
| W(put/delete) | **副本数 / 2 + 1** | 多数派:RF=3 → 2(存活 1 副本丢失)、RF=1 → 1。**实算,不做配置项** |
| R(get) | **1** | 不可变,读任一有的副本(副本按 `up` 排序,首个成功即返回) |
| ack 时机 | page-cache(吞吐优先) | 高持久档可选 fsync-ack;W 跨故障域缓解 |
| 读修复 | 不做 | 靠后台拉复制,保持异步 |

**持久性窗口(§3 必须点)**:W=2 ack = 数据进 2 副本 page-cache(未必 fsync);若 flush 前这 2 份同时掉电可能丢 → 靠 W=2 跨故障域 + 周期 fsync 缩窗;高持久档再议 fsync-ack / W=3。与 store.md §3「flush 是持久化边界」一致。

## 11. admin web(Q6 落地)

**已实现**(`crates/admin`,见 `openspec/changes/admin-web/`):独立服务 = master 控制面 gRPC 客户端 + axum HTTP + 内嵌 SPA。对齐 CubeFS `console` / SeaweedFS `weed admin`。

```
浏览器 ──HTTP/JSON──▶ admin(axum) ──gRPC──▶ master
   内嵌 React/Vite SPA(vite singlefile 单文件,`include_str!` 进二进制)
   指标图表 ──外链──▶ Grafana(不自造图表,Q6)
```

- **master 侧新增** `ListPartitions` 只读 gRPC:联 hard-state placement(节点/盘/target_state)与 soft-state `current_state`/`lag`,供 dashboard 展示分区/副本拓扑(proto 带 version)。
- **JSON API**:`GET /api/cluster`(term/leader/节点数/分区数 + grafana_url)、`GET /api/nodes`(节点 + 盘容量/余量/健康 + up)、`GET /api/partitions`(分区 + 副本 target/current)、`POST /api/partitions`(建分区)、`POST /api/nodes/{id}/decommission`(下线)。
- **operator 鉴权**:`/api/*` 校验 `Authorization: Bearer <operator_token>`,缺/错 → 401;空 token = 关校验(仅本地)。写操作对 master 出示 `cluster_secret`。
- **SPA**:概览卡片 + 节点/盘表 + 分区/副本拓扑(BOOTSTRAP/ERROR/发散高亮)+ 建分区/下线表单 + Grafana 外链;token 存 localStorage,401 弹登录。
- **非 goals**:RBAC/多用户/审计持久化、分页(海量分区)、compaction/复制开关/EC 运维(EC 仍锁定)——后续按需。
- **UI 开发**:先起 admin 后端(内置 devcluster 或 `antfs-admin`),再在 `crates/admin/ui` 跑 `npm run dev`,前端经 vite 代理(`server.proxy`: `/api` 与 `/metrics` → `ANTFS_ADMIN_TARGET`,默认 `http://127.0.0.1:7300`)访问后端,免 CORS/免手改地址。

**增强(见 `openspec/changes/admin-enhancements/`)已实现**:
- **分页**:`ListPartitions` 加 `offset`/`limit`/`total`;admin `GET /api/partitions?offset=&limit=` → `{items,total,offset,limit}`;SPA 翻页。
- **RBAC 双角色**:`operator_token`(读+写)/ `viewer_token`(只读);写操作 viewer → **403**;`GET /api/me` 返回角色;SPA 按角色显隐写操作。
- **SetPartitionState 运维**:master `SetPartitionState` gRPC(RW/RO/SEALED,raft 命令 `MasterCommand::SetPartitionState`)+ admin `POST /api/partitions/{id}/state`;SPA 每行 可写/只读/封存 按钮。
- **中英双语**:SPA i18n(en/zh 字典 + 语言切换,localStorage 持久化)。

**指标聚合与副本修复/迁移运维(已实现)**:
- **`GET /metrics`**(RBAC 之外,Prometheus 抓取惯例):admin 自身 Registry 文本 + master `GetMetrics` 文本拼接;master 不可达时仍 200,只留 `# master metrics unavailable: ...` 注释行(并计 `admin_master_rpc_errors_total`)。
- **`GET /api/metrics`**(viewer 可读):聚合 JSON `{ts_ms, master_ok, master_error, master, admin, stores}`——master 段经 `GetMetrics` 文本 parse 取 `master_*` 无 label 指标(master 不可达 → `master_ok:false` + `master_error` 带原因、master 段全 0,仍 200,供 UI 显示"master 不可达(下方为占位 0)"而不是把 0 当现状);admin 段取自身 Registry;stores 段对每个已注册节点**并发**抓 `http://{addr}/metrics`(1s 超时,hyper-util client),按名求和 `store_server_*` 指标,失败 → `scrape_ok:false` + 其余 0 + 计 `admin_store_scrape_errors_total`。
  - **master 段字段**(名字 = 指标名去 `master_` 前缀,取不到填 0):除 term/leader/心跳/迁移等既有项外,还有**分配与读写路由**(`assign_requests_total`/`assign_failures_total`/`lookup_requests_total`)、**分区状态分布**(`partitions_rw`/`partitions_ro`/`partitions_sealed`)、**容量**(`writable_capacity_bytes`/`cluster_free_bytes`/`cluster_used_bytes`)、**健康**(`nodes_up`/`max_replica_lag_bytes`/`majority_replicas_down_partitions`)。其中 gauge 类只在 **leader 的 reconciler** 里刷新,follower 上恒 0,UI 据此只在 leader 视角画这类图。
  - **stores 段**含 `manual_compactions_total`(各 store 手动 compact 实际触发次数);`errors_total` 是带 `kind` label 的 counter,聚合时**按名求和**(io/unavailable/join_error 合并)。
  - **UI**(MetricsTab):KPI 增可写容量 / 分区状态分布 / 多数副本失联分区;图增分配速率(计数差分)、可写容量、集群已用容量增速(有符号差分)、最大副本 lag、在线节点数、多数失联分区(gauge 类仅 leader 视角画,follower 上显示提示行);分区页行内增 Compact 按钮,聚合结果渲染到消息条。
- **`GET /api/partitions/{id}`**:单分区详情(复用列表项结构,含 `moving_out_nodes`;用 `ListPartitions` 联表视图按 id 过滤——`LookupPartition` 只有路由用 ReplicaLoc,缺 RF/healthy/moving 字段);不存在 → 404。
- **`POST /api/partitions/{id}/repair`** / **`POST /api/partitions/{id}/move`**(operator):转发 master `RepairPartition`/`MoveReplica`(带 `cluster_secret`;move body `{from_node_id, to_node_id}`,`to_node_id` 缺省/0 = 自动选目标);master 侧 `ok=false` 也回 200 + `{"ok":false,"error":...}` 供前端展示,仅传输层错误走 502。
- **`POST /api/partitions/{id}/compact`**(operator):手动 compaction 运维(对齐 Ambry `TriggerCompaction` / Ceph `ceph osd` 类操作)。取 `partition_detail(id)` 得该分区**全部副本节点**,对每个节点并发 `POST http://{replica.addr}/admin/partitions/{pid}/compact`(2s 超时,与 `/api/metrics` 同一套 store client,见 `crates/admin/src/store_client.rs`),回 200 + `{"results":[{"node_id","ok","live_bytes","dead_bytes","segments_before","segments_after"}|{"node_id","ok":false,"error"}],"error":""}`(结果按 node_id 排序;单节点 404/超时/网络失败不中断其余节点)。分区不存在 → 404(与 `GET` 同判定);分区无副本 → 200 + 空结果 + 顶层 `error`。注:2s 是短超时,大库 compact 可能先在 admin 侧报超时而 store 侧继续完成(端点幂等,可重试)。

**批 2 增量(A3 异步 compact / A7 复制开关 / A9 多用户与审计,已实现)**:
- **`POST /api/partitions/{id}/compact?mode=async`**(operator):同一端点的**异步形态**——对每个副本节点并发 `POST http://{replica.addr}/admin/partitions/{pid}/compact?async=1`,回 200 + `{"results":[{"node_id","ok","job_id"}|{"node_id","ok":false,"error"}],"error":""}`(按 node_id 排序)。`mode` 缺省/`sync` 走上面的同步路径(响应体不变),其它取值在 admin 边界 **400**。
- **`GET /api/partitions/{id}/compact-jobs`**(viewer 可读):对每个副本节点并发 `GET http://{replica.addr}/admin/compactions` 并**只挑出该 pid** 的任务(每节点最多最近 5 条),回 `{"results":[{"node_id","ok","jobs":[CompactionJob...]}|{"node_id","ok":false,"error"}]}`。任务字段与 store 台账同契约(见 §9.1;未完成字段不出现)。
- **`POST|GET /api/stores/{node_id}/replication-control`**(POST 为 operator):把 `{paused}` 原样转发到该 store 的 `/admin/replication-control`(`node_id → addr` 同 request-control,无此节点 → 404;状态码/body 原样透出,传输层错误 → 502)。语义:暂停**该节点主动拉复制**(别人来拉不受影响),不持久化、store 重启即恢复;store 侧细节见 §9.1。
- **多用户(`admin.users`)+ 审计(`admin.audit_path`)**:`AuthConfig` 改为 `{users, operator_token, viewer_token}`,`resolve(token) → Actor{name, role}` **先按 token 精确匹配 `users`**,再回落旧的两个单令牌(actor 名 `operator`/`viewer`);三者皆空仍为关校验(本地,actor 名 `local`)。RBAC 中间件把 `Actor` 放进 request extensions;审计中间件在**最外层**(故 401/403 也落档),响应返回后按 `ts_ms/actor/role/method/path/status/summary/remote_addr` 追加一行 JSONL——只记**写操作**与 `/api/me`(登录成功与失败),读操作不记;写失败只 warn,绝不影响请求。来源 IP 取 axum `ConnectInfo`(服务端以 `into_make_service_with_connect_info` 启动)。
- **`GET /api/audit?limit=N`**(**operator 专属**,viewer → 403):默认 200、上限 2000,回 `{"entries":[AuditEntry...]}`(**最新在前**);实现只从文件**尾部反向按块读**(64 KiB/块),解析失败的行跳过,不把大文件读进内存。**审计文件 append-only、不做轮转,长期运行请自行 `logrotate`**。
- **UI**:节点页「限流」内联面板里加**「复制」子面板**(状态徽标 + 暂停/恢复按钮 + 说明,结果进面板消息条);分区页「整理」旁加**「后台整理」**(提交后在行下展开逐副本进度行:`节点 · running/done/failed · 回收字节`/失败原因,2s 轮询 `compact-jobs` 到全部终态或用户收起);新增 tab**「审计」**(operator:时间/操作者/角色/方法/路径·状态/摘要/来源 IP + 刷新 + 条数选择);登录页文案注明可用 `[[admin.users]]` 里的用户令牌。

**批 1 增量(维护模式 / 状态过滤 / 请求静默转发,已实现)**:
- **`GET /api/cluster`** 增 `maintenance` / `pause_autogrow`(来自 master `ClusterState`);SPA 顶栏与概览横幅据此显示"维护中"徽标。
- **`POST /api/maintenance`**(operator):body `{paused, pause_autogrow?}`(缺省 false)→ master `SetMaintenance`(raft 命令 `MasterCommand::SetMaintenance`);暂停后 master 不再执行再复制/迁移/下线 evacuation,**观测继续**(心跳/指标/指令下发不受影响),手动 Repair/Move 也不受阻;关闭时两个标志一并清零。打码 follower / 被拒 → 409 + master 错误文本。
- **`GET /api/partitions?state=`**:`state` 空 = 全部,否则 RW/RO/SEALED;取值非法在 admin 边界即 **400**(与 master 同一取值集,不把错误串透传给 master),合法则**透传**给 master `ListPartitions`(**过滤先于分页**,返回的 `total` 是过滤后的总数)。`PartitionDto` 增 `used_bytes`(该分区各副本 `ReplicaCurrent.used_bytes` 的**最大值**——副本互为拷贝,取最大而非求和),`ReplicaDto` 增 `used_bytes`。
- **`POST|GET /api/stores/{node_id}/request-control`**(POST 为 operator):把 `{op, partition_id?, enable, ttl_secs?}` 原样转发到该 store 的 `/admin/request-control`(`node_id → addr` 从 master 节点清单取,无此节点 → 404)。store 的**状态码与 body 原样透出**(未知 op → 400、本地无该副本 → 404),只有超时/连接失败 → 502 + 原因文本。store 侧语义与豁免清单见 §9.1。
- **均衡(计划式)四条路由**:`GET /api/balance`(viewer 可读)= 当前计划 + master 派生的进度(`{present, plan:{plan_id, status, created_at_ms, max_in_flight, total, pending, in_flight, done, failed, moves:[{partition_id, from_node_id, from_disk_id, to_node_id, to_disk_id, status, error}]}}`;`present=false` 时 `plan` 为 null);`POST /api/balance/eval`(operator,body `{max_moves?}`,省略 = master 配置默认)→ `{ok, error, plan}`(评估并落 PREVIEW 计划);`POST /api/balance/start`(operator,body `{plan_id}`)→ `{ok, error}`(确认执行,plan_id 防陈旧确认);`POST /api/balance/cancel`(operator,body `{plan_id}`)→ `{ok, error}`(清计划,**不回滚**已 apply 的搬迁)。master 侧 `ok=false`(如 plan_id 不匹配)仍回 **200 + `{ok:false, error}`**,错误文本交前端展示;viewer 调写接口 → 403。UI 见「均衡」tab(计划状态卡 + 进度条 + 迁移明细表 + 评估/执行/取消)。
- **一致性巡检(Wave3)两条路由**:`POST /api/partitions/{id}/scrub`(operator,query `deep=1` 才做深扫):逐副本并发取**只读素材**——`GET scrub-report`(摘要)+ `GET replica-metadata?max_bytes=16MiB`(最近窗口,按 key 求最终态;窗口被截断 → 该副本 `partial=true`),`deep=1` 时再加 `GET verify`;admin 侧做**逻辑态比对**(摘要逐字段 + key 存在性/生命版本/标志位,见 `crates/admin/src/scrub.rs`)→ 200 `{partition_id, checked_at_ms, deep, replicas:[{node_id, ok, error, report:{...}|null, key_count, partial, verify:{records,bad_records,index_mismatch,truncated}|null}], verdict:{consistent, inconclusive, partial, divergences:[{kind:"summary",field,detail} | {kind:"verify",node_id,detail} | {kind:"key",key_hex,nodes:[{node_id,state}]}]}, error}`。分区不存在 → 404。**素材取不全不致命**:某副本 `replica-metadata` 读不出来(典型:该副本 log 有坏记录)时,该副本不参与 key 级比对并标 `partial`(避免把"未知"报成"缺失"),`deep` 的 `verify` 仍照常执行并报出坏记录位置。`GET /api/scrub`(viewer 可读)= 最近 32 轮巡检的紧凑摘要(内存环形,重启即空)。指标 `admin_scrub_runs_total` / `admin_scrub_divergences_total`。UI:分区页行内「一致性」面板(结论徽标 + 逐副本摘要 + 差异明细 + 深度校验开关)、运维页「最近一致性巡检」列表。
- **`/api/metrics`** master 段增 `maintenance` / `writable_used_bytes` / `writable_free_bytes` / `disk_skew`;stores 段增 `blob_put_small_count`/`blob_put_medium_count`/`blob_put_large_count`/`blob_get_*_count`(取自 `store_server_blob_request_duration_seconds_count{op,size}`,**按标签精确取**——同名下有 6 组 sample,按名求和会把这 6 组混成一锅);stores 段另增 **`disks` 逐盘明细**(盘维度打点见 [store.md §12.11](./store.md)):`[{disk_id, requests_total, bytes_total, slow_requests_total, in_flight, rt_sum_seconds, rt_count, probe_stuck, probe_skipped_total}]` —— 盘列表**从样本里的 `disk_id` label 枚举**(不假设盘号/盘数,按 `disk_id` 升序),数值**跨 `op` 求和**,`probe_stuck` 取"任一为 1 则 1",`rt_sum_seconds`/`rt_count` 是直方图 `_sum`/`_count`(平均 RT 由 UI 算,**不做分位点**:`parse.rs` 刻意极简、不聚合 `le` 桶);抓取失败 → `disks: []`,失败与成功两条分支共用同一处构造(空样本入参),键集不会漂移。
- **UI**:概览页改分层仪表盘(健康横幅 + 4 张大卡 + 「容量趋势 / 集群概况」两列 + 「磁盘水位榜 / 可写分区速览」两列);新增 tab**「容量」**(viewer 可见:集群水位、可写集合四卡、增速读数与增速折线、可写分区列表含前端窗口差分的 1 分钟增量);运维页**「维护模式」**面板(开启需二次确认、可勾选连带暂停自动扩容);节点页行内**「限流」**面板(op × 有效期 + 当前禁用集);指标页增**按 blob 大小分桶的 put/get 速率**两张图。跨 tab 筛选走 hash query(`#partitions?state=RW`、`#nodes?q=101`),刷新/分享链接后仍生效。
- **UI(磁盘指标页)**:新增 tab **「磁盘指标」**(viewer 可见,hash `#hostdisks`)——按 **store 节点(=主机)** 分组,每主机一段:主机级汇总(Σ IOPS / Σ 带宽 / 平均 RT = Σrt_sum/Σrt_count / Σ 超时速率 / Σ 在飞 / 容量水位 + 最差盘健康徽标)+ **逐盘表**(盘 / 健康 / 容量 / IOPS / RT(平均 ms) / 带宽 / 超时速率 / 在飞 / 探测卡住)。数据 = `/api/metrics` 的单例窗口(5s)∪ `/api/nodes`,按 `(node_id, disk_id)` **两侧并集**(只有容量或只有指标的盘都留行,缺的一侧显示「—」,不静默丢行);IOPS/带宽/超时速率走 `trend.ts` 的窗口差分(抓取失败按**断线**处理,不把"抓不到"差分成假速率),RT/在飞取瞬时值。**「在飞」列刻意标注为排队深度代偿**:真队列深度需要 `RUSTFLAGS="--cfg tokio_unstable"`,刻意不加,随 `openspec/changes/store-disk-isolation/` 的每盘有界池落地;页首写明口径是**请求级**、引擎内部 IO(flush / compaction / 入向复制)尚未归盘。
> **⚠️ 已更新(2026-09-24)**:本页现在展示**真队列深度**(`disk_pool_queued / disk_pool_workers`)、
> 池在飞与池拒绝、**盘健康状态**(`disk_health_state`)、看门狗超期次数、后台跳过数,以及**两把 RT 尺子**
> (端到端 / syscall 闭包内)并排。原先那句"'在飞'列刻意标注为排队深度代偿"已不适用 —— 代偿换成了真值。
> **延迟分位点也已补上**:两列 RT 现在都显示 **平均 / p99**(另有 p50,放在单元格 hover 提示里,不占列宽),
> 由该盘 `le` 桶**逐盘跨 op 聚合**后按 Prometheus `histogram_quantile` 口径**线性插值**;
> 最高桶是 `+Inf`,秩落在里面时**只能给下界**(= 最高有限桶,读作"≥ X"),**不声称超出最高桶的精度**;
> 没有桶样本显示「—」而不是 `0.00 ms`。见 [store.md §12.12 残留风险 7](./store.md)。
- **UI(指标页采集与展现)**:`/api/metrics` 的轮询收敛到**进程内单例**(`crates/admin/ui/src/metricsStore.ts`)——概览/容量/指标三个 tab 共享同一份滚动窗口(此前各自轮询、切 tab 要从零攒样本),首个订阅者启动、无人订阅时停;窗口在 `sessionStorage` 留种子(只接受 5 分钟内的样本),刷新页面**瞬时出图**;挂载后 1s 补采一次,速率类曲线秒级有斜率。指标页新增采集状态条(最后更新时刻 / 样本数 / 各 store 抓取状态徽标 / 手动刷新)、图表按「请求与延迟 / 存储引擎 / 集群容量与健康」分组;`/api/metrics` 的 `master_ok:false` 时显示"master 不可达"横幅、master 类 KPI 显示 "—"、master 类曲线按**断线**(null)而非占位 0 绘制;样本超过 3× 采样间隔未更新时打 "stale" 标记(种子里的旧值不当作"当前值"喂 KPI)。uPlot 侧:`scales.x.range` 给最小 1 分钟窗口(首屏 1~2 个样本时不再切到难读的亚秒刻度、单点也不会撑出无意义跨度),y 轴 `size: 96`(自定义 `values` 后 uPlot 不再自动预留标签宽度,否则长标签被裁)。

### 11.1 配置项清单(`AdminConfig`,TOML `[admin]` 表下;加载用 `Loader::load_table("admin")`,**同时兼容顶层 flat key**)

> 两种写法二选一,别混用:表存在时 flat key 会被忽略。环境变量同理——表写法对应 `AMBRY_ADMIN__MASTER_ADDR`(嵌套),flat 写法对应 `AMBRY_MASTER_ADDR`。

| key | 类型 | 默认值 | 取值/校验 | 含义 |
|---|---|---|---|---|
| `admin.master_addr` | String | `127.0.0.1:7101` | 非空 | master 控制面 gRPC 地址 |
| `admin.listen_addr` | String | `127.0.0.1:7300` | 非空 | admin HTTP 监听地址 |
| `admin.operator_token` | String | `""` | — | operator 准入令牌(Bearer,读+写);空 = 关校验(仅本地) |
| `admin.viewer_token` | String | `""` | — | viewer 准入令牌(Bearer,只读);写操作返回 403;空 = 不启用只读角色 |
| `admin.users` | Array\<Table\> | `[]` | 每项 `role` ∈ `operator`/`viewer`、`token` 非空 | 多用户令牌表(`[[admin.users]]`: `name` + `token` + `role`);**优先匹配**,命中后 actor 名 = `name`;为空时回落上面两个单令牌 |
| `admin.audit_path` | String | `./admin-audit.jsonl` | — | 审计日志(JSONL,append-only)路径;启动时 create + append 打开,打开/写入失败只 warn 并降级(请求不受影响) |
| `admin.cluster_secret` | String | `""` | — | 集群准入密钥(与 master 一致),用于对 master 发写操作 |
| `admin.grafana_url` | String | `""` | — | Grafana 外链;空 = 不显示按钮 |

### 11.2 监控项清单(`AdminMetrics`)

| 指标名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `admin_api_requests_total` | counter | 次 | admin API 请求数 |
| `admin_ops_total` | counter | 次 | 运维操作数(建分区/下线/置态/修复/迁移) |
| `admin_master_rpc_errors_total` | counter | 次 | 调 master gRPC 失败数 |
| `admin_store_scrape_errors_total` | counter | 次 | 抓取 store `/metrics` 端点失败数(连接/超时/非 200/非法文本) |

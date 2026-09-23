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
                  replicas: [ { node_id, disk_id, target_state } ]  // ← ideal placement(应然)
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
- **盘将满 → 其上活跃 partition 转 RO、不再放新 partition**(对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage≈95%`);写流量由 `AssignPartition` 只返回 RW 分区自动导流。
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
  // count=0 → 用 master.id_alloc_segment_size;partition_id=0 → 由 master 按剩余容量加权挑(RW,D10)。
  rpc GetBlobIds(GetBlobIdsRequest) returns (GetBlobIdsResponse);

  // 读路由:某 partition 的存活副本位置 + 状态(据此按 §7.2 门控选副本)。
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
}
```
- master↔master 的 Raft 内部 RPC 走 openraft 自己的网络层(可另起一个 gRPC `RaftInternal` 服务),与上面控制面服务分开。
- proto **带版本字段**(D2 要求)。
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
④ 写:           client/frontend GetBlobIds(要 RW 分区 + objectId 号段)→ 向其副本并行 HTTP 写,收 W=副本数/2+1 个 ack 即返回(§10)
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
| `master.heartbeat_timeout_secs` | u64 | 15 | ≥1 | 超过则软状态标节点 down |
| `master.partition_capacity_bytes` | u64 | 100 GiB | ≥1 | 分区容量上界;选盘时盘可用容量须 ≥ 它(M2,§7.3) |
| `master.disk_near_full_ratio` | f64 | 0.95 | (0,1] | 盘用量超此比例:① 不再放新分区/新副本(选盘跳过);② 其上 RW 分区被 reconciler 自动转 RO(**只置不回**,恢复靠运维)(对齐 Ambry ≈95%) |
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
- [ ] **store 需补 `drop`**:关闭 + 删 partition-replica 目录、回收磁盘。

### 9.1 实现状态 + 配置/监控清单(CLAUDE.md §7 必须项)

**S1(数据节点骨架 + 数据面)已实现**(`crates/store`,见 `openspec/changes/store-server/`):`DiskManager`(`statfs` 探真实容量,Q1)、`ReplicaManager`(按 partition 管 `BlobStore`,幂等)、`DataHttpServer`(axum `PUT`/`GET`,同步引擎经 `spawn_blocking` offload)。过集成测试(put/get 往返 + 409/404/400)+ 单测(statfs、ensure_replica 幂等)。**不接 master / 不复制 / 不含 EC(暂不解锁)**。

**S2(接入集群 + 磁盘监控)已实现**(见 `openspec/changes/store-server-heartbeat/`):`RegisterNode` 启动注册 + 双向流心跳(上报盘 `statfs` 余量 + 各 replica `current_state` + 盘 `health`)+ ReplicaLifecycle(消费 `MasterDirective.transitions`:ACTIVE/BOOTSTRAP→建 replica,OFFLINE/DROPPED→关);**磁盘监控照搬 Ambry 两级**——被动(`store` 引擎连续 IO-error 自关 + hook → replica `current_state=ERROR` → 心跳上报 → **master reconcile 再复制**;master `plan_rereplication` 已纳入 ERROR 副本)、主动(`DiskHealthCheck` 周期 write/read/verify/delete + 超时,默认关,随心跳上报)。过集成测试(注册→放置指令→建 replica→HTTP put/get)+ 单测(errored 再复制、健康探测、lifecycle)。

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
  - **传输(细化 D2,见 design.md D-B)**:node↔node 复制元数据走 store-server 现有 **HTTP(axum)**(非新起 gRPC server);master↔node 控制面仍 gRPC。

**S3b(封存段物理拷贝,N3 混合完成)已实现**(见 `openspec/changes/store-bootstrap-copy/`):`blob` 引擎加封存段只读拷贝原语(`sealed_segments`/`read_segment_prefix`/`LogSegment::raw_prefix`)+ `open_rebuild_index_with`;数据节点加发送端点(`GET .../segments`、`GET .../segments/{position}`)+ 接收装载 `ReplicaManager::install_bootstrap`(段文件 `tmp→fsync→rename` 崩溃安全 + 重建打开);复制器对**空**副本首次先物理拷贝封存段(不可变),续拉游标 = 最高封存段末尾,再逻辑补活跃尾;无 peer/无封存段回落纯逻辑。过单测(封存段枚举/拷贝重建)+ 集成测试(多封存段 + 活跃尾混合 bootstrap,真 HTTP 收敛)。
 - 待续:disk-aware 每盘并发限流(N4);放置据 `DiskHealth` 避让;`DROPPED` 删目录回收。**EC 仍锁定(暂不解锁)**。
 - **已补齐(见 `blob-io-end-to-end`)**:router SDK(router 库 + CLI)、frontend(REST 网关)、master 发号 `GetBlobIds`、
   流式 put(网络流式 + 落盘暂存 + 引擎 `put_from_reader`)、`.../metadata` 端点、`StoreFull → 507`、`PUT ?expiration_ms=`。

**S4(本地维护 worker)已实现**(见 `openspec/changes/store-local-worker/`):数据节点常驻后台 `worker.rs`,对本节点每个本地 replica 分**两条**独立周期 loop 驱动 `blob` 引擎的 **flush**(推进持久化边界,缩小 W=2 page-cache 掉电窗口)、**compaction**(`compact_if_needed(compaction_dead_ratio)`,按封存段死数据占比阈值增量压缩);各周期时长独立可配置;同步引擎调用经 `spawn_blocking` offload。复用引擎既有崩溃安全实现(不改写路径语义),单店失败只记 warn 不拖垮全局。过单测(flush 后重开可读 / compaction 回收死数据)。

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
| `store_server.compaction_interval_secs` | u64 | 3600 | ≥1 | 本地 worker compaction 检查周期(S4) |
| `store_server.compaction_dead_ratio` | f64 | 0.5 | (0,1] | 封存段死数据占比 ≥ 此值才压缩(S4) |
| `store_server.max_body_bytes` | u64 | 2097152(2 MiB) | ≥1 | 数据面单请求 body 上限(挂在数据面路由的 `DefaultBodyLimit`;**流式 PUT 另按 `Content-Length` 前置校验,超限 413 且不读 body**)。默认与 axum 的 `DefaultBodyLimit` 默认值一致(§5.1 不擅改默认),要收真正的大 blob 须显式调大 |
| `store_server.request_control_default_ttl_secs` | u64 | 1800 | ≥0 | `/admin/request-control` 请求未传 `ttl_secs` 时的默认静默时长(秒);**0 = 不设默认时长(即留空即永久禁用)**(批 1) |
| `store_server.scrub_max_batch_bytes` | u64 | 67108864 | ≥1 | `GET /partitions/{pid}/verify` 缺省的单次扫描读取字节上限(深扫读全量数据体,限流防打爆盘;到点响应 `truncated=true`)(Wave3) |

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
| `store_server_bootstrap_segments_total` | counter | 个 | bootstrap 物理拷贝的封存段数(S3b) |
| `store_server_bootstrap_bytes_total` | counter | 字节 | bootstrap 物理拷贝字节数(S3b) |
| `store_server_flushes_total` | counter | 次 | 本地 worker flush 次数(S4) |
| `store_server_compactions_total` | counter | 次 | 本地 worker 发生压缩的 replica 次数(S4) |
| `store_server_blob_request_duration_seconds` | histogram | 秒 | **成功** blob 请求延迟,label `op`(put/get)× `size`(small/medium/large,分桶边界 50 KiB / 1 MiB);错误/早退路径不 observe(批 1) |
| `store_server_rejected_requests_total` | counter | 次 | 被 RequestControl 静默拒掉的请求数(在进引擎之前),label `op` × `reason`(目前仅 `disabled`)(批 1) |
| `store_server_replication_paused` | gauge | — | 拉复制是否被运维暂停(`/admin/replication-control`;1 = 暂停本节点主动拉,0 = 正常)(A7) |
| `store_server_scrub_reports_total` | counter | 次 | `GET /partitions/{pid}/scrub-report` 调用次数(Wave3) |
| `store_server_verify_runs_total` | counter | 次 | `GET /partitions/{pid}/verify` 调用次数(Wave3) |
| `store_server_verify_bad_records_total` | counter | 条 | 本地校验发现的坏记录总数(Wave3) |
| `store_server_verify_duration_seconds` | histogram | 秒 | 单次本地校验耗时(Wave3) |
| `store_server_compaction_jobs_total` | counter | 次 | 后台 compact 任务按状态计数,label `state`(`running` / `done` / `failed`;提交即计 `running`,终态再计一次)(A3) |
| `store_server_replicas_dropped_total` | counter | 个 | 按 master `DROPPED` 指令删掉的本地副本数(A3 同期) |

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
- 复用 store 已有 `FindToken`(每-peer log offset 游标)。**bootstrap 拉全量 = 混合(§9 N3,全部实现)**:封存段物理流式拷贝(快,MTTR 优先)+ 活跃段尾巴 token 逻辑补;非纯 token=0 逻辑回放。
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
- **`/api/metrics`** master 段增 `maintenance` / `writable_used_bytes` / `writable_free_bytes` / `disk_skew`;stores 段增 `blob_put_small_count`/`blob_put_medium_count`/`blob_put_large_count`/`blob_get_*_count`(取自 `store_server_blob_request_duration_seconds_count{op,size}`,**按标签精确取**——同名下有 6 组 sample,按名求和会把这 6 组混成一锅)。
- **UI**:概览页改分层仪表盘(健康横幅 + 4 张大卡 + 「容量趋势 / 集群概况」两列 + 「磁盘水位榜 / 可写分区速览」两列);新增 tab**「容量」**(viewer 可见:集群水位、可写集合四卡、增速读数与增速折线、可写分区列表含前端窗口差分的 1 分钟增量);运维页**「维护模式」**面板(开启需二次确认、可勾选连带暂停自动扩容);节点页行内**「限流」**面板(op × 有效期 + 当前禁用集);指标页增**按 blob 大小分桶的 put/get 速率**两张图。跨 tab 筛选走 hash query(`#partitions?state=RW`、`#nodes?q=101`),刷新/分享链接后仍生效。
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

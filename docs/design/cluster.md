# cluster 设计:集群/服务层角色与传输选型

本文固化 **store 之上的集群/服务层**的角色划分与关键选型。**状态:选型已定,实现规划中**
(当前仓库只落地了单机 [`store`](./store.md) 引擎)。选型依据来自对 Ambry / SeaweedFS / CubeFS 三套系统的源码调研。

## 1. 角色划分(目标形态)

```
  app + client SDK ──gRPC(lookup/assign)──▶ master     (Router 逻辑内嵌 SDK,无独立 frontend)
       │  ──HTTP 数据面(put W=2 / get R=1)──▶ store server  ← 直连,无中间跳
       ▼
  [可选] gateway(S3/REST 翻译 + auth;内嵌同一 client SDK;仅外部/多租户需要)

  master ◀─心跳/放置(gRPC)─▶ store server ◀─拉复制(gRPC ReplicaMetadata + HTTP 拉 blob)─▶ 其它 store server
   集群大脑:partition→replica 放置、节点状态机、故障处置(取代 Ambry 的 Helix+ZK)
```

| 角色 | 职责 | 状态 |
|---|---|---|
| **store server**(数据节点) | 承载 [`store`](./store.md) 引擎(挂 N disk、每 disk 一堆 BlobStore),服务 put/get,跑副本复制 | 引擎已成型,服务层规划(见 §9) |
| **master** | partition→replica 放置、节点/副本状态机、故障处置;**内建 Raft**(自包含,不依赖外部协调器) | 规划(见 §7) |
| **client SDK**(取代 frontend) | Router 逻辑做成**库**、链接进客户端 app:缓存拓扑/可写分区、拼 blobId(objectId 用 master 号段发的号)、直连 store server 做 W/R、重试切副本。**无独立 frontend 集群** | 规划 |
| **gateway**(可选) | S3/REST 翻译 + auth/quota;内嵌同一 client SDK;**仅外部/多租户需要** | 可选 |
| **scheduler**(待定 P1) | 集群级 compaction 编排 / 再复制 / 均衡 / EC 调度 | 待定(见 §4) |
| **admin web**(独立服务) | 运维操作(经 master admin gRPC)+ 控制面状态 dashboard(拓扑/节点/盘/分区/副本);指标时序复用 Prometheus+Grafana;需 operator 鉴权 | 规划 |
| **EC 冷层**(D5) | 老数据纠删码降本 | 已锁(见 §5) |

> **去掉 frontend 的连带**:① 鉴权归属——MVP 内部可信 SDK 先不做细粒度,需要时由可选 gateway / store server 粗校验承担(对齐 §8 B 不编 account/container、多租户推后);② S3/外部 → 可选 gateway(复用 SDK),非必需角色。

> **两种接入模式(并存,复用同一套 SDK / blobId / 数据面)**:
> - **① 直连模式(SeaweedFS blob 式)**:app 内嵌 client SDK,直接 gRPC 问 master(lookup/assign)+ HTTP 直连 store server 读写,**无中间跳**,低延迟高吞吐;适合**内部可信服务**。blobId 即取件凭证(对齐 SeaweedFS fid 直连 volume 的裸用姿势)。
> - **② 网关模式(REST / S3)**:app 走 **gateway**(内嵌同一 SDK)做 S3/REST 协议翻译 + auth/quota;适合**外部 / 多租户 / 标准 S3 客户端**。
> - 关系:gateway 只是在**同一 SDK 之上**叠"协议翻译 + 鉴权"层,底层 blobId、发号、W/R 数据面完全一致;两模式非互斥,可同集群并存。

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
**SOFT(内存,心跳重建;新 leader 上任后一个心跳周期重建)**:
```
Liveness       map<NodeId, { last_beat, up }>
DiskUsage      map<(NodeId,DiskId), { free_bytes, load }>
ReplicaCurrent map<(PartitionId,NodeId), { current_state, lag_bytes, sealed }>  // ← 实然
RoutingIndex   map<PartitionId, [ 存活副本位置 ]>                                 // 供 LookupPartition
```

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
| BOOTSTRAP | OFFLINE→STANDBY 中间态,追平 peers | GET / DELETE / TTL_UPDATE / UNDELETE(**不含 PUT**) |
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
```
master **只决策、只下发任务**,不亲自搬数据(D1);执行在 server/scheduler(P1)。

**容量感知放置(重要,解决"固定容量容器 vs 磁盘余量"矛盾)**:
- partition 是**段级懒增长 + 定容上界**(不预留整块;对齐 store.md"定容上界 + 写满滚动新 partition"),空间不浪费——长到上界或盘满即封存(RO)。
- **副本尺寸受最小盘拖累**:一个 partition 的 N 副本装同一批对象,有效容量 = 各副本盘余量的**最小值**;若余量悬殊,大盘空间被小盘拖累浪费。
- 故放置(`CreatePartitions` / 失联补副本)选盘约束:**从心跳 `DiskUsage.free_bytes`(§7.1 软状态)里,选余量 ≥ partition 上界、且彼此相近的 N 块盘**,并按故障域(dc/rack)打散。
- **盘将满 → 其上活跃 partition 转 RO、不再放新 partition**(对齐 Ambry `storeReadOnlyEnableSizeThresholdPercentage≈95%`);写流量由 `AssignPartition` 只返回 RW 分区自动导流。
- 尾部搁浅余量(< 1 个 partition 上界,100GB/16TB ≈ 0.6%)可忽略;**不做变尺寸尾 partition**(不值当)。

### 7.4 gRPC 接口草案(控制面,D2;仅接口示意,非实现)

```proto
service Master {
  // 数据服务器 ↔ master:双向流心跳(承 SeaweedFS SendHeartbeat)。
  // 上行:活着 + 我持有的副本及其 current_state/lag + 各盘余量;下行:状态转移指令 + 任务。
  rpc Heartbeat(stream NodeBeat) returns (stream MasterDirective);

  // 前台读路由:某 partition 的存活副本位置 + 状态(据此按 §7.2 门控选副本)。
  rpc LookupPartition(LookupRequest) returns (PartitionLocations);
  // 前台取一个可写(RW)分区放新 blob。
  rpc AssignPartition(AssignRequest) returns (AssignResponse);

  // 管理面。
  rpc CreatePartitions(CreateRequest) returns (CreateResponse);
  rpc ListNodes(Empty) returns (NodeList);
  rpc DecommissionNode(NodeRef) returns (Ack);
  rpc GetClusterState(Empty) returns (ClusterState);
}

message NodeBeat {
  string node_id = 1; string dc = 2; string rack = 3; string addr = 4;
  repeated DiskBeat disks = 5;      // disk_id, free_bytes, load
  repeated ReplicaBeat replicas = 6; // partition_id, disk_id, current_state, lag_bytes, sealed
  bool full_report = 7;             // true=全量(注册/重连), false=增量(承 SeaweedFS Incremental)
}
message MasterDirective {
  repeated Transition transitions = 1; // partition_id, disk_id, target_state
  repeated Task tasks = 2;             // 再复制 / 再平衡 / EC 转换(from,to_hint)
}
```
- master↔master 的 Raft 内部 RPC 走 openraft 自己的网络层(可另起一个 gRPC `RaftInternal` 服务),与上面控制面服务分开。
- proto **带版本字段**(D2 要求)。

### 7.5 关键流程时序

```
① 自注册+心跳:  SN 首心跳(full_report)→ master 固化 Nodes(raft)→ 重建 Liveness/RoutingIndex(soft)
② 建分区:       admin CreatePartitions → 分配 id(raft 单调)→ 故障域选盘置 N 副本(写 raft placement,target=BOOTSTRAP)
                 → 心跳下发 transition → 副本 BOOTSTRAP 追平 → STANDBY
③ 读:           frontend LookupPartition → 按副本 current_state 门控(§7.2)选一个 → 直连该 SN 走 HTTP 读
④ 写:           frontend AssignPartition(要 RW 分区)→ 向其 STANDBY/LEADER 副本并行 HTTP 写达 quorum(D4)
⑤ 节点失联:     心跳超时 → 相关副本标 ERROR → reconciler 选新盘补副本(target=BOOTSTRAP)→ 产再复制任务 → scheduler 执行
```

### 7.6 master 实现 TODO + 待拍决策(探索产出,落地前定案)

> 状态:**探索中**,未锁进 §6。落地走 OpenSpec change(`master-node`)时逐条定案。

**分期 TODO(每期可独立验收)**:
- [x] **M1 骨架**:openraft 起 3 节点 + 选主;节点自注册(心跳→raft 成员表);单调 partition_id;软状态(存活/容量)内存;gRPC `Heartbeat(stream)`/`RegisterNode`/`ListNodes`。**不做放置**。**(已实现:`crates/master`,redb 后端过 openraft `Suite::test_all` + 崩溃恢复属性测试 + 3 节点集成测试;配置/监控见 §7.7)**
- [x] **M2 放置**:`CreatePartitions`(分配 id + 故障域 rack-aware 选盘 + 写 raft placement)+ **容量感知选盘(余量 ≥ partition 上界且相近的 N 块盘;盘将满转 RO、不再放新,见 §7.3)** + `LookupPartition` + 路由索引心跳重建 + `AssignPartition`(取可写分区)。**(已实现:`crates/master`——M2a 选盘纯函数 + `CreatePartition` 确定性 apply;M2b `CreatePartitions`/`LookupPartition`/`AssignPartition` gRPC(路由由 hard-state placement + 软状态 liveness 直接联表,未单设 RoutingIndex 缓存);M2c Q5 自动扩容 loop + 防抖/限速/上限护栏;过属性测试 + 3 节点集成测试。见 `openspec/changes/master-placement/`)**
- [x] **M3 调谐**:副本状态机(BOOTSTRAP→STANDBY/LEADER,§7.2)+ 心跳失联检测 → 产再复制任务。**(已实现:`crates/master`——`reconcile.rs` 纯决策 `plan_transitions`/`plan_rereplication`;心跳反应式下发转移(current≠target)、leader-only 后台 loop 失联检测 → `AddReplica` 再复制补副本;过属性/单测 + 集成测试(转移下发 + 失联再复制)。见 `openspec/changes/master-reconcile/`。转移/拉复制的**执行**待 store server。)**
- [ ] **M4 再平衡/EC 钩子**:容量均衡、decommission、对接 D5 EC 转换决策。**(decommission 已实现:`crates/master`——`decommissioning` hard state + `DecommissionNode`/`RemoveReplica` 命令 + `plan_evacuation` 先补后删 evacuation(守 §3.1),过单测 + 集成测试(103 下线→迁到 104→移除 103)。见 `openspec/changes/master-decommission/`。容量再平衡 / D5 EC 转换钩子**推迟**:EC 依赖 store server + 封存段语义(§5)。)**

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

**配置项清单**(`MasterConfig`,TOML `[master]` 表下):

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
| `master.disk_near_full_ratio` | f64 | 0.95 | (0,1] | 盘用量超此比例不放新分区(M2,对齐 Ambry ≈95%) |
| `master.auto_grow_enabled` | bool | false | — | Q5 自动扩容开关(默认关,安全) |
| `master.auto_grow_min_writable` | u64 | 4 | — | 低水位:RW 分区数低于它就补建 |
| `master.auto_grow_max_partitions` | u64 | 100000 | — | 分区总量上限(护栏) |
| `master.auto_grow_interval_secs` | u64 | 30 | ≥1 | 扩容检查周期,兼作限速节拍 |
| `master.auto_grow_batch` | u64 | 4 | ≥1 | 单周期建分区上限(限速护栏) |
| `master.reconcile_interval_secs` | u64 | 5 | ≥1 | 调谐/失联检测周期(M3) |

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

## 8. blobId 结构(D3 的落地)

blobId = store 的 `StoreKey`,**定长 64bit(8B)**:高 32bit `partitionId` + 低 32bit `objectId`。store 只当它是不透明 key,**解析字段是 client SDK / master 层的事**。

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
| LocalWorker | 本地常驻:周期 flush + 单店 compaction(死占比阈值) + hard_delete |

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
- **本地 worker(store server 常驻,永远要有)**:flush / 单店 compaction / hard_delete —— 都是"单 BlobStore 内、本地、轻"(对齐 Ambry CompactionManager 跑在 StorageManager 里)。
- **P1 独立 scheduler(集群级、跨节点、重)**:再复制编排 / 跨节点再平衡 / EC 转换调度(master 决策 + 跨节点搬数据)。
- → compaction/flush 是本地事,**不等 P1**。

**store 连接点**:
- 生命周期:`create/open/close`(已成)+ **需补 `drop`(删目录回收)**。
- 容量:store 的 `available_capacity`/`is_full` → 心跳上报 master → 喂 §7.3 容量感知放置(闭环)。
- 读写/后台:`put/get`/`flush`/`compact_if_needed`/`hard_delete`(已成)直接被编排,**store 基本不改**。

**待拍/开放**:
- [x] **N1 命名**:`store server`(数据节点)。
- [x] **N2 拉复制归属**:**内嵌 store server**(对齐 Ambry ReplicationManager,同进程贴本地副本;用 N4 每盘池限流)。
- [x] **N3 bootstrap 拉全量方式**:**混合,全部实现(不推后,MTTR 优先)**——封存段(不可变)走**物理流式拷贝**(快;对齐 Ambry `FileCopyBasedReplication` / SeaweedFS 卷拷;disk-aware 每盘限并发,呼应 N4);活跃段尾巴走**逻辑 catch-up**(token,复用拉复制)补拷贝期间的新写。协议需含 FileCopy 类消息(§7.4)。
- [x] **N4 本地并发模型**:**每盘一个小线程池**(池大小按盘 IO 能力定;同盘上的 BlobStore 共享;贴合 §9 DiskManager + Ambry DiskIOScheduler)。异步网络层(axum/tonic)通过该池 offload 阻塞的 store 调用,不卡 tokio reactor;并发被限制在硬件可承受范围。衔接 store D4 P4。
- [ ] **store 需补 `drop`**:关闭 + 删 partition-replica 目录、回收磁盘。

### 9.1 实现状态 + 配置/监控清单(CLAUDE.md §7 必须项)

**S1(数据节点骨架 + 数据面)已实现**(`crates/store`,见 `openspec/changes/store-server/`):`DiskManager`(`statfs` 探真实容量,Q1)、`ReplicaManager`(按 partition 管 `BlobStore`,幂等)、`DataHttpServer`(axum `PUT`/`GET`,同步引擎经 `spawn_blocking` offload)。过集成测试(put/get 往返 + 409/404/400)+ 单测(statfs、ensure_replica 幂等)。**不接 master / 不复制 / 不含 EC(暂不解锁)**。

**S2(接入集群 + 磁盘监控)已实现**(见 `openspec/changes/store-server-heartbeat/`):`RegisterNode` 启动注册 + 双向流心跳(上报盘 `statfs` 余量 + 各 replica `current_state` + 盘 `health`)+ ReplicaLifecycle(消费 `MasterDirective.transitions`:ACTIVE/BOOTSTRAP→建 replica,OFFLINE/DROPPED→关);**磁盘监控照搬 Ambry 两级**——被动(`store` 引擎连续 IO-error 自关 + hook → replica `current_state=ERROR` → 心跳上报 → **master reconcile 再复制**;master `plan_rereplication` 已纳入 ERROR 副本)、主动(`DiskHealthCheck` 周期 write/read/verify/delete + 超时,默认关,随心跳上报)。过集成测试(注册→放置指令→建 replica→HTTP put/get)+ 单测(errored 再复制、健康探测、lifecycle)。

- 数据面端点(`crates/store`):`PUT`/`GET`(S1)+ `DELETE` / `POST .../undelete` / `POST .../ttl`(变更类,见 `openspec/changes/store-server-mutations/`,承 store 引擎语义 + 状态码映射)+ `GET .../replica-metadata` 与 blob 读 `?replication=1`(拉复制,见下 S3a)。

**S3a(拉复制逻辑核心)已实现**(见 `openspec/changes/store-server-replication/`):store 引擎新增**忠实 apply 原语** `BlobStore::apply_replicated`(newest-wins + lifeVersion,**原样保留** peer 的 flags/lifeVersion/expiration/op_time,只增不删幂等,守 §3.1;防副本发散);store-server 新增 `replica-metadata` HTTP 端点(step-1,`find_entries_since`)+ blob 读 `?replication=1`(step-2,含已删除/过期数据体)+ `replication.rs`(`PeerSource` trait + `replicate_from_peer` 收敛循环 + `HttpPeer` 阻塞 adapter);后台复制器经 master `LookupPartition` 发现 peer(排除自身、按 `up` 过滤),对每个本地 replica 的每个 peer 周期拉一轮;BOOTSTRAP 从 `token=Uninitialized` 全量逻辑回放追平。过单测(apply 四态 + 收敛/幂等/bootstrap)+ 集成测试(两 store-server 真 HTTP 收敛 + delete 传播)。
  - **N3 分期(文档化,非代码就地另选,见该 change design.md D-A)**:S3a 只落**逻辑拉复制**(正确收敛的完备核,含 bootstrap 全量逻辑回放);**S3b** 再叠加封存段**物理流式拷贝**(MTTR 优化)。N3 最终形态(混合)不变。
  - **传输(细化 D2,见 design.md D-B)**:node↔node 复制元数据走 store-server 现有 **HTTP(axum)**(非新起 gRPC server);master↔node 控制面仍 gRPC。

**S3b(封存段物理拷贝,N3 混合完成)已实现**(见 `openspec/changes/store-bootstrap-copy/`):`blob` 引擎加封存段只读拷贝原语(`sealed_segments`/`read_segment_prefix`/`LogSegment::raw_prefix`)+ `open_rebuild_index_with`;数据节点加发送端点(`GET .../segments`、`GET .../segments/{position}`)+ 接收装载 `ReplicaManager::install_bootstrap`(段文件 `tmp→fsync→rename` 崩溃安全 + 重建打开);复制器对**空**副本首次先物理拷贝封存段(不可变),续拉游标 = 最高封存段末尾,再逻辑补活跃尾;无 peer/无封存段回落纯逻辑。过单测(封存段枚举/拷贝重建)+ 集成测试(多封存段 + 活跃尾混合 bootstrap,真 HTTP 收敛)。
 - 待续:disk-aware 每盘并发限流(N4);放置据 `DiskHealth` 避让;`DROPPED` 删目录回收;真流式 put;client SDK(暂不做)。**EC 仍锁定(暂不解锁)**。

**S4(本地维护 worker)已实现**(见 `openspec/changes/store-local-worker/`):数据节点常驻后台 `worker.rs`,对本节点每个本地 replica 分三条独立周期 loop 驱动 `blob` 引擎的 **flush**(推进持久化边界,缩小 W=2 page-cache 掉电窗口)、**compaction**(`compact_if_needed(compaction_dead_ratio)`,按封存段死数据占比阈值增量压缩)、**hard_delete**(擦除已删对象数据体);各周期时长独立可配置;同步引擎调用经 `spawn_blocking` offload。复用引擎既有崩溃安全实现(不改写路径语义),单店失败只记 warn 不拖垮全局。过单测(flush 后重开可读 / compaction 回收死数据 / hard_delete 擦除计数)。

**配置项清单**(`StoreServerConfig`,TOML `[store_server]` 表下):

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
| `store_server.local_worker_enabled` | bool | true | — | 本地维护 worker 开关(S4:周期 flush/compaction/hard_delete) |
| `store_server.flush_interval_secs` | u64 | 30 | ≥1 | 本地 worker flush 周期(推进持久化边界)(S4) |
| `store_server.compaction_interval_secs` | u64 | 3600 | ≥1 | 本地 worker compaction 检查周期(S4) |
| `store_server.compaction_dead_ratio` | f64 | 0.5 | (0,1] | 封存段死数据占比 ≥ 此值才压缩(S4) |
| `store_server.hard_delete_interval_secs` | u64 | 300 | ≥1 | 本地 worker hard_delete 周期(擦除已删数据体)(S4) |

> store 引擎自身配置(段大小等)复用 `[store]` 段(见 store.md §10);`start_with` 可注入(测试/小部署)。

**监控项清单**(`StoreServerMetrics`):

| 指标名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `store_server_put_total` | counter | 次 | blob PUT 请求数 |
| `store_server_get_total` | counter | 次 | blob GET 请求数 |
| `store_server_put_bytes_total` | counter | 字节 | 写入字节数 |
| `store_server_get_bytes_total` | counter | 字节 | 读取字节数 |
| `store_server_replicas` | gauge | 个 | 本地 partition-replica 数 |
| `store_server_errors_total` | counter | 次 | 数据面错误数 |
| `store_server_heartbeats_total` | counter | 次 | 收到的心跳指令数(S2) |
| `store_server_replication_rounds_total` | counter | 次 | 拉复制轮次(S3a) |
| `store_server_replication_applied_total` | counter | 条 | 从 peer apply 的记录数(S3a) |
| `store_server_replication_bytes_total` | counter | 字节 | 从 peer apply 的字节数(S3a) |
| `store_server_replication_errors_total` | counter | 次 | 拉复制错误数(S3a) |
| `store_server_bootstrap_segments_total` | counter | 个 | bootstrap 物理拷贝的封存段数(S3b) |
| `store_server_bootstrap_bytes_total` | counter | 字节 | bootstrap 物理拷贝字节数(S3b) |
| `store_server_flushes_total` | counter | 次 | 本地 worker flush 次数(S4) |
| `store_server_compactions_total` | counter | 次 | 本地 worker 发生压缩的 replica 次数(S4) |
| `store_server_hard_deleted_total` | counter | 条 | 本地 worker 擦除数据体的记录数(S4) |

## 10. 数据面读写 + quorum(D4 展开)

**W=2 / R=1,靠"不可变 + 拉复制"收敛**(对齐 Ambry `routerPutSuccessTarget=2`/`routerGetSuccessTarget=1`)。Router 逻辑在 **client SDK**(无 frontend)。

**PUT(异步 quorum)**:
```
client SDK: 从缓存的可写分区挑 P → 拼 blobId=partitionId(P)+objectId(master 号段发号,§8)
  → 并行 HTTP PUT 到 P 的全部 N 副本(store server)
  → 收 W=2 ack 即返回成功;第 3 副本靠拉复制补
```
**GET(R=1,不可变红利)**:
```
client SDK: 解析 partitionId → LookupPartition(master,可缓存)→ 副本位置
  → HTTP GET 一个副本(§7.2 状态门控);not found/失败→换副本 → R=1 成功即返回
```
> 关键:blob 不可变 → **不需要 R+W>N**。W=2 保证 ≥2 份有,GET 试几个必命中,第 3 份复制补。省掉 quorum 读。

**变更(DELETE/TTL/UNDELETE)—— 唯一最终一致窗口**:并行写墓碑/更新 → W=2 ack;`newest-wins`+`lifeVersion`(store 已实现)解冲突;拉复制传播。⚠ 未追上的副本短暂可能返回旧态(接受,最终一致)。

**拉复制(anti-entropy)收敛**:
```
每 STANDBY 副本 对每 peer:周期发 ReplicaMetadataRequest(token=log offset 游标)
  → peer 返 token 后的新记录元数据 → 算缺失 key → GetRequest 拉 → apply(newest-wins) → 推进 token
```
- 复用 store 已有 `FindToken`(每-peer log offset 游标)。**bootstrap 拉全量 = 混合(§9 N3,全部实现)**:封存段物理流式拷贝(快,MTTR 优先)+ 活跃段尾巴 token 逻辑补;非纯 token=0 逻辑回放。
- **S3a 落地(见 §9.1 + `store-server-replication`)**:step-1 ReplicaMetadata 与 step-2 拉 blob **均走 store-server 现有 HTTP(axum)**(细化 D2:node↔node 复制走 HTTP,master↔node 仍 gRPC);写入用 store `apply_replicated` 忠实保留 peer lifeVersion。物理段拷贝(N3 的 MTTR 优化)分期到 S3b。

**参数(默认,已定)**:
| 参数 | 默认 | 说明 |
|---|---|---|
| put 并行度 | = RF | 并行写全部副本 |
| W(put/delete/ttl/undelete) | **2** | ≥2 算成功,存活 1 副本丢失 |
| R(get) | **1** | 不可变,读任一有的副本 |
| ack 时机 | page-cache(吞吐优先) | 高持久档可选 fsync-ack;W=2 跨故障域缓解 |
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

**增强(见 `openspec/changes/admin-enhancements/`)已实现**:
- **分页**:`ListPartitions` 加 `offset`/`limit`/`total`;admin `GET /api/partitions?offset=&limit=` → `{items,total,offset,limit}`;SPA 翻页。
- **RBAC 双角色**:`operator_token`(读+写)/ `viewer_token`(只读);写操作 viewer → **403**;`GET /api/me` 返回角色;SPA 按角色显隐写操作。
- **SetPartitionState 运维**:master `SetPartitionState` gRPC(RW/RO/SEALED,raft 命令 `MasterCommand::SetPartitionState`)+ admin `POST /api/partitions/{id}/state`;SPA 每行 可写/只读/封存 按钮。
- **中英双语**:SPA i18n(en/zh 字典 + 语言切换,localStorage 持久化)。

### 11.1 配置项清单(`AdminConfig`,TOML `[admin]` 表下)

| key | 类型 | 默认值 | 取值/校验 | 含义 |
|---|---|---|---|---|
| `admin.master_addr` | String | `127.0.0.1:7101` | 非空 | master 控制面 gRPC 地址 |
| `admin.listen_addr` | String | `127.0.0.1:7300` | 非空 | admin HTTP 监听地址 |
| `admin.operator_token` | String | `""` | — | operator 准入令牌(Bearer,读+写);空 = 关校验(仅本地) |
| `admin.viewer_token` | String | `""` | — | viewer 准入令牌(Bearer,只读);写操作返回 403;空 = 不启用只读角色 |
| `admin.cluster_secret` | String | `""` | — | 集群准入密钥(与 master 一致),用于对 master 发写操作 |
| `admin.grafana_url` | String | `""` | — | Grafana 外链;空 = 不显示按钮 |

### 11.2 监控项清单(`AdminMetrics`)

| 指标名 | 类型 | 单位 | 含义 |
|---|---|---|---|
| `admin_api_requests_total` | counter | 次 | admin API 请求数 |
| `admin_ops_total` | counter | 次 | 运维操作数(建分区/下线) |
| `admin_master_rpc_errors_total` | counter | 次 | 调 master gRPC 失败数 |

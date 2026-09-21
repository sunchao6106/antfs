# cluster 设计:集群/服务层角色与传输选型

本文固化 **store 之上的集群/服务层**的角色划分与关键选型。**状态:选型已定,实现规划中**
(当前仓库只落地了单机 [`store`](./store.md) 引擎)。选型依据来自对 Ambry / SeaweedFS / CubeFS 三套系统的源码调研。

## 1. 角色划分(目标形态)

```
                 ┌──────────────────────────┐
   client ──S3──▶│  frontend / S3 网关       │  无状态门面,内嵌 Router:分块/选副本/写 quorum
                 └───────────┬──────────────┘
             控制面 gRPC      │ 数据面 HTTP 流式
        (assign/lookup)       ▼
   ┌──────────┐  心跳/放置  ┌──────────────────┐   拉式复制(anti-entropy)
   │  master  │◀──gRPC────▶│  server/DataNode  │◀──────────────────▶ 其它 server
   │ 内建Raft │  状态转移   │  store 引擎 + 复制 │  gRPC(ReplicaMetadata)+ HTTP(拉 blob)
   └──────────┘            └──────────────────┘
   集群大脑:partition→replica 放置、节点状态机、故障处置(取代 Ambry 的 Helix+ZK)
```

| 角色 | 职责 | 状态 |
|---|---|---|
| **server / DataNode** | 承载 [`store`](./store.md) 引擎,服务 CRUD,跑副本复制 | 引擎已成型,服务层规划中 |
| **master** | partition→replica 放置、节点/副本状态机、故障处置;**内建 Raft**(自包含,不依赖外部协调器) | 规划 |
| **frontend / S3 网关** | 无状态门面,内嵌 Router:分块、选副本、写 quorum、读切副本 | 规划 |
| **scheduler**(待定) | compaction / 修复 / 均衡 / GC 的独立调度角色 | 待定(见 §4) |
| **EC 冷层**(待定) | 老数据纠删码降本 | 待定(见 §4) |

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
                  { capacity_bytes, tag }>, registered_at }>
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
- [ ] **M1 骨架**:openraft 起 3 节点 + 选主;节点自注册(心跳→raft 成员表);单调 partition_id;软状态(存活/容量)内存;gRPC `Heartbeat(stream)`/`RegisterNode`/`ListNodes`。**不做放置**。
- [ ] **M2 放置**:`CreatePartitions`(分配 id + 故障域 rack-aware 选盘 + 写 raft placement)+ **容量感知选盘(余量 ≥ partition 上界且相近的 N 块盘;盘将满转 RO、不再放新,见 §7.3)** + `LookupPartition` + 路由索引心跳重建 + `AssignPartition`(取可写分区)。- [ ] **M3 调谐**:副本状态机(BOOTSTRAP→STANDBY/LEADER,§7.2)+ 心跳失联检测 → 产再复制任务。
- [ ] **M4 再平衡/EC 钩子**:容量均衡、decommission、对接 D5 EC 转换决策。

**待拍决策(实现前确认)**:
- [ ] **Q1 节点注册 / 单机盘识别**:自注册 + 固化(SeaweedFS,倾向)vs 人工 HardwareLayout(Ambry)。
  - 证据(源码核实):Ambry `DiskId.getRawCapacityInBytes()` 来自 HardwareLayout 配置、从不探盘,可用空间靠进程内 `decrease/increaseAvailableSpaceInBytes` 记账;SeaweedFS `DiskLocation.CheckDiskSpace` 周期 `statfs` 上报真实 total/free 供均衡,`-max 0` = 空闲空间÷volume 大小自动定槽,`vol_dir.uuid`+`os.SameFile` 认盘去重,`loadExistingVolumes` 扫目录自恢复中断的 compaction 也一并回滚/前滚。
  - 倾向:取 SeaweedFS「配置给目录列表 → 节点 `statfs` 探真实容量 + 扫目录自恢复 + 健康探测上报 master」,不采 Ambry 把容量写死配置的做法(详见 §3「单机盘识别」行)。
- [ ] **Q2 Raft 边界**:锁在 §7.1 hard-state(id/Nodes/Partitions/Policy),其余全软?(倾向 是)
- [ ] **Q3 Raft 存储后端**:`redb`(倾向,纯 Rust 嵌入式、崩溃安全)vs 纯内存+快照;**不用 RocksDB**。
- [ ] **Q4 LEADER/STANDBY**:MVP 单 DC 折叠成 `ACTIVE`(倾向),多 DC 再启用 LEADER 做跨 DC 拉复制。
- [ ] **Q5 partition 分配触发**:手动 `CreatePartitions` + master 内置 rack-aware 自动放置(倾向,替代 Ambry 的静态 JSON+bootstrap);自动扩容(容量水位触发)留后。
- [ ] **Q6 admin 入口**:MVP 只给 gRPC/CLI(不做开箱 web 控制台,保持轻)。
- [ ] **Assign 交互对齐 SeaweedFS**:master 发 id + 位置,client 直连存储节点走 HTTP 写(D2/D4);master 不碰数据字节。

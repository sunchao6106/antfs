## Why

**一块挂死的盘能拖垮整台数据节点。** 数据节点的引擎 IO(数据面 + 后台 loop + 健康探测)全部挤在**同一个进程级阻塞池**上(tokio 默认 `max_blocking_threads = 512`,全仓无一处配置),且**没有任何服务端超时**:`crates/store/src/server.rs:4` 的注释自己写着"专用每盘线程池留 S4/性能期",而 `docs/design/cluster.md` §9 **N4** 早已锁定"**每盘一个小线程池**(池大小按盘 IO 能力定;同盘上的 BlobStore 共享)"——目标锁了半年,代码还是共享池。当前落在代码里的是这个反模式:

- **D 状态线程不可回收**:盘挂死(kernel D 状态,不可中断)时 `spawn_blocking` 的线程**永久占住**——`tokio::time::timeout` 只能放弃等待,**不能取消** blocking 任务(= 漏线程)。池是**进程级共享**的 ⇒ 一块盘吃满它,所有盘的引擎 IO 一起排队等死。
- **后台任务串行跨盘**:`worker.rs:155`(一次 compaction 遍历全部分区)、`worker.rs:294`(通用 flush loop)、`replication.rs:908`(逐 peer 拉)、`heartbeat.rs:211,234` 都在同一个池里**按序**跑 ⇒ 一块卡住 = **全体盘的 flush/compaction/复制停摆**。
- **探测自身就在漏**:`health.rs:56-77` 每轮 write→read→verify→delete 都 `spawn_blocking` + timeout,**超时即丢句柄、线程照漏**。默认 60s 周期 / 10s 超时时,一块坏盘**零流量**约 8.5 小时就能漏满 512 个线程;而 `tokio::time::interval` 默认 `MissedTickBehavior::Burst`,≥7 块盘 × 10s 就让追尾的轮次**背靠背**。且探测**默认关**(`disk_health_enabled = false`)。
- **现有自愈对 hang 完全无效**:引擎自关阈值默认 0 = 关(`crates/blob/src/config.rs:40`),只对**返回的** IO 错误计数、任一成功即清零(`crates/blob/src/blob.rs:954-983`);hang 不返回错误 ⇒ **永不触发** ⇒ 副本永远报 `ACTIVE`(`replica.rs:299`)。
- **健康只报不用**:`health.rs:2` 明说"仅上报观测",`HealthMap` 只有心跳读(`heartbeat.rs:86`),请求路径 / `worker.rs` / `replication.rs` **一律不看**;master 只用它避开**新**放置(`placement.rs:76`)——**没有任何东西把已有副本从坏盘上搬走**。更糟的是上报值把"未探测"与"健康"混成一个空串。
- **判活与盘 IO 耦合**:`heartbeat.rs:161` 在异步心跳任务里同步 `statvfs` 全盘;心跳一旦被盘卡住,master 判**节点死**(默认 15s),于是 `server.rs:425-436` + `486-505` 把**全节点**每个欠副本分区在一 tick 内串行再复制,**无并发预算、无速率限制**(均衡有 `master.balance_max_in_flight = 1`,这里没有)。

结果:一次单盘故障 = 整台节点数据面不可用 + 全节点再复制风暴 + 线程泄漏。而 `openspec/changes/io-index-design/design.md:38` 已明确"盘 hang 容错是**集群层**的可用性问题(盘健康检测 + 多副本),不该靠把引擎 IO 改异步解决"——**本 change 就是补上那个"集群层"**,引擎 IO 模型不动。

## What Changes

- **D1 每盘有界池(N4)**:每盘一个**归该盘所有**的有界执行池,取代共享 `spawn_blocking` 承载该盘的引擎 IO;池满 ⇒ **快失败 503**,绝不无界排队。卡死的线程只算在该盘头上,池打满只伤那一块盘。
- **D2 健康闸门**:`HealthMap` 从"只报"升为**本地可执行状态**——数据面入口对 `UNAVAILABLE` 盘**短路 503**(不碰盘);`worker.rs`/`replication.rs` 跳过坏盘。进入坏态需**连续 N 轮坏**,恢复需**连续 M 轮好且稳定 T2**;**"无观测" ≠ HEALTHY**。
- **D3 IO 看门狗**:每盘在途操作计数 + **在阻塞闭包内部**测单次 IO 时长(只含 syscall,不含池排队);超 deadline ⇒ 置 `UNAVAILABLE` + 后续请求快失败 + 计数 + 上报 master。并对**真挂死**(内核永不返回)留逃生舱:升级为**进程自杀 + supervisor 重启**(Ceph `osd_op_thread_suicide_timeout` 同思路)——进程内回收 D 状态线程是不可能的,必须承认这一点。
- **D4 从坏盘撤离**:master 侧新增动作,把 `UNAVAILABLE` 盘上的副本**搬走**(今天只有"新放置不落它");复用 M4 的 evacuation 机制,**先补后删**(继任者 ACTIVE 前绝不删源,RF=1 永不删);需**持续证据**才动手;受**并发预算/速率限制**(对齐均衡的 `max_in_flight` 风格);`maintenance` 期跳过。
- **D5 判活与健康解耦**:心跳路径上的盘 IO 移出 reactor(**已由前置 commit 落地**:逐盘 `statvfs` 单飞),并补上"拿不到新值就如实报 `UNAVAILABLE`/`UNKNOWN`"的上报语义,**心跳绝不被盘 IO 阻塞**;master 侧判活加迟滞(连续缺拍 / ≥2× 超时),让"一次盘抖动或调度停顿"不能升级成"节点死 ⇒ 全节点再复制"。
- **D6 恢复动作要有预算**:给每 tick 的再复制加**上限**(今天 Unlimited);恢复阶梯**从便宜到贵**先用尽:停新放置(免费)→ 客户端绕开该盘(便宜)→ 撤离/再复制(GB 级网络)→ 换硬件。误判代价**不对称**:多留一份副本几乎免费,一次不必要的迁移是真金白银。
- **D7 内核 fail-fast 是前提(部署要求,不是代码)**:设备级超时让 IO **返回 EIO 而不是永久 hang**(SCSI `/sys/class/scsi_device/*/device/timeout`=30s、`nvme_core.io_timeout`=30s/`max_retries`=5、dm-multipath `no_path_retry`——**绝不用** `queue_if_no_path`、NFS `timeo`),并打开既有的引擎自关(`io_error_count_to_trigger_shutdown` 取 3–10)。否则 D2/D3 **永远观察不到错误**,整条阶梯是空响;文档化为前置条件。

## Capabilities

### New Capabilities
- `store-disk-isolation`: 单进程多盘节点的**盘级故障隔离与处置**——每盘有界池 + 池满快失败、盘健康闸门(含迟滞与"未知≠健康")、IO 看门狗(含进程自杀逃生舱)、坏盘撤离(先补后删 + 持续证据 + 预算)、判活与盘 IO 解耦、恢复动作的速率预算与阶梯、以及部署侧的内核 fail-fast 前提。

### Modified Capabilities
<!-- master 侧的判活迟滞、坏盘撤离、再复制每 tick 预算**属于同一条"坏盘隔离"行为链**(离开 store 侧上报的 UNAVAILABLE 就没有意义),故并入新 capability `store-disk-isolation` 一处描述,不另开 master spec delta——把一个契约劈成两个文件只会让"谁触发谁"更难追。 -->

## 前置:Phase 0(单独小 commit,不阻塞本 change)

这两件**独立可交付**的止血动作由**单独的小 commit** 落地(本 change 只依赖其结果,不重复验收)。**当前工作树里已有一版未提交的实现**,覆盖情况如下:

1. **探测"最多一个在飞"** —— **已落地**:某轮探测未返回时下一轮**不再叠一个进去**,迟到的结果**仍作为该盘的观测被采纳**;跳过轮**不写 `HealthMap`**(保持上一轮判定,绝不因"没探到"而回到 `HEALTHY`)。补齐"跳过轮"这一半;但**"从未有过观测"仍上报空串**(而 master 把空串当成"可用"),这一格由本 change 修成显式 `UNKNOWN`。
2. **每盘在途观测 + 定位盘的插入点** —— **已落地**:`store_server_disk_in_flight_requests{disk_id,op}`、`store_server_disk_slow_requests_total{disk_id,op}`(端到端、含池排队,纯观测)、`OpTimer::with_disk(&handle.disk_id)`。本 change 的健康闸门与看门狗**直接挂在这个插入点上**。
3. **把盘 IO 移出 reactor** —— **已落地**:`health.rs` 的探测/`remove_file`、`server.rs:399-403` `StagedFile::drop` 的 `remove_file`(含 handler 取消路径)已移出;`heartbeat.rs` 的 `dm.refresh()`(全盘 `statvfs`)换成**逐盘单飞刷新**(出 reactor + 不叠加漏线程),未归的盘沿用上一拍容量值而心跳照发。余下的是**上报语义**(拿不到新值应报 `UNAVAILABLE`/`UNKNOWN` 而非沉默),见本 change 3.8。

## Impact

- **crate `store`**:新增 `disk_pool.rs`(每盘有界池 + 在途计时 + 看门狗);`health.rs` 的 `HealthMap` 值域与迟滞状态机;`server.rs` 数据面入口加健康闸门 + 全部 handler 改经该盘池;`worker.rs`/`replication.rs`/`heartbeat.rs` 跳过坏盘并改经该盘池;`config.rs` 新增配置;`metrics.rs` 新增指标。**引擎 `blob` crate 不动**(除"部署建议打开的自关阈值"外不改语义)。
- **crate `master`**:`reconcile.rs` 新增按 (node, disk) 的抖出计划(推广既有 `plan_evacuation` 的判据)+ 判活迟滞;`server.rs` 的 `add_targets` 加每 tick 上限、撤离动作置于 `maintenance` 检查之后;`placement.rs` 对新上报值域的处理(空串 / `UNKNOWN` / `HEALTHY` 均可放新副本,避免默认关探测的部署整集群停止放置);`config.rs` 新增配置。
- **proto**:`DiskBeat.health` 已是 string(tag 5),**值域扩展不需要改 proto**(字符串天然前后兼容);不新增 RPC。
- **保留契约**:master 只决策不搬数据(D1);先补后删(M3/M4 已有闸门复用);异步 + 拉复制(D4)不变;**引擎 IO 仍同步阻塞**(`io-index-design` D1);**EC 仍锁定**。
- **文档**:按 CLAUDE.md §7,实现的同一 PR 必须把新增/修改的配置项与监控项写进 `docs/design/store.md`(§10/§11/§12 的配置与监控表)与 `docs/design/cluster.md`(§7.7 master 清单、§9.1 store server 清单),并新增"部署前提:设备级超时"一节。**文档缺一视为改动未完成**。

## Non-goals

- **不改引擎 IO 模型**:仍同步阻塞 `std::fs::File`(`io-index-design` D1 已锁"盘 hang 归集群层");**不**迁 async/AIO/io_uring。
- **不自动换硬件**:D7 的设备级调优与换盘都是**运维动作**,代码只负责"把坏盘摘出来 + 把副本补回去"。
- **不做跨节点协调**:除 master 已有的放置/撤离/软状态之外,不引入新的一致性协议。
- **不改磁盘格式、不改数据面语义**(503 只新增两类 reason,不改变成功路径语义)。
- **不重复验收 Phase 0**:那两件由前置 commit 负责。

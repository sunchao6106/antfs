# tasks — store-disk-isolation(N4 每盘隔离 + 盘故障处置)

> 状态约定:**已收口(2026-09-24)** —— store 侧 D1/D2/D3 + master 侧 D4/D5/D6 + 部署前提 D7 的文档**全部落地并提交**,
> 代码/文档类待办**清零**。勾选项 = 已有代码 + 测试覆盖;每处落地都记了与原始设想的**差异**,别把勾选当成"和原文一模一样"。
> **真机类条目(阈值标定 1.2 / 8.5、故障注入端到端 8.4 的一半)按决策本轮不做**,已从"待办"移出、改成下方
> 「未执行:真机项」一节(不带勾选框,措辞保留"未执行")—— 清单与判据留在
> [`cluster.md` §9.4 / §9.5](../../../docs/design/cluster.md),将来上真机时按它做。
> 任务里的 `file:line` 按**当时的 HEAD**给出;后续改动(尤其 `crates/store/src/{server,heartbeat,worker,
> replication,config}.rs` 与新增的 `disk_pool.rs` / `disk_health.rs`)会让行号漂移 —— 以代码为准。

## 0. 前置(Phase 0,单独小 commit;本 change 依赖其完成,但不重复验收)

> **已在工作树落地(未提交)**,故勾选;勾选表示"**由前置 commit 覆盖**",**不是**本 change 的验收项。

- [x] 0.1 `crates/store/src/health.rs`:每盘**最多一发在飞**探测 —— 上一发未归 ⇒ 本轮**跳过**该盘(`SlotAction::Skip`,不写 `HealthMap`、不叠加新阻塞任务),计 `store_server_disk_probe_skipped_total{disk_id}` / gauge `store_server_disk_probe_stuck{disk_id}`;超时后**留住** `JoinHandle`(`StuckProbe` 槽位)—— *前置 commit 落地*
- [x] 0.2 同上:**迟到的探测结果被采纳**(`SlotAction::Adopt`,该盘下一轮取它的回答,不再新发;`warn` 日志记录)—— *前置 commit 落地*
- [x] 0.3 `health.rs`:`probe_disk_sync` 合并 `exists/write/read/remove_file` 四步为**一个**阻塞任务(原来删除步骤在超时之外裸跑),超时一律由异步侧加;步骤槽位 `Step` 用于按"卡在哪一步"判类型(`WriteTimeout`/`ReadTimeout`/…)—— *前置 commit 落地*
- [x] 0.4 `crates/store/src/metrics.rs` + `server.rs`:每盘在途请求 gauge `store_server_disk_in_flight_requests{disk_id,op}` + 拿到副本后定位盘的插入点 `OpTimer::with_disk`;每盘端到端慢请求 `store_server_disk_slow_requests_total{disk_id,op}`(阈值复用 `disk_health_timeout_secs`,口径含池排队)—— *前置 commit 落地*
- [x] 0.5 盘 IO 移出 reactor:`health.rs` 的探测/`remove_file`、`server.rs:399-403` `StagedFile::drop` 的 `remove_file`(含 handler 取消路径)—— *前置 commit 落地*
- [x] 0.6 `metrics.rs` + `server.rs`(admin"主机磁盘指标"页的数据面):每盘 `store_server_disk_requests_total{disk_id,op}`(rate = 该盘 IOPS)、`store_server_disk_bytes_total{disk_id,op}`(成功路径字节数,rate = 带宽)、`store_server_disk_request_duration_seconds{disk_id,op}`(端到端口径,与 `disk_slow_requests` 同一把尺子)、以及早退请求不落盘维度的边界 —— *前置 commit 落地*
- [x] 0.7 `heartbeat.rs` + `disk.rs`:`dm.refresh()`(全盘 `statvfs`,原先在异步心跳任务里同步调用)**换成逐盘单飞刷新** —— `spawn_refresh` 把每盘 `statvfs` 丢进阻塞池、`collect_refresh` 收已完成的一发并写回(`DiskManager::apply_statfs`);**每盘至多一发在飞**(与 0.1 同一套守卫),未归的盘沿用上一拍容量值,而**心跳照常发出**。`DiskManager::refresh` 随之删除(唯一调用点就是这里)—— *前置 commit 落地*
> **仍属本 change 的一项**(不在此重复列出):上一条只解决了"心跳被盘卡住"这个**机制**问题;刷新拿不到新值时应**如实上报** `UNAVAILABLE`/`UNKNOWN`(现在只是沉默沿用旧值),见 **3.8**。
>
> **口径提醒**:0.6 那三条是**请求级**数字且只覆盖数据面,**不是**块设备 IOPS/带宽 —— 引擎内部 IO(flush / compaction / 入向复制 apply)随 D1 每盘池归口后再计入;真正的 syscall 耗时口径是本 change 的 `store_server_disk_io_duration_seconds`(4.4)。别把两把尺子混用(见设计 §5.1)。

## 1. 文档骨架(先写,后实现)

- [x] 1.1 `openspec/changes/store-disk-isolation/{proposal,design,tasks}.md` + `specs/store-disk-isolation/spec.md`;`openspec validate store-disk-isolation --strict` 通过
- **1.2(标定类)** 配置/监控默认值**标定后再落**:§10/§11 标 **【提案】** 的默认值必须先压测标定,再改代码 `impl Default`,
  再改文档表(CLAUDE.md §5.1:不擅自定默认值)—— **本轮不做,见文末「未执行:真机项」**:12 个数字目前全是"经验初值",
  已由单测钉住、并在 `docs/design/store.md` §12.12 与 `cluster.md` §9.4 显式标注

## 2. D1 每盘有界池(N4)

> **⚠️ store 侧已落地**(见 `docs/design/store.md` §12.11「一、每盘有界池」,含完整测试证据表与残留风险清单);
> 勾选项 = 已由前置 commit 覆盖,**不是**本 change 的验收项。落地时与原始设想的三处差异:
> ① 线程模型取**每盘自建 OS 线程池**(不是每盘 tokio runtime,理由见 design §3.2);
> ② 池满取**"短暂排队 + 超时才拒"**(不是立即拒):排队容量与等待上限都进配置;
> ③ 配置形态改成 **每盘一条 `data_dirs` 项(可带 `pool_size`)+ 一个"每盘默认池大小"**
> (`pool_default_size`),**不做**"总预算均分"(盘数一变就悄悄改所有盘的并发,不可预期)。
> 仍属本 change 的:`ensure_replica` 与流式 put 的暂存 `unlink` 尚未归口(残留风险 1)、健康闸门尚未接入(§3)。

- [x] 2.1 新增 `crates/store/src/disk_pool.rs`:`DiskPool`/`DiskPools` —— 每盘 N 条专属 worker + **有界队列** + 池满**短暂排队后拒绝**(503)
- [x] 2.2 形态选型落地:**每盘自建线程池**(线程名 `antfs-disk-<id>-<i>`);每盘至多 `pool_size` 次在飞 IO
- [x] 2.3 `DiskPools` 按 `disk_id` 持有池(同盘共享一份,对齐 N4);`StoreServer::disk_pools()` 供自省/测试
- [x] 2.4 **数据面全部处理器改经该盘池**:put(内存/流式/缓冲回落)、get、delete、metadata、replica_metadata、segments(列清单与取段)、verify、scrub-report、手动 compact(同步与异步)
- [x] 2.5 **后台改经该盘池**:`worker.rs` flush(逐 replica 并发提交)与 compaction(顺序提交)、`replication.rs` 每轮拉取;`heartbeat.rs` 的 ensure/drop 与 `health.rs` 探测**不在池上**(理由见 store.md §12.11「谁在池上」表)
- [x] 2.6 池满 / 未知盘 / 池关停 ⇒ **503 + `Retry-After`**;任务 panic ⇒ **500 + `errors_total{kind="join_error"}`**;拒绝计入 `disk_pool_rejected_total{disk_id,op,reason}`(不进 `errors_total`)
- [x] 2.7 `statfs` 不经池:在阻塞池里**每盘单飞**刷新(未归则沿用上一拍值,心跳照发),不因池满判盘坏
- [x] 2.8 单测:容量恰好 = workers+queue、被拒不占容量、FIFO、并行度、跨盘互不影响、panic 不打死 worker/不漂计数/不漏许可、线程随析构退出、200 并发结果不错位、关停时排队者明确失败(清单见 store.md §12.11)
- [x] 2.9 配置 `store_server.data_dirs`(每盘 `pool_size`)、`pool_default_size`、`pool_queue_size`、`pool_acquire_timeout_ms`;指标 `disk_pool_workers` / `disk_pool_in_flight` / `disk_pool_queued` / `disk_pool_rejected_total`
- [x] 2.10 文档:`store.md` §12.11(醒目"地基性变更"标注 + 测试证据 + 残留风险)、`cluster.md` §9 配置/监控表加行

## 3. D2 健康闸门

> **⚠️ 已落地(store 侧)** —— 见 [`docs/design/store.md` §12.12](../../../docs/design/store.md)(含测试证据表与残留风险清单)。
> **两处与原始设想的差异(如实记录)**:
>
> ① **闸门落点是"数据面中间件",不是各 handler 里的 `with_disk` 位置**(3.4)。原因:`with_disk` 只在
> `put/get/metadata/replica_metadata/segments/delete` 这些 handler 里调过,而 `scrub_report`/`verify`
> **根本不调它**;更关键的是 **3.4 要求"复制读/`replica-metadata`/`segments` 一并被闸"**,而
> `request_control::classify` 恰恰**豁免**了这三类 —— 挂在 `with_disk` 位置就覆盖不到它们。
> 中间件方案用**同一个** `ReplicaManager::get(pid)` 定位盘(与 `with_disk` 同源,没另起寻盘逻辑),
> 覆盖 `/partitions/{pid}/**` **全部**路由;**豁免靠路由结构**保证(中间件只挂在数据面子 router 上,
> `/metrics` 与 `/admin/**` 是另外的路由),并额外显式判一次路径前缀防后人把路由合并回去。
> 副产品:闸门在 extractor 之前短路 ⇒ 被拒请求**连 body 都不读**。
>
> ② **`statfs` 刷新未归不判 `UNAVAILABLE`**(3.8)。设计 §3.3 自己就写了"`UNAVAILABLE` 只能由探测/看门狗
> 得出,不能被正常排队现象污染" —— 一次 `statvfs` 拿不到值只说明"容量读数暂时不可得",不等于"盘坏"。
> 故本 change 做的是:**容量沿用上一拍已知值(前置 commit 已做)+ 心跳照发 + 健康值如实报
> `UNKNOWN`/`UNAVAILABLE`(来自探测/看门狗的状态机)**,而**没有**把一次刷新超时直接升级成隔离。

- [x] 3.1 `health.rs`:`HealthMap` 值域改为每盘状态结构(`DiskHealth` 轮次判定 + `available` 可服务性 + 迟滞计数/时刻);维持"探测写、心跳与闸门读";**原始轮次判定与派生 `available` 必须分开**(否则一上线就变成"单轮超时即摘盘")—— 落地在**新模块** `crates/store/src/disk_health.rs`(纯逻辑 + 可注入时刻,便于零 sleep 单测);`health.rs::HealthMap` 成为它的别名
- [x] 3.2 迟滞状态机:`disk_health_bad_rounds` / `disk_health_bad_secs` **先到**进坏态;`disk_health_good_rounds` **且** `disk_health_recover_stable_secs` 且稳定才复位;坏态期间的好轮**不归零**
- [x] 3.3 `heartbeat.rs::disk_beats`:从未有过观测的盘上报 **`UNKNOWN`**(取代 `unwrap_or_default()` 空串);已隔离的盘上报 **`UNAVAILABLE`**
- [x] 3.4 数据面入口前置闸门 ⇒ 盘不可用 ⇒ `503 + Retry-After` + `store_server_rejected_requests_total{reason="disk_unavailable"}`;**复制读/`replica-metadata`/`segments` 一并被闸**(不沿用 `RequestControl` 的豁免)。**落点见本节开头的差异 ①(中间件)**;`op` 标签由新增的 `request_control::path_op` 给(有单测把它钉在"每条数据面路由都要归类"上)
- [x] 3.5 `/metrics` 与**任何 `/admin/**`** 永不被健康闸门短路(不能做出无法远程恢复的节点)—— 由路由结构保证 + 显式路径前缀判断 + 集成用例守住
- [x] 3.6 `worker.rs`:replica 遍历跳过不可用盘(flush + compaction 两条 loop);`replication.rs`:跳过不可用盘上的 pid(作源/作目标均跳),**不破坏游标**(跳过点在 `sessions` 之前,恢复后从原游标续拉)
- [x] 3.7 配置 `disk_health_bad_rounds` / `bad_secs` / `good_rounds` / `recover_stable_secs`;指标 `store_server_disk_health_state{disk_id,state}` / `store_server_disk_health_transitions_total{disk_id,from,to}` / `store_server_disk_worker_skipped_total{disk_id,task}`
- [x] 3.8 心跳上报语义:拿不到新容量的盘**如实上报**(容量沿用上一拍已知值,**心跳照常发出**)。**注意差异 ②:刷新超时本身不置 `UNAVAILABLE`**
- [x] 3.9 `placement.rs`:放新副本的可用集合扩为 `""` | `HEALTHY` | `UNKNOWN`,**同步其单测**(否则默认关探测的部署一块盘都放不了)
- [x] 3.10 单测/集成:迟滞不抖动;"从未观测 ⇒ `UNKNOWN` 且 ≠ `HEALTHY`";闸门 503 而 `/metrics`、`/admin/**` 仍 200;worker 跳过;`UNKNOWN` 仍可放新副本(清单见 store.md §12.12 的测试证据表)
- [x] 3.11 文档:同上(表 + `disk_health_*` 联合决定隔离延迟的公式;`disk_health_timeout_secs` 已被**三处**复用的警告)

## 4. D3 IO 看门狗

> **⚠️ 已落地(store 侧)** —— 见 [`docs/design/store.md` §12.12](../../../docs/design/store.md)。
> **两处补充**:
>
> ① "最久在途"用**每盘 `BTreeMap<开始时刻, 条数>` 多重集**,不是一个"最早开始时刻"标量 ——
> 任务完成顺序与开始顺序无关(A 先开始、B 后开始、A 先结束),标量会把健康盘的"最久在途"越算越大
> 直到误判(有单测钉住这个反例)。
> ② 时长观测写成**守卫**(`JobTimer`)而不是"函数尾部一行" ⇒ 有干净的不变量:**每个真正开跑的 job
> 恰好产生一个时长样本**(panic 也照记)。
>
> **4.3 的落地形态(与原始设想的差异)**:不是"状态变化时置标志位 + 清队列",而是**任务开跑前查一次
> 实时健康状态**(`disk_health::is_available`),命中即丢弃、连 syscall 都不碰。
> 为什么换:标志位需要**同步**,而同步总有一拍延迟 —— 盘**恢复**之后那一拍里所有任务都会被误拒。
> 查实时状态没有这个窗口(单测 `service_resumes_immediately_after_recovery` 钉住)。
> 另外把结果通道改成 `Result<T, PoolReject>`,好让"没跑就被丢"与"跑了但 panic"**精确区分** ——
> 用标志位猜原因会把引擎 bug 误报成"盘忙,换个副本就行"(单测
> `a_panicking_job_still_reports_job_panicked_even_when_the_disk_is_unavailable` 钉住)。

- [x] 4.1 `disk_pool.rs`:**闭包内**计时(`Instant::now()` 紧贴 syscall);在途条数不另造计数器,复用 `store_server_disk_in_flight_requests{disk_id,op}`;新增 `IoWatch` 提供"最久在途"
- [x] 4.2 看门狗任务(**独立于该盘池**,自己一个 tokio 任务):超 `disk_watchdog_ms` ⇒ 计 `store_server_disk_io_watchdog_trips_total{disk_id}` + 走 3.2 的进入判据(记一次 `IO_TIMEOUT` 坏观测)+ 随心跳上报。**不受 `disk_health_enabled` 约束**
- [x] 4.3 盘判坏后**丢弃该盘池里已排队的请求**(快失败)—— 见本节开头的"落地形态"
- [x] 4.4 指标 `store_server_disk_io_duration_seconds{disk_id,op}`(histogram,闭包内口径,桶铺到 120s);与既有 `disk_slow_requests`(端到端,含排队)**刻意区分**
- [x] 4.5 逃生舱:`disk_watchdog_escalate_secs` 满足 ⇒ `std::process::abort()`;**默认 0 = 关**(配置校验还要求它 ≥ 看门狗 deadline);日志里留下"为什么自杀"的结构化字段
- [x] 4.6 单测:计时不含排队(构造"至少排了 50ms"再断言记下的时长 ≪ 它);超期触发一次 trip 且只记一次坏观测不重复刷;`escalate = 0` 时**绝不** abort
- [x] 4.7 文档:指标表 + 单进程多盘自杀代价的明确记录(见 store.md §12.12「六」/「十」)。**supervisor 侧配置(`Restart=always`)与设备级超时前提写在 §7(D7)**

## 5. D4 从坏盘撤离(master 侧)

> **⚠️ 已落地(master 侧)** —— 见 [`cluster.md` §7.7](../../../docs/design/cluster.md)(配置/监控表)。
> **四处与原始设想的差异(如实记录)**:
>
> ① **`plan_evacuation` 没有被改成"抖出集 = `(node, disk)` 集合"这一种输入**,而是同时收
> `decommissioning`(node 粒度)与 `evicted_disks`(`(node, disk)` 粒度)—— 因为两者的**删源闸门刻意不同**
> (见 ②),用一个集合表达不出来。
> ② **坏盘撤离的删源闸门比节点下线更严**:只有"由坏盘触发"的分区才额外要求"数得上的每一份都已 `ACTIVE`"。
> 理由:节点下线是**运维主动、看着做的**动作(还有可行性前置校验),`healthy >= rf` 即可删源;坏盘撤离是
> **自动**动作,且此刻新补的副本很可能还在 `BOOTSTRAP` —— 把"还在追平的那份"算成已有数据就会删掉唯一一份
> 可用数据 ⇒ 丢数据。两种口径并存是**已知的不一致**,偏保守一侧。
> ③ 内部算了**两个**健康计数(`healthy` 决定要不要补 / `caught_up_healthy` 决定能不能删),而不是一个。
> 这不是冗余:若只用严格口径决定"要不要补",替代副本一进 placement 就不算数 ⇒ **每 tick 重补一发**,
> RF 静默膨胀(实测踩到);若只用宽松口径决定"能不能删",就会删源丢数据。
> 两数之间的窄缝 = "替代已就位、还在追平" ⇒ 本拍**什么都不做**,等它 `ACTIVE`。
> ④ 在途预算用 leader 本地 `evac_started` 记账,而**不是**"硬放置里含坏盘副本的分区" —— 后者在开出
> **第一发之前**就成立,会把预算一开始占满,于是**永远开不出第一发**(实测踩到)。
>
> **已补齐**:5.5 的"盘恢复后不自动回填"由用例钉住了 —— 撤离全链路用例的第 ⑤ 阶段把坏盘改回健康后
> 跑 4 个调谐周期,断言放置**一动不动**、撤离计划计数不涨(设计里明写的"只置不回")。

- [x] 5.1 `crates/master/src/reconcile.rs`:`plan_evacuation` 的"抖出集"从 `BTreeSet<NodeId>` 推广为 `(NodeId, DiskId)` 集合(`decommission` = 该节点所有盘进集合的特例),保持"先补后删"语义不变
- [x] 5.2 `crates/master/src/server.rs`:从软状态各盘 `health` 构建"`UNAVAILABLE` 且已持续 ≥ `disk_evac_min_bad_secs`"的抖出集;计划生成计 `master_disk_evac_planned_total{result}`
- [x] 5.3 执行位置严格在 `server.rs:483` 的 `if maintenance { continue; }` **之后**;受 `disk_evac_max_in_flight`(在途计 `master_disk_evacuating`,对齐 `balance` 的预算写法)与 `disk_evac_enabled` 约束
- [x] 5.4 不变量:删源一律复用 `plan_move` 的闸门(**继任者已 `ACTIVE`** 且存活副本 ≥ RF);`rf = 1` ⇒ **永不**删
- [x] 5.5 盘恢复可用后**不自动回填**副本(回填交给既有均衡/运维,同 `server.rs:441-473` 的"只置不回")
- [x] 5.6 配置 `disk_evac_enabled` / `min_bad_secs` / `max_in_flight`;文档:cluster.md §7.7 配置表 + §9.1
- [x] 5.7 单测:坏盘分区进 `Add`;继任者未 `ACTIVE` 不删源;`rf=1` 不删;`maintenance` 不执行

## 6. D5 判活与健康解耦 / D6 恢复预算

- [x] 6.1 (实现见 3.8)测试断言**心跳绝不被盘 IO 阻塞**:任何盘的 `refresh` 超时/池满都不得阻止本拍心跳发出(取上一拍容量 + 如实报 `UNAVAILABLE`/`UNKNOWN`)
- [x] 6.2 `soft.rs::up_nodes` / `reconcile.rs`:判活加迟滞 `master.node_down_debounce_ticks`(连续 K 个 reconcile tick 都过期才判 down);`0` = 保持今天行为;判定发生时计 `master_node_down_total`
- [x] 6.3 `server.rs:425-436` + `486-505`:`add_targets` 每 tick 只发起前 `master.rereplication_max_per_tick` 个(按 `PartitionId` 排序,确定性),其余下 tick 续;gauge `master_rereplication_deferred` 暴露被限速量
- [x] 6.4 单测:节点静默 1 个 tick **不**触发全节点再复制;真死(持续过期)仍会判 down;超限只发 K 个、下 tick 继续补齐
- [x] 6.5 文档:cluster.md §7.7 配置表 + §7.3 调谐循环描述更新(判活判据、每 tick 预算)

## 7. D7 内核 fail-fast(部署前提;只文档 + 一条启动告警)

- [x] 7.1 `docs/design/cluster.md` 新增「部署前提:设备级超时」小节:SCSI `/sys/class/scsi_device/*/device/timeout`=30s、`nvme_core.io_timeout`=30s / `max_retries`=5、dm-multipath `no_path_retry`(**禁用 `queue_if_no_path`**)、NFS `timeo`/`retrans`;并说明"否则 D2/D3 永远观察不到错误,整条阶梯是空响" —— 见 §9.2(含 §9.5 A 的逐机核对项)
- [x] 7.2 启动自检:`io_error_count_to_trigger_shutdown == 0` 时记一条 `tracing::warn!`(建议 3–10);**不改默认值**(CLAUDE.md §5.1)
      —— **落地差异**:合并成**一条**启动自检(`crates/store/src/server.rs` 的 `start_with`),条件是
      "`io_error_count_to_trigger_shutdown == 0` **或** `disk_health_enabled == false`",一次把两条被动输入
      的状态与后果说清(默认部署两条都关,打两条告警纯属噪声);原来挂在 `serve()` 里那条单独告警已撤掉
- [x] 7.3 `docs/design/store.md` §10.1 的 `store.io_error_count_to_trigger_shutdown` 行补一句"上线建议显式打开 + 与 D2/D3 的关系" —— 见 `store.md` 的 §10.1 表格该行

## 8. 集成验证与收口

- [x] 8.1 集成测试(真 HTTP 数据面,故障注入 = `StoreServer::force_disk_unavailable`,**走与生产同一台迟滞状态机**):该盘分区 503 且**另一块盘的 PUT/GET 不受影响** —— `tests/disk_health_gate.rs::unavailable_disk_fails_fast_on_every_data_plane_route` / `a_bad_disk_does_not_take_down_requests_on_another_disk`
- [x] 8.2 集成测试:坏盘 replica 不被 worker flush/compaction 选中(`background_worker_skips_the_unavailable_disk`);恢复后自动回归(`recovering_the_disk_restores_service`)。**"游标未失效"只做了结构性保证**(跳过点在 `sessions` 之前 ⇒ 不碰 prior token),**没有**端到端断言游标值 —— 诚实标注
- [x] 8.3 集成测试:master 撤离全链路 —— `tests/reconcile.rs::bad_disk_evacuation_copies_first_and_removes_only_after_successor_is_active`:坏盘 → 补副本 → 在"继任者已就位但未 `ACTIVE`"的窗口里**反复断言源仍在** → 继任者 `ACTIVE` 后才删源;并断言最终放置仍是 RF 份、在途 gauge 归 0
- **8.4 / 8.5(真机类)** 见文末「未执行:真机项」—— 8.4 的核心不变式已由单测钉住,真机那半本轮不做;
  8.5 的标定数字未跑(不填数字)
- [x] 8.6 `cargo test` 绿 + `cargo clippy` 无告警(跨 Linux/macOS)—— macOS 侧已验证;Linux 待 CI
- [x] 8.7 `openspec validate store-disk-isolation --strict` 通过

## 后续(不在本 change)

- [ ] **EC 仍锁定**(不解锁;见 `cluster.md` §5)
- [ ] 引擎 IO 模型不变(仍同步阻塞;**不**迁 AIO/io_uring)——`io-index-design` D1 已锁
- [ ] 自动换硬件、跨节点新协调协议:均不做
- [ ] 待议(Open Questions):池大小是否按盘分档、看门狗 deadline 是否按 op 分阈、`abort()` 前是否尽力上报一次

## 未执行:真机项(2026-09-24 按决策本轮不做 —— 不带勾选框)

> 下面三项**一条都没执行过**。它们不在"待办"里(本 change 的代码/文档待办已清零),而是**移交**给真机阶段:
> 步骤、查询与判据在 [`cluster.md` §9.4 / §9.5](../../../docs/design/cluster.md),将来做完在**那份清单**上打勾。

- **1.2 阈值标定后再落**:§10/§11 标【提案】的默认值必须先压测标定,再改代码 `impl Default`、再改两张文档表。
  现状:12 个数字全是**经验初值**,已由单测钉住(`pool_defaults_match_documented_values` /
  `disk_health_defaults_match_documented_proposal` / `degraded_defaults_are_off_pending_calibration` /
  `disk_fault_defaults_are_pinned_pending_calibration`),文档里也显式标着"经验值"。
- **8.4 端到端:refresh 超时 ⇒ 心跳照常、master 不判 down**。已钉住的只是**核心不变式**
  (`heartbeat::an_unfinished_refresh_never_blocks_the_beat_round`:未归的刷新绝不被 `await`、同盘至多一发在飞、
  没有结果就不改容量);**仍缺**"让 `statvfs` 真卡住 + 由真 master 断言没被判 down" —— 需真机挂盘或故障注入 seam。
- **8.5 压测标定**:数字必须在真实机器 + 真实负载上跑出来;拿到真实数据前**不填数字**
  (抄一组数字进来比留着"未标定"更危险)。

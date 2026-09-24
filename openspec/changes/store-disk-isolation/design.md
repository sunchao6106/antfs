# store-disk-isolation 设计(N4 每盘隔离 + 盘故障处置)

承 [`cluster.md` §9 N4](../../../docs/design/cluster.md)、[§6 锁定的 D1–D5](../../../docs/design/cluster.md)、[`store.md`](../../../docs/design/store.md),
以及 `openspec/changes/io-index-design/design.md` **D1**(盘 hang 容错归**集群层**,不靠把引擎 IO 改异步)。
**本 change 不动引擎 IO 模型**(仍同步阻塞 `std::fs::File`),也不动上面两份文档已锁的立场。

一句话:**把"一块盘坏"从"整台机不可用 + 全节点再复制风暴"降级为"那块盘被隔离、其上的副本被替换"。**

## 1. 现状事实核对(带 file:line;实现前请以代码为准)

| # | 事实 | 位置 |
|---|---|---|
| 1 | 数据面全部引擎 IO 经 `spawn_blocking` offload,注释自带"专用每盘线程池**留 S4/性能期**"(= 兑现 N4 被推后) | `crates/store/src/server.rs:4` |
| 2 | 池是**进程级共享**的 tokio 阻塞池,默认 `max_blocking_threads = 512`,**全仓无一处配置它**(`grep max_blocking_threads` = 0 命中) | 全仓 |
| 3 | 服务端**无任何请求超时**;所有 `spawn_blocking(...).await` 无界等待 | `server.rs:522`(ensure)、`571`/`614`(compact)、`799`/`845`(put 内存路径)、`820`(put 暂存路径)、`867`(get)、`922`(stat)、`952`(scrub-report)、`1011`(verify)、`1076`(replica-metadata)、`1139`(segments)、`1167`(delete) |
| 4 | 后台 loop 共用同一池且**串行跨全部 replica/peer** | `worker.rs:155`(一次 compaction 扫全部分区)、`worker.rs:294`(通用 loop)、`replication.rs:908`(逐 peer,`for loc` 内 await)、`heartbeat.rs:211`/`234` |
| 5 | 健康探测 write/read 用 `spawn_blocking` + `tokio::time::timeout`,**超时丢句柄 ⇒ 线程泄漏**;`remove_file` 与 `dir.exists()` 直接在 reactor 上调 | `health.rs:56-77`、`health.rs:79`、`health.rs:44` |
| 6 | 探测**默认关**且逐盘**串行**;`tokio::time::interval` 默认 `MissedTickBehavior::Burst`(轮次超期即背靠背) | `config.rs:85`、`health.rs:87-99`、`cluster.md` §9.1 |
| 7 | 引擎自关**默认关**(0)、只计**返回的** IO 错误、任一成功即清零、hook 只置 replica `ERROR` | `crates/blob/src/config.rs:40`、`crates/blob/src/blob.rs:954-983`、`crates/store/src/replica.rs:299` |
| 8 | 健康**只报不用**;`HealthMap` 唯一读者是心跳;上报把"未探测"与"健康"混成空串 | `health.rs:2`、`heartbeat.rs:86-103`(`unwrap_or_default()`) |
| 9 | master 只用盘健康**避开新放置**;没有撤离动作 | `crates/master/src/placement.rs:76-78`(`free.health != HEALTHY` ⇒ 跳过)、`DiskFree.health` 注释"空串 = 未启用探测,视为可用" |
| 10 | 再复制触发 = `healthy < rf`(healthy = 节点 up && replica 非 ERROR) | `crates/master/src/reconcile.rs:138-152` |
| 11 | 节点 up 判据 = 单窗口 `elapsed() <= heartbeat_timeout_secs`(默认 15s) | `crates/master/src/soft.rs:147-153`、`crates/master/src/config.rs:75` |
| 12 | 执行 loop:每 tick 把**所有**欠副本分区塞进 `add_targets` 后**串行** `try_add_replica`,**无并发预算、无速率限制** | `crates/master/src/server.rs:425-436`、`486-505` |
| 13 | 既有防抖先例:均衡 `balance_max_in_flight = 1`、切主宽限 `reconcile_startup_grace_secs = 15`、盘将满 RW→RO **只置不回**、`maintenance` 全局跳过自动动作 | `config.rs:88`、`config.rs:84`、`server.rs:441-473`、`server.rs:483` |
| 14 | `RequestControl` 是人工、TTL、节点/分区级数据面闸门;`/admin/**` 与 `/metrics` **刻意豁免**(否则做出"无法远程恢复"的节点) | `crates/store/src/request_control.rs:60-82` |
| 15 | N4 已**锁定**"每盘一个小线程池"为架构目标 | `cluster.md` §9 待拍项 N4(约 `:518`) |
| 16 | 盘 hang 容错被明确判为**集群层**问题 | `openspec/changes/io-index-design/design.md:38` |

> **行号口径**:上表按**已提交版本(HEAD)**给出。工作树里前置 Phase 0 的未提交改动已改写 `crates/store/src/{health.rs,metrics.rs,server.rs}`(见 §1.1),那三个文件的行号会漂移;实现时以代码为准。

> 与 `io-index-design` 的关系:**不冲突**。它说"不要靠把单次 IO 改异步来解决 hang",本 change 做的是"检测 + 隔离 + 换副本",正是它指定的落点。

### 1.1 前置 Phase 0 的落地状态(动手前必读)

工作树里**已有**一版未提交的 Phase 0 实现(`crates/store/src/{health.rs,metrics.rs,server.rs}`),本 change **不重做**它们,直接在其上叠:

| 已落地 | 位置 | 本 change 如何使用 |
|---|---|---|
| 每盘**至多一发**在飞探测:未归则跳过本轮,句柄留在槽位里 | `health.rs`(模块注释 + `StuckProbe`/`SlotAction`) | — |
| **迟到的探测结果被采纳**(`SlotAction::Adopt`) | `health.rs` | 不重做;在其上叠迟滞(D2) |
| 跳过轮**不写 `HealthMap`**(保持上一轮判定,绝不回到 `HEALTHY`) | `health.rs::spawn_prober` | 补另一半:从未观测过 → 显式 `UNKNOWN`(D2) |
| 探测 IO 与 `remove_file` 移出 reactor(单发阻塞任务 + 异步侧超时) | `health.rs::probe_disk_sync` | — |
| 每盘在途请求 gauge + 拿到副本后定位盘的唯一插入点 | `metrics.rs::disk_in_flight`、`server.rs::OpTimer::with_disk` | **D2 闸门挂同一位置;D3 看门狗复用同一口径** |
| 每盘探测卡住/跳过的观测 | `metrics.rs::disk_probe_stuck`/`disk_probe_skipped` | 不重复提指标 |
| 每盘端到端慢请求(含池排队)观测 | `metrics.rs::disk_slow_requests` | 只作观测,**不用作隔离判据**(见 §5.1) |

**还没有的**(= 本 change 的主体):每盘有界池、闸门(本地决策)、看门狗(syscall 计时)、撤离、判活迟滞、每 tick 预算。

## 2. 目标 / 非目标

**Goals**
- 一块盘的 IO 卡死**不消耗**其它盘的执行容量;该盘上的请求**快速失败**而不是排队等死。
- 坏盘对**所有**消费方可见且可执行:数据面、本地 worker、拉复制、(经心跳)master。
- 坏盘上的副本能被**安全地**搬走,且**永不**因为一次抖动就搬。
- 单盘故障**不**升级为"节点死 ⇒ 全节点再复制风暴"。
- 恢复动作**有预算**,且优先用便宜的手段。

**Non-Goals**
- 不改引擎 IO 模型(同步阻塞不变)、不迁 AIO、不做用户态块缓存。
- 不做自动换硬件、不做设备级调优的代码实现(只文档化 + 启动自检提醒)。
- 不引入新的一致性协议 / 跨节点协调。
- 不改磁盘格式、不改数据面成功路径语义。

## 3. D1 每盘有界池(N4 落地)

### 3.1 为什么"有界"必须是**盘所有**(而不是共享池上加信号量)

天真的做法(b)是:在共享 `spawn_blocking` 前面加一个**每盘 `Semaphore`**,限制"同时在跑的该盘操作数"。这挡不住真正的故障:信号量允许 N 个该盘操作**已经进入**共享池,D 状态下它们**永远不会归还**,于是共享池里被这块盘**永久占住** N 个线程;36 块盘最坏 = 36×N。N=8 时是 288,离默认 512 只差一倍——再来第二块坏盘或一段复制高峰,池就满了,**其它盘的健康副本一起被拖死**。也就是说:(b) 只限住了"并发数",没限住"被永久吃掉的线程属于谁"。

因此:**池必须归盘所有**。一块盘的池被卡死线程占满,后果被封闭在那块盘上——这正是 N4 的原意("同盘上的 BlobStore 共享"池)。

### 3.2 两种形态(a vs b),以及取舍

| 形态 | 做法 | 评价 |
|---|---|---|
| **(a) 每盘一个专属 tokio runtime / 线程池** | 每盘 `Builder::new_multi_thread().worker_threads(1).max_blocking_threads(N)` 起一个 runtime,提交走该 runtime 的 `spawn_blocking`;每盘线程命名 `disk-<id>-*` | **推荐**。复用现有 `spawn_blocking` 心智模型与 `JoinHandle` 语义;线程命名/`tokio::runtime::Runtime` 的 metrics 都能直接看;`max_blocking_threads` 天然就是"有界"。代价:每盘一个常驻 runtime(哪怕 1 个 worker 线程),且 `shutdown_timeout` 对 D 状态线程**无效**(见 §5.3) |
| **(b) 每盘 `Semaphore` + 共享池** | 共享 `spawn_blocking` + 每盘信号量 | **否决单独使用**(理由见 3.1)。可作为过渡,但必须同时把 `max_blocking_threads` 提到 `Σ每盘槽位 + 控制面余量`,否则"有界"是假象 |
| **(b') 每盘自建线程池(N 个专属线程 + 有界队列)** | 自建 `DiskPool`:`N` 个 `std::thread` + 有界 channel + 快失败 | 与 (a) 等价但更直白,不依赖 tokio 内部语义;代价是自己管线程生命周期与 `JoinHandle` 等价物 |

**取 (a)/(b') 同一条路:池归盘所有**;具体是 (a) 还是 (b') 留实现时按"是否愿意每盘一个 runtime"决定(见 Open Questions)。无论哪种,**槽位数 = `store_server.disk_pool_size`,队列 = 有界且不等待**。

### 3.3 谁进池、谁不进

**必须**走该盘池的:数据面全部引擎调用(put/get/delete/stat/scrub-report/verify/replica-metadata/segments)、`worker.rs` 的 flush 与 compaction、`replication.rs` 的 `sync_round`(它读本地 log 也写本地盘)、`heartbeat.rs` 的 `ensure_replica_with_origin`/`drop_replica`(建/开/关副本 = 碰盘)、`health.rs` 的探测 IO(卡死的探测线程也应记在那块盘的账上)。

**不进池**的:纯控制面网络(gRPC 心跳发送/接收)、内存态读写(`replica_states()`)、`/metrics`、`/admin/**` 的路由本身。

**`statfs`(容量)单独处理**:它也可能被 D 状态卡住。规则是——容量刷新经该盘池提交并**带超时**;拿不到新值就上报**上一拍的已知值**(绝不上报虚构的 0),且**不因为"池满"就判该盘坏**(重负载下池满 ≠ 盘坏,反而"池满"已由 §3.4 快失败处理)。这条区分很重要:`UNAVAILABLE` 只能由**探测/看门狗**得出,不能被正常排队现象污染。

### 3.4 池满 = 快失败,不排队

- 池满(槽位 + 有界队列都满)⇒ **立即 `503`**,reason = `pool_full`,**不进入引擎、不缓冲 body**(与 `RequestControl` 的 gate 同一位置、同一原则)。**绝不**无界排队:排队会把"慢"变成"雪崩",而且对一个已经卡死的盘,排队 1000 个请求毫无意义。
- 该盘的**后台任务**池满 ⇒ 跳过本轮(计 `disk_worker_skipped_total{task}`),不阻塞也不重试堆叠;下个周期自然重来(机制幂等)。

## 4. D2 健康闸门

### 4.1 状态值域与上报规则(未知 ≠ HEALTHY)

`HealthMap` 的值从"一个 `DiskHealth` 枚举"扩为**每盘状态结构**:`DiskHealth`(最近一次的**轮次判定**)+ `available: bool`(迟滞判定后的**可服务性**,这是闸门真正用的)+ 迟滞计数(坏/好连击、进入/退出时刻)+ 最近一次有效观测的时刻。

**关键区分:原始轮次判定 ≠ 可服务性**。今天 `DiskBeat.health` 带的是**单轮**判定;迟滞判定出的 `UNAVAILABLE` 是**派生态**。两者必须分开,否则 D2 一上线就会变成"单次探测超时即摘盘"(而今天单轮判定是无害的,因为没人用它做决策)。

上报值域(`DiskBeat.health`,string,proto 不用改):`HEALTHY` / 具体故障枚举(`WriteTimeout`…)/ **`UNAVAILABLE`**(迟滞判定后的隔离态)/ **`UNKNOWN`**(**从未有过**有效观测:刚启动、探测未启用)。

硬规则两条:

1. **无观测绝不报 `HEALTHY`**。今天的 `unwrap_or_default()` 把"没探过"变成空串,空串在 master 侧又被当成"可用"——两处叠加就是"从没检查过 = 健康"。改为显式 `UNKNOWN`。
   - **前置 commit 已完成这一半**:`spawn_prober` 跳过轮**不写 `HealthMap`**(该盘保持上一轮的坏值,绝不因"这轮没探到"而回到 `HEALTHY`),并计 `store_server_disk_probe_stuck` / `store_server_disk_probe_skipped_total`。本 change 只需补**另一半**:把"**从未有过**观测"那一格从空串改成显式 `UNKNOWN`(取代 `disk_beats` 的 `unwrap_or_default()`)。
2. **迟到的探测结果要采纳**。上一轮超时的那次探测**仍在飞**(线程卡着),它一旦返回(盘缓过来了),结果必须**记成该盘的新观测**,而不是被"本轮已经用新值覆盖了"而丢弃。否则一块抖动中的盘会永远停留在坏态。
   - **前置 commit 已实现**(`health.rs` 的 `StuckProbe` 槽位 + `SlotAction::Adopt`:留住 `JoinHandle`,下一轮若发现其结果已就绪即采纳,不重探)。本 change **不重做**这件事,只在其上叠迟滞。

### 4.2 迟滞(防抖是硬需求,不是优化)

| 方向 | 判据 |
|---|---|
| 进入 `UNAVAILABLE` | **连续 N 轮**坏(`disk_health_bad_rounds`,提案 3)**,或**坏态持续 ≥ T(`disk_health_bad_secs`,提案 30)——两者取**先到**者 |
| 退出(恢复 `available`) | **连续 M 轮**好(`disk_health_good_rounds`,提案 3)**且**稳定满 T2(`disk_health_recover_stable_secs`,提案 120) |
| 混合轮次 | 坏态期间出现好轮:计数**不归零**,而是要求"连续 M 轮好 + T2 稳定"才复位(避免坏盘与好盘交替抖动把状态刷来刷去,同 `server.rs:441-473` "只置不回"的防抖精神) |

**为什么取"先到"**:若只按轮数,探测周期一调大(比如 300s),"连续 3 轮"就是 15 分钟——一块真死的盘要 15 分钟才被隔离,不可接受;若只按时间,周期一调小又等于没有防抖。两个维度都卡住才两头不漏。

### 4.3 闸门作用面

> **⚠️ 落地差异(实现时改了落点,理由如下)**:闸门最终落成**数据面中间件**(挂在数据面子 router 上),
> 而不是本节建议的"`with_disk` 同一位置"。原因:① `scrub_report`/`verify` 两个 handler **根本**
> 不调 `with_disk`;② 本节要求"复制读 / `replica-metadata` / `segments` 一并被闸",而
> `request_control::classify` 恰恰**豁免**这三类 —— 挂在 `with_disk` 位置覆盖不到它们。
> 中间件用**同一个** `ReplicaManager::get(pid)` 定位盘(与 `with_disk` 同源,没另起寻盘逻辑),
> 覆盖 `/partitions/{pid}/**` **全部**路由;豁免(`/metrics`、`/admin/**`)靠**路由结构**保证(中间件只挂在
> 数据面子 router 上),并额外显式判一次路径前缀防后人把路由合并回去。副产品:闸门在 extractor 之前
> 短路 ⇒ 被拒请求**连 body 都不读**。详见 [`store.md` §12.12](../../../docs/design/store.md)。

- **数据面**:入口解析到分区 → 找到本地副本 → 查 `disk_id` → `available == false` ⇒ **`503` reason = `disk_unavailable`**,不进引擎。
  - **落点已经现成**:前置 commit 已经给出"拿到副本句柄 → 定位盘"的唯一插入点(`server.rs` 的 `OpTimer::with_disk(&handle.disk_id)`,每个 handler 恰一次,`put`/`get`/`metadata`/`replica_metadata`/`segments` 等均已接上)。健康闸门就挂在**同一位置**——`with_disk` 拿到盘之后、提交引擎之前。**不要**另起一套寻盘逻辑。
- **与 `RequestControl` 的关键区别(必须写清)**:`RequestControl` 是**人工**闸门,其豁免清单(`?replication=1`、`replica-metadata`、`segments`)是为了"别把自己变成收不到数据的孤岛"。**盘健康闸门不沿用该豁免** —— 从一块已经坏了的盘上拉数据/拉段本来就不可能成功,只会再占一个卡死的线程、再漏一个。所以复制读、`replica-metadata`、`segments` **一样被 503**(对端收到 503 后按既有语义"这轮拉不到"处理,`replica.rs` 侧保守留在 `BOOTSTRAP`,永不被误判为追平——方向是安全的)。
- **仍然豁免**:`/metrics` 与**任何 `/admin/**`**。理由与 `request-control` 的非目标 #2 一模一样:**不能做出一个无法远程恢复的节点**;盘坏了运维更要能看见、能下发指令。
- **本地 worker**:`worker.rs` 遍历 replica 时跳过 `available == false` 所在盘(否则每轮都会往卡死的盘上再塞一个线程)。
- **拉复制**:`replication.rs` 跳过该盘上的 pid(既做源也做目标都跳过),但**不动游标**(恢复后从原游标续拉,与 `/admin/replication-control` 的暂停语义一致)。
- **心跳**:如前(#1 事实 8),`UNKNOWN`/`UNAVAILABLE` 如实上报,不美化。

### 4.4 master 侧对新值域的处理(必须一起改,否则默认部署被打死)

`placement.rs:76` 现在是"非空且 ≠ HEALTHY 就不放新副本"。若 store 开始上报 `UNKNOWN`,**默认关探测的部署会立刻一块盘都放不了**。故:

- **放新副本**可用集合 = `""`(旧节点)| `HEALTHY` | **`UNKNOWN`**;明确故障值与 `UNAVAILABLE` ⇒ 不放(现有行为扩展,不变)。
- **撤离(D4)只认 `UNAVAILABLE`** 这一个值——它已经是 store 侧迟滞的产物,单轮故障枚举**不足以**触发撤离。

## 5. D3 IO 看门狗

### 5.1 计时必须在阻塞闭包**内部**

> **⚠️ 落地时的两处加强(均已实现并有单测)**:① "最久在途"不是"最早的开始时刻"这一个标量,而是每盘
> 一个 `BTreeMap<开始时刻, 条数>` **多重集** —— 任务完成顺序与开始顺序无关(A 先开始、B 后开始、
> A 先结束),标量会把健康盘的"最久在途"越算越大直到误判;② 时长观测做成**守卫**而非"函数尾部一行"
> ⇒ 不变量:**每个真正开跑的 job 恰好产生一个时长样本**(panic 也照记),直方图能与"准入后在飞数"对上。
> 另:排队中的任务**不盖戳**,故排队既不进"在途"也不进时长(这正是本节要防的那件事)。
> 详见 [`store.md` §12.12](../../../docs/design/store.md)。

**不能用 async 侧墙钟**衡量一次 IO:那段时间包含"在池里排队"的时长——盘正常但池忙时,一个 5ms 的读会被量成 2s,于是健康盘被误判成坏盘。计时点必须**在闭包里、紧贴 syscall 前后**:`let t0 = Instant::now(); let r = engine_op(); let dt = t0.elapsed();`。测的是**这个盘自己的服务质量**,不是"有没有排上队"。

### 5.2 触发与后果

每盘需要两样东西:**在途条数**(已有 `disk_in_flight` 口径,见下)与**最长在途时长**(由该盘池在每次提交时盖时间戳、闭包内更新,看门狗来读)。看门狗是一个**独立于该盘池的轻量任务**(否则它自己会被卡死),按 `disk_io_watchdog_ms`(提案 30000,需 **远高于** p99.9 与 compaction 的大 IO 时长)检查:

> **看门狗不受 `disk_health_enabled` 约束**:主动探测可以关(对齐 Ambry),但看门狗**不能**跟着一起关——默认关探测的部署才是绝大多数,若看门狗也被关,`UNAVAILABLE` 就再也没有第二来源,D4/D6 整条链路在默认配置下全哑。

- **在途条数的把手已经现成**:前置 commit 已加 `store_server_disk_in_flight_requests{disk_id,op}`(gauge)与 `OpTimer::with_disk` 的 inc/dec。看门狗**复用这一个口径**(按盘聚合"在途只增不减"),**不要**再造一个平行的在途计数器——两个口径迟早会对不上。覆盖面:数据面在途看这个 gauge;后台(worker/复制/生命周期)的池占用看 §3 的池自身水位。
- **但**"在途**时长**"必须来自**闭包内**的计时(该盘池提交时盖时间戳),gauge 只能告诉你"有多少在途",告诉不了"最久的那个已经多久没回来了"。
- 超时 ⇒ 记 `store_server_disk_io_watchdog_trips_total{disk_id}`、**置 `UNAVAILABLE`**(走 §4.2 的进入判据)、后续请求按 §4.3 快失败、随心跳上报 master;
- **与既有慢请求观测的分工(已被前置 commit 写清)**:`store_server_disk_slow_requests_total{disk_id,op}` 的口径是**端到端(含阻塞池排队)**,它自己那条注释就把"真正的盘慢(syscall 耗时)"指给了本看门狗这组指标。**别混用**:用端到端口径做隔离判据会把"池忙"判成"盘坏"。
- **诚实边界**:已经进到 D 状态的那次 IO **无法取消**,"让在途请求快速失败"**只对排队/后续请求成立**,对已经卡在 syscall 里的那一个不成立——Rust 里既不能取消 blocking 任务,也不能 kill 线程。这必须写在文档和指标语义里,不能假装做到了。
- 该盘池里**已排队的请求**在盘被判定坏掉后应被丢弃(快失败),不要等一个已知不会好的盘。

### 5.3 逃生舱:真挂死 ⇒ 进程自杀 + supervisor 重启

**前提诚实化**:内核永不返回(D 状态)时,进程内**没有任何**回收手段——`tokio` 的 shutdown 不杀 blocking 线程,`pthread_cancel` 对 D 状态无效,Rust 没有"杀线程"的 API。所以"泄漏"在有/无看门狗的情况下都会发生;**看门狗不解决泄漏,它把"泄漏 + 无人知晓"变成"泄漏 + 已知 + 有界决策"**。

于是要给一个**明确的终点**:当某盘池**100% 被超期操作占住**且该状态**持续 ≥ T** 时(`disk_watchdog_escalate_secs`;**默认 0 = 永不升级**,开启时建议 ≥600),说明这块盘永远不会回来了 ⇒ **`std::process::abort()`**,交 supervisor(`systemd Restart=always` / `docker-compose restart: always`)重启进程。

- 对齐:Ceph `osd_op_thread_suicide_timeout`(per-OSD 进程自杀 + 重启)。
- **偏离与代价(必须记录)**:antfs 是**单进程多盘**(Ceph 是一 OSD 一进程),自杀会**带走健康盘上的服务**。所以:(a) 该动作**默认关**(`disk_watchdog_escalate_secs = 0` = 不升级)**先观察再开**;(b) 门槛取得极高(池 100% 卡死 + 持续 T),宁可漏(靠运维)也不误(误杀一次等于自造一次全节点故障);(c) 它是"两害相权"的终局手段,不是"顺手加个重启"。
- 重启的**代价是可控的**:数据 fsync 边界 + 多副本 + 拉复制会把节点拉回来;而**不重启**的代价是一台慢慢烂掉的节点(线程被吃光、所有盘一起停),更贵。

## 6. D4 从坏盘撤离(master 侧)

**问题**:`placement.rs:76` 让新副本不落坏盘,但**已存在**的副本无人处理——坏盘上的副本会一直"假装健康"地占用 RF 配额,分区实际可用副本数已经掉了。

**做法:复用 M4 的 evacuation,只把判据从"节点"换成"盘"**

`reconcile.rs:166-198` 的 `plan_evacuation` 现在已经做对了"先补后删":含抖出目标的分区,健康副本 `< rf` ⇒ `Add`;`≥ rf` ⇒ 对每个抖出副本产 `Remove`。把它推广为"**抖出集**是 `(node, disk)` 集合,而非只有下线节点"——`decommission` 就是这个集合的特例(该节点所有盘都进集合),因此这**不是**新写一个平行函数,而是把已有正确闸门的作用域放大。这也顺带保证:

- **先补后删**:`Add` 先行;`Remove` 只在健康副本 ≥ RF 时产出。
- **删源闸门复用**:真正的删除走既有的 `plan_move`(`reconcile.rs:200+`,要求**继任者已 `ACTIVE`** 且存活副本 ≥ RF)——新副本还在 `BOOTSTRAP`(追平中)时**绝不删源**,守 §3.1。
- **RF=1 永不删**:`healthy >= rf` 在 `rf=1` 且源仍在时恒不成立 ⇒ 单副本分区**只会补、不会删**,这正是我们要的(哪怕 `Add` 一直补不上,也绝不先删)。

**限制与护栏(逐条)**

| 护栏 | 做法 |
|---|---|
| **持续证据** | 只对 `UNAVAILABLE` 且已持续 ≥ `master.disk_evac_min_bad_secs`(提案 300)的盘动手。单轮故障枚举 ⇒ **不动** |
| **预算/限速** | `master.disk_evac_max_in_flight`(提案 1,对齐 `balance_max_in_flight = 1` 的风格)限制同时在途的撤离分区数——撤离是**真搬 GB 数据**,不能一 tick 全开 |
| **可跳过** | 撤离动作必须放在 `server.rs:483` 的 `if maintenance { continue; }` **之后**——维护窗口换盘正是要靠它兜住,自动动作此时应当退出(与"盘将满转 RO"刻意放在维护检查**之前**的理由相反:那是止损,这是恢复) |
| **开关** | `master.disk_evac_enabled`(提案 **false**):新写的自动搬迁动作,与 `auto_grow_enabled = false` 同一保守风格,运维显式打开 |
| **回填不做** | 盘恢复可用后**不自动回填**副本(回填交给既有均衡 `StartBalance` 或运维);理由同 `server.rs:441-473` 的"只置不回"——抖动即回会把写流量又引回一块刚缓过来的盘 |
| **不删最后一份** | 由上面的 `healthy >= rf` + `plan_move` 双重闸门保证,并配测试 |

**一个已核实的简化**:placement 按**节点**去重(`server.rs:488-497` 的 `exclude` 含该分区所有已承载节点),故一个分区在一台机上**最多一个**副本 ⇒ 盘级抖出落到动作层仍然只需要 `EvacAction::Remove(pid, node)`,**不必**给 `EvacAction` 加 disk 维度。

## 7. D5 判活与健康解耦

**现状的两处耦合**

1. **心跳任务自己做盘 IO**:`heartbeat.rs:160-161` 在异步任务里 `dm.refresh()`(全盘 `statvfs`,同步)。一块盘 D 状态 ⇒ 心跳发不出去 ⇒ master 判节点死。**(前置 commit 已落一半:`dm.refresh()` 换成逐盘 `statvfs` 的单飞任务(`spawn_refresh`/`collect_refresh`),既出 reactor 又不叠加漏线程;本 change 补的是上游语义 —— 刷新拿不到新值时该**如实上报**(`UNAVAILABLE`/`UNKNOWN`),而不是像现在这样沉默沿用旧值。)**
2. **判活是单窗口无迟滞**:`soft.rs:147` 只看 `last_beat.elapsed() <= heartbeat_timeout_secs`(15s),而 store 每 3s 一拍。一次 GC/调度停顿、一次 `statvfs` 卡顿就能让它跨过阈值。

**改法**

- (Phase 0)盘 IO 移出 reactor + 带超时;**心跳周期任务在任何情况下都必须发出一拍**:拿不到新容量就报**上一拍的已知值**,并把该盘标成 `UNAVAILABLE`/`UNKNOWN`。**心跳的可用性不得依赖任何一块盘的健康**——这是"控制面不被数据面拖死"的最小要求,也是 D5 的全部要义。
- 判活加迟滞:`master.node_down_debounce_ticks`(提案 3)——节点被判定 down 需要**连续 K 个 reconcile tick**(K×5s = 15s)都观察到过期,叠加原有的 15s 超时 ⇒ **有效判死窗口从 15s 拉长到 ≈30s**。3s 心跳在这段窗口内**本应有 10 拍**,少一两拍不再等于死亡。
- 语义不变的部分:节点真的死了(进程没了/机器没了),30s 内照样会被发现并触发再复制——**只是不再把"抖动"当"死亡"**。这与 `reconcile_startup_grace_secs = 15`(切主后先养软状态再做失败驱动再复制)是同一思路的第二个实例。

## 8. D6 恢复动作要有预算(以及"从便宜到贵"的阶梯)

**今天的问题**:`server.rs:425-436` 把**所有**欠副本分区塞进 `add_targets`,`486-505` 逐个 `try_add_replica`。一个节点判死 ⇒ 它承载的所有分区在一 tick 内**全量**发起再复制,没有速率上限(只有"补不上"的失败标记)。扇出量 = 该节点承载的分区数,可能是几万个。

**阶梯(先用尽便宜的)**

| 档 | 动作 | 成本 | 现状 |
|---|---|---|---|
| ① 停新放置 | 坏盘不再接新副本 | **免费** | 已有(`placement.rs:76`) |
| ② 客户端绕开 | router 按 W/R 语义换副本重试 | 低(一次重试) | 已有(数据面 503 + 多副本) |
| ③ 撤离/再复制 | 在别处补副本、删源 | **贵**(GB 级网络 + 盘写) | 本 change 新增(D4) |
| ④ 换硬件 | 运维换盘 | 最高(人 + 物料) | 非代码 |

**预算**:`master.rereplication_max_per_tick`(提案 4)限制**每 tick 新发起**的再复制分区数;`add_targets` 按 `PartitionId` 排序后取前 K(确定性,便于复现),其余下 tick 继续;被压下的数量暴露成 gauge(`master_rereplication_deferred`),让运维**看得见**"我们正在限速"而不是"系统不动了"。

**误判代价不对称(本 change 的决策依据)**

> 多留一份副本 ≈ 占点容量(几乎免费);一次不必要的迁移 = 真金白银的 GB 网络 + 盘写,还可能让新副本与源副本一起进入追平状态。

所以**所有**防抖手段(迟滞、持续证据、每 tick 预算、`maintenance` 跳过)都是**同一偏向**的体现:**宁可迟,不可早**。这条要写进实现者的取舍依据里:任何"要不要现在动手"的争论,默认答案都是"再等一轮"。

**注意(实现细节,别搞错)**:今天的 `for pid in add_targets { try_add_replica(...).await }` 是**串行**的(并发度 1,但没有**速率**上限)。所以这里要的是**每 tick 数量上限**,不是 `max_in_flight` 那种信号量;两者名字相近但意义不同,不要照抄 `balance_max_in_flight` 的语义。

## 9. D7 内核 fail-fast 是前提(部署要求,不是代码)

**为什么是前提**:D2 的进入判据来自"探测失败",D3 的进入判据来自"超时",而 **D1/D3 的最终逃生舱是进程自杀**。如果设备层永远 hang:

- 引擎自关(`io_error_count_to_trigger_shutdown`)**永远等不到错误** ⇒ 被动链路全哑;
- 看门狗只能看到"没返回",**拿不到 errno** ⇒ 只能超时隔离,而隔离的代价是该盘请求**全部 503**、线程**仍然泄漏**;
- 于是整条阶梯变成"把盘摘掉但永远补不回来 + 线程继续漏",只剩自杀一条路——这显然不是我们想要的默认终局。

**部署前提清单(写入文档,作为上线检查项)**

| 层 | 配置 | 要点 |
|---|---|---|
| SCSI | `/sys/class/scsi_device/*/device/timeout` | 设 30s,让 IO 以 **EIO** 返回而非永久 D 状态(默认值随内核/发行版浮动,需显式落盘) |
| NVMe | `nvme_core.io_timeout = 30`、`nvme_core.max_retries = 5` | 同上;重试次数决定"多久才放弃" |
| dm-multipath | `no_path_retry = N` | **绝不用 `queue_if_no_path`**:那正是"无界排队 = 永久 hang"的教科书例子,与本 change 的"池满快失败"精神正好相反 |
| NFS(若把 `data_dir` 放在网络盘) | `timeo=` / `retrans=` / 必要时 `soft` | 网络盘的 hang 语义与本地盘不同,必须显式设超时 |
| antfs 引擎 | `[store] io_error_count_to_trigger_shutdown = 3..10` | **建议部署显式打开**(默认 0 = 关,见 CLAUDE.md §5.1:不擅改默认)。打开后"设备层 EIO → 连续错误 → 引擎自关 → hook → replica `ERROR` → 心跳上报 → master 再复制"这条**已有**闭环才真正工作 |

**代码侧只做一件小事**(便宜且不越权):启动时若 `io_error_count_to_trigger_shutdown == 0`,记一条 `tracing::warn!` 提示"盘 hang 只能靠超时隔离,建议开启引擎自关",**不改默认值**。

## 10. 配置项清单(CLAUDE.md §7 必须项;本 change 为**清单**,实现时落进 `docs/design/*.md`)

> 状态列说明:**新增** = 本 change 引入;**修改** = 既有项语义/文档变化(默认值不动);**部署建议** = 不改代码默认,要求运维显式设置。
> 所有标 **【提案】** 的默认值**待标定/待决策**,不得在实现时"顺手定一个"就当锁定的默认——CLAUDE.md §5.1:**不擅自改/定默认配置值**。

### 10.1 `crates/store/src/config.rs`(`StoreServerConfig`,TOML `[store_server]`)

| key | 类型 | 默认值 | 取值/校验 | 含义/影响 | 对齐 Ambry |
|---|---|---|---|---|---|
| ~~`store_server.disk_pool_size`~~ → **已落地为三项** | — | — | — | 每盘池**已经实现**(见 `docs/design/store.md` §12.11):`data_dirs` 每项可带 `pool_size`(**按盘**指定),未指定者用 `pool_default_size`(默认 **16**),另有 `pool_queue_size`(默认 32)与 `pool_acquire_timeout_ms`(默认 200)。**默认值仍是经验值,待压测标定**(§12 风险 5) | 无 |
| `store_server.disk_watchdog_ms` | u64 | **【提案】30000** | ≥1 | 单次引擎 IO 的**系统调用时长**软上限(闭包内测,不含池排队);超时 ⇒ 该盘 `UNAVAILABLE`。须远大于 p99.9 与 compaction 大 IO 时长,否则误判健康盘 | 无 |
| `store_server.disk_watchdog_escalate_secs` | u64 | **【提案】0(= 不升级)** | ≥0 | 某盘池 100% 被超期操作占住且持续此秒数 ⇒ `process::abort()` 交 supervisor 重启。**单进程多盘,误杀代价 = 全节点故障**,故先观察后开(见 design §5.3) | 无(思路对齐 Ceph `osd_op_thread_suicide_timeout`) |
| `store_server.disk_health_bad_rounds` | u64 | **【提案】3** | ≥1 | 连续 N 轮探测失败才置 `UNAVAILABLE`(迟滞) | 无 |
| `store_server.disk_health_bad_secs` | u64 | **【提案】30** | ≥1 | 与上者取**先到**:坏态持续 ≥ T 也置 `UNAVAILABLE`(防探测周期调大后隔离太慢) | 无 |
| `store_server.disk_health_good_rounds` | u64 | **【提案】3** | ≥1 | 连续 M 轮健康才考虑复位 | 无 |
| `store_server.disk_health_recover_stable_secs` | u64 | **【提案】120** | ≥0 | 复位还要求"稳定满 T2" | 无 |
| `store_server.disk_health_enabled` | bool | false(**不动**) | — | **修改(语义)**:探测从"纯观测"变为**闸门的输入**。因此文档必须写明:默认关 ⇒ **健康闸门不生效**(只有看门狗仍在);并新增指标/日志让"闸门未启用"可见。实现时评估是否需要在 `false` 时记录一条启动 `warn` | `store.io.error.count...` 同级(Ambry 探测亦默认关) |
| `store_server.disk_health_interval_secs` | u64 | 60(**不动**) | ≥1 | **修改(语义)**:与 `disk_health_timeout_secs` 及迟滞参数**联合**决定隔离延迟。文档须给出公式:`最坏隔离延迟 ≈ bad_rounds × interval`(与 `bad_secs` 取小)。另:探测**逐盘串行**,`盘数 × timeout` 超过 `interval` 时轮次会追尾(Burst),文档须点明并建议 `interval ≥ 盘数 × timeout` | 无 |
| `store_server.disk_health_timeout_secs` | u64 | 10(**不动**) | ≥1 | 同上。**它已被两处复用**:`replication.rs:861`(拉复制 HTTP timeout)与前置 commit 的 `server.rs` 慢请求观测阈值(`disk_slow_requests`)。改它**会连带影响复制与慢请求口径** —— 文档必须写明这一点,或实现时评估是否拆出独立配置(**不擅改默认**) | 无 |
| `store_server.replication_max_batch_bytes` 等 | — | 不动 | — | 本 change 不改复制参数 | — |

### 10.2 `[store]`(`crates/blob/src/config.rs`;引擎侧,不新增字段)

| key | 类型 | 默认值 | 取值/校验 | 含义/影响 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store.io_error_count_to_trigger_shutdown` | u32 | 0(= 禁用,**不动**) | 0 = 禁用,≥1 = 阈值 | **部署建议 3–10**;设备层 fail-fast 打开后,这是"错误驱动"链路的总开关。**不改默认** | `store.io.error.count.to.trigger.shutdown`(Ambry 默认 `Integer.MAX_VALUE`,同样等于关) |

### 10.3 `crates/master/src/config.rs`(`MasterConfig`,TOML `[master]`)

| key | 类型 | 默认值 | 取值/校验 | 含义/影响 | 对齐 Ambry |
|---|---|---|---|---|---|
| `master.node_down_debounce_ticks` | u64 | **【提案】3** | ≥0 | 判节点 down 需连续 K 个 reconcile tick 都观察到心跳过期。有效窗口 ≈ `heartbeat_timeout_secs + K × reconcile_interval_secs`(默认 15 + 15 = 30s)。**0 = 保持今天的行为** | 无 |
| `master.rereplication_max_per_tick` | u64 | **【提案】4**(对齐 `auto_grow_batch` 的护栏量级) | ≥1 | 每 tick 新发起的再复制分区上限(按 `PartitionId` 排序取前 K)。今天无上限 ⇒ 一个节点判死=一 tick 全量补齐 | 无 |
| `master.disk_evac_enabled` | bool | **【提案】false** | — | 坏盘撤离总开关。新写的自动搬迁动作,保守默认关(同 `auto_grow_enabled`) | 无 |
| `master.disk_evac_min_bad_secs` | u64 | **【提案】300** | ≥1 | 盘为 `UNAVAILABLE` 需持续这么久才允许撤离(**持续证据**);单轮故障枚举永不触发 | 无 |
| `master.disk_evac_max_in_flight` | u64 | **【提案】1** | ≥1 | 同时在途的撤离分区数(撤离是真搬数据,须限流;风格对齐 `balance_max_in_flight = 1`) | 无 |
| `master.heartbeat_timeout_secs` | u64 | 15(**不动**) | ≥1 | **修改(语义,文档)**:不再是唯一判活判据,需与 `node_down_debounce_ticks` 联合描述 | — |

### 10.4 明确**不**引入的旋钮(记录"想过但否决")

- **per-disk 池大小覆盖**(异构盘才需要):先行单一全局值,等真有异构盘再加(CLAUDE.md §1 简单优先)。
- **`unknown_is_healthy` 开关**:「未知 ≠ 健康」是**规则**,不是可配置项。
- **探测"单飞"开关**:Phase 0 无条件做,不给关的余地。
- **每盘独立看门狗 deadline**:先全局一个值;按 op 类型分阈(compaction 大 IO vs 普通读)留 Open Question。

## 11. 监控项清单(CLAUDE.md §7 必须项;实现时落进 `docs/design/store.md` §11/§12 与 `docs/design/cluster.md` 表)

### 11.1 store 侧(`StoreServerMetrics`,**已有**——前置 commit 落地,本 change 不再重复提)

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server_disk_in_flight_requests` | gauge | `disk_id`,`op` | 个 | **每盘**在途数据面请求(与全局 `in_flight` 同口径 + 盘维度);某盘"只增不减" = 该盘出事最直接的信号。**D3 看门狗复用此口径** | 无 |
| `store_server_disk_probe_stuck` | gauge | `disk_id` | — | 该盘有一发探测线程**卡在设备里未归**(1 = 卡住);仅 `disk_health_enabled` 时可能为 1 | 无 |
| `store_server_disk_probe_skipped_total` | counter | `disk_id` | 次 | 因上一发探测未归而**跳过**的累计轮次(× 周期 ≈ 该盘卡住时长)= Phase 0 单飞的观测面 | 无 |
| `store_server_disk_slow_requests_total` | counter | `disk_id`,`op` | 次 | 端到端(handler 入口→引擎完成,**含池排队**)超过 `disk_health_timeout_secs` 的请求数。**纯观测口径,不得用作隔离判据**(会把"池忙"判成"盘坏") | 无 |
| `store_server_disk_requests_total` | counter | `disk_id`,`op` | 次 | 落在该盘上的数据面请求数(rate = 该盘 IOPS);**请求级**且只覆盖数据面,引擎内部 IO 随 D1 归口后再计入 | 无 |
| `store_server_disk_bytes_total` | counter | `disk_id`,`op` | 字节 | 该盘上**成功**请求的字节数(rate = 该盘带宽;失败/早退不计) | 无 |
| `store_server_disk_request_duration_seconds` | histogram | `disk_id`,`op` | 秒 | 该盘上请求的**端到端**延迟(与 `disk_slow_requests` 同一把尺子);与 4.4 的闭包内 `disk_io_duration` **刻意区分** | 无 |

### 11.2 store 侧(本 change 新增)

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `store_server_disk_pool_in_use` | gauge | `disk_id` | 个 | 该盘池当前占用槽位(含队列) | 无 |
| `store_server_disk_pool_available` | gauge | `disk_id` | 个 | 该盘池剩余可用槽位(标定池大小用) | 无 |
| `store_server_disk_io_duration_seconds` | histogram | `disk_id`,`op` | 秒 | **闭包内**测得的单次 IO 时长(syscall,**不含**池排队)——"这盘到底快不快"的唯一正确口径(与 11.1 的端到端口径刻意区分,见 §5.1) | 无 |
| `store_server_disk_io_watchdog_trips_total` | counter | `disk_id` | 次 | 看门狗判定超时的次数 | 无 |
| `store_server_disk_health_state` | gauge | `disk_id`,`state` | — | 该盘当前状态(1/0),`state` ∈ `HEALTHY`/`UNAVAILABLE`/`UNKNOWN`/具体故障值 | 无(Ambry 只有 `DiskHealthStatus` 上报) |
| `store_server_disk_health_transitions_total` | counter | `disk_id`,`from`,`to` | 次 | 迟滞状态迁移次数(非零正常,频繁迁移 = 阈值定得太紧) | 无 |
| `store_server_disk_worker_skipped_total` | counter | `disk_id`,`task` | 次 | 因盘不可用/池满而跳过的后台工作(`task` = `flush`/`compaction`/`replication`) | 无 |

### 11.3 store 侧(既有指标**修改**,需同步改文档)

| 指标名 | 变化 |
|---|---|
| `store_server_rejected_requests_total` | label `reason` **新增取值** `disk_unavailable`(健康闸门短路)与 `pool_full`(池满快失败);与既有 `disabled`(`RequestControl`)并列。文档须写清三者的区别:**人工静默 / 盘不可用 / 池满** |
| `store_server_request_duration_seconds` | 语义微调:在途时长现在**包含**该盘池排队(引擎 IO 与本地 worker 共用该盘池后的新事实),文档须注明;**不**加 `disk_id` label(基数) |

### 11.4 master 侧(`MasterMetrics`,新增)

| 指标名 | 类型 | 标签 | 单位 | 含义 | 对齐 Ambry |
|---|---|---|---|---|---|
| `master_disk_evac_planned_total` | counter | `result`(`planned`/`unsatisfiable`) | 次 | 坏盘撤离计划生成/无法满足(无空闲盘)次数 | 无 |
| `master_disk_evacuating` | gauge | — | 个 | 在途撤离分区数(受 `disk_evac_max_in_flight` 约束) | 无 |
| `master_rereplication_deferred` | gauge | — | 个 | 因每 tick 预算被推后到下轮的欠副本分区数(让"限速"可见) | 无 |
| `master_node_down_total` | counter | — | 次 | 判定节点 down 的次数(迟滞后;便于确认判活没被盘抖动污染) | 无 |

> 位置:`master.*` 写入 `docs/design/cluster.md` §7.7;**新增/修改的每一行都必须与代码里的注册名、类型、标签逐项对得上**(CLAUDE.md §7)。

## 12. 风险 / 权衡 / 被否决方案

- **[池大小定错 ⇒ 自己造成 503]** → 默认值**必须**压测标定(候选 8 只是起点);`store_server_disk_pool_available` / `store_server_disk_pool_in_use` / `store_server_rejected_requests_total{reason="pool_full"}` 就是给它标定用的。这也是"提案默认值"不写死的原因。
- **[看门狗误判健康盘]** → `disk_io_watchdog_ms` 远大于 p99.9,且**只影响该盘**(不连坐);进入判据还有迟滞 N 轮。实现时先只开"观测+计数"(`escalate=0`、`bad_rounds` 调大),观察一周期再收紧。
- **[进程自杀误杀]** → 门槛极高 + 默认关;见 §5.3 的代价分析。**接受**它是一把"最后的手段",不是常规手段。
- **[健康闸门把整盘摘掉 ⇒ 副本数暂时下降]** → 这正是期望行为:坏盘上的副本**本来**就不可服务,继续发请求只会烧线程。RF 冗余 + D4 撤离负责把副本补回来。
- **[D4 撤离搬数据与正常流量抢带宽]** → `max_in_flight` + `min_bad_secs` + `maintenance` 跳过;宁可慢。
- **[上报值域变化打破默认部署的放置]** → §4.4 明确把 `UNKNOWN` 归入"可放新副本";实现必须同步 `placement.rs` 与其单测,否则是**回归**。
- **[否决:共享池 + 信号量(单独)]** → 见 §3.1,限并发不限归属,卡死线程照样进共享池。
- **[否决:把引擎 IO 改异步/AIO 来绕过 hang]** → `io-index-design` D1 已锁;对本质阻塞的设备,异步不能凭空提速,只会少占线程,而**检测 + 隔离 + 换副本**才是正解。
- **[否决:探测失败单次即隔离]** → 抖动就会摘盘,代价不对称(见 §8)。
- **[否决:坏盘上副本"就地标记 ERROR"了事]** → 只报不改,和今天没区别(master 已经收到了,只是不搬)。
- **[否决:给 `EvacAction` 加 disk 维度]** → §6 已核实一个分区在一台机上最多一个副本,加了也是废字段。

## 13. 验证

**单测(池与状态机,不依赖真挂死的盘)**

- `disk_pool`:槽位满 ⇒ 提交被**立即**拒绝(不排队、不阻塞);拒绝计数 +1;不同盘的池互不影响。
- 看门狗计时取自**闭包内**:构造"提交后排队等待"的场景,断言**排队时长不计入** `disk_io_duration_seconds`(这是本设计最容易写错的一处)。
- 迟滞状态机:坏→好交替的轮次序列不产生状态抖动;`bad_rounds`/`bad_secs` **先到者**触发;复位需 `good_rounds` **且** `recover_stable_secs`;**从未观测 ⇒ `UNKNOWN`,永远不等于 `HEALTHY`**。
- 健康闸门:盘置 `UNAVAILABLE` ⇒ 数据面(含 `?replication=1`/`replica-metadata`/`segments`)**503 + reason=disk_unavailable**;`/metrics` 与 `/admin/**` **仍 200**(与 `request-control` 同一条硬规则)。
- 后台跳过:盘 `UNAVAILABLE` ⇒ `compact_all` / flush loop 跳过该盘 replica;复制跳过该盘 pid 且**游标不被破坏**(恢复后从原游标续拉)。
- master:`plan_rereplication` + 判活迟滞(`node_down_debounce_ticks`)—— 节点短暂静默(1 个 tick)**不**触发全节点再复制;`add_targets` 超限只发 K 个、下 tick 续;撤离计划:坏盘 ⇒ 该分区进 `Add`;继任者未 `ACTIVE` ⇒ **不删**源;`rf = 1` ⇒ **永不**删;`maintenance` ⇒ 计划不执行。

**集成测试(2 store + 1 master 起真集群,用"直接置 `UNAVAILABLE`"当故障注入,避免依赖真 D 状态)**

1. 两块盘,一块置 `UNAVAILABLE` ⇒ 该盘分区请求 503、**另一块盘的 PUT/GET 完全不受影响**(并发的健康盘延迟无显著劣化)。
2. 该盘上的 replica 不出现在 worker 的 flush/compaction 里,也不被复制选中;恢复 `available` 后下一轮自动回归。
3. master 看到 `UNAVAILABLE` 并满足 `min_bad_secs` ⇒ 生成撤离计划 ⇒ 在健康盘补副本 ⇒ 继任者 `ACTIVE` 后才 `DROPPED` 源(两步间断言**任意时刻可用副本数 ≥ 原值**)。
4. 心跳不被盘 IO 阻塞:`refresh` 超时 ⇒ 心跳**照常发出**(带上上一拍的容量 + `UNAVAILABLE`),master **不**判节点 down。

**回归与质量门**:`cargo test` 绿 + `cargo clippy` 无告警(CLAUDE.md §2);`openspec validate store-disk-isolation --strict` 通过。

**标定(实现后)**:`disk_pool_size` / `disk_watchdog_ms` 的默认值由压测产出(健康盘延迟分位 + 池占用),产出的值再回头改代码 `impl Default` 与两张文档表。

## Open Questions

1. ~~**(a) 每盘一个专属 runtime vs (b') 每盘自建线程池**~~ **已拍板 = (b') 每盘自建 OS 线程池**
   (线程名 `antfs-disk-<盘id>-<i>`),已落地:不依赖 tokio 内部语义、线程生命周期与"有界"都是自己的账,
   池析构即线程退出(有单测守着)。见 [`store.md` §12.11](../../../docs/design/store.md)。
2. **池大小是否需要按盘容量/型号分档**:先单一全局值(简单优先),等真有异构盘再议。
3. **看门狗 deadline 是否按 op 分档**:compaction 的大块拷贝天然比一次 4KiB 读慢得多。先全局一个**远大于** p99.9 的值;若误判频发,再按 op 分阈。
4. ~~**`disk_health_enabled = false` 时闸门的退化行为**~~ **已拍板**(见 §5.2):探测可关,**看门狗不可关**;`false` 时闸门只剩看门狗这一条输入。留待实现时只需确认文档与指标标签把"闸门输入源"讲清。
5. **`UNKNOWN` 是否也要参与撤离判据**:当前设计是"只有 `UNAVAILABLE` 触发撤离"。若某部署长期不启用探测,坏盘只能靠看门狗产出 `UNAVAILABLE`——需在实现时核对这条路径确实可达(与 4 相互印证)。
6. **`abort()` 前的最后一步要不要尝试上报 master**:"我要死了,先把我的盘标脏"。实施上不可靠(自杀前的网络往返可能又卡在同一块盘上),但值得讨论是否发一次**不等待**的尽力而为上报。
7. **看门狗是否需要"最长在途"之外的第二个信号**:例如"该盘池队列深度持续贴顶 T 秒"。先只用"最长在途超期"一个信号(误判风险最低),若现网证明不够再补。

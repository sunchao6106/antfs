# store-disk-isolation

单进程多盘数据节点的**盘级故障隔离与处置**(承 `cluster.md` §9 N4):每盘有界池 + 池满快失败、盘健康闸门、IO 看门狗、坏盘撤离、判活与盘 IO 解耦、恢复预算与阶梯、内核 fail-fast 部署前提。
**引擎 IO 模型不变**(仍同步阻塞,见 `io-index-design` D1 与 `store.md` §2);**EC 仍锁定。**

## ADDED Requirements

### Requirement: 每盘有界执行池

数据节点 SHALL 为**每一块盘**提供一个**归该盘所有**的有界执行池,承载该盘上的全部引擎 IO(数据面、本地 worker、拉复制、副本生命周期、健康探测),取代进程级共享的 `tokio::task::spawn_blocking`;同盘上的 BlobStore SHALL 共享同一池(对齐 N4)。池满时 SHALL 立即失败并返回 `503`,**不得**无界排队。

#### Scenario: 池满快速失败而不是排队

- **WHEN** 某块盘的池的槽位与有界队列均已占满,新请求到达
- **THEN** 该请求 SHALL 立即以 `503` 失败,计数器 `store_server_rejected_requests_total{reason="pool_full"}` SHALL 递增,该请求 SHALL NOT 进入引擎、SHALL NOT 缓冲请求体

#### Scenario: 一块盘的池被打满不影响其它盘

- **WHEN** 某块盘的池被完全占满(例如其上 IO 全部挂住)
- **THEN** 其它盘上的数据面请求 SHALL 仍能正常提交与完成,SHALL NOT 因共享池耗尽而排队或失败

#### Scenario: 阻塞池不再承担该盘引擎 IO

- **WHEN** 完成本 change 的实现
- **THEN** 该盘的数据面与后台引擎 IO SHALL 经该盘池提交;`store_server_disk_pool_in_use{disk_id}` / `store_server_disk_pool_available{disk_id}` SHALL 反映其占用与余量

### Requirement: 盘健康闸门

store SHALL 维护每块盘的**可服务性**状态,并把该状态用于本地决策:数据面请求命中不可用盘时 SHALL 短路为 `503` 且**不触碰该盘**;本地 worker 与拉复制 SHALL 跳过不可用盘。`GET /metrics` 与任何 `/admin/**` SHALL 永不被该闸门短路。

#### Scenario: 不可用盘上的数据面请求被短路

- **WHEN** 某副本所在盘被判定为不可用,请求该分区副本的 `PUT`/`GET`/`DELETE`/`replica-metadata`/`segments`
- **THEN** store SHALL 返回 `503` 并计 `store_server_rejected_requests_total{reason="disk_unavailable"}`,SHALL NOT 调用引擎、SHALL NOT 占用该盘池槽位

#### Scenario: 复制读不享受豁免

- **WHEN** 请求携带 `?replication=1` 指向不可用盘上的副本
- **THEN** 该请求 SHALL 与其他数据面请求一同被拒绝为 `503`(**不**沿用 `RequestControl` 的复制读豁免),对端 SHALL NOT 因此判定已追平

#### Scenario: 运维与观测入口保持可达

- **WHEN** 某块盘不可用,请求 `GET /metrics` 或任意 `/admin/**`
- **THEN** 请求 SHALL 正常处理(节点 SHALL 保持可远程观测与可远程恢复)

#### Scenario: 后台工作跳过不可用盘

- **WHEN** 本地 worker 的 flush/compaction 或后台拉复制遍历到位于不可用盘上的副本
- **THEN** SHALL 跳过该副本并计 `store_server_disk_worker_skipped_total{disk_id,task}`,其它盘的副本 SHALL 照常处理;该副本的复制游标 SHALL NOT 被破坏,盘恢复后 SHALL 从原游标续拉

### Requirement: 健康状态迟滞与"未知不等于健康"

store SHALL 用迟滞判定可服务性:仅在**连续 N 轮**探测失败**或**失败持续达到 T 时(两者先到者)标记为不可用;仅在**连续 M 轮**探测健康**且**稳定达到 T2 时恢复可用。**无有效观测时 SHALL 上报 `UNKNOWN` 且 SHALL NOT 视为 `HEALTHY`**;迟到的探测结果 SHALL 被采纳为该盘的新观测。

#### Scenario: 单轮抖动不摘盘

- **WHEN** 某盘仅有一轮探测失败,随后恢复正常
- **THEN** 该盘 SHALL 保持可用,SHALL NOT 产生状态迁移

#### Scenario: 坏轮之间夹好轮不阻止隔离

- **WHEN** 某盘的连续探测以"坏、好、坏、好…"交替,坏态累计达到进入判据
- **THEN** 该盘 SHALL 被标记为不可用(好轮 SHALL NOT 清零坏态计数)

#### Scenario: 从未观测不等于健康

- **WHEN** 某盘尚无任何有效探测结果(刚启动、探测未启用)
- **THEN** 上报的 `DiskBeat.health` SHALL 为 `UNKNOWN`,SHALL NOT 为 `HEALTHY`;本地闸门 SHALL NOT 据此宣称该盘健康

#### Scenario: 跳过探测的轮次保持上一轮判定

- **WHEN** 某盘的上一发探测尚未返回,本轮因此被跳过
- **THEN** 该盘的观测值 SHALL 保持上一轮判定,SHALL NOT 因"本轮未探到"而变为 `HEALTHY`

#### Scenario: 迟到的探测结果被采纳

- **WHEN** 某轮探测超时(其阻塞线程仍卡住)而后续轮次仍在推进,该超时探测随后返回了结果
- **THEN** 该结果 SHALL 被记录为该盘的最新观测,SHALL NOT 被丢弃

### Requirement: 单盘同时最多一个在飞探测

store SHALL 保证对同一块盘的主动探测**同时最多一个在飞**:上一轮探测尚未返回时,新一轮 SHALL 跳过该盘并计数,SHALL NOT 叠加新的阻塞任务。

#### Scenario: 卡死的探测不被叠加

- **WHEN** 某盘的探测线程因盘挂起而未返回,探测周期再次到来
- **THEN** SHALL 跳过该盘的本次探测并计 `store_server_disk_probe_skipped_total{disk_id}`(gauge `store_server_disk_probe_stuck{disk_id}` SHALL 为 1),该盘的在飞探测数 SHALL NOT 超过 1

### Requirement: 每盘 IO 看门狗

store SHALL 按盘跟踪在途引擎 IO 的**系统调用时长**,计时 SHALL 在阻塞执行体内部完成(**不含**池排队时间);超过配置的时限时 SHALL 把该盘标记为不可用、丢弃该盘池中已排队的请求、计数并随心跳上报 master。

#### Scenario: 超时 IO 触发隔离

- **WHEN** 某盘上的一次引擎 IO 的闭包内时长超过 `store_server.disk_watchdog_ms`
- **THEN** `store_server_disk_io_watchdog_trips_total{disk_id}` SHALL 递增,该盘 SHALL 进入不可用状态,其后续请求 SHALL 快速失败

#### Scenario: 排队时长不计入 IO 时长

- **WHEN** 某盘健康但池繁忙,一次 IO 在池中排队较久后才被执行
- **THEN** `store_server_disk_io_duration_seconds{disk_id,op}` SHALL 只反映该次系统调用的时长,SHALL NOT 把排队时长计入,该盘 SHALL NOT 因此被判为不可用

#### Scenario: 真挂死时升级为进程重启

- **WHEN** 某盘池被超期操作**全部占满**且该状态持续达到 `store_server.disk_watchdog_escalate_secs`(配置为非 0)
- **THEN** 进程 SHALL 主动终止(`abort`),由外部 supervisor 重启;**进程内回收 D 状态线程 SHALL NOT 被作为可行手段**

### Requirement: 坏盘上的副本撤离

master SHALL 在坏盘满足持续证据时,把该盘上的副本撤离到健康盘:SHALL 复用既有 evacuation 语义(先补后删),SHALL 受并发预算与总开关约束,SHALL 在 `maintenance` 期间跳过,SHALL 永不删除分区的最后一份可用副本。

#### Scenario: 先补后删且冗余度不下降

- **WHEN** master 判定某块盘不可用且其不可用已持续达到 `master.disk_evac_min_bad_secs`
- **THEN** master SHALL 先在该分区补一个新副本,且仅在**继任者已上报 `ACTIVE`** 且存活副本数 ≥ 复制因子时才删除坏盘上的旧副本

#### Scenario: 单副本分区永不被删

- **WHEN** 分区的复制因子为 1 且其唯一副本位于坏盘
- **THEN** master SHALL 只尝试补副本,SHALL NOT 删除该唯一副本

#### Scenario: 单轮故障观测不足以撤离

- **WHEN** 某盘仅上报了单轮的故障枚举而非持续不可用
- **THEN** master SHALL NOT 为该盘生成撤离动作

#### Scenario: 维护模式与预算约束撤离

- **WHEN** 集群处于 `maintenance` 模式,或同时在途的撤离分区数达到 `master.disk_evac_max_in_flight`
- **THEN** master SHALL 暂停/推迟新的撤离动作,SHALL NOT 无界并发搬迁

### Requirement: 判活与盘健康解耦

节点的判活 SHALL NOT 依赖任何一块盘的 IO:心跳周期任务 SHALL 在任何单盘挂起或池满的情况下照常发出;容量取不到新值时 SHALL 上报上一拍的已知值并如实标注该盘不可用/未知。master 判定节点 down SHALL 具备迟滞(连续多个调谐周期观察到心跳过期),使一次盘抖动或调度停顿 SHALL NOT 直接升级为节点死亡。

#### Scenario: 盘 IO 卡住不影响心跳

- **WHEN** 某盘的容量刷新或探测超时/池满
- **THEN** 本拍心跳 SHALL 照常发出,携带上一拍的已知容量与该盘的不可用/未知标注;master SHALL NOT 因该盘而判定节点 down

#### Scenario: 短暂静默不触发全节点再复制

- **WHEN** 某节点在一个调谐周期内未上报心跳但其心跳过期尚未满足迟滞判据
- **THEN** master SHALL 继续视该节点存活,SHALL NOT 为其承载的全部分区生成再复制

#### Scenario: 节点真死仍会被发现

- **WHEN** 某节点持续未上报心跳直至满足迟滞判据
- **THEN** master SHALL 判定其 down,并按再复制判据补齐其承载的欠副本分区

### Requirement: 恢复动作的速率预算与代价阶梯

master 执行的补副本动作 SHALL 有**每调谐周期**的数量上限,并 SHALL 优先采用代价更低的手段:先停止把新副本放到坏盘,再依赖客户端绕开,最后才执行需要搬运数据的撤离/再复制。误判代价不对称(多留一份副本的成本远低于一次不必要的迁移),故一切自动化动作 SHALL 偏向"迟一步"。

#### Scenario: 每周期补副本数量受限

- **WHEN** 一个调谐周期内待补副本的分区数超过 `master.rereplication_max_per_tick`
- **THEN** master SHALL 只发起其中不超过上限的数量(按分区 id 的确定顺序选取),其余 SHALL 留待后续周期;被推迟的数量 SHALL 通过 gauge 暴露

#### Scenario: 坏盘不再接收新副本

- **WHEN** 某盘上报为不可用
- **THEN** master 的新放置 SHALL 不再把副本放到该盘(该判定 SHALL NOT 需要任何数据搬运)

#### Scenario: 未探测的盘仍可接收新副本

- **WHEN** 某盘上报 `UNKNOWN`(探测未启用或尚无有效观测)
- **THEN** master SHALL 仍允许把新副本放到该盘(避免默认关闭探测的部署完全停止放置);`UNKNOWN` SHALL NOT 触发撤离

### Requirement: 内核 fail-fast 是部署前提

本 change 的检测与处置链条以"设备层把 IO 以错误返回而非永久挂起"为前提;该前提 SHALL 作为**部署要求**记录在项目文档中(设备级超时与既有引擎自关阈值),store SHALL 在启动时对未开启引擎自关的配置给出告警,SHALL NOT 静默改写既有默认值。

#### Scenario: 未配置设备级超时时给出提示

- **WHEN** store 以 `store.io_error_count_to_trigger_shutdown = 0`(默认关)启动
- **THEN** store SHALL 记录一条告警,说明盘挂起只能靠超时隔离、建议显式开启引擎自关;该配置的默认值 SHALL NOT 被代码改写

#### Scenario: 文档写明前提与禁用项

- **WHEN** 查阅项目设计文档
- **THEN** SHALL 能找到设备级超时的清单(SCSI / NVMe / dm-multipath / NFS)与**禁用 `queue_if_no_path`** 的明确要求,以及"缺少该前提时健康检测与看门狗无法观察到错误"的说明

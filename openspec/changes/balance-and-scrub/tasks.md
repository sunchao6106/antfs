# Tasks(批 2 — 尚未实现,本文件是拆解)

## A. 计划式均衡

- [x] `types.rs`:`BalancePlan`/`BalanceMove`/`BalanceStatus` + `MasterState.balance`(serde default)+ `MasterCommand::SetBalancePlan`/`SetBalanceStatus` + apply + 旧 JSON 兼容测试
- [x] `crates/master/src/balance.rs`(新):`eval_balance(...) -> BalancePlan`、`plan_progress(plan, partitions, moving) -> Progress` 纯函数 + 单测/proptest(含"目标已存在"的幂等陷阱用例)
- [x] `master.proto` + `server.rs`:`EvalBalance`/`StartBalance`/`CancelBalance`/`GetBalanceState` 四个 RPC(照既有写路径模板:cluster_secret + leader 检查 + client_write)
- [x] reconciler 执行器:受维护开关/grace 门控,`balance_max_in_flight` 限并发,stuck 判据(超时标 failed 并释放预算),每分区同时只 1 个 in-flight
- [x] 指标 `master_balance_moves_total`/`master_balance_pending`/`master_balance_failed_total`;配置 `balance_enabled`(默认 false)/`balance_max_in_flight`(默认 1)/`balance_skew_threshold`/`balance_max_moves`
- [x] admin:`GET /api/balance` + `POST /api/balance/eval|start|cancel` + DTO + 测试
- [x] UI:均衡预览表格(from→to/状态/失败原因)+ 确认/取消 + 进度;oplab 入口放在运维页或新 tab
- [x] 集成测试:3 节点 + 多盘倾斜场景,断言 eval 产出 move、start 后按预算逐个完成、cancel 后计划清空且在途自然收敛
- [x] 文档:cluster.md §7.3/§7.4/§7.7/§11 + openspec 落地记录

## B. 副本一致性 scrub

- [x] blob:`verify() -> VerifyReport{records, bad_records, first_bad_offset, index_mismatch}`(复用私有 `for_each_record`/`scan_segment`/`rebuild_index`)+ 单测(注入坏字节/截断)
- [x] store:`GET /partitions/{pid}/verify`(只读,`spawn_blocking`,可带限流参数)+ `GET /partitions/{pid}/scrub-report`(摘要:记录数/活字节/墓碑数/段清单;需 blob 新增 `index_stats()`)+ 指标 `store_server_scrub_runs_total`/`scrub_bad_records`
- [x] admin:`POST /api/partitions/{id}/scrub`(fan-out 各副本 + 比对逻辑 + 报告,仿 compact 聚合)、`GET /api/scrub`;报告带各副本 token/bytes_read 以标注可比性
- [x] (可选)`GET /partitions/{pid}/entries?from_key=&limit=` + blob 按键有序区间遍历,支撑逐 key 全量对拍与 digest 分桶定位
- [x] 配置 `scrub_max_batch_bytes`(默认 64 MiB)+ Validate。~~`scrub_interval_secs`(后台周期)~~ 当时未落地(本行原勾账夸大,2026-10-19 复核发现;后台周期已由 `store-scrub-background` change 以 `scrub_background_enabled`/`scrub_background_interval_secs`/`scrub_background_max_in_flight` 三旋钮补齐落地)
- [x] UI:分区页 scrub 按钮 + 报告面板(逐副本摘要、比对结果、坏记录位置)
- [x] 测试:store 侧 verify 检出人为损坏;admin 侧比对检出"某副本缺 key"(在 `replication_enabled` 的 harness 里制造落后);`?replication=1`/心跳路径不受 verify 影响
- [x] 文档:cluster.md §9 端点/配置/指标 + store.md scrub 语义与"发现不了什么"(字节内容损坏、并发写导致的假不一致)
## A 节落地记录(已完成)

- 实现:`types.rs`(`BalanceStatus`/`BalanceMoveRecord`/`BalancePlanRecord` + `MasterState.balance` + `SetBalancePlan`/`SetBalanceStatus`/`FailBalanceMove`,均 serde default)、`balance.rs`(planner `plan_balance` + 进度派生 `move_phase`/`plan_view`,纯函数 + 8 单测)、`server.rs`(4 个 RPC + reconciler 执行器)、`config.rs`(4 项)、`metrics.rs`(4 个指标);proto 4 个 RPC + `BalancePlan`/`BalanceMove`。
- admin:`GET /api/balance` + `POST /api/balance/{eval,start,cancel}` + DTO(`BalanceStateDto`/`BalancePlanDto`/`BalanceMoveDto`/`BalanceOpOutcome`)+ 4 个测试(含 viewer 403、plan_id 不匹配回 200+ok:false)。
- UI:新「均衡」tab(状态卡 + 进度条 + 迁移明细表 + 评估/执行/取消/刷新,viewer 只读,5s 轮询)。
- **真集群验证(3→4→5 store 造倾斜,14/14 PASS)**:`eval` 对"新空盘"产出 9 条迁移;`start` 后逐条执行且**并发从未超过 `max_in_flight=1`**;DONE 时分布收敛 9/9/9/9、`master_balance_moves_total=9`、`master_balance_pending=0`、A6 记账(源目录删除)最终清空;再次造倾斜(5 store)→ eval(7 条)→ start → **cancel**:计划立即清空、在途搬迁自然收敛(无待删记账、无 RF+1 残留)。
- 已知边界(代码注释 + cluster.md §7.3 已写):判败的 move 会留下"目标已补、源未摘"的中间态,需运维重新 eval 或手工处理;cancel 不回滚已 apply 的搬迁。
## B 节落地记录(已完成)

- blob:`VerifyReport`/`IndexStats` 类型 + `verify(max_bytes)`(**坏记录记下继续** + 从记录头重同步 + `first_bad_offset`/`index_mismatch`/`truncated`)+ `index_stats()`(走索引/mmap 不读数据体)+ `log_end()`;索引新增只读遍历 `PersistentIndex::for_each` / `SealedSegment::for_each_entry`。
- store:`GET /partitions/{pid}/scrub-report`(摘要 + 段清单 + log 末端游标)、`GET /partitions/{pid}/verify?max_bytes=`(深扫,缺省配置上限);指标 4 个、配置 `scrub_max_batch_bytes`(默认 64 MiB);两条端点均在 RequestControl 豁免清单内(有测试断言)。
- admin:纯函数比对模块 `crates/admin/src/scrub.rs`(13 个单测)+ `POST /api/partitions/{id}/scrub`(fan-out + 逐副本素材 + 逻辑态比对)+ `GET /api/scrub`(32 条内存环形)+ 指标 `admin_scrub_runs_total`/`admin_scrub_divergences_total`;4 个集成测试(假 store:一致 / 缺 key / 摘要不一致 / 死副本 inconclusive / viewer 403 / 404)。
- UI:分区页行内「一致性」面板(结论徽标 + 逐副本记录数/字节/墓碑/段数/keys/partial + verify 摘要 + 差异明细 + 深度校验开关 + 重新巡检)、运维页「最近一致性巡检」列表(10s 轮询);i18n en/zh 双份。
- **真集群验证(3 store,15/15 PASS)**:① 写入收敛 → 一致;② 暂停 102 拉复制后写新 key → 检出 key 级差异(`n102=missing`)+ 摘要差异;③ `deep=1` 带回各副本 verify;④ **在线翻转 103 段文件字节** → deep 报 `bad_records=1`、该副本标 `partial`、结论含 `verify` 差异(**即使其元数据窗口已因坏记录读不出来**——这正是"素材取不全不致命"那条设计要覆盖的场景);⑤ `/api/scrub` 最近列表;⑥ viewer 触发 403 / 读列表 200;UI 面板与运维页在浏览器核对通过。
- 已知缺口(已写入 store.md §12.10):记录损坏会让 `BlobStore::open`(索引重建)整体失败 → 该副本起不来,因而无法经端点检查;修它需改恢复路径语义(Ambry `IndexRecovery` 方向),属后续增强。

## B 节追加:带伤启动(容错恢复,已完成)

原"已知缺口:记录损坏让 `open` 整体失败 → 副本起不来也无法被 scrub 检查"已修:

- `BlobStore::open` / `open_rebuild_index` 的**三处全量扫描全部容错**(索引重建、段字节核算、有效数据核算):
  坏记录跳过并记录,能重同步就重同步,不能则放弃该段剩余部分(其它段仍恢复);**不删不改写不截断盘上字节**。
- 大声报错:逐条 + 汇总 ERROR 日志、`OpenDamage`(`bad_records`/`first_bad_offset`/`abandoned_segments`)、
  指标 `store_open_bad_records_total`/`store_open_abandoned_segments_total`、经 `scrub-report` 的
  `damaged_records`/`abandoned_segments` 暴露 → admin 一致性巡检算作**摘要级差异**;UI 副本行显示"带伤启动"红标。
- 测试:blob 3 例(可重同步跳过并暴露 / 不可重同步放弃该段但恢复后续段 / 干净副本零副作用)+ store 1 例
  (损坏后重开装载成功、健康记录仍可读、`scrub-report` 与指标如实上报)+ admin 1 例(带伤算摘要差异)。
- 顺带修一个真 bug:重同步未校验"跳过长度是否越过段尾",被写坏的长度字段会把整段剩余内容**静默带走**却不计
  `abandoned_segments`;另 `open` 的容错回退原先只认 `Corrupt`,漏掉长度字段损坏时的 `OutOfRange`。
- 残留边界(真集群实测校准):带伤副本仍被 master 当正常副本计数;**逻辑数据可能被拉复制补回(自愈)**,
  但**盘上坏字节不删不改**,`verify` 与 `damaged_records` 永远如实报告 —— 两者不互相掩盖;
  peer 也丢了该记录时才需人工 `verify` 定位 + `RepairPartition`/`MoveReplica` 处置。已写入 store.md §12.10。

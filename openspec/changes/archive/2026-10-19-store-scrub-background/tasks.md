# Tasks — store-scrub-background

## A. blob 续扫能力

- [x] `VerifyReport` 新增 `bytes_read` / `end_cursor`(预算内扫完 = `None`)字段,`verify` 语义不变(= `verify_from(None, …)`),导出 crate 根
- [x] `verify_from(start, max_bytes)` 游标语义:游标段在 ⇒ 从 offset 续;段压短 ⇒ 钳 data_end 顺延;段被 compaction 摘走 ⇒ 从其后第一段接;超全部段 ⇒ 空手返回;段内坏墙 ⇒ 弃段剩余后续照扫
- [x] blob 单测 5 件:预算恰停(游标精指下一条)/ 多批接力恰好覆盖一次(Σ=全量,不重不漏)/ 坏记录重同步后可续(计一次、后续批推进)/ 悬空游标前跳两路覆盖一致 / 越全部段空手返回 —— `tests/verify.rs` 11/11 绿

## B. store 后台循环

- [x] config 三旋钮:`scrub_background_enabled`(默认 false)/`scrub_background_interval_secs`(60)/`scrub_background_max_in_flight`(u32,默认 1)+ Validate(at_least ≥1 ×2)+ 单测 2 件(默认值锁 + 危险零拒绝)
- [x] `scrub_worker.rs`:`scrub_round`(单 tick:健康闸+追平闸筛候选、按 pid 定序 round-robin、每 lane 一个 batch 经每盘池 `pool.run(disk,"verify")` 并发、JoinSet 汇拢推游标)+ `spawn_scrub_worker`(抖动间隔 + paused 闸)+ `jittered`(±25% 带界单测)
- [x] `serve()` 起腿(仅 enabled);`AppState.scrub_paused`(AtomicBool,不持久化)
- [x] `POST|GET /admin/scrub-control`(仿 replication-control:回显开关、同步 gauge、info 日志;不动手动端点)
- [x] verify 端点回显 `bytes_read`/`end_cursor`(增量字段,手动分批运维据此续)
- [x] worker 单测 6 件:三轮接力+第四轮开新周期(字节守恒 4×152)/ UNAVAILABLE 盘跳过且可见(disk_worker_skipped{scrub})/ BOOTSTRAP 集群跳过+单机照扫 / SEALED 照扫(与 compaction 候选集刻意相反)/ 池满退避不挂等+放行恢复 / 抖动界
- [x] 集成测试 `tests/scrub_background.rs` 5 件(真 serve() 循环):自动爬(30s 带界轮询,手动 verify 计数零占) / 在线翻坏字节被后台指标发现且循环不停摆 / 掐停两 tick 零增长+暂停期手动 verify 可用+恢复续爬 / 端点回显契约(partial: truncated+cursor;full: null) / 默认关 = 零批
- [x] metrics 4 个:`scrub_background_batches_total` / `scrub_background_bytes_total` / `scrub_background_bad_records_total` / `scrub_background_paused`(gauge)

## C. 勾账修正 + 文档

- [x] balance-and-scrub tasks.md B-5 行:`scrub_interval_secs` 夸大勾改为如实(仅 `scrub_max_batch_bytes` 落地;后台周期由本 change 以三旋钮补齐)
- [x] store.md:新节(§12.14 后台巡检)+ 配置项清单 3 行 + 监控项清单 4 行 + 看门狗折页提示(`[disk_watchdog_ms_by_op] verify` 配置建议)
- [x] DEVLOG 新步
- [x] 门禁:`cargo test -p antfs-blob`(全量)+ `-p antfs-store --lib`/三个 test 目标 + clippy 0 + fmt 净 + `openspec validate store-scrub-background`

## 落地记录(证据)

- blob `tests/verify.rs` **11/11**(5 新);store `scrub_worker` 单测 **6/6**;`tests/scrub_background.rs` **5/5**;workspace **75 套 739 绿、0 失败**(store lib 147/147,数据面/compaction/复制回归全绿);clippy workspace 0;fmt 净;`openspec validate` 过(specs 13/13)。
- 真实路径验证:集成测试走 `serve()`(后台 task 的真实出生地;手动 axum serve(router()) 不起循环 —— 首轮测试翻案教训写进了 harness 注释)。
- 已知边界(如实):① 周期化的是**逐盘 bit-rot 自查**;admin 级跨副本比对的周期化(发现分歧→建议 Repair)未做,另议;② 小副本每 tick 全量重扫(预算钳幅,无害);③ 游标内存态,重启重扫(幂等)。

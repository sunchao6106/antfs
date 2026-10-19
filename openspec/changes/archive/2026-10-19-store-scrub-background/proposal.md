# Proposal: store-scrub-background — 后台一致性巡检蜗牛爬

## Why

一致性巡检(balance-and-scrub B 节)把"巡检"做成了**按需动作**:`POST /api/partitions/{id}/scrub` 由运维手点。但巡检真正护的是**冷尾巴** —— 热数据读路径逐条验 CRC、副本追平窗口有拉复制流过;没人读、又已追平的段,坏字节**没有任何机制会发现**,直到一次冷读 500,或更糟:peer 副本也把数据压实丢了,自愈窗口关闭。原 change 的 tasks 勾了"可选 `scrub_interval_secs`(后台周期)"但**代码从未落地**(本次复核发现的勾账夸大)—— 本 change 补上这条腿,并顺手把勾账修实。

巡检周期化最大风险是**震荡**:一台 24 盘机若同时全速深扫,等于对自家盘发起顺序读风暴。故本 change 的设计重心是"自动跑"与"别打爆"同时成立。

## What Changes

- **`antfs-blob`:`verify` 续扫能力** —— `verify_from(start, max_bytes)`:从上轮预算耗尽处的"下一条未扫字节"续扫;`VerifyReport` 新增 `bytes_read` / `end_cursor`(`None` = 预算内扫完全量)。游标语义:游标段被增量 compaction 摘走 ⇒ 从其后第一个段接(新段位置恒大,不漏);段内坏墙 ⇒ 弃该段剩余、后续段照扫(与 open 带伤恢复同语义)。旧 `verify(max_bytes)` 语义不变(= `verify_from(None, …)`)。
- **`antfs-store`:后台蜗牛爬循环**(`scrub_worker.rs`,仅 `scrub_background_enabled`(默认 **false**)时由 `serve()` 起腿):
  - 节拍:每 tick 每 lane 一个 batch(字节预算 = 既有 `scrub_max_batch_bytes`),tick 间睡 `scrub_background_interval_secs`(默认 60s,叠 ±25% 抖动去相位,不与 compaction 凌晨钟点共振);
  - 并发闸:跨盘同扫数 ≤ `scrub_background_max_in_flight`(默认 1,家风同 master `balance_max_in_flight`);**同盘**由每盘有界池(D1)天然串行,池饱和 ⇒ 退避跳轮,绝不重试轰炸;
  - 候选:本节点全部副本(**SEALED 也扫** —— 封存冷数据正是巡检对象,与 compaction 候选集刻意相反);集群模式下 `BOOTSTRAP`(未追平)副本跳过;盘 `UNAVAILABLE/DEGRADED` 跳过并计 `disk_worker_skipped{task="scrub"}`;
  - 游标:内存 `HashMap`(重启即从各副本头部重扫 —— verify 只读幂等,多扫无害;不持久化,家风同 compaction 台账);周期走完自动从头开新周期;
  - **只报警不修复**:发现坏记录/索引漂移 ⇒ warn + `store_server_scrub_background_*` 指标;处置永远是运维弧(手点 verify 定位 + RepairPartition/MoveReplica)。
- **`POST|GET /admin/scrub-control`**(store 数据面 admin):运行期掐停/恢复后台循环。手动 verify/scrub-report 端点**不受影响**(应急检查能力必须常在);不持久化、不动游标 —— 恢复后从原处续爬。与 `/admin/replication-control` 同家风(它也仍被 `request_control` 豁免:可静默的只有 put/get/delete,禁巡检是"禁出数据问题"的反面)。
- **verify 端点回显续扫字段**(`bytes_read`/`end_cursor`):手动分批运维据此续扫;后台循环与手动端点共用引擎与预算、**不共用计数字径**(`scrub_background_*` 独立于 `verify_*`)。
- 勾账修正:balance-and-scrub tasks.md 第 B-5 行的 `scrub_interval_secs` 夸大勾改为如实描述并指向本 change。

## Impact

- `crates/blob/src/blob.rs`(+`tests/verify.rs` 5 件):`verify_from` + `VerifyReport` 两字段;`VerifyReport` 导出至 crate 根。
- `crates/store/src/scrub_worker.rs`(新):循环 + `scrub_round` + 单测 6 件;`config.rs` 三旋钮 + validate;`metrics.rs` 4 指标;`server.rs`(AppState.scrub_paused / serve 起腿 / 路由 / 端点 / verify 回显);`tests/scrub_background.rs` 5 件。
- **不改任何默认行为**(`scrub_background_enabled = false` ⇒ 现有部署零变化);不动磁盘格式(纯读路径,§3.4 无涉);不动手动 verify 响应既有字段(仅增字段,增量兼容)。
- 文档:store.md 新节 + 配置项/监控项两表;DEVLOG 新步。

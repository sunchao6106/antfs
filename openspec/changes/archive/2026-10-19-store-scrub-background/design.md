# Design: store-scrub-background

## Context / 目标

把"运维手点的 scrub"升级为"机器自动跑的蜗牛爬",同时**绝不震荡整机**。红线:只读(不改盘上字节)、不动持久化格式、不给现有部署加默认 IO。

## Motivation

- 热数据有读路径逐条 CRC(`read_full_record` 每次解码都验);追平窗口有拉复制流过。**冷尾巴两不沾** —— 坏字节的发现只能靠周期性深扫,而现状验证发现能力按需触发(依赖运维记得点)。
- 原设计认领过这条腿(tasks 勾了 `scrub_interval_secs`)但从未落地;本次补腿时把"防震荡"从口号落成四条机制。

## Goals / Non-Goals

**Goals**
1. 存量冷数据周期性全量覆盖(慢没关系,**必然**比**快**重要);
2. 自动性与温和性同时成立:单机多盘不得 IO 风暴、不得与 compaction/复制共振;
3. 坏发现"大声"(warn 日志 + 独立指标),但**不自动修**;
4. 应急开关:运维能掐停自动扫描而保留手动检查。

**Non-Goals**
- 不做跨副本逻辑比对(admin 级 fan-out 比对已存在,继续走按需人工触发;周期化它属于另一个决定);
- 不做自动修复/自动 Repair ——发现→人工处置,记于裁决;
- 不做巡检调度的持久化游标(重启重扫,幂等无害);
- 不动 request_control 的可静默集(put/get/delete)。

## Decisions

### D1 开关语义:独立 bool,不用哨兵值

对照全仓 interval 家族(均为 `u64` ≥1):"-1 表关"类型装不下,"0 表关"会把"调周期"和"关功能"耦在一个字段(想调慢误关 / 想关被 validate 拒)。三旋钮各司其职:

```toml
scrub_background_enabled        = false   # 开关(默认关:后台 IO 不给现有部署加塞)
scrub_background_interval_secs  = 60      # 两 tick 之间的基础间隔(±25% 抖动)
scrub_background_max_in_flight  = 1       # 跨盘同时扫数上限
# scrub_max_batch_bytes(既有,64MiB)= 每 tick 字节预算,复用不新增
```

### D2 节拍 = 蜗牛爬,不是"天级批量"

"interval 描述全量节奏(天级)、每拍只敢 64MiB"折出来的形状是:**恒定低速逐一 batch**:每 lane 每 tick ≤1 batch(≤64MiB),拍间睡 interval。100GiB 副本 ≈ 26h 一周期 —— 慢即特性。抖动 ±25% 去相位:多节点同配间隔会在 compaction 凌晨钟点周围排成尖峰队,抖动把队列打散。抖动源用墙上时钟纳秒(不引 rand 依赖)。

### D3 并发的两把闸,分工不同

- **同盘**:每批走 `pool.run(&disk, "verify", …)` —— 与 put/get/compaction 在同一个 D1 有界池排队串行;池满 ⇒ `Err(reject)` ⇒ 本轮退避(debug 日志),下 tick 再试。**同盘并发闸天然存在,不新造。**
- **跨盘**:`max_in_flight`(默认 1);24 盘机不同时 24 路顺序读。两闸正交:前者管公平/隔离,后者管整机带宽。

### D4 候选集与 compaction 刻意相反

- **SEALED 照扫**:compaction 不碰封存段是"别在 EC 对象身上搬字节";巡检只读不搬,恰是冷数据唯一看护人;
- **BOOTSTRAP 跳过**(集群模式):未追平副本还在被拉复制填充,游标漂 + 白扫;判断沿 compaction 同款(状态下发已知才判,单机模式免闸);
- **健康闸**:UNAVAILABLE/DEGRADED 盘跳过 + `disk_worker_skipped{task="scrub"}`(与 flush/compaction 同族指标,"没在推进"必须可见)。

### D5 续扫游标(blob 层新能力)

`verify_from(start, max_bytes)`,`VerifyReport.end_cursor` = 预算耗尽处"下一条未扫字节",`None` = 预算内扫完。续扫正确性:
- 游标段在 ⇒ 从 offset 续(段被压短 ⇒ 钳到 data_end 顺延下段);
- 游标段被增量 compaction 摘走 ⇒ 从其后第一段接(新段位置恒更大,数据延续不漏);
- 段内坏墙(重同步失败)⇒ 弃该段剩余、后续段照扫 —— 与 `BlobStore::open` 带伤恢复同一语义家族;该 case 无游标可写(放弃区不可恢复,修复是人工弧)。
- 游标只存循环内存;周期完成重开新周期(新写入在下一周期覆盖)。

### D6 与既有平面的关系

- `request_control` 豁免不动:可静默集仍只有 put/get/delete("宁可禁不掉,不可禁出数据问题");自动循环的应急闸是新的 `/admin/scrub-control`(运行期 bool,不持久化、不动游标);
- 看门狗:`verify` 已在 `KNOWN_OPS`,想给后台批一档宽门槛直接配 `[store_server.disk_watchdog_ms_by_op] verify = 300000`(compaction 同量级,大顺序 IO 家族)—— 纯配置即可,不改码。默认空表 ⇒ 不开,不变;
- 计数口径分离:`scrub_background_{batches,bytes,bad_records}_total` vs 手动 `verify_*` —— 运维要能分清"机器发现"和"人发现";坏发现一条 `warn!`(损坏=需关注的意外失败级别恰如其分)。

## Risks / Trade-offs

- **重复扫热 replicas**:小副本每 tick 全量重扫(预算钳住 ≤64MiB/拍,5MiB 副本 = 5MiB/分钟,无害但指标批次增长快)。接受,简化 > 微优化。
- **游标 vs compaction 竞态**:compaction 在两拍之间摘段,游标按 D5 规则前跳;最坏重扫(幂等)绝不漏扫。
- **抖动用墙上时钟纳秒**:分布不均匀但够用(目的只是去相位,不是密码学随机)。
- **崩溃/重启**:游标丢 ⇒ 重扫,只读幂等,无崩溃一致性面。
- **默认关**:想要"开箱即自动巡检"的部署需显式打开 —— 用户指令"别震荡"优先级别高于"默认智能"。

## Migration Plan

无迁移:纯增量(新配置键、新端点、新指标、verify 响应加字段)。`enabled=false` 时行为与现状 byte-identical。

## Open Questions

无(已知残留归档在 store.md:"周期化的是逐盘 bit-rot 自查,admin 级跨副本比对的周期化未做,另议")。

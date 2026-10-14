## Why

4 GiB 段(生产默认口径)实测:每次滚转 ≈ 一次 **1-1.5 秒的写锁全线停摆**(`Log::roll_over` 在写路径临界区内同步 `fsync` 写满段;VM ext4 实测 max 1.16 s 裸 / 1.53 s 带周期 flush——fsync 大段是盘写吞吐墙,分摊不掉总量)。p99 干净纯是采样稀释(1/5000),风险全在 max / p99.9+: SLA 长尾、复制追赶、同盘心跳全被这一个动作劫持。详见 `benchmark/BASELINE.md`「4 GiB 段复测」节。

## What Changes

- **滚转移出写路径临界区**:`Log::roll_over` 不再同步 `flush()` 旧段;旧段(`Arc<LogSegment>`)入 `pending_seals` 队列,临界区内只保留「新建段」(~1.4 ms,fallocate 实测)。
- **新配置项 `store.defer_seal_flush`(bool,默认 true)**:`false` 完整保留旧行为(滚转点同步 fsync 旧段,封存段即刻越出 at-risk 窗口,代价=写路径承受满段秒级停摆)。开关经 `finish_di` 以原子量注入 Log(不穿透工厂签名)。**默认 true = 修复默认生效;保守部署一键回旧行为。**
- **借既有 flush 通道收敛**:纯 backward 无新线程、无新配置 ——
  - `BlobStore::flush`(worker 周期驱动):先**写锁外** `Log::drain_pending()`(重头 fsync,与写路径并发),再进写锁走 `flush_inner`;
  - `flush_inner`(与 compaction 复用):锁内先做一次**增量 drain**(通常为空),守住「log 全量持久先于 index flush」顺序不变量,再 active flush → index → watermark;
  - `Log::flush` / `flush_all` / `close` 同样 drain(`close` 兜底,防调用方漏 flush)。
- **持久性口径不变**:flush 仍是唯一持久化边界;封存段与活跃段的 at-risk 窗口拉平(≤ flush 间隔),无新契约。
- **EIO 链路不断**:公开 `flush()` 成败接入 `record_success`/`record_io_error`,滚转 flush 失败原经 put 错误路径,现经 flush 错误路径进同一条「EIO → 自关 → replica ERROR → 再复制」链。
- **文档同步**:log.rs 内三处「先 flush 旧段再滚动」句式、store.md 写路径描述更新;BASELINE.md 补修复后对照行;DEVLOG 45。

## Capabilities

### New Capabilities
- `log-rollover-off-critical`:段滚转的延迟-flush 语义 —— 滚转不在写锁临界区做 fsync;收敛点与持久性边界契约。

### Modified Capabilities
<!-- 无既有 spec 涉引擎滚转(为引擎新立)。 -->

## Impact

- **crate**:`crates/blob`(log.rs 结构 + 滚转/flush/close;blob.rs flush 分相 + 错误记账)。
- **契约**:对外接口零变化;崩溃暴露面上界不变(flush 间隔),`flush()` 返回前「全部已写数据落盘」语义保持(原来由滚转 bonus 保证的子集现在由 drain 保证)。
- **不涉**:磁盘格式(§3.4 不触发)、MetaStore/REST/S3、store 配置/监控(零新增)。
- **验证计划**:log 级字节断言测试(flush 前封存段 header data_end 陈旧、flush 后追平;close 兜底)、既有 `crash_consistency` 属性面自动覆盖(_FLUSH 是边界)、VM 基准 A/B(1 MiB×5000@4 GiB max 塌缩)。

## Non-goals

- 不做组提交/双缓冲/后台线程池(仅重排 fsync 时机,不加机制)。
- 不改 flush 间隔默认值;不开新配置项。
- 不处理 VM 虚拟盘的 ~700 ms 环境毛刺(与滚转无关)。

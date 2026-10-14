## Context

`Log::roll_over` 在写路径临界区(`write_lock` + `entries.write`)内同步 `flush()` 写满段 = write_header + `sync_all`。4 GiB 段(生产默认)实测该 fsync ≈ 盘写吞吐墙(~3 GB/s)⇒ 单次 1.16-1.53 s 停摆,期间写锁全线冻结(触发滚转的请求 + 其后排队全部写)。周期 flush worker 救不了(只是把同样的字节挪时间刷)。证据链见 `benchmark/BASELINE.md`「4 GiB 段复测」。

## Goals / Non-Goals

**Goals:**
- 滚转不再在写锁临界区 fsync;写路径滚转成本 ≤ 新建段(~1.4 ms)。
- 持久性契约**逐字不变**:「`flush()` 是持久化边界;`flush()` 返回前,调用时刻之前的全部已写记录落盘」。
- 零新线程/配置/格式;EIO 自关链路不断。

**Non-Goals:**
- 不优化 fsync 本身(吞吐墙是物理);不引入组提交。

## Decisions

### D1. 延迟到既有 flush 通道,不自建后台机制

旧段入 `Log::pending_seals: Mutex<Vec<Arc<LogSegment>>>`,由现有调用面收敛:`BlobStore::flush`(store worker 周期驱动,默认 30 s)、`Log::flush`/`flush_all`/`close`。生产收敛延迟 = flush 间隔,与 active 段同节奏。
否决方案:①滚转时后台线程 fsync —— 引擎层新增线程违背「引擎无自有线程、由 store worker 驱动」既有分层;②预滚(提前建下一段)—— fallocate 实测 1.4 ms 无收益,重点在 fsync 不在 create。

### D2. flush 分相(flush 停摆的转移必须真的转移)

`BlobStore::flush()`:
1. **锁外** `drain_pending()`(重头 fsync,与写路径并发 —— 段已 sealed 不可变,写路径再不触碰);
2. 进 `write_lock`:`flush_inner()`。

`flush_inner`(compaction 亦复用)内部首行再做一次**增量 `drain_pending`**(通常空,零成本):覆盖「相 1 与相 2 之间发生新滚转」的窗口,守住顺序不变量 —— **log 全量持久必须先于 index flush**(index 不得引用 log 尚未持久的 offset;`flush_inner` 现序 log→index→watermark 不动)。
若把 drain 整个留在锁内:1.3 s 停摆只是从滚转点挪到 flush tick,**净效果为零** —— 本决策是修复成立的命门。

### D3. 持久性风险面(CLAUDE.md §3.3 清单逐项)

| 检查项 | 结论 |
|---|---|
| 写路径 | 变更本体;ack 语义不变(ack 本就先于 fsync) |
| 持久化边界 | **唯一边界仍是 flush**(crash-consistency 属性测试断言的就是这条);封存段 at-risk 窗口从 `min(滚转即落盘, 下次 flush)` 变为 `≤ 下次 flush` —— 上界不变,与 active 段拉平 |
| 成功回报时机 | 不变(flush 返回仍严格后于 fsync) |
| 资源清理 | `Log::close` 先 drain(队列 Arc 克隆释放后再 `Arc::try_unwrap`,否则句柄泄漏卡死) |
| 重试/幂等 | 不变(滚转幂等性未动) |
| 部分失败 | sealed 段落盘窗口与 active 段崩溃同构:恢复认 header data_end,torn record 由既有 CRC 容错扫描处理(同一套机制,无新形态) |
| 损坏处理 | 不变;flush 失败照报(且见 D4 记账) |

### D4. EIO 发现时机后移,显式接回自关链

滚转 fsync 失败原来经触发 put 的错误路径进 `io_error_count`;现在入口换成 flush 通道。`BlobStore::flush()` 成/败接 `record_success`/`record_io_error`(阈值、swap 触发、hook 全复用),「EIO → 自关 → replica ERROR → 再复制」链路保活;发现延迟 ≤ flush 间隔,与 active 段 flush 失败**同权**(统一而非弱化)。

### D5. flush_all 跳过已 drain 段

`flush_all`(硬删除后全段统一 fsync)先 drain,再遍历 entries 时用 `Arc::ptr_eq` 跳过刚 drain 过的段 —— 防止 4 GiB 级重复 fsync(秒级白费)。`flush` 本身(只碰 active+pending)无此问题。

### D6. 配置开关 `store.defer_seal_flush`(默认 true,false = 旧行为)

用户拍板要求保留同步路径作选项。实现为 Log 上的原子量,经 `finish_di`(config 注入收尾,
与 `io_error_count_to_trigger_shutdown` 同点)注入 —— 不穿透 `BlobStore` 三层工厂签名,测试/
裸构造缺省走新行为。仓内行为门先例:`replication_enabled` / `bootstrap_copy_enabled` /
`disk_health_enabled`。`false` 模式下 pending 队恒空,flush 分相自然退化为旧形(零额外路径);
两模式各有独立回归测试(异步:flush 前 header 陈旧→追平;同步:滚转后即落盘)。

## Risks / Trade-offs

- **[drain 漏接 = 窗口无界]** `Log::flush` 若不 drain,worker(只调 flush)会让封存段永不落盘 → 字节级测试钉死(flush 前封存段 header data_end 陈旧/flush 后追平;close 兜底重开可见)。
- **[锁序]** `roll_over` 持 `entries.write` 内取 `pending_seals` 短锁;drain 只瞬持 pending 后释放再 fsync —— 无人「持 pending 拿 entries」,无环。
- **[分相间隙新滚转]** 由 D2 锁内增量 drain 兜住;极端场景(一个 flush 周期内写满一整段)出现锁内一次大 fsync,概率 = 滚转与 flush 同拍,可接受。
- **[无 worker 的裸调用方]** 封存段落盘延迟退化到 close()(兜底 drain)——与「从不调 flush 的 active 段」同权,无新类暴露。

## Migration Plan

无格式/契约变更,无迁移;revert = 回滚提交。

## Open Questions

无。

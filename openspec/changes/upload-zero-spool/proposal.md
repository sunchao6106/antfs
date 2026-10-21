# Proposal: upload-zero-spool — 上传路径零本地临时盘(方案设计,待所有者裁决)

> 状态:**设计已出品、未实施**。B/A 两段各自独立裁决(router.md §7 TODO 条目的正式答卷;
> 原 B0-B4 存亡矩阵由本设计收敛)。代码落地须所有者点选裁决点后另批执行。

## Why

上传路径今天有两处**额外临时盘**(数据最终会落在数据盘,这是结局;这两处是"中途再写一遍"的
中转,是可消除的写放大与运维面):

| 墙 | 位置 | 机理(行号级事实) | 代价 |
|---|---|---|---|
| ① 客户端 | router `put_file` 压缩帧:`compress_stream_to_temp` → `tempfile::NamedTempFile`(落 **TMPDIR**,非数据盘) | 诚实比率闸需压缩后尺寸;一个帧供 N 副本并 PUT + 507 重试重读;RAII 清删 | 每次 put_file 额外写一遍压缩流;SDK 对 TMPDIR 依赖(无盘/只读 tmp 容器不可用);明文帧散落客户机盘上的窗口 |
| ② 服务端 | store `put_blob`:CL > 1 MiB → `StagedFile`(`(staging_dir 或各盘)tmp/put_*.tmp`)后进引擎 | 引擎 `put_from_reader` 单写者(写锁全程)+ 忠实长度;暂存认网络时长与写锁解耦(文档化、实测过的 p50 1ms→9ms 教训) | 大对象双写(写两遍盘 = 写放大 2×);暂存文件生命周期(cancellation Drop 兜底 + 启动清扫) |

## What Changes(两段,独立可裁决)

**B 段(router,客户端墙,低风险速赢)**:压缩帧改**内存帧**(`Bytes`,17B 头 + 编码流),
带可配上界 `compression_frame_mem_max_bytes`(默认 8 MiB):超界 → `Skipped("spill")`
回落**原文直传**(新监控标签;上界×并发 = 内存上界,同 `INLINE_BODY_MAX` 精神)。
帧跨副本/重试 = `Bytes` Arc clone —— 比今日的文件 reopen 更省;TMPDIR 依赖清零。

**A 段(store,服务端墙,引擎攻坚)**:> 1 MiB CL PUT 去 `StagedFile`,改为
**log 预留区直写 + 大小分道**:`Log` 增设大对象专用活跃段(lane;默认 2 条,每 lane
同时至多 1 个活动预留),body 逐块直写预留区(spawn_blocking 散量子任务,CRC 随算),
短池任务收尾(尾部 CRC 补写 + `commit` + 索引)。小对象直行道**完全不动**(时延形态保真);
大对象传输期间小对象 ack 不排队(正是暂存设计要守住的点,分道是唯一两全解)。写放
大 2×→1×。崩溃/磁盘格式零变(恢复只认 flush 过的 `data_end`;未 commit 预留不可见;
见 design.md「持久性红线走查」)。

## Impact

- B:router 压缩路径;`router_compression_attempts{outcome}` 标签集 +`spill`;配置面 +1 旋钮。
  密度语义变化:**帧 > 上界的可压大文件不再压缩**(mitted tradeoff,见裁决点 ①)。
- A:`blob`(Log/lane 机制 + abort 回卷)、`store`(handler 三段化)、文档(store.md/router.md);
  `store_server.staging_dir` 落地后退役( knob 保留解析,文档明示);监控 +4 项。
  engine 写路径 = 持久性红线 → 全套崩溃一致性/并发/abort 测试随批。
- 不碰:s3/frontend 内存分块路径(本就零盘)、磁盘格式、chunked(无 CL)端口语义(维持
  缓冲回落——router 全链路 PUT 恒带 CL)。

## Out of Scope

- 无 CL(chunked)大 body 的流式落盘(router 无内部生产者,引擎记录 u32 长度形态)。
- 压缩两遍确定性 / 采样启发(否决矩阵,见 design.md)。
- lane 段回收给小对象复用的微优化(密度已由 seal 阈值收口)。

## 待所有者裁决(3 点)

1. **B 段 spill 语义**:超内存上界的可压文件 → 推荐**原文直传保真**(本提案);若密度回归
   不可接受,备选两遍流式(CPU 1+N 税)或回落今日 tempfile(零临时盘不再绝对)—— 记档再议。
2. **A 段道型**:推荐 **A-ii 大小分道**(本提案);保守护栏 = **A-iii 维持暂存 +
   `staging_dir` 指外置 scratch 盘**(零代码,"服务端盘"可控但非零)。
3. **实施顺序**:推荐 B 先(低风险速赢)→ A 后(引擎攻坚);亦可只批 B 段。

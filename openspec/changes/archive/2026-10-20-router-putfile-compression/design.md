# Design: router-putfile-compression

## D1 一次判定,三口同权

三闸判定发生在 `put_file_with_ttl` 进 507 重试环**之前**、每次文件上传恰一次(与 Bytes 口
"一次判定供全部副本"同构):`disabled` / `too_small` 在仲裁侧短路计数;`ratio` 与封帧下沉到
`compress_stream_to_temp`。重试换分区不重复压 —— 同一帧文件被全部副本并行 PUT 与
`MAX_STORE_FULL_RETRIES` 次重试**共用**;`_frame`(NamedTempFile 守护)存活到函数返回,
Drop 自动清删(含 panic/unwind 路径)。

## D2 流式形态(内存红线)

- 预压在 `tokio::task::spawn_blocking`(CPU+同步 IO 重,不碰 reactor);
- 四算法臂统一 `std::io::copy` 逐块流过(缓冲 KB 级):先把 17B 头写进 NamedTempFile,
  编码器以该文件为 sink;级别:zstd 用 `compression_level`,gzip clamp(0..=9),
  lz4/snappy 帧编码器内建档;
- **显式收尾**:zstd/gzip/lz4 `finish()`(lz4 不显式 finish 会截尾 —— 前批真踩),snap 无
  `finish()` → `into_inner()`,全部之后 `flush()` 落盘;
- 比率闸口径 = **帧文件总长(含头) vs 原文件长**,与 Bytes 口(17B + 流长 vs 原长)同义。

## D3 失败语义:压缩永不让写入失败

| 环节 | 行为 |
|---|---|
| 原文件打不开/读失败 | 原本就是 `Error::Io`(既有语义,不变) |
| 编码器失败 | `Original{low_ratio>` 形态(与 Bytes 口 fold 掉编码错一致) |
| 临时文件 IO/元数据失败 | `Original{io}` 回落原文直传 |
| spawn_blocking join 失败(panic 等) | 同 `io` 回落 |

任何一种回落下,写路径继续走**原文件直传**,成功语义不变。

## D4 监控与兼容

- 复用 `router_compression_attempts_total{outcome}` / `saved_bytes`:仅取值集新增 `io`
  (调用方与文档同步);`saved` 口径 `orig − stored`(帧文件长);
- Bytes 口决策函数/行为零改动 ⇒ 无回归面;
- 帧格式为读口既有约定 ⇒ 集群侧零新语义。

## D5 测试

- lib:四算法 kept roundtrip(帧解压恒等 + 正收益)+ 熵 low_ratio + 零长 too_small +
  不存在路径 io;
- e2e(真 master+store+router):kept 128 KiB(逻辑大小恒等 + 逐字节 + kept≥1)、
  100 B too_small、128 KiB 强熵 low_ratio —— 三臂监控各记一笔。

## Risks / Rollback

- 回滚 = revert 单 commit(tempfile dep 一并退);
- 新暴露面仅"客户端本地临时盘写"—— 失败已闭合成 io 回落,不影响正确性。

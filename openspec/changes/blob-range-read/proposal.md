# Proposal: blob-range-read — 部分读(offset+length / Range)四层暴露

> 状态:**已实施(2026-10-21,与 router 读侧下沉同日同批落地;spec/tasks 见下)**。
> 裁决定稿:P0 全闭环 —— D1 **消解**(压缩功能当日整体移除,世上无帧;重开压缩时回访)、
> D2 = `?offset=&length=`,offset 缺省 0、length 缺省读到尾、start 越界 416;
> D3 = HEAD 带 Range 回 206(对齐 Ambry);D4 = 复制读整契约零触碰(保持)。
> 实施偏差记档:P2 的"缓冲版 get_range"未做(所有者裁决⑭:读口只留 stream 式);
> P3 的 s3 纯函数复用改为 frontend 自持同语义模块(跨 crate 依赖方向不正,`frontend/src/range.rs`)。

## Why(证据链)

- **Ambry 先例坐实**(本地源码 `~/code/opensource/ambry`):`GetBlobOptions` 原生带 `ByteRange`;
  REST 层 `Range: bytes=s-e`/`bytes=s-`/`bytes=-k` 单区间 → 206 + `Content-Range`,越界 →
  `RangeNotSatisfiable`(416);HEAD 带 Range 也回 206。**frontend `/blobs` 补 Range 是忠实对齐债**,
  不是投机特性。
- **Ambry 未做、antfs 可做的优化**:Ambry 的 store 协议(`PartitionRequestInfo`)只有
  partitionId + blobIds,**无 offset/length** —— 单 blob 部分读 = store 整读 + router 切片
  (`GetBlobOperation.filterChunkToRange`)。antfs 引擎层 `read_range` 原语已存在
  (`store-read-streaming` 变更落地),把它接到 HTTP 边界 = 超越 Ambry 的"业界最佳"替换项。
- **浪费量级**(antfs 现状):
  - frontend `/blobs` composed 大对象 GET 逐片**全量**取回 —— 10 GiB 对象读 1 MiB → 10 GiB 过网(~10⁴ 倍);
  - S3 网关 `get_plain_object` 整读后切片(≤4 MiB/次);multipart 边界片整片取回(≤1 片/次);
  - 小对象(<1 MiB,设计主流)整读 = 一次位置读,收益 ≈ 0。

## What Changes(分层)

| 层 | 改动 | 对齐定性 |
|---|---|---|
| store 数据面 | `GET /partitions/{pid}/blobs/{key}?offset=&length=`:`GetParams` 扩字段,`StreamingRead.offset` 参数化(现为硬编码 0),复用既有 512 KiB 分块 `read_range` 流 | **超越 Ambry**(优化) |
| router SDK | `get_range`(缓冲,供网关小对象)+ `get_range_stream`(流式):复用 `open_replica_stream`/`read_from_all`,R=1 语义不变 | 超越 Ambry |
| frontend `/blobs` | `Range` 头解析(复用 s3 crate `parse_range`/`ByteWindow` 纯函数);206/416/忽略回 200 语义矩阵对齐既有 s3 口径;composed 对象窗口映射到覆盖片(复用 `window_parts` 思路) | **忠实对齐**(还债) |
| S3 网关 | `get_plain_object` 整读切片 → `get_range`;`PartStream` 边界片裁边 | 收尾(收益有界) |

## Impact

- 读路径行为变更(新增可选参数/头,缺省行为不变);`?replication=1` 复制读分支**零触碰**
  (整读 + include_expired + 整条 CRC 契约,`store-read-streaming` 已裁决)。
- 磁盘格式零改;写路径零触碰 → 无持久性红线命中。
- **关键裁决点(P0,见 design §3)**:压缩帧 blob 的窗口读语义 —— store 窗口读操作磁盘字节,
  压缩帧上直接开窗会切坏帧;Ambry 口径是 store 永远整读、router 在解压后坐标上切片。
- 文档义务:store.md / router.md / frontend.md / s3.md 各自 API 表 + §7 配置/监控清单。

## Out of Scope

- 复制读(`?replication=1`)流式化/窗口化(复制语义另案)。
- mmap sealed 段 zero-copy 响应(段生命周期重构,收益另案)。
- 多区间 Range(`bytes=0-1,3-4` 按 RFC 7233 忽略回 200,对齐既有 s3 口径)。
- D5 EC 冷层落地后的条带对齐窗口读(远期,design 记一笔)。

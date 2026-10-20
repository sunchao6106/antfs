# Proposal: router-putfile-compression — put_file 压缩入口(v1 取舍收官)

## Why

router.md §7 记档取舍:「`put_file` v1 不压(海量小文件 + 网关分片走 Bytes 口)」—— 后果是
SDK 两个写入口**行为不对称**:SDK 直传本地文件(benchmark、backfill、CLI 场景)不享压缩,
同一条数据走 `put` 与走 `put_file` 落盘字节不同。取舍动机(海量小文件)是分布假设而非
机制约束,应当补齐。

## What Changes

| 位置 | 改动 |
|---|---|
| `compression.rs` | `StreamedPut`(Frame/Original) + `compress_stream_to_temp`(流式三闸) + `encode_file`(四算法流式编码臂;zstd/gzip 显式 finish、lz4 显式 finish[截尾教训]、snap into_inner) |
| `lib.rs` | `prepare_file_upload`:三闸与 Bytes 口同语义/同监控;`put_file_with_ttl` 进重试环前一次判定,帧临时文件 **RAII 守护跨 507 重试与全副本共用** |
| Cargo.toml | `tempfile` dev-dep → dep(临时帧文件 RAII;小而标准) |
| 监控 | `router_compression_attempts_total{outcome}` 增取值 `io`(流压 IO 失败回落);saved 口径不变 |
| spec | `router-blob-compression` 两处 Requirement MODIFIED(见 delta;顺带校准原文本遗留的"zstd 流/试压"措辞为算法族真态) |
| 测试 | lib:四算法 roundtrip + 熵回落 + 空文件 + io 回落;e2e:真 store 三闸走全 + 监控三笔 |

## Impact

- **持久性红线(CLAUDE.md §3.1)逐项过**:内存恒 O(拷贝缓冲)—— 文件**全程不入内存**
  (§5.3);压缩任一环节失败(编码/临时盘 IO/后台 join)一律回落**原文件直传** —— 压缩
  永不让写入失败;写成功语义与多数派闸门零变化。
- 帧语义零新增:读口(`get`/`get_file`)的解压能力前批已全,本变更纯写侧。
- 每文件多一次本地临时盘写 + 读(CPU 流压单飞 `spawn_blocking`)—— 大件上传 CPU/盘
  IO 税,换来 ALL 副本网络/集群存储的压缩收益;多副本**只压一次**。
- `tempfile` 升为编译依赖(传递引入 fastrand 等小件)。

## Out of Scope

- `put_file` 分片(stitch)链路组合压缩 —— 网关自动分片已逐片走 Bytes 口全享。
- 流式"比率预判"不可能比率闸前置(须全量压完才知道)—— 本变更维持诚实判定:
  压完再判、不达标弃帧用原文。

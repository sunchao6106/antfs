# Proposal: router-blob-compression — SDK 层透明数据压缩(Ambry 上游对齐)

## Why(调研驱动的裁决)

本地 `~/code/opensource` 六系统实读:Ambry 上游在 **router 侧**压缩(`CompressionService`/`router.compression.*`:全局使能 + 上传时对象启发 + 比率不足回滚),store 对压缩**不感知**;SeaweedFS 在 needle 层无开关自动启发;CubeFS/SDFS/FastDFS 无落盘压缩。antfs 忠实 Ambry 架构(D2/D4 全程不透明字节),故取 **router SDK 面**:

- 扇出前压 ⇒ 复制带宽 3 副本全收红利;
- **磁盘格式零改**:store/复制(`?replication=1` 原样帧字节)/compaction(CRC copy)全程视 payload 为不透明 —— 持久性红线面零暴露;
- 自描述帧(magic+algo+orig_len)使读侧无需任何带外元数据。

## What Changes

| 位置 | 改动 |
|---|---|
| `crates/common/router/src/compression.rs`(新) | 帧编解码(zstd):`8B magic b"\x89ANTFSZ1"` + `algo u8(=1)` + `orig_len u64 LE` + zstd 流,共 17B 头;`maybe_compress`(三闸)/`decompress_frame`(bulk)/ `is_frame` 探测 |
| `crates/common/router/src/config.rs` | 新配置 5 项:`compression_enabled`(**默认 false**)· `compression_algorithm`(**默认 gzip**;zstd/lz4/snappy 可选)· `compression_min_size_bytes`(4096)· `compression_min_ratio`(1.05)· `compression_level`(1) |
| `crates/common/router/src/lib.rs` | put 路径(`put_keyed`/`put_auto_keyed`,Bytes 形态,含 composed 描述符)过三闸压缩;`get_from`(bulk 解压);`get_file`/`drain_to_file` 流式探测 + zstd 流解码(bounded channel + blocking 解码线程,内存 = 块) |
| metrics | `router_compression_attempts_total{outcome=kept\|too_small\|low_ratio\|disabled}` + `router_compression_saved_bytes_total`(零语义漂移,新增量) |
| 测试 | 帧矩阵(可压坡道/不可压熵/小体量/零长)、roundtrip(put→get/put→get_file)、默认关=逐字节零变化(铁证)、multipart 自动分片(auto_chunk 每片 Bytes 口全享)、复制不破(帧原样透传) |
| 文档 | `router.md` 压缩节 + **§7 双表(新配置 5 项×新监控 2 项必列)**;`common.md` 公共机制面;DEVLOG 步 83 |

## Impact

- **默认关 ⇒ 行为零变化**(主回归保障);开 ⇒ payload 换帧形态,store 全链不透明透传。
- REST HEAD 的 `x-antfs-blob-size` 对压缩对象呈 **stored size**(router.stat 读盘上长度)—— S3 层 HEAD 用 metastore 的 logical size 无感;记档为已知取舍(REST 面主消费按 GET 语义,GET 恒解压回原字节)。
- put_file 路径 v1 **不压**(大件流式取舍;antfs 主战场海量小文件 + S3/frontend 自动分片每片 4 MiB 走 Bytes 口全享)—— 记档。
- 压缩对象 write amplification 降、读 CPU 升(解压在 router 网关 —— 与 Ambry 同位);监控 provided 观测面。

## Out of Scope

- store 侧记录级压缩(格式 v5 方案,红线成本)与封存段级压缩(归 B2 EC 领域)。
- content-type 闸(antfs REST 层无类型概念;比率闸在语义上覆盖类型启发;S3 网关层类型面未来可另行透传)。
- put_file 压缩入口、mmap 页压缩、传输层(gzip)项。

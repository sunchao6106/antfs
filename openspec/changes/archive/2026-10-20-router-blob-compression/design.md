# Design: router-blob-compression

## D0 裁决链(2026-10-20,两轮所有者裁定)

1. 调研六本地开源系统(Ambry Java 上游带 router 级压缩;SeaweedFS needle 级无开关自动启发;CubeFS/SDFS/FastDFS 无)—— 所有者选 **Ambry 对齐路线**(SDK 面)。
2. 所有者追加:算法**可配置**(zstd / gzip / lz4 / snappy);帧头带算法号承载混存语义。
3. 所有者终裁:**默认算法 gzip**。

## D1 帧格式(自描述;store 全程不透明)

```
偏移  0..8   : magic  b"\x89ANTFSZ1"(PNG 风格 8B,高熵致伪检概率 ~2^-64)
偏移  8      : algo   u8(zstd=1 | gzip=2 | lz4=3 | snappy=4;新算法永远追加号,不复用)
偏移  9..17  : orig_len u64 LE(原始逻辑长度 = 解压容量上界 + 校验值)
偏移  17..   : 编码流(zstd stream / gzip stream / lz4 frame / snappy frame)
```

- 帧即 blob payload 全体:store 记录/索引/CRC/复制/compaction 均视为普通字节 → **磁盘格式零改(§3.4 免议)**;
- 帧族魔数刻意单一(非按算法区分):算法语义由 algo 字节承载,演进 = 追加新算法号(老读者遇未知号**响亮报错**,不静默;
  对齐 Ambry `Compression.java` "名字存数据、升级不兼容换名"的设计训);
- composed 通路正交:描述符与压缩同走 Bytes 口,`?composed=true` 位与数据帧两维度独立,拼装读回 router 解压得原描述符。

## D2 三闸门(put 侧;决策每次上传即时)

| 闸 | 条件(短路顺序) | 失败去向 |
|---|---|---|
| enabled | `compression_enabled = false`(**默认关**) | 原文直写(outcome=disabled) |
| min_size | `len < compression_min_size_bytes`(默认 4096;0 视为 1) | 原文(too_small) |
| ratio | 算法试压比率 `orig/stored >= compression_min_ratio`(默认 1.05) | 原文,压缩缓冲丢弃(low_ratio) |

- 全过 ⇒ 帧落盘(kept,`saved = orig − stored` 计入 saved_bytes);
- **算法选择是配置项**:`compression_algorithm`(默认 **gzip**;zstd/lz4/snappy 可选);改配置只影响新写入,老帧按算法号读回(集群混存演进);
- **无 content-type 闸**(antfs REST 层无类型概念;比率闸语义覆盖类型启发 —— 不可压数据自动回落;S3 层类型面属网关层未来透传项);
- **级别**:zstd 1..=19 / gzip 0..=9 / lz4、snappy 定档(忽略);默认** 1 = 速度面**;
- 编码器失败一律回落原文(压缩是优化,不因压缩器故障变成写入失败)。

## D3 读路径两口

- **bulk 口(`get`/`get_from`)**:Bytes 读回后 `is_frame` 判 17B 头 → 按算法号解压(`zstd::bulk` / gzip / lz4 / snappy 整块)→ 产出长度必须恰等 `orig_len`;非帧原样透传;帧损坏(伪 magic/未知算法/长度不符)→ 以 `ReplicaStatus{status:500,detail}` **按副本损坏上报**,上层 get 的 R=1 循环自然换下一副本重试;
- **流式口(`get_file`/`drain_to_file`)**:
  1. 窥满 17B(peek 期溢收块存 `carry`,**判定之后才分派** —— 修过的 bug:不能判帧前引去解码管道);
  2. 非帧 → head + 溢收块补写,原直落盘路径;
  3. 帧 → 单条有界 channel(容量 4)喂网络块 → `spawn_blocking` 解码线程(`Read` 桥拉通道 + 全家 Reader Decoder)→ 逐块(64 KiB)write_all → 产出累计校验等于 `orig_len`(超过/不足都响错,不产截短文件)→ fsync;
  内存 = 两 channel + 解码块(64 KiB 级),与 orig 大小无关。

## D4 复制/CRC/compaction 正交性(证明链)

- 拉复制 `?replication=1` 返回**帧原样字节** → 接收方普通落盘 ⇒ 副本同为帧;副本读回经 bulk/流式口照常解压 —— 内容一致性由"同帧字节"保证(e2e `compression_survives_replication_frame_passthrough` 钉住);
- 引擎 CRC 覆盖帧字节;compaction 搬帧;scrub 比对帧 CRC —— 全部结算在"字节不变"公理,压缩层无介入点。

## D5 配置 & 监控(§7 双表同步)

- `RouterConfig` 新 5 项:`compression_enabled`(false)· `compression_algorithm`(gzip)· `compression_min_size_bytes`(4096)· `compression_min_ratio`(1.05)· `compression_level`(1);
- 监控 2 项:`router_compression_attempts_total{outcome=kept|too_small|low_ratio|disabled}` 与 `router_compression_saved_bytes_total`。

## D6 已知取舍(记档)

- REST HEAD 的 `x-antfs-blob-size` 对压缩对象呈 **stored size**(store 索引只认盘上字节;S3 层 HEAD 读 metastore 的 logical size 无感 —— 主消费面不受影响;REST 面 GET 恒解压回原字节);
- `put_file` v1 不圧(大件流式取舍;antfs 主战场海量小文件 + S3/frontend 自动分片每片 4 MiB 走 Bytes 口全享);
- RouterConfig 的 `Eq` 派生因 f64 字段退到 PartialEq(下游 frontend/s3 的 Eq 派生同步放宽 —— 无语义面损耗,f64 本不适合 Eq)。

## D7 测试证据链

1. codec 单测矩阵(4 算法 × kept/roundtrip/too_small/low_ratio/disabled/零长)+ 非帧透传 + orig_len 谎报响错 + 未知算法号响错 + 4 算法混存齐读;
2. e2e(真 store 三副本 harness):默认关逐字节零变化(disabled 计数)+ 开态 zstd kept/节省/流式 get_file 产出=orig_len 恒等 + too_small 原样 + RF=2 复制帧透传;
3. devcluster 真面:关态(默认)5 MiB 自动分片 roundtrip;开态(**gzip 默认算法**)5 MiB 分片 roundtrip + `router_compression_attempts_total` 实增。

## Risks / Rollback

- 默认关 = 主保障;开后的 HEAD size 口径漂移与 put_file 不压已记档;回滚 = revert 单 commit。
- 非目标(store 记录级/段级压缩、类型闸)见 proposal Out of Scope。

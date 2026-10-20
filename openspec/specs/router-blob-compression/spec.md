# router-blob-compression Specification

## Purpose
TBD - created by archiving change router-blob-compression. Update Purpose after archive.
## Requirements
### Requirement: SDK 在写路径按三闸做透明压缩
Router SDK 的全部写入口 —— Bytes 形态(`put`/`put_with_ttl`/`put_composed*`/`put_auto*`)与文件形态(`put_file`/`put_file_with_ttl`)—— SHALL 在 `compression_enabled` 为真时对每次写入做同一三闸判定:体量低于 `compression_min_size_bytes`(0/空文件以 1 折算)或试压比率不足 `compression_min_ratio` 的对象 SHALL 以原文写入(不产生部分压缩态);判定通过的 SHALL 以 17B 自描述帧(`\x89ANTFSZ1` + algo u8 + orig_len u64 + 编码流;算法族可配 zstd/gzip/lz4/snappy,默认 `gzip`)为 payload 落入存储,级别受 `compression_level` 约束(zstd 1..=19 / gzip 0..=9,lz4/snappy 内建档);文件形态 SHALL 为流式临时帧文件:客户端先行把帧压进本地临时文件(内存恒 O(拷贝缓冲),不随文件大小增长),该次上传的全部副本并行 PUT 与换分区重试 SHALL 共用同一帧、上传结束自动清删,预压缩任一环节失败(编码/临时文件 IO/后台汇合)SHALL 回落原文件直传并以 `io` 记入 attempts 监控 —— 压缩 SHALL 永不让写入失败;`compression_enabled` 为假(默认)时 SHALL 行为与无该特性完全一致。

#### Scenario: 可压缩对象节省且读回恒等
- **WHEN** 开启压缩后写入一段可压缩数据并读回
- **THEN** 集群内存储字节为帧形态(带 17B 头)且短于原文,读回字节与原文逐字节一致

#### Scenario: 熵数据与小对象原样落盘
- **WHEN** 写入高熵或小于 min_size 的数据
- **THEN** 落盘字节与原文一致(无帧头),监控 attempts 记 low_ratio / too_small

#### Scenario: put_file 流式帧与多副本共用
- **WHEN** 开启压缩后经 `put_file` 写入一个可通过三闸的本地文件
- **THEN** 全部副本收到同一帧内容,读回文件与原文件逐字节一致且逻辑大小等于原文件大小,上传结束后客户端本地不再残留临时帧文件

#### Scenario: put_file 压缩失败回落原文
- **WHEN** 预压缩途中 IO 失败(如临时目录不可写)
- **THEN** 该写仍以原文件直传完成且读回恒等,监控 attempts 记 `io`

#### Scenario: 默认关为零行为变化
- **WHEN** 未显式开启 `compression_enabled`
- **THEN** 所有读写路径行为与该特性引入前逐字节一致

### Requirement: 读侧对帧透明解压,bulk 与流式两口等价
SDK 读 SHALL 对帧对象自动解压回原始字节:`get`(bulk)读后探测解压;`get_file`(流式)SHALL 以窥探帧头判定,帧对象经流式 zstd 解码写盘,内存占用与对象大小无关,且解码累计长度 SHALL 校验等于帧头 `orig_len`(不符即报错,不产出截短文件)。非帧对象 SHALL 原样返回/落盘。

#### Scenario: 流式口大对象 roundtrip
- **WHEN** 压缩开启后写入大于解压分块上限的对象经 `get_file` 读回
- **THEN** 落盘文件字节与原文一致,内存不随对象大小增长

### Requirement: 存储集群对压缩不感知
store 数据面、复制(含 `?replication=1` 拉)、compaction、CRC/scrub SHALL 全程以不透明字节处理帧对象 —— 复制后的副本与原副本保存同一帧字节,拼装描述符对象(FLAGS composed)可与压缩正交叠加;监控 SHALL 以 `router_compression_attempts_total{outcome}`(kept|too_small|low_ratio|disabled|io,`io` = put_file 流压失败回落,`router-putfile-compression`)与 `router_compression_saved_bytes_total` 观测压缩面。

#### Scenario: 复制透传保持内容一致
- **WHEN** 压缩开启后写入对象(含经 `put_file`)并经复制收敛所有副本
- **THEN** 任意副本读回字节与原文一致


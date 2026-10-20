# router-blob-compression Specification

## Purpose
TBD - created by archiving change router-blob-compression. Update Purpose after archive.
## Requirements
### Requirement: SDK 在写路径按三闸做透明压缩
Router SDK 的 Bytes 形态写入(`put`/`put_with_ttl`/`put_composed*`/`put_auto*`)SHALL 在 `compression_enabled` 为真时对每次写入做即时判定:体量低于 `compression_min_size_bytes` 或 zstd 试压比率不足 `compression_min_ratio` 的对象 SHALL 以原文写入(不产生部分压缩态);判定通过的 SHALL 以 17B 自描述帧(`\x89ANTFSZ1` + algo + orig_len + zstd 流)为 payload 落入存储。`compression_enabled` 为假(默认)时 SHALL 行为与无该特性完全一致。zstd level SHALL 受 `compression_level` 约束(1..=19)。

#### Scenario: 可压缩对象节省且读回恒等
- **WHEN** 开启压缩后写入一段可压缩数据并读回
- **THEN** 集群内存储字节为帧形态(带 17B 头)且短于原文,读回字节与原文逐字节一致

#### Scenario: 熵数据与小对象原样落盘
- **WHEN** 写入高熵或小于 min_size 的数据
- **THEN** 落盘字节与原文一致(无帧头),监控 attempts 计 low_ratio / too_small

#### Scenario: 默认关为零行为变化
- **WHEN** 未显式开启 `compression_enabled`
- **THEN** 所有读写路径行为与该特性引入前逐字节一致

### Requirement: 读侧对帧透明解压,bulk 与流式两口等价
SDK 读 SHALL 对帧对象自动解压回原始字节:`get`(bulk)读后探测解压;`get_file`(流式)SHALL 以窥探帧头判定,帧对象经流式 zstd 解码写盘,内存占用与对象大小无关,且解码累计长度 SHALL 校验等于帧头 `orig_len`(不符即报错,不产出截短文件)。非帧对象 SHALL 原样返回/落盘。

#### Scenario: 流式口大对象 roundtrip
- **WHEN** 压缩开启后写入大于解压分块上限的对象经 `get_file` 读回
- **THEN** 落盘文件字节与原文一致,内存不随对象大小增长

### Requirement: 存储集群对压缩不感知
store 数据面、复制(含 `?replication=1` 拉)、compaction、CRC/scrub SHALL 全程以不透明字节处理帧对象 —— 复制后的副本与原副本保存同一帧字节,拼装描述符对象(FLAGS composed)可与压缩正交叠加。监控 SHALL 以 `router_compression_attempts_total{outcome}`(kept|too_small|low_ratio|disabled)与 `router_compression_saved_bytes_total` 观测压缩面。

#### Scenario: 复制透传保持内容一致
- **WHEN** 压缩开启后写入对象并经复制收敛所有副本
- **THEN** 任意副本读回字节与原文一致


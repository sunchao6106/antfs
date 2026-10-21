# upload-zero-spool 规约

> 裁决生效后即验收契约;变体改选须先改本文。

## ADDED Requirements

### Requirement: 客户端上传压缩零临时盘(B 段)
router SDK 的 `put_file` 系上传承诺用户侧零中转落盘:压缩帧 SHALL 在内存(`Bytes`)内完成且尺寸受可配上界约束(`router.compression_frame_mem_max_bytes`,默认 8 MiB,0 = 关闭内存帧),编码累计超上界 SHALL 以监控标签 `spill` 记录并回落原文直传; retained 帧判定的比率 SHALL 仍基于实际编码尺寸;上传全程(任意体量、任意闸 outcome)SHALL NOT 在客户端本地(含 TMPDIR)创建任何文件;内存帧 SHALL 以引用克隆方式供多副本并传与 507 换分区重试共用。

#### Scenario: 内存上限内的可压文件
- **WHEN** put_file 上传可压文件且压缩帧尺寸 ≤ 上界
- **THEN** 三闸判定与 Bytes 口逐语义一致,临时目录文件数增量为零,读回逻辑等长,监控 outcome=kept

#### Scenario: 超上限回落原文
- **WHEN** 压缩帧在累计写入中超过上界
- **THEN** 立即停止编码、以 outcome=spill 计数、原文直传且逻辑等长读回

### Requirement: 服务端大对象 PUT 零暂存(A 段)
store 数据面带 Content-Length 且超过内存直进阈值的 PUT SHALL 经 log 预留区直写落盘而不产生任何暂存文件;大对象专区(lane 段)的 commit SHALL 维持段内 `data_end` 前缀推进语义,小对象直行道的时延形态 SHALL 不受同时在途大对象传输影响;上传中途失败(字节数不符、空闲超时、客户端断开)SHALL 使预留回收(无空洞、`data_end` 不动)且该次写对读者与恢复完全不可见;持久化边界与恢复判定 SHALL 保持"flush 是唯一持久化边界、恢复只认上次 flush 的 data_end";磁盘格式 SHALL 零变更。

#### Scenario: 大对象全程无暂存文件
- **WHEN** 100 MiB 带 CL PUT 完成并读回
- **THEN** 201、读回逐字节一致、(各盘与 staging)tmp 目录内容增量为零

#### Scenario: 传输中途断开
- **WHEN** body 少于 CL 或客户端中途断开
- **THEN** 400、`data_end` 与索引不动、预留逻辑起点回卷、后续 PUT 复用该区间

#### Scenario: 大对象在途不拖小对象
- **WHEN** 大对象慢速传输中并发小对象 PUT
- **THEN** 小对象 201 的 ack 延迟与无大对象在途的对照组一致(确定性对照,禁 sleep)

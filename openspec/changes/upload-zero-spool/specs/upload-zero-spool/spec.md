# upload-zero-spool 规约

> 裁决生效后即验收契约;变体改选须先改本文。

## ADDED Requirements

### Requirement: 客户端上传压缩零临时盘(B 段)
router SDK 的 `put_file` 系上传承诺用户侧零中转落盘且压缩覆盖**任意体量**:压缩帧尺寸 ≤ 可配上界(`router.compression_frame_mem_max_bytes`,默认 8 MiB,0 = 关闭内存帧)时 SHALL 以内存帧(`Bytes`)完成并以引用克隆供多副本并传与 507 换分区重试共用;累计超上界时 SHALL 转入两遍流式 —— 第一遍全程计数编码取得真实压缩尺寸并据其做比率判定,判定通过后 SHALL 逐副本流式编码上送且 `Content-Length` 恰为第一遍所数尺寸(编码确定性保证逐字节一致,字节数不符 SHALL 被服务端拒收且不落任何记录);比率判定 SHALL 在任意体量均基于实际编码尺寸而非估计;上传全程(任意体量、任意闸 outcome)SHALL NOT 在客户端本地(含 TMPDIR)创建任何文件;超上界 SHALL NOT 回落"不压缩原文直传"(仅比率不过时可回落原文)。

#### Scenario: 内存上限内的可压文件
- **WHEN** put_file 上传可压文件且压缩帧尺寸 ≤ 上界
- **THEN** 三闸判定与 Bytes 口逐语义一致,临时目录文件数增量为零,读回逻辑等长,监控 outcome=kept

#### Scenario: 超上限转两遍流式(压缩不放弃)
- **WHEN** 可压文件的压缩帧累计写入超过上界
- **THEN** 转两遍流式:计数判定过闸、逐副本边压边传,读回逻辑等长、临时目录文件数增量为零,监控 outcome=two_pass

#### Scenario: 两遍确定性与 CL 恰等
- **WHEN** 同一文件两轮完整传输(含 507 换分区重试)
- **THEN** 两轮实际传输的帧字节完全一致,声明的 Content-Length 恰等于实际字节数;构造偏差时服务端 400 拒收且库中无半记录

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

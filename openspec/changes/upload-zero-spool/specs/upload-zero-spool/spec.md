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

### Requirement: S3 网关单文件上限双闸(⑥,2026-10-21)
S3 网关 SHALL 对 `PutObject` 与 multipart upload 执行与 REST 网关**同一值**的公共单文件上限(`[upload] max_object_bytes`,顶层公共表):PutObject 声明的对象大小(aws-chunked 以 `x-amz-decoded-content-length` 为准)超限 SHALL 在读 body 之前回 413 `EntityTooLarge`;S3 multipart upload 的 CompleteMultipartUpload SHALL 按 manifest 所列各片实际尺寸合计对账,超限 SHALL 回 413 且 session 与 parts 行保持不动(客户端可 abort 或缩减后重发);片单件 ≤ `max_body_bytes` 不受影响;S3 网关的 PutObject/UploadPart 仍 SHALL 零本地临时文件。

#### Scenario: 单传超限
- **WHEN** PutObject 声明(CL 或解码长度)超过 `[upload] max_object_bytes`
- **THEN** 413 EntityTooLarge,body 不读

#### Scenario: multipart 合计超限
- **WHEN** 两片各 640 KiB(单片 ≤ max_body_bytes=4 MiB)合计 1.25 MiB > 上限 1 MiB,发 CompleteMultipartUpload
- **THEN** 413 EntityTooLarge;session 保留(ListParts 200),Abort 204,对象不存在(GET 404)

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

### Requirement: 网关上传恒切零暂存与公共上限(墙③,④+⑤+⑥+⑦ 裁决)
frontend 网关的 `POST /blobs` 上传 SHALL NOT 在本地(含 TMPDIR)写入任何文件且 SHALL 恒切(auto_chunk_enabled 废弃,2026-10-21):有声明的 body ≤ `max_body_bytes` 时 SHALL 全程有界内存缓冲且仅在字节数与声明核验一致后才写入;超过 `max_body_bytes` 或无声明长度时 SHALL 边接收边按 `auto_chunk_bytes` 分片逐片入库,传输未达 EOF、字节数与声明不符、片数超限或任一片失败时 SHALL 以墓碑清理由此已入库的全部片并回 400;**公共单文件上限 `[upload] max_object_bytes`(REST 与 S3 网关共用的顶层公共表)超过 SHALL 413**(有声明读 body 前拒,无声明边收边超已入库片墓碑回收后 413);成功 SHALL 以拼装描述符原子发布且对象对外语义(大小/GET/HEAD/拼装)与既有形态一致;**启用 `frontend.upload_session_wal_dir` 后 SHALL 全程记账**(start/逐片 chunk/commit/abort)且超时孤儿会话(最后记账 6 小时无终态)SHALL 由清扫任务逐片墓碑并核销;`GET /blobs` 大对象的既有中转属下载面,不在本契约(维持现状另裁)。

#### Scenario: 大对象上传全程零文件
- **WHEN** 100 MiB 带声明的 POST /blobs 完成并 GET 读回
- **THEN** 201 + 描述符发布、读回逐字节一致、TMPDIR 文件数增量为零

#### Scenario: 断流清理
- **WHEN** 大对象传输中途断开或实发与声明不符
- **THEN** 已入库各片被逐一墓碑删除、回 400、`frontend_upload_chunk_cleanups_total` 递增

#### Scenario: 中小对象语义不变
- **WHEN** 有声明的 body ≤ `max_body_bytes` 的上传
- **THEN** 单 blob 落库(非拼装)、内存有界、等长核验失败时 400 且零片写入

### Requirement: router 层恒切与透明逻辑读(⑧/⑨/⑩/⑭,2026-10-21)
router SDK SHALL 对超过 `router.put_bytes_chunk_bytes`(默认 4 MiB,64 KiB..=1 GiB,**禁 0** —— 2026-10-22 裁决:恒切不可关)的 Bytes 写(`put`/`put_auto` 族)内部切片 —— 片 SHALL 为普通多数派 blob 且经发号轮转散向可写分区,尾笔 SHALL 以 composed 位写拼装描述符并返回其 blobId;任一失败 SHALL 尽力墓碑已入库片并透传原错误,超容量(`MAX_CHUNKS × 片宽`)SHALL 前置拒绝且一字节不写;读侧 SHALL 透明拼装:`get` 返回逻辑对象字节、`get_object_stream`/`get_object_range_stream` 流式且内存有界不落盘、`stat_object` 报逻辑大小与片数,裸字节 SHALL 仅存于受控口 `get_blob`,落盘式读接口 SHALL 不存在(§⑭);内嵌 router 配置的网关 SHALL 在装配期交叉校验"网关片/单体宽 ≤ router 片宽",违者 SHALL 拒绝启动;router 自身 SHALL 拒绝片宽 0
(恒切无关闭态)。

#### Scenario: SDK 大对象直写与读回
- **WHEN** 直连 SDK 用户 `put_auto(17 MiB)`(片宽 4 MiB)
- **THEN** 5 片落库且分属两个可写分区;`get` 逐字节等于原文;`stat_object` 报 17 MiB / 5 片;`get_blob` 返回描述符字节

#### Scenario: 失败回收
- **WHEN** `put_with_ttl` 撞既有 key(描述符落 key 409)
- **THEN** 返回 AlreadyExists;已写片逐片墓碑(`router_put_chunked_orphan_deletes_total`)且原 key 旧对象不受扰

#### Scenario: 网关误配结构性拒绝
- **WHEN** frontend TOML 设 `router.put_bytes_chunk_bytes=64 KiB` 而 `auto_chunk_bytes=4 MiB`
- **THEN** 启动校验拒绝(网关片将被二次切)

## ADDED Requirements(2026-10-22 追补)

### Requirement: S3 单笔直传流式内部 multipart

S3 网关 SHALL 接受申报尺寸 > `s3.max_body_bytes` 且 ≤ 共享单文件上限(`[upload] max_object_bytes`)
的 PutObject:网关 SHALL 以固定片宽(= `max_body_bytes`)缓冲整流、逐片经约定的多数派写落地并
登记内部 multipart session 片行,收完 SHALL 走与显式 multipart 同一 complete 干道(上限对账、
complete 级 FULL_OBJECT 校验按 manifest 流式复验、配额、遮蔽丢弃回收);任何中途失败(断流/
尺寸不符/校验失配/配额拒)SHALL 墓碑已写片并清 session 行;成功响应的 ETag SHALL 为 multipart
式合成口径。SSE 申报 SHALL 拒 501(与显式 multipart 同族)。

#### Scenario: 大对象直传零整存往返
- **WHEN** 客户端单笔 PUT 申报 9 MiB(max_body 4 MiB,总上限 16 GiB)
- **THEN** 200;内部片 = 3;GET 回读逐字节恒等;HEAD 逻辑尺寸;网关内存峰值 = 1 片

#### Scenario: 坏校验声明清场
- **WHEN** 整对象 checksum 声明值失配
- **THEN** 400 BadDigest;GET 该对象 404;session 片行零残留(abort 已墓碑全部内部片)

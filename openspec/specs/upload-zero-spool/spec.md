# upload-zero-spool 能力规约

> 2026-10-26 自变更 `upload-zero-spool` 归档同步而生;裁决链 ①-⑱(含 B 作废/A 冻结/#17/#18
> 终局)见 `openspec/changes/archive/2026-10-26-upload-zero-spool/`。

## Purpose

上传全链路零暂存与公共上限的验收契约:网关恒切边收边分片(墙③)、S3/REST 同值的单文件上限
双闸、router 层恒切与透明逻辑读、S3 单笔大直传流式内部 multipart、store PUT 两岔零暂存,
以及分体蓝图无法静态校验时的运行期错配显形。

## Requirements

### Requirement: S3 网关单文件上限双闸(⑥,2026-10-21)
S3 网关 SHALL 对 `PutObject` 与 multipart upload 执行与 REST 网关**同一值**的公共单文件上限(`[upload] max_object_bytes`,顶层公共表):PutObject 声明的对象大小(aws-chunked 以 `x-amz-decoded-content-length` 为准)超限 SHALL 在读 body 之前回 413 `EntityTooLarge`;S3 multipart upload 的 CompleteMultipartUpload SHALL 按 manifest 所列各片实际尺寸合计对账,超限 SHALL 回 413 且 session 与 parts 行保持不动(客户端可 abort 或缩减后重发);片单件 ≤ `max_body_bytes` 不受影响;S3 网关的 PutObject/UploadPart 仍 SHALL 零本地临时文件。

#### Scenario: 单传超限
- **WHEN** PutObject 声明(CL 或解码长度)超过 `[upload] max_object_bytes`
- **THEN** 413 EntityTooLarge,body 不读

#### Scenario: multipart 合计超限
- **WHEN** 两片各 640 KiB(单片 ≤ max_body_bytes=4 MiB)合计 1.25 MiB > 上限 1 MiB,发 CompleteMultipartUpload
- **THEN** 413 EntityTooLarge;session 保留(ListParts 200),Abort 204,对象不存在(GET 404)

### Requirement: 服务端 PUT 两岔零暂存(#17/#18 终局口径,2026-10-22)

store 数据面 PUT SHALL 只有两岔:带 `Content-Length` 且超过 `store_server.max_body_bytes`
SHALL 在读 body 前回 413;其余(带 CL ≤ 上限,以及无 CL 的 chunked 回落)SHALL **内存整收**
后一次写入 —— 带 CL 时以声明值为缓冲上限且收完核验字节恰等(不符 SHALL 400 且零记录),无 CL
时以收单上限兜底(超限 413)。SHALL NOT 产生任何中转暂存文件(暂存路径连同 `staging_dir`
整体退役;mount 期清扫仅为升级部署收历史 `tmp/*.tmp` 残渣);每请求内存上界 SHALL =
`max_body_bytes`(**默认 5 MiB,盖过 router 默认片宽 4 MiB —— 分体裸默认自洽**,原
"对齐 axum 2 MiB"口径作废),聚合内存 = 在途请求数 × 上限(在途不受限,与旧小对象路径
同口径;并发闸被否决,极简形态;A 段 lane/预留区机制冻结不作,重启条件见变更档案)。

#### Scenario: 片宽级单笔直写零文件
- **WHEN** 带 CL 3 MiB(片宽 4 MiB、盘上限 8 MiB)的 PUT 完成并读回
- **THEN** 201、读回逐字节一致、`tmp/` 目录从未被创建

#### Scenario: 断连半截不可见
- **WHEN** 声明 4 MiB、只发 64 KiB 后断开连接
- **THEN** 对象不可见(404)、盘上无任何中转文件、库中无半条记录

#### Scenario: 超上限 413
- **WHEN** 带声明长度超过 `store_server.max_body_bytes`
- **THEN** 收 body 前回 413、引擎零触动(跨层错配的运行期显形见"单笔上限错配"requirement)

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

### Requirement: store 单笔上限错配的运行期显式化

对分体部署 TOML 之间无法静态交叉校验的"网关/router 恒切片宽 ≤ store `max_body_bytes`"红线,router SHALL 在运行期把 store 413 拒收升为独立错误家族 `body_too_large`(区别于恒切容量 `too_large`),错误分类、多数派失败聚合 detail 与 `router_put_replica_errors_total` 标签 SHALL 可辨;frontend SHALL 映射 413、S3 SHALL 映射 EntityTooLarge;devcluster 一体配置 SHALL 在装配期断言该红线。

#### Scenario: 片宽错配以专属家族显形
- **WHEN** store `max_body_bytes` 被配为低于片宽,router 发出单笔写
- **THEN** 多数派失败聚合 detail 含 `body_too_large×N`;`router_put_replica_errors_total{kind="body_too_large"}` 计数;frontend 回 413、S3 回 EntityTooLarge

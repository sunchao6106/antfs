# Design: blob-range-read — 调研证据链与裁决点

> 调研日期:2026-10-21。证据 = antfs 本地源码 file:line + Ambry 本地源码
> (`~/code/opensource/ambry`,master)file:line,全部亲自读取(子代理因环境模型配置故障不可用)。

## 1. Ambry 先例(忠实对齐论证)

| 环节 | Ambry 实现 | 证据 |
|---|---|---|
| REST 语义 | `Range: bytes=s-e` / `bytes=s-` / `bytes=-k` 单区间三种形态;语法坏 → 400 类;越界 → `RangeNotSatisfiable`(416);206 + `Content-Range: bytes a-b/size` | `ambry-api/.../rest/RestUtils.java` L1503-1522 `buildByteRange`、L881-890 `buildContentRangeAndLength` |
| 响应装配 | `range != null → 206`;**HEAD 带 Range 也回 206**(无 body);`If-Modified-Since` → 304;`x-ambry-resolve-range-on-empty-blob` 特殊头 | `ambry-frontend/.../AmbrySecurityService.java` L247/L269 |
| 子资源限制 | `Range` 只允许 Blob 本体与 `Segment` 子资源,其余子资源带 Range 直接报错 | `RestUtils.java` L852-856 |
| Router | `GetBlobOptions` 原生 `ByteRange range` 字段(`ambry-api/.../router/GetBlobOptions.java`);复合 blob **只取覆盖窗口的 chunk**(`getStoreKeysInByteRange`,`GetBlobOperation.java` L1773/L2023);单一 blob = store 整读 + router `filterChunkToRange` 切片(L1394) |  |
| **Store 协议** | **`PartitionRequestInfo` 只有 partitionId + blobIds,无 offset/length** —— Ambry 的 store 层不做窗口读 | `ambry-protocol/.../PartitionRequestInfo.java` L32-50 |
| Range 模型 | `ByteRange` 三类型(LAST_N_BYTES / FROM_START_OFFSET / OFFSET_RANGE),`toResolvedByteRange(totalSize)` 统一解析 | `ambry-api/.../router/ByteRange.java` |

**结论**:frontend `/blobs` 加 Range = 忠实对齐(该补);store 数据面窗口读 = 超越 Ambry 的
优化项,按"局部替换为业界最佳"原则走本变更显式记档。

## 2. antfs 现状盘点(逐层 file:line)

| 层 | 现状 | 差距/机会 |
|---|---|---|
| 引擎 `crates/blob/src/blob.rs` | `read_range(key, offset, len)` L1210 已存在(`store-read-streaming` 落地):校验口径同 `get`(24B 头重验,不重验整条 CRC —— 既有裁决),越界 → Corrupt;`read_range_matrix_and_chunked_equivalence` 属性测试把守 | 零差距,原语现成 |
| store 数据面 `crates/store/src/server.rs` | 流式 GET `StreamingRead{offset: 0 硬编码, remain}` L1381;`GetParams` query 抽取先例(`replication=1`)L1319;`classify` 对带未知 query 的普通 GET 仍判 `DataOp::Get`(`request_control.rs` L72-78,按 `replication=` 键值区分,未知 query 不误伤) | 起始 offset 参数化 + remain=窗口长 |
| router `crates/common/router/src/lib.rs` | 读口 `get`(L1092,逻辑对象)/`get_blob`(L1099,裸整读)/`get_file`(L1176,流式落盘);R=1 迭代器 `read_from_all` L1147 / `open_replica_stream` L1185 现成;读路径 `compression::decode_frame` 透明解压(L1247) | 缺 `GetBlobOptions.range` 等价物 |
| frontend `crates/frontend/src/server.rs` | `download` L836 整对象语义:小对象整读,大对象 `get_file` 落暂存盘再流式;composed 对象 `stitched_body` L1094 逐片**全部**取回 | 10⁴ 倍浪费点;窗口下免暂存盘 |
| S3 网关 `crates/s3` | 单区间 Range 语义全套已有(`handlers/range.rs` `parse_range`/`ByteWindow`/`window_parts`,纯函数可复用);`get_plain_object` 整读后切片(object.rs L447);multipart 边界片整片取回 | 网关层浪费有界(≤4 MiB/次) |

## 3. 裁决点(实施前必须定)

### D1 压缩帧 × 窗口读(本变更最大风险点)

router 读路径对磁盘字节做压缩帧透明解压 —— store 窗口读操作的是**磁盘字节**,压缩帧上
直接开窗会切坏帧。Ambry 口径:store 永远整读,router 在**解压后**坐标上切片。

候选:
- **(a) 拒绝**:带压缩帧的 blob 拒绝窗口读,返回明确错误(建议 400/415 类),调用方退整读。
  语义响亮、实现最小;代价 = 压缩 blob 无 Range 能力(可观测)。
- (b) stat 判别 + 整读退化为网关切片:对压缩 blob 自动降级整读,窗口在上层切。
  调用方无感;代价 = 静默放大(与"预期条件不打 error"冲突面小但存在)。

**倾向 (a)**:显式失败优于静默放大;`get_range` 错误带 blobId + is_compressed 语境,便于
调用方决策。P0 需补调研:压缩默认开关与命中率(决定拒绝路径触发面)。

### D2 数据面窗口参数语义

数据面(内部 RPC 性质)不走 RFC 7233 全套:`?offset=&length=` 直接语义,越界 → 416
(或 400,提案定稿时定);206/`Content-Range` 装配留给网关层。`length` 省略 = 读到尾
(对齐 `bytes=s-`),`offset` 省略 = 0。

### D3 frontend HEAD × Range

Ambry HEAD 带 Range 回 206(无 body)。antfs frontend HEAD 现不处理 Range;提案需显式
裁决:对齐 Ambry(206)或对齐 s3 crate 现口径(HEAD 不处理 Range)。**建议对齐 Ambry**
(忠实优先,两网关行为分化记档)。

### D4 已删/过期/复制读边界(不可协商)

- `read_range` 校验口径已同构 `get`(Deleted/Expired/NotFound)—— 窗口化**不绕过**拒读;
- `?replication=1` 整读契约(include_expired + 整条 CRC)**零触碰**;
- CRC 取舍沿用 `store-read-streaming` 裁决:切片不构成 CRC 覆盖单元,bit-rot 防线 =
  复制整读 + scrub 巡检。

### D5 远期交互

D5 EC 冷层落地后,EC 段上的窗口读需条带对齐 —— 当前全副本不阻塞;本变更 design 档
记录,EC 提案时必须回访。

## 4. `classify`/`path_op` 兼容性(已验证不构成障碍)

`request_control.rs`:普通 GET + 未知 query(非 `replication=`)→ 仍判 `DataOp::Get`
(L72-78);`path_op` 把 `GET blobs/{key}` 归 `get`/`replication_get` 只看 method+path+replication
query(L113-129)。`?offset=&length=` 不改变分类;但新增数据面参数后需过
`path_op_names_every_data_plane_route` 测试门。

## 5. 残留调研项(P0 内闭环)

- 压缩默认开关与命中率(D1 前置);
- `/blobs` 调用方画像:整取 vs 部分读需求占比(影响 P3 优先级,不影响"该补"定性);
- S3 `max_body_bytes`(4 MiB)与 auto-chunk(4 MiB)边界对象的实际占比(P4 收益预估)。

# s3-checksums 设计

## Context

写读路径现状:

- `put_object`(object.rs):`read_bounded_body` 全缓冲(≤ `max_body_bytes`,SSE
  前明文在手)→ md5 etag → blob 写 → 行写;**校验头零消费**;
- `upload_part`(multipart.rs):同构缓冲路径,同为零消费;
- 分帧:auth 中间件 `wrap_request` 把 body 换成 `AwsChunkedBody`(decode 在流内);
  trailer 头行在 poll 中解析后**丢弃**(chunked.rs「trailer header ignored」),
  归档裁决记为「值不验证(UNSIGNED 信任面)」;`x-amz-trailer` 声明头未被消费;
- metastore:`named_objects`(001 + 002/003/004 迁移先例,sqlx migrate! 启动跑),
  行死于 `PgObjectRow`/MySQL 对应 Row 结构;`PutObjectMeta` 12 字段直插。
- GetObjectAttributes:三属回显,Checksum/ObjectParts「501 招牌」。

## Goals / Non-Goals

**Goals:** 四族校验在写路径全量验证(头 + trailer 两种载体);恰一份持久化 +
三面回显(PUT 响应/GET+HEAD on-mode/attributes);fail-fast(验证先行,零孤儿 blob);
信任面闭合值不留死(块签名链不动)。

**Non-Goals:** CRC64NVME;multipart complete 级(composite 合并 / FULL_OBJECT 派生——
需要 per-part 持久化,成套另立档);拷贝产物补算;`Content-MD5` 老族(维持 etag);
 trailer 段自身的签名验证(规格外 / S3 同不做);MultipartChecksumMode/查询 API 改动。

## Decisions

### D1:registry 形态——四算法、base64 严格、错误三态

`ChecksumAlgo{Crc32,Crc32c,Sha1,Sha256}`:`header()`(如 `x-amz-checksum-crc32`)、
`xml_elem()`(`ChecksumCRC32C`)、`digest_len()`(4/4/20/32)。值编码 = **标准 base64
带填充**(AWS 口径)。请求侧判定三态:已知族 → 参与验证;`x-amz-checksum-crc64nvme`
等未知同族头 → **501 `NotImplemented`(点名头)**;`x-amz-sdk-checksum-algorithm`
(SDK 自报算法)与 `x-amz-checksum-type: FULL_OBJECT` 为信息面(其余 type 值 400)。
digest 长度对不上算法 → `InvalidArgument`(不是 BadDigest——那是「算了不匹配」,
「根本不是合法值」先拒)。

### D2:验证时序——先于任何 blob 写,失败零痕迹

`put_object`/`upload_part` 在 etag 计算处顺路计算所需各算法(read 已全缓冲,单遍
多哈希成本 ≤ 4×安全哈希,量级无感)。**校验失败(坏 base64/长度/失配)在 router.put
之前 return** → 无 blob 写、无行写、无墓碑——与既有「配额拒绝同请求墓碑」的存活性
面完全正交,不需要兜底路径。失配 → 新变体 `S3Error::BadDigest{algorithm}`(400,
message 携带头名与两侧值形态——观测面友好)。

### D3:持久化——migration 005 可空双列,存「最强一份」

`checksum_algo VARCHAR(8) NULL` + `checksum_b64 VARCHAR(64) NULL`(SHA256 base64 = 44
字符,64 足够)。多算法同发(头×N、头+trailer 跨算法):**全部验证**,存
`SHA256 > SHA1 > CRC32C > CRC32` 优先级恰好一份(AWS SDK 请求常见单算法;并存时不
丢验证面,回显面拿单一权威值——文档记优先级)。SSE:按**明文**算/存/回显
(与 size/etag 的「持久代语义 = 明文」既有口径同源,加密层无感)。历史行/拷贝/
multipart 装配行 = NULL → 回显面自然缺省(头不发),无回填。

### D4:trailer 承接——decoder 收集、extensions 上抛、写路径消费

`chunked.rs`:`AwsChunkedBody` 增字段 `trailers: TrailerSink`
(`Arc<Mutex<HashMap<String,String>>>`);trailer 段头行解析处(原丢弃点)收进 sink
(同名后到覆盖先到—— AWS 只发一行);`wrap_request` 把同一 sink clone 注
`req.extensions()`。`put_object`/`upload_part` 在调用 `read_bounded_body` **前**取出
sink(读后 EOF,trailer 已齐)。校验收集序:显式头 ∪ trailer 值,逐算法验证;
**同算法双载体并存 → 400 `InvalidArgument`**(语义二义,AWS 不容);`x-amz-trailer`
声明了校验头而 sink 未见 → 400(残缺帧)。SIGNED-TRAILER 档的 trailer 段不签名
维持(签名面继续只指数据块链)。**对既档「明示不验证」的翻案范围 = 值**;
`decoded-length accounting` 要求文的对应面随 delta 改写,chunk 签名链要求零字不动。

### D5:回显——PUT 恒回(算出的),GET/HEAD 查询门控

- PUT/UploadPart 响应:回**存选算法**的算出值 + `x-amz-checksum-type: FULL_OBJECT`
  (part 无持久化,回算出值即可);
- GET/HEAD:仅当请求带 `x-amz-checksum-mode: ENABLED`(其他值 400)且行有存储值
  → `x-amz-checksum-{algo}` + type 头。200/206 均回(已存值与 range 无关,
  FULL_OBJECT 语义);对象三形态只有 plain/SSE 有值(multipart 无);**304/412 不回**
  (协商面不复读数据话语);HEAD 同 GET(查询头不参与 SigV4 已签头集,无签名影响)。
- GetObjectAttributes:`Checksum` 请求集合法;有值 →
  `<Checksum><Checksum{ALGO}>{base64}</Checksum{ALGO}><ChecksumType>FULL_OBJECT></ChecksumType></Checksum>`;
  无值 → 元素省略(不造空)。`ObjectParts` 501 维持。

### D6:SSE 与分帧正交矩阵

SSE PUT + checksum:明文算、验、存(frarming 后的 blob 校验无从参与——**对象语义
校验就是明文**,AWS 同口径)。aws-chunked + checksum:trailer 载体天然走 D4;与
SSE 互斥(分帧明文明 PUT 不能带 SSE 头?实际 SSE 头与分帧可并存——决策:**允许**,
明文体验证照算,SSE 面沿既有弧)。multipart SSE 一贯 501(既有)。

### D7:无观测/配置新增

`400 BadDigest` / `501 CRC64NVME` 均落 `s3_requests_total{op,status}` 既有面;
CLAUDE.md §7 表零行变化(行为记 s3.md 正文 + spec)。

### D8:依赖四件

`crc32fast`(`CRC32` IEEE)、`crc32c`、`sha1`、`base64`(sha2 从 SSE 档已在)——
均业界事实标准实现,零自研 CRC(避开 init 常数/reflect  семantics 踩坑)。

## Risks / Trade-offs

- **trailer EOF 时序**:sink 读点必须在 body 读完之后(实现顺序固定:先
  read_bounded_body 再查 sink);若 read 失败(超限/坏帧)不消费 sink,trailer 面
  随请求失败——不需要额外处理;
- **多算法并存的回显「降维」**:全验但单存,最强优先——客户端拿到的回显可能与
  其提交面不同(AWS 新语义也只回主算法);文档明示;
- **收紧面**:此前「带坏校验头照样成功」的行为变为 400——属纠错性收紧,唯一
  可见变化即不合法请求从不被观察变为被拒;
- MySQL `ALTER TABLE ADD COLUMN`(005):sqlx migrate! 单次跑,不存在重复列;
  PG 用 `ADD COLUMN IF NOT EXISTS`(沿用 002 先例口径)防重;MySQL 无 IF NOT
  EXISTS(8.0 仍无)——migrate! 表记录已应用版本,天然幂等。

# s3-read-compat —— S3 读协议补齐:条件 GET(+If-Range)/?partNumber/ListObjectsV1/?location

## Why

S3 网关 2026-10-16 审计(优化路线档 1,用户点名「S3 协议补齐(条件 GET/?partNumber/V1)」)确认的读面缺口:

1. **GET/HEAD 无条件请求语义**——`If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since` 四头完全不评估;客户端缓存协商(304)/乐观并发(412)全失效,SDK 条件重试与 CDN 回源全部退化为全量传输。拷贝路径(`evaluate_copy_conditions`)已有四头谓词,但 GET 语义不同(命中 `If-None-Match` 回 **304** 而非 412)未接;
2. **GET 无 `?partNumber`**——multipart 对象无法单片直读,SDK 并行下载(`DownloadFile`)无法分片并发;
3. **ListObjectsV1 缺失**——`GET /{bucket}` 无 `list-type=2` 时直接走 V2 形态回答;老客户端(boto2/旧 s3cmd/V1 SDK)解析 `NextMarker`/`Marker` 的分页协议对不上;
4. **GetBucketLocation 缺失**——`?location` 无路由(顺带 spec 已声明 Last-Modified SHALL 回但实码不发,本档一并顺修)。

## What Changes

- **条件 GET/HEAD(RFC 7232 §6 求值序)**:`If-Match`(失配 412,通过则短路)> `If-Unmodified-Since`(无 If-Match 时,已改 412)> `If-None-Match`(命中 304)> `If-Modified-Since`(无 If-None-Match 时,未改 304)。谓词复用拷贝路径的 `strip_etag_quotes`/`parse_http_date_ms` 纯函数;304 = 空体 + ETag + Last-Modified,**不触数据面**;坏日期 400 InvalidArgument(与拷贝路径一致)。GET/HEAD 同语义(HEAD 304/412 空体)。
- **If-Range(RFC 7233)**:validator(强 ETag 相等,或日期 ≥ Last-Modified)过 → 206 照 Range;不过(失配/过旧/无法解析)→ **忽略 Range,200 全对象**;同时闸 `?partNumber` 窗口(AWS 语义:precondition 失败回整对象)。
- **`?partNumber`(仅 GET;HEAD 忽略)**:multipart → manifest 累积偏移定位该片,206 + `Content-Range` 按全对象偏移,体=仅该片;`< 1` 或 `> 片数` → 416 `InvalidRange`;单片对象(plain/SSE):part 1 → 206 全量 + 全幅 Content-Range,其余 416;与 `Range` 组合 = 片内窗口(交集)。
- **ListObjectsV1**:`GET /{bucket}`(无子资源时)无 `list-type=2` 即 V1:`marker`(回显)/`NextMarker`(仅 delimiter 存在且截断时回,值 = metastore 排他 token,即末个已扫 raw key——含于被分组吞掉的 key 也不回头,翻页零缺漏零重复)/`IsTruncated`/`Contents`(含 `StorageClass: STANDARD`)/`CommonPrefixes`;复用 `MetaStore::list_objects` 核心(marker→start_after 同为 exclusive)。V2 路径与响应字节不动。
- **GetBucketLocation**:`?location` 子资源路由,owner 门(`require_bucket_owner`),`LocationConstraint` XML(region==us-east-1 → 空元素,取 `s3.region` 配置)。
- **Last-Modified 顺修**:GET/HEAD/304 全路径补 `Last-Modified: <RFC1123>`(值=对象 `created_at`)——关闭 s3-core spec 已声明的 SHALL 缺口。
- 无新配置、无新指标、无 MetaStore schema 变更、无 frontend 层改动(304/412 由既有 `s3_requests_total{op,status}` 观测)。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-core`:GetObject 要求增补条件请求/If-Range/partNumber/Last-Modified 语义;HeadObject 要求增补条件请求 + Last-Modified + `Range`/`partNumber` 忽略声明。
- `s3-list`:新增(ADDED)ListObjectsV1 要求(V1 形态/marker 分页/NextMarker 规则);新增(ADDED)GetBucketLocation 要求。

## Impact

- 代码:`crates/s3/src/handlers/object.rs`(条件谓词 + get/head 重构接窗)、`crates/s3/src/handlers/range.rs`(manifest 偏移投影纯函数)、`crates/s3/src/handlers/bucket.rs`(V1 handler + location)、`crates/s3/src/server.rs`(S3QueryParams 增 `marker`/`location`,HEAD handler 下探 headers)、`crates/s3/src/xml.rs`(V1 序列化器 + LocationConstraint)。
- 契约:GET 响应头集合新增 `Last-Modified`;新增 304/412 状态与 `?partNumber`;`GET /{bucket}` 无 list-type 时默认语义从 V2 翻为 **V1**(既有 e2e 全部显式带 `list-type=2`,零回归,已核查);不触碰写路径、SigV4、ACL、MetaStore。
- 测试:谓词/偏移/序列化纯函数单测;e2e 双言矩阵(条件矩阵、If-Range、partNumber 矩阵、V1 分页/location)走既有 `TEST_S3_DRIVER` 惯例。
- 明示不做:`?versioning` 等显式 501 拒绝族(继续现状既有行为);V1 Owner 元素(MinIO 先例省略);多 range(保持忽略)。

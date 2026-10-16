# s3-read-compat 设计

## Context

读路径现状(`crates/s3/src/handlers/object.rs`,档 0 s3-range-streaming 落地后):

- `get_object(st, bucket, key, range)`:MetaStore 查元数据 → 按 kind(plain/SSE/multipart)分发 → `object_read_builder` 统一 200/206 头;`Range` 已支持(解析/窗口/416/忽略),multipart 走 `PartStream` 流式;
- `head_object(st, bucket, key)`:**不接任何请求头**(签名里根本没有 Request),回 ETag/Content-Type/meta/Accept-Ranges,**无 Last-Modified**(spec 说 SHALL——实码缺口);
- 条件谓词只有拷贝路径一份:`evaluate_copy_conditions`(If-Match>If-None-Match>If-Unmodified-Since>If-Modified-Since,任何失败都 412)——GET/HEAD 语义不同(If-None-Match/If-Modified-Since 命中应 **304**),不能直接复用判定,但其原子件(`strip_etag_quotes`/`parse_http_date_ms`/`http_date_from_ms`)可整用;
- `bucket_get_handler`:`?acl` → `?uploads`(POST)→ 无条件 `list_objects_v2`;`GET /{bucket}`(**无** `list-type=2`)也被 V2 回答——V1 客户端解析不出分页协议;
- `S3QueryParams` 已有 `part_number: Option<i32>` 字段(UploadPart 用),GET 路由未消费;无 `marker`/`location`;
- metastore `list_objects`(pg.rs:200):`start_key = continuation_token.or(start_after)`,`key > start_key` 严格排他,`limit = max+1`,`is_truncated = rows > max`,`next_token = 末条 raw key`;delimiter 分组在**截断之后**(内存里)做;返回 `{entries, common_prefixes, is_truncated, next_continuation_token, key_count}`。

## Goals / Non-Goals

**Goals:**

- GET/HEAD 支持 RFC 7232 条件请求四头(正确求值序 + 412/304 语义);304 零数据面读取;
- 支持 If-Range(与 Range/partNumber 组合);
- GET 支持 `?partNumber`(multipart 单片 + 单片对象全幅 + 越界 416 + 与 Range 交集);
- ListObjectsV1(形态/分页/NextMarker 规则);GetBucketLocation(owner 门 + us-east-1 空元素);
- GET/HEAD 补 Last-Modified。

**Non-Goals:**

- 拷贝条件语义变化(evaluate_copy_conditions 保持 412-only 不动——已发布行为);
- `?versioning` 显式 501 拒绝族(独立 scope-ruling,不混入);
- V1 `Owner`/`ListBuckets ownership` 元素(MinIO 先例省略;e2e 断言不依赖);
- 多 range 逗号语义(维持档 0 的忽略回 200);
- V2 响应任何字节变化(V1/V2 共用 metastore 核心,V2 序列化器不动);
- 修 delimiter 分组在截断后的共享简化(V2 行为继承,见 D5 已知项)。

## Decisions

### D1:条件求值序照 RFC 7232 §6,412 族先于 304 族

判定序(与拷贝语义差异处加粗):

1. `If-Match`:失配 → **412**;通过 → **短路**(后续条件头全忽略);
2. `If-Unmodified-Since`(仅无 If-Match 时评估):对象 `created_at > since` → **412**;
3. `If-None-Match`:剥引号相等或 `*` → **GET/HEAD 回 304 空体**(拷贝路径此处是 412);不匹配 → 短路(忽略 4);
4. `If-Modified-Since`(仅无 If-None-Match 时评估):`created_at ≤ since` → **304**。

共享谓词实现为新纯函数 `evaluate_read_conditions(meta, headers) -> ReadOutcome{Proceed, NotModified, PreconditionFailed}` 放 `object.rs`,内部只用既有三个原子件;`*` 通配仅 `If-None-Match` 认(AWS 语义)。坏日期 → 400 InvalidArgument(与拷贝路径一致,响亮不吞)。条件评估在 MetaStore 元数据取得之后、任何 blob 读取之前——304/412 零数据面成本(与 Range 窗口化的「先元数据后取件」同构)。

### D2:304 形态,并顺修 Last-Modified

304 = 空体 + `ETag` + `Last-Modified`(RFC:304 由缓存协商,不回 body;S3 同)。HEAD 304/412 同理(本就无体)。Last-Modified 全路径补:`http_date_from_ms(meta.created_at)`(RFC 1123 格式,与拷贝响应同源函数)。这关闭 s3-core spec「SHALL include Last-Modified」缺口——授权:停摆 spec 一致性属本档「读协议补齐」范畴,测试即时锁定(所有 200/206/304 的 GET 与 HEAD 用例断言该头)。

### D3:If-Range 闸门——validator 失败一律回全对象 200

判定 `if_range_passes(meta, val)`:

- `val` 形如 ETag(带引号/`W/` 前缀)→ 剥引号与 `meta.etag` 相等才过(弱比较不做,`W/` 视为失配——S3 回强验证器);
- 否则当日期解析:过当且仅当 `date ≥ meta.created_at`(对象在日期后改过 → validator 过期);
- 解析失败 → 不过。

不过时:若请求带 Range → **忽略 Range 回 200 全对象**(RFC 7233 MUST ignore);若带 partNumber → **回整对象 200**(AWS GetObject partNumber 语义:「If-Range precondition 失败回整个对象」)——两种来源统一为「窗口失效 → 全读」。无 If-Range 头 → 恒过。与条件四头并存时:**四头先评估**(304/412 短路在前,一面 304 不该再回 Range 体)。

### D4:partNumber = manifest 偏移投影,复用档 0 窗口机制

`range.rs` 增纯函数 `manifest_part_window(parts, n) -> Option<PartWindow{start, len, total}>`(`window_parts` 同族):

- n 越界(`<1` 或 `> parts.len()`)→ None → 416 `InvalidRange`(requested 写 `partNumber=<n>`,actual_size 携对象总量;message 注明实际片数);
- 命中 → `start = Σ sizes[0..n-1]`,`len = sizes[n-1]`,`total = Σ all`。

响应链:**恒 206**(AWS:成功 partNumber 总是 206)+ `Content-Range: bytes start-(start+len-1)/total` + 窗口体。分发复用档 0:multipart 窗口直接喂 `window_parts`(窗口起止落进哪几片就取哪几片,单 part 窗口=取该片);plain/SSE 单片对象:part 1 = 全幅 206(Content-Range 全区间),体=全对象(SSE 解密后按需切);part ≥2 → 416。与 `Range` 组合:先各自成窗再求交(交集空 → 416)。HEAD 忽略 partNumber(与忽略 Range 同声明)。multipart+SSE 组合对象不存在(写侧 501),不设路径。If-Range 失败按 D3 提权为全对象 200。

### D5:ListObjectsV1——同核心、异形态,marker 映射 exclusive 对齐

分派:`GET /{bucket}` 无子资源(acl/uploads/location 之外)且无 `list-type=2` → V1 handler。入参 `prefix/delimiter/max-keys/marker` 映射到既有 `ListObjectsReq`:`marker → start_after`(两者都是「严格大于该 key 起排」——pg 实现同一路径 `key > $2`,零方言适配),`continuation_token=None`。响应 XML 形态(V2 序列化器不动,新增 `serialize_list_bucket_result_v1`):

- 恒有:`Name/Prefix(可为空)/Marker(回显,可空)/MaxKeys/IsTruncated`;`Delimiter` 仅给定时;**无** `KeyCount`/`NextContinuationToken`;
- `NextMarker`:**仅 delimiter 存在且 IsTruncated 时**输出,值 = metastore 排他 token(末个已扫 raw key)。**否决「末可见项字典序大者」**:末 raw key 被分组吞掉时其前缀严格小于组内各 key → 末可见项会让下一页 `key > marker` 重扫该 key(翻页重复);排他 token 零缺漏零重复,代价是 NextMarker 可为响应内不可见的 key(组内成员,AWS 客户端作不透明续位);
- `Contents`:`Key/LastModified/ETag/Size/StorageClass`(STANDARD 恒定——无分层,如实;文档注明);`CommonPrefixes` 同 V2。

**已知共享简化(记录,不在本档修)**:metastore 在 delimiter 分组前按 raw rows 截断 → 同一分组可能跨页重复出现(`a/` 的公共前缀可能两页都回)。这是 V2/V1 共同继承的 metastore 核心行为(档 1 只做协议形态,不动列表语义);分页不丢不重 key,只是分组视图可重复。e2e 以「遍历两页合集后 key 集无缺漏」断言。真 S3 的「分组不跨页截断」属列表语义立项(roadmap 档外)。

**默认语义翻转核查**:`GET /{bucket}` 无 list-type 从「答 V2」翻为「答 V1」——已核查既有 e2e(`tests/list.rs`)全部显式带 `list-type=2`(6/6),零回归;devcluster 脚本若用裸 GET 属 V1 客户端,V1 形态恰好是其期待。

### D6:GetBucketLocation——owner 门 + us-east-1 空元素

`S3QueryParams` 增 `location` 字段;`bucket_get_handler` 在 acl/uploads 之后路由。鉴权 = `require_bucket_owner`(GetBucketLocation 泄露的是租户归属元数据,与 `?acl` 同面,非 owner 403)。响应:`<LocationConstraint xmlns="…s3.amazonaws.com/doc/2006-03-01/">REGION</LocationConstraint>`;`region == "us-east-1"` → 空元素内容(AWS 官方行为:经典区不回字符串);region 取启动配置 `s3.region`。

### D7:无观测/配置新增

304/412 是 s3-gateway 既有 op=status 观测面(`s3_requests_total{op,status}`);无新配置项、无新指标、无 schema/磁盘格式/front-end 改动——§7 文档义务仅 s3.md 矩阵与操作映射行。

## Risks / Trade-offs

- **默认翻 V1 的可见性**:行为变化(裸 GET 的响应 XML 形态),但 S3 客户端协议本就按「无 list-type=2 → V1」预期;e2e 显式带参,零回归;风险披露在 proposal Impact 与本节。
- **条件头与已发布拷贝语义并存**:同一对象同头名,GET 回 304、copy 回 412——AWS 官方就是分语义,文档双节各自声明,不构成不一致。
- 档 0 建立的窗口机制被再消费,若 window_parts 求交语义有 bug 会双倍暴露——用 partNumber×Range 交集单测锁边界(空交 416/等价窗/子窗)。
- **V1 NextMarker 可为响应内不可见的 key**(排他 token 口径):AWS 客户端把 marker 当不透明续位,无碍;风险已转化为 design D5 记录 + e2e 翻页重扫断言。

# Design: s3-copy-attributes

## CopyObject 执行模型

```
PUT /{dest_bucket}/{dest_key}  +  x-amz-copy-source: [/]{src_bucket}/{src_key}
  ├─ 写门(既有 object_put_handler 顶闸):require_bucket_owner(dest)   ── 403/404
  ├─ 源解析:percent-decode + 首 '/' 拆桶;空桶/空 key → 400;携 ?versionId → 501
  ├─ 源闸:require_public_read(src_bucket, principal)                 ── 403/404
  ├─ 源元数据:metastore.get_object → NoSuchKey(404)
  ├─ 条件评估(源 meta;优先序 match > none-match > unmodified > modified,失配 412)
  ├─ 指令:x-amz-metadata-directive ∈ {COPY(默认), REPLACE};非法值 400
  │    自拷贝(dest ≡ src) 且 COPY → 400 InvalidRequest
  ├─ 执行:
  │    单块源:router.get(src_blob ≤ max)  → router.put_auto(数据) → metastore.put_object
  │            (配额 507 弧 = put_object 全同:拒 → 同请求墓碑 + quota 指标)
  │    multipart 源:metastore.create_multipart → 逐 ManifestPart:
  │            router.get(part) → put_auto → put_part(重算 etag) → complete_multipart
  │            (遮蔽/弃片回收 + 配额拒保 session,与既有 complete 面 Zero 分叉)
  └─ 200 + <CopyObjectResult><LastModified/>{ISO}<ETag>"…"</ETag></CopyObjectResult>
```

- **内存红线**:multipart 复制逐 part 物化(≤ 4 MiB/步),绝不整 blob 缓存;单块源 ≤ 4 MiB
  复用既有物化路径(与 put_object 同包络)。
- **所有权一致**:源读门/终写门与 GET/PUT 的 ACL 面完全同闸;public-read 源可被匿名读者
  拷贝到自己的桶(读允许 + 写允许 = 拷贝允许,与 S3 语义一致)。
- **ETag 语义**:单块拷贝 = 内容 MD5(与源相等,诚实重算);multipart 拷贝 = complete 的
  组装 ETag。LastModified = 拷贝时刻。
- **条件评估细则**:ETag 头值剥引号比较(带引号/裸值皆受理);日期头 rfc2822 解析,非法
  格式 400(不猜);if-modified-since 的 `≤` vs `<`(源 created_at 严格早于参照时刻才 412,
  按同刻不更新的 S3 口径)。

## GetObjectAttributes

- 请求侧:query `?attributes`(存在即义) + 头 `x-amz-object-attributes` CSV(容空白);
  可请求集 = `{ETag, Checksum, ObjectParts, StorageClass, ObjectSize}`;未知名 400
  (`InvalidArgument`);缺失头 400。
- 响应侧:读门同 GET object;体 = `GetObjectAttributesOutput`(xmlns 同族),请求∩支持
  才出项:`<ETag>`(裸值)/`<ObjectSize>`/`<StorageClass>STANDARD</StorageClass>`;
  `Checksum`/`ObjectParts` 请求 → **501**(`NotImplemented`,不静默);响应头带
  `ETag`(带引号)与 `Last-Modified`(RFC 7231)。

## Mechanism lesson(设计期预定 + 实装)

- 复合原语的正确性靠**闸门正交**而非新闸:源=读门、终=写门,不引入第三种权限概念;
  配额/遮蔽/回收全部复用既有弧 → 拷贝不产生新的崩溃一致性面(red line §3.3 复查:
  无新持久化边界,写路径与 put/complete 逐字节同弧)。
- 内部 multipart 会话在配额拒绝时**保留**(与线上面同策):507 后操作员调额即可由
  调用方重发拷贝(session 不可见但幂等重试语义一致)。

## 安全/持久化边界

- 不触及磁盘格式、GC、引擎写路径(全部动作 = 既有 metastore/router 原语的新编排);
- 条件评估服务于客户端竞态防护,服务端保持无锁同判定读写(拷贝执行时源可能被覆盖 ——
  读到的元数据与数据的原子性 = 既有 get_object 的同一口径,不引入跨行快照)。

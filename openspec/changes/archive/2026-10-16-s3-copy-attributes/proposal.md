# Proposal: CopyObject / GetObjectAttributes(`s3-copy-attributes`)

## Why

用户 2026-10-16 点名「除了 EC,其他继续」——队列第二件。拷贝是对象面的**复合原语**
(服务端读源→写终,客户端零体往返):无它,跨桶复制/改名必须 GET+PUT 双走网关。
属性读取(GetObjectAttributes)是 HEAD 的选择性回显面,多个重量级客户端(AWS CLI
`head-object` 现默认走它)用它替代 HEAD。

## What Changes

- **CopyObject**(`PUT /{bucket}/{key}` + `x-amz-copy-source`):
  - 源闸 = 公开读门(owner 或 public-read;私有桶跨租户 403,未知桶 404),
    终闸 = 既有 PUT 写门(桶 owner);
  - 条件拷贝四头(`x-amz-copy-source-if-match / -if-none-match / -if-unmodified-since /
    -if-modified-since`)按 AWS 优先序评估,失配 `PreconditionFailed`(412,新变体);
  - 元数据指令 `COPY`(默认,复制源 content-type + x-amz-meta-*)/ `REPLACE`(按本请求重定);
    自拷贝(同桶同 key)且 COPY → `InvalidRequest`(400,新变体);REPLACE 自拷贝 = 元数据重写,合法;
  - 单块源(≤ `s3.max_body_bytes` 的直写对象):读源 blob → 新 blob → 元数据行,配额 507
    弧与 put_object 全同(拒则同请求墓碑);
  - multipart 源:**内部限制会话重写**(逐 part 读→写,单 part 内存 ≤ 4 MiB —— 不把整个
    blob 缓进内存的红线);complete 复用既有原子路径(遮蔽/弃片回收、配额拒后 session 保留);
  - 响应 200 + `CopyObjectResult` XML(ETag + LastModified);PUT 体不读为零(有体即
    400 `InvalidArgument`)。
- **GetObjectAttributes**(`GET /{bucket}/{key}?attributes` + `x-amz-object-attributes`):
  - 读门 = 公开读门(与 GET object 一致);
  - 属性集 `ETag|Checksum|ObjectParts|StorageClass|ObjectSize`;`Checksum`/`ObjectParts`
    → **显式 501 `NotImplemented`**(checksum 家族未落,出界不静默);未知属性名 400;
  - 支持三属:ETag(报头+体)、ObjectSize、StorageClass(恒 `STANDARD`);Last-Modified
    以响应头回显(RFC 7231 日期)。

## Impact

- 到达面:`object_put_handler` 检出 `x-amz-copy-source` 分叉(写门先行,分叉先于体读);
  `object_get_handler` 检出 `?attributes` 分叉(需新收请求头)。
- 零格式/零引擎变化:全部动作由既有 metastore/router 原语拼装;不动磁盘格式、不动 GC。
- 语义不变面:非 copy/非 attributes 请求逐字节等价。

## Non-goals

- `x-amz-copy-source` 的 `?versionId` 子资源(versioning 永久拒绝;携 query 即 501)。
- `UploadPartCopy`(multipart part 级服务端复制)。
- GetObjectAttributes 的条件请求头(if-match 族)与 `ObjectParts` 分页。
- SSE 复制时的加密上下文再生(SSE 属下轮,拷贝对加密对象按明文读侧规则走)。

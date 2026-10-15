# Proposal: 桶级 canned ACL(private / public-read)+ 公开桶匿名读

## Why

s3-bucket-acl 首轮只落了**隔离**(私有桶 owner 独占),矩阵把「公开桶 / canned ACL /
桶策略 JSON / 跨 owner 授权」留评估(后续方向:ACL 授权面)。本轮把其中**最小可交付切片**
落地:canned ACL 的两档桶级读写面,先把最常见的「公开读」诉求(静态资源、图床、回调拉取)
变成一等公民;policy JSON 与跨 owner 授权面**不在本轮**(记档,二期评估)。

## What Changes

- **新增主语:匿名(Anonymous)**——无任何凭证附着的请求不再被 auth 中间件一刀 403,
  而是作为 `Principal::Anonymous` 进入路由层,由各闸门裁决。**验签失败绝不降级为匿名**
  (带错凭证 = 403,安全红线)。
- **桶级 canned ACL**:`buckets.acl ∈ {private, public-read}`(默认 private = 首轮语义
  逐字保留):
  - `public-read`:GET/HEAD object 与 ListObjectsV2 对**任何人**开放(匿名 / 任意签名
    租户 / owner);其余一切(写、删、批删、multipart 全家族、ListMultipartUploads、
    HeadBucket、管理面)仍 owner 独占;
  - `private`:行为与首轮完全一致。
- **API 面**:
  - `CreateBucket` 接受 `x-amz-acl: private | public-read`(缺省 private);
  - `PUT /{bucket}?acl`(PutBucketAcl,owner-only)同上 **canned header 形式**;
    XML body 形式 → **501 NotImplemented**(显式出界,不静默);
  - `GET /{bucket}?acl`(GetBucketAcl,owner-only)回 AccessControlPolicy XML,
    public-read 时含 AllUsers READ grant;
  - 未知 `x-amz-acl` 值 → 400 InvalidArgument。
- **MetaStore**:`set_bucket_acl`(UPDATE 0 行 → NoSuchBucket)+ `head_bucket` 携带 acl
  (迁移 003 双方言,默认 'private' 回填;私→公方向单调,create→set 的间隙天然 fail-safe)。

## Impact

- **默认零变化**:不动任何 `impl Default`/配置;存量桶 003 迁移后全部 private。
- **安全面变化(有意)**:开启 public-read 的桶把读面开放到未认证世界——显式 per-bucket
  opt-in,不碰鉴权算法、凭证体系、admin 域。
- 路由层 handler 签名全局换 `Principal`(机械传递,行为等价测试即原有 ACL 首轮用例
  必须原样全绿)。

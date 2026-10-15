# s3-bucket-acl —— S3 多租户 ACL 首轮:私有桶语义(跨租户 403)

## Why

配额轮补上了"写多少"的管控闸,多租户的另一半缺口是"谁能碰谁的数据":当前任意有效凭证
可访问**任意桶**(对象读写/列举/删除/multipart 全家族)。多租户上线即租户互踩。用户已令
"多租户 ACL 这个做"(2026-10-15),且序为 ACL 先、B2 EC 后。

## What Changes

- **私有桶语义**:每个 bucket-scoped 操作(对象 PUT/GET/HEAD/DELETE、batch delete、
  ListObjectsV2、HeadBucket/DeleteBucket、multipart 全家族)要求**请求凭证 owner == 桶
  owner**,否则 403 `AccessDenied`;桶不存在 → 404 `NoSuchBucket`。
- **落点**:server.rs 包装器层为唯一闸门(单一choke point,handler 零改动)。鉴权中间件
  已在 extensions 提供带 owner 的 AuthContext(SigV4 验签后),闸门仅追加一次
  `head_bucket` PK 查询(~亚毫秒,池化)。
- **不设闸的**:CreateBucket(创建即按凭证 owner 认领名字;他人已占名 → 409 既有语义)、
  ListBuckets(本就按 owner 过滤)、`/admin/*` 与 `/metrics`(独立鉴权域)。
- 无新配置项、无新指标(403 计数进 `s3_requests_total{op,status}` 既有口径)。

## Capabilities

### New Capabilities
(无独立新能力;delta 见 `specs/s3-core`。)

### Modified Capabilities
- `s3-core`:ADDED「Private bucket access control」requirement(跨租户 403 / 未知桶 404 /
  CreateBucket 认领语义 / ListBuckets 隔离 四组场景)。

## Impact

- crate:`crates/s3`(server.rs:闸门助手 + 9 个 bucket-scoped 包装器接 Extension;
  tests:夹具发证/签发助手 + 跨租户矩阵用例)。
- 文档:s3.md「多租户访问控制(私有桶语义)」节 + 支持矩阵行更新(私有桶 ✅;
  公开桶 / canned ACL / policy 授权面维持暂不支持)。
- 兼容:单租户使用者(所有桶同 owner)行为不变;跨租户 403 是新契约,矩阵宣告。

## Non-goals

- 公开桶 / canned ACL(`x-amz-acl`)/ 桶策略 JSON / 跨 owner 授权——矩阵已列暂不支持;
- 不做 owner 级别的角色细分(读写分离)——凭证即租户身份,v1 全有或全无;
- 不计 ACL 决策的独立指标(403 已有口径);
- presigned URL 的 ACL 语义跟随凭证(签发者 owner),不单列。

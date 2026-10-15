# Tasks

## 1. 闸门(server.rs 包装器层)

- [x] 1.1 `require_bucket_owner` 助手(head_bucket PK 查询;owner 一致放行 / 跨租户
     403 AccessDenied / 未知桶 404 NoSuchBucket)
- [x] 1.2 九个 bucket-scoped 包装器接 `Extension<AuthContext>` + 闸门:
     bucket_get(list/upload 回合)/ bucket_post(batch delete)/ object_put / object_get /
     object_head / object_delete / object_post(multipart 三分发+batch)/ delete_bucket /
     head_bucket;Create/ListBuckets 不设闸(既有 owner 语义)

## 2. 夹具 + 测试

- [x] 2.1 夹具助手:`issue_credential(owner)` 签发第二租户凭证;`request_as(cred, ...)`
     以指定凭证签发 signed 请求
- [x] 2.2 e2e 跨租户矩阵:PUT/GET/HEAD/DELETE/ListV2/batch/multipart create 全 403;
     owner 一切照旧;ListBuckets 隔离;未知桶 404

## 3. 文档与收口

- [x] 3.1 s3.md「多租户访问控制(私有桶语义)」节(判定/错误映射/凭证即租户/闸门代价)
     + 支持矩阵行更新(私有桶 ✅;公开桶/canned ACL/policy 维持暂不支持)
- [x] 3.2 净/脏双跑全绿 + workspace + fmt/clippy + push CI 绿 + 归档 + DEVLOG 步骤 53
     (并入:计费否决追认——L0 计量导出亦不做,s3-scope-rulings 档已补)

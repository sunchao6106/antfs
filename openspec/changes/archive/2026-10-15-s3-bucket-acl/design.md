# s3-bucket-acl — 设计说明

## 闸门位置:为什么在 server.rs 包装器层

- 中间件(auth_middleware)已完成验签并把 `AuthContext{owner, access_key}` 放进 request
  extensions——身份免费可得;包装器是**bucket 从 Path 提取后的第一站**,在此设闸可覆盖
  全部分发分支(object put/get/head/delete/post 各自内部再分 multipart/batch——闸门一次
  全盖),且 handler 层零改动(职责不蔓延)。
- 备选(否决):在 metastore 加权限查询层——把"owner 一致"这种 v1 全有或全无的判定下沉
  到存储层,过度设计;未来做 policy 时再评估分层。

## 判定与错误语义

```rust
async fn require_bucket_owner(st, ctx, bucket) -> Result<(), S3Error>
  match head_bucket(bucket)?
    Some(meta) if meta.owner == ctx.owner => Ok(())
    Some(_) => AccessDenied("bucket belongs to another tenant")   // 403
    None => NoSuchBucket                                            // 404
```

- 桶不存在返回 404 `NoSuchBucket`(顺带比旧行为更贴 S3:旧路径 GET 未知桶回
  `NoSuchKey`)。已有测试只打已建桶,不受影响。
- CreateBucket 不设闸:创建即认领(重复名 → 既有 409 家族),防止"查询他人桶名是否存在"
  成为旁路探测面。
- 代价:每请求一次 `buckets` 主键查询(PG/MySQL 亚毫秒,池化);后续有量级再做缓存,
  独立 change。

## 凭证即租户

- `credentials.owner` 是唯一租户标识;一张凭证 = 一个租户(凭证吊销即驱逐)。多凭证同
  owner 天然共享桶——这是 v1 的"租户"定义,文档如实写。

## presigned URL

- 验签走签发凭证;owner 判定同请求(presigned 不过是进中间件的另一形态),不单列语义。

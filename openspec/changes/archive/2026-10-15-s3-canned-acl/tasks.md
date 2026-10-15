# Tasks

## 1. 类型与存储

- [x] 1.1 `BucketAcl` 枚举(Private | PublicRead;as_str/parse 白名单)+
     `BucketMeta.acl` 字段
- [x] 1.2 迁移 003 双方言(`buckets.acl VARCHAR(16) NOT NULL DEFAULT 'private'`)
- [x] 1.3 `MetaStore::set_bucket_acl`(UPDATE 0 行 → NoSuchBucket);
     `head_bucket` 读出 acl(未知值兜底 Private)

## 2. Principal 与闸门

- [x] 2.1 `Principal { Signed(AuthContext), Anonymous }`;auth 中间件:「无凭证完全
     缺席」→ Anonymous 注入;验签失败仍硬 403(绝不降级)
- [x] 2.2 `require_bucket_owner(Principal)`(语义与首轮逐字一致 + 匿名 403);
     `require_public_read`(public-read ∨ owner-signed)
- [x] 2.3 全部 handler 签名换 Principal;CreateBucket/ListBuckets 增 Signed 最低要求

## 3. API 面

- [x] 3.1 `x-amz-acl` 于 CreateBucket(create private → set,间隙 fail-safe 方向)
- [x] 3.2 `PUT /{bucket}?acl`(owner-only,canned 头;XML body → 501 NotImplemented,
     未知值 → 400 InvalidArgument)
- [x] 3.3 `GET /{bucket}?acl`(owner-only,AccessControlPolicy XML,AllUsers READ
     grant iff public-read)
- [x] 3.4 读面三门切换:GET/HEAD object、ListObjectsV2 → require_public_read
     (ListParts/ListMultipartUploads 仍 owner 门)

## 4. 测试

- [x] 4.1 内联单测(双方言):set/head BucketAcl 往返;未知桶 set → NoSuchBucket
- [x] 4.2 e2e:私有默认 403(匿名/跨租户)→ public-read 开闸(匿名 GET/HEAD/ListV2
     200;匿名 PUT/DELETE 403;跨租户读 200)→ 切回私 403;GetBucketAcl XML(AllUsers
     iff public);CreateBucket 带 x-amz-acl 即公开;错值 400;验错的请求不匿名化(403)
- [x] 4.3 首轮 private 用例原样全绿(行为等价不存在回退)

## 5. 文档与收口

- [x] 5.1 s3.md「多租户访问控制」节扩为「访问控制(私有桶 + 公开读桶)」+ 矩阵行更新
     + 管理接口 ?acl 两行 + 操作映射增注
- [x] 5.2 净/脏双跑(双言)+ workspace + fmt/clippy + push CI 绿 + 归档 + DEVLOG 57

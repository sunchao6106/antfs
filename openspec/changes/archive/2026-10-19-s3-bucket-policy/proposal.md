# s3-bucket-policy —— 桶策略 JSON:数据面跨租户授权(Put/Get/DeleteBucketPolicy)

## Why

兼容矩阵 116 行(`s3-canned-acl` 档记):「object 级 ACL / 桶策略 JSON / 跨 owner
**授权面**——授予留评估(二期)」。当前授权形态只有 owner 蕴含 + canned
public-read 两档:无法把「读这个前缀」「只许写不许删」这类**按资源/按动作**的跨租户
授权表达出来。用户 2026-10-19 指令「全力完善 s3 功能」——完结批 C2:以桶策略 JSON
(AWS 授权主形态)补齐授权面。

## What Changes

- **三 API**:`PUT /{bucket}?policy`(owner 门;body = 策略 JSON,≤ 20KB,服务端
  全量校验)、`GET ?policy`(无策略 → 404 `NoSuchBucketPolicy`)、`DELETE ?policy`
  (幂等 204)。策略**永不被策略本身评估**(put/get/delete policy 恒 owner 面——
  防 owner 被 deny 锁死自家策略管理,canned-ACL 档同工)。
- **持久化(migration 008,双方言)**:`buckets` +`policy TEXT NULL`;`BucketMeta` +
  `policy` 字段(head_bucket 读出;list_buckets 面不带);`set_bucket_policy(
  bucket, Option<&str>)` upsert(空 = 撤)。
- **策略语言(有界子集,put 时全验)**:`Version`(必须 `2012-10-17`)+
  `Statement[]{Sid?, Effect(Allow|Deny), Principal("*" 或 {"AWS": [owner-id|"*"]}),
  Action(s3: 动作名/*;数组可), Resource(arn:antfs:s3:::<bucket>[/<键-or-前缀>],
  尾部 `*` 前缀匹配)}`。**支持动作集** = 数据面八件:GetObject / HeadBucket 并入
  ListBucket / ListBucket / PutObject / DeleteObject / AbortMultipartUpload /
  ListMultipartUploadParts / ListBucketMultipartUploads / `*`。
  `Condition` / `NotPrincipal` / `NotAction` / `NotResource` / IAM-ARN 由
  `arn:` 前缀形式 principal → **put 时 400 响亮拒**(不支持即拒,不静默忽略)。
- **评估序(数据面两门收编)**:explícit Deny > policy Allow > 既有兜底(owner
  蕴含允许 / canned public-read[读族动作] / 私有 403)。挂入既有 gate 位:
  GET/HEAD/attributes(GetObject)、ListObjectsV1/V2(ListBucket)、PUT object/
  copy/Part/complete/create-multipart(PutObject)、DELETE object(Abort 或
  DeleteObject)、ListParts/ListMultipartUploads。策略存坏(读侧解析失败)→
  **fail-closed 500**(绝不 fail-open)。
- **明示出界(记档)**:策略不评估**管理面**(?acl/?policy/location/quota/
  batch-delete——owner 恒独占;batch 逐键评估 + object ACL XML 形态 = C3 与
  后续);Condition 运算符族不做(不支持即 put 400);资源 glob 仅尾缀 `*`。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-core`:新增(ADDED)「桶策略与授权评估」要求(三 API、语言子集、评估序
  Deny>Allow>兜底、fail-closed、管理面出界)。
- `s3-metastore`:新增(ADDED)「桶策略持久化」要求(migration 008、BucketMeta
  扩展、set_bucket_policy upsert 语义)。

## Impact

- 代码:`policy.rs`(新,解析/校验/评估)、`server.rs`(两门 → 三参数 authorize
  化 + 策略路由)、`handlers/bucket.rs`(三个策略 handler)、`metastore/{types,mod,
  pg,mysql}.rs`、migration 008、fake 签名、`copy_object` 内部双门同步、e2e。
- 契约:既有面零破坏(无策略行 = 评估 NotApplicable → 兜底 = 原语义);新增 400/404
  概率面走既有指标观测;无新配置/监控。
- 持久性:纯元数据列(无 blob/无飞行写);策略 put = 单行 UPDATE,原子;幂等重试安全。
- 测试:e2e 双言(跨租户 Allow 读/写、Deny 压 owner 蕴含、匿名 Allow、动作域/资源
  前缀域、坏策略 400 全形态、无策略 404、删除回私有、owner 恒可管策略、fail-closed
  ——直改库注坏策略 mock)。

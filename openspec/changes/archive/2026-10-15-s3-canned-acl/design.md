# Design: 桶级 canned ACL + 匿名公开读

## 机制

### 1. Principal:请求主语的唯一权威形状

auth 中间件在 route_layer 前置裁决后,向 extensions 注入**唯一**主语类型:

```rust
pub enum Principal { Signed(AuthContext), Anonymous }
```

- **Anonymous 的判定口径(安全红线)**:「请求上**完全没有**附着凭证」——无
  `Authorization` 头且无 `X-Amz-Algorithm` 预签 query。**携带凭证但验签失败 / 凭证不存在 /
  时间偏移超限的请求一律在中间件层硬 403,绝不降级为匿名**(错凭证混进匿名通道 = 鉴权
  绕过面,不可协商)。
- 验签成功 → `Signed(AuthContext{owner, access_key})`(形状与首轮一致)。
- `/metrics`、`/admin/*` 既有直通语义不动(admin 自带 operator token 域)。

### 2. 闸门(路由包装层单一 choke point,延续首轮)

| 闸门 | 通过条件 | 应用面 |
|---|---|---|
| `require_bucket_owner` | Signed ∧ owner == 桶 owner | 一切私有面:写/删/批删/multipart 全家族/ListParts/ListMultipartUploads/HeadBucket/DeleteBucket/PutBucketAcl/GetBucketAcl/CreateBucket(仅要求 Signed)/ListBuckets(仅要求 Signed) |
| `require_public_read` | 桶存在 ∧ (acl = public-read ∨ Signed ∧ owner == 桶 owner) | **读面三件**:GET object、HEAD object、ListObjectsV2 |

- 未知桶在两闸门均为 **404 NoSuchBucket 先决**(与首轮一致,不因 ACL 语义改变存在性判定
  顺序)。
- Anonymous 触任何 owner 面 → 403 `AccessDenied`("authentication required")。
- presigned URL 属 Signed(凭证效力同验签);public-read 桶的 presigned 读亦通过
  (本就通过全读门)。

### 3. 存储(双方言同构)

- 迁移 `<dialect>/003_add_bucket_acl.sql`:
  `ALTER TABLE buckets ADD COLUMN acl VARCHAR(16) NOT NULL DEFAULT 'private';`
  存量行回填 private = 行为零变化(默认即现状)。
- `BucketMeta` 增 `acl: BucketAcl`(枚举 `Private | PublicRead`,`as_str/parse` 白名单);
  DB 读入未知值兜底 `Private`(防御位:写路径白名单保证不出现;兜底方向取更严侧)。
- `set_bucket_acl`:`UPDATE buckets SET acl = ? WHERE name = ?`,0 行 → `NoSuchBucket`。
- **CreateBucket 带 x-amz-acl 的非原子性是刻意取舍**:create 恒 private → 再 set。间隙
  方向是**由宽收严**(fail-safe:暴露窗口内仍是 private);反之(set 先于 create 存在)
  才会出危险窗。与既有「先落冗余再放开」方向一致,不为省一记 UPDATE 引入组合写事务。
- **ACL 变更即时生效**:门每次请求实时读 `head_bucket`(与首轮相同的一记 PK 查询,
  无缓存——缓存化留独立 change)。

### 4. API 面

- `x-amz-acl` 桶级三处入口:CreateBucket 头、`PUT /{bucket}?acl` 头;取值白名单
  `private | public-read`,其它 → 400 InvalidArgument(**不做**大小写归一,对齐真 S3
  严格口径)。
- `PUT ?acl` 无 canned 头(AccessControlPolicy XML body 形式)→ **501 NotImplemented**
  (显式出界;不静默 200 也不 400 误导——headless 工具链按此判能力)。新增 error 变体。
- `GET ?acl` → owner 专属,回 AccessControlPolicy XML:owner FULL_CONTROL grant 恒在;
  public-read 时追加 AllUsers READ grant。

## 安全/持久性清单(CLAUDE.md §3.1–§3.3)

- [x] **写路径** —— 不触及 put/delete/compaction 语义;änder的唯一写面 = `buckets.acl`
      单列 UPDATE(0 行→NoSuchBucket),元数据原子、无半态;DB WAL 承担持久化。
- [x] **安全** —— 鉴权算法/凭证表/admin 域零改动;匿名通道**只**由「无凭证」触发且
      **只**抵达读门;错的凭证照样 403。public-read 是 per-bucket 显式 opt-in。
- [x] **幂等/重试** —— set_bucket_acl 幂等(同值再写);CreateBucket 重复走既有 409 家族。
- [x] 无新配置项、无新监控打点(矩阵/文档面值更);error 新增 NotImplemented 变体
      (code/status/message 全链落位)。

## 授权面分期(本 change 只做一期)

- 一期(本轮):桶级 private / public-read + 匿名读 + canned 三 API 落点。
- 二期(留评估,不立项):object ACL / 桶策略 JSON(IAM policy 引擎)/ 跨 owner 显式
  授权(Principal grant 语义)。理由:每项都是独立量级的语义+存储+测试面,且真实需求
  要等公开桶用起来之后才可验证;先让一期的最小面跑在真实流量下。

## 被否决的形式

- **sentinel AuthContext(owner=空串)表示匿名**:字符串魔法值穿越整个 handler 层,
  类型系统不设防 —— 否决,枚举类型化。
- **`Option<AuthContext>` 注入 extensions**:所有 handler 改判 `Option`,与 sentinel
  同病但少一层伪装;语义上「匿名」不是「没有结果」而是**一种主语** —— 否决。
- **per-route 双中间件(公开路由免鉴权)**:匿名属性其实是**桶数据**(public-read)的
  函数,路由表在编译期定义,做不到 per-bucket —— 由门在运行时裁决是结构必然。
- **支持 AccessControlPolicy XML body 全量 Put**:引入 policy 等价物 = 二期工作量混进
  一期,还带 XML 解析攻击面 —— 501 显式出界。

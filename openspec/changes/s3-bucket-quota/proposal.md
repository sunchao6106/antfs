# s3-bucket-quota —— 桶级存储配额(S3 门面族首轮)

## Why

S3 门面族(versioning / ACL / quota)中,quota 是运维最刚需的控成本闸门:多租户下一个 Bucket
写飞=整个集群遭殃。无配额的多租户对象网关不能上生产。首轮先落 quota(B1 里语义最独立、与既有
s3-gc 死点模型零冲突);versioning 有独立决策点(与 s3-gc「死点=语义动作时刻」的冲突,见
proposal 末尾——须用户拍板后另立 change)。

## What Changes

- **MetaStore**: `buckets` 表增 `quota_bytes BIGINT NOT NULL DEFAULT 0`(0=不限)+ 
  `used_bytes BIGINT NOT NULL DEFAULT 0`(= 该桶 state=1 行 size 之和)。PG 走 sqlx migration;
  MySQL 同构 DDL。
- **写入路径守卫**(PG):

  ```sql
  UPDATE buckets SET used_bytes = used_bytes + $new_size
  WHERE name = $1 AND (quota_bytes = 0 OR used_bytes + $new_size <= quota_bytes)
  ```

  在 put_object / complete_multipart 的**同一事务**内、INSERT 之前执行;0 行命中 → 
  `S3Error::QuotaExceeded`(HTTP **507**,与全系统「507=容量」契约一致);此时 blob 已写,
  handler 照 s3-gc 死点模型**同请求 router.delete 墓碑**再回 507。
- **扣减维护**(与标记同事务):delete_object / overwrite-shadow 置 state=0 时按行 size 扣
  `used_bytes`;GC purge 只删已扣过的行(state=0),无需再动。complete_multipart 的行 size=
  各 part size 之和(manifest 逻辑量)。
- **管理端点**(admin token 域,对齐 creds 管理形态):`GET /_admin/buckets/{name}/quota`、
  `PUT /_admin/buckets/{name}/quota`(body: `{"quota_bytes": N}`,0=解除)。
- **指标**:`s3_quota_rejected_total{bucket}` counter(§7 同步 s3.md 监控表)。
- **竞态口径(如实入 spec)**:守卫与计数是**单语句原子**(PG/InnoDB 短行锁);不做跨语句长锁。
  热桶吞吐上限 ≈ 单行 UPDATE 串行率(千级 ops/s/bucket),记为已知上界;跨语句竞态仅存在于
  「读配额展示」,不存在于「守卫」。

## Capabilities

### New Capabilities
(无独立新能力,均嵌入既有能力 spec,见 specs/ 的 s3-core / s3-metastore delta。)

### Modified Capabilities
- `s3-core`:PutObject 与 complete 入口被配额闸门约束;507 QuotaExceeded 错误契约。
- `s3-metastore`:buckets 表口径与 used_bytes 记账不变量(与 state=1 行集逐点同步)。

## Impact

- crate:`crates/s3`(migration ×1、trait +1 方法组、pg/mysql、handlers、admin、metrics、config 无新增)。
- 文档:`docs/design/s3.md`(配额节 + 监控表 +1 行;配额语义/竞态口径)。
- 测试:PG 集成(quota 拒/放/调大恢复/删释放/覆盖扣旧/complete 计入 multipart)。

## Non-goals

- 不做 user/account 级配额(只有桶级;二级主题在 ACL 轮次后再评估);
- 不做配额预警(soft threshold)通知——只做硬闸门;
- versioning 的死点冲突决策(A:versioned 桶改为保留行、GC 按「非当前版」策略;B:暂缓实现
  versioning)由用户拍板后另立 change,不在本 change 内。

# s3-bucket-quota — 设计说明

## 计数模型

`used_bytes(bucket) = Σ size WHERE bucket = $1 AND state = 1`(named_objects)。

**不变量:任何改变 state=1 行集/行 size 的事务,同事务同步 used_bytes。**

| 路径 | 行为 | used_bytes |
|---|---|---|
| put_object / complete_multipart INSERT | 守卫 UPDATE(原子)+ INSERT | `+ new_size`(守卫语句自带) |
| overwrite shadow(put/complete 落行遮蔽旧活行) | shadow UPDATE … RETURNING size | `− Σ 被遮蔽行 size` |
| delete_object(置 state=0) | UPDATE … RETURNING size | `− size` |
| GC purge(删 state=0 行) | 行已不在计数集合 | 不动 |
| admin PUT quota | 只改 quota_bytes | 不动(调小不追溯挡新写) |

**口径细节**:put 失败被重试时同 blobId 重写(handler 语义不变);quota 拒绝的 blob 已写入 →
同请求 router.delete(对齐 s3-gc 死点模型,墓碑幂等)。multipart:parts 不在 named_objects,
complete 时按 Σ part size 一次计入;abort 的 parts 走 GC 既有路径,与 used_bytes 无关。

## 守卫为什么用「单语句 UPDATE」而不是显式事务锁

- PG/InnoDB 的单语句 UPDATE 自带短行锁,天然原子;显式 `SELECT … FOR UPDATE` 会把锁窗口
  拉到整个 INSERT+shadow 事务,PUT 路径锁足迹恶化,无收益。
- 代价:热桶的 put 在 buckets 行上短锁串行化(µs~百 µs 级),单桶上限 ≈ 千级 ops/s——
  **如实记为已知上界**(spec 场景钉死),多桶互不影响。
- 竞态面:守卫原子 ⇒ 无超卖;写完 blob 才知配额拒绝的窗口 ⇒ 由死点模型应付(同请求墓碑),
  与 overwrite 拒绝路径同构。

## 错误契约

- `S3Error::QuotaExceeded` → **HTTP 507**(`InsufficientContentLike`?否——恒用
  `INSUFFICIENT_STORAGE`),XML code `QuotaExceeded`,带 bucket/quota/used 三要素。
  选 507 而非 403 的理由:全系统契约「507=容量」(store 侧 StoreFull、frontend 同映射);
  客户端语义=退避/清理后重试,与 403(权限,不可自动处置)不同族。

## MySQL 平价

同构 DDL + 单语句 UPDATE(WHERE 守卫)+ shadow/delete 的 `UPDATE … RETURNING` 缺失 →
按既有 s3-gc 的两步模式(先圈后改,InnoDB 当前读语义已验证过取件的等价性);MySQL 侧由
C1(CI 门)兜底真机验证,代码评审对齐 PG 语义。

## 与 s3-gc 的关系

零冲突:守卫/扣减都发生在「死点=语义动作时刻」的同一事务;GC purge 不触碰 used_bytes
(删的是已扣减行)。属于 s3-gc 死点模型的一次普通应用而非例外。

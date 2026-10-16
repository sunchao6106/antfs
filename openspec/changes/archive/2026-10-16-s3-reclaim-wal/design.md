# s3-reclaim-wal 设计

## Context

回收时序(五动作同构,以 overwrite 为例):

```text
① blob 先落(router.put,W 多数派)          ← ①前崩溃:无行无引用(经典孤儿,勘探性收回
② metastore 事务:遮蔽行/删行/换行 … commit     │   计,另行立项,本档外的既有已知面)
③ handler:router.delete(旧/弃/换下 blob)     ← ②③之间崩溃 = WAL 前的世界:泄漏,无记录
```

既有 GC 无从知道 ③ 未发生:put_object 遮蔽行 purg 时只清行不碰 blob(死点模型:
blob 墓碑责任在语义动作);put_part 换下的行在 ② 已被替换;complete 弃片与 abort
的 part 行在 ② 已删除——**②之后没有任何行携带这些 blob_id**。

关键事实:

1. 五个动作都有**同一事务**内的 RETURNING/SELECT 拿到待删 blob 列表(遮蔽
   RETURNING / 软删 RETURNING / part 行锁读换下值 / discarded 过滤 / abort 全
   片 SELECT)——入账材料现成,加一条 INSERT 即可;
2. 引擎 delete 幂等(墓碑重删 = no-op,S3 层三处既有注释与 GC 交叠面依赖);
3. handler 正常路径立刻 delete 成功——**常态下 WAL 行寿命 = 毫秒级**(写入→
   消账);表规模由异常频率决定,不是对象规模(对比:软删行保留期是小时/天级)。

## Goals / Non-Goals

**Goals:** 五动作的待删 blob 事务内入账;handler 成功即消账(常态趋空);GC 第
3 面补删残行(崩窗/个体失败收敛);多实例安全;观测(积压 gauge + 补删计数)。

**Non-Goals:** ①前孤儿(router.put 成功、metastore 拒绝/崩溃,无行无引用——
需要 blobId 空间对账/勘探,另行立项;upload-part 中间 blob 的同类窗在 ①② 之间
部分重叠,同样出界);master/store 层跨系统对账;purge 面语义改动(软删行保留
期渐清**不变**——pending 表独立,行 purg 顺序与新表无关);把 WAL 做成带
lease/重试计数的完整队列(失败重试天然收敛,不需要 lease——见 D2)。

## Decisions

### D1:WAL 形态 —— `pending_reclaims(blob_id, queued_at)`,主键即去重

入账用幂等 upsert(`ON CONFLICT (blob_id) DO NOTHING` / `INSERT IGNORE`)——
同一 blob 理论上只死一次(delete 幂等保),重入账(理论不可能,防御)免费去重。
不带来源/计数/重试列:**唯一用途是"这个 blob 还该删"**,消账口径单一(删除成功
即删行),多语义并列只会引来“半消账”状态机。`queued_at` 只为观测(gauge 可分
档老幼,故障时看 5 分钟前 fitter 积压即知道谁卡了多久)——不参与任何行为。

不 requeue:GC 对 `router.delete` 失败的 pending 行**不删行**,下轮自然重试——
这就是"行即重试单位";不引入 claim 位。

### D2:GC 取件形态 —— 非破坏批读 + 成功后批量 resolve

三个候选:
A. 取件即删(`DELETE … RETURNING`):违反本 change 目的——把崩溃窗从 handler
   平移进 GC(取完删完,GC 自己崩 = 同窗泄漏);**否决**;
B. lease/claim 位 + 超时重取:防双实例双删——但双删本就幂等无害,lease 引入
   时钟语义与搁浅态(claim 后崩 = 等超时),复杂度买不来安全性;**否决**;
C.(选)**`SELECT` 批读(带 limit)→ 逐个 router.delete → 成功者 `DELETE
   WHERE blob_id IN (…)` 批量消账,失败者留行下轮重试**。多实例交叠可双删
   (幂等安全,既有 GC 交叠口径同);每轮重读未消行(批 100 常量,表常态为空
   ~毫秒级残行,重读成本无视)。**崩窗闭合成立:行只随 delete 成功消失**。

### D3:五挂点入账位置 —— 一律在既有事务内,材料零额外读

| 动作 | 事务 | 待删材料(已有) | 入账 |
|---|---|---|---|
| put_object(含 complete 嵌套) | 记账事务 | 遮蔽 `RETURNING blob_id` | 同 tx INSERT |
| delete_object | 墓碑事务 | 软删 `RETURNING blob_id`(MySQL 先 SELECT) | 同 tx INSERT |
| put_part | 会话锁事务 | part 行锁读到的 old | 同 tx INSERT |
| complete_multipart | 会话锁事务 | `discarded_part_blobs`(内置过滤) | 同 tx INSERT |
| abort_multipart | 会话锁事务 | part 全列 SELECT | 同 tx INSERT |

失败传播:INSERT 失败 = 整个语义事务失败回滚 → **不产生"无 WAL 的语义动作"**
(WAL 先于可见性,不存在先 commit 后补账的窗)。这是本 change 的原子性核心。

### D4:handler 消账点 —— 成功一个消一个,失败留行不 warn 降级

既有 `if let Err(e) = router.delete(old)` 的三种现场(put_object 遮蔽/配额墓碑、
multipart 旧片/弃片、delete_object 现无失败容器——查后补):改为删除成功后
`resolve_pending_reclaim(bid)`;失败时原 warn 保留 + **行留**(GC 补)——warn
文案从"leaked until manual cleanup"改为指向 GC 兜底(语义变化如实落文档)。
配额拒绝路径的墓碑 blob 同一处理(同请求 WAL+消账)。

### D5:观测 —— gauge 每轮采样 + GC 补删计数

`s3_gc_pending_backlog`(gauge:本轮批读后的残留计数,`COUNT(*)` 一发;常态 0)
+ `s3_gc_pending_resolved_total`(counter:GC 面补删成功数,与 handler 即时消账
**分开计数**——发生率即崩窗/故障率的直接信号)。§7 文档表两行落 s3.md 监控表。

## Risks / Trade-offs

- **写放大**:每语义动作 +1 INSERT(~几十字节)——比软删行保留期(小时级持行)
  轻数个量级;主键去重防重;
- **GC 双删交叠**(D2-C):幂等安全,spec「多实例安全」要求原文已含"blob-delete
  overlap remains safe by engine idempotency"——非破坏读恰用该豁免句,行为一致;
- **表陈年风险**:若 `router.delete` 对某 blob **永久**失败(节点永久下线),行
  永生 + gauge 持续 >0 告警——这正是需要运维介入的信号(比静默泄漏进步;把
  "永久失败需要替换副本"归入 store/master 的副本修复职责,不在 s3 层解)。

## 测试证伪面

- metastore 单测(双方言真库):五动作各自→ WAL 行恰现(数量/身份);resolve →
  行清;同 blob 双入账幂等一行。
- e2e:正常 overwrite/delete/重传/abort/complete 序列后 WAL 表**空**(快速消账
  断言);手工注入残行(模拟崩窗)→ `run_once_at` → 墓碑到位(poll_tombstoned)
  + 行清 + `s3_gc_pending_resolved_total` 恰增(真接入 Registry 的断言,CLAUDE.md
  §6)。
- 既有 68+ 用例零回归(死点语义/配额/所有权恰一次等全部不变)。

# s3-reclaim-wal 任务

## 1. Schema + API(metastore)

- [x] 1.1 /**006 双言落盘(CREATE IF NOT EXISTS;双言 e2e 迁移自动应用真跑)** `migrations/{postgres,mysql}/006_add_pending_reclaims.sql`
  (`CREATE TABLE IF NOT EXISTS pending_reclaims (blob_id <同 named_objects
  类型> PRIMARY KEY, queued_at BIGINT NOT NULL)`)
- [x] 1.2 /**trait 增 pending_reclaims_batch/resolve/count;pg/mysql 实现齐;FakeMetaStore 补桩** MetaStore trait:`pending_reclaims_batch(limit) -> Vec<BlobId>`(非破坏
  批读,升 queued_at)+ `resolve_pending_reclaims(&[BlobId]) -> u64`(删行);
  pg/mysql 实现(SELECT … LIMIT / DELETE WHERE IN = … 二次 bind 直接拼常量)
- [x] 1.3 /**五挂点入位(pg/mysql 同构,同事务幂等 upsert;入账失败随主事务回滚由语句级传播即证)** 五事务挂点(pg/mysql):put_object 遮蔽行 RETURNING→INSERT;delete_object
  软删 RETURNING→INSERT(MySQL 先 SELECT 处);put_part 旧值→INSERT;
  complete discarded→INSERT;abort 全片→INSERT(幂等 upsert 两者);入账失败
  = 主事务自动回滚(语句级传播验证)
- [x] 1.4 /**reclaim_wal_transactional_recording 双方言绿(五挂点/resolve/主键去重/自洁尾)** metastore 单测(双方言真库,`#[ignore]`):put_object overwrite → WAL 行
  恰现;delete/put_part 换值/complete 弃片/abort 各挂点;resolve 清行;同 blob
  双入账幂等

## 2. handler 消账 + 指标

- [x] 2.1 /**gc_pending_resolved_total + gc_pending_backlog 注册;s3.md 监控表 +2 行同步** metrics.rs:`pending_resolved_total`(counter)+ `pending_backlog`
  (gauge);注册与 s3.md §7 监控表两行同步
- [x] 2.2 /**tombstone_and_resolve 单点;五消账点接线(配额路径维持不入账=回滚无 WAL,proposal/design 记档);warn 文案改 GC 口径** 五消账点:put_object 配额墓碑 + 遮蔽循环(S3Error::warn 文案整改为
  GC 兜底口径);delete_object;upload_part 旧片;complete 弃片+遮蔽;abort
  全片——成功→resolve,失败→留行原 warn
- [x] 2.3 /**gc.rs 第 3 面(批读 200/逐删/成功 resolve/失败留行/轮末 gauge);GcConfig 零新增** gc.rs 第 3 面:批读(GC_BATCH_RECLAIMS=200)→ 逐删 → 成功者批量
  resolve → failure 记 `s3_gc_errors_total{kind="pending"}`;轮末
  `pending_backlog.set(count 采样)`;GcConfig 无新增

## 3. e2e(双言)

- [x] 3.1 /**reclaim_wal_steady_state_runs_dry 绿(含 backlog gauge = 0)** `reclaim_wal_steady_state_runs_dry`:正常 overwrite/delete/重传/
  abort/complete 序列后 `pending_reclaims_batch` 空(handler 消账全绿)
- [x] 3.2 /**reclaim_wal_gc_closes_crash_window 绿(poll_tombstoned + 恰 +1 计数真 Registry)** `reclaim_wal_gc_closes_crash_window`:手工注入残行(直插 metastore)
  →`run_once_at`→blob 墓碑到位(poll 复用)+ 行清 +
  `s3_gc_pending_resolved_total` 恰增(真接入 Registry 的断言,CLAUDE.md §6)
  + `s3_gc_pending_backlog` 归零
- [x] 3.3 /**PG 87/87 + MySQL 87/87;全量跑毕两方言表 0 残行(lib 测试八处 wal_wipe 自洁)** 双言全量零回归

## 4. 文档与收尾

- [x] 4.1 /**s3.md GC 段/监控表 +2/新节「回收 WAL」/前端矩阵运维面/后续方向;design README 路线行(条目1更正+条目3✅);DEVLOG 步 66** s3.md:操作映射/矩阵不变行;新节「回收 WAL」(WAL 语义/五挂点/GC 第
  3 面/多实例交叠/永久失败归 store 副本修复);监控表 +2 行;后续方向;design
  README 路线档 3 行(条目 1 更记「已由 s3-multipart-lifecycle 落地」、条目 3 ✅)
- [x] 4.2 /**workspace 72 套件绿;clippy 0;fmt 干净;tasks 勾满;归档于本步执行** workspace test / clippy 0 / fmt 干净;DEVLOG 步 66;tasks 勾满;
  归档(用户验收后)

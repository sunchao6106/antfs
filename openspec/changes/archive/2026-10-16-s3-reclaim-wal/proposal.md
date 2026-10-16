# s3-reclaim-wal —— 回收 WAL:待删 blob 上表,GC 兜(commit↔墓碑崩窗闭合)

## Why

五个语义动作的 blob 回收都是「metastore 事务提交后,handler 再 `router.delete`」
两段式(put_object 遮蔽 / delete_object 软删 / put_part 同号重传换下旧片 /
complete 弃片+遮蔽 / abort 全片)。**commit 与墓碑之间崩窗存在**:网关在该窗内
崩溃(或 router.delete 个体性失败只 warn)即泄漏——部分行/行早已删除或已被替换,
没有任何残留记录知道"这个 blob 该删",GC 永不收。

- DEVLOG 步 54 留观原文:「commit 后回收下发前的崩溃窗(同 `shadowed_blobs`);
  part 回收并入 GC 死点留独立 change」;
- 阶段二优化路线档 3 第三行:「GC 死点崩溃窗兜底(待回收 blob 上表,GC 周期补删)」;
- CLAUDE.md §3.1 持久性偏执红线:资源清理生命周期位(Jvmk 3.3 清单「资源清理」项)。

## What Changes

- **`pending_reclaims` 表(migration 006,双方言)**:`blob_id` 主键 + `queued_at`
  可观测时间列——**回收 WAL**:五个语义动作的**同一事务内**把待删 blob 落行
  (幂等 upsert,`ON CONFLICT DO NOTHING` / `INSERT IGNORE`),commit 先于墓碑
  保证零丢(窗移到表内,GC 可见)。
- **五挂点**(pg/mysql 同构,均在既有事务里追加一条 INSERT):
  `put_object`(遮蔽 RETURNING 的 blob 列表)、`delete_object`(软删 RETURNING 的
  blob)、`put_part`(被换下的旧 blob)、`complete_multipart`(未列入 manifest 的
  弃片;遮蔽路径经嵌套 `put_object` 自动覆盖)、`abort_multipart`(全片)。
- **handler 即时消账**:既有立即 `router.delete` 不变;成功后调
  `resolve_pending_reclaim(blob_id)` 删 WAL 行(表日常趋空,只有崩窗/失败残行)。
  `router.delete` 失败时**不再孤立**:WAL 行留,GC 补删。
- **GC 第 3 面**(`gc.rs`/两 spec):批读 pending(非破坏读,limit 常量)
  → 逐个 `router.delete` → 成功者批量 resolve;失败者留行下轮重试。多实例交叠
  双删安全(引擎墓碑幂等,既有口径);非破坏读+成功后 resolve 的取件形态是
  **崩窗安全**的必要选择(取件即删会把窗从 handler 移进 GC——记入 design D2)。
- **消账指标** `s3_gc_pending_resolved_total`(counter,GC 补删成功数;§7 文档
  表同步)+ WAL 积压 gauge `s3_gc_pending_backlog`(每轮采样)。
- 无新配置;`s3_gc_errors_total{kind="pending"…}` 复用。

## Capabilities

### New Capabilities
(无)

### Modified Capabilities
- `s3-metastore`:新增(ADDED)「回收 WAL 表与事务内入账」要求(migration 006、
  五动作入账不变量、`pending_reclaims_batch`/`resolve_pending_reclaims` API)。
- `s3-gc`:GC 循环加第 3 面(pending 补删;非破坏读+成功 resolve 语义;并发
  交叠安全性并入既有「多实例安全」要求)。
- `s3-gc`:遮蔽即回收要求补 WAL 口径 + 新增(ADDED)GC 第 3 面要求。
- `s3-multipart`:UploadPart 重传回收补 WAL 口径(abort/complete 由 metastore
  的 WAL 表要求统一覆盖,不再另立 delta)。

## Impact

- 代码:`metastore/{types,mod,pg,mysql}.rs`(表+两 API+五事务挂点)、
  `handlers/{object,multipart}.rs`(成功后消账,五处)、`gc.rs`(第 3 面循环)、
  `metrics.rs`(两指标)、`migrations/*/006_add_pending_reclaims.sql`、
  `tests/{object,multipart,common}`(GC 兜底 e2e + 既有断言不变)。
- 契约:零 API 变化(表新增 + handler 内部消账);GC 行为只增不改(既有两面
  断言不动);migration 幂等(CREATE IF NOT EXISTS,双方言)。
- 持久性风险分析(§3.3 清单):本 change 只**收紧**泄漏语义(bump-on-write 模式:
  WAL 行随事务落,不引入新 fsync 边界——MetaStore 自持原子性即足够;WAL 写失败
  = 主事务同滚,不产生"无 WAL 的删除")。
- 测试:metastore 单测(五动作事务内落行/resolve 清行);e2e(正常路径表清空 +
  模拟残行→GC 补删→墓碑到位→表清 + gauge);双言全量零回归。

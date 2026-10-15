# Proposal: multipart 会话生命周期原子化(所有权恰一次)

## Why

multipart 会话的三个参数操作(`put_part` / `complete_multipart` / `abort_multipart`)此前是
**多语句自动提交**,存在三处真实泄漏口(均 = 泄漏的 blob/part 行**无墓碑、GC 永不收**):

1. **并发重传同 part 泄漏中间 blob**:`put_part` 先无锁快照读旧 blob_id 再 upsert;并发
   重传时后到者读到**过时快照**的 old,而被换下的中间 blob 无人负责删(exactly-one 失效)。
2. **abort/complete × put 插队产生无主 part 行**:先验后写两语句间,对端已把 session 行
   删除,put 的 upsert 落在 DELETE 之后 → 只剩 part 行与 blob,无人回收。
3. **CompleteMultipartUpload 丢弃未列入 manifest 的 part(S3 语义)**:现行实现删 part 行
   却不回收对应 blob → 确定性泄漏(无需竞态即触发)。

## What Changes

- 双方言(PG / MySQL)`put_part` / `abort_multipart` / `complete_multipart` 重写为
  **单事务 + `multipart_uploads` 行锁(`SELECT … FOR UPDATE`)全序化**:
  - 会话行锁 = 会话生命周期的全序点:put/abort/complete 三者互斥,插队窗口结构性关闭;
  - `put_part` 在事务内**锁读**(`FOR UPDATE`)part 行拿旧值——锁读读 latest committed,
    并发重传时后到者恰拿到前驱提交后的真 old;
  - `complete_multipart` 会话锁跨越 `put_object`(对象行事务与会话行不相交、加锁单向,
    无死锁)——manifest 落库期间无 put 可插队。
- `CompleteMultipartResult` 新增 `discarded_part_blobs`:未列入 manifest 的 part blob 上报
  给 handler,与 `shadowed_blobs` 同路径逐一 `router.delete`(幂等墓碑)。
- **不改任何外部 API、错误码、配置、指标**;死点/GC 模型不动。

## Impact

- **兼容**:纯行为收敛(泄漏口修复);trait 签名不变,`CompleteMultipartResult` 加字段
  (结构为 crate 内部构造,仅两处 Ok 构造 + Fake 走 `Err`)。
- **延迟**:put_part/abort/complete 各多一记事务包裹与会话行锁(亚毫秒池化);同 session
  并发操作串行化是预期语义(它们本就互斥)。
- **持久性红线分析**见 design.md(CLAUDE.md §3. 清单逐项)。

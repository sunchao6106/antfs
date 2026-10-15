# Design: multipart 会话生命周期原子化

## 上下文

泄漏口三处(提案书 What/Why)全在「元数据先读后写 + 自动提交」这一共同形状上;修复 =
把读-改-写收进一个事务,并以 `multipart_uploads` 行锁(`FOR UPDATE`)作为**会话生命周期
全序点**。PG 与 InnoDB 的锁读语义一致(读 latest committed、阻到持有者提交),双方言
共享同一论证与同一测试不变量。

## 事务形状

```
put_part:   BEGIN → 锁 multipart_uploads 行(FOR UPDATE;不存在即回滚返 NoSuchUpload)
            → 锁读 multipart_parts 行(FOR UPDATE,= 真旧值)→ upsert
            → COMMIT → 返回旧 blob_id(handler 幂等 router.delete)
abort:      BEGIN → 锁 uploads 行 → 读终版 parts → DELETE parts → DELETE uploads
            → COMMIT → 返回全部 part blob_id(handler 逐一删)
complete:   BEGIN → 锁 uploads 行 → 读终版 parts → 校验/计账逻辑
            → put_object(独立事务,见下)→ DELETE parts + DELETE uploads → COMMIT
            → 返回 manifest + shadowed_blobs + discarded_part_blobs
```

关键推论:

- **重传恰一次**:两个 put_part 竞争同 part,先提交者换 A→X 并持锁到 commit;后到者的
  part 行锁读**阻塞至前者提交**、读到 X(而非过时快照 A),于是正好对 X 负责——每个被
  顶替的 blob 恰一人回收。
- **abort × put 全序**:put 先 → abort 锁内看到该 part、删除并交还其 blob;abort 先 →
  put 在会话锁上拿到「行已不存在」→ `NoSuchUpload`(无插队窗口)。
- **complete × put 同理**;会话锁跨越 `put_object` 窗口,manifest 落库即终稿。
- `put_part` 的 part 行锁读看似多余(会话锁已串行化所有会话级操作),但它防的是本函数
  自己的「先读后写」回退:没有它,直接绕回旧的快照失真形状。这是显式的正确性结构,保留。

## 持久性红线分析(CLAUDE.md §3.3 清单)

- [x] **写路径** —— 触及(put/complete/abort 的元数据事务):元数据侧原子化,事务半途
      崩溃即回滚,不存在「part 行存活而 session 行已删」的半态(这正是被修复的对象)。
      DB 侧 WAL 承担持久性;不涉磁盘格式。
- [x] **成功回报时机** —— handler 的 blob 回收(router.delete,幂等墓碑)发生在事务提交
      **之后**;「元数据先行、回收随后」的次序与既有 `shadowed_blobs` 路径一致。
- [x] **原子性边界 / 死锁** —— `put_object` 开自己的事务(objects/buckets 行),与本会话
      锁(multipart_uploads 行)不相交且获取序单向(会话锁 → 对象行),无环;不同 session
      互不阻塞。跨事务持锁时长 = 一次 put_object(毫秒级),可接受。
- [x] **幂等 / 重试** —— put_part 上游重试安全(upsert 幂等,被顶替 blob 恰一人负责是
      返回值语义,重传替换仍 exactly-one);abort 重试 → `NoSuchUpload`;complete 重试 →
      `NoSuchUpload`(既有语义,本轮不动)。
- [x] **部分失败** —— 崩溃可发生在「事务已提交、回收未下发」之间:该 blob 无墓碑、GC
      不收。**既有类风险**(`shadowed_blobs` 同窗),本轮既未扩大也未消除;完整修法是把
      part 回收并入 s3-gc 死点(墓碑先行、删除后收),记为 limitation,留后续独立 change。
      修复前竞态泄漏的触发概率远高于该崩溃窗,先封大口。
- [x] **损坏处理** —— 不涉盘格式;etag 不符依旧 `InvalidPart`。

## 被否决的形式

- **PG 单语句 CTE 拿旧值**:CTE 快照在竞态下同样可能读到过时值(锁读才保证 latest),
  且双方言不同构。事务 + 两记锁读,双方言同一论证,胜出。
- **handler 层互斥**:网关进程内锁盖不住多实例(网关设计即无状态可横扩),在 metastore
  处根除才普适。

## 测试策略

- 内联单测(双方言,真机 PG / MySQL):
  ①`put_part_concurrent_retransmit_exactly_once` —— 8 路并发重传,N 个 blob 的所有权
  (终版行 + 返回的旧值)合计恰 N、无一缺失无一重复;
  ②`complete_multipart_discards_unlisted_parts` —— 传 3 片只列举 2,`discarded_part_blobs`
  恰为第 3 片,会话行已清;
  ③`abort_vs_put_part_never_leaves_orphan_row` —— 并发 abort×put,两个方向终局之后
  会话与 part 行均不存在,成功方恰带回对应 blob。
- e2e(用户可见面):`concurrent_retransmit_completes_and_reads` —— 8 路并发 UploadPart
  全 200 → ListParts 读回权威 etag → complete 200 → GET 数据自洽。

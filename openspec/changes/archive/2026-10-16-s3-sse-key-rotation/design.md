# s3-sse-key-rotation — Design

## Context / 轮换形态选型

- 帧几何/描述符/key_id 语义**零改动**——轮换 = 把密文行换到新钥下,引擎面只见
  「读一个 blob、写一个 blob」。
- 存量前提:网关单主钥(`s3.sse_master_key_file`),行描述符自报 `key_id`,帧头冗余
  同值互证(读侧已如此)。**这给轮换免费的自描述进度表**:每行描述符即「当前在哪把
  钥下」。

## D1:离线工具,不做在线轮换

并发写同 key 与轮换的读-改-写天然竞态(工具读到旧版本,期间网关 PUT 新数据,工具
`put_object` 会把**旧数据的换钥版**盖上去 = 丢更新)。在线安全需要跨「engine IO +
crypto」的长事务锁或 epoch 协调,代价不成比例——离线(网关停机、引擎层可用)是
业界常规形态,记 s3.md 为硬前提。工具起手打印停机警告,不做在线检测(无可靠判据)。

## D2:可续跑 = 构造所得,非工具状态

失败中止(宁停勿错:解不明/写不进就停,续跑面交给操作员)。**无进度表、无水位行**:
已轮换行的描述符 key_id 即新钥 → 重跑时归「他 key 跳过」。重跑同一 (旧,新) 对即
天然幂等收敛;已中止的坏行修好后重跑即可。不引入任何轮换专用持久状态(零 schema
变化的直接推论)。

## D3:换行弧复用既有 PUT 语义,不自砌

`put_object`(blob 先落 → 行后写 → 遮蔽返回 shadowed_blobs → 墓碑 + WAL 消账)=
`s3-reclaim-wal` 定型的弧。轮换行 = 全字段原样(etag/size/content_type/user_meta/
checksum 恒明文口径不变,**换 blob_id + sse 描述符**)。中途夭折暴露面与普通 PUT
等价(孤儿 blob 同款收口;行写失败按配额拒弧同请求墓碑新 blob——幂等删)。
SSE 行带 `parts_manifest` = 不可达状态(multipart SSE 写侧 501、拷贝重钥产单 blob),
按守卫**中止**而非默默展开。

## D4:他 key_id 行(过往轮换残留 / 异常数据)跳过不动

旧钥解不开(不属本代),动了就是瞎猜。跳过计数进报告;重跑恒见恒跳(不收敛)即
操作员信号:查那些行属于哪一代钥。保持 `version` / blob 原引用字面不变。

## D5:分页扫描 `(bucket, key)` 排他游标,无幻读环

轮换会就地重写已扫过的行(同 bucket+key、更高 version、旧行 state=0)。扫描按
`(bucket, key) > cursor` 推进:重写行键值 = cursor 左侧,严格大于判永不再入页——
循环免疫。state=1 每键唯一(遮蔽语义),页内无重复键。

## 交付面

CLI `--rotate-sse-key <file>`(同 config 起状态,不 GC 不监听);`rotate()` 为库函数
(e2e 直调,免起进程)。报告 = `RotationReport { scanned, rotated, skipped_other_key }`
+ 中止态 `RotationFailure { report, bucket, key, error }`。无新配置/监控(CLI 面,
stdout + 退出码)。

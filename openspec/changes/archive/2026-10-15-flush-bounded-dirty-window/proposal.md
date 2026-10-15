# Proposal: flush 有界脏窗口(水位早醒,fsync 拆窗)

## Why

BASELINE 真机复验(2026-10-15)钉死的问题:滚转异步化(`defer_seal_flush`)修掉了
滚转点的秒级停摆,但把 GB 级脏回积**挪进**了周期 flush worker ——「异步 seal + flush
2s」模式 p99 182-185 ms / max 1.19-1.54 s(四模式 max 皆 1.1-1.6 s = 多 GB 脏积压
一次 fsync 的真实代价)。worker 的一次全量 fsync 会占住盘池 worker 并与并发写互踩
(APFS 上互相限流),p99/max 被污染。BASELINE 原文留的 P2 记号:bounded dirty
window(fsync 拆窗)。

## What Changes

- **引擎(机制)**:`Log` 挂脏字节计数(`Arc<AtomicU64>`):
  - `append` 成功后按记录物理长度递增;`Reserved`(流式发布径)携带克隆句柄,
    `commit()` 成功发布时递增 —— 两发布径全覆盖;
  - `Log::flush` / `Log::flush_all` 末尾**尝试即复位**(成功路径精确;失败路径放弃
    计数 —— 重新武装需再来一整段阈值的新字节,配 30s 周期兜底,杜绝失败风暴);
  - `BlobStore::dirty_bytes()` 透出。**纯咨询量**:不参与任何正确性判定。
  - 不计:compaction 重写径(其自身在写锁流内收敛)、bootstrap 物理拷贝径(自带
    flush)—— 计少只会晚醒,方向安全。
- **worker(策略,store-server)**:flush loop 从纯 30s 周期改**混合 tick**(100ms):
  - 距上次全量 ≥ `flush_interval_secs` → 全量轮(现状语义,兜底保留);
  - 否则,`flush_dirty_threshold_bytes != 0` 且某副本 `dirty_bytes() ≥ 阈值` → 只把
    **超阈副本**提交到它那块盘的池执行 flush(盘池/坏盘跳过口径与全量轮一致);
  - 溢出预算:100ms 轮询粒度 × 7.4 GB/s(本机上界)≈ ≤740 MB 惊人端;生产百 MB/s
    级磁盘 ≈ ≤30 MB **稳态**。256 MiB 阈值的一次 fsync(百 MB/s 盘约 0.3~2.5 s 墙钟
    上界按盘而定,闪存 ≪)远小于 flush 的 120s 看门狗预算。
- **配置**:`store_server.flush_dirty_threshold_bytes`(u64,默认 **0(关)**:同日真机 APFS/GB-s 反例证据见 design「真机证据」,ext4 未测;机制完整落地,显式开启即生效);监控新增 `store_server_flush_early_wakes_total`(见 cluster.md 指标表)。
- **基准**:bench-blob 增 `--dirty-threshold-mib`(自建 flush 线程同构混合 tick);真机 A/B 起初拟 5000-op 口径,因相位短于间隔不触发病理,改 **20 GB 口径**取证 —— 结果反转默认取值,记档见 design/BASELINE。
  真机重放「1 MiB × 5000 @ 4 GiB + flush 2s」A/B,证据回填 BASELINE(P2 记号销账)。

## Impact

- **持久化语义零变化、单调增强**:flush 仍是唯一持久化边界;本轮只**增加**边界推进
  频率(达阈早醒),从不减少;不变量「log 全量持久先于 index flush」不动(early 提交
  的也是同一个 `BlobStore::flush` 两相流程)。
- 写路径新增一条 Relaxed 原子加(纳秒级);水位门槛在 worker 侧,不阻塞任何写者
  (刻意:写者背压本就是 OS page-cache 的职责,本项解 p99 污染而非配额)。
- 失败语义:early flush 失败 = warn + 跳过(与全量轮同一口径);计数复位后需新
  `阈值×1` 字节才再触发,坏盘上不会形成 100ms 风暴。
- **证据反转(同日,诚实记档)**:APFS/GB-s 口径水位臂吞吐 −60%、max 反劣(至 5.5 s);生产写入速率下两态等价 —— 故**默认 0(关)**,不改生产行为;Linux/ext4 情形待真测再议开启。

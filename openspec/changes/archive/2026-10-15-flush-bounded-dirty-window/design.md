# Design: flush 有界脏窗口(水位早醒)

## 机制

### 引擎侧(crates/blob,纯机制,无配置无策略)

`Log` 增一计数字段:

```rust
pub(crate) struct Log { ..., dirty_bytes: Arc<AtomicU64> }
```

- **两发布径全覆盖**:
  - `Log::append` 成功后 `fetch_add(record.len())`;
  - `Log::reserve` 把 `Arc<AtomicU64>` 克隆塞进 `Reserved`;`Reserved::commit` 成功
    发布后 `fetch_add(self.total)`。(`Reserved` 由 `Log::reserve` 唯一构造,逃逸到
    store 层也照数不误 —— 计数在 commit 点与数据可见性同步。)
- **复位点 = flush 尝试本身**:`Log::flush` / `Log::flush_all` 末尾 `store(0)`
  (**尝试即复位**,无论成败)。
  - 成功路径:精确(此刻全部字节已 fsync;活跃段 flush 在 `write_lock` 内、无并发
    append,复位无竞态)。
  - 失败路径:放弃本窗计数 —— 重新武装需**再来一整段阈值的新字节**,加上 30s 周期
    兜底照常重试;坏盘上不会形成 100ms 重试风暴。
  - `drain_pending`(只收敛封存段)与 `close` **不**复位 —— 不是全量边界点。
- **不看守**:compaction 重写径(其写收敛在自己的写锁流内)、bootstrap 物理拷贝径
  (自带 flush)。计少 ⇒ 只会晚醒(回落到周期语义),方向安全。
- `BlobStore::dirty_bytes()` 透出。**全量为咨询**(`Ordering::Relaxed` 足够 —— 它只
  是水位,不建立任何 happens-before)。

### worker 侧(crates/store,策略)

flush loop 由纯周期 `spawn_loop(interval)` 换为专用 **混合 tick**:

```
tick = 100ms(常量 FLUSH_CHECK_TICK,溢出预算见下)
每 tick:
  if 自上次全量 ≥ flush_interval_secs → 全量轮(与现状同语义;兜底恒存)
  else if threshold != 0:
    选出 dirty_bytes() ≥ threshold 的副本集
    非空 → 只把这些副本提交到各自盘池 flush(早醒轮)→ flush_early_wakes +=
```

- 早醒轮复用全量轮的执行核心(抽取 `flush_replicas(pool, iter, health, metrics)`,
  `flush_all` 变为全集包装,原签名不变);盘池准入/坏盘跳过/`store_server_flushes_total`
  口径完全一致 —— 早醒只是**更频繁、更小批**的同一种轮次。
- 判活选集在查询线程(tokio)读原子,不碰盘池;提交本身 `pool.run`,不阻 reactor。
- 首拍即触发全量(与 `tokio::time::interval` 首拍语义一致,保持启动行为)。

### 溢出预算(为什么不撞看门狗)

- 唤醒延迟 ≤ tick 100ms ⇒ 唤醒时脏量 ≤ threshold + 写速×100ms:
  本机(Apple SSD 7.4 GB/s)极端 ≈ 256 MiB + 740 MB;生产百 MB/s 级 ≈ 256 MiB + ≤30 MB。
- 一次水位 fsync 的墙钟 ≈ 脏量 ÷ 盘吞吐:本机 ≈ 100 ms 级;慢盘(200 MB/s)上
  ≤ ~2s —— 仍远小于 flush 的**126s…120s** 看门狗预算(`disk_watchdog_ms_by_op`
  flush=120000),而「数百 MB 而非 4 GiB」的量级本身就是 p99 修复。
- 早醒后 write_lock 内活跃段 flush + index flush 的锁内段不变(与现有周期 flush 同
  批判过)。GB/s 级持续写入下早醒轮背靠背也只是把 4 GiB 的秒级停摆摊成若干次百 ms 级。

## 持久性红线分析(CLAUDE.md §3.3 清单)

- [x] **写路径** —— append/commit 逻辑零变化,仅追加一条 Relaxed 原子加(纳秒级);
      持久化边界定义("flush 是唯一持久化边界,data_end 只在 fsync 点推进")逐字保留。
- [x] **持久化边界/原子性** —— 早期醒与周期轮执行**同一个** `BlobStore::flush` 两相
      流程(锁外 drain 封存段 → 锁内 log 先于 index);不变量零改动。
- [x] **成功回报时机** —— put 的 ack 路径不依赖 flush;不改任何回报语义。
- [x] **坏路径** —— early flush 失败 = warn + 跳过(与周期轮同口径);"这块盘没在推进"
      依旧由 skipped 指标/日志可见。
- [x] **重试/幂等** —— flush 幂等(重复同步无副作用);失败窗的计数放弃 ⇒ 退化为
      纯周期语义,30s 兜底重试 —— **任何计数诡异的最坏结果 = 回到改动前行为**。
- [x] **资源清理** —— 无新文件/线程;一个 Arc 原子量 per Log,一条 tick 任务 per 节点。
- [x] **测试** —— 既有崩溃一致性属性测试(flush 边界、deferred seal 收敛)原样必须
      全绿(语义不变即回归保护);新增计数/复位/水位单测。

## 被否决的形式

- **写者背压(脏超硬顶阻塞 put)**:新阻塞面 + p50 回归风险;内存压力本属 OS
  page-cache 职责;本项解 p99 污染而非配额 —— 否决。
- **`sync_file_range` 按段区间后台拆刷**(Linux-only):非可移植(macOS 开发目标),
  std 不暴露,复杂度高 —— 否决,留二期评估。
- **事件驱动(Condvar/Notify 唤醒 worker)**:引擎 std-only × worker tokio 的跨crate
  异步桥不划算;100ms 轮询一条 Relaxed load 的 CPU 可忽略 —— 轮询胜出。
- **按段计账(每段自知脏量)**:`entries` 读锁 + 遍历求和 vs 一条聚合原子 ——
  聚合胜出;复位点构造性安全(锁内全量 flush 完成点)。
- **seal 时刻拆块刷**:与水位机制重复(水位已把封存段遗留脏量压到 ≤ 阈值+溢出)。

## 真机证据与默认取值(2026-10-15,APFS/Apple SSD,20 GB 口径)

| 臂(1 MiB × 20k,4 线程,4 GiB 段,flush 2s)| ops/s | p50 / p99 | max |
|---|---|---|---|
| 裸(无 flush 线程)x2 | 4269-6042 | 141-155 µs / 0.3-2.8 ms | 953-1060 ms |
| 周期 flush 2s(现默认,threshold=0)x2 | 3229-3521 | 144-172 µs / 1.4-2.5 ms | **1136-2065 ms** |
| 水位 256 MiB(早醒)x2 | **1347-1403** | 415-424 µs / 1.4-17.6 ms | **1651-5508 ms** |

- **周期臂如实复现 BASELINE 病理**(max 1.1-2.1 s = 多 GB 脏积压一次 fsync),方法论
  有效(5000-op 旧口径今日已不足以触发病理 —— 相位 < 间隔,故扩量至 20 GB)。
- **水位臂在 APFS 上全面劣化**:吞吐 −60% vs 周期臂,max 反而更大(5.5 s)。机制归因:
  APFS「fsync × 并发写」互相限流随 **fsync 频数**放大 —— 拆窗把一次巨停摆换成一串
  中等停摆,在本 FS 上净亏。
- 生产写入速率(≈百 MB/s)下水位命中周期 ≈ 2.5 s ≈ flush 间隔,两态行为等价。
- **结论:机制保留,默认 0(关)。** Linux/ext4(无此互踩形状)待真测;同机当日
  状态波动按「±本机真铁」口径,臂内双跑一致、臂间差异稳定。

## 配置/监控(CLAUDE.md §7,表随码同改)

- `store_server.flush_dirty_threshold_bytes`| u64 | **0(关)** | 任意正字节(典型
  128-512 MiB;过小 = 近乎逐写 fsync)| 早醒水位 —— 默认关(APFS 反例证据见上节,
  ext4 未测;开启 = 机制生效,交 Operator 判断)
- `store_server_flush_early_wakes_total` | counter | 无标签 | 次 | 早醒轮触发数
  (观测水位机制是否在工作)

## 测试 & 证据

- 引擎单测(log.rs):append/commit 两径计数;flush/flush_all 复位;滚转+deferred
  seal 后 flush 复位;reserve 未 commit 不计数。
- worker 单测(store):`flush_dirty` 只提交超阈副本(低于阈值不动)+ 早醒计数落真
  指标;阈值 0 = 无早醒。
- 既有全量(含崩溃一致性属性用例)原样绿 = 语义不变的回归证明。
- **真机证据**:bench-blob `--dirty-threshold-mib`,重放「1 MiB × 5000 @ 4 GiB + flush
  2s」A/B,回填 BASELINE(P2 记号销账);口径沿「±本机真铁」。

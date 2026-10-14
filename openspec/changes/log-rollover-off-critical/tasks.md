## 1. 引擎(log.rs)

- [x] 1.1 `Log` 增 `pending_seals: Mutex<Vec<Arc<LogSegment>>>`(`create`/`open` 初始化);`roll_over` 去掉 `active.segment.flush()?` → 入队;注释与模块文档同步(append/reserve/flush 的「先 flush 旧段再滚动」句式)
- [x] 1.2 `Log::drain_pending()`(瞬持锁取队 → 无锁逐段 flush);`Log::flush` = drain + active;`flush_all` = drain + entries 遍历(`Arc::ptr_eq` 跳过已 drain 段);`close` = 先 drain(兜底)再 `Arc::try_unwrap` 关文件
- [x] 1.3 配置开关 `store.defer_seal_flush`(config.rs 字段+默认 true);Log 原子量 + `set_defer_seal_flush`;`roll_over` 分支(false = 同步旧行为);`finish_di` 注入;**全仓 17 处 `StoreConfig` 字面量补字段**

## 2. 门店面(blob.rs)

- [x] 2.1 `BlobStore::flush()` 分相:锁外 `drain_pending`(重头,与写并发)→ 锁内 `flush_inner`;成/败接 `record_success`/`record_io_error`(D4 链路)
- [x] 2.2 `flush_inner` 首行增量 `drain_pending`()(D2 顺序不变量:log 全量持久先于 index flush)

## 3. 测试(钉死三个风险点)

- [x] 3.1 log.rs 字节级:滚转后、flush 前,封存段文件 header `data_end`(offset 16,LE)保持建段初值;`flush()` 后追平;`close` 兜底(不显式 flush 直接 close → 重开可读)
- [x] 3.2 既有套面全绿:`cargo test -p antfs-blob`(crash_consistency 属性面自动覆盖「flush 是边界」)与 `cargo test --workspace`

## 4. 基准与文档

- [x] 4.1 VM 复测 A/B:**64 MiB 段主赛场 p99 17.5→0.92 ms(×19)、吞吐 +19%**;4 KiB×50k 无回归(172k ops/s);4 GiB 口径在 colima 被虚拟盘容量毛刺遮蔽(async/sync max 同 ~1.2s),标「真机复验」;判别链揪出并修复 `LogSegment::create` 次生病灶(preallocate→sync_all 修序为 header 先 sync,fallocate 后置不强落盘;`Log::open` 对 0 字节半建段残骸宽容跳过);bench-blob 增 `--sync-seal-flush` 对照口;全部入 BASELINE.md「修复后决算」
- [x] 4.2 store.md §3 写路径句式更新(滚转不再同步 flush)+ §10.1 配置表补 `store.defer_seal_flush` 行;DEVLOG 步骤 45

## 5. 收口

- [x] 5.1 `cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings` + workspace 测试全绿;推送后 CI 绿

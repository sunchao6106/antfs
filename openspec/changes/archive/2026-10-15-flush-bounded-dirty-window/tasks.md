# Tasks

## 1. 引擎计数(机制)

- [x] 1.1 `Log` 增 `Arc<AtomicU64>` 脏计数(create/open 双构造);`Log::append` 成功后
     递增;`Reserved` 携带克隆句柄,`commit()` 成功后递增
- [x] 1.2 `Log::flush` / `Log::flush_all` 末尾**尝试即复位**;`drain_pending`/`close`
     不复位(非全量点);`Log::dirty_bytes()` + `BlobStore::dirty_bytes()` 透出
- [x] 1.3 log.rs 单测:两发布径计数(未 commit 不计)、flush 复位、滚转后 flush 复位、
     flush_all 复位

## 2. worker 混合 tick(策略)

- [x] 2.1 抽取 `flush_replicas`(提交/汇合/坏盘跳过/指标口径与全量轮一致);
     `flush_all` 变全集包装(原签名不变,既有测试不动)
- [x] 2.2 `flush_dirty(pool, replicas, health, metrics, threshold)`:只提交
     `dirty_bytes() ≥ threshold` 的副本;`spawn_flush_loop`(100ms tick;全量 =
     interval 到点;早醒 = 阈值非 0 且有超阈副本;首拍全量保持);删除通用
     `spawn_loop`
- [x] 2.3 `StoreServerConfig.flush_dirty_threshold_bytes`(**默认 0 = 关** —— 初拟
     256 MiB,同日真机反例证据后反转,见 design「真机证据」;取值不设额外 Validate,
     range 记入 cluster.md 表);`store_server_flow` 误写已清:正式指标
     `store_server_flush_early_wakes_total` 注册
- [x] 2.4 worker 单测:早醒只动达阈副本、阈值 0 空手、flush 尝试即复位;既有
     flush_all 用例原样绿

## 3. 基准与证据

- [x] 3.1 bench-blob 增 `--dirty-threshold-mib`(自建 flush 线径同构混合 tick)
- [x] 3.2 真机 A/B:**5000-op 旧口径失效**(相位 < flush 间隔,病理不触发,弃),
     改 20 GB / 4 线程口径:周期臂 max 1.14-2.07 s(病理如实复现);**水位 256 Markdown
     MiB 臂吞吐 −60%、max 反至 1.65-5.51 s** → 证据反转,默认改 0(关);BASELINE
     P2 行已反转记档(非销账 —— 结论是反向的)

## 4. 文档与收口

- [x] 4.1 cluster.md 配置表 + 指标表插行(默认 0 与证据反例理由);BASELINE P2 行
     反转记档;DEVLOG 58;design 档含机制/溢出预算/失败语义/真机证据节
- [x] 4.2 fmt/clippy + workspace + e2e 回归全绿 + push CI 绿 + 归档
     `flush-bounded-dirty-window`(新 capability:store-flush-window,ADDED 要求)

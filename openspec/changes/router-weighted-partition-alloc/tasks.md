# Tasks

- [x] proto:`PartitionEntry` 新增 `uint32 write_weight = 10;`(千分比 `0..=1000`,0 = 未下发/无意见 → 客户端按均权)
- [x] master:`capacity_weight_permille(cap, used) -> u32` 纯函数(`u128` 内部乘防溢出;下限 1;`cap == 0` → 1000)
      + 常量 `WEIGHT_PERMILLE_MAX = 1000`
- [x] master:两处共用同一函数 —— ① `pick_writable_partition`(`partition_id` 缺省时加权随机,口径由"字节余量"统一为千分比);
      ② `list_partitions` 下发 `PartitionEntry.write_weight`
- [x] master 单测:`capacity_weight_is_remaining_ratio_with_floor_one`(空=1000 / 半满=500 / 满或超卖=1 /
      只剩 1 字节=1 / `cap=0`=1000 / u64 上界不溢出)
- [x] router:`IdCache` 的 `cursor` 换成 `credit: HashMap<PartitionId, i64>`,`take_weighted()` = 平滑加权轮转
      (SWRR:每拍各加 `weight` 信用、取最大者、减在册权重总和)
- [x] router:`segment_shares(pool, weights)` —— `pool = id_segment_size × 可写分区数`(总预留与改动前不变),
      `share_i = max(1, pool × w_i / Σw)`,余数按「小数部分降序,pid 升序」;极端偏斜不产生 0 长段
- [x] router:`sync_segments()` 按新份额**收敛已有段的段尾** `end = min(end, next + share)`(余号成空洞,D3 无害;
      收敛到没号的段丢掉),并把 `credit` 只留给手上还有号段的分区
- [x] router:`list_writable()` 返回 `Vec<PartitionWeight>`,`write_weight.max(1)` 在此归一(0/未下发 = 均权);
      新增只读方法 `Router::write_weights() -> Vec<(PartitionId, u32)>`
- [x] router 单测(+8,合计 14):均权逐位等价、20 个号按 1000:500:500 精确 10:5:5、权重 0 = 均权、单号段边界、
      段空跳过、`order` 收缩不 panic、`segment_shares`(含 `128 → [85, 43]`、极端偏斜 `2048 → [2046, 2]` 不产生 0 长段)、
      段尾收敛(`trim_segment`:只减不增;无号可发时报 false 让调用方弃段)
- [x] e2e(合计 8):新增 `partition_write_weight_steers_allocation` —— 软状态直注(不依赖心跳),
      p1 报用掉一半容量 → `ListPartitions` 里 `p1.write_weight == 500` / `p0 == 1000`,
      256 个号实际 p0=170 : p1=86;把占用对调后**翻转为 86:170**,并断言 client 侧 `write_weights()` 快照跟随
- [x] benchmark/router:分配分布报告新增「权重 → 期望 %」列(经 `write_weights()`),与**实际分布**并列打印
- [x] 文档:`router.md`(§2 写路径 + 流程片段 + §5 配置表/表下"请求哪个 partition" + §6 表后"权重是否生效怎么观测"
      + §7 差异与待办三条)、`cluster.md`(§7.4 `write_weight` 语义 + §7.5 第 ④ 条写路径时序)、
      `frontend.md`(内嵌配置表 `frontend.router.id_segment_size`)、`DEVLOG.md`(步骤 29)
- [x] 验证:`cargo test -p antfs-router`(单测 14 + e2e 8)、`cargo test -p antfs-master`、`cargo test --workspace` 全绿;
      `cargo clippy --workspace --all-targets` 通过,本轮触及的 crate 无新增告警
- [ ] 待办(不在本 change):巨大分区数下 SWRR 的 O(n) → O(log n) 结构;admin 手动权重入口 → master 同一字段;
      507 / silenced 驱动的本地降权

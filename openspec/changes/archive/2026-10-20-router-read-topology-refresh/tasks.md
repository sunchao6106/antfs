# Tasks: router-read-topology-refresh

- [x] 1. `TopoEntry` 化 + `replicas_for` TTL 重拉(失败保旧)+ 填充点带时戳
- [x] 2. `get` 全败驱逐 + Lookup 一次 + 新表不同则重试一次;NotFound 纯 miss 不驱逐
- [x] 3. metrics `router_topology_refreshes_total{reason}`(§7 表同步)
- [x] 4. lib 单测(TTL 决策/驱逐分类)+ e2e(ttl 与 evict 两 reason 的 metric 断言)
- [x] 5. spec delta + router.md §7 勾销/监控行 + DEVLOG 85 + 全绿 + archive + 单 commit + CI

## 落地记录(已回填)

- router **3 套 40**:lib 23(新增 TopoEntry TTL 边界单测:0=关 / ±边界 / 时钟快进);
  e2e 17(新增 `topology_refresh_counts_ttl_and_evict_reasons`:短旋钮轮询到
  `reason="ttl"` 实增;dead 分区全员失败 → `reason="evict"` 恰 +1 且读仍上抛)。
- **workspace 77 套 781/0;clippy 0;fmt 净**。
- 实现注记:std 读锁只克隆条目(不持锁 await,clippy 守);驱逐后"重拉列表相同 → 不
  重试"(不自造重试风暴);`replicas_for` 的错误归一逐字保留旧语义(miss 路径零漂移)。

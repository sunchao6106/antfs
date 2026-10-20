# Tasks: router-id-prefetch

- [x] 1. `Segment.start` + `low_water_pid` 探针 + `extend_segment` 回填三态 + lib 单测
- [x] 2. `spawn_prefetch` 单飞(std 锁布防 + 字段 Arc 副本 + 失败计数)
- [x] 3. e2e:4 号极小段连续 32 号 + 预取零错误
- [x] 4. 监控行 + router.md §7 勾销 + DEVLOG 步 86 + validate + archive + CI

## 落地记录(已回填)

- router **3 套 41**:lib 24(新增 `low_water_and_extend_segment_semantics`:低水位边界矩阵 +
  回填三态);e2e 18(新增 `id_prefetch_keeps_allocation_fluent_on_tiny_segments`)。
- **workspace 77 套 783/0;clippy 0;fmt 净**。
- 实现注记:master 字段 Arc 化(任务持副本);段 `start` 记忆使 ¼ 判定无猜;竞态落空段保守
  丢弃(proposal 记档:有效空洞口径)。

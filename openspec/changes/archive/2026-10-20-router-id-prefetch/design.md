# Design: router-id-prefetch

## D1 触发与单飞

- 触发点唯一:`allocate_one_inner` 命中取号后(`low_water_pid()` 探针,**零成本**不加锁);
- 低水位:剩余 `remain` 满足 `remain ≤ 2` 或 `remain × 4 < 段长`(`end − start` 严格小于
  —— 恰 ¼ 不触发,保守);段长需要 `start` 字段(本 change 引入,`segment_from_response`
  统一填);
- 单飞:`prefetching: Arc<std::sync::Mutex<HashSet<PartitionId>>>`:同区一拍一次;任务尾自清。
  std 锁(临界区两条纳秒操作);
- 预取量 = `id_segment_size` 段容(与同步补段同口径)。

## D2 回填三态(保守优先)

| 形态 | 行为 |
|---|---|
| 现段 `end == 预段 next` | **无缝续**:`cur.end = seg.end`(master 连号恰对上,零空洞) |
| 对不上 | 丢弃(不插不续):并发期间段被耗尽/摘除的竞态 residue,保守不造额外空洞(预段号位自然成无效空洞,可控) |
| 无段 | 直接插入(耗尽后新段形态) |

## D3 并发与正确性

- 预取任务写回 `ids` 拿 tokio 锁(与发号路径同锁)—— 串行化短临界区;
- `sync_segments` 不变:耗尽路径自己补(正确性主权);预取只是让耗尽路径**更少到达**;
- 失败计数 `router_id_prefetch_errors_total`;成功不打点(理由 § proposal);
- 预取响应经 `segment_from_response` 复用 ⇒ topology 条目顺带带时戳刷新(读 TTL 的免费收益)。

## D4 测试

- lib 单测:低水位边界(3/4 不触发、≤2 触发、<¼ 触发、恰 ¼ 不触发);回填三态(续/丢弃/直插);
- e2e:`id_segment_size=4` 极小段连续 32 号(段边界高频出现,a冷暖两路径任一保证流畅)、
  号互不重复、`prefetch_errors == 0`。

## Risks / Rollback

- 无正确性面风险(增强性,失败静默);回滚 = revert 单 commit。

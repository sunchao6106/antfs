# Tasks

- [x] router:`IdCache` 改 `HashMap<PartitionId, Segment>` + `order`/`cursor`,`take_rr` 顺序轮转(取完即丢)
- [x] router:`sync_segments` = `ListPartitions(state=RW)` + 对每个缺段分区显式点名 `GetBlobIds`(并发;单分区失败只跳过)
- [x] router:`Segment` 语义改为 `next` 含 / `end` 不含(单号段 `count=1` 也能发)
- [x] router:`connect` 起后台刷新任务(`partition_refresh_secs`,0 = 关闭;持号段池 `Weak`,析构自退;保持轮转相位)
- [x] router 单测:`take_rr` 轮转 / 单号段 / 段空跳过与空缓存 / 分区从 `order` 消失后游标回绕
- [x] e2e:两可写分区下 `allocate_batch(10)` 断言分区序列严格交替
- [x] e2e:后台刷新 —— 分区转 RO 后号段被丢掉、新分区被补段并参与发号(`background_refresh_tracks_writable_partitions`)
- [x] 配置/监控:`partition_refresh_secs` + `router_writable_partitions` + `router_partition_refresh_total{outcome}`,
      文档(`router.md` §2/§5/§6、`frontend.md`)同步
- [ ] 真集群压测:多盘集群下确认写入在各盘摊开(`benchmark/router --concurrency`),按结果调 `id_segment_size`

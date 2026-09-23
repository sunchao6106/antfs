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
- [x] benchmark/router:打印写成功的 blobId 在**各分区**上的分布(轮转生效的直接证据)
- [x] 真集群实测(devcluster:1 master + 3 store,`--partitions 8`,`benchmark/router --segment-size 64 --ops 2000 -c 16`):
      **分区级摊开成立** —— 每分区恰好 250 个(12.5%),跨 4 轮补段仍均等,0 错误;
      **盘级摊开取决于放置**:RF=1 时 8 个分区**全部落在同一个 store**(该 store 64MB、另两个 0MB);
      RF=3 时三个 store 各 64MB(每写扇出到 3 盘,本来就没有热点)
- [ ] **放置侧跟随(本次未做,盘级摊开的前提)**:`select_disks` 候选按余量降序、同余量时按 `node_id` 升序 tie-break,
      于是**同一批**建出来的分区会全挤在同一个盘(RF < 盘数时盘级 IOPS 摊不开)。需要"同余量轮转 / 按该盘已有分区数分摊"
      的 tie-break + 放置单测 + 更新 `cluster.md §7.3`

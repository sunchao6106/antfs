# Tasks

- [x] store 心跳上报 `ReplicaBeat.used_bytes`(blob 活数据)与真实 `lag_bytes`(游标线性化估算)
- [x] master 软状态接住 used_bytes;`ReplicaCurrent.used_bytes` 回填;只读访问器(每分区取副本最大值)
- [x] master 5 个新 gauge:`writable_used_bytes`/`writable_free_bytes`/`replicas_by_disk`/`disk_skew`/`maintenance`,reconciler 周期刷新 + 集成测试
- [x] `ListPartitionsRequest.state` 过滤(先过滤后分页;非法值 InvalidArgument)+ 测试
- [x] `MasterState.maintenance`/`pause_autogrow` + `SetMaintenance` RPC(密钥准入/非 leader 回地址)+ 兼容测试
- [x] reconciler 跳过点(gauge 之后)+ autogrow 子开关 + 心跳指令下发保持运行 + 集成测试(开维护不补副本、关维护收敛)
- [x] admin:state 透传(非法 400)、`POST /api/maintenance`、`/api/cluster` 与 `/api/metrics` 新字段、DTO 加 used_bytes + 测试
- [x] 前端:概览仪表盘改版(横幅/大卡/两列/水位榜/可写速览)、新「容量」tab(统计 + 曲线 + 增速 + RW 列表)、维护开关与徽标、大小分桶速率图、hash query 跨 tab 筛选
- [x] 文档:cluster.md §7.4/§7.7/§9/§11 同步(含 store 配置与指标 4 行)
- [x] 真集群验证:state 过滤带 used_bytes、maintenance 开关经 cluster 反映、浏览器逐页核对

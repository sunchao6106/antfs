## Why

admin-web 已有只读 dashboard + 建分区/下线/RBAC/双语,但有两块短板:

1. **指标不可达**:全项目打点早已注册进各进程的 `prometheus::Registry`,却没有任何 `/metrics` 暴露端点,`gather()` 仅测试用;admin 页面也无指标展示,只外链 Grafana。
2. **布局不可扩展**:SPA 全部内容(概览/运维/节点/分区)挤在一个长页面,节点/磁盘/分区一多就拥挤;分区仅固定 10 条/页,无筛选。
3. **运维操作不足**:欠副本修复只能等 reconcile 周期;副本无法手动迁移(均衡/换盘场景的基元)。

## What Changes

- **指标暴露(pull 模型,对齐 Ceph/SeaweedFS)**:
  - store 在现有 HTTP 数据面直出 `GET /metrics`(无鉴权,Prometheus 文本)。
  - master 新增 `GetMetrics` gRPC(master 无 HTTP 端口,自身指标经 gRPC 出口,任何节点可答)。
  - admin `GET /metrics`(RBAC 之外)= 自身 registry + master 文本聚合,Prometheus 抓 admin + 各 store;`GET /api/metrics`(RBAC 内)= master 指标 + admin 指标 + **直连并发抓各 store /metrics**(2s 超时,失败 `scrape_ok=false`)的聚合 JSON,供 UI。admin 保持无状态,不做 push 时序缓存。
  - `common/metric` 新增 Prometheus 文本解析器 `parse()`。
- **master 新运维 RPC**:
  - `RepairPartition`:手动立即补副本,复用 reconciler 的 `try_add_replica` 选盘,不等 reconcile 周期。
  - `MoveReplica`:副本迁移(节点级,盘由 select_disks 选)。状态机新增 `moving: BTreeSet<(PartitionId, NodeId)>`(`#[serde(default)]`,JSON 序列化前后兼容);apply = AddReplica + 标记迁出源;reconciler 新增 `plan_move`,**新副本健康后才移除源**(add-before-remove,守 §3.1)。`ListPartitions` 回填 `moving_out_nodes`。
  - 新指标:`master_repairs_triggered_total`、`master_replicas_moved_total`、`admin_store_scrape_errors_total`。
- **admin 新 API**:`GET /api/partitions/{id}`(list+find 全量视图,非 LookupPartition——后者缺 RF/健康/moving 字段)、`POST /api/partitions/{id}/repair`、`POST /api/partitions/{id}/move`(master ok=false 时仍 200 + `{ok:false,error}`,仅传输错误才 502)。
- **前端 SPA 重构**:
  - 拆分 `App.tsx` 为 tab 结构(概览/节点/分区/指标/运维,hash 同步,viewer 隐藏运维 tab)。
  - 节点页:搜索 + 仅看离线 + 磁盘聚合行(盘数/用量进度条/最差健康),点击展开明细;sticky 表头。
  - 分区页:每页 10/20/50/100、状态筛选 + 仅欠副本(开启时全量拉取前端分页)、复选批量 RW/RO/Seal、行内修复/迁移(内联面板选源/目标)、迁移中徽标。
  - 指标页:uPlot(新依赖,singlefile 内联)+ 5s 轮询 `/api/metrics` + 360 点滚动窗口;KPI 卡片 + 欠副本/心跳速率/transitions+rereplications/各 store put/get 速率/副本数图表。
  - 运维页:建分区、下线(拒绝时错误+受影响分区渲染在面板内,旁置强制按钮)、分区查找。

## Capabilities

### New Capabilities
- `admin-metrics-ops`:全组件 `/metrics` 暴露与 admin 聚合、RepairPartition/MoveReplica 运维原语、SPA tab 化布局与指标图表页。

### Modified Capabilities
<!-- master 状态机新增 moving 集合(serde default,前后兼容);admin `/metrics` 在 RBAC 之外(只读指标,对齐 Prometheus 抓取惯例),`/api/*` 门控不变。 -->

## Impact

- **crate**:`common/metric`(parse)、`store`(/metrics)、`master`(proto×3 RPC + 状态机 moving + reconcile plan_move + 指标)、`admin`(聚合 + 新路由 + SPA 重构)、`devcluster`(接线)。
- **契约变化**:`/metrics` 无鉴权(只读指标);`PartitionDto` 加 `moving_out_nodes`(serde default)。
- **被否决方案**:store push 指标给 admin(admin 变有状态时序缓存,偏离 pull 惯例);master 开独立 metrics 端口(多开端口,gRPC 出口已够);uPlot 以外的图表库(更重)。

## Non-goals

- 不做盘级指定迁移(仅节点级,盘由 placement 选);不做自动 rebalance 策略(manual move 是其基元)。
- 不做 admin 侧时序持久化/告警(滚动窗口仅内存,告警归 Prometheus/Grafana)。
- 不做 blob 引擎 `antfs_blob*` 指标的 UI 展示(经 /metrics 文本可达,UI 只用 store_server_* 聚合)。

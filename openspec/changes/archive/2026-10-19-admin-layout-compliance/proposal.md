# Proposal: admin-layout-compliance — 布局反查 + 放置合规审计(admin web「布局」页)

## Why

运维侧两个已核实的小缺口(HANDOFF「GROUND TRUTH」纠错后的真残余 #1):

1. **布局只有正查**:`GET /api/partitions` 按分区列副本(副本[节点/盘/状态]),没有"**这台机器上
   承载哪些副本**"的反查视图 —— 排水/搬机前盘点全靠翻页人肉拼。
2. **放置合规无独立审计**:反亲和(同分区副本不同节点)只在 placement 创建时强制;修复流
   (手工 `MoveReplica`/`RepairPartition` 的落点)或任何状态机异常造成的事后违例,**没有任何
   面会显示**。"查一遍全表、把违例亮出来"是只读的十几行计算,缺的只是一个入口。

## What Changes

- **admin `GET /api/layout`**(viewer 可读,点击式):server 侧经既有 master API(`nodes()` +
   分页循环拉全量 `partitions()`)join 出布局报表:
  - **反查清单**:每节点(node_id/addr/up)承载的副本条目(partition/disk/target_state,按 pid 升序) +
    计数;零副本节点也列出(空是有效布局事实);
  - **合规扫描**(纯函数 `crates/admin/src/layout.rs`,同 scrub.rs 的"纯函数可单测"家风):
    `same_node`(同分区 ≥2 副本落同节点 —— 反亲和违例)、`unknown_node`(副本引用未注册节点,
    join 完整性)、`unknown_disk`(盘不在该节点注册盘表);清单空 =`compliant=true`。
    慢盘/下线/BOOTSTRAP 是**状态事实**不是违规(节点行已可见;把预期状态计成红灯会训练运维
    无视红灯);
  - **只读不自治**:亮违例不做修复(处置 = 运维经既有 `move`/`repair`/重 eval 弧)。
- **UI 新「布局」tab**(viewer 可见):汇总卡(节点/分区/副本数 + 合规徽标)+ 违规表 +
  逐节点副本清单(行展开);i18n en/zh 双份;5s 轮询。
- FakeMaster 测试桩增加 `custom_partitions` 覆写(旧用例默认行为不变)。

## Impact

- `crates/admin/src/layout.rs`(新,纯函数 + 单测)、`server.rs`(路由 + join + 序列化 + 测试)、
  `crates/admin/ui/src/{api.ts, App.tsx, i18n.ts, tabs/LayoutTab.tsx}` + `ui/dist` 重构建提交。
- master/proto/store/迁移:**零改动**;无新配置、无新指标(`admin_api_requests_total` 已覆盖请求口径)。
- 不触写路径/磁盘格式(纯只读报表)。
- 文档:cluster.md §11 契约行 + DEVLOG 新步。

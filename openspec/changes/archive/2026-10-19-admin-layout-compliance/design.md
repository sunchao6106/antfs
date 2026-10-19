# Design: admin-layout-compliance

## Context

运维存货 #1(2026-10-19 验码核准):`GET /api/partitions` 已有正查(分区→副本);缺反查
(节点→副本)与独立的反亲和合规扫描。本 change 只做 admin 层,不动 master/store。

## Goals / Non-Goals

**Goals**
1. "这台机器承载哪些副本"一页看清(盘点/排水前的眼镜);
2. 全表放置审计:违例亮灯,清白给"合规"结论;
3. 点击式只读、零常驻成本(与 `GET /api/partitions` 同哲学)。

**Non-Goals**
- 不做机架/DC 级反亲和审计(placement 本身未强制 rack 约束,审计一个不存在的约束只会
  常年全红 —— 先如实,后扩展);
- 不把 down/BOOTSTRAP/DEGRADED 计成违规(状态事实,另见表格);
- 不自动修复、不落任何写路径;
- 不做趋势/留存(要历史走 Prometheus,不新开 admin 存储)。

## Decisions

### D1 server 侧 join,复用既有 master API,零 proto 改动

`nodes()` 一拍;`partitions("", offset, 500)` 循环直到齐 `total`(页空提前停,防分区被删时
死循环)。数据全在 master 的 Raft 表里,admin 只做转译 —— 不新增 master 端点、不进 proto、
不改 store。分页循环 admin 侧完成,与前端翻页(正查)互不干扰。

### D2 违规判定集合 = placement 真实强制的约束 + join 完整性

placement(`select_disks`/`try_add_replica`)强制的核心不变量:同一分区的副本落在**互异的
节点**上。审计集:
- `same_node`:同分区 ≥2 副本同节点 —— 不变量违例,真红灯;
- `unknown_node`:副本节点不在注册表 —— 理论不可能(Raft 单表),计它是对 join 完整性的
  诚实防御,出现即表状态损坏;
- `unknown_disk`:副本的 disk_id 不在该节点注册盘表 —— 注册面与放置面漂移。

刻意**不**计:慢盘/下线节点上的副本、BOOTSTRAP、`OFFLINE` 残留(状态事实,各自已有可见面;
把"进行中的正常过程"计成违规会训练运维无视红灯)。

### D3 纯函数计算核 + 手工 JSON(同 scrub.rs 家风)

`layout::compute(&[NodeDto], &[PartitionDto]) -> LayoutReport` 无 IO/时钟/锁,单测直打
(scrub.rs 13 单测同款)。server 只做 join、序列化、`generated_at_ms`。violation 的 `detail`
是英文兜底串(UI 双语按 `kind` 取 i18n 文案渲染,结构字段才是主显示)。

### D4 点击式,无缓存无配置无新指标

请求即计算(整表 join,千分区级毫秒;真要更大表也是秒级只读)。与 `GET /api/scrub`
(环形内存)不同:布局无需历史,零状态。`admin_api_requests_total`/`ops` 家族已覆盖请求
观测 —— 不为新端点单开指标(与 partitions/nodes 端点同等公民)。

### D5 权限:viewer 可读

纯只读报表,与 `/api/partitions`、`/api/balance` 同级(viewer 可读;无写操作可门)。

## Risks / Trade-offs

- **大表分页延迟**:每页 500 的串行循环,极端大表 = 多拍 master RPC(秒级)。点击式可接受;
  真急了再谈 master 端聚合。
- **审计结果与 placement 决策时点的差异**:节点表/分区表分两次取,`nodes()` 后 `partitions()`
  期间发生的变更可能造成瞬时假 `unknown_node` —— Raft 单表下变更同源,概率极低且下一拍
  自愈(admin 也可稍后复核)。如实文档化,不做快照一致性工程。
- **决策缺失的 rack 约束**:文档明记"非 scope"(见 Non-Goals),将来 rack 进 placement 后
  审计集同步扩展(D2 的集合是"约束跟随 enforcement"原则)。

## Migration Plan

无迁移:一个 GET 端点 + 一个 UI tab,增量并入;旧部署零行为差异。

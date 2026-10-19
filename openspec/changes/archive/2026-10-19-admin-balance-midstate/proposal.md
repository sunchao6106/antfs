# Proposal: admin-balance-midstate — 均衡搬迁中间态可见性

## Why
= 实读收敛链后的裁决(交接红线"动 master 前先实读,别猜"已履行):

`MoveReplica` 落位即记 `moving` + `move_targets`(目标已补、源未摘);清账只有一条路 ——
reconciler **无条件**跑的 `plan_move` 在"继任者 ACTIVE + 健康副本 ≥ RF"时 `RemoveReplica` 摘源
(与是否有均衡计划无关 ⇒ cancel/换计划后自然收敛,语义无洞)。**真正的洞是可见性**:

1. 继任者永不 ACTIVE(追平失败/目标节点长眠)⇒ `moving` 条目 + 多余副本**无限悬挂**;
2. `FailBalanceMove` 只记在**计划**的 `failed` 上,取消/换计划后随旧计划消失,而 `moving` 还在;
3. **重新 eval 清不掉这些**——planner 刻意跳过 `moving` 中的分区;
4. admin 的 `MoveReplica`**手动迁移**也走同一记账(计划外大户)。

⇒ 运维没有镜头看"哪些分区正卡在搬一半"。本 change 只补镜头,不加语义杠杆(清 `moving`
而不摘多余副本会留 RF 膨胀 + 永无收敛器,故不做)。

## What Changes
- **master**(`GetBalanceStateResponse` 增量加 `moving = 4`,旧读者忽略):
  纯函数从 `moving` + `move_targets` + 当前计划派生逐条视图
  (节,`BTreeSet` 序确定):`partition_id` / `from_node_id` / `to_node_id`
  (记账缺失防御性 0)/ **status**:
  - `failed` —— 当前计划(任意 status)的 `failed` 记着这条(带 `error` 原文);
  - `in_flight` —— 当前 `RUNNING` 计划认领且未判败(正常推进);
  - `unplanned` —— 无计划认领:**手动迁移**、或计划取消/替换/DONE 后的遗留
    (≠ 异常态;自然收敛照常跑)。
  不新增指标/配置(纯读派生);plan_view 语义不变。
- **admin**:`/api/balance` 响应随之带 `moving`(viewer 可读,零新端点);
  BalanceTab 新「搬迁中间态」小节(表:分区/源/继任/状态徽标/错误)+ hint 道出
  **真话**:继任副本追平后自动摘源;failed/unplanned 长期不动 → 查**目标节点**健康,
  重 eval 不会清(它跳过 moving 中的分区)。
- **零语义改动**:不动 `FailBalanceMove`/`plan_move`/apply 任何收敛逻辑;
  均"看",不"做"。

## Impact
- `crates/master/proto/master.proto`(+message `BalanceMovingEntry`,response +field)、
  `crates/master/src/balance.rs`(纯函数 `moving_entries` + 单测)、
  `crates/master/src/server.rs`(get_balance_state 两分支接线)、
  `crates/master/tests/balance.rs`(集成:手动 move → unplanned 可见 → 全员 ACTIVE
  → 重放 beats 收敛清零,证明"可见性 × 自然收敛"闭环);
- `crates/admin/src/{master_api.rs, server.rs}`(DTO + 映射 + FakeMaster 夹具 + 测试)、
  ui(api.ts / BalanceTab / i18n)+ dist 重构建;
- 文档:cluster.md §11 路由段语义 + DEVLOG 步 75。默认行为逐字节等价(纯增读路径)。

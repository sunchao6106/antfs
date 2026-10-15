# p1-scheduler-lock —— P1 锁定方案 A:master 决策 + store 执行(不设独立 scheduler 角色)

## Why

P1(后台重活的执行宿主:再复制编排 / 跨节点再平衡 / EC 转换与修复)自 cluster.md §4 起处于
parked 状态,是 B2(EC 冷层,D5 已锁目标)实现的前置决策——EC 转换需要明确的调度与执行归属
才能开工。2026-10-15 经三方案对比后拍板 A。

## What Changes

- **锁定**:不设独立 scheduler 角色。执行宿主 = 各 store server 的后台 worker;master 只做
  决策与任务下发(选段/选节点/配额/验收)。compaction/flush 维持本地 worker(原澄清不变)。
- **cluster.md 五处落文**:§2 角色表(待定→已锁)、§4 整节改写为决议(依据/带宽口径/重开条件)、
  §5.4 归属行、§6 约束行(撤「P1 未锁」)、§6 TODO 行(改写为执行宿主落地项)。
- **重开条件入册**:多节点部署后实测出现后台任务×前台吞吐带宽争抢、且任务配额不够用 → 单独
  change 重开独立 scheduler 届时按真实需求重设计协议,不预建抽象接缝。

## Impact

- 文档:`docs/design/cluster.md`(§2/§4/§5.4/§6;零代码)。
- 流程:解锁 B2(EC 冷层)实现路径——EC 转换/修复按「master 下发 + 段 owner 节点 worker
  执行 + 校验分片走 HTTP 数据面散写」落地。

## Non-goals

- 不实现 EC 本体(单独 change);
- 不预建"可外部接管"的任务协议抽象(重开时按真实需求重设计);
- 不改 compaction/flush 的本地归属。

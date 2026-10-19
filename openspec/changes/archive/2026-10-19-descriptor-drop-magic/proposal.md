# Proposal: descriptor-drop-magic —— 描述符格式去 MAGIC,类型判定归位独占

## Why
= 所有者终裁(2026-10-19 两轮问询升级):D7 复议已证明"旧版本二进制"约束为空;进一步
裁决 **"不做新格式,ASTC magic 从代码与文档全部干掉,就当从来没有出现过"**。

可行性判据(实码推演,MAGIC 的**全部**现役职责逐条落地):

| MAGIC 现役职责 | 去掉后依赖它的路径 | 结论 |
|---|---|---|
| 嗅探路由(download/HEAD 无位行判定) | 两条嗅探分支 | **随之删除** —— 位独占路由 |
| 回填甄别漏斗(MAGIC 形似才进 parse) | store 回填 sweep | **随之删除** —— 该子系统存在意义即给无位旧数据落位 |
| parse 内格式校验(置位行的纵深) | 置位行装配路径 | **由 version/计数界/定长/尾随挑大梁**;判别虽弱 ~4 字节熵,但 parse 从此**只吃自己写的置位字节**,不再面对任何无位/不可信输入 ⇒ 弱化无从暴露 |

关键不变量:**凡"对无位字节做 parse/判型"的路径全部消失** ⇒ MAGIC 弱化后的判别力
永远用不到不可信数据。类型判定归 `FLAG_COMPOSED` 独占(写侧唯一置位者)。

## What Changes
- `antfs-descriptor`:`MAGIC` 常量与 parse 首检删除,格式 = `version(1) | chunk_count(u32)
  | session_len(u16) | session | × entries`(头 11B → 7B,`BadMagic` 错误型退役)。**单一
  格式,就当 v1 从未有过前缀**。
- frontend:download/HEAD 的嗅探分支删除(无位 ⇒ 便普通对象:GET 原字节、HEAD 不读
  数据体);`composed_flag_only` 旋钮与誓约全文删除(没有可翻的东西 —— 行为静态
  flag-only)。有位行置信路径语义逐字不变(超限/坏描述符响亮 502)。
- store/blob:**回填子系统整建制删除**(`POST /admin/backfill-composed`、sweep/报告/
  单飞闸,引擎 `mark_composed` / `for_each_entry_winner` 及其单测)—— 其存在意义
  (给无位旧数据落位)随所有者裁决归零。
- 已发布 spec 增量:`blob-composed-flag` 第 4 条 MODIFIED(嗅探句退役,flag-first
  **且 flag-only**)、`store-composed-backfill` 3 个 Requirement REMOVED(能力撤销)。
- **不动的无关 MAGIC**(盘点区分):log 记录帧 `REC_MAGIC`、S3 SSE 帧自家 magic——
  与描述符无关,分毫不动。

## Owner Rulings(本 change 的兼容性承载,记档为凭)
1. 仓库**无部署态旧二进制**(D7 复议裁定) ⇒ §3.4 前向半句(旧代码读新数据)为空集;
2. **无需要保留的落盘存量**(本裁定的直接后果)⇒ 若某个人盘上仍有带 `ASTC` 前缀的
   旧描述符,新 parse 将以"unsupported descriptor version 65"**响亮 502**(不静默、
   不误叠),按所有者裁决接受;混合版本滚动升级窗口不支持(同上前提)。
3. 历史档案(DEVLOG 各步、openspec 历版归档)按"改记不改史"保留轨迹;本 change 为
   最新裁决,语义以新为准。

## Impact
- `crates/common/descriptor`(格式与单测重写)、`crates/frontend/src/{config.rs,server.rs}`
  (旋钮+嗅探删除)+ `tests/rest.rs`(两用例重写、一用例删除)、`crates/store/src/server.rs`
  + `tests/backfill_composed.rs`(删除)+ Cargo 依赖(antfs-descriptor 自 store 撤出)、
  `crates/blob/src/{blob.rs,index/mod.rs,lib.rs}`(回填 API 族删除)+ 相关单测删除;
- 文档:frontend.md(§4 读序/§8 行删/§11 待办收口)、store.md(§12.14 整节删)、
  design/README.md 路线档行改写(终态)、DEVLOG 步 78;
- 零新配置、零新监控;`?composed=true` 数据面与 `metadata.composed` 全链保持不变。

# Proposal: admin-scrub-schedule — 周期性跨副本比对(admin 级巡检调度)

## Why

一致性巡检有两条腿:`store-scrub-background`(44df731)把**逐盘 bit-rot 自查**周期化了
(蜗牛爬深扫);但**跨副本逻辑比对**仍只有手动 `POST /api/partitions/{id}/scrub` —— 运维不点
就不比,"三份是否逻辑同一"的发现依旧靠人记得。原 scrub 批的已知边界就记着这条
("admin 级跨副本比对的周期化未做,另议")。本 change 补上这半边,巡检周期化收口。

## What Changes

- **admin 起定时循环**(仅 `admin.scrub_schedule_enabled`,默认 **false**):每 tick 对**一个**
  健康分区(`healthy_replicas ≥ 2`)跑一次**浅层**比对 —— 与手动端点同一条管线
  (post_scrub 抽出的共享核心 `run_scrub`,素材 fan-out + 逻辑态比对),**deep 恒否**
  (bit-rot 周期深扫是 store 侧的职责,两层各司其职);节拍 = `scrub_schedule_interval_secs`
  (默认 600s,蜗牛家风:全量覆盖周期 = 分区数 × 间隔,千分区 ≈ 7 天一轮)。
- **结果进既有面**:环形表新条目带 `origin`(`manual`/`scheduled`,响应与 `GET /api/scrub`
  增字段,增量兼容)+ 复用既有计数(`admin_scrub_runs_total` / `admin_scrub_divergences_total`);
  UI「运维 → 最近一致性巡检」列表对scheduled 条目显示「周期」徽标(hint 同步说明)。
- **`POST|GET /api/scrub-schedule-control`**(operator 写 / viewer 读):运行期掐停/恢复
  (不持久化、不动轮转游标);**手动 scrub 端点永不受它管**(应急检查常在)。新 gauge
  `admin_scrub_schedule_paused`。
- **只报警不修复**(裁决不变):发现分歧 → 指标/环形表/日志;处置走运维弧
  (手动再比 → repair/move)。并发写假不一致按既有 `partial` 口径如实呈现;
  周期轮盘天然"下轮重跑",连续多轮报同一处差异才值得人工。

## Impact

- `crates/admin/src/{config.rs(2 键+validate), scrub.rs(ScrubRun.origin/ScrubOrigin),
  server.rs(共享核心 run_scrub + 控制端点 + AppState.scrub_schedule_paused),
  scrub_schedule.rs(新:循环+候选过滤), main.rs(起腿), metrics.rs(gauge)}`;
  `crates/devcluster`(AppState 新字段);ui(api/types/OpsTab/i18n)+ dist 重构建。
- master/proto/store/迁移 **零改动**;默认关 ⇒ 现部署逐字节等价。
- 文档:cluster.md §11.1/§11.2 两行 + 契约段;DEVLOG 步 74(顺修步 73"76 套"笔误)。

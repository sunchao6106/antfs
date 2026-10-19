# Tasks — admin-scrub-schedule

## A. 核心与端点

- [x] `config.rs`:`scrub_schedule_enabled`(默认 false)/ `scrub_schedule_interval_secs`
      (默认 600,at_least ≥1)+ 单测 2(默认锁 + 0 拒)
- [x] `scrub.rs`:`ScrubOrigin`(Manual/Scheduled,as_str)+ `ScrubRun.origin`
- [x] `server.rs`:post_scrub 抽出共享核心 `run_scrub(st, pid, deep, origin)`
      (详情→fan-out→比对→计数→入环形表;网络/不存在错误上抛,单副本失败沉进 facts);
      响应与 `GET /api/scrub` 增 `origin` 字段;`AppState.scrub_schedule_paused`;
      `GET|POST /api/scrub-schedule-control`(operator 写/viewer 读,gauge 同步)
- [x] `scrub_schedule.rs`(新):循环(睡 interval → 暂停闸 → 候选列表 paged →
      round-robin 一区 → `run_scrub(shallow, Scheduled)`)+ 候选过滤单测;
      `metrics.rs` gauge `admin_scrub_schedule_paused`;`main.rs` 起腿(仅 enabled);
      devcluster AppState 新字段
- [x] 测试:真循环用例(1s tick + FakeMaster + 假 store:周期条目 origin=scheduled 经
      `/api/scrub` 可见 / 掐停两 tick 零增长 + gauge=1 / **暂停期手动比对照常且 origin=manual**
      / 恢复后继续产出)+ 控制端点角色门(viewer GET 200、POST 403、operator 回显)
      + 候选过滤纯逻辑单测

## B. UI

- [x] `api.ts`:`ScrubRun.origin`
- [x] `OpsTab.tsx`:scheduled 条目「周期」徽标(info pill);hint 文案双语扩句
      (注明条目来源与开关)
- [x] `npm run build` 重建 `ui/dist/index.html`

## C. 文档与收尾

- [x] cluster.md:§11.1 配置表 2 行 + §11.2 监控表 1 行(gauge)+ §11
      routes 段坐标(调度语义 + origin 字段 + 单 admin 实例假设)
- [x] DEVLOG 步 74(顺修步 73"76 套"笔误 → 75 套 748)
- [x] 门禁:workspace 绿 / clippy 0 / fmt 净 / `openspec validate admin-scrub-schedule`

## 落地记录(证据)

- admin lib **73/73**(新增 6:config 默认锁/0 拒 2、候选过滤 1、真循环 1、控制端点
  角色门 1;另有共享核心重构后全部旧 scrub 用例零回归);workspace **75 套 753/0**;
  clippy workspace 0;fmt 净;UI `npm run build` 过。
- 真面(devcluster 重建二进制):`POST /api/scrub-schedule-control` 操作回显
  {"paused":true}→GET 回显→viewer POST 403→resume 回显;手动 scrub `origin=manual`
  且 `GET /api/scrub` 条目带 origin;循环本身(默认关)由带假 store 的真路径集成用例
  覆盖(FakeMaster + 真 axum + 真 HTTP fan-out;首验翻案:stale 二进制的新路由会
  fallback 到 SPA,教训 = 真面验证必须重建 target)。
- 已知边界(如实):① 多 admin 实例会重复比对(无 leader 选举;只读幂等,单实例假设
  文档已记);② 单拍成本 = 1 paged 列表 + 1 详情 + N 副本素材(默认 600s 节拍可忽略);
  ③ 重启丢轮转位置(从列表头重转,幂等);④ 部署需自行保证 interval ≥ 想
  要的全量覆盖周期/分区数(文档给了公式)。

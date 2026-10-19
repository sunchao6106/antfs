# Design: admin-scrub-schedule

## Context

巡检两层周期化的后半件:store 层(逐盘 bit-rot 深扫)已由 `store-scrub-background` 落地;
admin 层(跨副本逻辑比对)仍靠手点。本 change 补定时腿,不新造机制 —— 全部复用手动
scrub 的既有管线/口径/观测面。

## Goals / Non-Goals

**Goals**
1. 跨副本比对从"运维记得才比"变成"轮盘必然转一圈";
2. 自动手动同管线同口径,结果落同一张表(分得清 origin);
3. 温和节拍 + 应急闸(掐自动不动手动)。

**Non-Goals**
- 不做 deep 周期化(bit-rot 归 store 侧蜗牛爬,两层职责不同:admin 逻辑态、store 字节态);
- 不自动修复、不自动重试加压;
- 不做跨 admin 实例的分布式去重(单 admin 实例假设,文档如实)。

## Decisions

### D1 共享核心,不复制管线

post_scrub 抽出 `run_scrub(st, pid, deep, origin)`:详情 → fan-out 素材 → 比对 → 计数 →
入环形表。手动端点与调度循环各带 origin 调它 —— 两来源的行为/口径**共享同一实现**,
不会漂移。错误边界:网络/不存在向上抛(手动路径照旧 404/502;调度路径 warn 跳过本拍)。

### D2 节拍:每 tick 一个分区,候选 = 健康分区

- 候选 `healthy_replicas ≥ 2`:欠副本分区正在被修复流照看,比它只会刷 inconclusive 噪声;
- 每 tick 一个(浅层比对也要 fan-out N 个副本的素材请求,一拍一区是上界拥塞控制);
- 覆盖周期 = 分区数 × interval(默认 600s)—— 慢即特性,与 store 侧蜗牛爬同哲学;
- deep 恒否:理由见 Non-Goals,两层各司其职是**设计**不是省事。

### D3 origin 字段与计数口径

- 环形表/响应加 `origin`(`manual`/`scheduled`),增量兼容;
- 计数**复用**既有 `admin_scrub_runs_total`/`admin_scrub_divergences_total`(它们度量的是
  "比对发生了多少/发现了多少",来源无关,告警口径不因加自动腿而变);分来源看 `GET /api/scrub`;
- 新 gauge 仅一个:`admin_scrub_schedule_paused`。

### D4 应急闸仿 store scrub-control

`/api/scrub-schedule-control`(POST operator / GET viewer),不持久化、不动游标(恢复续转)。
循环没开时置位无副作用 —— 表达运维意愿而非运行状态。手动 scrub 永不受管
(store 侧同款裁决:应急检查能力必须常在)。

### D5 假不一致的预期管理

并发写下比对窗口末端天然不同 → `partial`/差异如实进表。**不**加"两轮确认才入表"的抑制:
抑制会把真分歧也静默一拍,而周期轮盘本身就是"下轮重跑" —— 连续多轮报同一处
才是人工信号,由运维读表判断,不由代码预筛。

## Risks / Trade-offs

- **多 admin 实例会重复比对**(无 leader 选举):文档明记单实例假设;重复只读无害;
- **单拍成本**:一拍 = 1 次 master 列表(paged)+ 1 次详情 + N 副本素材(≤16MiB 窗口);
  默认 600s 节拍下可忽略;调小 interval 的代价由配置者自负(validate 只拦 0);
- **重启丢游标**:从列表头重转,比对幂等无害(家风同 store 侧)。

## Migration Plan

无迁移:纯增量(2 配置键、1 端点、1 gauge、响应加字段);默认关零行为差异。

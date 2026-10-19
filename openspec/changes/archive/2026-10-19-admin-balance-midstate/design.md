# Design — admin-balance-midstate

## 现状事实链(全部源码核实)

链路(谁写、谁清、谁判败):

| 事件 | 对 `moving` 的作用 |
|---|---|
| `MasterCommand::MoveReplica` apply | 记 `moving (pid,from)` + `move_targets[pid][from]=to`,placement 追加目标副本(**先补**) |
| reconciler `plan_move`(每 tick,**与计划无关**) | 继任者 ACTIVE ∧ 非源健康副本 ≥ RF → `RemoveReplica(pid,from)`:摘 placement、**清 `moving`**、进 `pending_drop` |
| 心跳 `ConfirmDrop` | 源不再上报 → 清 `pending_drop`/`move_targets`(**后删收尾**) |
| `FailBalanceMove` apply | **只**写计划的 `failed`;`moving`/`move_targets` 刻意不动(自然收敛刻意保留) |
| eval(planner) | `moving` 中的分区**整段跳过**(`partition_already_moving_is_skipped` 钉死) |

⇒ 设计裁决:
1. **不加"清 moving"杠杆**。若运维清了 `moving` 而多余副本未摘,分区永久 RF+1 且从此无
   收敛器碰它(比悬挂更糟)。演进到的正确链条是:目标副本追平 → 自动摘源。可见性 ≠ 操作权。
2. **status 三态而不是二态**:计划外条目不叫"孤儿"——手动 `MoveReplica`(admin 端点)是
   正常入口,只有"无计划认领"这一事实可下。
3. **`failed` 优先**于 `in_flight` 判定(DONE 计划的 failed 记录仍带着 error,让运营看见
   为什么判过败);计划被 cancel 后 error 随计划消失(设计如此,cancel 即弃.history),条目
   降级为 `unplanned` 照常收敛。
4. **`to_node_id = 0` 防御**:moving 有而 move_targets 无 ≈ 状态组合不可能
   (同一 apply 写入、无单独清理路径),如实透出 0 而不是跳过(可见性件宁可难看,不可说谎)。

## 派生算法(纯函数,`balance.rs`,`BTreeSet` 迭代序 = (pid, from) 确定排序)

```
for (pid, from) in moving:
    to   = move_targets[pid][from] ?? 0
    failed_err = 当前计划.failed 里找 (pid, from)        // 任意 plan status
    covered    = 当前计划.status == RUNNING
                 && 当前计划.moves 含 (pid, from)
    status = failed_err ? "failed"(error=failed_err)
            : covered    ? "in_flight"
            :              "unplanned"
```

## 契约

- proto(gRPC,增量字段,§3.4 精神——新旧前后兼容):
  `BalanceStateResponse.moving = 4 (repeated BalanceMovingEntry)`;
  `BalanceMovingEntry{partition_id=1, from_node_id=2, to_node_id=3, status=4, error=5}`,
  status ∈ {in_flight, failed, unplanned}。**无计划(present=false)也可有 moving 条目**
  ——正是 cancel 后遗留/手动迁移的镜头。
- admin:`/api/balance` 响应随 DTO 带 `moving`(viewer 可读不变);UI 表格列
  分区 / 源节点 / 继任节点 / 状态徽标(failed=红、unplanned=黄、in_flight=蓝)/ error;
  hint 一句话道出真话(自动收敛条件 + 重 eval 不清 + 卡滞查目标节点)。
- 不新增配置项、监控项、端点;`GetBalanceState` 本就任何节点可答(读 hard state)。

## 测试策略

- 单测(balance.rs 纯函数):空集 / RUNNING 认领 in_flight / 判败带 error /
  无计划 unplanned / 计划换目标后旧条目 unplanned / DONE 计划的 failed 条目保留 error /
  move_targets 缺失防御 0 / 排序确定。
- 集成(master tests/balance.rs,真 3 节点 + 真 gRPC):注册造偏 → 手动
  `MoveReplica` → `GetBalanceState.moving` 出现 unplanned 条目(to 正确)→ 心跳全员
  ACTIVE(带超时轮询)→ 摘源收敛 → 条目清零。顺带验 `pending_drop` 不出现在 moving 视图
  (收敛走完即无痕)。
- admin:JSON 形状测试 + FakeMaster 夹具;UI tsc 构建即验。

## 风险与不做

- 悬挂条目无自动清除(并发写卡 BOOTSTRAP 等)是**既有语义**,本批不引操作面;演进轨道
  (换落点重搬需要先摘多余副本的运维弧)记在 cluster.md 残余风险,不写代码。
- `pending_drop` 镜头(源已摘、盘未确认删)不做:它有自愈引擎(DROPPED 每拍重发),
  停滞主因"节点 down"在节点页已可见;此处扩张 =  scope 膨胀。

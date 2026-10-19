# Tasks — admin-balance-midstate

## A. master 派生与暴露

- [x] `master.proto`:`BalanceMovingEntry` 消息 + `BalanceStateResponse.moving = 4`
      (注释写清三态语义与"无计划也可有条目")
- [x] `balance.rs`:纯函数 `moving_entries(moving, move_targets, plan)` →
      `Vec<BalanceMovingEntry>`(failed 优先/RUNNING 认领 in_flight/其余 unplanned;
      to 缺失防御 0)
- [x] `server.rs::get_balance_state`:两分支(present / 无计划)都接 `moving_entries`
      ——**无计划也必须有条目**(手动迁移/取消遗留的镜头)
- [x] 单测:空 / in_flight / failed+error / unplanned(无计划)/ 计划换血后旧条目
      unplanned / DONE 保留 failed error / 防御 0 / 确定排序

## B. master 集成

- [x] `tests/balance.rs`:手动 `MoveReplica` → `GetBalanceState.moving` 出 unplanned
      条目(to 对)→ 全员 ACTIVE 心跳 → 摘源收敛(带超时轮询)→ moving 清零
      (顺带断言收敛后 `pending_drop`/`move_targets` 也已收尾)

## C. admin 与 UI

- [x] `master_api.rs`:`BalanceMovingDto` + `BalanceStateDto.moving` + 真客户端映射
      (`moving_dto` 对齐 `plan_dto` 风格)
- [x] `server.rs` 测试:FakeMaster 夹具带一条 unplanned;`GET /api/balance` 形状断言 +
      既有平衡端点测试更新(字段增量)
- [x] ui:`api.ts` 类型;`BalanceTab` 新「搬迁中间态」小节(非空才渲染;计划无/有都渲染):
      表(分区/源/继任/徽标/错误)+ hint(自动收敛 + 重 eval 不清 + 查目标节点);
      i18n en/zh;`npm run build` 重建 dist

## D. 文档与门禁

- [x] `cluster.md` §11 路由段:balance 响应 `moving` 字段与三态语义 + hint 文案事实
      (plan_view 不变;无新配置/指标)
- [x] `DEVLOG.md` 步 75(含收敛链验读结论表)
- [x] 门禁:workspace 全绿 + clippy 0 + fmt 净 + `openspec validate` 过
      + devcluster 真面(`POST /api/partitions/{id}/move` → `GET /api/balance` 见
      unplanned 条目)

## 落地记录(已回填)

- master lib **116/116**(新单测 4)/ balance 集成 **5/5**(新增 1:手动 move → unplanned
  可见(present=false)→ 重 eval 出 PREVIEW 不认领 → 全员 ACTIVE 收敛 → 镜头清零 +
  `pending_drop` 同步收尾);admin lib **71/71**(形状断言扩展 + FakeMaster 夹具,
  未新增用例——字段属既有端点契约);workspace **75 套 758/0**;clippy 0;fmt 净;
  UI build 过(302.94 kB)。
- 真面(devcluster `--store-count 4`):手动 move 101→104 → `GET /api/balance` 出
  `{present:false, moving:[{pid:0,from:101,to:104,status:unplanned}]}`;约 3s 后自然收敛,
  镜头清零、placement 换血完成。
- 顺修:DEVLOG 步 74 的"admin 73/73"笔误(实 71/71,以 `cargo test -p antfs-admin`
  现场输出为准)。

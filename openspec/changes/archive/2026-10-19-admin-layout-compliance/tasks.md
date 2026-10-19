# Tasks — admin-layout-compliance

## A. 计算核 + 端点

- [x] `crates/admin/src/layout.rs`(纯函数):`ReplicaEntry`/`NodeInventory`/`Violation`/`LayoutReport`
      + `compute(nodes, partitions)`(反查聚合按 pid 升序;违规三 kind:`same_node`/`unknown_node`/`unknown_disk`;
      清白 = `compliant`)
- [x] server:分页拉全量(`partitions("", offset, 500)` 循环,页空/齐 total 即停)+ join +
      `GET /api/layout`(viewer 可读,手工 JSON,`generated_at_ms`)
- [x] 单测:反查聚合含零副本节点 / same_node / unknown_node / unknown_disk / 清白合规 / 状态事实不入违规
- [x] 端点测试(走 `app_with` oneshot):3 节点 3 分区反查数值 / 违规三 kind 与 compliant=false /
      viewer 200 / 多页拉全(700+ 分区跨页)/ master 不可达 502

## B. UI

- [x] `api.ts`:`LayoutReport` 族类型 + `api.layout()`
- [x] `LayoutTab.tsx`:汇总卡(节点/分区/副本数 + 合规徽标)+ 违规表(kind 走 i18n)+ 逐节点
      副本清单(行展开,partition/disk/target pill);5s 轮询;错误横幅
- [x] i18n en/zh 双份(含 `tab_layout`)
- [x] `App.tsx` 注册「布局」tab(viewer 可见,排「分区」之后)
- [x] `npm run build` 产出 `ui/dist/index.html`(singlefile)入库

## C. 文档与收尾

- [x] cluster.md §11 契约行(`GET /api/layout` 形状与三违规 kind;零新配置/指标注记)
- [x] DEVLOG 新步
- [x] 门禁:workspace test 绿 / clippy 0 / fmt 净 / `openspec validate admin-layout-compliance`

## 落地记录(证据)

- admin lib **66/66**(纯函数 5 新 + 端点 4 新);workspace 门禁见 DEVLOG 步 73;clippy 0;
  fmt 净;`openspec validate` 过。FakeMaster 增 `custom_partitions` 覆写,旧用例默认路径不动
  (全部旧 admin 测试零回归)。
- **真集群验证**(devcluster,3 节点 × 3 分区 RF=3):`GET /api/layout` → compliant=true、
  合计 3/9/3、逐节点 3/3/3(replicas 升序)、violations 空;坏 token 401;SPA 新 tab 可达。
- UI `npm run build` 过(tsc + vite singlefile,`ui/dist/index.html` 入库)。

已知边界(如实):① 审计集**有意只含 placement 真实强制的约束**(不含 rack —— enforcement
还没有它);② nodes 与 partitions 两拍的瞬时视图差异理论上可造一过性 `unknown_node`,
Raft 单表下极低且下一拍自愈;③ 大表分页串行循环,千分区毫秒级,点击式可接受;④
violation 的 `detail` 是英文兜底串,UI 主显示按 kind 走 i18n 双语。

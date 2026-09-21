# design — admin-web

承 cluster.md §7.6 Q6(admin web 形态已锁)、§7.4(master admin gRPC)。本文记落地决策。

## D-A. 架构:独立 crate,master gRPC 客户端 + axum HTTP + 内嵌 SPA

```
浏览器 ──HTTP/JSON──▶ admin(axum) ──gRPC──▶ master(Master service)
   静态 SPA(React/Vite,内嵌进 admin 二进制)          │
   指标图表 ──外链──▶ Grafana(读 master/store 的 /metrics)
```

- admin **不碰 store 数据面**、**不入 Raft**;纯 master gRPC 客户端 + 展示/操作层。对齐 CubeFS console(独立 web)/SeaweedFS `weed admin`。
- SPA 构建产物用 `rust-embed`(或 `include_dir`)内嵌进二进制,单文件部署;`/api/*` 之外的路径回退 SPA `index.html`(前端路由)。

## D-B. master 新增 `ListPartitions`(dashboard 必需)

现有 admin gRPC 无"列全部分区 + 副本实然态"。新增只读 RPC:

```proto
message PartitionEntry {
  uint64 partition_id = 1;
  string state = 2;                 // RW/RO/SEALED
  uint32 replication_factor = 3;
  repeated ReplicaLoc replicas = 4; // 复用:node_id/addr/disk_id/target_state/up
  repeated ReplicaCurrent currents = 5; // partition×node 的 current_state/lag(soft)
}
message ReplicaCurrent { uint64 node_id = 1; string current_state = 2; uint64 lag_bytes = 3; }
message ListPartitionsResponse { uint32 version = 1; repeated PartitionEntry partitions = 2; }
```

- 实现:读 `sm_data.state.partitions`(hard placement)联 `soft.replica_state(pid,node)`(current/lag),节点 addr 取 hard `nodes`,up 取 `soft.is_up`。纯只读,任意 master 节点可服务(follower 也能读本地状态机快照;若要强一致再走 leader)。
- 大规模注意:分区可达数十万;MVP 一次性返回。后续加分页(token/limit),先不做(YAGNI,先能用)。

## D-C. 鉴权:单一 operator token(Bearer)

- 所有 `/api/*` 校验 `Authorization: Bearer <admin.operator_token>`;缺/错 → 401。空 token = 关校验(仅本地开发,对齐 master `cluster_secret` 空=关的惯例)。
- MVP 不做多用户/RBAC/会话;单 token 足够运维准入(Q6 只要求 operator 鉴权)。写操作(建分区/下线)与读同一 token(MVP)。

## D-D. 前端 SPA(React + Vite)

- 页面:① 概览(term/leader/节点数/分区数/RW 分区数);② 节点表(id/dc/rack/addr/up + 各盘 容量/余量/健康);③ 分区表(id/state/RF + 每副本 node/target/current/up,发散/降级高亮);④ 运维(建 N 分区表单、选节点下线);⑤ Grafana 外链按钮。
- token 存 localStorage,注入 `Authorization`;401 弹登录框。
- 构建:`vite build` → `crates/admin/ui/dist` → `rust-embed` 嵌入。开发期 admin 可选 `--dev` 代理到 vite dev server(先不做,构建产物即可)。

## D-E. 测试策略

- 后端:`tower::oneshot` 测 axum 路由——① 无/错 token → 401;② 带 token 命中 handler;③ JSON DTO 序列化。master 交互用一个**内存 fake master gRPC**或直接对 handler 注入一个 `MasterApi` trait 的 mock(避免起真 master + raft)。
- master `ListPartitions`:单测联表逻辑(hard placement + soft current)。
- 前端:`vite build` 通过即可(MVP 不做前端单测)。
- 遵 §6:确定性、无 sleep。

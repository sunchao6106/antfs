# design — store-server-replication (S3a)

承 cluster.md §10(数据面读写 + quorum)、§9(store server 角色 N2/N3)、D4(数据异步 + 拉复制)。本文档记两处对已锁目标的**分期/细化决策**(CLAUDE.md:偏离先记档,不在代码就地另选)。

## D-A. N3 分期:S3a 逻辑拉复制先行,S3b 物理段拷贝后补

**已锁 N3** = bootstrap 拉全量走"混合"(封存段物理流式拷贝 + 活跃段尾 token 逻辑补,MTTR 优先,不推后)。

**本 change 的分期**:S3a 只实现**逻辑拉复制**(含 bootstrap 从 `token=Uninitialized` 的全量逻辑回放),S3b 再叠加物理段拷贝。

- **理由**:逻辑路径是**正确收敛的完备核**——它保证冗余最终一致(W=2 后第 3 副本补齐、失联再复制追平);物理段拷贝是**其上的 MTTR 性能优化**(大封存段免逐记录回放)。二者是"正确性 vs 加速",非"要么全有要么全无"。先落正确核、再叠加速,符合"简单优先 + 每期可独立验收"。
- **不是代码就地另选**:该分期在此记档 + cluster.md §9.1/§10 同步标注 S3a/S3b,N3 的最终形态(混合)不变。
- **S3b 待办**(不在本 change):封存段流式拷贝端点(整段字节 + 段头)、接收侧 rebuild_index、disk-aware 每盘并发(N4)。

## D-B. ReplicaMetadata 传输:走 store-server 现有 axum HTTP(细化 D2)

**D2** 名义把 step-1 ReplicaMetadata 归控制面 gRPC。**本 change 细化**:node↔node 的复制元数据走 store-server 已有的 **HTTP(axum)** 端点,master↔node 控制面仍 gRPC。

- **理由**:store-server 当前只跑 HTTP server(对 master 是 gRPC **客户端**)。为 ReplicaMetadata 单起一个 tonic gRPC server = 第二套 server + proto + 布线,收益低。step-2 拉 blob 本就 HTTP;把 step-1 也放同一 axum 上,单一 server 面,最简。
- **D2 精神不违**:D2 的核心是"blob 字节走 HTTP 流式、结构化控制走 gRPC 且 master↔node 契约稳定"。复制元数据是 node↔node 数据面邻域,JSON over HTTP 足够;master↔node 仍严格 gRPC。
- cluster.md §10 已把复制描述为"ReplicaMetadata 走 gRPC",本 change 在 §9.1/§10 标注:node↔node 复制元数据走 HTTP(S3a 决策)。

## 忠实 apply 语义(newest-wins + lifeVersion)

`apply_replicated(rec)` 对本地赢家 `cur` 判定:

| 本地 | peer rec | 动作 |
|---|---|---|
| 缺失 | 存活(PUT/UNDELETE/TTL 赢家,带 data)| 原样写整记录(peer flags/lv/exp/op_time + data),index 赢家 |
| 缺失 | 墓碑(DELETE,无 data)| 原样写墓碑(original_offset=self,无 data;`get` 非 include_deleted → 404)|
| 有 | `rec.life_version > cur.life_version` | 原样写(存活缺 data 时从本地 original PUT 取 data)|
| 有 | `== lv` 且 rec 引入新 flag(DELETE/TTL)| 应用该变更(墓碑 original_offset 指本地 PUT;TTL data 取本地)|
| 有 | `< lv` 或无新 flag | `Skipped`(幂等)|

- **只增不删**:apply 绝不删除本地已有更"新"的记录 → 部分失败可安全重试(§3.1 幂等)。
- **op_time 漂移**:mutation 在本地/peer 的 op_time 可能微差(各自 `now_ms`),仅影响 hard-delete 保留窗口,不影响可见性/收敛;缺失+存活 case 保留 peer op_time。
- **数据体来源**:仅"本地缺失 + peer 存活"需从 peer 拉 data;所有基于已有 key 的变更 data 均在本地(承既有 delete/undelete/update_ttl 的本地读法)。

## 收敛循环(per replica × per peer)

```
token = per_peer_token[peer]              // BOOTSTRAP 从 Uninitialized
loop:
  info = peer.metadata(token, max_bytes)  // step-1 HTTP
  for msg in info.entries:
      match local.stat(msg.key):
        缺失 & msg 存活 → data = peer.fetch_data(msg.key)   // step-2 HTTP(replication=1)
        else            → data = None
      local.apply_replicated(rec_from(msg, data))           // 幂等
  token = info.token
  if info.entries.is_empty(): break        // 追平
per_peer_token[peer] = token
```

- `PeerSource` trait 抽象 peer:生产 = HTTP adapter(reqwest);测试 = 进程内 `BlobStore` 包装 → 收敛/apply 逻辑确定性单测,免 socket、免 sleep(§6)。
- peer 地址来源:master `LookupPartition(pid)` → `ReplicaLoc.addr`(= store-server 注册的 `http_addr`)+ `up` 过滤,排除自身 node_id。

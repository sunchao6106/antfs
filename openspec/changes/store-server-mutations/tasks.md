# tasks — store-server-mutations (Phase 3)

## 1. 变更文档
- [x] 1.1 proposal / spec delta 写就,`openspec validate store-server-mutations --strict` 通过。

## 2. 端点实现
- [x] 2.1 `server.rs`:`DELETE /partitions/{pid}/blobs/{key}`(spawn_blocking `delete`)+ 状态映射(204/404/409/400/503)。
- [x] 2.2 `POST /partitions/{pid}/blobs/{key}/undelete`(`undelete`)+ 映射(200/404/409/400/503)。
- [x] 2.3 `POST /partitions/{pid}/blobs/{key}/ttl?expiration_ms=<i64>`(`update_ttl`)+ 映射(204/404/409/400/503)。
- [x] 2.4 `metrics.rs`:`store_server_mutations_total`(或按 op)。

## 3. 测试
- [x] 3.1 集成测试(oneshot):put→delete(204)→get(404)→重复 delete(409);put→delete→undelete(200)→get(200);put→ttl(204)→重复 ttl(409);非法 key(400)。
- [x] 3.2 `cargo test` 绿 + `cargo clippy` 无告警。

## 4. 文档
- [x] 4.1 `docs/design/cluster.md` §9.1 数据面端点说明补变更类。

## 后续
- [ ] 变更经拉复制传播(S3);client SDK(暂不做);**EC 仍锁定**。

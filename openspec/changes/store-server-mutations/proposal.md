## Why

S1/S2 让数据节点能 `PUT`/`GET` blob 并接入集群,但**变更类操作**(delete)还没有 HTTP 端点。`store` 引擎早已实现该语义(且 Phase 1 已给它套上被动 IO-error 追踪),只差数据面暴露。补齐后 router SDK / gateway 才能对对象做删除。

> **后续变更(2026-09-23,`simplify-delete-model`)**:本 change 曾落地的「复活」「改 TTL」两条端点
> 已随"对象不可变 + 删除不可逆"删除,数据面**只剩 `DELETE`**(TTL 改为写入时经 `PUT ?expiration_ms=` 给定);
> `Error::NotDeleted` / `AlreadyUpdated` 也已不存在,变更类 409 只剩「重复 DELETE 命中墓碑」。下文保留为当时的记录。

## What Changes

- store-server 数据面新增变更端点(承 store 引擎语义,经 `spawn_blocking` offload):
  - `DELETE /partitions/{pid}/blobs/{key}` → `BlobStore::delete`(写墓碑)。
- 错误→状态码映射(对齐 store 语义):`NotFound`→404、`Deleted`(已删)→409、`Unavailable`(IO 自关)→503、非法 key→400。
- 指标:复用 `store_server_errors_total`;新增 `store_server_mutations_total{op}`(或分 op 计数)。

## Capabilities

### New Capabilities
- `store-server-mutations`: 数据面变更端点(delete)及其到 store 引擎语义的状态码映射。

### Modified Capabilities
<!-- 复用 store-server 数据面;不改 store 引擎(其语义已具备)。 -->

## Impact

- **crate**:`store-server` `server.rs` 加 3 路由 + handler;`metrics.rs` 加 mutations 计数。
- **保留契约**:blob 不整缓 / 同步引擎经 `spawn_blocking`;blob id 永久占用(delete 写墓碑不释放 id);**EC 仍锁定**。

## Non-goals

- 不做 router SDK(用户指示暂不做)。
- 不做拉复制传播这些变更(S3);S1/S2 单副本本地语义已够测试。
- **EC 暂不解锁**。

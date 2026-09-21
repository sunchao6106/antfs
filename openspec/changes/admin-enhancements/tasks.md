# tasks — admin-enhancements

## 1. 变更文档
- [x] 1.1 proposal / spec delta,`openspec validate admin-enhancements --strict` 通过。

## 2. master
- [x] 2.1 proto:`ListPartitionsRequest` 加 `offset`/`limit`,`ListPartitionsResponse` 加 `total`;新增 `SetPartitionState` rpc + 请求/响应消息。
- [x] 2.2 `types.rs`:`MasterCommand::SetPartitionState { partition_id, state }` + apply 改 `PartitionInfo.state`(幂等)+ 单测。
- [x] 2.3 `server.rs`:`list_partitions` 分页(offset/limit/total,按 pid 升序稳定切片)+ `set_partition_state`(leader-only client_write,解析 RW/RO/SEALED)。

## 3. admin 后端
- [x] 3.1 `config.rs`:加 `viewer_token`;`Validate` 不变。
- [x] 3.2 `master_api.rs`:`partitions(offset,limit) -> (Vec, total)`;`set_partition_state(pid, state)`。
- [x] 3.3 `server.rs`:RBAC 中间件(GET=viewer|operator,写=operator,viewer 写 403;空关校验)+ `GET /api/me`(返回角色)+ 分页透传 + `POST /api/partitions/{id}/state`。
- [x] 3.4 测试:viewer 读 200/写 403、operator 写 200、分页返回 total、set-state 转调。

## 4. 前端 SPA
- [x] 4.1 i18n 字典(en/zh)+ 语言切换(localStorage 持久化)。
- [x] 4.2 分区表分页控件(offset/limit + total)。
- [x] 4.3 角色感知(viewer 隐藏写操作)+ 分区行 标 RO/SEALED 按钮 + 登录识别角色。
- [x] 4.4 `vite build` 通过,产物内嵌。

## 5. 验证 / 文档
- [x] 5.1 `cargo test` 绿 + `cargo clippy` 无告警 + workspace build。
- [x] 5.2 `docs/design/cluster.md` §11 admin:补分页/RBAC/SetPartitionState/i18n + 配置项(viewer_token)。
- [x] 5.3 浏览器 QA(中英双语 + 角色显隐)。

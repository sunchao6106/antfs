## Why

admin-web MVP 交付了只读 dashboard + 建分区/下线 + 单 token 鉴权。运维实践还需:海量分区**分页**(一次性返回不可扩展)、**RBAC**(只读观察者 vs 可写操作者,避免误操作)、**更多 ops**(标分区 RO/SEALED,对齐 §7.3 盘满转 RO)、以及**中英双语**界面(团队协作)。均为 admin-web change 里显式列出的"后续"。

## What Changes

- **master**:
  - `ListPartitions` 加**分页**:请求 `offset`/`limit`,响应回 `total`(便于前端翻页)。
  - 新增 `SetPartitionState` gRPC(RW/RO/SEALED)+ raft 命令 `MasterCommand::SetPartitionState`(确定性 apply 改 `PartitionInfo.state`)。
- **admin 后端**:
  - **RBAC**:配置分 `operator_token`(读+写)与 `viewer_token`(只读);`GET /api/*` 二者皆可,写操作(`POST`)仅 `operator_token`,viewer 写 → **403**。空 token 关校验(仅本地)。
  - `GET /api/partitions?offset=&limit=` 分页透传,响应 `{ items, total, offset, limit }`。
  - `POST /api/partitions/{id}/state`(body `{state}`)→ master `SetPartitionState`。
  - 登录返回**角色**(operator/viewer),供前端按角色显隐写操作。
- **前端 SPA**:分区表**分页**控件;按角色显隐 ops(viewer 隐藏建分区/下线/改状态);分区行加**标 RO/SEALED**按钮;**中英双语**(EN/中文切换,持久化选择,全文案走 i18n 字典)。

## Capabilities

### New Capabilities
- `admin-enhancements`:分页、RBAC(viewer/operator)、SetPartitionState 运维、SPA 中英双语。

### Modified Capabilities
<!-- master ListPartitions 加分页 + 新增 SetPartitionState 只写命令;admin 鉴权升级为双角色。 -->

## Impact

- **crate**:`master`(proto + `types.rs` 命令 + `server.rs` 两处);`admin`(config/master_api/server/metrics + SPA)。
- **保留契约**:master 只决策;admin 只经 gRPC;指标不自造(仍外链 Grafana)。**EC 仍锁定**。

## Non-goals

- 不做多用户/OIDC/审计持久化(双固定 token 即够);不做游标式分页(offset/limit 足够,海量再议)。
- SetPartitionState 只改分区级写闸门,不触发数据搬迁/EC(EC 仍锁定)。

# admin-enhancements

admin 分页 + RBAC(viewer/operator)+ SetPartitionState 运维 + SPA 中英双语。承 admin-web change 的"后续"。

## ADDED Requirements

### Requirement: 分区列表分页

`ListPartitions` SHALL 支持 `offset`/`limit` 分页并返回总数 `total`;admin `GET /api/partitions` SHALL 透传分页并返回 `{ items, total, offset, limit }`。

#### Scenario: 分页取子集

- **WHEN** 请求 `GET /api/partitions?offset=0&limit=10`
- **THEN** SHALL 返回至多 10 个分区及全部分区总数 `total`

### Requirement: 设置分区状态

master SHALL 提供 `SetPartitionState`(RW/RO/SEALED,leader 提交 raft 命令确定性改状态);admin SHALL 提供 `POST /api/partitions/{id}/state` 转调之。

#### Scenario: 标只读

- **WHEN** operator 对某 RW 分区 `POST .../state` 设为 `RO`
- **THEN** 其后 `ListPartitions` 中该分区 state SHALL 为 `RO`

### Requirement: 双角色 RBAC

admin SHALL 支持两类令牌:`operator_token`(读+写)与 `viewer_token`(只读)。`GET /api/*` 接受任一有效令牌;写操作(非 GET)SHALL 仅接受 `operator_token`,viewer 令牌发起写 SHALL 返回 403。两令牌均空时关闭校验(仅本地)。

#### Scenario: viewer 只读放行、写被拒

- **WHEN** 持 `viewer_token` 请求 `GET /api/partitions`,再 `POST /api/partitions`
- **THEN** 读 SHALL 放行,写 SHALL 返回 403

#### Scenario: operator 读写皆可

- **WHEN** 持 `operator_token` 发起读与写
- **THEN** 二者 SHALL NOT 因鉴权返回 401/403

### Requirement: SPA 中英双语

SPA SHALL 提供中英文切换,所有界面文案走 i18n 字典;所选语言 SHALL 持久化(刷新后保持)。

#### Scenario: 切换语言

- **WHEN** 用户在界面切换到中文
- **THEN** 界面文案 SHALL 变为中文,且刷新页面后仍为中文

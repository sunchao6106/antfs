# core-gaps-wave1

admin 侧收口:复制开关转发 + UI(A7)、异步 compact 转发与进度聚合 + UI(A3)、vite dev 代理(A8)、配置化多用户 RBAC + JSONL 审计(A9)。store/master 侧接口已存在,本 change 只做 admin 落地。

## ADDED Requirements

### Requirement: 复制开关转发

admin SHALL 提供 `POST /api/stores/{node_id}/replication-control`(operator,body `{"paused": bool}`)与 `GET /api/stores/{node_id}/replication-control`,把请求转发到该 store 的 `/admin/replication-control`。`node_id` SHALL 按 master 节点清单解析为 `addr`,无此节点 SHALL 返回 404;store 的状态码与响应体 SHALL 原样透出;仅超时/连接失败 SHALL 返回 502 且带原因。

#### Scenario: 暂停某节点的主动拉复制

- **WHEN** operator `POST /api/stores/101/replication-control`,body `{"paused":true}`
- **THEN** 响应 SHALL 为 store 回的 `{"paused":true}`(状态 200),且其后 `GET` 同一端点 SHALL 回 `{"paused":true}`

#### Scenario: 未知节点与不可达节点

- **WHEN** 请求的 `node_id` 不在 master 节点清单里,或该节点端口无监听
- **THEN** 前者 SHALL 返回 404,后者 SHALL 返回 502 且响应体含传输层原因

### Requirement: 异步 compact 提交

admin `POST /api/partitions/{id}/compact` SHALL 接受 `mode` query:`async` 时对每个副本并发 `POST http://{addr}/admin/partitions/{pid}/compact?async=1`,回 200 + `{"results":[{"node_id","ok","job_id"}|{"node_id","ok":false,"error"}],"error":""}`(按 `node_id` 排序);缺省或 `sync` SHALL 保持原同步语义与响应体;其它取值 SHALL 在 admin 边界返回 400。分区不存在 → 404,分区无副本 → 200 + 空 `results` + 顶层 `error`。

#### Scenario: 提交后台整理

- **WHEN** operator `POST /api/partitions/3/compact?mode=async`(分区有 2 个副本,其中一个不可达)
- **THEN** 响应 SHALL 含两行结果,可达副本 `ok:true` 且带 store 回的数字 `job_id`,不可达副本 `ok:false` 且带原因;整体状态 SHALL 为 200

#### Scenario: 非法 mode

- **WHEN** 请求 `POST /api/partitions/3/compact?mode=bogus`
- **THEN** SHALL 返回 400,且 SHALL NOT 向 store 转发

### Requirement: compact 任务聚合

admin SHALL 提供 `GET /api/partitions/{id}/compact-jobs`:对每个副本并发 `GET http://{addr}/admin/compactions`,**只保留该分区**(`partition_id == id`)的任务、每节点最多最近 5 条,回 `{"results":[{"node_id","ok","jobs":[CompactionJob...]}|{"node_id","ok":false,"error"}]}`。`CompactionJob` SHALL 与 store 台账同契约(`job_id`/`partition_id`/`state`/`started_at_ms`,终态才有 `finished_at_ms`,`done` 才有 `result`,`failed` 才有 `error`)。

#### Scenario: 过滤与聚合

- **WHEN** 请求 `GET /api/partitions/3/compact-jobs`,而某副本台账里既有 pid=3 的任务也有其它分区的任务
- **THEN** 该节点的 `jobs` SHALL 只含 pid=3 的任务(最新在前),每个元素 SHALL 带 `state`,且运行中的任务 SHALL NOT 出现 `result` 字段

### Requirement: 多用户令牌与角色解析

`AdminConfig` SHALL 支持 `users: Vec<AdminUser>`(`name` + `token` + `role`)与 `audit_path`;`Validate` SHALL 拒绝空 `token` 与不在 `{operator, viewer}` 内的 `role`。鉴权 SHALL 以 `AuthConfig{users, operator_token, viewer_token}` 为准:`resolve(token)` SHALL **先按 token 精确匹配 `users`**(命中时 actor 名 = `name`、角色取自配置),再回落 `operator_token` / `viewer_token`(actor 名分别为 `operator` / `viewer`);三者皆空 SHALL 视为关校验(本地,actor 名 `local`)。RBAC 门控 SHALL 保持"viewer 只读":viewer 的写操作 SHALL 返回 403,未知/缺失令牌 SHALL 返回 401。

#### Scenario: 多用户各按角色

- **WHEN** 配置了两个用户(alice/operator、bob/viewer),分别用各自令牌请求
- **THEN** `GET /api/me` SHALL 分别回 `operator` / `viewer`;alice 的写操作 SHALL 成功,bob 的写操作 SHALL 返回 403

### Requirement: 审计记录

admin SHALL 把审计记录以 JSONL 追加到 `audit_path`(启动时 create + append 打开;打开或写入失败 SHALL 只 warn 并降级为空实现,**SHALL NOT** 影响请求结果)。记录时机 SHALL 在响应返回后,覆盖:① 所有写操作(POST/PUT/DELETE…);② `/api/me`(鉴权成功与失败都记,失败 `status` 为 401)。每条 SHALL 含 `ts_ms`/`actor`/`role`/`method`/`path`/`status`/`summary`/`remote_addr`,其中 `summary` 为 `方法 + 路径 + query`。读操作 SHALL NOT 落档。

#### Scenario: 写操作与被拒请求都留痕

- **WHEN** 依次发起一次成功的写操作、一次 `/api/me` 成功请求、一次未知令牌的 `/api/me`、一次 viewer 的写操作,然后 `GET /api/audit`
- **THEN** 审计文件 SHALL 恰好新增 4 行,`status` 分别为 200 / 200 / 401 / 403,`actor` 分别为对应操作者 / 对应操作者 / `anonymous` / 对应 viewer;读操作(如 `GET /api/cluster`)SHALL NOT 新增行

#### Scenario: 审计不可用时请求照常

- **WHEN** `audit_path` 的父目录不存在(打不开)
- **THEN** 写操作 SHALL 仍返回 200,`GET /api/audit` SHALL 回空表

### Requirement: 审计查询

admin SHALL 提供 `GET /api/audit?limit=N`(operator 专属,viewer SHALL 返回 403):`limit` 缺省 200、上限 2000;回 `{"entries":[AuditEntry...]}`(**最新在前**)。实现 SHALL 只从文件尾部反向按块读取,解析失败的行 SHALL 跳过而不是整表失败。

#### Scenario: limit 与顺序

- **WHEN** 审计文件已有 4 条记录,operator 请求 `GET /api/audit?limit=2`
- **THEN** SHALL 回 2 条,且顺序为最新在前;`limit=0` SHALL 回空数组,超过上限的 `limit` SHALL 只截断不报错

### Requirement: admin UI 新面板

SPA SHALL 在节点页「限流」面板内提供复制开关(当前状态徽标 + 暂停/恢复按钮,结果与错误显示在面板消息条);在分区页提供「后台整理」,提交后在该行下方展开逐副本进度(`node_id` + `running`/`done`/`failed`,完成时显示回收字节,失败显示原因),并轮询 `compact-jobs` 直到全部终态或用户收起;并提供 operator 可见的「审计」tab(时间/操作者/角色/方法/路径/状态/摘要/来源 IP + 手动刷新 + 条数选择)。文案 SHALL 有中英两份。

#### Scenario: 展开的进度行跟随任务终态

- **WHEN** operator 点某分区「后台整理」,store 侧任务随后完成
- **THEN** 进度行 SHALL 从 `运行中` 变为 `完成` 并显示回收字节,轮询 SHALL 在全部终态后停止

### Requirement: vite dev 代理

`crates/admin/ui/vite.config.ts` SHALL 配置 `server.proxy`,把 `/api` 与 `/metrics` 代理到 `process.env.ANTFS_ADMIN_TARGET ?? "http://127.0.0.1:7300"`,使 `npm run dev` 无需 CORS 配置即可访问 admin 后端。

#### Scenario: dev 模式访问后端

- **WHEN** 本地已起 admin 后端,前端以 `npm run dev` 启动并请求 `/api/cluster`
- **THEN** 请求 SHALL 被代理到 `ANTFS_ADMIN_TARGET`(默认 `http://127.0.0.1:7300`)并得到后端响应

## Why

对齐 Ambry 运维基元与生产可运维性的最后一批"接口缺环",都在 admin 这一侧打通即可,store/master 侧的基础能力(A7 复制开关、A3 异步 compact 台账)已落地:

1. **复制开关无 UI/无转发**:store 已有 `POST|GET /admin/replication-control`(暂停**本节点主动拉复制**),但运维只能 curl 到节点上,admin/SPA 够不着。
2. **compact 只有同步路径**:`POST /admin/partitions/{id}/compact` 转发是 2s 短超时,大库 compact 必然先在 admin 侧报超时(store 继续跑完)。store 侧已加 `?async=1` + `GET /admin/compactions` 台账,但 admin 未转发、UI 也不展示进度。
3. **鉴权只有两个固定角色**:`operator_token`/`viewer_token` 无法给多人分别发令牌、也无法辨认"是谁做的"(审计/追责需求)。
4. **没有审计**:谁在什么时候改了哪个分区状态/下线了哪个节点,admin 侧无任何留痕(admin-web change 当初把"审计持久化"列为非 goals)。
5. **UI 开发体验**:前端跑 `npm run dev` 时没有代理,得手改地址或另配 CORS。

## What Changes

- **A7 复制开关(转发 + UI)**:admin 增 `POST|GET /api/stores/{node_id}/replication-control`(照 `request-control` 的写法:`node_id → addr` 从 master 节点清单取,无此节点 404;body 原样转发;store 的状态码/JSON 原样透出,仅传输层错误 → 502)。SPA 节点页「限流」面板里加「复制」子面板:状态徽标 + 暂停/恢复按钮 + 说明,结果/错误进面板消息条。
- **A3 异步 compact(转发 + 进度 UI)**:admin `POST /api/partitions/{id}/compact?mode=async` 对每个副本 `POST .../compact?async=1` 回 `job_id`;`GET /api/partitions/{id}/compact-jobs` 聚合各副本 `GET /admin/compactions` 中**该 pid** 的任务(每节点最近 5 条)。`mode` 缺省/`sync` 走原同步路径,非法值 400。SPA 分区页加「后台整理」:提交后在行下展开逐副本进度(running/done/failed + 回收字节/失败原因),2s 轮询到全部终态或用户收起。
- **A8 vite dev 代理**:`server.proxy` 把 `/api` 与 `/metrics` 指向 `ANTFS_ADMIN_TARGET`(默认 `http://127.0.0.1:7300`);文档补一句开发流程。
- **A9 配置化多用户 + 审计**:
  - `AdminConfig` 增 `users: Vec<AdminUser>`(`name`/`token`/`role`,`role` 仅 `operator`/`viewer`,非法值与非空 token 校验进 `Validate`)与 `audit_path`(默认 `./admin-audit.jsonl`)。既有 `operator_token`/`viewer_token` 默认值语义**不变**。
  - `AuthConfig{users, operator_token, viewer_token}` + `resolve(token) → Actor{name, role}`(先查 users,再回落旧令牌,三者皆空仍为关校验、actor 名 `local`);RBAC 中间件把 `Actor` 放进 request extensions。
  - 新 `audit.rs`:`AuditLog`(启动 create+append 打开,失败只 warn 降级为空实现)按 JSONL 追加 `{ts_ms, actor, role, method, path, status, summary, remote_addr}`;中间件在响应返回后记**写操作**与 `/api/me`(含 401/403),**写失败只 warn、绝不影响请求**。
  - `GET /api/audit?limit=N`(operator,默认 200/上限 2000):从文件**尾部反向按块读**,解析失败的行跳过;回 `{"entries":[...]}`,最新在前。审计 append-only,需自行 logrotate。
  - SPA 新增 operator 可见的「审计」tab(表格 + 刷新 + 条数选择);登录页文案注明 `[[admin.users]]` 多用户令牌。

## Capabilities

### New Capabilities

- `core-gaps-wave1`:admin 侧复制开关转发与 UI、异步 compact 转发与进度聚合、多用户 RBAC + JSONL 审计(含尾部查询端点)、vite dev 代理。

### Modified Capabilities

- `admin-enhancements`(既有能力):`AuthConfig` 由"两个固定令牌"扩展为"多用户表 + 旧令牌回落",viewer 的可读端点集合新增例外(`/api/audit` 仅 operator);`POST /api/partitions/{id}/compact` 增加 `mode` query(缺省行为不变)。既有语义与默认值不破坏。

## Impact

- **crate**:`admin`(config/audit/server/store_client 与 UI)、`devcluster`(审计落 `--dir/admin-audit.jsonl`、以 `into_make_service_with_connect_info` 起服务以记录来源 IP)。
- **契约变化**:新增 4 个端点(见 spec);`AuthConfig` 结构变化(库内 API,非外部契约);`/api/me` 仍只返回 `role`。
- **兼容性**:store 侧无改动(直接调用已落地的 `?async=1` / `/admin/compactions` / `/admin/replication-control`);旧部署只配 `operator_token`/`viewer_token` 时行为与之前完全一致(审计默认开写 `./admin-audit.jsonl`,可用 `audit_path` 指向别处)。
- **被否决方案**:把审计写进 master/Raft(admin 观测面数据不该占控制面共识);把审计记在 handler 里(每个 handler 都要改,且 401/403 记不到);`/api/audit` 允许 viewer 读(viewer 是"看集群",不该看谁在什么时候做了什么、从哪个 IP);审计全量读文件(大文件会吃内存,故反向分块读)。

## Non-goals

- 不做审计轮转/压缩/远程汇聚(交给 logrotate / 外部采集)。
- 不做 admin 侧操作者写操作的"业务语义摘要"(summary 就是方法 + 路径 + query;语义化摘要需要每个 handler 参与,收益不抵改动面)。
- 不做异步 compact 的持久台账(store 台账本就是内存态,重启即丢;UI 只在提交后跟进一段时间)。
- 不动 store 侧复制开关的语义(仍是不持久化、重启恢复拉取)。

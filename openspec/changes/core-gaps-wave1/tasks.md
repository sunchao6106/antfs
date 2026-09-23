# Tasks

- [x] admin:`POST|GET /api/stores/{node_id}/replication-control` 转发(照 request-control:404 / 原样透出 / 502)+ 真 loopback 假 store 测试(A7)
- [x] admin:`POST /api/partitions/{id}/compact?mode=async` 逐副本提交 `job_id`;`mode` 缺省/`sync` 不变、非法 400 + 测试(A3)
- [x] admin:`GET /api/partitions/{id}/compact-jobs` 逐副本聚合(只挑该 pid、按 node_id 排序、节点失败 ok:false)+ 测试(A3)
- [x] admin:`store_client` 增 `CompactionJob` / `CompactionsResponse` / `CompactionSubmitted` DTO(与 store 台账同契约)
- [x] admin 配置:`AdminConfig.users`(`AdminUser{name,token,role}`)+ `audit_path`(默认 `./admin-audit.jsonl`)+ `Validate`(role 仅 operator/viewer、token 非空)+ 单测(A9)
- [x] admin:`AuthConfig{users, operator_token, viewer_token}` + `resolve() → Actor{name, role}`(先 users 再旧令牌,皆空 = 关校验 actor `local`);RBAC 中间件写入 request extensions(A9)
- [x] admin:新 `audit.rs`(`AuditLog`:create+append 打开、打开/写失败降级只 warn、JSONL 追加、尾部反向分块读跳过坏行)+ 单测(含跨 64 KiB 块的边界)(A9)
- [x] admin:审计中间件(最外层,记写操作 + `/api/me`,含 401/403;`summary = 方法 + 路径 + query`;`remote_addr` 取 `ConnectInfo`)(A9)
- [x] admin:`GET /api/audit?limit=N`(operator 专属、默认 200 上限 2000、最新在前;viewer → 403)(A9)
- [x] admin:`main.rs` / `devcluster` 接线(审计路径、`into_make_service_with_connect_info`)
- [x] UI:节点页「复制」子面板(状态徽标 + 暂停/恢复 + 消息条)(A7)
- [x] UI:分区页「后台整理」+ 行下逐副本进度(running/done/failed + 回收字节/失败原因)+ 2s 轮询到终态或收起(A3)
- [x] UI:新「审计」tab(operator 可见:表格 + 刷新 + 条数选择)+ i18n en/zh(A9)
- [x] UI:登录页文案补 `[[admin.users]]` 多用户令牌;`vite.config.ts` 加 `server.proxy`(`/api`、`/metrics` → `ANTFS_ADMIN_TARGET`,默认 `http://127.0.0.1:7300`)(A8/A9)
- [x] 文档:`docs/design/cluster.md` §9.1(store 新端点 + 3 个新指标)、§11(批 2 增量、`admin.users` / `admin.audit_path` 配置行、UI 开发代理说明)
- [x] 真集群 curl 端到端(**4 store 集群**):A6 正/负例(暂停目标拉复制 → 目标停 BOOTSTRAP、源目录保留、不下发 DROPPED;paused_drop 不记账;恢复后追平 → 摘源 + 删源目录 + `dropped_total=1` + 幂等;副本集合 102/103/104、RF 3/3、数据可读)、A4(把 `disk_near_full_ratio` 调低重启 → 12 个 RW 分区全部强制 RO 且 15s 不回退)、A5(health 回填全 HEALTHY、`master_unhealthy_disks=0`)、A7(暂停期间 rounds 不涨、恢复续涨,经 admin 转发同样生效)、A3(经 admin `mode=async` 逐副本 job_id → `compact-jobs` 全部 done 且回收字节正确)、A9(alice=operator/bob=viewer/未知令牌 401/bob 写 403;审计文件记录 200/401/403 三类事件;`/api/audit` limit 生效、viewer → 403)
- [x] 浏览器核对新 UI:审计 tab(表格含 actor/role/请求/状态/来源,最新在前)、节点页「复制」开关、分区页「后台整理」
- [x] 全量回归:`cargo test --workspace` 全绿、`clippy --workspace --all-targets` 无新增告警、`npm run build` 通过

## 测试中发现并修掉的两个真 bug(不在原计划内)

- [x] **store 没有真实 BOOTSTRAP 阶段**:`replica.rs` 新建副本直接置 `ACTIVE`,使 master 的"等继任者 ACTIVE"确认形同虚设 → 迁移时可能在**新副本还是空的时候**就删掉源数据(源是唯一持有者时即丢数据)。已修:副本自 BOOTSTRAP 起,拉复制判定真追平后才 `ACTIVE`(含"所有 peer 都无数据"与"重启重开"两个易错分支),新增 `store_server_replicas_bootstrapping`;`tests/replica_state.rs` 4 例 + `tests/replication_control.rs` 暂停用例(6 例在回退初值时全部失败,证明有效)。
- [x] **配置表被静默忽略**:文档与代码注释都写"字段在 `[admin]`/`[store_server]`/`[master]` 表下",但 `Loader::load` 只认根级字段 → 按文档写的配置文件**全部值被 serde 默认掩盖**(实测:`[store_server]` 报 `data_dirs` 为空、`[admin]` 连不上 master)。已修:新增 `Loader::load_table("<表名>")`(表存在取表、字段写错即报错;不存在回落 flat 兼容),admin/master/store 三个二进制改用之;4 个新测试覆盖表/flat/嵌套 env/优先级;文档同步(common.md 新增说明、cluster.md 三处配置清单标注、frontend.md 交叉引用)。

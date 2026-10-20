# Tasks: master-leader-address

## 1. Master Side

- [x] 1.1 `RegisterMasterEndpoint` 命令 + `master_endpoints` 字段(serde default)+ apply 幂等 upsert + 单测
- [x] 1.2 `serve()` 后自报循环(退避重试);`leader_addr()` 两级解析(表优先,raft fallback)+ 单测
- [x] 1.3 proto `leader_addr` 注释统一改 gRPC 语义

## 2. Router Self-Healing

- [x] 2.1 `Error::NotLeader { detail, leader_addr }` 变体(含 kind 标签/全部调用面 arm)
- [x] 2.2 `Router::retarget_master`(1s 防抖 + Mutex 换 MasterClient)+ `allocate_one` 单次重试接线
- [x] 2.3 e2e:3 节点 master、client 连 follower → 自愈切 leader → 写成功 + 错误分类单测

## 3. Docs & Closure

- [x] 3.1 spec delta(自报注册 + leader_addr 语义 + router 自愈)+ validate
- [x] 3.2 router.md §7 勾销 + 错误表 NotLeader 行;cluster.md 注册段;DEVLOG 步 84
- [x] 3.3 workspace 全绿 + devcluster 真面自愈复验 + archive + 单 commit + CI 盯绿

## 落地记录(已回填)

- master **12 套 157**:新增 `register_master_endpoint_is_idempotent_upsert` +
  `stored_state_without_master_endpoints_fails_loudly`(严契约负向钉);4 个既有"缺字段
  可读" fixture 补 `master_endpoints`(被测语义 = **其余** serde(default) 字段;严字段
  另钉);3 处集成断言旧 raft 语义改 gRPC 唯一真值 + 注册收敛确定性轮询(禁 sleep)。
- router **3 套 38**:map_master_error 分类单测 + e2e
  `router_self_heals_when_master_addr_points_at_follower`(3 节点真 master,client 故意连
  follower:前置轮询 follower 提示可得 → 首号 not-leader 自愈切换 → 发号成功 → 二号直达);
  frontend `router_status` 增 NotLeader→503 arm(自愈尽头的上抛位)。
- **workspace 77 套 779/0;clippy 0;fmt 净**。devcluster 为单 master(无 follower)不经此
  面 —— 真面证据以三节点 e2e harness 为准。
- 实现注记:所有者已在提案 review 中终裁(无历史包袱,gRPC 唯一真值,提案/design/spec 已同步);行尾 prop 注释改语义时曾出现一次双重替换叠加(git 可回看),当场清平。

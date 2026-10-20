# master-leader-address Specification

## Purpose
TBD - created by archiving change master-leader-address. Update Purpose after archive.
## Requirements
### Requirement: master 节点经 Raft 注册可用的 gRPC 端点
每个 master 节点在启动后 SHALL 通过 Raft 命令注册自身的(node_id, 对外 gRPC 地址)到状态机(幂等 upsert,重试安全);非 leader 请求的 `leader_addr` 回填 SHALL 返回 leader 的注册 gRPC 地址(**唯一真值**,不回退 membership 的 raft 地址 —— 所有者裁定:系统无历史包袱);注册未收敛的窗口期 SHALL 返回空地址(调用方重试,自报循环秒级收敛)。

#### Scenario: 切主后非 leader 回可用地址
- **WHEN** 客户端向 follower 发号而 leader 已切换,且各节点已完成自报注册
- **THEN** 响应的 `leader_addr` 为新 leader 的 gRPC 地址,客户端可直接连接

#### Scenario: 注册窗口期无提示但不误导
- **WHEN** follower 的自报注册尚未收敛(刚选主/刚启动)
- **THEN** `leader_addr` 为空(不回退"用不了的 raft 地址");调用方在无提示下重试,注册收敛后恢复正常重定向

### Requirement: router 发号遇 not-leader 自动重定向
Router 在发号收到"非 leader"且回执携带可用 leader gRPC 地址时,SHALL 将内部 master 客户端整体切换到该地址(带防抖)并重试发号一次;重试仍失败 SHALL 上抛携带 leader 地址提示的 `NotLeader` 错误(调用方/运维可见 hint)。防抖窗口内的重复 not-leader SHALL NOT 反复切换通道。

#### Scenario: 配置指向 follower 的 router 自愈
- **WHEN** router 的 master_addr 指向某 follower,master 集群已有 leader
- **THEN** 首次发号触发通道切换并成功发号;后续写不再经过 not-leader 路径


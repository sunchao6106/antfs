# master-control-plane

master 的控制面 gRPC:数据节点接入、心跳、软状态与只读查询(D2 / Q1)。

## ADDED Requirements

### Requirement: 数据节点自注册与容量自报

数据节点 SHALL 通过 gRPC 向 master 自注册,自报节点身份(dc/rack/addr)与各盘经 `statfs` 探得的**真实容量**(Q1);master SHALL 将节点身份固化进 Raft `Nodes`。master SHALL NOT 依赖写死的硬件布局配置获取容量。

#### Scenario: 首次注册固化身份

- **WHEN** 一个数据节点首次注册,上报其节点 id、地址与磁盘容量
- **THEN** master leader SHALL 经 Raft 提案将该节点写入 `Nodes`,提交后 `ListNodes` 在多数派节点上可见该节点

#### Scenario: 重复注册幂等

- **WHEN** 同一 `node_id` 再次注册(重启/重连)
- **THEN** master SHALL 以最新上报覆盖该节点身份,不产生重复节点记录

### Requirement: 注册与心跳的准入校验

master SHALL 对注册与心跳做准入身份校验(M1 用共享 `cluster_secret`);校验失败 SHALL 以 `Unauthenticated` 拒绝并记 `warn`(预期拒绝不打 `error`)。

#### Scenario: 无效凭证被拒

- **WHEN** 一个未携带正确 `cluster_secret` 的节点尝试注册
- **THEN** master SHALL 拒绝该请求(`Unauthenticated`),SHALL NOT 将其写入 `Nodes`

### Requirement: 双向流心跳与软状态重建

master SHALL 提供双向流 `Heartbeat`:上行携带 liveness 与各盘余量,下行携带指令(M1 下行暂空)。master SHALL 将 liveness/磁盘余量维护为内存软状态,并在新 leader 上任后由心跳重建。

#### Scenario: 心跳更新 liveness

- **WHEN** 已注册节点周期发送心跳
- **THEN** master SHALL 更新该节点的 `last_beat` 与磁盘余量软状态,`ListNodes` 据此反映其存活状态

#### Scenario: 新 leader 重建软状态

- **WHEN** 发生 leader 切换
- **THEN** 新 leader SHALL 在一个心跳周期内从收到的心跳重建 liveness/磁盘余量软状态,不要求软状态经 Raft 复制

### Requirement: 写请求重定向到 leader

当 follower 收到写请求(RegisterNode / 分配 id)时,SHALL 返回当前 leader 地址供客户端重定向;只读查询任意节点 SHALL 可应答。

#### Scenario: follower 重定向写

- **WHEN** 客户端把 `RegisterNode` 发给一个 follower
- **THEN** 该 follower SHALL 返回 leader 地址,SHALL NOT 自行提交该写

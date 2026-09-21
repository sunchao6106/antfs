# store-server

数据节点的 blob 数据面与本地副本管理(承 cluster.md §9)。**S1 不含 EC、不接 master、不复制。**

## ADDED Requirements

### Requirement: 磁盘容量运行时探测

store server SHALL 对每个声明的盘目录用 `statfs` 运行时探测真实总容量与可用容量(Q1),SHALL NOT 依赖写死的容量配置。

#### Scenario: 报告非零真实容量

- **WHEN** store server 挂载一个存在的盘目录
- **THEN** 该盘的 `capacity_bytes` 与 `free_bytes` SHALL 来自 `statfs` 且 `capacity_bytes > 0`

### Requirement: 分区副本本地管理

store server SHALL 以 partition 为单位管理本地 `BlobStore`(每个 partition-replica 一个),`ensure_replica` SHALL 幂等(重复请求不重建、不报错);未建 replica 的 partition 数据请求 SHALL 返回未找到。

#### Scenario: 幂等创建 replica

- **WHEN** 对同一 partition 连续两次 `ensure_replica`
- **THEN** SHALL 只创建一个 `BlobStore`,第二次幂等返回

#### Scenario: 未知 partition 的数据请求被拒

- **WHEN** 对一个没有本地 replica 的 partition 发 `PUT`/`GET`
- **THEN** store server SHALL 返回 404(未找到)

### Requirement: HTTP 数据面 put/get

store server SHALL 提供 HTTP `PUT`/`GET` 端点按 partition + key 读写 blob;同步存储引擎调用 SHALL 经阻塞线程池 offload 以不阻塞异步 reactor。已存在 key 的 `PUT` SHALL 返回冲突(409),不存在 key 的 `GET` SHALL 返回未找到(404)。

#### Scenario: put 后 get 字节一致

- **WHEN** 向某 partition `PUT` 一个 blob(32B key)后 `GET` 同 key
- **THEN** SHALL 返回 201 写入成功,`GET` SHALL 返回 200 且响应体与写入字节完全一致

#### Scenario: 重复 put 冲突

- **WHEN** 对同一 key 第二次 `PUT`
- **THEN** SHALL 返回 409(对齐 store 引擎 `AlreadyExists`:blob id 永久占用)

#### Scenario: 非法 key 被拒

- **WHEN** `PUT`/`GET` 的 key 不是 32 字节(64 hex)
- **THEN** SHALL 返回 400(非法请求)

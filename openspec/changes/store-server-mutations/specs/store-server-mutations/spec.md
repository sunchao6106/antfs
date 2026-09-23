# store-server-mutations

数据节点变更类端点(delete),承 store 引擎语义。

> **后续变更(2026-09-23,`simplify-delete-model`)**:对象不可变 + **删除不可逆** ⇒ 原先的「复活对象」与「更新 TTL」两个
> Requirement 已被**删除**(数据面只剩 `DELETE`;TTL 只能在写入时经 `PUT ?expiration_ms=` 给定);`Error::NotDeleted` /
> `AlreadyUpdated` 亦不存在,变更类 409 只剩「重复 DELETE 命中墓碑」。保留为当时的设计记录。

## ADDED Requirements

### Requirement: 删除对象(写墓碑)

store server SHALL 提供 `DELETE /partitions/{pid}/blobs/{key}` 对已存在对象写墓碑;成功返回 204;key 不存在返回 404;已删除返回 409;非法 key 返回 400。

#### Scenario: 删除后不可读

- **WHEN** 对一个已写入的 key 发 `DELETE`,随后 `GET` 同 key
- **THEN** `DELETE` SHALL 返回 204,`GET` SHALL 返回 404

#### Scenario: 重复删除冲突

- **WHEN** 对一个已删除的 key 再次 `DELETE`
- **THEN** SHALL 返回 409

### Requirement: 同步引擎 offload 与自关拒绝

变更端点 SHALL 经阻塞线程池 offload 同步 store 调用;当底层 `BlobStore` 因连续 IO 错误自关(Phase 1)时,变更请求 SHALL 返回 503。

#### Scenario: 自关后变更返回 503

- **WHEN** 某 replica 的 `BlobStore` 已因 IO 错误自关
- **THEN** 对该分区的变更请求 SHALL 返回 503

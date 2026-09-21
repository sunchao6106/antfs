# store-server-mutations

数据节点变更类端点(delete / undelete / updateTtl),承 store 引擎语义。

## ADDED Requirements

### Requirement: 删除对象(写墓碑)

store server SHALL 提供 `DELETE /partitions/{pid}/blobs/{key}` 对已存在对象写墓碑;成功返回 204;key 不存在返回 404;已删除返回 409;非法 key 返回 400。

#### Scenario: 删除后不可读

- **WHEN** 对一个已写入的 key 发 `DELETE`,随后 `GET` 同 key
- **THEN** `DELETE` SHALL 返回 204,`GET` SHALL 返回 404

#### Scenario: 重复删除冲突

- **WHEN** 对一个已删除的 key 再次 `DELETE`
- **THEN** SHALL 返回 409

### Requirement: 复活对象(undelete)

store server SHALL 提供 `POST /partitions/{pid}/blobs/{key}/undelete` 复活已删除对象;成功返回 200;对未删除对象返回 409;key 不存在返回 404。

#### Scenario: 删除后复活可读

- **WHEN** 对一个已删除的 key `undelete`,随后 `GET`
- **THEN** `undelete` SHALL 返回 200,`GET` SHALL 返回 200

### Requirement: 更新 TTL

store server SHALL 提供 `POST /partitions/{pid}/blobs/{key}/ttl?expiration_ms=<i64>` 更新对象过期时间;成功返回 204;已更新过返回 409;已删除返回 409;key 不存在返回 404。

#### Scenario: 更新 TTL 成功

- **WHEN** 对一个已写入且未改 TTL 的 key 发 `ttl` 更新
- **THEN** SHALL 返回 204

### Requirement: 同步引擎 offload 与自关拒绝

变更端点 SHALL 经阻塞线程池 offload 同步 store 调用;当底层 `BlobStore` 因连续 IO 错误自关(Phase 1)时,变更请求 SHALL 返回 503。

#### Scenario: 自关后变更返回 503

- **WHEN** 某 replica 的 `BlobStore` 已因 IO 错误自关
- **THEN** 对该分区的变更请求 SHALL 返回 503

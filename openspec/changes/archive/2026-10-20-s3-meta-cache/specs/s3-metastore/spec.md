# s3-metastore 规约增量(s3-meta-cache)

## ADDED Requirements

### Requirement: 网关每请求双查缓存 TTL 装饰器
S3 网关 SHALL 经 `CachingMetaStore` 装饰器缓存每请求两条热路径 —— `lookup_credential`(SigV4 验签)与 `head_bucket`(授权门):正条目 SHALL 按 `s3.metastore.meta_cache_ttl_secs`(默认 30)缓存,负条目(查无)SHALL 按 ≤5s 缓存;`meta_cache_ttl_secs = 0` SHALL 不装配缓存(原对象直返,零行为变化)。本进程的凭证增删与桶元数据写(create/delete bucket、set_bucket_quota/acl/policy/acl_grants、create/delete credential)SHALL 在调用后即时失效对应缓存条目(同实例管理面改立即可见);多实例共享同一 metastore 时变更 SHALL 在 ≤ TTL 内收敛。对象面(put/get/list/multipart)与错误 SHALL 一律穿透不缓存;缓存 SHALL 以 `s3_meta_cache_lookups_total{kind, result}` 观测(hit/miss/error)。

#### Scenario: 每请求双查命中缓存
- **WHEN** 同一 access_key 连续验签两次而期间无凭证变更
- **THEN** 仅第一次打 metastore,第二次由缓存供给,指标记 hit

#### Scenario: 本进程删除凭证立即生效
- **WHEN** 管理面删除某 access_key 后数据面立即以该 key 验签
- **THEN** 查询穿透并见到删除后的真相(403),不再命中旧正条目

#### Scenario: 负条目短 TTL 防挡新凭证
- **WHEN** 某 access_key 先被负缓存(查无)随后同进程创建
- **THEN** 创建成功即摘除负条目,新凭证立即可用(不被 5s 负缓存挡)

#### Scenario: ttl=0 零行为逃生口
- **WHEN** 配置 `s3.metastore.meta_cache_ttl_secs = 0`
- **THEN** 装饰器不装配,所有请求逐次打 metastore,行为与无该特性逐项一致

#### Scenario: 对象面不缓存
- **WHEN** 任意对象操作(put/get/list/multipart)执行
- **THEN** 每次穿透到 metastore,S3 读写一致性不受 TTL 影响

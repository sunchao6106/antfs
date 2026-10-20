# Design: s3-meta-cache

## D1 装配与逃生口

`wrap(inner, ttl_secs, metrics)`:ttl=0 → 原对象直返(**不装配**,零行为零开销);
>0 → `Arc<CachingMetaStore>` 包一层。装配点唯一:`metastore::connect()` 之后、
`AppState` 组装之前(独立二进制与 devcluster 同点)。config 默认 30。

## D2 缓存面(两条热路径)

- `lookup_credential`(auth 验签)与 `head_bucket`(授权门/桶元数据);
- **正条目 TTL = 配置**;**负条目(查无)固定 5s** —— 未知 access_key 轰炸不逐次打库,
  且"刚建的凭证"最长 5s 后负缓存过期;
- std `RwLock<HashMap>`(读=克隆值纳秒级;无 await 持锁,clippy 守卫);
- 错误**不缓存**:fail-open 到 DB 真相,只计 `result=error`。

## D3 失效面(本进程写路径)

| 原语 | 失效 |
|---|---|
| `create_credential` | Ok 后按返回 access_key 摘(负)缓存 |
| `delete_credential` | 无条件摘 |
| `create_bucket` / `delete_bucket` / `set_bucket_quota/acl/policy/acl_grants` | Ok 后摘桶条目 |

Err 后不摘(DB 状态未变,摘了只是多余重查 —— 但为极简统一,桶路径一律 Ok 后摘,
凭证删除无条件摘:Err 时摘也无害)。跨实例:≤ TTL 收敛(S3 最终一致同角,proposal 记档)。

## D4 容量兜底

每图 4096:插入时满 → 先 `retain(未过期)`;仍满 → `clear()`(重建风暴兜底,记档;
桶/凭证远不及该量级)。

## D5 观测

`s3_meta_cache_lookups_total{kind=credential|bucket, result=hit|miss|error}`,
命中率 = hit/(hit+miss)。既有 `s3_metastore_queries_total/errors_total` 保持原状
(此前即未接线,不在本变更加面 —— 报告已提,留待后续统一)。

## D6 测试

计数 mock(只实现缓存面触碰的原语,其余 InternalError):
命中/负缓存 5s 语义/删凭证失效/建凭证摘负缓存/桶写失效/建桶摘负缓存/确定性过期
(直构私有图倒拨到期时刻,免 sleep,§5.6)/ttl=0 Arc::ptr_eq 身份 + 无缓存行为旁证。
断言走 `wrap` 实际持有的 S3Metrics 实例(§6)。e2e 夹具 ttl=0 保持真值。

## Risks / Rollback

- 回滚 = revert 单 commit;
- 陈旧风险面已被失效模型 + 逃生口钳制(0=关)。

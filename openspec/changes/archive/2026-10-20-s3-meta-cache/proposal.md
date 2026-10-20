# Proposal: s3-meta-cache — 网关每请求双查缓存

## Why

每个 S3 数据面请求在路由分发前打**两次** metastore:`lookup_credential`(SigV4 验签,
auth 中间件)+ `head_bucket`(authorize 授权门)—— 每请求两跳 DB 往返,是网关吞吐/q99
的主要固定税(此前记档"升吞吐前不动"档,现所有者指示落地)。

## What Changes

| 位置 | 改动 |
|---|---|
| `src/metastore/cache.rs`(新) | `CachingMetaStore` 装饰器:两条热路径 TTL 缓存(正负条目;负 5s);8 个写原语本进程失效;错误不缓存;容量 4096/图兜底 |
| `metrics.rs` | `s3_meta_cache_lookups_total{kind, result}`(hit/miss/error) |
| `config.rs` | `s3.metastore.meta_cache_ttl_secs`(默认 30;**0 = 关**:wrap 原样归还,零行为逃生口) |
| 装配 | 独立二进制 `main.rs` + devcluster 内嵌网关(connect 之后、AppState 之前) |
| 测试 | lib 单测 8 个(计数 mock);e2e 夹具显式 `ttl=0` 保持真值直连 |
| 文档 | s3.md 配置/监控双行 + 「网关每请求双查缓存」节;DEVLOG 步 88 |

## Impact

- **语义边界(偏执序:安全>可用>性能)**:本进程写路径即时失效(管理面改完立即可见);
  跨实例共享 metastore 的多网关形态 ≤ TTL 收敛 —— 与 S3 自身最终一致语义同角(凭证撤销
  跨实例最长一个 TTL 窗口;需要立即全局撤销 → 调小 TTL/摘实例)。
- 对象面(put/get/list/multipart)一律穿透:**读写一致性契约优先**,配额记账同事务性
  不受影响。
- 集成测试夹具 `ttl=0`:真值直连,缓存语义由单测覆盖,不遮蔽判定。

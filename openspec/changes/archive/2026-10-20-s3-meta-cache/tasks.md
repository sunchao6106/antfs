# Tasks: s3-meta-cache

- [x] 1. `CachingMetaStore` 装饰器(缓存/失效/容量兜底)+ `wrap` 逃生口(ttl=0 原样归还)
- [x] 2. `s3_meta_cache_lookups_total{kind,result}` 指标 + `meta_cache_ttl_secs` 配置(默认 30,0=关,≤3600 校验)
- [x] 3. 装配:独立二进制 + devcluster 内嵌网关;e2e 夹具显式 ttl=0
- [x] 4. lib 单测 8 个(计数 mock,确定性无 sleep)+ 既有测试全绿
- [x] 5. s3.md 双表行 + 新节 + DEVLOG 步 88 + validate + archive + 单 commit + CI

## 落地记录(已回填)

- antfs-s3 lib 98 → **106**(+8:credential 命中/负缓存、删凭证失效、建凭证摘负缓存、
  桶写失效、建桶摘负缓存、确定性过期、ttl=0 身份);
- **workspace 77 套 794/0;clippy 0;fmt 净**;
- 实现注记:负条目 5s 固定(`ttl` 与 NEG_TTL 取 min);桶失效 Ok 后摘、凭证删除无条件摘;
  容量兜底 = 满图先扫过期再清空(记档重建风暴路径)。

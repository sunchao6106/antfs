# Proposal: 虚拟主机风格路由(<bucket>.s3.domain)

## Why

原「永久拒绝」项,用户 2026-10-15 同日翻案重排队列尾(记录:`s3-scope-rulings`
「追认(第二批)」)。这是标准 S3 客户端的两寻址风格之一(AWS SDK 与 CLI 的默认),
落地后 `https://bucket.s3.example.com/key` 与既有路径风格
`https://s3.example.com/bucket/key` 并存 —— 对象语义层(ACL/配额/multipart/SigV4)
零改动。

## What Changes

- **服务级管线(Host → 路径改写,替路由层中间件)**:`build_app` 返回
  `GatewayPipeline`(tower Service,Clone)—— auth 先按**原 URI/原 Host** 验签
  (canonical 与客户端两种口径天然一致),Principal 注入;再 vhost 改写
  (`Host = {bucket}.{suffix}` → 路径前挂桶名,`:port` 剥离,非法桶标放行);
  最后 Router 分发,handler 一行不动。**不做成 route_layer 的原因(axum 机制)**:
  路由匹配先于层执行,URI 改写必须在分发之前。
- **配置**:`s3.virtual_host_domain`(String,默认空 = 关,部署行为零变化;
  非空须合法域名形态)。
- **语义边界**:`/admin/*`、`/metrics` 仅裸基础域名;TLS 泛证书只盖一层(桶名带点
  撞证书,S3 官方同款限制)。
- **夹具**:e2e 经 `TEST_S3_VHOST_DOMAIN` 注入后缀,双风格并存用例可测。

## Impact

- 默认关:不改任何现部署行为;全部既有 e2e(路径风格)原样绿 = 行为等价回归。
- 验签零改动:SigV4/presigned 的 canonical 与客户端视角一致(管线顺序保证);ACL
  (Principal/公共读)、配额、multipart 全部经路由层自然继承。
- 请求改写无新失败面:命中失败一律放行,错误语义回到既有路径风格路由。

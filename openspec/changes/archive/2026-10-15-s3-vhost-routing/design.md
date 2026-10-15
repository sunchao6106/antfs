# Design: 虚拟主机风格路由(Host → 路径改写)

## 机制:服务级管线(GatewayPipeline)

**关键架构发现(axum 机制课)**:`router.route_layer(...)` 中间件在**路由匹配之后**执行
—— 路由匹配与 `Path` 提取在中间件运行前已定死;在中间件里改写 `req.uri()` 改不了
既定匹配(裸 TCP 探针实证)。因此改写**必须发生在 Router 之前** —— 服务级管线。

```
请求(H = bucket.s3.domain[, :port],URI = [/key[?q]] 或 /)
  build_app(state) -> GatewayPipeline(#[derive(Clone)],impl tower::Service)
    ① is_public_path(/metrics、/admin/*)→ 直发(不鉴权不改写;admin 自带 token 面)
    ② authenticate(metastore, region, 原 method/uri/headers)→ Principal::Signed /
       Anonymous(注入 extensions)/ Err → 直接回错误响应
       —— 验签按**原 path + 原 Host**(vhost 客户端签名 host 含桶、路径不含桶,与路径
          风格口径各自天然成立,零补偿;坏凭证绝不降级匿名)
    ③ apply_vhost_rewrite(配置门控;Host = {bucket}.{suffix} → URI 前挂 /{bucket},
       :port 剥离匹配;非法桶标放行)
    ④ router.oneshot(req)(改写后的 URI 决定匹配;handler 的 Extension<Principal> 不变)
  serve:axum::serve(listener, app.into_make_service())(axum::ServiceExt 桥)
```

关键不变量:
1. **auth 在改写前看到原 URI** —— 两种签名口径天然成立,验签零补偿。
2. **改写是纯路由层动作** —— Principal/ACL/配额/业务语义不动;改写后路径与等价
   路径风格请求同字节(route 内遗留全部摘除,`build_router` 只剩纯路由)。
3. **管线类型可服务**:`GatewayPipeline` 具名结构体(`#[derive(Clone)],字段 AppState +
   Router`)impl `tower::Service<Request>`,`Future = Pin<Box<dyn Future + Send>>` ——
   满足 `axum::serve` 的 Clone/Service/`S::Future: Send` 全部界。(教训:impl-trait 的
   opaque 关联 Future 无法向 serve 证 Send;`tower::service_fn(...).boxed()` 的
   BoxService 不 Clone;**具名结构体 + 具名 Future**是三全解。)

### 改写规则表

| 原 path | 改写后 | 对应操作 |
|---|---|---|
| `/` | `/{bucket}` | 桶级(GET=list / PUT=create / HEAD / DELETE) |
| `/k` | `/{bucket}/k` | 对象操作 |
| `/k?uploads` | `/{bucket}/k?uploads` | multipart |
| `/a/b/` | `/{bucket}/a/b/` | key 含 `/` 与尾斜杠,原样保留 |

- Host 尾缀 `:port` 剥离后匹配(SDK 带端口发 Host 的场景)。
- 桶标校验:非空,字符 ∈ 小写字母/数字/`-`/`.`;不合法 → 放行(路径风格兜底)。
- 裸基础域名(Host == suffix 本身)= 路径风格端点,不命中。
- **admin/metrics 只在裸基础域名**:vhost Host 的 `/admin/*` 改写后落对象路由
  (404 族)—— 管理面不被 vhost 语义破坏。

## 配置(CLAUDE.md §7,入 s3.md 配置表)

- `s3.virtual_host_domain` | String | `""`(关)| 非空时:合法域名形态(小写字母/
  数字/`-`/`.`;无 scheme/端口/前后导点)| vhost Host 匹配后缀;空 = 关(部署行为
  零变化,全套既有 e2e 即其回归)。

## 安全/持久性清单

- 触不到写路径与持久化(纯 HTTP 路由层);鉴权顺序反而**增强**(验签恒先于改写,
  Host 混淆无法绕过签名)。
- 桶标注入安全:改写产物仍是「受既有桶/对象校验的路径段」(桶字符白名单;path 其余
  逐字透传,与等价路径风格请求同字节)。
- 无新监控(路由层行为;`s3_requests_total{op,status}` 族照旧覆盖)。

## 测试口径(夹具学到的)

- **Host 字节一致铁律**:签名的 host 串与实发 Host 头**逐字一致**才可验证
  (portless/portful 自洽皆过 —— SIGNED-RAW 双探针断言:字节级裸 TCP 两种形态各签
  各发均 200);夹具 `vhost_authority` 取 authority 形态(host:port)。
- **reqwest `.header()` 是 append 语义**(经典坑,canonical-diff 双打印实证):
  HeaderMap 已带 host 后再 `.header(HOST, …)` → **双 Host 出线** → 服务器按 SigV4
  规范把多值头逗号连接 → 验签必败。夹具修复:删冗余 `.header("host")` 追加,让
  map 自带的 host 出线。
- 夹具经 `TEST_S3_VHOST_DOMAIN` 注入后缀;其余二进制不设 → 默认关(全套
  fresh/dirty 即「关态零变化」回归)。

## 被否决/学到的形式

- **route_layer 中间件改写 URI(本 change 初版方案)**:axum 路由匹配先于层执行,
  改写不可达路由 —— 否决,学到的机制课记档。
- **按 Host 预解出 bucket 当参数传 handler**:改全部 handler 签名、路径/vhost 双栈
  维护;服务级管线单点零侵入,胜出。
- **tower::service_fn + impl-trait 返回**:serve 边界无法证明 opaque Future: Send;
  **BoxService**:不 Clone(serve 每连接克隆) —— 具名 `GatewayPipeline` 胜出。
- **对非法桶标返回 4xx**:语义说不出桶错还是 Host 错;放行(路径风格兜底)更诚实。

## 修改清单(实现落点)

- `crates/s3/src/config.rs`:`virtual_host_domain` + `is_plain_domain`
- `crates/s3/src/server.rs`:`vhost_rewrite`(纯函数)+ `apply_vhost_rewrite` +
  `GatewayPipeline` + `build_app`;`build_router` 摘除全部 route_layer(纯路由)
- `crates/s3/src/auth/middleware.rs`:`authenticate`/`is_public_path` pub(crate);
  `auth_middleware` 包装删除(管线内联)
- `crates/s3/src/main.rs` + `tests/common/mod.rs`:`build_router` → `build_app` +
  `.into_make_service()`
- `crates/s3/tests/vport.rs`(新二进制):全矩阵 e2e + 匿名探针 + 裸 TCP 双签名探针
- `Cargo.toml`:tower(util 特性)

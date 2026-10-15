# Tasks

## 1. 配置与纯函数

- [x] 1.1 `S3GatewayConfig.virtual_host_domain`(默认 `""`;Validate:非空合法域名形态)
- [x] 1.2 `vhost_rewrite(host, path_and_query, suffix) -> Option<String>`(端口剥离/
     桶标校验放行/根路径去尾斜杠/深 key 与 query 原样)+ 单测矩阵 vhost_rewrite_matrix

## 2. 服务级管线(axum 机制发现后的架构修正)

- [x] 2.1 `GatewayPipeline`(#[derive(Clone)] AppState + Router)impl
     tower::Service<Request>,Future = Pin<Box<dyn Future + Send>>(serve 边界
     Clone/Send 三全:impl-trait 无法证明 Future::Send、BoxService 不 Clone,具名
     结构体胜出)
- [x] 2.2 `build_app` → GatewayPipeline 四步:公共面直发 → authenticate(原
     URI/Host)→ Principal 注入 → apply_vhost_rewrite → router.oneshot
- [x] 2.3 auth/middleware `authenticate`/`is_public_path` pub(crate);
     `auth_middleware` 包装删除(管线内联);main.rs + 夹具 `build_router` →
     `build_app` + `.into_make_service()`;build_router 摘除全部 route_layer
- [x] 2.4 (机制课)route_layer 在路由匹配之后执行 —— 改写不可达路由;裸 TCP 探针
     实证后架构修正为服务级管线(设计档记档)

## 3. 测试(学到的口径 + 坑)

- [x] 3.1 tests/vhost.rs(夹具 `TEST_S3_VHOST_DOMAIN` 后缀注入):全矩阵 e2e(对象
     读写 head 404 族/ListV2/跨租户 403/public-read 匿名 200/带端口 Host/根 create/
     双风格互证)
- [x] 3.2 匿名探针 + **裸 TCP 双签名探针**(SIGNED-RAW portless/portful 各签各发均
     断言 200 —— Host 字节一致铁律记档)
- [x] 3.3 (坑)reqwest `.header()` append 语义:map 已带 host 再追加 → 双 Host →
     canonical-diff 实证多值头逗号连接验签必败 → 删四处冗余追加修平
- [x] 3.4 既有 e2e 全套 fresh/dirty 原样绿(默认关零变化);fmt/clippy 0;lib 51 绿

## 4. 文档与收口

- [x] 4.1 s3.md(矩阵行翻转 ✅ + 配置表行 + 「虚拟主机寻址」节 + D5 + 后续方向重排)
     + README 双语 + DEVLOG 59
- [x] 4.2 push CI 绿 + 归档 `s3-vhost-routing`(推后 CI 硬验收)

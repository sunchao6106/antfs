use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use serde::Deserialize;

use tower::ServiceExt;

use antfs_router::Router as BlobRouter;

use crate::auth::middleware::Principal;
use crate::config::S3GatewayConfig;
use crate::error::S3Error;
use crate::metastore::types::BucketAcl;
use crate::metastore::MetaStore;
use crate::metrics::S3Metrics;

#[derive(Clone)]
pub struct AppState {
    pub router: Arc<BlobRouter>,
    pub metastore: Arc<dyn MetaStore>,
    pub metrics: Arc<S3Metrics>,
    pub registry: antfs_metric::Registry,
    pub config: Arc<S3GatewayConfig>,
    /// `s3-sse` 主钥(None = 未部署 → 带 SSE 头的写 501;读侧行描述符 None 的对象
    /// 永不触此面)。
    pub sse: Option<Arc<crate::sse::MasterKey>>,
}

#[derive(Deserialize, Default)]
pub struct S3QueryParams {
    #[serde(rename = "list-type")]
    pub list_type: Option<String>,
    pub uploads: Option<String>,
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    pub part_number: Option<i32>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    #[serde(rename = "max-keys")]
    pub max_keys: Option<i32>,
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
    #[serde(rename = "start-after")]
    pub start_after: Option<String>,
    #[serde(rename = "max-parts")]
    pub max_parts: Option<i32>,
    #[serde(rename = "part-number-marker")]
    pub part_number_marker: Option<i32>,
    #[serde(rename = "key-marker")]
    pub key_marker: Option<String>,
    #[serde(rename = "upload-id-marker")]
    pub upload_id_marker: Option<String>,
    #[serde(rename = "max-uploads")]
    pub max_uploads: Option<i32>,
    pub delete: Option<String>,
    pub acl: Option<String>,
    pub policy: Option<String>,
    pub marker: Option<String>,
    pub location: Option<String>,
    #[serde(rename = "attributes")]
    pub attributes: Option<String>,
}

/// vhost Host → 改写后的 path+query;`None` = 放行(路径风格路由兜底)。
///
/// 规则(产物与等价路径风格请求逐字节同形,验签不受影响 —— 见中间件注释):
/// - Host 剥离可选 `:port` 后须为 `{bucket}.{suffix}`;**裸基础域名(= suffix 本身)
///   不命中**(那是路径风格端点);空/非法桶标(要求小写字母/数字/`-`/`.`)放行;
/// - `/` → `/{bucket}`(根去尾斜杠恰命中桶路由);`/k` → `/{bucket}/k`;深 key 与
///   尾斜杠逐字保留;query 原样重挂。
fn vhost_rewrite(host: &str, path_and_query: &str, suffix: &str) -> Option<String> {
    let host = host.rsplit_once(':').map_or(host, |(h, _)| h);
    let bucket = host.strip_suffix(suffix)?.strip_suffix('.')?;
    if bucket.is_empty()
        || !bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return None;
    }
    let (path, query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_and_query, None),
    };
    let rel = path.strip_prefix('/')?;
    let new_path = if rel.is_empty() {
        format!("/{bucket}")
    } else {
        format!("/{bucket}/{rel}")
    };
    Some(match query {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path,
    })
}

/// vhost 改写(纯同步助手,由服务级管线 [`build_app`] 在验签后调用):后缀未配置
/// = 关;Host 匹配 `{bucket}.{suffix}` 则把 URI 改写为 `/{bucket}{path}`(规则见
/// [`vhost_rewrite`])。只影响**后续路由分发** —— 调用点保证 auth 已按原
/// URI/原 Host 验签完毕。
fn apply_vhost_rewrite(state: &AppState, req: &mut Request) {
    let suffix = state.config.virtual_host_domain.as_str();
    if suffix.is_empty() {
        return;
    }
    let Some(host) = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
    else {
        return;
    };
    let Some(pq) = req.uri().path_and_query().map(|pq| pq.as_str().to_string()) else {
        return;
    };
    if let Some(rewritten) = vhost_rewrite(&host, &pq, suffix) {
        match rewritten.parse::<Uri>() {
            Ok(uri) => {
                tracing::debug!(host = %host, to = %rewritten, "vhost path rewrite");
                *req.uri_mut() = uri;
            }
            Err(e) => {
                // 理论不可达(桶字符白名单 + 其余逐字透传);防御位:放行原请求。
                tracing::warn!(host = %host, error = %e, "vhost rewrite parse failed");
            }
        }
    }
}

/// 服务级管线(替 route_layer 版本 —— **axum 的路由匹配先于层执行**,URI 改写必须
/// 发生在分发之前才能改命中,route_layer 中改写只对 handler 视图生效、改不了路由):
/// ① `/metrics`、`/admin/*` 公共面直发(路径风格裸域端点,设计语义);② auth 按
/// **原 path/原 Host** 验签(vhost 客户端签名 host 含桶、路径不含桶,与路径风格
/// 两种口径**各自天然成立**),Principal 注入 extensions,验签失败在此回绝;③
/// vhost 改写(配置门控);④ Router 分发 —— handler 提取 `Extension<Principal>`
/// 的方式不变。
#[derive(Clone)]
pub struct GatewayPipeline {
    state: AppState,
    /// `Router<()>`:Clone + Service + Send + Sync(axum 0.8 自带断言);每请求
    /// 克隆一份用 `ServiceExt::oneshot` 分发,poll_ready 恒就绪,无共享可变状态。
    router: Router,
}

impl tower::Service<Request> for GatewayPipeline {
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response, std::convert::Infallible>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request) -> Self::Future {
        let dispatch = self.router.clone();
        let st = self.state.clone();
        Box::pin(async move {
            if crate::auth::middleware::is_public_path(req.uri().path()) {
                return dispatch.oneshot(req).await;
            }
            let method = req.method().as_str().to_string();
            let uri = req
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            let headers = req.headers().clone();

            let principal = match crate::auth::middleware::authenticate(
                st.metastore.as_ref(),
                &st.config.region,
                &method,
                &uri,
                &headers,
            )
            .await
            {
                Ok(Some(resolution)) => {
                    if let Some(info) = resolution.chunked {
                        match crate::auth::chunked::wrap_request(
                            req,
                            info,
                            st.config.max_body_bytes,
                            &st.metrics.chunked_requests_total,
                            &st.metrics.chunked_rejected_total,
                        ) {
                            Ok(wrapped) => req = wrapped,
                            Err(error) => return Ok(error.into_response()),
                        }
                    }
                    Principal::Signed(resolution.context)
                }
                Ok(None) => Principal::Anonymous,
                Err(error) => return Ok(error.into_response()),
            };
            req.extensions_mut().insert(principal);
            apply_vhost_rewrite(&st, &mut req);
            dispatch.oneshot(req).await
        })
    }
}

pub fn build_app(state: AppState) -> GatewayPipeline {
    GatewayPipeline {
        state: state.clone(),
        router: build_router(state),
    }
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(list_buckets_handler))
        .route(
            "/{bucket}",
            put(create_bucket_handler)
                .delete(delete_bucket_handler)
                .head(head_bucket_handler)
                .get(bucket_get_handler)
                .post(bucket_post_handler),
        )
        .route(
            "/{bucket}/{*key}",
            put(object_put_handler)
                .get(object_get_handler)
                .delete(object_delete_handler)
                .head(object_head_handler)
                .post(object_post_handler),
        )
        .route("/admin/credentials", post(create_credential_handler))
        .route(
            "/admin/credentials/{access_key}",
            delete(delete_credential_handler),
        )
        .route(
            "/admin/buckets/{bucket}/quota",
            get(get_bucket_quota_handler).put(set_bucket_quota_handler),
        )
        .route("/metrics", get(metrics_handler))
        // 鉴权与 vhost 改写在服务级管线 [`build_app`] 完成(必须先于本 Router 的
        // 路由匹配 —— axum 的层在匹配之后执行);此处是纯路由 + handler
        // (`Extension<Principal>` 提取方式不变)。
        .with_state(state)
}

/// 私有面门(s3-bucket-acl / s3-canned-acl):bucket-scoped 操作要求**已验证签名**
/// 的请求且凭证 owner == 桶 owner;跨租户/匿名 403 AccessDenied,未知桶 404。
/// CreateBucket/ListBuckets 不设此闸(创建即按凭证 owner 认领名字;列举本就按
/// owner 过滤),但契约要求 Signed(见各 handler)。
pub(crate) async fn require_bucket_owner(
    st: &AppState,
    principal: &Principal,
    bucket: &str,
) -> Result<(), S3Error> {
    match st.metastore.head_bucket(bucket).await? {
        Some(meta) => match principal {
            Principal::Signed(ctx) if ctx.owner == meta.owner => Ok(()),
            Principal::Signed(_) => Err(S3Error::AccessDenied(
                "bucket belongs to another tenant".into(),
            )),
            Principal::Anonymous => Err(S3Error::AccessDenied("authentication required".into())),
        },
        None => Err(S3Error::NoSuchBucket),
    }
}

/// 兜底种类(策略 NotApplicable 时):PublicRead = 读族門的 canned 兜底,OwnerOnly =
/// 原私有语义。同动作同脸(HEAD bucket 与 ListObjects 都是 s3:ListBucket,但兜底
/// 不同——策略评估一致,既有兜底逐位保形)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateFallback {
    PublicRead,
    OwnerOnly,
}

/// 数据面授权门(`s3-bucket-policy`):评估序 = 显式 Deny > policy Allow > 既有兜底。
/// 无策略行 = NotApplicable → 兜底(零行为变化);存坏策略 fail-closed(500,绝不
/// 静默放松);管理面(策略/ACL/location/quota/batch)不走此门,恒 owner。
pub(crate) async fn authorize(
    st: &AppState,
    principal: &Principal,
    bucket: &str,
    key: Option<&str>,
    action: crate::policy::PolicyAction,
    fallback: GateFallback,
) -> Result<(), S3Error> {
    let meta = st
        .metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    let decision = match &meta.policy {
        None => crate::policy::PolicyDecision::NotApplicable,
        Some(raw) => {
            let policy = crate::policy::BucketPolicy::parse_trusted(raw, bucket).map_err(|e| {
                S3Error::InternalError(format!("corrupt stored bucket policy: {e}"))
            })?;
            policy.evaluate(principal_owner(principal), action, bucket, key)
        }
    };
    match decision {
        crate::policy::PolicyDecision::Deny => Err(S3Error::AccessDenied(
            "request is explicitly denied by the bucket policy".into(),
        )),
        crate::policy::PolicyDecision::Allow => Ok(()),
        crate::policy::PolicyDecision::NotApplicable => {
            // ACL grants 腿(`s3-acl-xml`):策略缺位且兜底将拒时才评估——只抬,
            // 永不压显式 Deny/Allow。桶域 grants 抬 ListBucket(AllUsers 走 canned
            // 不在列);对象域 grants 抬 GetObject(懒查行,常态零开销)。
            let would_pass = match principal {
                Principal::Signed(ctx) if ctx.owner == meta.owner => true,
                _ if fallback == GateFallback::PublicRead
                    && matches!(meta.acl, BucketAcl::PublicRead) =>
                {
                    true
                }
                _ => false,
            };
            if !would_pass {
                let owner = principal_owner(principal).map(str::to_string);
                if action == crate::policy::PolicyAction::ListBucket
                    && crate::policy::acl_grants_allow(meta.acl_grants.as_deref(), owner.as_deref())
                {
                    return Ok(());
                }
                if let (crate::policy::PolicyAction::GetObject, Some(k)) = (action, key) {
                    // 匿名主语也走(对象 AllUsers 授予 = 任何请求者)——
                    // acl_grants_allow 内部按具名/AllUsers 分别匹配。
                    let granted = st
                        .metastore
                        .get_object(bucket, k)
                        .await?
                        .map(|m| {
                            crate::policy::acl_grants_allow(
                                m.acl_grants.as_deref(),
                                owner.as_deref(),
                            )
                        })
                        .unwrap_or(false);
                    if granted {
                        return Ok(());
                    }
                }
            }
            fallback_gate(meta, principal, fallback)
        }
    }
}

fn principal_owner(principal: &Principal) -> Option<&str> {
    match principal {
        Principal::Signed(ctx) => Some(&ctx.owner),
        Principal::Anonymous => None,
    }
}

fn fallback_gate(
    meta: crate::metastore::types::BucketMeta,
    principal: &Principal,
    fallback: GateFallback,
) -> Result<(), S3Error> {
    match principal {
        Principal::Signed(ctx) if ctx.owner == meta.owner => Ok(()),
        _ if fallback == GateFallback::PublicRead && meta.acl == BucketAcl::PublicRead => Ok(()),
        Principal::Signed(_) => Err(S3Error::AccessDenied(
            "bucket belongs to another tenant".into(),
        )),
        Principal::Anonymous => Err(S3Error::AccessDenied("authentication required".into())),
    }
}

async fn bucket_get_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    // GET /{bucket}?policy = GetBucketPolicy(管理面恒 owner,策略永不评估策略面)。
    if q.policy.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::get_bucket_policy(st, &bucket)
            .await
            .into_response();
    }
    // GET /{bucket}?acl = GetBucketAcl(owner 专属)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::get_bucket_acl(st, &bucket)
            .await
            .into_response();
    }
    // ListMultipartUploads 属 multipart 会话目录;策略 s3:ListBucketMultipartUploads,
    // 兜底维持 owner 面(public-read 不放开)。
    if q.uploads.is_some() {
        if let Err(e) = authorize(
            &st,
            &principal,
            &bucket,
            None,
            crate::policy::PolicyAction::ListBucketMultipartUploads,
            GateFallback::OwnerOnly,
        )
        .await
        {
            return e.into_response();
        }
        return crate::handlers::multipart::list_multipart_uploads(st, &bucket, q)
            .await
            .into_response();
    }
    // GET /{bucket}?location = GetBucketLocation(租户归属元数据,owner 面,同 ?acl)。
    if q.location.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::get_bucket_location(st, &bucket)
            .await
            .into_response();
    }
    // 列举:策略 s3:ListBucket,兜底 = canned public-read(匿名可列举);
    // 无 `list-type=2` = ListObjectsV1。
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        None,
        crate::policy::PolicyAction::ListBucket,
        GateFallback::PublicRead,
    )
    .await
    {
        return e.into_response();
    }
    if q.list_type.as_deref() == Some("2") {
        crate::handlers::bucket::list_objects_v2(st, &bucket, q)
            .await
            .into_response()
    } else {
        crate::handlers::bucket::list_objects_v1(st, &bucket, q)
            .await
            .into_response()
    }
}

async fn bucket_post_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
        return e.into_response();
    }
    if q.delete.is_some() {
        return crate::handlers::batch::delete_objects(st, &bucket, req)
            .await
            .into_response();
    }
    S3Error::InternalError("unsupported POST operation on bucket".into()).into_response()
}

async fn object_put_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    // PUT /{key}?acl = PutObjectAcl(管理面 owner 独占;全量换置对象 grants,不 bump 版本)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::object::put_object_acl(st, &bucket, &key, req)
            .await
            .into_response();
    }
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        Some(&key),
        crate::policy::PolicyAction::PutObject,
        GateFallback::OwnerOnly,
    )
    .await
    {
        return e.into_response();
    }
    if req.headers().get("x-amz-copy-source").is_some() {
        return crate::handlers::object::copy_object(st, &principal, &bucket, &key, req)
            .await
            .into_response();
    }
    if let (Some(upload_id), Some(part_number)) = (&q.upload_id, q.part_number) {
        return crate::handlers::multipart::upload_part(
            st,
            &bucket,
            &key,
            upload_id,
            part_number,
            req,
        )
        .await
        .into_response();
    }
    crate::handlers::object::put_object(st, &bucket, &key, req)
        .await
        .into_response()
}

async fn object_get_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    // GET /{key}?acl = GetObjectAcl(管理面 owner 独占)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::object::get_object_acl(st, &bucket, &key)
            .await
            .into_response();
    }
    if q.attributes.is_some() {
        // GET object?attributes:读门同 GET object(读面);属性集由请求头定。
        if let Err(e) = authorize(
            &st,
            &principal,
            &bucket,
            Some(&key),
            crate::policy::PolicyAction::GetObject,
            GateFallback::PublicRead,
        )
        .await
        {
            return e.into_response();
        }
        return crate::handlers::object::get_object_attributes(st, &bucket, &key, req.headers())
            .await
            .into_response();
    }
    let upload_id = q.upload_id.clone();
    if let Some(uid) = &upload_id {
        // ListParts 属 multipart 会话目录;策略动作 s3:ListMultipartUploadParts,兜底维持 owner。
        if let Err(e) = authorize(
            &st,
            &principal,
            &bucket,
            Some(&key),
            crate::policy::PolicyAction::ListMultipartUploadParts,
            GateFallback::OwnerOnly,
        )
        .await
        {
            return e.into_response();
        }
        return crate::handlers::multipart::list_parts(st, uid, q)
            .await
            .into_response();
    }
    // GET object:策略 s3:GetObject(对象级资源),兜底 = canned public-read。
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        Some(&key),
        crate::policy::PolicyAction::GetObject,
        GateFallback::PublicRead,
    )
    .await
    {
        return e.into_response();
    }
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok());
    crate::handlers::object::get_object(st, &bucket, &key, range, q.part_number, req.headers())
        .await
        .into_response()
}

async fn object_head_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    // HEAD object:门同 GET object(GetObject 动作,public 兜底;无子资源分派)。
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        Some(&key),
        crate::policy::PolicyAction::GetObject,
        GateFallback::PublicRead,
    )
    .await
    {
        return e.into_response();
    }
    // HEAD 忽略 Range 与 partNumber(S3 语义:眼里只有整对象元数据);条件头生效。
    crate::handlers::object::head_object(st, &bucket, &key, req.headers())
        .await
        .into_response()
}

async fn object_delete_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    if let Some(upload_id) = &q.upload_id {
        if let Err(e) = authorize(
            &st,
            &principal,
            &bucket,
            Some(&key),
            crate::policy::PolicyAction::AbortMultipartUpload,
            GateFallback::OwnerOnly,
        )
        .await
        {
            return e.into_response();
        }
        return crate::handlers::multipart::abort_multipart_upload(st, upload_id)
            .await
            .into_response();
    }
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        Some(&key),
        crate::policy::PolicyAction::DeleteObject,
        GateFallback::OwnerOnly,
    )
    .await
    {
        return e.into_response();
    }
    crate::handlers::object::delete_object(st, &bucket, &key)
        .await
        .into_response()
}

async fn object_post_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    // create-multipart / complete 都是对象写面(PutObject 动作;batch-delete 子资源
    // 不在此路由)。
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        Some(&key),
        crate::policy::PolicyAction::PutObject,
        GateFallback::OwnerOnly,
    )
    .await
    {
        return e.into_response();
    }
    if q.uploads.is_some() {
        return crate::handlers::multipart::create_multipart_upload(st, &bucket, &key, req)
            .await
            .into_response();
    }
    if let Some(upload_id) = &q.upload_id {
        return crate::handlers::multipart::complete_multipart_upload(
            st, &bucket, &key, upload_id, req,
        )
        .await
        .into_response();
    }
    if q.delete.is_some() {
        return crate::handlers::batch::delete_objects(st, &bucket, req)
            .await
            .into_response();
    }
    S3Error::InternalError("unsupported POST operation".into()).into_response()
}

async fn list_buckets_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Response {
    // 无凭证的匿名主语列举桶清单 = 无意义(本就按 owner 过滤),403。
    match principal {
        Principal::Signed(ctx) => crate::handlers::bucket::list_buckets(st, &ctx.owner)
            .await
            .into_response(),
        Principal::Anonymous => {
            S3Error::AccessDenied("authentication required".into()).into_response()
        }
    }
}

async fn create_bucket_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    let headers = req.headers().clone();
    // PUT /{bucket}?policy = PutBucketPolicy(管理面恒 owner,body = JSON 策略)。
    if q.policy.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::put_bucket_policy(st, &bucket, req)
            .await
            .into_response();
    }
    // PUT /{bucket}?acl = PutBucketAcl(owner 专属,canned 头形式;XML body 形式 501)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::put_bucket_acl(st, &bucket, req, headers)
            .await
            .into_response();
    }
    // CreateBucket 要求签名:创建即按凭证 owner 认领名字,匿名认领 = 霸名攻击面。
    let Principal::Signed(ctx) = &principal else {
        return S3Error::AccessDenied("authentication required".into()).into_response();
    };
    let canned = headers
        .get("x-amz-acl")
        .and_then(|value| value.to_str().ok());
    crate::handlers::bucket::create_bucket(st, &bucket, &ctx.owner, canned)
        .await
        .into_response()
}

async fn delete_bucket_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    // DELETE /{bucket}?policy(管理面恒 owner;幂等,无策略也 204)。
    if q.policy.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::delete_bucket_policy(st, &bucket)
            .await
            .into_response();
    }
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
        return e.into_response();
    }
    crate::handlers::bucket::delete_bucket(st, &bucket)
        .await
        .into_response()
}

async fn head_bucket_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
) -> Response {
    // HEAD bucket = s3:ListBucket(策略可授予跨租户;兜底维持原 owner 面)。
    if let Err(e) = authorize(
        &st,
        &principal,
        &bucket,
        None,
        crate::policy::PolicyAction::ListBucket,
        GateFallback::OwnerOnly,
    )
    .await
    {
        return e.into_response();
    }
    crate::handlers::bucket::head_bucket(st, &bucket)
        .await
        .into_response()
}

async fn create_credential_handler(State(st): State<AppState>, req: Request) -> Response {
    crate::handlers::admin::create_credential(st, req)
        .await
        .into_response()
}

async fn delete_credential_handler(
    State(st): State<AppState>,
    Path(access_key): Path<String>,
) -> Response {
    crate::handlers::admin::delete_credential(st, &access_key)
        .await
        .into_response()
}

async fn get_bucket_quota_handler(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
    req: Request,
) -> Response {
    crate::handlers::admin::get_bucket_quota(st, &bucket, req)
        .await
        .into_response()
}

async fn set_bucket_quota_handler(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
    req: Request,
) -> Response {
    crate::handlers::admin::set_bucket_quota(st, &bucket, req)
        .await
        .into_response()
}

async fn metrics_handler(State(st): State<AppState>) -> Response {
    match antfs_metric::gather(&st.registry) {
        Ok(text) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            text,
        )
            .into_response(),
        Err(e) => S3Error::InternalError(format!("metrics: {e}")).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::vhost_rewrite;

    #[test]
    fn vhost_rewrite_matrix() {
        let s = "s3.example.test";
        // 命中:基本形态 / 端口 / 根路径(去尾斜杠)/ ListV2 根 query / 深 key 尾斜杠。
        assert_eq!(
            vhost_rewrite("b.s3.example.test", "/k", s).as_deref(),
            Some("/b/k")
        );
        assert_eq!(
            vhost_rewrite("b.s3.example.test:7500", "/k?uploads", s).as_deref(),
            Some("/b/k?uploads")
        );
        assert_eq!(
            vhost_rewrite("b.s3.example.test", "/", s).as_deref(),
            Some("/b")
        );
        assert_eq!(
            vhost_rewrite("b.s3.example.test", "/?list-type=2", s).as_deref(),
            Some("/b?list-type=2")
        );
        assert_eq!(
            vhost_rewrite("b.s3.example.test", "/a/b/c/", s).as_deref(),
            Some("/b/a/b/c/")
        );
        // 放行:裸基础域名 / 无关 Host / 空桶标 / 非法桶标 / 后缀未配置。
        assert_eq!(vhost_rewrite("s3.example.test", "/k", s), None);
        assert_eq!(vhost_rewrite("other.example.com", "/k", s), None);
        assert_eq!(vhost_rewrite(".s3.example.test", "/k", s), None);
        assert_eq!(vhost_rewrite("Bad_Bucket.s3.example.test", "/k", s), None);
        assert_eq!(vhost_rewrite("b.s3.example.test", "/k", ""), None);
    }
}

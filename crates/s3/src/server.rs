use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use serde::Deserialize;

use antfs_router::Router as BlobRouter;

use crate::auth::middleware::{auth_middleware, Principal};
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
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        .with_state(state)
}

/// 私有面门(s3-bucket-acl / s3-canned-acl):bucket-scoped 操作要求**已验证签名**
/// 的请求且凭证 owner == 桶 owner;跨租户/匿名 403 AccessDenied,未知桶 404。
/// CreateBucket/ListBuckets 不设此闸(创建即按凭证 owner 认领名字;列举本就按
/// owner 过滤),但契约要求 Signed(见各 handler)。
async fn require_bucket_owner(
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

/// 公开读门(s3-canned-acl):读面(GET/HEAD object、ListObjectsV2)通行证 =
/// 桶存在且(acl = public-read,或请求为桶 owner 的签名请求)。public-read 对
/// 匿名/跨租户/owner 一视同仁放行;私有桶维持首轮语义(owner 独占)。
async fn require_public_read(
    st: &AppState,
    principal: &Principal,
    bucket: &str,
) -> Result<(), S3Error> {
    match st.metastore.head_bucket(bucket).await? {
        None => Err(S3Error::NoSuchBucket),
        Some(meta) if meta.acl == BucketAcl::PublicRead => Ok(()),
        Some(meta) => match principal {
            Principal::Signed(ctx) if ctx.owner == meta.owner => Ok(()),
            _ => Err(S3Error::AccessDenied(
                "bucket belongs to another tenant".into(),
            )),
        },
    }
}

async fn bucket_get_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    // GET /{bucket}?acl = GetBucketAcl(owner 专属)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::get_bucket_acl(st, &bucket)
            .await
            .into_response();
    }
    // ListMultipartUploads 属 multipart 会话目录,维持 owner 面(public-read 不放开)。
    if q.uploads.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::multipart::list_multipart_uploads(st, &bucket, q)
            .await
            .into_response();
    }
    // ListObjectsV2 走公开读门(public-read 桶匿名可列举)。
    if let Err(e) = require_public_read(&st, &principal, &bucket).await {
        return e.into_response();
    }
    crate::handlers::bucket::list_objects_v2(st, &bucket, q)
        .await
        .into_response()
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
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
        return e.into_response();
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
) -> Response {
    let upload_id = q.upload_id.clone();
    if let Some(uid) = &upload_id {
        // ListParts 属 multipart 会话目录,维持 owner 面。
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::multipart::list_parts(st, uid, q)
            .await
            .into_response();
    }
    // GET object 走公开读门(public-read 桶匿名可读)。
    if let Err(e) = require_public_read(&st, &principal, &bucket).await {
        return e.into_response();
    }
    crate::handlers::object::get_object(st, &bucket, &key)
        .await
        .into_response()
}

async fn object_head_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    // HEAD object 走公开读门(无子资源分派)。
    if let Err(e) = require_public_read(&st, &principal, &bucket).await {
        return e.into_response();
    }
    crate::handlers::object::head_object(st, &bucket, &key)
        .await
        .into_response()
}

async fn object_delete_handler(
    State(st): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
        return e.into_response();
    }
    if let Some(upload_id) = &q.upload_id {
        return crate::handlers::multipart::abort_multipart_upload(st, upload_id)
            .await
            .into_response();
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
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
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
    headers: HeaderMap,
) -> Response {
    // PUT /{bucket}?acl = PutBucketAcl(owner 专属,canned 头形式;XML body 形式 501)。
    if q.acl.is_some() {
        if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
            return e.into_response();
        }
        return crate::handlers::bucket::put_bucket_acl(st, &bucket, &headers)
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
) -> Response {
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
    if let Err(e) = require_bucket_owner(&st, &principal, &bucket).await {
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

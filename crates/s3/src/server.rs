use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use serde::Deserialize;

use antfs_router::Router as BlobRouter;

use crate::auth::middleware::{auth_middleware, AuthContext};
use crate::config::S3GatewayConfig;
use crate::error::S3Error;
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

async fn bucket_get_handler(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    let is_uploads = q.uploads.is_some();
    if is_uploads {
        return crate::handlers::multipart::list_multipart_uploads(st, &bucket, q)
            .await
            .into_response();
    }
    crate::handlers::bucket::list_objects_v2(st, &bucket, q)
        .await
        .into_response()
}

async fn bucket_post_handler(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
    if q.delete.is_some() {
        return crate::handlers::batch::delete_objects(st, &bucket, req)
            .await
            .into_response();
    }
    S3Error::InternalError("unsupported POST operation on bucket".into()).into_response()
}

async fn object_put_handler(
    State(st): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
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
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
) -> Response {
    let upload_id = q.upload_id.clone();
    if let Some(uid) = &upload_id {
        return crate::handlers::multipart::list_parts(st, uid, q)
            .await
            .into_response();
    }
    crate::handlers::object::get_object(st, &bucket, &key)
        .await
        .into_response()
}

async fn object_head_handler(
    State(st): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    crate::handlers::object::head_object(st, &bucket, &key)
        .await
        .into_response()
}

async fn object_delete_handler(
    State(st): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
) -> Response {
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
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<S3QueryParams>,
    req: Request,
) -> Response {
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
    Extension(ctx): Extension<AuthContext>,
) -> Response {
    crate::handlers::bucket::list_buckets(st, &ctx.owner)
        .await
        .into_response()
}

async fn create_bucket_handler(
    State(st): State<AppState>,
    Extension(ctx): Extension<AuthContext>,
    Path(bucket): Path<String>,
) -> Response {
    crate::handlers::bucket::create_bucket(st, &bucket, &ctx.owner)
        .await
        .into_response()
}

async fn delete_bucket_handler(State(st): State<AppState>, Path(bucket): Path<String>) -> Response {
    crate::handlers::bucket::delete_bucket(st, &bucket)
        .await
        .into_response()
}

async fn head_bucket_handler(State(st): State<AppState>, Path(bucket): Path<String>) -> Response {
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

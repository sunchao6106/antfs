use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use antfs_router::NO_EXPIRATION;

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::AppState;

/// `Content-Type` 存储默认(AWS 语义:上传未带即按此类型存储,GET 原样回吐)。
const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";
/// `x-amz-meta-*` 总上限(Σ 键+值 UTF-8 字节;S3 语义官方值,不做成配置)。
const MAX_USER_META_BYTES: usize = 2048;
/// `Content-Type` 最大长度,对齐 MetaStore `VARCHAR(256)` 列宽(再长应 400,而非落库时 500)。
const MAX_CONTENT_TYPE_BYTES: usize = 256;

/// 从请求头采集对象元数据(PutObject 与 CreateMultipart 共用),含 S3 语义边界校验:
/// 无 `Content-Type` → 存 `binary/octet-stream`;超长 → `InvalidArgument`;
/// meta 总字节超 2048 → `MetadataTooLarge`。元数据只进 MetaStore,与 blob 引擎隔离。
pub(crate) fn capture_object_meta(headers: &HeaderMap) -> Result<(String, UserMeta), S3Error> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut user_meta = UserMeta::new();
    for (name, value) in headers {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                user_meta.insert(meta_key.to_string(), v.to_string());
            }
        }
    }

    if let Some(ct) = content_type.as_deref() {
        if ct.len() > MAX_CONTENT_TYPE_BYTES {
            return Err(S3Error::InvalidArgument(format!(
                "Content-Type header exceeds {MAX_CONTENT_TYPE_BYTES} bytes"
            )));
        }
    }
    let meta_bytes: usize = user_meta.iter().map(|(k, v)| k.len() + v.len()).sum();
    if meta_bytes > MAX_USER_META_BYTES {
        return Err(S3Error::MetadataTooLarge);
    }

    let content_type = content_type.unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string());
    Ok((content_type, user_meta))
}

pub async fn put_object(
    st: AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let (content_type, user_meta) = capture_object_meta(req.headers())?;

    let body_bytes = crate::handlers::read_bounded_body(&st, req).await?;

    let etag = {
        use md5::{Digest, Md5};
        let hash = Md5::digest(&body_bytes);
        format!("{:x}", hash)
    };

    let size = body_bytes.len() as u64;
    let (blob_key, _report) = st
        .router
        .put_auto_with_ttl(body_bytes, NO_EXPIRATION)
        .await?;

    let outcome = match st
        .metastore
        .put_object(PutObjectMeta {
            bucket: bucket.to_string(),
            key: key.to_string(),
            blob_id: blob_key,
            size,
            etag: etag.clone(),
            content_type: Some(content_type),
            user_meta,
            parts_manifest: None,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(e @ S3Error::QuotaExceeded { .. }) => {
            // 死点模型:配额拒绝 → 本次已写的 blob 同请求墓碑(幂等),再回 507。
            if let Err(de) = st.router.delete(&blob_key).await {
                tracing::warn!(error = %de, "s3 quota: tombstone of rejected write failed (blob leaked until manual cleanup)");
            }
            st.metrics
                .quota_rejected_total
                .with_label_values(&[bucket])
                .inc();
            return Err(e);
        }
        Err(e) => return Err(e),
    };

    // overwrite 遮蔽的旧版本即刻回收(引擎 delete 幂等;失败只降级为泄漏计数,不回滚新版本)。
    for old in &outcome.shadowed_blobs {
        if let Err(e) = st.router.delete(old).await {
            tracing::warn!(error = %e, "s3 gc: shadowed blob delete failed(old version leaked until manual cleanup)");
        }
    }

    Ok((StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))], "").into_response())
}

pub async fn get_object(st: AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    if let Some(manifest_json) = &meta.parts_manifest {
        return get_multipart_object(st, &meta, manifest_json).await;
    }

    let data = st.router.get(&meta.blob_id).await?;
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, meta.size.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag));
    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    }
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{k}"), v.as_str());
    }
    Ok(builder.body(Body::from(data)).unwrap())
}

async fn get_multipart_object(
    st: AppState,
    meta: &ObjectMeta,
    manifest_json: &str,
) -> Result<Response, S3Error> {
    let parts: Vec<ManifestPart> = serde_json::from_str(manifest_json)
        .map_err(|e| S3Error::InternalError(format!("corrupt manifest: {e}")))?;

    let mut all_data = Vec::new();
    for part in &parts {
        let blob_id = antfs_blob::StoreKey::from_slice(&part.blob_id)
            .ok_or_else(|| S3Error::InternalError("bad blob_id in manifest".into()))?;
        let chunk = st.router.get(&blob_id).await?;
        all_data.extend_from_slice(&chunk);
    }

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, meta.size.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag));
    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    }
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{k}"), v.as_str());
    }
    Ok(builder.body(Body::from(all_data)).unwrap())
}

pub async fn head_object(st: AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_LENGTH, meta.size.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag));
    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    }
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{k}"), v.as_str());
    }
    Ok(builder.body(Body::empty()).unwrap())
}

pub async fn delete_object(st: AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    if let Some(deleted) = st.metastore.delete_object(bucket, key).await? {
        let _ = st.router.delete(&deleted.blob_id).await;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

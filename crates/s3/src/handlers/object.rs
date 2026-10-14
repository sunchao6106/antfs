use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use antfs_router::NO_EXPIRATION;

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::AppState;

pub async fn put_object(
    st: AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut user_meta = UserMeta::new();
    for (name, value) in req.headers() {
        if let Some(meta_key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                user_meta.insert(meta_key.to_string(), v.to_string());
            }
        }
    }

    let content_length: Option<u64> = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    let body_bytes = axum::body::to_bytes(req.into_body(), st.config.max_body_bytes as usize)
        .await
        .map_err(|_| S3Error::EntityTooLarge)?;

    if let Some(cl) = content_length {
        if body_bytes.len() as u64 != cl {
            return Err(S3Error::InternalError(format!(
                "body size {} != Content-Length {cl}",
                body_bytes.len()
            )));
        }
    }

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

    st.metastore
        .put_object(PutObjectMeta {
            bucket: bucket.to_string(),
            key: key.to_string(),
            blob_id: blob_key,
            size,
            etag: etag.clone(),
            content_type,
            user_meta,
            parts_manifest: None,
        })
        .await?;

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

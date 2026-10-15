use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use antfs_router::NO_EXPIRATION;

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::{AppState, S3QueryParams};

pub async fn create_multipart_upload(
    st: AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let (content_type, user_meta) = crate::handlers::object::capture_object_meta(req.headers())?;

    let upload_id = st
        .metastore
        .create_multipart(bucket, key, Some(content_type.as_str()), &user_meta)
        .await?;

    let xml = crate::xml::serialize_initiate_multipart(bucket, key, &upload_id);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

pub async fn upload_part(
    st: AppState,
    _bucket: &str,
    _key: &str,
    upload_id: &str,
    part_number: i32,
    req: Request,
) -> Result<Response, S3Error> {
    let body_bytes = axum::body::to_bytes(req.into_body(), st.config.max_body_bytes as usize)
        .await
        .map_err(|_| S3Error::EntityTooLarge)?;

    let etag = {
        use md5::{Digest, Md5};
        format!("{:x}", Md5::digest(&body_bytes))
    };
    let size = body_bytes.len() as u64;

    let (blob_key, _) = st
        .router
        .put_auto_with_ttl(body_bytes, NO_EXPIRATION)
        .await?;

    let old_blob = st
        .metastore
        .put_part(
            upload_id,
            PartMeta {
                part_number,
                blob_id: blob_key,
                size,
                etag: etag.clone(),
            },
        )
        .await?;

    if let Some(old) = old_blob {
        let _ = st.router.delete(&old).await;
    }

    Ok((StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))], "").into_response())
}

pub async fn complete_multipart_upload(
    st: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .map_err(|_| S3Error::MalformedXML("body too large".into()))?;

    let xml_str =
        std::str::from_utf8(&body).map_err(|_| S3Error::MalformedXML("invalid utf-8".into()))?;

    let parts = crate::xml::parse_complete_multipart(xml_str)?;

    let result = match st.metastore.complete_multipart(upload_id, &parts).await {
        Ok(result) => result,
        Err(e @ S3Error::QuotaExceeded { .. }) => {
            // parts 不计账、session 未清:调大配额后客户端原样重发 complete 即可重试。
            st.metrics
                .quota_rejected_total
                .with_label_values(&[bucket])
                .inc();
            return Err(e);
        }
        Err(e) => return Err(e),
    };

    // 被本次 complete 遮蔽的旧版本即刻回收(同 put_object 语义,幂等)。
    for old in &result.shadowed_blobs {
        if let Err(e) = st.router.delete(old).await {
            tracing::warn!(error = %e, "s3 gc: shadowed blob delete failed after multipart complete");
        }
    }

    let xml = crate::xml::serialize_complete_multipart(bucket, key, &result.etag);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

pub async fn abort_multipart_upload(st: AppState, upload_id: &str) -> Result<Response, S3Error> {
    let blob_ids = st.metastore.abort_multipart(upload_id).await?;
    for bid in blob_ids {
        let _ = st.router.delete(&bid).await;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn list_parts(
    st: AppState,
    upload_id: &str,
    q: S3QueryParams,
) -> Result<Response, S3Error> {
    let opts = ListPartsOpts {
        max_parts: q.max_parts.unwrap_or(1000),
        part_number_marker: q.part_number_marker.unwrap_or(0),
    };
    let resp = st.metastore.list_parts(upload_id, opts).await?;
    let xml = crate::xml::serialize_list_parts(upload_id, &resp);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

pub async fn list_multipart_uploads(
    st: AppState,
    bucket: &str,
    q: S3QueryParams,
) -> Result<Response, S3Error> {
    let opts = ListUploadsOpts {
        prefix: q.prefix.clone(),
        key_marker: q.key_marker.clone(),
        upload_id_marker: q.upload_id_marker.clone(),
        max_uploads: q.max_uploads.unwrap_or(1000),
    };
    let resp = st.metastore.list_multipart_uploads(bucket, opts).await?;
    let xml = crate::xml::serialize_list_multipart_uploads(bucket, &resp);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

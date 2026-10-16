use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use antfs_router::NO_EXPIRATION;

use super::range::{
    manifest_total_size, parse_range, window_parts, ByteWindow, PartStream, RangeOutcome,
};
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
    let sse_requested = parse_sse_request_header(req.headers(), &st.sse)?;

    let body_bytes = crate::handlers::read_bounded_body(&st, req).await?;

    let etag = {
        use md5::{Digest, Md5};
        let hash = Md5::digest(&body_bytes);
        format!("{:x}", hash)
    };

    let size = body_bytes.len() as u64;
    let (blob_key, _report) = match (&sse_requested, &st.sse) {
        (Some(master), _) => {
            st.router
                .put_auto_with_ttl(
                    Bytes::from(crate::sse::frame_object(master, &body_bytes)?),
                    NO_EXPIRATION,
                )
                .await?
        }
        _ => {
            st.router
                .put_auto_with_ttl(body_bytes, NO_EXPIRATION)
                .await?
        }
    };
    // 持久化语义:size/etag 恒按**明文**(配额与 ETag 口径不变);帧只影响 blob 字节。
    // 崩溃一致性沿用既有弧:blob 先落、行后写,中途夭折 = 孤儿 blob,GC 兜底(sse 未变)。
    let put_sse = sse_requested
        .as_ref()
        .map(|master| crate::sse::descriptor(master.key_id()));

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
            sse: put_sse,
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

    let mut response =
        (StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))], "").into_response();
    if let Some(master) = &sse_requested {
        response.headers_mut().insert(
            "x-amz-server-side-encryption",
            "AES256".parse().expect("static header value"),
        );
        let _ = master;
    }
    Ok(response)
}

/// SSE 请求头裁决:值须 AES256(其余 400);网关未配主钥(= 能力未部署)→ 501;
/// 返回可用的主钥引用(未请求 → `None`,不碰加密面)。
fn parse_sse_request_header(
    headers: &HeaderMap,
    configured: &Option<std::sync::Arc<crate::sse::MasterKey>>,
) -> Result<Option<std::sync::Arc<crate::sse::MasterKey>>, S3Error> {
    let value = headers
        .get("x-amz-server-side-encryption")
        .and_then(|v| v.to_str().ok());
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.eq_ignore_ascii_case("AES256") {
        return Err(S3Error::InvalidArgument(format!(
            "unsupported x-amz-server-side-encryption value `{value}` (only AES256)"
        )));
    }
    match configured {
        Some(master) => Ok(Some(master.clone())),
        None => Err(S3Error::NotImplemented(
            "server-side encryption is not configured on this gateway (s3.sse.master_key_file)"
                .into(),
        )),
    }
}

pub async fn get_object(
    st: AppState,
    bucket: &str,
    key: &str,
    range_header: Option<&str>,
) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    let window = match parse_range(range_header, meta.size) {
        RangeOutcome::Full => None,
        RangeOutcome::Partial(w) => Some(w),
        RangeOutcome::Unsatisfiable { spec } => {
            return Err(S3Error::InvalidRange {
                requested: spec,
                actual_size: meta.size,
            })
        }
    };

    if meta.sse.is_some() {
        return get_encrypted_object(&st, &meta, window).await;
    }
    if let Some(manifest_json) = &meta.parts_manifest {
        return get_multipart_object(st, &meta, manifest_json, window).await;
    }
    get_plain_object(&st, &meta, window).await
}

/// GET 读响应头装配(三路径共用):恒回 `Accept-Ranges: bytes`,窗口 → 206 +
/// `Content-Range`(S3 语义:leader 对象元数据头照回)。
fn object_read_builder(
    meta: &ObjectMeta,
    sse: bool,
    body_len: u64,
    window: Option<ByteWindow>,
) -> axum::http::response::Builder {
    let status = if window.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let mut builder = Response::builder()
        .status(status)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, body_len.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag));
    if sse {
        builder = builder.header("x-amz-server-side-encryption", "AES256");
    }
    if let Some(w) = window {
        builder = builder.header(header::CONTENT_RANGE, w.content_range(meta.size));
    }
    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    }
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{k}"), v.as_str());
    }
    builder
}

fn response_build_error(e: axum::http::Error) -> S3Error {
    S3Error::InternalError(format!("response build: {e}"))
}

/// 缓冲体切片:先对账(体长 == 行 size,Content-Length 不说谎),再切窗口。
fn apply_window(
    data: Bytes,
    meta: &ObjectMeta,
    window: Option<ByteWindow>,
) -> Result<Bytes, S3Error> {
    if data.len() as u64 != meta.size {
        return Err(S3Error::InternalError(format!(
            "blob size {} != row size {} (bucket {}, key {})",
            data.len(),
            meta.size,
            meta.bucket,
            meta.key
        )));
    }
    let Some(w) = window else { return Ok(data) };
    let start = w.start as usize;
    let end = start + w.len as usize;
    if end > data.len() {
        return Err(S3Error::InternalError(
            "range window crosses past the object body (invariant broken)".into(),
        ));
    }
    Ok(data.slice(start..end))
}

async fn get_plain_object(
    st: &AppState,
    meta: &ObjectMeta,
    window: Option<ByteWindow>,
) -> Result<Response, S3Error> {
    let data = st.router.get(&meta.blob_id).await?;
    let body = apply_window(data, meta, window)?;
    let len = body.len() as u64;
    object_read_builder(meta, false, len, window)
        .body(Body::from(body))
        .map_err(response_build_error)
}

async fn get_multipart_object(
    st: AppState,
    meta: &ObjectMeta,
    manifest_json: &str,
    window: Option<ByteWindow>,
) -> Result<Response, S3Error> {
    let parts: Vec<ManifestPart> = serde_json::from_str(manifest_json)
        .map_err(|e| S3Error::InternalError(format!("corrupt manifest: {e}")))?;
    let total = manifest_total_size(&parts);
    if total != meta.size {
        return Err(S3Error::InternalError(format!(
            "manifest parts total {total} != row size {} (bucket {}, key {})",
            meta.size, meta.bucket, meta.key
        )));
    }
    let effective = window.unwrap_or(ByteWindow {
        start: 0,
        len: total,
    });
    let covering = window_parts(&parts, effective)?;
    let stream = PartStream::new(st, covering);
    object_read_builder(meta, false, effective.len, window)
        .body(Body::from_stream(stream))
        .map_err(response_build_error)
}

/// SSE 对象读:blob = 帧 → 明文(解帧失败 = InternalError,响亮不吞)。解密仍走
/// 缓冲路径(单 blob ≤ `max_body_bytes` 不变量),窗口在明文上切片。
async fn get_encrypted_object(
    st: &AppState,
    meta: &ObjectMeta,
    window: Option<ByteWindow>,
) -> Result<Response, S3Error> {
    let master = st.sse.as_ref().ok_or_else(|| {
        S3Error::InternalError(
            "row is encrypted but the gateway has no SSE master key configured".into(),
        )
    })?;
    let descriptor = crate::sse::parse_descriptor(meta.sse.as_deref().unwrap_or_default())
        .ok_or_else(|| S3Error::InternalError("corrupt sse descriptor".into()))?;
    if descriptor.alg != "AES256" {
        return Err(S3Error::InternalError(format!(
            "unknown sse algorithm `{}`",
            descriptor.alg
        )));
    }
    let frame = st.router.get(&meta.blob_id).await?;
    let plain = crate::sse::read_object_frame(master, frame.as_ref(), &descriptor.key_id)?;
    let body = apply_window(Bytes::from(plain), meta, window)?;
    let len = body.len() as u64;
    object_read_builder(meta, true, len, window)
        .body(Body::from(body))
        .map_err(response_build_error)
}

pub async fn head_object(st: AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, meta.size.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag));
    if meta.sse.is_some() {
        builder = builder.header("x-amz-server-side-encryption", "AES256");
    }
    if let Some(ct) = &meta.content_type {
        builder = builder.header(header::CONTENT_TYPE, ct.as_str());
    }
    for (k, v) in &meta.user_meta {
        builder = builder.header(format!("x-amz-meta-{k}"), v.as_str());
    }
    builder.body(Body::empty()).map_err(response_build_error)
}

pub async fn delete_object(st: AppState, bucket: &str, key: &str) -> Result<Response, S3Error> {
    if let Some(deleted) = st.metastore.delete_object(bucket, key).await? {
        let _ = st.router.delete(&deleted.blob_id).await;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

// ── CopyObject / GetObjectAttributes (s3-copy-attributes) ────────────────────

fn md5_hex(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    format!("{:x}", Md5::digest(data))
}

fn parse_copy_source(value: &str) -> Result<(String, String), S3Error> {
    let (path, query) = match value.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (value, None),
    };
    if let Some(q) = query {
        if !q.is_empty() {
            return Err(S3Error::NotImplemented(format!(
                "copy-source query `{q}` (versioning subresource) is permanently out of scope"
            )));
        }
    }
    let decoded = percent_encoding::percent_decode_str(path)
        .decode_utf8_lossy()
        .into_owned();
    let stripped = decoded.strip_prefix('/').unwrap_or(&decoded);
    let (bucket, key) = stripped
        .split_once('/')
        .ok_or_else(|| S3Error::InvalidArgument("copy-source must be bucket/key".into()))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(S3Error::InvalidArgument(
            "copy-source bucket and key must be non-empty".into(),
        ));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn strip_etag_quotes(value: &str) -> &str {
    value.trim().trim_matches('"')
}

fn parse_http_date_ms(value: &str) -> Result<i64, S3Error> {
    chrono::DateTime::parse_from_rfc2822(value.trim())
        .map(|dt| dt.timestamp_millis())
        .map_err(|_| {
            S3Error::InvalidArgument(format!("malformed HTTP date in copy condition: {value}"))
        })
}

fn http_date_from_ms(epoch_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms)
        .unwrap_or_else(chrono::Utc::now)
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

fn evaluate_copy_conditions(meta: &ObjectMeta, headers: &HeaderMap) -> Result<(), S3Error> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(expected) = header("x-amz-copy-source-if-match") {
        if strip_etag_quotes(expected) != meta.etag {
            return Err(S3Error::PreconditionFailed);
        }
        return Ok(());
    }
    if let Some(not) = header("x-amz-copy-source-if-none-match") {
        let not = not.trim();
        if not == "*" || strip_etag_quotes(not) == meta.etag {
            return Err(S3Error::PreconditionFailed);
        }
        return Ok(());
    }
    if let Some(since) = header("x-amz-copy-source-if-unmodified-since") {
        if meta.created_at > parse_http_date_ms(since)? {
            return Err(S3Error::PreconditionFailed);
        }
        return Ok(());
    }
    if let Some(since) = header("x-amz-copy-source-if-modified-since") {
        if meta.created_at <= parse_http_date_ms(since)? {
            return Err(S3Error::PreconditionFailed);
        }
    }
    Ok(())
}

pub async fn copy_object(
    st: AppState,
    principal: &crate::auth::middleware::Principal,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let headers = req.headers().clone();
    let copy_source = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::InvalidArgument("missing x-amz-copy-source".into()))?;
    if let Some(len) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        if len > 0 {
            return Err(S3Error::InvalidArgument(
                "a copy request must not carry a body".into(),
            ));
        }
    }

    let (src_bucket, src_key) = parse_copy_source(copy_source)?;
    crate::server::require_public_read(&st, principal, &src_bucket).await?;
    let src_meta = st
        .metastore
        .get_object(&src_bucket, &src_key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;
    let directive = headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("COPY")
        .to_ascii_uppercase();
    if !matches!(directive.as_str(), "COPY" | "REPLACE") {
        return Err(S3Error::InvalidArgument(format!(
            "invalid x-amz-metadata-directive `{directive}` (COPY | REPLACE)"
        )));
    }
    if src_bucket == bucket && src_key == key && directive == "COPY" {
        return Err(S3Error::InvalidRequest(
            "copy onto itself with the COPY directive (no metadata change)".into(),
        ));
    }
    evaluate_copy_conditions(&src_meta, &headers)?;
    let sse_request = parse_sse_request_header(&headers, &st.sse)?;

    let (content_type, user_meta) = if directive == "REPLACE" {
        let (ct, meta) = capture_object_meta(&headers)?;
        (Some(ct), meta)
    } else {
        (src_meta.content_type.clone(), src_meta.user_meta.clone())
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let etag = match &src_meta.parts_manifest {
        Some(manifest_json) => {
            if sse_request.is_some() {
                return Err(S3Error::NotImplemented(
                    "multipart objects do not support server-side encryption this round".into(),
                ));
            }
            let parts: Vec<ManifestPart> = serde_json::from_str(manifest_json)
                .map_err(|e| S3Error::InternalError(format!("corrupt manifest: {e}")))?;
            copy_via_multipart(st.clone(), bucket, key, &parts, &content_type, &user_meta).await?
        }
        None => {
            copy_single_part(
                st,
                bucket,
                key,
                &src_meta,
                &content_type,
                &user_meta,
                &sse_request,
            )
            .await?
        }
    };
    let xml = crate::xml::serialize_copy_object_result(&etag, now_ms);
    let mut response = (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
    if sse_request.is_some() {
        response.headers_mut().insert(
            "x-amz-server-side-encryption",
            "AES256".parse().expect("static"),
        );
    }
    Ok(response)
}

fn decrypt_source_bytes(st: &AppState, meta: &ObjectMeta, raw: &[u8]) -> Result<Vec<u8>, S3Error> {
    let Some(descriptor) = meta.sse.as_deref().and_then(crate::sse::parse_descriptor) else {
        return Ok(raw.to_vec());
    };
    let master = st.sse.as_ref().ok_or_else(|| {
        S3Error::InternalError(
            "row is encrypted but the gateway has no SSE master key configured".into(),
        )
    })?;
    crate::sse::read_object_frame(master, raw, &descriptor.key_id)
}

async fn copy_single_part(
    st: AppState,
    bucket: &str,
    key: &str,
    src_meta: &ObjectMeta,
    content_type: &Option<String>,
    user_meta: &UserMeta,
    sse: &Option<std::sync::Arc<crate::sse::MasterKey>>,
) -> Result<String, S3Error> {
    let raw = st.router.get(&src_meta.blob_id).await?;
    // 加密源:engine 读回的是帧 → 解出明文再走重钥逻辑(拷贝按明文重算 ETag/重定加密态)。
    let data = Bytes::from(decrypt_source_bytes(&st, src_meta, raw.as_ref())?);
    let etag = md5_hex(&data);
    let size = data.len() as u64;
    let blob_id = match sse {
        Some(master) => {
            let framing_payload = Bytes::from(crate::sse::frame_object(master, &data)?);
            st.router
                .put_auto_with_ttl(framing_payload, NO_EXPIRATION)
                .await?
                .0
        }
        None => st.router.put_auto_with_ttl(data, NO_EXPIRATION).await?.0,
    };
    let outcome = st
        .metastore
        .put_object(PutObjectMeta {
            bucket: bucket.to_string(),
            key: key.to_string(),
            blob_id,
            size,
            etag: etag.clone(),
            content_type: content_type.clone(),
            user_meta: user_meta.clone(),
            parts_manifest: None,
            sse: sse.as_ref().map(|m| crate::sse::descriptor(m.key_id())),
        })
        .await;
    match outcome {
        Ok(outcome) => {
            for old in &outcome.shadowed_blobs {
                if let Err(e) = st.router.delete(old).await {
                    tracing::warn!(error = %e, "s3 gc: shadowed blob delete failed after copy");
                }
            }
            Ok(etag)
        }
        Err(e @ S3Error::QuotaExceeded { .. }) => {
            if let Err(de) = st.router.delete(&blob_id).await {
                tracing::warn!(error = %de, "s3 quota: tombstone of rejected copy failed");
            }
            st.metrics
                .quota_rejected_total
                .with_label_values(&[bucket])
                .inc();
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// 逐 part 重建(≤ 4 MiB/步,整 blob 绝不进内存):put 失败即弃,孤儿 session 交
/// 既有 upload-TTL GC 兜底;配额拒 = complete 面(507,session 保留)。
async fn copy_via_multipart(
    st: AppState,
    bucket: &str,
    key: &str,
    parts: &[ManifestPart],
    content_type: &Option<String>,
    user_meta: &UserMeta,
) -> Result<String, S3Error> {
    let upload_id = st
        .metastore
        .create_multipart(bucket, key, content_type.as_deref(), user_meta)
        .await?;
    let mut complete_parts: Vec<CompletePart> = Vec::with_capacity(parts.len());
    for (idx, part) in parts.iter().enumerate() {
        let part_number = i32::try_from(idx + 1)
            .map_err(|e| S3Error::InternalError(format!("part index overflow: {e}")))?;
        let blob_id = antfs_blob::StoreKey::from_slice(&part.blob_id)
            .ok_or_else(|| S3Error::InternalError("bad blob_id in manifest".into()))?;
        let data = Bytes::from(st.router.get(&blob_id).await?.to_vec());
        let size = data.len() as u64;
        let etag = md5_hex(&data);
        let (new_blob, _) = st
            .router
            .put_auto_with_ttl(data.clone(), NO_EXPIRATION)
            .await?;
        st.metastore
            .put_part(
                &upload_id,
                PartMeta {
                    part_number,
                    blob_id: new_blob,
                    size,
                    etag: etag.clone(),
                },
            )
            .await?;
        complete_parts.push(CompletePart { part_number, etag });
    }
    let result = match st
        .metastore
        .complete_multipart(&upload_id, &complete_parts)
        .await
    {
        Ok(result) => result,
        Err(e @ S3Error::QuotaExceeded { .. }) => {
            st.metrics
                .quota_rejected_total
                .with_label_values(&[bucket])
                .inc();
            return Err(e);
        }
        Err(e) => return Err(e),
    };
    for old in result
        .shadowed_blobs
        .iter()
        .chain(result.discarded_part_blobs.iter())
    {
        if let Err(e) = st.router.delete(old).await {
            tracing::warn!(error = %e, "s3 gc: blob reclaim failed after server-side copy");
        }
    }
    Ok(result.etag)
}

pub async fn get_object_attributes(
    st: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let requested: Vec<String> = headers
        .get("x-amz-object-attributes")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| S3Error::InvalidArgument("missing x-amz-object-attributes header".into()))?
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    for attr in &requested {
        match attr.as_str() {
            "etag" | "storageclass" | "objectsize" => {}
            "checksum" | "objectparts" => {
                return Err(S3Error::NotImplemented(format!(
                    "object attribute `{attr}` is not implemented (checksum family / part listing)"
                )))
            }
            other => {
                return Err(S3Error::InvalidArgument(format!(
                    "unknown object attribute `{other}`"
                )))
            }
        }
    }
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;
    let wants = |name: &str| requested.iter().any(|a| a == name);
    let out = crate::xml::ObjectAttributesOut {
        etag: wants("etag").then(|| meta.etag.clone()),
        object_size: wants("objectsize").then_some(meta.size),
        storage_class: wants("storageclass"),
    };
    let body = crate::xml::serialize_object_attributes(&out);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header("last-modified", http_date_from_ms(meta.created_at))
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .map_err(|e| S3Error::InternalError(format!("response build: {e}")))?;
    Ok(response)
}

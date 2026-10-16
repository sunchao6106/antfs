use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use antfs_router::NO_EXPIRATION;

use super::range::{
    intersect_windows, manifest_part_window, manifest_total_size, parse_range, window_parts,
    ByteWindow, PartStream, RangeOutcome,
};
use crate::checksum::checksum_mode_enabled;
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
    let header_claims = crate::checksum::collect_claim_headers(req.headers())?;
    let declared_trailer = req
        .headers()
        .get("x-amz-trailer")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let trailer_sink = req
        .extensions()
        .get::<crate::auth::chunked::ChunkedTrailers>()
        .cloned();

    let body_bytes = crate::handlers::read_bounded_body(&st, req).await?;

    // checksum 收集/验证:分帧 trailer 在 body EOF 后才齐;sink 为空(非分帧请求)
    // 时只是无补充面。验证先于任何 blob 写 → 失败零孤儿(blob/行/墓碑)。
    let trailer_claims = trailer_sink
        .map(|wrapper| wrapper.0.lock().expect("trailer sink lock").clone())
        .unwrap_or_default();
    let claims = crate::checksum::merge_trailer_claims(
        header_claims,
        &trailer_claims,
        declared_trailer.as_deref(),
    )?;
    let stored_checksum =
        crate::checksum::verify_claims(&claims, &body_bytes)?.map(|claim| StoredChecksum {
            algo: claim.algo,
            value: claim.value,
        });

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
            checksum: stored_checksum.clone(),
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

    // overwrite 遮蔽的旧版本即刻回收(引擎 delete 幂等;失败留 WAL 行,GC 周期补删)。
    for old in &outcome.shadowed_blobs {
        let _ = crate::handlers::tombstone_and_resolve(&st, old).await;
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
    if let Some(c) = &stored_checksum {
        response.headers_mut().insert(
            c.algo.lower_name(),
            c.value
                .parse()
                .map_err(|_| S3Error::InternalError("checksum header".into()))?,
        );
        response.headers_mut().insert(
            "x-amz-checksum-type",
            "FULL_OBJECT".parse().expect("static header value"),
        );
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
    part_number: Option<i32>,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    // 条件评估只吃 MetaStore 行:304/412 决不触数据面。
    match evaluate_read_conditions(&meta, headers)? {
        ReadOutcome::PreconditionFailed => return Err(S3Error::PreconditionFailed),
        ReadOutcome::NotModified => return not_modified_response(&meta),
        ReadOutcome::Proceed => {}
    }
    let parts: Option<Vec<ManifestPart>> = match &meta.parts_manifest {
        Some(json) => Some(
            serde_json::from_str(json)
                .map_err(|e| S3Error::InternalError(format!("corrupt manifest: {e}")))?,
        ),
        None => None,
    };
    if let Some(ps) = &parts {
        let total = manifest_total_size(ps);
        if total != meta.size {
            return Err(S3Error::InternalError(format!(
                "manifest parts total {total} != row size {} (bucket {}, key {})",
                meta.size, meta.bucket, meta.key
            )));
        }
    }
    let if_range_ok = match headers.get(header::IF_RANGE).and_then(|v| v.to_str().ok()) {
        Some(v) => if_range_passes(&meta, v),
        None => true,
    };
    let window = resolve_read_window(
        &meta,
        parts.as_deref(),
        range_header,
        part_number,
        if_range_ok,
    )?;

    let echo_checksum = if checksum_mode_enabled(headers)? {
        meta.checksum.as_ref()
    } else {
        None
    };
    if meta.sse.is_some() {
        return get_encrypted_object(&st, &meta, window, echo_checksum).await;
    }
    if let Some(ps) = &parts {
        return get_multipart_object(st, &meta, ps, window, echo_checksum).await;
    }
    get_plain_object(&st, &meta, window, echo_checksum).await
}

/// GET 读响应头装配(三路径共用):恒回 `Accept-Ranges: bytes`,窗口 → 206 +
/// `Content-Range`(S3 语义:leader 对象元数据头照回)。
fn object_read_builder(
    meta: &ObjectMeta,
    sse: bool,
    body_len: u64,
    window: Option<ByteWindow>,
    echo_checksum: Option<&StoredChecksum>,
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
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::LAST_MODIFIED, http_date_from_ms(meta.created_at));
    if sse {
        builder = builder.header("x-amz-server-side-encryption", "AES256");
    }
    if let Some(w) = window {
        builder = builder.header(header::CONTENT_RANGE, w.content_range(meta.size));
    }
    if let Some(c) = echo_checksum {
        builder = builder
            .header(c.algo.lower_name(), c.value.as_str())
            .header("x-amz-checksum-type", "FULL_OBJECT");
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

/// 304 空体(RFC 7232:命中缓存校验器;不回 body/Content-Length)。ETag 与
/// Last-Modified 必带,GET/HEAD 共用。
fn not_modified_response(meta: &ObjectMeta) -> Result<Response, S3Error> {
    let mut builder = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::LAST_MODIFIED, http_date_from_ms(meta.created_at))
        .header(header::ACCEPT_RANGES, "bytes");
    if meta.sse.is_some() {
        builder = builder.header("x-amz-server-side-encryption", "AES256");
    }
    builder.body(Body::empty()).map_err(response_build_error)
}

/// `?partNumber=n` → 片窗(416 携实际片数)。单片对象:part 1 = 全幅窗,余皆
/// 无此片;零长窗(空对象 / 零尺寸片)按不可满足 → 416,与「空对象任何 range
/// 皆 416」既有语义对齐(AWS 会发退化 Content-Range `0--1`,我们不效仿)。
fn part_window_or_416(
    meta: &ObjectMeta,
    parts: Option<&[ManifestPart]>,
    n: i32,
) -> Result<ByteWindow, S3Error> {
    let count = parts.map_or(1, <[ManifestPart]>::len);
    let window = match parts {
        Some(ps) => manifest_part_window(ps, n),
        None => (n == 1).then_some(ByteWindow {
            start: 0,
            len: meta.size,
        }),
    };
    match window {
        Some(w) if w.len > 0 => Ok(w),
        _ => Err(S3Error::InvalidRange {
            requested: format!("partNumber={n} (object has {count} part(s))"),
            actual_size: meta.size,
        }),
    }
}

/// 读窗解析(Range × partNumber × If-Range;`None` = 200 全对象):
/// validator 失败 → 两窗口双双作废(早于 416:坏窗不升显);partNumber 与 Range
/// 各自成窗再求交(空交 → 416);Range 不可满足 → 416 既有语义。
fn resolve_read_window(
    meta: &ObjectMeta,
    parts: Option<&[ManifestPart]>,
    range_header: Option<&str>,
    part_number: Option<i32>,
    if_range_ok: bool,
) -> Result<Option<ByteWindow>, S3Error> {
    if !if_range_ok {
        return Ok(None);
    }
    let part_window = match part_number {
        None => None,
        Some(n) => Some(part_window_or_416(meta, parts, n)?),
    };
    let range_window = match parse_range(range_header, meta.size) {
        RangeOutcome::Full => None,
        RangeOutcome::Partial(w) => Some(w),
        RangeOutcome::Unsatisfiable { spec } => {
            return Err(S3Error::InvalidRange {
                requested: spec,
                actual_size: meta.size,
            })
        }
    };
    match (part_window, range_window) {
        (None, None) => Ok(None),
        (Some(w), None) | (None, Some(w)) => Ok(Some(w)),
        (Some(p), Some(r)) => intersect_windows(p, r)
            .map(Some)
            .ok_or(S3Error::InvalidRange {
                requested: format!("part={} × range={:?}", p.start, range_header),
                actual_size: meta.size,
            }),
    }
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
    echo_checksum: Option<&StoredChecksum>,
) -> Result<Response, S3Error> {
    let data = st.router.get(&meta.blob_id).await?;
    let body = apply_window(data, meta, window)?;
    let len = body.len() as u64;
    object_read_builder(meta, false, len, window, echo_checksum)
        .body(Body::from(body))
        .map_err(response_build_error)
}

async fn get_multipart_object(
    st: AppState,
    meta: &ObjectMeta,
    parts: &[ManifestPart],
    window: Option<ByteWindow>,
    echo_checksum: Option<&StoredChecksum>,
) -> Result<Response, S3Error> {
    let total = manifest_total_size(parts);
    let effective = window.unwrap_or(ByteWindow {
        start: 0,
        len: total,
    });
    let covering = window_parts(parts, effective)?;
    let stream = PartStream::new(st, covering);
    object_read_builder(meta, false, effective.len, window, echo_checksum)
        .body(Body::from_stream(stream))
        .map_err(response_build_error)
}

/// SSE 对象读:blob = 帧 → 明文(解帧失败 = InternalError,响亮不吞)。解密仍走
/// 缓冲路径(单 blob ≤ `max_body_bytes` 不变量),窗口在明文上切片。
async fn get_encrypted_object(
    st: &AppState,
    meta: &ObjectMeta,
    window: Option<ByteWindow>,
    echo_checksum: Option<&StoredChecksum>,
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
    object_read_builder(meta, true, len, window, echo_checksum)
        .body(Body::from(body))
        .map_err(response_build_error)
}

pub async fn head_object(
    st: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .get_object(bucket, key)
        .await?
        .ok_or(S3Error::NoSuchKey)?;

    match evaluate_read_conditions(&meta, headers)? {
        ReadOutcome::PreconditionFailed => return Err(S3Error::PreconditionFailed),
        ReadOutcome::NotModified => return not_modified_response(&meta),
        ReadOutcome::Proceed => {}
    }
    let echo_checksum = if checksum_mode_enabled(headers)? {
        meta.checksum.as_ref()
    } else {
        None
    };

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, meta.size.to_string())
        .header(header::ETAG, format!("\"{}\"", meta.etag))
        .header(header::LAST_MODIFIED, http_date_from_ms(meta.created_at));
    if let Some(c) = echo_checksum {
        builder = builder
            .header(c.algo.lower_name(), c.value.as_str())
            .header("x-amz-checksum-type", "FULL_OBJECT");
    }
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
        let _ = crate::handlers::tombstone_and_resolve(&st, &deleted.blob_id).await;
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

/// RFC 7232 §6 求值序(GET/HEAD;拷贝路径走 `evaluate_copy_conditions`,语义差异:
/// If-None-Match / If-Modified-Since 命中在此是 304 缓存协商,不是 412)。
/// If-Match(失配 412,过则短路)> If-Unmodified-Since(无 If-Match 时;已改 412)
/// \> If-None-Match(命中 304,`*` 通配;不匹配短路)> If-Modified-Since(无
/// If-None-Match 时;未改 304)。坏日期 400(与拷贝路径同响亮)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadOutcome {
    Proceed,
    NotModified,
    PreconditionFailed,
}

fn evaluate_read_conditions(
    meta: &ObjectMeta,
    headers: &HeaderMap,
) -> Result<ReadOutcome, S3Error> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    if let Some(expected) = header("if-match") {
        if strip_etag_quotes(expected) != meta.etag {
            return Ok(ReadOutcome::PreconditionFailed);
        }
        return Ok(ReadOutcome::Proceed);
    }
    if let Some(since) = header("if-unmodified-since") {
        if meta.created_at > parse_http_date_ms(since)? {
            return Ok(ReadOutcome::PreconditionFailed);
        }
    }
    if let Some(not) = header("if-none-match") {
        let not = not.trim();
        if not == "*" || strip_etag_quotes(not) == meta.etag {
            return Ok(ReadOutcome::NotModified);
        }
        return Ok(ReadOutcome::Proceed);
    }
    if let Some(since) = header("if-modified-since") {
        if meta.created_at <= parse_http_date_ms(since)? {
            return Ok(ReadOutcome::NotModified);
        }
    }
    Ok(ReadOutcome::Proceed)
}

/// If-Range validator(RFC 7233):ETag 形(带引号,强比较;`W/` 弱标签按失配)须与
/// 对象 ETag 相等;HTTP-date 形须 ≥ 对象最后修改时刻;解析失败 = 不过。不过的
/// 语义由调用方落实:忽略 Range / partNumber,回 200 全对象。
fn if_range_passes(meta: &ObjectMeta, value: &str) -> bool {
    let v = value.trim();
    if v.starts_with('"') {
        strip_etag_quotes(v) == meta.etag
    } else if v.starts_with("W/") || v.starts_with("w/") {
        false
    } else {
        match parse_http_date_ms(v) {
            Ok(ms) => ms >= meta.created_at,
            Err(_) => false,
        }
    }
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
            checksum: None,
        })
        .await;
    match outcome {
        Ok(outcome) => {
            for old in &outcome.shadowed_blobs {
                if let Err(e) = crate::handlers::tombstone_and_resolve(&st, old).await {
                    tracing::warn!(error = %e, "s3 gc: shadowed blob delete failed after copy (WAL row kept)");
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
        if let Err(e) = crate::handlers::tombstone_and_resolve(&st, old).await {
            tracing::warn!(error = %e, "s3 gc: blob reclaim failed after server-side copy (WAL row kept)");
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
            "etag" | "storageclass" | "objectsize" | "checksum" => {}
            "objectparts" => {
                return Err(S3Error::NotImplemented(format!(
                    "object attribute `{attr}` is not implemented (part listing)"
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
        checksum: wants("checksum")
            .then_some(meta.checksum.as_ref())
            .flatten()
            .map(|c| (c.algo.algo_name().to_string(), c.value.clone())),
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

#[cfg(test)]
mod read_tests {
    use super::*;
    use antfs_router::StoreKey;
    use axum::http::{HeaderName, HeaderValue};

    const CREATED: i64 = 1_700_000_000_000;
    const DAY_BEFORE: &str = "Mon, 13 Nov 2023 22:13:20 GMT";
    const DAY_AFTER: &str = "Wed, 15 Nov 2023 22:13:20 GMT";

    fn meta() -> ObjectMeta {
        ObjectMeta {
            bucket: "b".into(),
            key: "k".into(),
            blob_id: StoreKey::from_slice(&[1; 8]).expect("8 bytes"),
            version: 1,
            size: 100,
            etag: "e".into(),
            content_type: None,
            created_at: CREATED,
            user_meta: UserMeta::new(),
            parts_manifest: None,
            sse: None,
            checksum: None,
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).expect("lowercase name"),
                HeaderValue::from_str(v).expect("ascii value"),
            );
        }
        h
    }

    #[test]
    fn read_conditions_matrix() {
        assert_eq!(
            evaluate_read_conditions(&meta(), &HeaderMap::new()).unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-match", "\"e\"")])).unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-match", "\"x\"")])).unwrap(),
            ReadOutcome::PreconditionFailed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-none-match", "\"e\"")])).unwrap(),
            ReadOutcome::NotModified
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-none-match", "*")])).unwrap(),
            ReadOutcome::NotModified
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-none-match", "\"x\"")])).unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-modified-since", DAY_AFTER)]))
                .unwrap(),
            ReadOutcome::NotModified
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-modified-since", DAY_BEFORE)]))
                .unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-unmodified-since", DAY_BEFORE)]))
                .unwrap(),
            ReadOutcome::PreconditionFailed
        );
        assert_eq!(
            evaluate_read_conditions(&meta(), &headers(&[("if-unmodified-since", DAY_AFTER)]))
                .unwrap(),
            ReadOutcome::Proceed
        );
    }

    #[test]
    fn read_conditions_short_circuit_order() {
        assert_eq!(
            evaluate_read_conditions(
                &meta(),
                &headers(&[("if-match", "\"e\""), ("if-modified-since", DAY_AFTER)])
            )
            .unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(
                &meta(),
                &headers(&[("if-none-match", "\"x\""), ("if-modified-since", DAY_AFTER)])
            )
            .unwrap(),
            ReadOutcome::Proceed
        );
        assert_eq!(
            evaluate_read_conditions(
                &meta(),
                &headers(&[
                    ("if-none-match", "\"e\""),
                    ("if-unmodified-since", DAY_BEFORE)
                ])
            )
            .unwrap(),
            ReadOutcome::PreconditionFailed
        );
    }

    #[test]
    fn read_conditions_malformed_date_is_400() {
        for name in ["if-modified-since", "if-unmodified-since"] {
            let e = evaluate_read_conditions(&meta(), &headers(&[(name, "definitely-not-a-date")]))
                .unwrap_err();
            assert!(matches!(e, S3Error::InvalidArgument(_)), "{e}");
        }
    }

    #[test]
    fn if_range_validator_matrix() {
        assert!(if_range_passes(&meta(), "\"e\""));
        assert!(!if_range_passes(&meta(), "e"));
        assert!(!if_range_passes(&meta(), "\"x\""));
        assert!(!if_range_passes(&meta(), "W/\"e\""));
        assert!(if_range_passes(&meta(), DAY_AFTER));
        assert!(if_range_passes(&meta(), "Tue, 14 Nov 2023 22:13:20 GMT"));
        assert!(!if_range_passes(&meta(), DAY_BEFORE));
        assert!(!if_range_passes(&meta(), "definitely-not-a-date"));
    }

    fn window(start: u64, len: u64) -> Option<ByteWindow> {
        Some(ByteWindow { start, len })
    }

    #[test]
    fn read_window_matrix_multipart() {
        let m = meta();
        let json = serde_json::to_string(&vec![
            ManifestPart {
                blob_id: [1; 8],
                size: 10,
            },
            ManifestPart {
                blob_id: [2; 8],
                size: 20,
            },
            ManifestPart {
                blob_id: [3; 8],
                size: 30,
            },
        ])
        .unwrap();
        let parts: Option<Vec<ManifestPart>> = serde_json::from_str(&json).unwrap();
        let parts = parts.as_deref();

        assert_eq!(
            resolve_read_window(&m, parts, None, None, true).unwrap(),
            None
        );
        assert_eq!(
            resolve_read_window(&m, parts, None, Some(2), true).unwrap(),
            window(10, 20)
        );
        assert_eq!(
            resolve_read_window(&m, parts, Some("bytes=90-"), None, true).unwrap(),
            window(90, 10)
        );
        assert_eq!(
            resolve_read_window(&m, parts, Some("bytes=15-19"), Some(2), true).unwrap(),
            window(15, 5)
        );
        for n in [0, 4] {
            let e = resolve_read_window(&m, parts, None, Some(n), true).unwrap_err();
            assert!(matches!(e, S3Error::InvalidRange { .. }), "{e}");
        }
        assert!(matches!(
            resolve_read_window(&m, parts, Some("bytes=0-9"), Some(2), true).unwrap_err(),
            S3Error::InvalidRange { .. }
        ));
        assert!(matches!(
            resolve_read_window(&m, parts, Some("bytes=100-"), None, true).unwrap_err(),
            S3Error::InvalidRange { .. }
        ));
    }

    #[test]
    fn read_window_matrix_single_part() {
        let m = meta();
        assert_eq!(
            resolve_read_window(&m, None, None, Some(1), true).unwrap(),
            window(0, 100)
        );
        assert!(matches!(
            resolve_read_window(&m, None, None, Some(2), true).unwrap_err(),
            S3Error::InvalidRange { .. }
        ));
        assert_eq!(
            resolve_read_window(&m, None, Some("bytes=10-19"), None, true).unwrap(),
            window(10, 10)
        );
        assert_eq!(
            resolve_read_window(&m, None, Some("bytes=10-19"), Some(1), true).unwrap(),
            window(10, 10)
        );
    }

    #[test]
    fn if_range_failure_vacuums_the_window() {
        let m = meta();
        assert_eq!(
            resolve_read_window(&m, None, Some("bytes=10-19"), None, false).unwrap(),
            None
        );
        assert_eq!(
            resolve_read_window(&m, None, None, Some(1), false).unwrap(),
            None
        );
        assert_eq!(
            resolve_read_window(&m, None, None, Some(9), false).unwrap(),
            None
        );
    }
}

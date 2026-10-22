use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use antfs_router::NO_EXPIRATION;

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::{AppState, S3QueryParams};
use std::collections::HashMap;

use futures::StreamExt;

use crate::checksum::{ChecksumAlgo, ChecksumKind, StreamingDigest};
use crate::handlers::range::{PartStream, WindowPart};
use crate::metastore::types::CompleteChecksum;

pub async fn create_multipart_upload(
    st: AppState,
    bucket: &str,
    key: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let (content_type, user_meta) = crate::handlers::object::capture_object_meta(req.headers())?;
    if req.headers().get("x-amz-server-side-encryption").is_some() {
        return Err(S3Error::NotImplemented(
            "multipart uploads do not support server-side encryption this round".into(),
        ));
    }

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
    if req.headers().get("x-amz-server-side-encryption").is_some() {
        return Err(S3Error::NotImplemented(
            "multipart parts do not support server-side encryption this round".into(),
        ));
    }
    let header_claims = crate::checksum::collect_claim_headers(req.headers())?;
    crate::checksum::ensure_full_object_type(req.headers())?;
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

    // 与 PutObject 同一条验证管;验证过的最强值随行落库(migration 007,complete 级
    // composite 的推导原料)。
    let trailer_claims = trailer_sink
        .map(|wrapper| wrapper.0.lock().expect("trailer sink lock").clone())
        .unwrap_or_default();
    let claims = crate::checksum::merge_trailer_claims(
        header_claims,
        &trailer_claims,
        declared_trailer.as_deref(),
    )?;
    let best_claim = crate::checksum::verify_claims(&claims, &body_bytes)?;

    let etag = {
        use md5::{Digest, Md5};
        format!("{:x}", Md5::digest(&body_bytes))
    };
    let size = body_bytes.len() as u64;

    let (blob_key, _) = st
        .router
        .put_auto_with_ttl(body_bytes, NO_EXPIRATION)
        .await?;

    let stored_checksum = best_claim.as_ref().map(|claim| StoredChecksum {
        algo: claim.algo,
        value: claim.value.clone(),
        kind: crate::checksum::ChecksumKind::FullObject,
    });
    let old_blob = st
        .metastore
        .put_part(
            upload_id,
            PartMeta {
                part_number,
                blob_id: blob_key,
                size,
                etag: etag.clone(),
                checksum: stored_checksum,
            },
        )
        .await?;

    if let Some(old) = old_blob {
        let _ = crate::handlers::tombstone_and_resolve(&st, &old).await;
    }

    let mut response =
        (StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))], "").into_response();
    if let Some(claim) = best_claim {
        response.headers_mut().insert(
            claim.algo.lower_name(),
            claim
                .value
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

pub async fn complete_multipart_upload(
    st: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let headers = req.headers().clone();
    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .map_err(|_| S3Error::MalformedXML("body too large".into()))?;

    let xml_str =
        std::str::from_utf8(&body).map_err(|_| S3Error::MalformedXML("invalid utf-8".into()))?;

    let parts = crate::xml::parse_complete_multipart(xml_str)?;

    let result = finish_multipart(&st, bucket, upload_id, &parts, &headers).await?;

    let xml = crate::xml::serialize_complete_multipart(bucket, key, &result.etag);
    let mut response = (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
    if let Some(c) = &result.checksum {
        response.headers_mut().insert(
            c.algo.lower_name(),
            c.value
                .parse()
                .map_err(|_| S3Error::InternalError("checksum header".into()))?,
        );
        response.headers_mut().insert(
            "x-amz-checksum-type",
            c.kind.as_str().parse().expect("static header value"),
        );
    }
    Ok(response)
}

/// complete 终段(显式 XML 路径与 `streamed_put_object` 内部复用同一干道):
/// 共享上限对账(members 实际尺寸合计)→ complete 级校验裁决(claims 从 `ruling_headers`
/// 读:XML 路径 = 本请求头,流式路径 = 合并后的整对象 claim 合成头)→ 会话锁定落行 →
/// 遮蔽丢弃账回收。配额拒绝的"session/parts 行不动可重发"语义对两条调用方同样成立
/// (流式调用方拿到 Err 后走自己的 abort 清场)。
async fn finish_multipart(
    st: &AppState,
    bucket: &str,
    upload_id: &str,
    parts: &[CompletePart],
    ruling_headers: &axum::http::HeaderMap,
) -> Result<CompleteMultipartResult, S3Error> {
    // 公共单文件上限([upload] 表,与 PutObject 闸同一值):按 manifest 所列各片的
    // 实际尺寸合计,超限 413;session 与 parts 行不动(客户端可 abort 或缩减后重发)。
    {
        let known = st.metastore.session_parts(upload_id).await?;
        let total: u64 = parts
            .iter()
            .filter_map(|cp| known.iter().find(|p| p.part_number == cp.part_number))
            .map(|pm| pm.size)
            .sum();
        if total > st.max_object_bytes {
            return Err(S3Error::EntityTooLarge);
        }
    }

    // complete 级校验声明裁决(FULL_OBJECT 路径在此流式全验;composite 终判在会话锁内)。
    let checksum_param = complete_checksum_ruling(st, upload_id, ruling_headers, parts).await?;

    let result = match st
        .metastore
        .complete_multipart(upload_id, parts, checksum_param)
        .await
    {
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

    // 被本次 complete 遮蔽的旧版本 + **未列入 manifest** 的 part blob(S3 语义:complete 即
    // 丢弃)→ 即刻回收(幂等;失败留 WAL 行,GC 周期补删)。
    for old in result
        .shadowed_blobs
        .iter()
        .chain(result.discarded_part_blobs.iter())
    {
        if let Err(e) = crate::handlers::tombstone_and_resolve(st, old).await {
            tracing::warn!(error = %e, "s3 gc: blob reclaim failed after multipart complete (WAL row kept)");
        }
    }
    Ok(result)
}

pub async fn abort_multipart_upload(st: AppState, upload_id: &str) -> Result<Response, S3Error> {
    let blob_ids = st.metastore.abort_multipart(upload_id).await?;
    for bid in blob_ids {
        let _ = crate::handlers::tombstone_and_resolve(&st, &bid).await;
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

/// complete 级校验声明裁决(design D4):族门(含 CRC64NVME 501,复用 PUT/Part 收集器)
/// → `x-amz-checksum-type` 缺省 FULL_OBJECT → FULL_OBJECT 路径按 manifest 流式全验
/// (PartStream 逐片,至多一片在内存;比对通过即取最强入参)→ composite 路径只携
/// claim 值,族性/同族性/比对终判在 metastore 会话锁内(快照即终版)。
async fn complete_checksum_ruling(
    st: &AppState,
    upload_id: &str,
    headers: &axum::http::HeaderMap,
    parts: &[CompletePart],
) -> Result<CompleteChecksum, S3Error> {
    let claims = crate::checksum::collect_claim_headers(headers)?;
    let declared_type = headers
        .get("x-amz-checksum-type")
        .and_then(|v| v.to_str().ok());

    match declared_type {
        Some("COMPOSITE") => {
            if claims.is_empty() {
                return Ok(CompleteChecksum::Composite { claimed: None });
            }
            if claims.len() > 1 {
                return Err(S3Error::InvalidRequest(
                    "composite complete-level checksum accepts at most one CRC-family value".into(),
                ));
            }
            let claim = &claims[0];
            if !matches!(claim.algo, ChecksumAlgo::Crc32 | ChecksumAlgo::Crc32c) {
                return Err(S3Error::InvalidRequest(format!(
                    "composite checksum is not defined for {} (CRC family only)",
                    claim.algo.algo_name()
                )));
            }
            Ok(CompleteChecksum::Composite {
                claimed: Some(claim.value.clone()),
            })
        }
        None | Some("FULL_OBJECT") => {
            if claims.is_empty() {
                if declared_type.is_some() {
                    return Err(S3Error::InvalidRequest(
                        "x-amz-checksum-type declared without any checksum value header".into(),
                    ));
                }
                return Ok(CompleteChecksum::None);
            }
            verify_full_object_claims(st, upload_id, parts, claims).await
        }
        Some(other) => Err(S3Error::InvalidArgument(format!(
            "invalid x-amz-checksum-type `{other}` (FULL_OBJECT or COMPOSITE)"
        ))),
    }
}

/// FULL_OBJECT 声明集在装配字节流上的全验:manifest 序逐片流式喂所有声明摘要器,
/// 任一失配即 `BadDigest`(零副作用:session/parts 原样,可重试);通过者取最强一份。
async fn verify_full_object_claims(
    st: &AppState,
    upload_id: &str,
    parts: &[CompletePart],
    claims: Vec<crate::checksum::ChecksumClaim>,
) -> Result<CompleteChecksum, S3Error> {
    let stored = st.metastore.session_parts(upload_id).await?;
    let by_number: HashMap<i32, &PartMeta> = stored.iter().map(|p| (p.part_number, p)).collect();
    let mut window_parts = Vec::with_capacity(parts.len());
    for cp in parts {
        let pm = by_number
            .get(&cp.part_number)
            .copied()
            .ok_or(S3Error::InvalidPart)?;
        window_parts.push(WindowPart {
            key: pm.blob_id,
            skip: 0,
            take: pm.size,
        });
    }

    let mut digesters: Vec<(crate::checksum::ChecksumClaim, StreamingDigest)> = claims
        .into_iter()
        .map(|c| {
            let d = StreamingDigest::new(c.algo);
            (c, d)
        })
        .collect();

    let mut stream = PartStream::new(st.clone(), window_parts);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for (_, digest) in digesters.iter_mut() {
            digest.update(&chunk);
        }
    }

    let mut verified: Option<StoredChecksum> = None;
    for (claim, digest) in digesters {
        let computed = digest.finish();
        if computed != claim.value {
            return Err(S3Error::BadDigest {
                checksum_form: format!("complete full-object {} mismatch", claim.algo.lower_name()),
            });
        }
        let stronger = match &verified {
            Some(v) => v.algo < claim.algo,
            None => true,
        };
        if stronger {
            verified = Some(StoredChecksum {
                algo: claim.algo,
                value: computed,
                kind: ChecksumKind::FullObject,
            });
        }
    }
    Ok(CompleteChecksum::FullObject(verified.expect(
        "claims were non-empty so at least one value is verified",
    )))
}

/// PutObject 大对象流式通路(2026-10-22,裁决 #13):申报尺寸 > `s3.max_body_bytes`
/// 且 ≤ 共享单文件上限的直传,不再 413,改为网关固定片缓冲整流 —— 内存恒 = 1 片,
/// 边收边写。全套闸与账与显式 multipart 共用:session 行落库即受既有 GC 会话过期
/// 兜底;上限/声明中段对账;complete 级校验;配额;遮蔽回收。任何中途失败(网络断/
/// 尺寸不符/校验失配/配额拒)显式 abort 墓碑已写片 —— 与小对象"先验后写"路径相比,
/// 失败瞬时性留片(由 abort/GC 收账),换取大对象零整存。SSE 暂不支持(与显式
/// multipart 同族限制)。ETag 为 multipart 式合成(`N=1` 时与单传 md5 同形)——与 AWS
/// 单传 md5 惯例存在已知偏差,记档 s3.md。
pub(crate) async fn streamed_put_object(
    st: AppState,
    bucket: &str,
    key: &str,
    req: Request,
    declared_total: Option<u64>,
) -> Result<Response, S3Error> {
    if req.headers().get("x-amz-server-side-encryption").is_some() {
        return Err(S3Error::NotImplemented(
            "streamed PutObject does not support server-side encryption this round".into(),
        ));
    }
    let header_claims = crate::checksum::collect_claim_headers(req.headers())?;
    crate::checksum::ensure_full_object_type(req.headers())?;
    let declared_trailer = req
        .headers()
        .get("x-amz-trailer")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    // trailer 扩展必须在 into_body 消费 request 前拿走(其终值在 body EOF 后才齐)。
    let trailer_sink = req
        .extensions()
        .get::<crate::auth::chunked::ChunkedTrailers>()
        .cloned();
    let (content_type, user_meta) = crate::handlers::object::capture_object_meta(req.headers())?;

    let upload_id = st
        .metastore
        .create_multipart(bucket, key, Some(content_type.as_str()), &user_meta)
        .await?;

    let (parts, total) = match streamed_parts(
        &st,
        &upload_id,
        req.into_body(),
        st.config.max_body_bytes as usize,
        st.max_object_bytes,
        declared_total,
    )
    .await
    {
        Ok(parts) => parts,
        Err(e) => {
            abort_streamed_session(&st, &upload_id).await;
            return Err(e);
        }
    };

    if parts.is_empty() || declared_total.is_some_and(|d| d != total) {
        abort_streamed_session(&st, &upload_id).await;
        return Err(S3Error::IncompleteBody);
    }

    // 声明合并(头 + trailer,EOF 后终值;验证在 complete 级,这里只并集)。
    let trailer_claims = trailer_sink
        .map(|wrapper| wrapper.0.lock().expect("trailer sink lock").clone())
        .unwrap_or_default();
    let merged = match crate::checksum::merge_trailer_claims(
        header_claims,
        &trailer_claims,
        declared_trailer.as_deref(),
    ) {
        Ok(merged) => merged,
        Err(e) => {
            abort_streamed_session(&st, &upload_id).await;
            return Err(e);
        }
    };
    let mut ruling_headers = axum::http::HeaderMap::new();
    for claim in &merged {
        let value = claim
            .value
            .parse()
            .map_err(|_| S3Error::InternalError("checksum header".into()))?;
        ruling_headers.insert(claim.algo.lower_name(), value);
    }

    let result = match finish_multipart(&st, bucket, &upload_id, &parts, &ruling_headers).await {
        Ok(result) => result,
        Err(e) => {
            abort_streamed_session(&st, &upload_id).await;
            return Err(e);
        }
    };
    st.metrics.put_objects_streamed_total.inc();
    st.metrics
        .put_object_streamed_parts_total
        .inc_by(parts.len() as u64);

    let mut response = (
        StatusCode::OK,
        [(header::ETAG, format!("\"{}\"", result.etag))],
        "",
    )
        .into_response();
    if let Some(c) = &result.checksum {
        response.headers_mut().insert(
            c.algo.lower_name(),
            c.value
                .parse()
                .map_err(|_| S3Error::InternalError("checksum header".into()))?,
        );
        response.headers_mut().insert(
            "x-amz-checksum-type",
            c.kind.as_str().parse().expect("static header value"),
        );
    }
    Ok(response)
}

/// 边收边切边写:固定片宽缓冲,满一片写一片(router 单笔 + session 片行);
/// 片 etag = 片 md5(与 UploadPart 同口径,complete 验配对账)。
async fn streamed_parts(
    st: &AppState,
    upload_id: &str,
    body: axum::body::Body,
    chunk_cap: usize,
    max_object: u64,
    declared_total: Option<u64>,
) -> Result<(Vec<CompletePart>, u64), S3Error> {
    let mut stream = body.into_data_stream();
    let mut buf = bytes::BytesMut::new();
    let mut total: u64 = 0;
    let mut parts: Vec<CompletePart> = Vec::new();
    let mut next_part: i32 = 1;
    while let Some(item) = stream.next().await {
        let chunk = item.map_err(crate::handlers::map_body_error)?;
        total += chunk.len() as u64;
        if total > max_object || declared_total.is_some_and(|d| total > d) {
            // 中段熔断:一字节不多写已落片的账,清场职责在调用方(abort)。
            return Err(S3Error::IncompleteBody);
        }
        buf.extend_from_slice(&chunk);
        while buf.len() >= chunk_cap {
            let piece = buf.split_to(chunk_cap).freeze();
            parts.push(write_streamed_part(st, upload_id, next_part, piece).await?);
            next_part += 1;
        }
    }
    if !buf.is_empty() {
        parts.push(write_streamed_part(st, upload_id, next_part, buf.freeze()).await?);
    }
    Ok((parts, total))
}

async fn write_streamed_part(
    st: &AppState,
    upload_id: &str,
    part_number: i32,
    data: bytes::Bytes,
) -> Result<CompletePart, S3Error> {
    let etag = {
        use md5::{Digest, Md5};
        format!("{:x}", Md5::digest(&data))
    };
    let size = data.len() as u64;
    let (blob_key, _) = st.router.put_auto_with_ttl(data, NO_EXPIRATION).await?;
    st.metastore
        .put_part(
            upload_id,
            PartMeta {
                part_number,
                blob_id: blob_key,
                size,
                etag: etag.clone(),
                checksum: None,
            },
        )
        .await?;
    Ok(CompletePart { part_number, etag })
}

async fn abort_streamed_session(st: &AppState, upload_id: &str) {
    match st.metastore.abort_multipart(upload_id).await {
        Ok(blob_ids) => {
            for bid in blob_ids {
                let _ = crate::handlers::tombstone_and_resolve(st, &bid).await;
            }
        }
        // abort 自身失败(元库抖动)不吞原错:会话行仍在,过期 GC 兜底收尾。
        Err(e) => tracing::warn!(error = %e, upload_id, "s3 streamed put abort failed"),
    }
}

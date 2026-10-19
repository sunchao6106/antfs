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

    // complete 级校验声明裁决(FULL_OBJECT 路径在此流式全验;composite 终判在会话锁内)。
    let checksum_param = complete_checksum_ruling(&st, upload_id, &headers, &parts).await?;

    let result = match st
        .metastore
        .complete_multipart(upload_id, &parts, checksum_param)
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
        if let Err(e) = crate::handlers::tombstone_and_resolve(&st, old).await {
            tracing::warn!(error = %e, "s3 gc: blob reclaim failed after multipart complete (WAL row kept)");
        }
    }

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

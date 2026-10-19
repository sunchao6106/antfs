use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::{AppState, S3QueryParams};

pub async fn create_bucket(
    st: AppState,
    bucket: &str,
    owner: &str,
    canned_acl: Option<&str>,
) -> Result<Response, S3Error> {
    validate_bucket_name(bucket)?;
    // create 恒 private;显式公开在 create 后追加 set —— 间隙方向由宽收严,fail-safe
    //(间隙窗口内桶仍是 private,不存在先公开后回退的危险窗口)。
    let acl = parse_canned_acl(canned_acl)?;
    st.metastore.create_bucket(bucket, owner).await?;
    if acl != BucketAcl::Private {
        st.metastore.set_bucket_acl(bucket, acl).await?;
    }
    Ok(StatusCode::OK.into_response())
}

pub async fn delete_bucket(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    st.metastore.delete_bucket(bucket).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn head_bucket(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    st.metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    Ok(StatusCode::OK.into_response())
}

pub async fn list_buckets(st: AppState, owner: &str) -> Result<Response, S3Error> {
    let buckets = st.metastore.list_buckets(owner).await?;
    let xml = crate::xml::serialize_list_all_my_buckets(&buckets);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

pub async fn list_objects_v2(
    st: AppState,
    bucket: &str,
    q: S3QueryParams,
) -> Result<Response, S3Error> {
    st.metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;

    let req = ListObjectsReq {
        bucket: bucket.to_string(),
        prefix: q.prefix.clone(),
        delimiter: q.delimiter.clone(),
        max_keys: q.max_keys.unwrap_or(1000),
        continuation_token: q.continuation_token.clone(),
        start_after: q.start_after.clone(),
    };
    let resp = st.metastore.list_objects(req).await?;
    let xml = crate::xml::serialize_list_bucket_result(
        bucket,
        q.prefix.as_deref(),
        q.delimiter.as_deref(),
        q.max_keys.unwrap_or(1000),
        &resp,
    );
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

/// ListObjectsV1(`GET /{bucket}` 无 `list-type=2`):marker(排他起点,与
/// continuation-token 同走 `key > start_after` 通路)→ 复用同一 list 核心,V1 只换
/// XML 形态。NextMarker 仅在 delimiter 存在且截断时回,值取末 key / 末公共前缀的
/// 字典序大者(exclusive 排他起点,翻页零缺漏零重复)。
pub async fn list_objects_v1(
    st: AppState,
    bucket: &str,
    q: S3QueryParams,
) -> Result<Response, S3Error> {
    st.metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;

    let max_keys = q.max_keys.unwrap_or(1000);
    let req = ListObjectsReq {
        bucket: bucket.to_string(),
        prefix: q.prefix.clone(),
        delimiter: q.delimiter.clone(),
        max_keys,
        continuation_token: None,
        start_after: q.marker.clone(),
    };
    let resp = st.metastore.list_objects(req).await?;
    let xml = crate::xml::serialize_list_bucket_result_v1(
        bucket,
        q.prefix.as_deref(),
        q.marker.as_deref(),
        q.delimiter.as_deref(),
        max_keys,
        &resp,
    );
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

/// GET /{bucket}?location(owner 专属 —— owner 闸在 server.rs 包装层)。
/// region == us-east-1 回空元素(经典区官方行为),否则回配置 region 字符串。
pub async fn get_bucket_location(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    st.metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    let xml = crate::xml::serialize_location_constraint(&st.config.region);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    if name.len() < 3 || name.len() > 63 {
        return Err(S3Error::InvalidBucketName(
            "must be between 3 and 63 characters".into(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
    {
        return Err(S3Error::InvalidBucketName(
            "must contain only lowercase letters, numbers, hyphens, and periods".into(),
        ));
    }
    if !name.chars().next().unwrap().is_ascii_alphanumeric() {
        return Err(S3Error::InvalidBucketName(
            "must start with a letter or number".into(),
        ));
    }
    Ok(())
}

/// PUT /{bucket}?acl(PutBucketAcl,owner 专属 —— owner 闸在 server.rs 包装层)。
/// 一期只接 canned 头形式;AccessControlPolicy XML body 形式显式 501(不静默、不误导)。
pub async fn put_bucket_acl(
    st: AppState,
    bucket: &str,
    req: Request,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    // canned 头形式:既有路径逐字不动(顺带换置语义下清空 grants)。
    if let Some(raw) = headers.get("x-amz-acl").and_then(|v| v.to_str().ok()) {
        let acl = parse_canned_acl(Some(raw))?;
        st.metastore.set_bucket_acl(bucket, acl).await?;
        st.metastore.set_bucket_acl_grants(bucket, None).await?;
        return Ok(StatusCode::OK.into_response());
    }
    // XML body 形式(`s3-acl-xml`):AccessControlPolicy 全量换置(canned + grants)。
    let body = crate::handlers::read_bounded_body(&st, req).await?;
    let xml_str = std::str::from_utf8(&body)
        .map_err(|_| S3Error::InvalidRequest("ACL body is not UTF-8".into()))?;
    let parsed = crate::xml::parse_access_control_policy(xml_str)?;
    validate_acl_body(&st, bucket, &parsed, false).await?;
    let (acl_cell, grants) = translate_bucket_acl(&parsed, &parsed.owner);
    st.metastore.set_bucket_acl(bucket, acl_cell).await?;
    let grants_json: Option<String> = if grants.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&grants)
                .map_err(|e| S3Error::InternalError(format!("serialize acl grants: {e}")))?,
        )
    };
    st.metastore
        .set_bucket_acl_grants(bucket, grants_json.as_deref())
        .await?;
    Ok(StatusCode::OK.into_response())
}

/// XML 体的白名单校验(桶/对象共用):Owner 必须是桶 owner;权限白名单
/// FULL_CONTROL(仅 owner 自授)/READ,其余(WRITE 家族、READ_ACP/WRITE_ACP)
/// 一律 400 响亮(不支持即拒,不静默忽略)。
pub(crate) async fn validate_acl_body(
    st: &AppState,
    bucket: &str,
    parsed: &crate::xml::ParsedAcl,
    object_face: bool,
) -> Result<(), S3Error> {
    let meta = st
        .metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    if parsed.owner != meta.owner {
        return Err(S3Error::AccessDenied(
            "ACL Owner ID does not match the bucket owner".into(),
        ));
    }
    let owner_grantee = crate::policy::AclGrantee::Owner(meta.owner.clone());
    for grant in &parsed.grants {
        match grant.permission.as_str() {
            "FULL_CONTROL" if grant.grantee == owner_grantee => {}
            "READ" => {}
            other => {
                let _ = other;
                return Err(S3Error::InvalidArgument(
                    "unsupported ACL permission/grantee combination (supported: FULL_CONTROL \
                     for the owner, READ)"
                        .into(),
                ));
            }
        }
        if !object_face && grant.grantee == crate::policy::AclGrantee::AllUsers {
            continue; // 桶域 AllUsers READ = canned public-read 直映射,translate 处理。
        }
    }
    Ok(())
}

/// 桶域翻译:AllUsers READ → canned public-read;具名 READ(非 owner)→ grants 集
/// (owner 自授 READ 无操作,过滤——它已在 owner 蕴含内)。
fn translate_bucket_acl(
    parsed: &crate::xml::ParsedAcl,
    owner: &str,
) -> (BucketAcl, Vec<crate::policy::AclGrant>) {
    let mut public = false;
    let mut grants: Vec<crate::policy::AclGrant> = Vec::new();
    for grant in &parsed.grants {
        match (&grant.grantee, grant.permission.as_str()) {
            (crate::policy::AclGrantee::AllUsers, "READ") => public = true,
            (crate::policy::AclGrantee::Owner(id), "READ") if id != owner => {
                grants.push(crate::policy::AclGrant {
                    grantee: crate::policy::AclGrantee::Owner(id.clone()),
                    permission: "READ".into(),
                });
            }
            _ => {}
        }
    }
    (
        if public {
            BucketAcl::PublicRead
        } else {
            BucketAcl::Private
        },
        grants,
    )
}

/// GET /{bucket}?acl(GetBucketAcl,owner 专属):有命名授予出 grants 形态
/// (canned public-read 就地合成 AllUsers 项,形态与真 S3 一致);无授予回 canned 形态。
pub async fn get_bucket_acl(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    let mut grants: Vec<crate::policy::AclGrant> = match meta.acl_grants.as_deref() {
        Some(raw) => serde_json::from_str(raw)
            .map_err(|e| S3Error::InternalError(format!("corrupt stored ACL grants: {e}")))?,
        None => Vec::new(),
    };
    if !grants.is_empty() || matches!(meta.acl, BucketAcl::PublicRead) {
        if matches!(meta.acl, BucketAcl::PublicRead) {
            grants.push(crate::policy::AclGrant {
                grantee: crate::policy::AclGrantee::AllUsers,
                permission: "READ".into(),
            });
        }
        let xml = crate::xml::serialize_access_control_policy_for_grants(&meta.owner, &grants);
        return Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response());
    }
    let xml = crate::xml::serialize_access_control_policy(&meta.owner, meta.acl);
    Ok((StatusCode::OK, [("content-type", "application/xml")], xml).into_response())
}

fn parse_canned_acl(raw: Option<&str>) -> Result<BucketAcl, S3Error> {
    match raw {
        None | Some("private") => Ok(BucketAcl::Private),
        Some("public-read") => Ok(BucketAcl::PublicRead),
        Some(other) => Err(S3Error::InvalidArgument(format!(
            "unsupported x-amz-acl value `{other}` (supported: private | public-read)"
        ))),
    }
}

// ── 桶策略(s3-bucket-policy,管理面恒 owner——闸在 server.rs 包装层)──────────

/// PUT /{bucket}?policy:body = 策略 JSON(≤ 20 KiB);put 时全量校验(不支持即
/// 响亮 400),存的是受信形态。
pub async fn put_bucket_policy(
    st: AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, S3Error> {
    let headers = req.headers().clone();
    let body = crate::handlers::read_bounded_body(&st, req).await?;
    let raw = std::str::from_utf8(&body)
        .map_err(|_| S3Error::InvalidRequest("policy is not UTF-8".into()))?;
    let _ = headers;
    crate::policy::BucketPolicy::parse(raw, bucket)?;
    st.metastore.set_bucket_policy(bucket, Some(raw)).await?;
    Ok(StatusCode::OK.into_response())
}

/// GET /{bucket}?policy:回存档原文;无策略 404 NoSuchBucketPolicy。
pub async fn get_bucket_policy(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
    match meta.policy {
        Some(raw) => {
            Ok((StatusCode::OK, [("content-type", "application/json")], raw).into_response())
        }
        None => Err(S3Error::NoSuchBucketPolicy),
    }
}

/// DELETE /{bucket}?policy:幂等撤(无策略同 204;桶不存在 404)。
pub async fn delete_bucket_policy(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    st.metastore.set_bucket_policy(bucket, None).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// 对象域校验():共享白名单 + 对象面(AllUsers 合法)口径。
pub(crate) async fn validate_acl_object_body(
    st: &AppState,
    bucket: &str,
    parsed: &crate::xml::ParsedAcl,
) -> Result<(), S3Error> {
    validate_acl_body(st, bucket, parsed, true).await
}

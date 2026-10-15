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
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let Some(raw) = headers.get("x-amz-acl").and_then(|v| v.to_str().ok()) else {
        return Err(S3Error::NotImplemented(
            "PutBucketAcl supports only the x-amz-acl canned form (private | public-read); \
             AccessControlPolicy XML bodies are out of scope"
                .into(),
        ));
    };
    let acl = parse_canned_acl(Some(raw))?;
    st.metastore.set_bucket_acl(bucket, acl).await?;
    Ok(StatusCode::OK.into_response())
}

/// GET /{bucket}?acl(GetBucketAcl,owner 专属 —— owner 闸在 server.rs 包装层)。
/// public-read 时多一条 AllUsers READ grant,与真 S3 canned ACL 的可视形态一致。
pub async fn get_bucket_acl(st: AppState, bucket: &str) -> Result<Response, S3Error> {
    let meta = st
        .metastore
        .head_bucket(bucket)
        .await?
        .ok_or(S3Error::NoSuchBucket)?;
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

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::S3Error;
use crate::metastore::types::*;
use crate::server::{AppState, S3QueryParams};

pub async fn create_bucket(st: AppState, bucket: &str, owner: &str) -> Result<Response, S3Error> {
    validate_bucket_name(bucket)?;
    st.metastore.create_bucket(bucket, owner).await?;
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

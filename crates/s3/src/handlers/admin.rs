use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::error::S3Error;
use crate::server::AppState;

pub async fn create_credential(st: AppState, req: Request) -> Result<Response, S3Error> {
    check_operator_token(&st, req.headers())?;

    let body = axum::body::to_bytes(req.into_body(), 4096)
        .await
        .map_err(|_| S3Error::InternalError("body read failed".into()))?;

    #[derive(serde::Deserialize)]
    struct CreateReq {
        owner: String,
    }
    let parsed: CreateReq = serde_json::from_slice(&body)
        .map_err(|e| S3Error::InternalError(format!("invalid json: {e}")))?;

    let cred = st.metastore.create_credential(&parsed.owner).await?;

    let json = serde_json::json!({
        "access_key": cred.access_key,
        "secret_key": cred.secret_key,
        "owner": cred.owner,
    });

    Ok((
        StatusCode::OK,
        [("content-type", "application/json")],
        json.to_string(),
    )
        .into_response())
}

pub async fn delete_credential(st: AppState, access_key: &str) -> Result<Response, S3Error> {
    let deleted = st.metastore.delete_credential(access_key).await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT.into_response())
    } else {
        Ok(StatusCode::NOT_FOUND.into_response())
    }
}

pub async fn get_bucket_quota(
    st: AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, S3Error> {
    check_operator_token(&st, req.headers())?;

    let Some(quota) = st.metastore.get_bucket_quota(bucket).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    let json = serde_json::json!({
        "bucket": quota.name,
        "quota_bytes": quota.quota_bytes,
        "used_bytes": quota.used_bytes,
    });
    Ok((
        StatusCode::OK,
        [("content-type", "application/json")],
        json.to_string(),
    )
        .into_response())
}

pub async fn set_bucket_quota(
    st: AppState,
    bucket: &str,
    req: Request,
) -> Result<Response, S3Error> {
    check_operator_token(&st, req.headers())?;

    let body = axum::body::to_bytes(req.into_body(), 4096)
        .await
        .map_err(|_| S3Error::InternalError("body read failed".into()))?;
    #[derive(serde::Deserialize)]
    struct SetReq {
        quota_bytes: u64,
    }
    let parsed: SetReq = serde_json::from_slice(&body)
        .map_err(|e| S3Error::InternalError(format!("invalid json: {e}")))?;
    st.metastore
        .set_bucket_quota(bucket, parsed.quota_bytes)
        .await?;

    let json = serde_json::json!({
        "bucket": bucket,
        "quota_bytes": parsed.quota_bytes,
    });
    Ok((
        StatusCode::OK,
        [("content-type", "application/json")],
        json.to_string(),
    )
        .into_response())
}

fn check_operator_token(st: &AppState, headers: &axum::http::HeaderMap) -> Result<(), S3Error> {
    let configured = &st.config.operator_token;
    if configured.is_empty() {
        return Ok(());
    }
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    match provided {
        Some(token) if token == configured => Ok(()),
        _ => Err(S3Error::AccessDenied("invalid operator token".into())),
    }
}

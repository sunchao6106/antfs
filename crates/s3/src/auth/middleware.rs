//! Axum middleware that authenticates S3 requests with AWS Signature V4.
//!
//! Requests carrying an `Authorization` header are verified as header-signed;
//! requests carrying `X-Amz-Algorithm` query parameters are verified as
//! presigned URLs. `/metrics` and `/admin/*` are served without auth (the
//! admin surface carries its own operator-token auth).

use axum::extract::{Request, State};
use axum::http::HeaderMap;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::S3Error;
use crate::metastore::MetaStore;
use crate::server::AppState;

use super::sigv4::{self, AuthInfo, CanonicalRequest, EMPTY_BODY_SHA256, UNSIGNED_PAYLOAD};

/// Authenticated caller, inserted into request extensions on success.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub owner: String,
    pub access_key: String,
}

/// Axum middleware entry point. Applied to the S3 router in [`crate::server`].
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
    if is_public_path(req.uri().path()) {
        return next.run(req).await;
    }

    // Copy the request parts the verifier needs before the first `.await`;
    // `&Request` is not `Sync`, so it must not be held across an await point.
    let method = req.method().as_str().to_string();
    let uri = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let headers = req.headers().clone();

    match authenticate(
        state.metastore.as_ref(),
        &state.config.region,
        &method,
        &uri,
        &headers,
    )
    .await
    {
        Ok(context) => {
            req.extensions_mut().insert(context);
            next.run(req).await
        }
        Err(error) => error.into_response(),
    }
}

fn is_public_path(path: &str) -> bool {
    path == "/metrics" || path.starts_with("/admin")
}

async fn authenticate(
    metastore: &dyn MetaStore,
    expected_region: &str,
    method: &str,
    uri: &str,
    headers: &HeaderMap,
) -> Result<AuthContext, S3Error> {
    let info = extract_auth_info(uri, headers)?;

    let timestamp = resolve_timestamp(&info, headers)?;
    sigv4::check_time(&info, &timestamp)?;

    let payload_hash = payload_hash(&info, headers);

    let credential = metastore
        .lookup_credential(&info.access_key)
        .await?
        .ok_or(S3Error::InvalidAccessKeyId)?;

    if info.region != expected_region {
        return Err(S3Error::AccessDenied(format!(
            "credential scope region `{}` does not match gateway region `{expected_region}`",
            info.region
        )));
    }

    let exclude_query: &[&str] = if info.presigned {
        &["X-Amz-Signature"]
    } else {
        &[]
    };
    let request = CanonicalRequest {
        method,
        uri,
        headers,
        signed_headers: &info.signed_headers,
        payload_hash: &payload_hash,
        exclude_query,
    };
    sigv4::verify_signature(&credential.secret_key, &timestamp, &info, &request)?;

    Ok(AuthContext {
        owner: credential.owner,
        access_key: credential.access_key,
    })
}

fn extract_auth_info(uri: &str, headers: &HeaderMap) -> Result<AuthInfo, S3Error> {
    if let Some(header) = headers.get(axum::http::header::AUTHORIZATION) {
        let header = header
            .to_str()
            .map_err(|_| S3Error::AccessDenied("malformed authorization header".into()))?;
        return sigv4::parse_authorization_header(header);
    }
    if let Some((_, query)) = uri.split_once('?') {
        if query.contains("X-Amz-Algorithm") {
            return sigv4::parse_presigned(query);
        }
    }
    Err(S3Error::AccessDenied("Missing authentication".into()))
}

fn resolve_timestamp(info: &AuthInfo, headers: &HeaderMap) -> Result<String, S3Error> {
    if let Some(timestamp) = &info.timestamp {
        return Ok(timestamp.clone());
    }
    for name in ["x-amz-date", "date"] {
        if let Some(value) = headers.get(name) {
            if let Ok(value) = value.to_str() {
                return Ok(value.to_string());
            }
        }
    }
    Err(S3Error::AccessDenied("missing x-amz-date header".into()))
}

fn payload_hash(info: &AuthInfo, headers: &HeaderMap) -> String {
    if info.presigned {
        return UNSIGNED_PAYLOAD.to_string();
    }
    headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(EMPTY_BODY_SHA256)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::types::*;
    use crate::metastore::Result as MetaResult;
    use chrono::Utc;

    const ACCESS_KEY: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const OWNER: &str = "alice";

    struct FakeMetaStore {
        credential: Option<S3Credential>,
    }

    fn not_implemented() -> S3Error {
        S3Error::InternalError("not implemented in test fake".into())
    }

    #[async_trait::async_trait]
    impl MetaStore for FakeMetaStore {
        async fn put_object(&self, _req: PutObjectMeta) -> MetaResult<PutObjectOutcome> {
            Err(not_implemented())
        }
        async fn get_object(&self, _bucket: &str, _key: &str) -> MetaResult<Option<ObjectMeta>> {
            Err(not_implemented())
        }
        async fn delete_object(
            &self,
            _bucket: &str,
            _key: &str,
        ) -> MetaResult<Option<DeletedObject>> {
            Err(not_implemented())
        }
        async fn delete_objects(
            &self,
            _bucket: &str,
            _keys: &[String],
        ) -> MetaResult<Vec<DeleteResult>> {
            Err(not_implemented())
        }
        async fn list_objects(&self, _req: ListObjectsReq) -> MetaResult<ListObjectsResp> {
            Err(not_implemented())
        }
        async fn create_bucket(&self, _name: &str, _owner: &str) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn delete_bucket(&self, _name: &str) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn head_bucket(&self, _name: &str) -> MetaResult<Option<BucketMeta>> {
            Err(not_implemented())
        }
        async fn list_buckets(&self, _owner: &str) -> MetaResult<Vec<BucketMeta>> {
            Err(not_implemented())
        }
        async fn get_bucket_quota(&self, _bucket: &str) -> MetaResult<Option<BucketQuota>> {
            Err(not_implemented())
        }
        async fn set_bucket_quota(&self, _bucket: &str, _quota_bytes: u64) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn create_multipart(
            &self,
            _bucket: &str,
            _key: &str,
            _content_type: Option<&str>,
            _user_meta: &UserMeta,
        ) -> MetaResult<String> {
            Err(not_implemented())
        }
        async fn put_part(&self, _upload_id: &str, _part: PartMeta) -> MetaResult<Option<BlobId>> {
            Err(not_implemented())
        }
        async fn complete_multipart(
            &self,
            _upload_id: &str,
            _parts: &[CompletePart],
        ) -> MetaResult<CompleteMultipartResult> {
            Err(not_implemented())
        }
        async fn abort_multipart(&self, _upload_id: &str) -> MetaResult<Vec<BlobId>> {
            Err(not_implemented())
        }
        async fn list_parts(
            &self,
            _upload_id: &str,
            _opts: ListPartsOpts,
        ) -> MetaResult<ListPartsResp> {
            Err(not_implemented())
        }
        async fn list_multipart_uploads(
            &self,
            _bucket: &str,
            _opts: ListUploadsOpts,
        ) -> MetaResult<ListUploadsResp> {
            Err(not_implemented())
        }
        async fn lookup_credential(&self, access_key: &str) -> MetaResult<Option<S3Credential>> {
            Ok(self
                .credential
                .as_ref()
                .filter(|cred| cred.access_key == access_key)
                .cloned())
        }
        async fn create_credential(&self, _owner: &str) -> MetaResult<S3Credential> {
            Err(not_implemented())
        }
        async fn delete_credential(&self, _access_key: &str) -> MetaResult<bool> {
            Err(not_implemented())
        }

        async fn purge_expired_deletions(&self, _cutoff_ms: i64, _limit: u64) -> MetaResult<u64> {
            Err(not_implemented())
        }

        async fn take_expired_uploads(
            &self,
            _cutoff_ms: i64,
            _limit: u64,
        ) -> MetaResult<Vec<String>> {
            Err(not_implemented())
        }

        async fn delete_parts_of_session(&self, _upload_id: &str) -> MetaResult<Vec<BlobId>> {
            Err(not_implemented())
        }
    }

    fn known_store() -> FakeMetaStore {
        FakeMetaStore {
            credential: Some(S3Credential {
                access_key: ACCESS_KEY.to_string(),
                secret_key: SECRET.to_string(),
                owner: OWNER.to_string(),
                created_at: 0,
            }),
        }
    }

    fn now_timestamp() -> String {
        Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
    }

    fn signed_get(uri: &str, timestamp: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        headers.insert("x-amz-date", timestamp.parse().unwrap());
        headers.insert("x-amz-content-sha256", EMPTY_BODY_SHA256.parse().unwrap());

        let signed_headers = vec!["host".to_string(), "x-amz-date".to_string()];
        let info = AuthInfo {
            access_key: ACCESS_KEY.to_string(),
            date: timestamp[..8].to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers: signed_headers.clone(),
            signature: String::new(),
            timestamp: Some(timestamp.to_string()),
            expires: None,
            presigned: false,
        };
        let canonical = CanonicalRequest {
            method: "GET",
            uri,
            headers: &headers,
            signed_headers: &signed_headers,
            payload_hash: EMPTY_BODY_SHA256,
            exclude_query: &[],
        };
        let signature = sigv4::compute_signature(SECRET, timestamp, &info, &canonical).unwrap();

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{}/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature={signature}",
            &timestamp[..8]
        );
        headers.insert("authorization", authorization.parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn authenticates_a_valid_header_signed_request() {
        let timestamp = now_timestamp();
        let headers = signed_get("/bucket/key", &timestamp);
        let context = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap();
        assert_eq!(context.owner, OWNER);
        assert_eq!(context.access_key, ACCESS_KEY);
    }

    #[tokio::test]
    async fn rejects_unknown_access_key() {
        let timestamp = now_timestamp();
        let mut headers = signed_get("/bucket/key", &timestamp);
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential=UNKNOWN/{}/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature={}",
            &timestamp[..8],
            "0".repeat(64)
        );
        *headers.get_mut("authorization").unwrap() = authorization.parse().unwrap();

        let error = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap_err();
        assert!(matches!(error, S3Error::InvalidAccessKeyId));
    }

    #[tokio::test]
    async fn rejects_missing_authentication() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        let error = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap_err();
        assert!(matches!(error, S3Error::AccessDenied(_)));
    }

    #[tokio::test]
    async fn rejects_signature_mismatch() {
        let timestamp = now_timestamp();
        let mut headers = signed_get("/bucket/key", &timestamp);
        let original = headers
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let signature = original.rsplit("Signature=").next().unwrap();
        let first = if signature.starts_with('0') { '1' } else { '0' };
        let tampered = original.replace(signature, &format!("{first}{}", &signature[1..]));
        *headers.get_mut("authorization").unwrap() = tampered.parse().unwrap();

        let error = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap_err();
        assert!(matches!(error, S3Error::SignatureDoesNotMatch));
    }

    #[tokio::test]
    async fn rejects_wrong_region() {
        let timestamp = now_timestamp();
        let headers = signed_get("/bucket/key", &timestamp);
        let error = authenticate(&known_store(), "eu-west-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap_err();
        assert!(matches!(error, S3Error::AccessDenied(_)));
    }

    #[tokio::test]
    async fn authenticates_a_valid_presigned_request() {
        let timestamp = now_timestamp();
        let path = "/bucket/key";
        let signed_headers = vec!["host".to_string()];
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());

        let info = AuthInfo {
            access_key: ACCESS_KEY.to_string(),
            date: timestamp[..8].to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers: signed_headers.clone(),
            signature: String::new(),
            timestamp: Some(timestamp.clone()),
            expires: Some(3600),
            presigned: true,
        };
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={ACCESS_KEY}%2F{}%2Fus-east-1%2Fs3%2Faws4_request&\
             X-Amz-Date={timestamp}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host",
            &timestamp[..8]
        );
        let uri = format!("{path}?{query}");
        let canonical = CanonicalRequest {
            method: "GET",
            uri: &uri,
            headers: &headers,
            signed_headers: &signed_headers,
            payload_hash: UNSIGNED_PAYLOAD,
            exclude_query: &["X-Amz-Signature"],
        };
        let signature = sigv4::compute_signature(SECRET, &timestamp, &info, &canonical).unwrap();
        let uri = format!("{uri}&X-Amz-Signature={signature}");

        let context = authenticate(&known_store(), "us-east-1", "GET", &uri, &headers)
            .await
            .unwrap();
        assert_eq!(context.owner, OWNER);
    }
}

//! S3 request authentication with AWS Signature V4, invoked by the gateway's
//! service-level pipeline ([`crate::server::build_app`]) **before routing**.
//!
//! Each request is resolved into a [`Principal`]: *Signed* (a verified
//! credential, from the `Authorization` header or a presigned `X-Amz-Algorithm`
//! URL) or *Anonymous* — no credential attached at all; the anonymous request
//! is carried through to the route layer, where per-bucket ACL gates decide.
//! **Failed verification is never downgraded to Anonymous** — a request
//! carrying a bad credential is rejected before dispatch.
//! `/metrics` and `/admin/*` are served without credentialed auth (the admin
//! surface carries its own operator-token auth).

use axum::http::HeaderMap;

use crate::error::S3Error;
use crate::metastore::MetaStore;

use super::chunked::ChunkedInfo;
use super::sigv4::{self, AuthInfo, CanonicalRequest, EMPTY_BODY_SHA256, UNSIGNED_PAYLOAD};

/// Authenticated caller (verified SigV4 credential).
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub owner: String,
    pub access_key: String,
}

/// Successful resolution of a credentialed request: the verified caller plus
/// the `aws-chunked` declaration (absent unless the request streams a framed
/// body; see [`crate::auth::chunked`]). Anonymous requests resolve to `None`.
#[derive(Debug)]
pub struct AuthResolution {
    pub context: AuthContext,
    pub chunked: Option<ChunkedInfo>,
}

/// Requester identity as resolved by the middleware: a verified credential, or
/// an anonymous caller (**no credential attached at all**). Requests carrying a
/// credential that fails verification are rejected by the middleware — never
/// downgraded to `Anonymous`.
#[derive(Debug, Clone)]
pub enum Principal {
    Signed(AuthContext),
    Anonymous,
}

/// 公共面(`/metrics`、`/admin/*`)不要求凭证鉴权 —— admin 面自带 operator token。
pub(crate) fn is_public_path(path: &str) -> bool {
    path == "/metrics" || path.starts_with("/admin")
}

/// 验证附着的凭证;`Ok(None)` = 完全无凭证(匿名),`Err` = 凭证存在但验证失败。
/// 由服务级管线(`crate::server::build_app`)在路由分发**之前**调用 —— 验签视角
/// 恒为**客户端原请求**(path 不含桶 + Host 含桶的 vhost 口径,与路径风格口径
/// 各自天然成立)。
pub(crate) async fn authenticate(
    metastore: &dyn MetaStore,
    expected_region: &str,
    method: &str,
    uri: &str,
    headers: &HeaderMap,
) -> Result<Option<AuthResolution>, S3Error> {
    let Some(info) = extract_auth_info(uri, headers)? else {
        return Ok(None);
    };

    let timestamp = resolve_timestamp(&info, headers)?;
    sigv4::check_time(&info, &timestamp)?;

    let chunked_shape = super::chunked::classify(method, headers, info.presigned)?;

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

    let chunked =
        chunked_shape.map(|shape| shape.into_info(&credential.secret_key, &info, &timestamp));

    Ok(Some(AuthResolution {
        context: AuthContext {
            owner: credential.owner,
            access_key: credential.access_key,
        },
        chunked,
    }))
}

/// 提取凭证形状;`Ok(None)` = 完全无凭证(匿名);`Err` = 凭证存在但形状坏(仍 403)。
fn extract_auth_info(uri: &str, headers: &HeaderMap) -> Result<Option<AuthInfo>, S3Error> {
    if let Some(header) = headers.get(axum::http::header::AUTHORIZATION) {
        let header = header
            .to_str()
            .map_err(|_| S3Error::AccessDenied("malformed authorization header".into()))?;
        return Ok(Some(sigv4::parse_authorization_header(header)?));
    }
    if let Some((_, query)) = uri.split_once('?') {
        if query.contains("X-Amz-Algorithm") {
            return Ok(Some(sigv4::parse_presigned(query)?));
        }
    }
    Ok(None)
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
        async fn set_bucket_policy(&self, _bucket: &str, _policy: Option<&str>) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn set_bucket_acl_grants(
            &self,
            _bucket: &str,
            _grants: Option<&str>,
        ) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn set_object_acl_grants(
            &self,
            _bucket: &str,
            _key: &str,
            _grants: Option<&str>,
        ) -> MetaResult<()> {
            Err(not_implemented())
        }
        async fn set_bucket_acl(&self, _bucket: &str, _acl: BucketAcl) -> MetaResult<()> {
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
            _checksum: CompleteChecksum,
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
        async fn pending_reclaims_batch(&self, _limit: u64) -> MetaResult<Vec<BlobId>> {
            Err(not_implemented())
        }
        async fn resolve_pending_reclaims(&self, _blob_ids: &[BlobId]) -> MetaResult<u64> {
            Err(not_implemented())
        }
        async fn pending_reclaims_count(&self) -> MetaResult<u64> {
            Err(not_implemented())
        }
        async fn scan_sse_objects(
            &self,
            _bucket: Option<&str>,
            _after: Option<(String, String)>,
            _limit: u64,
        ) -> MetaResult<Vec<crate::metastore::types::ObjectMeta>> {
            Err(not_implemented())
        }
        async fn session_parts(&self, _upload_id: &str) -> MetaResult<Vec<PartMeta>> {
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
        let resolution = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap()
            .expect("signed request yields a context");
        assert_eq!(resolution.context.owner, OWNER);
        assert_eq!(resolution.context.access_key, ACCESS_KEY);
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
    async fn unsigned_request_resolves_to_anonymous() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        let resolved =
            authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers).await;
        // 无凭证完全缺席 = 匿名(Ok(None)),不再由中间件拒绝(s3-canned-acl 起:
        // 匿名是合法主语,由路由层按桶 ACL 闸门裁决)。
        assert!(matches!(&resolved, Ok(None)));
    }

    #[tokio::test]
    async fn malformed_authorization_is_rejected_not_anonymous() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        headers.insert("authorization", "definitely-not-sigv4".parse().unwrap());
        let error = authenticate(&known_store(), "us-east-1", "GET", "/bucket/key", &headers)
            .await
            .unwrap_err();
        // 安全红线:带坏凭证 ≠ 匿名;必须拒绝,不存在降级路径。
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

        let resolution = authenticate(&known_store(), "us-east-1", "GET", &uri, &headers)
            .await
            .unwrap()
            .expect("presigned request yields a context");
        assert_eq!(resolution.context.owner, OWNER);
    }
}

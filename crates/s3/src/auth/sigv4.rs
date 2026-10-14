//! AWS Signature Version 4 verification for the S3 gateway.
//!
//! This module implements the server side of SigV4: it parses the
//! `Authorization` header (or the `X-Amz-*` presigned query parameters),
//! rebuilds the canonical request, derives the signing key and compares the
//! resulting signature in constant time.
//!
//! Reference: <https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html>

use std::collections::HashMap;

use axum::http::HeaderMap;
use chrono::{Duration, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::S3Error;

type HmacSha256 = Hmac<Sha256>;

/// The only signing algorithm this gateway accepts.
pub const ALGORITHM: &str = "AWS4-HMAC-SHA256";
/// Marker used when the request payload is not part of the signature.
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
/// SHA-256 of an empty body; the default payload hash for body-less requests.
pub const EMPTY_BODY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
/// Maximum accepted difference between request and server time (15 minutes).
pub const MAX_CLOCK_SKEW_SECS: i64 = 15 * 60;
/// AWS caps presigned URL lifetimes at 7 days.
pub const MAX_PRESIGN_EXPIRES_SECS: i64 = 7 * 24 * 60 * 60;

const TERMINATOR: &str = "aws4_request";

/// Credential and signature material extracted from an incoming request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthInfo {
    pub access_key: String,
    /// `YYYYMMDD` from the credential scope.
    pub date: String,
    pub region: String,
    pub service: String,
    /// Lowercased, sorted, de-duplicated signed header names.
    pub signed_headers: Vec<String>,
    /// Lowercase hex signature supplied by the client.
    pub signature: String,
    /// `X-Amz-Date` value; `None` until resolved from the request headers.
    pub timestamp: Option<String>,
    /// Presigned lifetime in seconds (`X-Amz-Expires`).
    pub expires: Option<i64>,
    pub presigned: bool,
}

/// Everything needed to rebuild the canonical request for one HTTP call.
#[derive(Debug)]
pub struct CanonicalRequest<'a> {
    pub method: &'a str,
    /// Path plus query string exactly as received (e.g. `/bucket/key?acl`).
    pub uri: &'a str,
    pub headers: &'a HeaderMap,
    pub signed_headers: &'a [String],
    /// Hex SHA-256 of the body, or [`UNSIGNED_PAYLOAD`].
    pub payload_hash: &'a str,
    /// Query parameter names omitted from the canonical query string (used to
    /// drop `X-Amz-Signature` for presigned URLs).
    pub exclude_query: &'a [&'a str],
}

impl CanonicalRequest<'_> {
    /// Build the canonical request string defined by the SigV4 spec.
    pub fn build(&self) -> Result<String, S3Error> {
        let (path, query) = split_uri(self.uri);
        let canonical_uri = canonical_uri(path);
        let canonical_query = canonical_querystring(query, self.exclude_query);
        let canonical_headers = canonical_headers(self.headers, self.signed_headers)?;

        let mut names: Vec<String> = self
            .signed_headers
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect();
        names.sort();
        names.dedup();
        let signed = names.join(";");

        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            self.method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed,
            self.payload_hash
        ))
    }
}

/// Parse an `Authorization` header of the form
/// `AWS4-HMAC-SHA256 Credential=.../..., SignedHeaders=..., Signature=...`.
pub fn parse_authorization_header(header: &str) -> Result<AuthInfo, S3Error> {
    let rest = header
        .strip_prefix(ALGORITHM)
        .ok_or_else(|| malformed(format!("unsupported algorithm, expected {ALGORITHM}")))?
        .trim_start();

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for field in rest.split(',') {
        let field = field.trim();
        if let Some(value) = field.strip_prefix("Credential=") {
            credential = Some(value);
        } else if let Some(value) = field.strip_prefix("SignedHeaders=") {
            signed_headers = Some(value);
        } else if let Some(value) = field.strip_prefix("Signature=") {
            signature = Some(value);
        }
    }

    let credential = credential.ok_or_else(|| malformed("missing Credential"))?;
    let signed_headers = signed_headers.ok_or_else(|| malformed("missing SignedHeaders"))?;
    let signature = signature.ok_or_else(|| malformed("missing Signature"))?;

    let (access_key, date, region, service) = parse_scope(credential)?;
    let signed_headers = parse_signed_headers(signed_headers)?;
    validate_signature_hex(signature)?;

    Ok(AuthInfo {
        access_key,
        date,
        region,
        service,
        signed_headers,
        signature: signature.to_string(),
        timestamp: None,
        expires: None,
        presigned: false,
    })
}

/// Parse the `X-Amz-*` query parameters of a presigned URL.
pub fn parse_presigned(query: &str) -> Result<AuthInfo, S3Error> {
    let params = query_params(query);

    let algorithm = get_param(&params, "X-Amz-Algorithm")?;
    if algorithm != ALGORITHM {
        return Err(malformed(format!("unsupported algorithm {algorithm}")));
    }

    let credential = get_param(&params, "X-Amz-Credential")?;
    let (access_key, date, region, service) = parse_scope(&credential)?;

    let timestamp = get_param(&params, "X-Amz-Date")?;
    let expires_raw = get_param(&params, "X-Amz-Expires")?;
    let expires: i64 = expires_raw
        .parse()
        .map_err(|_| malformed("X-Amz-Expires must be an integer"))?;
    if !(1..=MAX_PRESIGN_EXPIRES_SECS).contains(&expires) {
        return Err(malformed("X-Amz-Expires out of range"));
    }

    let signed_headers = parse_signed_headers(&get_param(&params, "X-Amz-SignedHeaders")?)?;
    let signature = get_param(&params, "X-Amz-Signature")?;
    validate_signature_hex(&signature)?;

    Ok(AuthInfo {
        access_key,
        date,
        region,
        service,
        signed_headers,
        signature,
        timestamp: Some(timestamp),
        expires: Some(expires),
        presigned: true,
    })
}

/// Derive the SigV4 signing key for a (secret, date, region, service) tuple.
pub fn signing_key(
    secret_key: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Result<[u8; 32], S3Error> {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, TERMINATOR.as_bytes())
}

/// The `<date>/<region>/<service>/aws4_request` credential scope.
pub fn credential_scope(info: &AuthInfo) -> String {
    format!(
        "{}/{}/{}/{}",
        info.date, info.region, info.service, TERMINATOR
    )
}

/// Build the SigV4 string to sign from a raw canonical request.
pub fn string_to_sign(timestamp: &str, scope: &str, canonical_request: &str) -> String {
    let request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    format!("{ALGORITHM}\n{timestamp}\n{scope}\n{request_hash}")
}

/// Compute the lowercase-hex signature the client should have sent.
pub fn compute_signature(
    secret_key: &str,
    timestamp: &str,
    info: &AuthInfo,
    request: &CanonicalRequest<'_>,
) -> Result<String, S3Error> {
    let canonical_request = request.build()?;
    let scope = credential_scope(info);
    let to_sign = string_to_sign(timestamp, &scope, &canonical_request);
    let key = signing_key(secret_key, &info.date, &info.region, &info.service)?;
    let signature = hmac_sha256(&key, to_sign.as_bytes())?;
    Ok(hex::encode(signature))
}

/// Verify a request signature in constant time.
pub fn verify_signature(
    secret_key: &str,
    timestamp: &str,
    info: &AuthInfo,
    request: &CanonicalRequest<'_>,
) -> Result<(), S3Error> {
    let expected = compute_signature(secret_key, timestamp, info, request)?;
    constant_time_eq_hex(&expected, &info.signature)
}

/// Enforce the clock-skew and presigned-expiry rules against the server clock.
pub fn check_time(info: &AuthInfo, timestamp: &str) -> Result<(), S3Error> {
    let request_time =
        parse_timestamp(timestamp).ok_or_else(|| malformed("X-Amz-Date must be ISO8601 basic"))?;
    let now = Utc::now().naive_utc();

    match info.expires {
        Some(expires) => {
            let expiry = request_time + Duration::seconds(expires);
            if now > expiry {
                return Err(S3Error::AccessDenied("Request has expired".into()));
            }
            if (request_time - now).num_seconds() > MAX_CLOCK_SKEW_SECS {
                return Err(S3Error::RequestTimeTooSkewed);
            }
        }
        None => {
            if (now - request_time).num_seconds().abs() > MAX_CLOCK_SKEW_SECS {
                return Err(S3Error::RequestTimeTooSkewed);
            }
        }
    }
    Ok(())
}

/// Constant-time comparison of two hex-encoded signatures.
pub fn constant_time_eq_hex(expected: &str, provided: &str) -> Result<(), S3Error> {
    let expected = hex::decode(expected).map_err(|_| S3Error::SignatureDoesNotMatch)?;
    let provided = hex::decode(provided).map_err(|_| S3Error::SignatureDoesNotMatch)?;
    if expected.len() != provided.len() {
        return Err(S3Error::SignatureDoesNotMatch);
    }
    if expected.ct_eq(&provided).into() {
        Ok(())
    } else {
        Err(S3Error::SignatureDoesNotMatch)
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], S3Error> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| S3Error::InternalError(format!("hmac key init failed: {e}")))?;
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn parse_scope(credential: &str) -> Result<(String, String, String, String), S3Error> {
    let parts: Vec<&str> = credential.split('/').collect();
    if parts.len() != 5 || parts.iter().any(|p| p.is_empty()) {
        return Err(malformed("invalid credential scope"));
    }
    if parts[4] != TERMINATOR {
        return Err(malformed("credential scope must end with aws4_request"));
    }
    if parts[1].len() != 8 || !parts[1].bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed("credential date must be YYYYMMDD"));
    }
    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
        parts[3].to_string(),
    ))
}

fn parse_signed_headers(raw: &str) -> Result<Vec<String>, S3Error> {
    let mut headers: Vec<String> = raw
        .split(';')
        .map(|name| name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect();
    if headers.is_empty() {
        return Err(malformed("SignedHeaders must not be empty"));
    }
    headers.sort();
    headers.dedup();
    Ok(headers)
}

fn validate_signature_hex(signature: &str) -> Result<(), S3Error> {
    if signature.len() != 64 || !signature.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(malformed("signature must be 64 hex characters"));
    }
    Ok(())
}

fn query_params(query: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        let key = percent_decode_str(key).decode_utf8_lossy().into_owned();
        let value = percent_decode_str(value).decode_utf8_lossy().into_owned();
        params.insert(key, value);
    }
    params
}

fn get_param(params: &HashMap<String, String>, key: &str) -> Result<String, S3Error> {
    params
        .get(key)
        .cloned()
        .ok_or_else(|| malformed(format!("missing presigned parameter {key}")))
}

fn parse_timestamp(timestamp: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(timestamp, "%Y%m%dT%H%M%SZ").ok()
}

/// AWS URI-encoding set: unreserved characters (`A-Za-z0-9-_.~`) stay literal.
const URI_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn uri_encode(input: &str) -> String {
    utf8_percent_encode(input, URI_ENCODE_SET).to_string()
}

fn split_uri(uri: &str) -> (&str, &str) {
    match uri.split_once('?') {
        Some((path, query)) => (path, query),
        None => (uri, ""),
    }
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let decoded = percent_decode_str(path).decode_utf8_lossy();
    decoded
        .split('/')
        .map(uri_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn canonical_querystring(query: &str, exclude: &[&str]) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        let key = percent_decode_str(key).decode_utf8_lossy();
        if exclude.iter().any(|skip| *skip == key.as_ref()) {
            continue;
        }
        let value = percent_decode_str(value).decode_utf8_lossy();
        pairs.push((uri_encode(&key), uri_encode(&value)));
    }
    pairs.sort();
    pairs
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &HeaderMap, signed: &[String]) -> Result<String, S3Error> {
    let mut names: Vec<String> = signed
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    names.sort();
    names.dedup();

    let mut out = String::new();
    for name in names {
        let values = headers.get_all(name.as_str());
        if values.iter().next().is_none() {
            return Err(malformed(format!("signed header `{name}` not present")));
        }
        let joined = values
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(normalize_header_value)
                    .map_err(|_| malformed(format!("signed header `{name}` is not valid UTF-8")))
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        out.push_str(&name);
        out.push(':');
        out.push_str(&joined);
        out.push('\n');
    }
    Ok(out)
}

fn normalize_header_value(value: &str) -> String {
    let trimmed = value.trim();
    let mut out = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for ch in trimmed.chars() {
        if ch == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out
}

fn malformed(message: impl Into<String>) -> S3Error {
    S3Error::AccessDenied(format!("malformed authorization: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const TIMESTAMP: &str = "20150830T123600Z";
    const AWS_SIGNATURE: &str = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7";

    fn aws_example_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/x-www-form-urlencoded; charset=utf-8"
                .parse()
                .unwrap(),
        );
        headers.insert("host", "iam.amazonaws.com".parse().unwrap());
        headers.insert("x-amz-date", TIMESTAMP.parse().unwrap());
        headers
    }

    fn aws_example_signed_headers() -> Vec<String> {
        vec![
            "content-type".to_string(),
            "host".to_string(),
            "x-amz-date".to_string(),
        ]
    }

    fn aws_example_request<'a>(
        headers: &'a HeaderMap,
        signed: &'a [String],
    ) -> CanonicalRequest<'a> {
        CanonicalRequest {
            method: "GET",
            uri: "/?Action=ListUsers&Version=2010-05-08",
            headers,
            signed_headers: signed,
            payload_hash: EMPTY_BODY_SHA256,
            exclude_query: &[],
        }
    }

    fn aws_example_info() -> AuthInfo {
        AuthInfo {
            access_key: "AKIDEXAMPLE".to_string(),
            date: "20150830".to_string(),
            region: "us-east-1".to_string(),
            service: "iam".to_string(),
            signed_headers: aws_example_signed_headers(),
            signature: AWS_SIGNATURE.to_string(),
            timestamp: Some(TIMESTAMP.to_string()),
            expires: None,
            presigned: false,
        }
    }

    #[test]
    fn canonical_request_matches_aws_documented_example() {
        let headers = aws_example_headers();
        let signed = aws_example_signed_headers();
        let request = aws_example_request(&headers, &signed);

        let expected = format!(
            "GET\n/\nAction=ListUsers&Version=2010-05-08\n\
             content-type:application/x-www-form-urlencoded; charset=utf-8\n\
             host:iam.amazonaws.com\n\
             x-amz-date:{TIMESTAMP}\n\n\
             content-type;host;x-amz-date\n{EMPTY_BODY_SHA256}"
        );
        assert_eq!(request.build().unwrap(), expected);
    }

    #[test]
    fn string_to_sign_hashes_canonical_request() {
        let headers = aws_example_headers();
        let signed = aws_example_signed_headers();
        let canonical = aws_example_request(&headers, &signed).build().unwrap();
        let scope = credential_scope(&aws_example_info());

        let expected = format!(
            "AWS4-HMAC-SHA256\n{TIMESTAMP}\n{scope}\n\
             f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59"
        );
        assert_eq!(string_to_sign(TIMESTAMP, &scope, &canonical), expected);
    }

    #[test]
    fn signing_key_matches_aws_documented_example() {
        let key = signing_key(SECRET, "20150830", "us-east-1", "iam").unwrap();
        assert_eq!(
            hex::encode(key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn signature_matches_aws_documented_example() {
        let headers = aws_example_headers();
        let signed = aws_example_signed_headers();
        let request = aws_example_request(&headers, &signed);
        let info = aws_example_info();

        let signature = compute_signature(SECRET, TIMESTAMP, &info, &request).unwrap();
        assert_eq!(signature, AWS_SIGNATURE);
        assert!(verify_signature(SECRET, TIMESTAMP, &info, &request).is_ok());
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let headers = aws_example_headers();
        let signed = aws_example_signed_headers();
        let request = aws_example_request(&headers, &signed);
        let mut info = aws_example_info();
        info.signature = info.signature.replace('5', "6");

        assert!(matches!(
            verify_signature(SECRET, TIMESTAMP, &info, &request),
            Err(S3Error::SignatureDoesNotMatch)
        ));
    }

    #[test]
    fn constant_time_compare_handles_equal_and_unequal() {
        assert!(constant_time_eq_hex("00ff", "00ff").is_ok());
        assert!(constant_time_eq_hex("00ff", "00fe").is_err());
        // Different lengths must fail without panicking.
        assert!(constant_time_eq_hex("00ff", "00ff00").is_err());
    }

    #[test]
    fn parses_a_valid_authorization_header() {
        let header = format!(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-date, Signature={AWS_SIGNATURE}"
        );
        let info = parse_authorization_header(&header).unwrap();
        assert_eq!(info.access_key, "AKIDEXAMPLE");
        assert_eq!(info.date, "20150830");
        assert_eq!(info.region, "us-east-1");
        assert_eq!(info.service, "s3");
        assert_eq!(info.signed_headers, vec!["host", "x-amz-date"]);
        assert_eq!(info.signature, AWS_SIGNATURE);
        assert!(!info.presigned);
        assert_eq!(info.timestamp, None);
    }

    #[test]
    fn rejects_malformed_authorization_headers() {
        assert!(parse_authorization_header("").is_err());
        assert!(parse_authorization_header("Basic abc").is_err());
        // Missing Signature.
        assert!(parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/s3/aws4_request, \
             SignedHeaders=host"
        )
        .is_err());
        // Scope not ending in aws4_request.
        assert!(parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/s3/other, \
             SignedHeaders=host, Signature=deadbeef"
        )
        .is_err());
        // Signature not 64 hex chars.
        assert!(parse_authorization_header(
            "AWS4-HMAC-SHA256 Credential=AKID/20150830/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=deadbeef"
        )
        .is_err());
    }

    #[test]
    fn parses_presigned_query_parameters() {
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2Fus-east-1%2Fs3%2Faws4_request&\
             X-Amz-Date={TIMESTAMP}&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature={AWS_SIGNATURE}"
        );
        let info = parse_presigned(&query).unwrap();
        assert!(info.presigned);
        assert_eq!(info.access_key, "AKIDEXAMPLE");
        assert_eq!(info.region, "us-east-1");
        assert_eq!(info.service, "s3");
        assert_eq!(info.expires, Some(3600));
        assert_eq!(info.timestamp.as_deref(), Some(TIMESTAMP));
        assert_eq!(info.signed_headers, vec!["host"]);
    }

    #[test]
    fn presigned_signature_excludes_signature_param() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        let signed = vec!["host".to_string()];
        let info = AuthInfo {
            access_key: "AKIDEXAMPLE".to_string(),
            date: "20150830".to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers: signed.clone(),
            signature: String::new(),
            timestamp: Some(TIMESTAMP.to_string()),
            expires: Some(3600),
            presigned: true,
        };
        let uri = format!(
            "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date={TIMESTAMP}&\
             X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=IRRELEVANT"
        );
        let request = CanonicalRequest {
            method: "GET",
            uri: &uri,
            headers: &headers,
            signed_headers: &signed,
            payload_hash: UNSIGNED_PAYLOAD,
            exclude_query: &["X-Amz-Signature"],
        };
        let canonical = request.build().unwrap();
        assert!(!canonical.contains("IRRELEVANT"));
        assert!(canonical.contains("X-Amz-Expires=3600"));
        assert!(canonical.ends_with(UNSIGNED_PAYLOAD));

        let signature = compute_signature(SECRET, TIMESTAMP, &info, &request).unwrap();
        let mut verified = info;
        verified.signature = signature;
        assert!(verify_signature(SECRET, TIMESTAMP, &verified, &request).is_ok());
    }

    #[test]
    fn unsigned_payload_is_used_verbatim() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "s3.example.com".parse().unwrap());
        let signed = vec!["host".to_string()];
        let request = CanonicalRequest {
            method: "PUT",
            uri: "/bucket/key",
            headers: &headers,
            signed_headers: &signed,
            payload_hash: UNSIGNED_PAYLOAD,
            exclude_query: &[],
        };
        assert!(request.build().unwrap().ends_with(UNSIGNED_PAYLOAD));
    }

    #[test]
    fn presigned_url_within_expiry_is_accepted() {
        let now = Utc::now();
        let info = AuthInfo {
            access_key: "AKIDEXAMPLE".to_string(),
            date: now.format("%Y%m%d").to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers: vec!["host".to_string()],
            signature: AWS_SIGNATURE.to_string(),
            timestamp: Some(now.format("%Y%m%dT%H%M%SZ").to_string()),
            expires: Some(3600),
            presigned: true,
        };
        assert!(check_time(&info, info.timestamp.as_deref().unwrap()).is_ok());
    }

    #[test]
    fn expired_presigned_url_is_rejected() {
        let issued = Utc::now() - Duration::hours(2);
        let info = AuthInfo {
            access_key: "AKIDEXAMPLE".to_string(),
            date: issued.format("%Y%m%d").to_string(),
            region: "us-east-1".to_string(),
            service: "s3".to_string(),
            signed_headers: vec!["host".to_string()],
            signature: AWS_SIGNATURE.to_string(),
            timestamp: Some(issued.format("%Y%m%dT%H%M%SZ").to_string()),
            expires: Some(60),
            presigned: true,
        };
        assert!(matches!(
            check_time(&info, info.timestamp.as_deref().unwrap()),
            Err(S3Error::AccessDenied(_))
        ));
    }

    #[test]
    fn clock_skew_beyond_fifteen_minutes_is_rejected() {
        let stale = Utc::now() - Duration::minutes(20);
        let info = AuthInfo {
            timestamp: Some(stale.format("%Y%m%dT%H%M%SZ").to_string()),
            ..aws_example_info()
        };
        assert!(matches!(
            check_time(&info, info.timestamp.as_deref().unwrap()),
            Err(S3Error::RequestTimeTooSkewed)
        ));

        let future = Utc::now() + Duration::minutes(20);
        let info = AuthInfo {
            timestamp: Some(future.format("%Y%m%dT%H%M%SZ").to_string()),
            ..aws_example_info()
        };
        assert!(matches!(
            check_time(&info, info.timestamp.as_deref().unwrap()),
            Err(S3Error::RequestTimeTooSkewed)
        ));
    }

    #[test]
    fn canonical_uri_encodes_segments_but_keeps_slashes() {
        assert_eq!(canonical_uri(""), "/");
        assert_eq!(canonical_uri("/"), "/");
        assert_eq!(canonical_uri("/bucket/my key.txt"), "/bucket/my%20key.txt");
        assert_eq!(
            canonical_uri("/bucket/my%20key.txt"),
            "/bucket/my%20key.txt"
        );
    }

    #[test]
    fn canonical_querystring_sorts_and_encodes() {
        let out = canonical_querystring("b=2&a=1&a=0", &[]);
        assert_eq!(out, "a=0&a=1&b=2");
        assert_eq!(canonical_querystring("k=a%20b", &[]), "k=a%20b");
    }
}

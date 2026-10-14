//! SigV4 authentication (task 13.4): signed requests, tampering, unknown keys, unsigned payloads,
//! and operator credential management.
//!
//! All tests are `#[ignore]`d — they need a live PostgreSQL via `TEST_S3_POSTGRES_URL`.

mod common;

use common::{signed_headers_for, S3Fixture};
use reqwest::Method;

fn tamper_signature(authorization: &str) -> String {
    let marker = "Signature=";
    let prefix_len = authorization
        .find(marker)
        .expect("authorization has Signature=")
        + marker.len();
    format!("{}{}", &authorization[..prefix_len], "0".repeat(64))
}

/// A correctly signed request is accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn valid_signature_accepted() {
    let fx = S3Fixture::start().await;
    let resp = fx.list_buckets().await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "signed request must be accepted"
    );
}

/// A tampered signature is rejected with 403.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn wrong_signature_rejected() {
    let fx = S3Fixture::start().await;
    let mut headers = fx.signed_headers("GET", "/", &[]);
    let authorization = headers
        .get("authorization")
        .expect("signed headers carry authorization")
        .to_str()
        .expect("authorization is utf8")
        .to_string();
    headers.insert(
        "authorization",
        tamper_signature(&authorization)
            .parse()
            .expect("valid header value"),
    );

    let resp = fx.send(Method::GET, "/", headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 403, "tampered signature must 403");
}

/// A signature made with a non-existent access key is rejected with 403.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn unknown_access_key_rejected() {
    let fx = S3Fixture::start().await;
    let headers = signed_headers_for(
        &fx.s3_addr,
        "AKIAUNKNOWNAccessKey",
        "unknown-secret-key",
        "GET",
        "/",
        &[],
    );

    let resp = fx.send(Method::GET, "/", headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 403, "unknown access key must 403");
}

/// A request declaring `UNSIGNED-PAYLOAD` is accepted (the body is excluded from the signature).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn unsigned_payload_accepted() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("unsigned").await;
    let body = b"payload excluded from the signature";

    let resp = fx.put_object_unsigned(&bucket, "k", body).await;
    assert_eq!(resp.status().as_u16(), 200, "UNSIGNED-PAYLOAD PUT must 200");

    let resp = fx.get_object(&bucket, "k").await;
    assert_eq!(resp.status().as_u16(), 200, "GET must 200");
    let got = resp.bytes().await.expect("read body");
    assert_eq!(got.as_ref(), body, "stored body must match");
}

/// The admin credential endpoint creates and deletes credentials.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn credential_crud() {
    let fx = S3Fixture::start().await;

    let resp = fx
        .http
        .post(fx.url("/admin/credentials"))
        .json(&serde_json::json!({ "owner": "crud-owner" }))
        .send()
        .await
        .expect("create credential request");
    assert_eq!(resp.status().as_u16(), 200, "create credential must 200");
    let cred: serde_json::Value = resp.json().await.expect("credential json");
    let access_key = cred["access_key"]
        .as_str()
        .expect("access_key present")
        .to_string();
    assert!(!access_key.is_empty(), "access_key must not be empty");
    assert!(
        cred["secret_key"].as_str().is_some_and(|s| !s.is_empty()),
        "secret_key must not be empty"
    );

    let resp = fx
        .http
        .delete(fx.url(&format!("/admin/credentials/{access_key}")))
        .send()
        .await
        .expect("delete credential request");
    assert_eq!(resp.status().as_u16(), 204, "delete credential must 204");
}

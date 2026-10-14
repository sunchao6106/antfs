//! Object-level S3 operations (task 13.1): PutObject / GetObject / HeadObject / DeleteObject.
//!
//! All tests are `#[ignore]`d — they need a live PostgreSQL via `TEST_S3_POSTGRES_URL`.

mod common;

use common::{header, md5_hex, pseudo_random, S3Fixture};

/// A small object round-trips byte-for-byte and carries an ETag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn put_get_roundtrip_small() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(100);

    let resp = fx.put_object(&bucket, "small.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must return 200");
    assert!(
        resp.headers().contains_key("etag"),
        "PUT must return an ETag header"
    );

    let resp = fx.get_object(&bucket, "small.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "GET must return 200");
    let got = resp.bytes().await.expect("read GET body").to_vec();
    assert_eq!(got, body, "GET body must match the PUT body");
}

/// A 2 MiB object round-trips byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn put_get_roundtrip_large() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(2 * 1024 * 1024);

    let resp = fx.put_object(&bucket, "large.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must return 200");

    let resp = fx.get_object(&bucket, "large.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "GET must return 200");
    let got = resp.bytes().await.expect("read GET body").to_vec();
    assert_eq!(got, body, "large object body must match byte-for-byte");
}

/// HEAD reports the object's size and returns no body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn head_object_metadata() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(1234);

    let resp = fx.put_object(&bucket, "meta.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must return 200");

    let resp = fx.head_object(&bucket, "meta.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "HEAD must return 200");
    assert_eq!(
        header(&resp, "content-length"),
        body.len().to_string(),
        "HEAD Content-Length must equal the object size"
    );
    let got = resp.bytes().await.expect("read HEAD body");
    assert!(got.is_empty(), "HEAD must not carry a body");
}

/// DELETE returns 204 and a following GET returns 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn delete_then_get_404() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(64);

    assert_eq!(
        fx.put_object(&bucket, "gone.bin", &body)
            .await
            .status()
            .as_u16(),
        200,
        "PUT must return 200"
    );
    assert_eq!(
        fx.delete_object(&bucket, "gone.bin")
            .await
            .status()
            .as_u16(),
        204,
        "DELETE must return 204"
    );
    assert_eq!(
        fx.get_object(&bucket, "gone.bin").await.status().as_u16(),
        404,
        "GET after DELETE must return 404"
    );
}

/// Overwriting a key returns the latest body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn overwrite_returns_latest() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;

    assert_eq!(
        fx.put_object(&bucket, "k", b"v1").await.status().as_u16(),
        200,
        "first PUT must return 200"
    );
    assert_eq!(
        fx.put_object(&bucket, "k", b"v2").await.status().as_u16(),
        200,
        "second PUT must return 200"
    );

    let resp = fx.get_object(&bucket, "k").await;
    assert_eq!(resp.status().as_u16(), 200, "GET must return 200");
    let got = resp.bytes().await.expect("read GET body");
    assert_eq!(got.as_ref(), b"v2", "GET must return the latest version");
}

/// The single-part ETag is the lowercase hex MD5 of the body, quoted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn etag_is_md5_hex() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = b"the quick brown fox jumps over the lazy dog";

    let resp = fx.put_object(&bucket, "etag.bin", body).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must return 200");

    let etag = header(&resp, "etag");
    let unquoted = etag.trim_matches('"');
    assert_eq!(
        unquoted,
        md5_hex(body),
        "ETag must be the hex MD5 of the body"
    );
}

/// Object metadata (Content-Type + x-amz-meta-*) round-trips: stored in MetaStore on
/// PutObject, echoed back on GetObject and HeadObject (engine carries no metadata).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn object_metadata_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(64);

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "meta.bin",
            &body,
            &[
                ("content-type", "video/mp4"),
                ("x-amz-meta-project", "demo"),
                ("x-amz-meta-owner", "alice"),
            ],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "PUT with metadata must 200");

    let resp = fx.get_object(&bucket, "meta.bin").await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "content-type"), "video/mp4");
    assert_eq!(header(&resp, "x-amz-meta-project"), "demo");
    assert_eq!(header(&resp, "x-amz-meta-owner"), "alice");

    let resp = fx.head_object(&bucket, "meta.bin").await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "content-type"), "video/mp4");
    assert_eq!(header(&resp, "x-amz-meta-owner"), "alice");
}

/// PutObject without a Content-Type header stores the S3 default binary/octet-stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn default_content_type_when_absent() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let body = pseudo_random(16);

    let resp = fx.put_object(&bucket, "plain.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "PUT must return 200");

    let resp = fx.get_object(&bucket, "plain.bin").await;
    assert_eq!(header(&resp, "content-type"), "binary/octet-stream");

    let resp = fx.head_object(&bucket, "plain.bin").await;
    assert_eq!(header(&resp, "content-type"), "binary/octet-stream");
}

/// x-amz-meta-* headers totaling over 2048 bytes get rejected with 400 MetadataTooLarge
/// and no object is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn metadata_too_large_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let big = "x".repeat(1100);

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "bigmeta.bin",
            b"payload",
            &[("x-amz-meta-a", &big), ("x-amz-meta-b", &big)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "oversized metadata must 400");
    let text = resp.text().await.expect("error body");
    assert!(text.contains("MetadataTooLarge"), "error code: {text}");

    let resp = fx.get_object(&bucket, "bigmeta.bin").await;
    assert_eq!(resp.status().as_u16(), 404, "no object must be written");
}

/// A Content-Type header longer than the 256-byte storage column gets rejected with
/// 400 InvalidArgument (not a 500 from a failed INSERT).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn oversized_content_type_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("obj").await;
    let ct = "a".repeat(257);

    let resp = fx
        .put_object_with_headers(&bucket, "longct.bin", b"payload", &[("content-type", &ct)])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "oversized Content-Type must 400"
    );
    let text = resp.text().await.expect("error body");
    assert!(text.contains("InvalidArgument"), "error code: {text}");

    let resp = fx.get_object(&bucket, "longct.bin").await;
    assert_eq!(resp.status().as_u16(), 404, "no object must be written");
}

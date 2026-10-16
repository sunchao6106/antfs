//! Server-side encryption (`s3-sse`): AES256 envelope, transparent reads.
//!
//! All tests are `#[ignore]`d — they need a live database via
//! `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL` (same as the other e2e).

mod common;

use common::{header, md5_hex, pseudo_random, S3Fixture};

const SSE_HDR: (&str, &str) = ("x-amz-server-side-encryption", "AES256");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_put_get_head_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("sse").await;
    let data = pseudo_random(200_001);

    let put = fx
        .put_object_with_headers(&bucket, "secret.bin", &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200, "sse PUT must 200");
    assert_eq!(header(&put, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(header(&put, "etag"), format!("\"{}\"", md5_hex(&data)));

    let get = fx.get_object(&bucket, "secret.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(header(&get, "content-length"), data.len().to_string());
    let body = get.bytes().await.expect("get body");
    assert_eq!(body.as_ref(), data.as_slice());

    let head = fx.head_object(&bucket, "secret.bin").await;
    assert_eq!(head.status().as_u16(), 200);
    assert_eq!(header(&head, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(header(&head, "content-length"), data.len().to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_blob_differs_from_plaintext() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ssecipher").await;
    let data = pseudo_random(9_001);

    fx.put_object_with_headers(&bucket, "secret.bin", &data, &[SSE_HDR])
        .await;

    let meta = fx
        .state
        .metastore
        .get_object(&bucket, "secret.bin")
        .await
        .expect("meta query")
        .expect("row present");
    assert!(meta.sse.is_some(), "row must carry the sse descriptor");
    let blob = fx
        .state
        .router
        .get(&meta.blob_id)
        .await
        .expect("engine read");
    assert_ne!(blob.as_ref(), data.as_slice());
    assert_eq!(&blob.as_ref()[..8], b"ANTFSSE1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_unconfigured_gateway_is_501() {
    let fx = S3Fixture::start_with_sse(false).await;
    let bucket = fx.create_unique_bucket("ssoff").await;
    let resp = fx
        .put_object_with_headers(&bucket, "a.bin", b"x", &[SSE_HDR])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        501,
        "no master key -> capability is out"
    );
    let xml = resp.text().await.expect("501 body");
    assert!(xml.contains("<Code>NotImplemented</Code>"), "{xml}");
    assert_eq!(fx.get_object(&bucket, "a.bin").await.status().as_u16(), 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_bad_value_and_multipart_are_gated() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ssegate").await;
    fx.put_object(&bucket, "src", b"x").await;

    let bad = fx
        .put_object_with_headers(
            &bucket,
            "bad.bin",
            b"x",
            &[("x-amz-server-side-encryption", "aws:kms")],
        )
        .await;
    assert_eq!(bad.status().as_u16(), 400, "only AES256 is valid");
    let xml = bad.text().await.expect("400 body");
    assert!(xml.contains("<Code>InvalidArgument</Code>"), "{xml}");

    let create = fx
        .create_multipart_with_headers(&bucket, "m.bin", &[SSE_HDR])
        .await;
    assert_eq!(
        create.status().as_u16(),
        501,
        "multipart sse is out this round"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_quota_gates_the_write() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ssequota").await;
    let data = pseudo_random(64 * 1024);
    fx.put_object(&bucket, "src.bin", &data).await;
    fx.set_bucket_quota(&bucket, 16 * 1024).await;

    let resp = fx
        .put_object_with_headers(&bucket, "copy-of-src.bin", &data, &[SSE_HDR])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        507,
        "quota gates encrypted writes too"
    );
    let xml = resp.text().await.expect("507 body");
    assert!(xml.contains("<Code>QuotaExceeded</Code>"), "{xml}");
    assert_eq!(
        fx.get_object(&bucket, "copy-of-src.bin")
            .await
            .status()
            .as_u16(),
        404,
        "quota-rejected encrypted write must not create the object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_copy_dest_opt_in_and_rekey() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ssecopy").await;
    let data = pseudo_random(5_123);

    fx.put_object_with_headers(&bucket, "plain-src.bin", &data, &[])
        .await;
    let copy_in = fx
        .put_object_with_headers(
            &bucket,
            "encrypted-dest.bin",
            b"",
            &[
                (
                    "x-amz-copy-source",
                    format!("/{bucket}/plain-src.bin").as_str(),
                ),
                SSE_HDR,
            ],
        )
        .await;
    assert_eq!(
        copy_in.status().as_u16(),
        200,
        "copy with sse header encrypts the destination"
    );
    assert_eq!(header(&copy_in, "x-amz-server-side-encryption"), "AES256");
    let get = fx.get_object(&bucket, "encrypted-dest.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    let body = get.bytes().await.expect("body");
    assert_eq!(body.as_ref(), data.as_slice());

    let copy_out = fx
        .put_object_with_headers(
            &bucket,
            "plain-dest.bin",
            b"",
            &[(
                "x-amz-copy-source",
                format!("/{bucket}/encrypted-dest.bin").as_str(),
            )],
        )
        .await;
    assert_eq!(
        copy_out.status().as_u16(),
        200,
        "copying an encrypted source rewrites plaintext"
    );
    assert!(
        copy_out
            .headers()
            .get("x-amz-server-side-encryption")
            .is_none(),
        "destination without the header is plaintext"
    );
    let meta = fx
        .state
        .metastore
        .get_object(&bucket, "plain-dest.bin")
        .await
        .expect("meta")
        .expect("row");
    assert!(meta.sse.is_none(), "dest row carries no sse descriptor");
    let blob = fx
        .state
        .router
        .get(&meta.blob_id)
        .await
        .expect("engine read");
    assert_eq!(
        blob.as_ref(),
        data.as_slice(),
        "dest bytes are plaintext (re-keyed to none)"
    );
}

//! Server-side object copy and object attributes (`s3-copy-attributes`).
//!
//! All tests are `#[ignore]`d — they need a live database via
//! `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL` (same as the other e2e).

mod common;

use chrono::{Duration, Utc};
use common::{header, pseudo_random, S3Fixture};
use reqwest::Method;

/// Copies replicate bytes and report `CopyObjectResult` (quoted ETag + ISO date).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copy").await;
    let data = pseudo_random(120_000);

    let put = fx.put_object(&bucket, "src.bin", &data).await;
    assert_eq!(put.status().as_u16(), 200);
    let src_etag = header(&put, "etag");

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "dest.bin",
            b"",
            &[("x-amz-copy-source", &format!("/{bucket}/src.bin"))],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "copy must 200");
    let xml = resp.text().await.expect("copy body");
    assert!(xml.contains("<CopyObjectResult"), "{xml}");
    assert!(
        xml.contains(&format!(
            "<ETag>&quot;{}&quot;</ETag>",
            src_etag.trim_matches('"')
        )),
        "{xml}"
    );
    assert!(xml.contains("<LastModified>"), "{xml}");

    let get = fx.get_object(&bucket, "dest.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "etag"), src_etag);
    assert_eq!(header(&get, "content-length"), data.len().to_string());
    let body = get.bytes().await.expect("dest body");
    assert_eq!(body.as_ref(), data.as_slice());
}

/// COPY (default) replicates source metadata; REPLACE recaptures from the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_metadata_directives() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copymeta").await;

    let put = fx
        .put_object_with_headers(
            &bucket,
            "src.bin",
            b"metadata-payload",
            &[
                ("content-type", "text/plain"),
                ("x-amz-meta-team", "core-storage"),
            ],
        )
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let copy_source = format!("/{bucket}/src.bin");
    let copied = fx
        .put_object_with_headers(
            &bucket,
            "copy-default.bin",
            b"",
            &[("x-amz-copy-source", copy_source.as_str())],
        )
        .await;
    assert_eq!(copied.status().as_u16(), 200);
    let head = fx.head_object(&bucket, "copy-default.bin").await;
    assert_eq!(header(&head, "content-type"), "text/plain");
    assert_eq!(header(&head, "x-amz-meta-team"), "core-storage");

    let replace_source = format!("/{bucket}/src.bin");
    let replaced = fx
        .put_object_with_headers(
            &bucket,
            "copy-replace.bin",
            b"",
            &[
                ("x-amz-copy-source", replace_source.as_str()),
                ("x-amz-metadata-directive", "REPLACE"),
                ("content-type", "application/json"),
                ("x-amz-meta-team", "gateway"),
            ],
        )
        .await;
    assert_eq!(replaced.status().as_u16(), 200);
    let head = fx.head_object(&bucket, "copy-replace.bin").await;
    assert_eq!(head.status().as_u16(), 200);
    assert_eq!(header(&head, "content-type"), "application/json");
    assert_eq!(header(&head, "x-amz-meta-team"), "gateway");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_source_conditions() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copycond").await;

    let put = fx.put_object(&bucket, "src", b"conditional").await;
    assert_eq!(put.status().as_u16(), 200);
    let etag = header(&put, "etag");
    let source = format!("/{bucket}/src");
    let past = (Utc::now() - Duration::hours(2)).to_rfc2822();
    let future = (Utc::now() + Duration::hours(2)).to_rfc2822();

    let ok = fx
        .put_object_with_headers(
            &bucket,
            "ok-match",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-match", etag.as_str()),
            ],
        )
        .await;
    assert_eq!(ok.status().as_u16(), 200, "if-match satisfied must copy");

    let wrong = fx
        .put_object_with_headers(
            &bucket,
            "never-match",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                (
                    "x-amz-copy-source-if-match",
                    "\"00000000000000000000000000000000\"",
                ),
            ],
        )
        .await;
    assert_eq!(wrong.status().as_u16(), 412);
    let xml = wrong.text().await.expect("412 body");
    assert!(xml.contains("<Code>PreconditionFailed</Code>"), "{xml}");
    let none_match = fx
        .put_object_with_headers(
            &bucket,
            "never-star",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-none-match", "*"),
            ],
        )
        .await;
    assert_eq!(none_match.status().as_u16(), 412);

    let unmodified_future = fx
        .put_object_with_headers(
            &bucket,
            "ok-unmodified",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-unmodified-since", future.as_str()),
            ],
        )
        .await;
    assert_eq!(
        unmodified_future.status().as_u16(),
        200,
        "unmodified in the future must copy"
    );
    let unmodified_past = fx
        .put_object_with_headers(
            &bucket,
            "never-unmodified",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-unmodified-since", past.as_str()),
            ],
        )
        .await;
    assert_eq!(unmodified_past.status().as_u16(), 412);

    let modified_past = fx
        .put_object_with_headers(
            &bucket,
            "ok-modified",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-modified-since", past.as_str()),
            ],
        )
        .await;
    assert_eq!(
        modified_past.status().as_u16(),
        200,
        "modified since a past date must copy"
    );

    let modified_future = fx
        .put_object_with_headers(
            &bucket,
            "never-modified2",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-copy-source-if-modified-since", future.as_str()),
            ],
        )
        .await;
    assert_eq!(modified_future.status().as_u16(), 412);

    for never in [
        "never-match",
        "never-star",
        "never-unmodified",
        "never-modified2",
    ] {
        assert_eq!(
            fx.get_object(&bucket, never).await.status().as_u16(),
            404,
            "412 paths must not create {never}"
        );
    }
}

/// Self copy with COPY -> 400 InvalidRequest; REPLACE -> metadata rewrites in place.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_self_copy_is_rejected_unless_replacing() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copyself").await;
    let source = format!("/{bucket}/self.bin");

    let put = fx
        .put_object_with_headers(
            &bucket,
            "self.bin",
            b"self",
            &[("x-amz-meta-team", "original")],
        )
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let rejected = fx
        .put_object_with_headers(
            &bucket,
            "self.bin",
            b"",
            &[("x-amz-copy-source", source.as_str())],
        )
        .await;
    assert_eq!(rejected.status().as_u16(), 400);
    let xml = rejected.text().await.expect("400 body");
    assert!(xml.contains("<Code>InvalidRequest</Code>"), "{xml}");

    let rewritten = fx
        .put_object_with_headers(
            &bucket,
            "self.bin",
            b"",
            &[
                ("x-amz-copy-source", source.as_str()),
                ("x-amz-metadata-directive", "REPLACE"),
                ("x-amz-meta-team", "replacement"),
            ],
        )
        .await;
    assert_eq!(rewritten.status().as_u16(), 200);
    let head = fx.head_object(&bucket, "self.bin").await;
    assert_eq!(header(&head, "x-amz-meta-team"), "replacement");
    let get = fx.get_object(&bucket, "self.bin").await;
    assert_eq!(get.bytes().await.expect("body").len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_missing_and_cross_tenant_sources() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copysrc").await;
    fx.put_object(&bucket, "src", b"src").await;

    let missing_key = format!("/{bucket}/missing-key");
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "dest1",
            b"",
            &[("x-amz-copy-source", missing_key.as_str())],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 404);
    let xml = resp.text().await.expect("404 body");
    assert!(xml.contains("<Code>NoSuchKey</Code>"), "{xml}");

    let missing_bucket = "/totally-absent-bkt/src";
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "dest2",
            b"",
            &[("x-amz-copy-source", missing_bucket)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 404);
    let xml = resp.text().await.expect("404 body 2");
    assert!(xml.contains("<Code>NoSuchBucket</Code>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_cross_tenant_public_read_source() {
    let fx = S3Fixture::start().await;
    let public = fx.create_unique_bucket("copypublic").await;
    let payload = b"from-public-bucket".to_vec();
    fx.put_object(&public, "src.bin", &payload).await;

    let acl_uri = format!("/{public}?acl");
    let mut acl_headers = fx.signed_headers("PUT", &acl_uri, b"");
    acl_headers.insert(
        "x-amz-acl",
        "public-read".parse().expect("acl header value"),
    );
    let resp = fx
        .send(Method::PUT, &acl_uri, acl_headers, Vec::new())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "set public-read on {public}");

    let (other_ak, other_sk) = fx.issue_credential("tenant-b").await;
    let dest_bucket = fx.unique("copydest-b");
    let resp = fx
        .request_as(&other_ak, &other_sk, "PUT", &format!("/{dest_bucket}"), b"")
        .await;
    assert_eq!(resp.status().as_u16(), 200, "tenant creates own bucket");

    let copy_uri = format!("/{dest_bucket}/copied.bin");
    let mut headers =
        common::signed_headers_for(&fx.s3_addr, &other_ak, &other_sk, "PUT", &copy_uri, b"");
    headers.insert(
        "x-amz-copy-source",
        format!("/{public}/src.bin")
            .parse()
            .expect("copy source value"),
    );
    let resp = fx.send(Method::PUT, &copy_uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "public-read source is copyable cross-tenant"
    );
    let xml = resp.text().await.expect("copy body");
    assert!(xml.contains("<CopyObjectResult"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_multipart_source_assembles_equal() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copymp").await;

    let part_a = pseudo_random(100_003);
    let part_b = pseudo_random(77_777);
    let initiated = fx.create_multipart(&bucket, "assembled.bin").await;
    let upload_id = common::xml_tag(&initiated.text().await.expect("initiate body"), "UploadId");
    let mut etags: Vec<(i32, String)> = Vec::new();
    for (number, data) in [(1, &part_a), (2, &part_b)] {
        let resp = fx
            .upload_part(&bucket, "assembled.bin", &upload_id, number, data)
            .await;
        assert_eq!(resp.status().as_u16(), 200);
        etags.push((number, header(&resp, "etag")));
    }
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in &etags {
        xml.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    let done = fx
        .complete_multipart(&bucket, "assembled.bin", &upload_id, xml.as_bytes())
        .await;
    assert_eq!(done.status().as_u16(), 200, "source upload must complete");

    let source = format!("/{bucket}/assembled.bin");
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "assembled-copy.bin",
            b"",
            &[("x-amz-copy-source", source.as_str())],
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "multipart source copy must 200"
    );

    let mut expected = part_a.clone();
    expected.extend_from_slice(&part_b);
    let get = fx.get_object(&bucket, "assembled-copy.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "content-length"), expected.len().to_string());
    let body = get.bytes().await.expect("copy body");
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_quota_gates_the_write() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copyquota").await;
    let data = pseudo_random(64 * 1024);
    fx.put_object(&bucket, "src.bin", &data).await;

    fx.set_bucket_quota(&bucket, 16 * 1024).await;
    let source = format!("/{bucket}/src.bin");
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "copy-of-src.bin",
            b"",
            &[("x-amz-copy-source", source.as_str())],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 507, "quota rejects the copy");
    let xml = resp.text().await.expect("507 body");
    assert!(xml.contains("<Code>QuotaExceeded</Code>"), "{xml}");
    assert_eq!(
        fx.get_object(&bucket, "copy-of-src.bin")
            .await
            .status()
            .as_u16(),
        404,
        "quota-rejected copy must not create the destination"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn copy_object_rejects_body_and_versioned_source() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("copybody").await;
    fx.put_object(&bucket, "src.bin", b"byte").await;

    let source = format!("/{bucket}/src.bin");
    let uri = format!("/{bucket}/dest-with-body.bin");
    let mut headers = fx.signed_headers("PUT", &uri, b"");
    headers.insert(
        "x-amz-copy-source",
        source.parse().expect("copy source value"),
    );
    let resp = fx
        .send(Method::PUT, &uri, headers, b"unexpected-body".to_vec())
        .await;
    assert_eq!(resp.status().as_u16(), 400, "copy with body must 400");
    let xml = resp.text().await.expect("400 body");
    assert!(xml.contains("<Code>InvalidArgument</Code>"), "{xml}");

    let versioned = format!("/{bucket}/src.bin?versionId=abc");
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "dest-versioned.bin",
            b"",
            &[("x-amz-copy-source", versioned.as_str())],
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        501,
        "versioned copy source is explicitly out"
    );
    let xml = resp.text().await.expect("501 body");
    assert!(xml.contains("<Code>NotImplemented</Code>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn object_attributes_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("attrs").await;
    let data = pseudo_random(4_242);
    let put = fx.put_object(&bucket, "attr.bin", &data).await;
    assert_eq!(put.status().as_u16(), 200);
    let etag = header(&put, "etag").trim_matches('"').to_string();

    let uri = format!("/{bucket}/attr.bin?attributes");
    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert(
        "x-amz-object-attributes",
        "ETag,ObjectSize,StorageClass".parse().expect("attrs value"),
    );
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 200, "attributes must 200");
    let response_etag = header(&resp, "etag");
    let response_last_modified = header(&resp, "last-modified");
    let body = resp.text().await.expect("attributes body");
    assert!(body.contains("<GetObjectAttributesOutput"), "{body}");
    assert!(body.contains(&format!("<ETag>{etag}</ETag>")), "{body}");
    assert!(body.contains("<ObjectSize>4242</ObjectSize>"), "{body}");
    assert!(
        body.contains("<StorageClass>STANDARD</StorageClass>"),
        "{body}"
    );
    assert_eq!(
        response_etag,
        format!("\"{etag}\""),
        "response ETag header must carry quotes"
    );
    assert!(
        response_last_modified.contains("GMT"),
        "Last-Modified header must be an HTTP date: {response_last_modified}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn object_attributes_boundaries() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("attrb").await;
    fx.put_object(&bucket, "present.bin", b"x").await;

    let uri = format!("/{bucket}/present.bin?attributes");
    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert(
        "x-amz-object-attributes",
        "Checksum,ObjectSize".parse().expect("attrs"),
    );
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 501, "Checksum is explicitly out");
    let xml = resp.text().await.expect("501 body");
    assert!(xml.contains("<Code>NotImplemented</Code>"), "{xml}");

    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert("x-amz-object-attributes", "Bogus".parse().expect("attrs"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 400, "unknown attribute must 400");

    let headers = fx.signed_headers("GET", &uri, b"");
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "missing attributes header must 400"
    );

    let missing = format!("/{bucket}/absent.bin?attributes");
    let mut headers = fx.signed_headers("GET", &missing, b"");
    headers.insert(
        "x-amz-object-attributes",
        "ObjectSize".parse().expect("attrs"),
    );
    let resp = fx.send(Method::GET, &missing, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 404, "missing object must 404");
}

//! Multipart upload flow (task 13.2): initiate / upload parts / complete / abort / list.
//!
//! All tests are `#[ignore]`d — they need a live PostgreSQL via `TEST_S3_POSTGRES_URL`.

mod common;

use common::{header, xml_tag, S3Fixture};

/// Build the `<CompleteMultipartUpload>` XML body from `(part_number, etag)` pairs. The ETag is
/// inserted verbatim (the client echoes back the quoted value from UploadPart).
fn complete_xml(parts: &[(i32, String)]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in parts {
        xml.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// Initiate an upload and return its upload id.
async fn initiate(fx: &S3Fixture, bucket: &str, key: &str) -> String {
    let resp = fx.create_multipart(bucket, key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "CreateMultipartUpload must 200"
    );
    let xml = resp.text().await.expect("initiate body");
    xml_tag(&xml, "UploadId")
}

/// Three parts complete into one object whose GET body is the concatenation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "assembled.bin";

    let upload_id = initiate(&fx, &bucket, key).await;

    let part_bodies: [&[u8]; 3] = [b"hello ", b"multipart ", b"world"];
    let mut etags = Vec::new();
    for (idx, chunk) in part_bodies.iter().enumerate() {
        let number = i32::try_from(idx + 1).expect("small index");
        let resp = fx
            .upload_part(&bucket, key, &upload_id, number, chunk)
            .await;
        assert_eq!(resp.status().as_u16(), 200, "UploadPart must 200");
        etags.push((number, header(&resp, "etag")));
    }

    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "CompleteMultipartUpload must 200"
    );

    let expected: Vec<u8> = part_bodies.concat();
    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200, "GET must 200");
    let got = resp.bytes().await.expect("read assembled body").to_vec();
    assert_eq!(
        got, expected,
        "assembled body must be the part concatenation"
    );
}

/// Abort removes the upload; ListParts then reports it gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn abort_multipart() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "aborted.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let resp = fx
        .upload_part(&bucket, key, &upload_id, 1, b"part-one")
        .await;
    assert_eq!(resp.status().as_u16(), 200, "UploadPart must 200");

    let resp = fx.abort_multipart(&bucket, key, &upload_id).await;
    assert_eq!(resp.status().as_u16(), 204, "AbortMultipartUpload must 204");

    let resp = fx.list_parts(&bucket, key, &upload_id).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "ListParts after Abort must 404"
    );
}

/// Completing with a mismatched ETag is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn etag_mismatch_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "bad-etag.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let resp = fx
        .upload_part(&bucket, key, &upload_id, 1, b"part-one")
        .await;
    assert_eq!(resp.status().as_u16(), 200, "UploadPart must 200");

    let bogus = complete_xml(&[(1, "\"00000000000000000000000000000000\"".to_string())]);
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, bogus.as_bytes())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "Complete with a wrong ETag must 400"
    );
}

/// Two open uploads on a bucket are both listed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn list_multipart_uploads() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;

    let first = initiate(&fx, &bucket, "first.bin").await;
    let second = initiate(&fx, &bucket, "second.bin").await;
    assert_ne!(first, second, "each upload gets a distinct id");

    let resp = fx.list_multipart_uploads(&bucket).await;
    assert_eq!(resp.status().as_u16(), 200, "ListMultipartUploads must 200");
    let xml = resp.text().await.expect("list uploads body");
    assert!(
        xml.contains("<Key>first.bin</Key>"),
        "first upload listed: {xml}"
    );
    assert!(
        xml.contains("<Key>second.bin</Key>"),
        "second upload listed: {xml}"
    );
    assert!(
        xml.contains(&format!("<UploadId>{first}</UploadId>")),
        "first id listed"
    );
    assert!(
        xml.contains(&format!("<UploadId>{second}</UploadId>")),
        "second id listed"
    );
}

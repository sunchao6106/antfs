//! `aws-chunked` streaming framed payloads (`s3-chunked-payload`): the three
//! STREAMING literals decode server-side, chained chunk signatures verify, and
//! the negative wire mutations (tamper / length lie / oversize declaration)
//! are rejected loudly with nothing stored.
//!
//! All tests are `#[ignore]`d — they need a live database via
//! `TEST_S3_POSTGRES_URL` / `TEST_S3_MYSQL_URL` (same as the other e2e runs).

mod common;

use common::{
    aws_chunked_unsigned_frame_without_trailer, aws_chunked_unsigned_trailer_body,
    aws_chunked_unsigned_trailer_body_with_value, header, md5_hex, pseudo_random, xml_tag,
    S3Fixture, STREAMING_UNSIGNED_TRAILER,
};
use reqwest::Method;

/// Multi-chunk signed PUT round trip: decoded bytes stored, MD5 ETag agreed,
/// and a plain (non-chunked) PUT on the same bucket still works afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_put_roundtrip_multi_chunk() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunked").await;
    let key = "framed/object.bin";
    let data = pseudo_random(300_003);

    let resp = fx.put_object_chunked(&bucket, key, &data, 100_000).await;
    assert_eq!(resp.status().as_u16(), 200, "chunked PUT must 200");
    let etag = header(&resp, "etag");
    assert_eq!(etag, format!("\"{}\"", md5_hex(&data)));

    let get = fx.get_object(&bucket, key).await;
    assert_eq!(get.status().as_u16(), 200, "GET after chunked PUT");
    assert_eq!(header(&get, "content-length"), data.len().to_string());
    let body = get.bytes().await.expect("get body");
    assert_eq!(body.as_ref(), data.as_slice());

    // Plain PUT coexistence: the non-chunked path is untouched by the wrap.
    let plain = fx
        .put_object(&bucket, "plain/object.bin", b"plain-path-still-works")
        .await;
    assert_eq!(plain.status().as_u16(), 200, "plain PUT must stay 200");
}

/// Empty payload edge: a stream of exactly one final chunk, declared 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_put_empty_payload_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkempty").await;
    let key = "empty.bin";

    let resp = fx.put_object_chunked(&bucket, key, b"", 64).await;
    assert_eq!(resp.status().as_u16(), 200, "empty chunked PUT must 200");
    assert_eq!(header(&resp, "etag"), format!("\"{}\"", md5_hex(b"")));

    let get = fx.get_object(&bucket, key).await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "content-length"), "0");
}

/// Chunked UploadPart: multipart assembly from framed part bodies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_upload_part_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkpart").await;
    let key = "assembled.bin";

    let initiated = fx.create_multipart(&bucket, key).await;
    assert_eq!(initiated.status().as_u16(), 200);
    let upload_id = xml_tag(&initiated.text().await.expect("initiate body"), "UploadId");

    let part_a = pseudo_random(150_001);
    let part_b = pseudo_random(73_777);
    let mut etags = Vec::new();
    for (number, data) in [(1, &part_a), (2, &part_b)] {
        let resp = fx
            .upload_part_chunked(&bucket, key, &upload_id, number, data, 50_000)
            .await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "framed UploadPart {number} must 200"
        );
        etags.push((number, header(&resp, "etag")));
    }

    let mut xml = String::from("<CompleteMultipartUpload>");
    for (number, etag) in &etags {
        xml.push_str(&format!(
            "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>");

    let completed = fx
        .complete_multipart(&bucket, key, &upload_id, xml.as_bytes())
        .await;
    assert_eq!(completed.status().as_u16(), 200, "Complete must 200");

    let get = fx.get_object(&bucket, key).await;
    assert_eq!(get.status().as_u16(), 200);
    let body = get.bytes().await.expect("get body");
    let mut expected = part_a.clone();
    expected.extend_from_slice(&part_b);
    assert_eq!(body.as_ref(), expected.as_slice());
}

/// Unsigned trailer flavor with a **real** crc32c value: framing decodes, the
/// trailer checksum is collected and verified against the decoded body, and
/// the persisted checksum echoes on mode-enabled reads (s3-checksums).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn unsigned_trailer_put_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunktrailer").await;
    let key = "opaque.bin";
    let data = pseudo_random(20_007);
    let value = antfs_s3::checksum::ChecksumAlgo::Crc32c.compute(&data);

    let framed = aws_chunked_unsigned_trailer_body(&data, 8_192);
    let resp = fx
        .put_object_chunked_raw(
            &bucket,
            key,
            STREAMING_UNSIGNED_TRAILER,
            data.len() as u64,
            framed,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "unsigned trailer PUT must 200");
    assert_eq!(header(&resp, "etag"), format!("\"{}\"", md5_hex(&data)));
    assert_eq!(
        header(&resp, "x-amz-checksum-crc32c"),
        value,
        "写路径回显验过的 trailer 值"
    );
    assert_eq!(header(&resp, "x-amz-checksum-type"), "FULL_OBJECT");

    let get = fx.get_object(&bucket, key).await;
    assert_eq!(get.status().as_u16(), 200);
    let body = get.bytes().await.expect("get body");
    assert_eq!(body.as_ref(), data.as_slice());

    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(
        header(&get, "x-amz-checksum-crc32c"),
        value,
        "trailer 载体落行后同样回显"
    );
}

/// Trailer 值错:BadDigest 400,零写。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn trailer_wrong_value_is_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkbad").await;
    let key = "wrong.bin";
    let data = pseudo_random(5_120);

    let framed = aws_chunked_unsigned_trailer_body_with_value(&data, 2_048, "AAAAAA==");
    let resp = fx
        .put_object_chunked_raw(
            &bucket,
            key,
            STREAMING_UNSIGNED_TRAILER,
            data.len() as u64,
            framed,
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "wrong trailer value must 400");
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("BadDigest"), "{body}");
    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 404, "failed write leaves nothing");
}

/// `x-amz-trailer` 声明了校验头但帧尾未到:400。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn trailer_declared_but_missing_is_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkmiss").await;
    let key = "missing.bin";
    let data = pseudo_random(4_096);

    let framed = aws_chunked_unsigned_frame_without_trailer(&data, 2_048);
    let mut headers = fx.streaming_headers(
        "PUT",
        &format!("/{bucket}/{key}"),
        STREAMING_UNSIGNED_TRAILER,
        data.len() as u64,
    );
    headers.insert(
        "x-amz-trailer",
        "x-amz-checksum-crc32c".parse().expect("trailer header"),
    );
    let uri = format!("/{bucket}/{key}");
    let resp = fx.send(Method::PUT, &uri, headers, framed).await;
    assert_eq!(resp.status().as_u16(), 400, "declared-but-missing must 400");
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("InvalidArgument"), "{body}");
}

/// 同算法「显式头 + trailer」双载体:400。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn trailer_and_header_dual_carrier_is_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkdual").await;
    let key = "dual.bin";
    let data = pseudo_random(4_096);
    let value = antfs_s3::checksum::ChecksumAlgo::Crc32c.compute(&data);

    let framed = aws_chunked_unsigned_trailer_body(&data, 2_048);
    let mut headers = fx.streaming_headers(
        "PUT",
        &format!("/{bucket}/{key}"),
        STREAMING_UNSIGNED_TRAILER,
        data.len() as u64,
    );
    headers.insert(
        "x-amz-checksum-crc32c",
        value.parse().expect("checksum header value"),
    );
    let uri = format!("/{bucket}/{key}");
    let resp = fx.send(Method::PUT, &uri, headers, framed).await;
    assert_eq!(resp.status().as_u16(), 400, "dual carrier must 400");
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("InvalidArgument"), "{body}");
    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 404);
}

/// One flipped chunk-signature byte: 403, and the object never lands (404).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_signature_tamper_is_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunktamper").await;
    let key = "tampered.bin";
    let data = pseudo_random(1_024);

    let uri = format!("/{bucket}/{key}");
    let (mut headers, mut framed) = fx.chunked_sign_and_frame("PUT", &uri, &data, 300);
    let _ = &mut headers;
    let sig_at = framed
        .windows(16)
        .rposition(|w| w.starts_with(b"chunk-signature="))
        .expect("framed body carries chunk signatures");
    let sig_byte = sig_at + 16 + 3;
    framed[sig_byte] = if framed[sig_byte] == b'0' { b'1' } else { b'0' };

    let resp = fx.send(Method::PUT, &uri, headers, framed).await;
    assert_eq!(resp.status().as_u16(), 403, "tampered chunk chain must 403");
    let err_xml = resp.text().await.expect("error body");
    assert!(
        err_xml.contains("<Code>SignatureDoesNotMatch</Code>"),
        "{err_xml}"
    );

    let get = fx.get_object(&bucket, key).await;
    assert_eq!(
        get.status().as_u16(),
        404,
        "tampered object must not be stored"
    );
}

/// Declared decoded length mismatch, both directions: declared short of the
/// framing budget is a framing 400 (`InvalidArgument`); declared over the
/// decoded stream is `IncompleteBody`. Neither stores the object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_decoded_length_mismatch_is_rejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunklie").await;

    // Direction A: declaring less than the frames deliver → framing 400.
    let key_short = "lied-short.bin";
    let data = pseudo_random(2_048);
    let uri = format!("/{bucket}/{key_short}");
    let (headers, framed) = fx.chunked_sign_and_frame("PUT", &uri, &data, 512);
    let mut headers_short = headers;
    headers_short.insert(
        "x-amz-decoded-content-length",
        (data.len() as u64 - 1)
            .to_string()
            .parse()
            .expect("length value"),
    );
    let resp = fx.send(Method::PUT, &uri, headers_short, framed).await;
    assert_eq!(resp.status().as_u16(), 400, "short declared must 400");
    let err_xml = resp.text().await.expect("error body");
    assert!(
        err_xml.contains("<Code>InvalidArgument</Code>") && err_xml.contains("aws-chunked"),
        "{err_xml}"
    );
    assert_eq!(
        fx.get_object(&bucket, key_short).await.status().as_u16(),
        404,
        "lied-short object must not be stored"
    );

    // Direction B: declaring more than the frames deliver → IncompleteBody.
    let key_long = "lied-long.bin";
    let uri = format!("/{bucket}/{key_long}");
    let (headers, framed) = fx.chunked_sign_and_frame("PUT", &uri, &data, 512);
    let mut headers_long = headers;
    headers_long.insert(
        "x-amz-decoded-content-length",
        (data.len() as u64 + 1)
            .to_string()
            .parse()
            .expect("length value"),
    );
    let resp = fx.send(Method::PUT, &uri, headers_long, framed).await;
    assert_eq!(resp.status().as_u16(), 400, "long declared must 400");
    let err_xml = resp.text().await.expect("error body");
    assert!(err_xml.contains("<Code>IncompleteBody</Code>"), "{err_xml}");
    assert_eq!(
        fx.get_object(&bucket, key_long).await.status().as_u16(),
        404,
        "lied-long object must not be stored"
    );
}

/// Declared decoded length over `s3.max_body_bytes`: pre-rejected with 413
/// before a single body byte is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn chunked_oversize_declaration_is_prejected() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("chunkoversize").await;
    let key = "oversize-declaration.bin";

    let resp = fx
        .put_object_chunked_raw(
            &bucket,
            key,
            common::STREAMING_SIGNED,
            fx.state.config.max_body_bytes + 1,
            b"1\r\nZ\r\n0\r\n".to_vec(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 413, "oversize declaration must 413");
    let err_xml = resp.text().await.expect("error body");
    assert!(err_xml.contains("<Code>EntityTooLarge</Code>"), "{err_xml}");
}

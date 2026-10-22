//! PutObject 大对象流式通路(裁决 #13,2026-10-22):申报尺寸 > `s3.max_body_bytes`
//! (夹具 8 MiB)的直传整流为内部 multipart —— 内容正确性、校验声明、失败清场。
//!
//! All tests are `#[ignore]`d — they need a live MetaStore database(惯例同 object.rs)。

mod common;

use common::{header, pseudo_random, S3Fixture};

/// 超过单体内存上限(8 MiB 夹具)的直传:9 MiB = 2 内部片,字节恒等回读,
/// ETag 为 multipart 式合成(`-N` 尾),HEAD 逻辑尺寸。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn streamed_put_roundtrips_byte_equal() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("streamed").await;
    let body = pseudo_random(9 * 1024 * 1024 + 7);

    let resp = fx.put_object(&bucket, "big.bin", &body).await;
    assert_eq!(resp.status().as_u16(), 200, "streamed PUT must return 200");
    let etag = header(&resp, "etag");
    // 3 = ceil(9 MiB+7 / 4 MiB):S3GatewayConfig 默认单笔上限(夹具未覆写)。
    assert!(
        etag.ends_with("-3\""),
        "compound etag with part count: {etag}"
    );

    let resp = fx.get_object(&bucket, "big.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "GET must return 200");
    let got = resp.bytes().await.expect("read GET body").to_vec();
    assert_eq!(got, body, "streamed object must round-trip byte-for-byte");

    let resp = fx.head_object(&bucket, "big.bin").await;
    assert_eq!(resp.status().as_u16(), 200, "HEAD must return 200");
    assert_eq!(
        header(&resp, "content-length"),
        (9 * 1024 * 1024 + 7).to_string()
    );
}

/// 整对象校验声明按 complete 级 FULL_OBJECT 复验:正确值 200 并随响应回显。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn streamed_put_full_object_checksum_round_trips() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("streamedck").await;
    let body = pseudo_random(2 * 8 * 1024 * 1024 + 123);
    let value = antfs_s3::checksum::ChecksumAlgo::Crc32c.compute(&body);

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "ck.bin",
            &body,
            &[("x-amz-checksum-crc32c", &value)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "verified claim must 200");
    assert_eq!(header(&resp, "x-amz-checksum-crc32c"), value);
    assert_eq!(header(&resp, "x-amz-checksum-type"), "FULL_OBJECT");
}

/// 坏声明:400 BadDigest,片被 abort 清场,对象不存在(404)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn streamed_put_checksum_mismatch_reclaims_parts() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("streamedbad").await;
    let body = pseudo_random(3 * 8 * 1024 * 1024);
    // compute 是 base64 口径;坏值用"同格式不同数据"构造(保证格式合法、值必不等)。
    let mut corrupted = body.clone();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 0xff;
    let bad = antfs_s3::checksum::ChecksumAlgo::Crc32c.compute(&corrupted);

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "bad.bin",
            &body,
            &[("x-amz-checksum-crc32c", &bad)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "mismatched claim must 400");
    let text = resp.text().await.expect("400 body");
    assert!(text.contains("BadDigest"), "{text}");

    let resp = fx.get_object(&bucket, "bad.bin").await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "failed streamed put must leave no object"
    );
    // 会话与片行必须已清:遗留行 = 过期 GC 的孤儿账(此处验证强清场)。
    let leftovers = fx.parts_rows_in_bucket(&bucket).await;
    assert_eq!(leftovers, 0, "abort must remove session part rows");
}

/// SSE + 大对象:与显式 multipart 同族 501。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn streamed_put_sse_not_implemented() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("streamedsse").await;
    let body = pseudo_random(9 * 1024 * 1024);

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "sse.bin",
            &body,
            &[("x-amz-server-side-encryption", "AES256")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 501, "SSE on streamed path must 501");
    let text = resp.text().await.expect("501 body");
    assert!(text.contains("NotImplemented"), "{text}");
    let resp = fx.get_object(&bucket, "sse.bin").await;
    assert_eq!(resp.status().as_u16(), 404);
}

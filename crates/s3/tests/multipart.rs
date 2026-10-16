//! Multipart upload flow (task 13.2): initiate / upload parts / complete / abort / list.
//!
//! All tests are `#[ignore]`d — they need a live PostgreSQL via `TEST_S3_POSTGRES_URL`.

mod common;

use common::{header, pseudo_random, xml_tag, S3Fixture};

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

/// Metadata carried on CreateMultipartUpload flows through CompleteMultipartUpload into
/// the stored named object; GetObject echoes Content-Type and x-amz-meta-* back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_metadata_roundtrip() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "with-meta.bin";

    let resp = fx
        .create_multipart_with_headers(
            &bucket,
            key,
            &[
                ("content-type", "video/mp4"),
                ("x-amz-meta-project", "demo"),
            ],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "initiate must 200");
    let xml = resp.text().await.expect("initiate body");
    let upload_id = xml_tag(&xml, "UploadId");

    let chunk = pseudo_random(64);
    let resp = fx.upload_part(&bucket, key, &upload_id, 1, &chunk).await;
    assert_eq!(resp.status().as_u16(), 200, "part upload must 200");
    let etag = header(&resp, "etag");

    let resp = fx
        .complete_multipart(
            &bucket,
            key,
            &upload_id,
            complete_xml(&[(1, etag)]).as_bytes(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "complete must 200");

    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "content-type"), "video/mp4");
    assert_eq!(header(&resp, "x-amz-meta-project"), "demo");
}

/// 孤儿 multipart TTL 回收:create 后放一片(**已知** blob 直写 metastore)但不 complete →
/// TTL 外被 `run_once_at` 整体回收(session 行、parts 行、part blob 墓碑化),
/// TTL 内原封不动;指标落 `s3_gc_orphan_uploads_total`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn orphan_upload_expires_via_gc() {
    use antfs_s3::config::GcConfig;
    use antfs_s3::metastore::types::{ListUploadsOpts, PartMeta};

    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("gc").await;
    let key = "orphan.bin";

    // 真实 blob(router 写活),blob_id 已知 → 墓碑断言可判。
    let (blob, _) = fx
        .state
        .router
        .put_auto(bytes::Bytes::from_static(b"orphan-part"))
        .await
        .expect("router put part blob");
    let create = fx.create_multipart(&bucket, key).await;
    assert_eq!(create.status().as_u16(), 200, "initiate must 200");
    let xml = create.text().await.expect("initiate body");
    let upload_id = xml_tag(&xml, "UploadId");
    fx.state
        .metastore
        .put_part(
            &upload_id,
            PartMeta {
                part_number: 1,
                blob_id: blob,
                size: 12,
                etag: "\"x\"".to_string(),
            },
        )
        .await
        .expect("put part row");
    assert!(
        fx.state.router.get(&blob).await.is_ok(),
        "part blob alive before GC"
    );

    fn list_opts() -> ListUploadsOpts {
        ListUploadsOpts {
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        }
    }

    let cfg = GcConfig {
        enabled: false,
        interval_secs: 1,
        deletion_retention_secs: 3600,
        upload_ttl_secs: 3600,
    };
    let now = chrono::Utc::now().timestamp_millis();

    antfs_s3::gc::run_once_at(&fx.state, &cfg, now).await;
    let in_flight = fx
        .state
        .metastore
        .list_multipart_uploads(&bucket, list_opts())
        .await
        .expect("list");
    assert_eq!(
        in_flight.uploads.len(),
        1,
        "in-flight session must survive TTL"
    );

    let after = now + 3601 * 1000;
    antfs_s3::gc::run_once_at(&fx.state, &cfg, after).await;
    let reclaimed = fx
        .state
        .metastore
        .list_multipart_uploads(&bucket, list_opts())
        .await
        .expect("list");
    assert_eq!(reclaimed.uploads.len(), 0, "expired session reclaimed");
    // 墓碑收敛有界轮询(W=2 删除 + R=1 首跳可能读到滞后副本)。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if fx.state.router.get(&blob).await.is_err() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "part blob must converge to tombstoned"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let exported = antfs_metric::gather(&fx.state.registry).expect("gather");
    assert!(
        exported.contains("s3_gc_orphan_uploads_total"),
        "metric must land:\n{exported}"
    );
}

/// 并发取件:两个连接并发 `purge_expired_deletions` 同批候选,总和恰 4(行级取件互斥)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_purge_claims_rows_exactly_once() {
    use futures::future::join;

    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("gc").await;

    for k in ["a", "b", "c", "d"] {
        let resp = fx.put_object(&bucket, k, b"gone").await;
        assert_eq!(resp.status().as_u16(), 200);
    }
    for k in ["a", "b", "c", "d"] {
        let resp = fx.delete_object(&bucket, k).await;
        assert_eq!(resp.status().as_u16(), 204);
    }

    let ms = fx.state.metastore.clone();
    let ms2 = fx.state.metastore.clone();
    let (r1, r2) = join(
        ms.purge_expired_deletions(i64::MAX, 2),
        ms2.purge_expired_deletions(i64::MAX, 2),
    )
    .await;
    assert_eq!(
        r1.expect("purge 1") + r2.expect("purge 2"),
        4,
        "each row claimed exactly once"
    );
}

/// complete 计入总量(Σ part size)并受配额守卫;被拒时 session 保留(可调大后重试),
/// 原样重发即成功;used 记账闭环,GET 拼装数据一致。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_respects_quota_and_retries() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("quota-mp").await;
    let key = "quota-m.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let part_bodies: [&[u8]; 2] = [b"aaaa", b"bbbb"];
    let mut etags = Vec::new();
    for (idx, chunk) in part_bodies.iter().enumerate() {
        let number = i32::try_from(idx + 1).expect("small index");
        let resp = fx
            .upload_part(&bucket, key, &upload_id, number, chunk)
            .await;
        assert_eq!(resp.status().as_u16(), 200, "UploadPart must 200");
        etags.push((number, header(&resp, "etag")));
    }
    // parts 不计账(仅 complete 的 manifest 行计入)。
    assert_eq!(fx.bucket_quota(&bucket).await, (0, 0));

    fx.set_bucket_quota(&bucket, 4).await;
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 507, "8 > 4 配额必须被拒");
    let body = resp.text().await.expect("error body");
    assert!(
        body.contains("QuotaExceeded"),
        "错误体必须点名配额错误码: {body}"
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (4, 0));

    // 被拒后 session 保留(ListMultipartUploads 仍可重试)。
    let list = fx.list_multipart_uploads(&bucket).await;
    assert_eq!(list.status().as_u16(), 200, "list uploads");
    let list_body = list.text().await.expect("list body");
    assert!(
        list_body.contains(upload_id.as_str()),
        "被拒后 session 必须保留: {list_body}"
    );

    // 调大到 8 → 原样重发即成功;used = 8(manifest 逻辑总量)。
    fx.set_bucket_quota(&bucket, 8).await;
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "调大配额后重发必须成功");
    assert_eq!(fx.bucket_quota(&bucket).await, (8, 8));

    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().await.expect("body"), &b"aaaabbbb"[..]);
}

/// 并发重传同一 part(会话所有权恰一次的事务路径真机过):8 路并发 UploadPart
/// partNumber=1 全 200;终版 etag 事后从 ListParts 权威读回 → complete 200 →
/// GET 体为 8 个版本之一(终版 nondeterministic,但必须自洽可读)。
/// 「每个被顶替 blob 恰一人回收」的元数据不变量由双方言内联单测断言
/// (put_part_concurrent_retransmit_exactly_once),e2e 断用户可见面不被竞态破坏。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_retransmit_completes_and_reads() {
    use std::sync::Arc;

    let fx = Arc::new(S3Fixture::start().await);
    let bucket = fx.create_unique_bucket("mp").await;
    let key = "concurrent.bin";

    let upload_id = initiate(&fx, &bucket, key).await;

    let bodies: Vec<Vec<u8>> = (0u8..8).map(|i| vec![i; 64]).collect();
    let mut tasks = Vec::new();
    for body in bodies.clone() {
        let fx = fx.clone();
        let bucket = bucket.clone();
        let key = key.to_string();
        let upload_id = upload_id.clone();
        tasks.push(tokio::spawn(async move {
            fx.upload_part(&bucket, &key, &upload_id, 1, &body).await
        }));
    }
    for t in tasks {
        assert_eq!(
            t.await.expect("upload task").status().as_u16(),
            200,
            "每个并发重传都必须 200"
        );
    }

    let resp = fx.list_parts(&bucket, key, &upload_id).await;
    assert_eq!(resp.status().as_u16(), 200, "ListParts must 200");
    let xml = resp.text().await.expect("list parts body");
    let etag = xml_tag(&xml, "ETag").replace("&quot;", "\"");
    assert!(!etag.is_empty(), "part etag must be present: {xml}");

    let resp = fx
        .complete_multipart(
            &bucket,
            key,
            &upload_id,
            complete_xml(&[(1, etag)]).as_bytes(),
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "complete with authoritative etag"
    );

    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    let data = resp.bytes().await.expect("final body").to_vec();
    assert_eq!(data.len(), 64);
    assert!(
        bodies.contains(&data),
        "final body must be one of the 8 retransmitted versions"
    );
}

// ── Streaming assembly + ranged reads (s3-range-streaming) ──────────────────

/// 60 × 64 KiB streaming read keeps Content-Length exact and bytes byte-identical
/// (exercises the sequential part stream across many chunk boundaries).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_streaming_read_60_parts() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp-stream").await;
    let key = "many-parts.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    const PART: usize = 64 * 1024;
    const COUNT: usize = 60;

    let mut expected = Vec::with_capacity(PART * COUNT);
    let mut etags = Vec::new();
    for idx in 0..COUNT {
        let body = pseudo_random(PART);
        let number = (idx + 1) as i32;
        let resp = fx
            .upload_part(&bucket, key, &upload_id, number, &body)
            .await;
        assert_eq!(resp.status().as_u16(), 200, "UploadPart {number} must 200");
        etags.push((number, header(&resp, "etag")));
        expected.extend_from_slice(&body);
    }
    let total = expected.len();

    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "complete must 200");

    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "content-length"), total.to_string());
    assert_eq!(header(&resp, "accept-ranges"), "bytes");
    let got = resp.bytes().await.expect("assembled body").to_vec();
    assert_eq!(got, expected, "60-part assembly must be byte-exact");
}

/// Ranged reads on a 3 × 1 MiB multipart object: whole-part window, boundary
/// cross-slicing, and suffix windows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_ranged_windows() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp-range").await;
    let key = "windows.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    const PART: usize = 1024 * 1024;

    let mut expected = Vec::with_capacity(PART * 3);
    let mut etags = Vec::new();
    for idx in 0..3 {
        let body = pseudo_random(PART);
        let number = idx + 1;
        let resp = fx
            .upload_part(&bucket, key, &upload_id, number, &body)
            .await;
        assert_eq!(resp.status().as_u16(), 200, "UploadPart {number} must 200");
        etags.push((number, header(&resp, "etag")));
        expected.extend_from_slice(&body);
    }
    let total = expected.len() as u64;

    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "complete must 200");

    // 窗口恰为中间整片。
    let resp = fx
        .get_object_range(&bucket, key, "bytes=1048576-2097151")
        .await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        header(&resp, "content-range"),
        format!("bytes 1048576-2097151/{total}")
    );
    assert_eq!(
        resp.bytes().await.expect("part-2 window").to_vec(),
        expected[PART..2 * PART].to_vec()
    );

    // 跨片边界:片 1 尾 100 B + 片 2 首 100 B。
    let resp = fx
        .get_object_range(&bucket, key, "bytes=1048476-1048675")
        .await;
    assert_eq!(resp.status().as_u16(), 206);
    let got = resp.bytes().await.expect("boundary window").to_vec();
    let want = {
        let mut v = expected[PART - 100..PART].to_vec();
        v.extend_from_slice(&expected[PART..PART + 100]);
        v
    };
    assert_eq!(
        got, want,
        "window crossing the part boundary must splice correctly"
    );

    // 后缀窗:对象尾 5 B。
    let resp = fx.get_object_range(&bucket, key, "bytes=-5").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        resp.bytes().await.expect("suffix window").to_vec(),
        expected[expected.len() - 5..].to_vec()
    );

    // 首 byte 越界 → 416。
    let resp = fx
        .get_object_range(&bucket, key, &format!("bytes={total}-"))
        .await;
    assert_eq!(
        resp.status().as_u16(),
        416,
        "multipart EOF boundary must 416"
    );
}

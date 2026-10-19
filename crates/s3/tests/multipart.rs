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
                checksum: None,
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

// ── ?partNumber(multipart 单片直读,s3-read-compat)──────────────────────────

/// multipart `?partNumber`:片窗 206(全对象坐标 Content-Range)/ 越界 416 /
/// ×Range 交集(片内窗、空交 416)/ ×If-Range 失败提权整对象 200。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn part_number_multipart_reads_single_part() {
    use reqwest::Method;
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp-part").await;
    let key = "three.bin";

    let p1 = pseudo_random(10);
    let p2 = pseudo_random(20);
    let p3 = pseudo_random(30);
    let blobs = [&p1, &p2, &p3];

    let upload_id = initiate(&fx, &bucket, key).await;
    let mut etags = Vec::new();
    for (idx, body) in blobs.iter().enumerate() {
        let number = (idx + 1) as i32;
        let resp = fx.upload_part(&bucket, key, &upload_id, number, body).await;
        assert_eq!(resp.status().as_u16(), 200, "UploadPart {number} must 200");
        etags.push((number, header(&resp, "etag")));
    }
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "complete must 200");

    let whole = [p1.as_slice(), p2.as_slice(), p3.as_slice()].concat();

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=2"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 206, "part 2 → 206");
    assert_eq!(header(&resp, "content-range"), "bytes 10-29/60");
    assert_eq!(header(&resp, "content-length"), "20");
    assert_eq!(resp.bytes().await.expect("part body").to_vec(), p2.to_vec());

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=1"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(header(&resp, "content-range"), "bytes 0-9/60");
    assert_eq!(resp.bytes().await.expect("part body").to_vec(), p1.to_vec());

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=3"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(header(&resp, "content-range"), "bytes 30-59/60");
    assert_eq!(resp.bytes().await.expect("part body").to_vec(), p3.to_vec());

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=0"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 416, "partNumber=0 out of range");
    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=4"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 416, "beyond part count");
    let body = resp.text().await.expect("416 body");
    assert!(
        body.contains("InvalidRange") && body.contains("3 part"),
        "{body}"
    );

    let uri = format!("/{bucket}/{key}?partNumber=2");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("range", "bytes=15-19".parse().expect("range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        206,
        "part 2 ∩ Range = in-part window"
    );
    assert_eq!(header(&resp, "content-range"), "bytes 15-19/60");
    assert_eq!(
        resp.bytes().await.expect("body").to_vec(),
        whole[15..20].to_vec()
    );

    let uri = format!("/{bucket}/{key}?partNumber=2");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("range", "bytes=0-9".parse().expect("range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        416,
        "Range entirely before part 2 → empty intersection"
    );

    let etag = {
        let resp = fx.get_object(&bucket, key).await;
        header(&resp, "etag")
    };
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert(
        "if-range",
        etag.parse::<reqwest::header::HeaderValue>().expect("etag"),
    );
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        206,
        "fresh validator keeps the part window"
    );
    assert_eq!(header(&resp, "content-range"), "bytes 10-29/60");

    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("if-range", "\"stale\"".parse().expect("if-range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "stale validator escalates to whole object"
    );
    assert!(!resp.headers().contains_key("content-range"));
    assert_eq!(
        resp.bytes().await.expect("body").to_vec(),
        whole,
        "entire assembled object"
    );

    let resp = fx
        .request(
            Method::HEAD,
            &format!("/{bucket}/{key}?partNumber=2"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "HEAD ignores partNumber");
    assert_eq!(header(&resp, "content-length"), "60");
}

// ── part 级 checksum(s3-checksums)───────────────────────────────────────────

/// UploadPart 校验回环:正确值双头回显;错值 400 不落片;complete 装配行无
/// 校验(回显面缺省,记档的明示不做面)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn part_checksum_verify_and_assembled_default() {
    use antfs_s3::checksum::ChecksumAlgo;
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mp-cksum").await;
    let key = "parted.bin";
    let upload_id = initiate(&fx, &bucket, key).await;

    let p1 = pseudo_random(64 * 1024);
    let p2 = pseudo_random(32 * 1024);
    let good = ChecksumAlgo::Crc32.compute(&p1);

    let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber=1");
    let mut headers = fx.signed_headers("PUT", &uri, &p1);
    headers.insert(
        "x-amz-checksum-crc32",
        good.parse().expect("checksum value"),
    );
    let resp = fx
        .send(reqwest::Method::PUT, &uri, headers, p1.clone())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "part 1 with checksum must 200");
    assert_eq!(header(&resp, "x-amz-checksum-crc32"), good);
    assert_eq!(header(&resp, "x-amz-checksum-type"), "FULL_OBJECT");
    let etag1 = header(&resp, "etag").trim_matches('"').to_string();

    let wrong = ChecksumAlgo::Crc32.compute(b"something-else");
    let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber=2");
    let mut headers = fx.signed_headers("PUT", &uri, &p2);
    headers.insert(
        "x-amz-checksum-crc32",
        wrong.parse().expect("checksum value"),
    );
    let resp = fx
        .send(reqwest::Method::PUT, &uri, headers, p2.clone())
        .await;
    assert_eq!(resp.status().as_u16(), 400, "wrong part checksum must 400");
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("BadDigest"), "{body}");

    let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber=2");
    let mut headers = fx.signed_headers("PUT", &uri, &p2);
    headers.insert(
        "x-amz-checksum-sha256",
        ChecksumAlgo::Sha256
            .compute(&p2)
            .parse()
            .expect("checksum value"),
    );
    let resp = fx
        .send(reqwest::Method::PUT, &uri, headers, p2.clone())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "re-upload corrected part must 200"
    );
    let etag2 = header(&resp, "etag").trim_matches('"').to_string();

    let etags = vec![(1, etag1), (2, etag2)];
    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "complete must 200");

    let whole = [p1.as_slice(), p2.as_slice()].concat();
    let get = fx.get_object(&bucket, key).await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(get.bytes().await.expect("body").to_vec(), whole);

    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert!(
        !get.headers()
            .keys()
            .any(|k| k.as_str().starts_with("x-amz-checksum-")),
        "装配行无持久校验(complete 级记档)"
    );
}

// ── complete 级校验(s3-multipart-complete-checksum)─────────────────────────

/// composite 期望值:base64(逐片 raw 校验字节串接)——与网关推导式对拍。
fn composite_of(part_values: &[String]) -> String {
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    let mut raw = Vec::new();
    for v in part_values {
        raw.extend_from_slice(&B64.decode(v).expect("part checksum b64"));
    }
    B64.encode(raw)
}

/// 带(可选)校验头的片上传;断言 200 并回吐该片的 etag(去引号)。
async fn put_part_claiming(
    fx: &S3Fixture,
    bucket: &str,
    key: &str,
    upload_id: &str,
    number: i32,
    data: &[u8],
    claim: Option<(&str, String)>,
) -> String {
    let uri = format!("/{bucket}/{key}?uploadId={upload_id}&partNumber={number}");
    let mut headers = fx.signed_headers("PUT", &uri, data);
    if let Some((name, value)) = claim {
        headers.insert(
            name.parse::<reqwest::header::HeaderName>()
                .expect("checksum header name"),
            value.parse().expect("checksum value"),
        );
    }
    let resp = fx
        .send(reqwest::Method::PUT, &uri, headers, data.to_vec())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "part {number} must 200");
    header(&resp, "etag").trim_matches('"').to_string()
}

/// 带(可选)extra 头的 complete 调用。
async fn complete_with_headers(
    fx: &S3Fixture,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etag_pairs: &[(i32, String)],
    extra: &[(&str, &str)],
) -> reqwest::Response {
    let body = complete_xml(etag_pairs);
    let uri = format!("/{bucket}/{key}?uploadId={upload_id}");
    let mut headers = fx.signed_headers("POST", &uri, body.as_bytes());
    for (name, value) in extra {
        headers.insert(
            name.parse::<reqwest::header::HeaderName>()
                .expect("complete header name"),
            value.parse().expect("complete header value"),
        );
    }
    fx.send(reqwest::Method::POST, &uri, headers, body.into_bytes())
        .await
}

/// 三片同族 crc32:无头 complete 仍自动推导 composite 并回显;GET(mode)同值同
/// COMPOSITE 型;裸片对照会话无校验头(零变化)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_composite_derived_and_echoed() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mpckcomp").await;
    let key = "composed.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let parts: [&[u8]; 3] = [b"alpha", b"beta-grams", b"gamma!"];
    let mut etags = Vec::new();
    let mut values = Vec::new();
    for (idx, data) in parts.iter().enumerate() {
        let number = i32::try_from(idx + 1).expect("small index");
        let v = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(data);
        values.push(v.clone());
        let etag = put_part_claiming(
            &fx,
            &bucket,
            key,
            &upload_id,
            number,
            data,
            Some(("x-amz-checksum-crc32", v)),
        )
        .await;
        etags.push((number, etag));
    }

    let resp = fx
        .complete_multipart(&bucket, key, &upload_id, complete_xml(&etags).as_bytes())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "plain complete with valued parts must 200"
    );
    let expected = composite_of(&values);
    assert_eq!(
        header(&resp, "x-amz-checksum-crc32"),
        expected,
        "composite must auto-derive and echo"
    );
    assert_eq!(header(&resp, "x-amz-checksum-type"), "COMPOSITE");

    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "x-amz-checksum-crc32"), expected);
    assert_eq!(header(&get, "x-amz-checksum-type"), "COMPOSITE");
    let body = get.bytes().await.expect("body");
    let mut want = Vec::new();
    for p in &parts {
        want.extend_from_slice(p);
    }
    assert_eq!(body.as_ref(), want.as_slice());

    let get_silent = fx.get_object(&bucket, key).await;
    assert!(
        get_silent.headers().get("x-amz-checksum-crc32").is_none(),
        "no mode → no echo"
    );

    // 裸片对照:无任何校验参与的旧会话语义零变化。
    let key2 = "bare.bin";
    let upload2 = initiate(&fx, &bucket, key2).await;
    let mut etags2 = Vec::new();
    for (idx, data) in [b"bare-a", b"bare-b"].iter().enumerate() {
        let number = i32::try_from(idx + 1).expect("small index");
        let resp = fx.upload_part(&bucket, key2, &upload2, number, *data).await;
        assert_eq!(resp.status().as_u16(), 200);
        etags2.push((number, header(&resp, "etag")));
    }
    let resp = fx
        .complete_multipart(&bucket, key2, &upload2, complete_xml(&etags2).as_bytes())
        .await;
    assert_eq!(resp.status().as_u16(), 200, "bare complete unchanged");
    assert!(
        resp.headers().get("x-amz-checksum-crc32").is_none(),
        "bare complete must not invent a checksum"
    );
}

/// SDK 供 composite 值:错值 400(BadDigest 零副作用)、对值 200——重试闭环。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_composite_claim_mismatch_then_good() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mpckclaim").await;
    let key = "claimed.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let p1 = pseudo_random(128);
    let p2 = pseudo_random(64);
    let v1 = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(&p1);
    let v2 = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(&p2);
    let etag1 = put_part_claiming(
        &fx,
        &bucket,
        key,
        &upload_id,
        1,
        &p1,
        Some(("x-amz-checksum-crc32", v1.clone())),
    )
    .await;
    let etag2 = put_part_claiming(
        &fx,
        &bucket,
        key,
        &upload_id,
        2,
        &p2,
        Some(("x-amz-checksum-crc32", v2.clone())),
    )
    .await;
    let etags = [(1, etag1), (2, etag2)];

    let wrong = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(b"bogus");
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &etags,
        &[
            ("x-amz-checksum-type", "COMPOSITE"),
            ("x-amz-checksum-crc32", &wrong),
        ],
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "wrong composite claim must 400"
    );
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("BadDigest"), "{body}");

    let expected = composite_of(&[v1.clone(), v2.clone()]);
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &etags,
        &[
            ("x-amz-checksum-type", "COMPOSITE"),
            ("x-amz-checksum-crc32", &expected),
        ],
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "retry on intact session must 200"
    );
    assert_eq!(header(&resp, "x-amz-checksum-crc32"), expected);
    assert_eq!(header(&resp, "x-amz-checksum-type"), "COMPOSITE");
}

/// FULL_OBJECT:流式全验 → 行存 + 回显(同发 sha1+sha256 存最强);坏值 400 后
/// 重试成功(零副作用闭环);GET(mode)回 FULL_OBJECT 型。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_full_object_verified() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mpckfull").await;
    let key = "fullobj.bin";

    let upload_id = initiate(&fx, &bucket, key).await;
    let p1 = pseudo_random(64 * 1024);
    let p2 = pseudo_random(999);
    let etag1 = {
        let resp = fx.upload_part(&bucket, key, &upload_id, 1, &p1).await;
        assert_eq!(resp.status().as_u16(), 200);
        header(&resp, "etag").trim_matches('"').to_string()
    };
    let etag2 = {
        let resp = fx.upload_part(&bucket, key, &upload_id, 2, &p2).await;
        assert_eq!(resp.status().as_u16(), 200);
        header(&resp, "etag").trim_matches('"').to_string()
    };
    let etags = [(1, etag1), (2, etag2)];

    let mut full = p1.clone();
    full.extend_from_slice(&p2);
    let sha256 = antfs_s3::checksum::ChecksumAlgo::Sha256.compute(&full);
    let sha1 = antfs_s3::checksum::ChecksumAlgo::Sha1.compute(&full);
    let bad = antfs_s3::checksum::ChecksumAlgo::Sha256.compute(b"not-the-object");

    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &etags,
        &[
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-checksum-sha256", &bad),
        ],
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "wrong full object value must 400"
    );
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("BadDigest"), "{body}");

    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &etags,
        &[
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-checksum-sha256", &sha256),
            ("x-amz-checksum-sha1", &sha1),
        ],
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "retry with good values must 200"
    );
    assert_eq!(header(&resp, "x-amz-checksum-sha256"), sha256);
    assert_eq!(header(&resp, "x-amz-checksum-type"), "FULL_OBJECT");
    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(
        header(&get, "x-amz-checksum-sha256"),
        sha256,
        "strongest of the multi-algo set persists"
    );
    assert!(get.headers().get("x-amz-checksum-sha1").is_none());
    assert_eq!(header(&get, "x-amz-checksum-type"), "FULL_OBJECT");
}

/// composite 拒绝面:混族/半值/全裸三种形态都响亮 400(带定位文案)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_composite_rejects_mixed_partial_and_bare() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mpckreject").await;

    // 混族:crc32 + crc32c
    let key = "mixed.bin";
    let upload_id = initiate(&fx, &bucket, key).await;
    let a = pseudo_random(8);
    let b = pseudo_random(8);
    let va = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(&a);
    let vb = antfs_s3::checksum::ChecksumAlgo::Crc32c.compute(&b);
    let e1 = put_part_claiming(
        &fx,
        &bucket,
        key,
        &upload_id,
        1,
        &a,
        Some(("x-amz-checksum-crc32", va)),
    )
    .await;
    let e2 = put_part_claiming(
        &fx,
        &bucket,
        key,
        &upload_id,
        2,
        &b,
        Some(("x-amz-checksum-crc32c", vb)),
    )
    .await;
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &[(1, e1), (2, e2)],
        &[("x-amz-checksum-type", "COMPOSITE")],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400, "mixed families must 400");
    let body = resp.text().await.expect("body");
    assert!(body.contains("mixed checksum algorithms"), "{body}");

    // 半值:一片带校验一片裸
    let key = "partial.bin";
    let upload_id = initiate(&fx, &bucket, key).await;
    let a = pseudo_random(8);
    let b = pseudo_random(8);
    let va = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(&a);
    let e1 = put_part_claiming(
        &fx,
        &bucket,
        key,
        &upload_id,
        1,
        &a,
        Some(("x-amz-checksum-crc32", va)),
    )
    .await;
    let resp2 = fx.upload_part(&bucket, key, &upload_id, 2, &b).await;
    assert_eq!(resp2.status().as_u16(), 200);
    let e2 = header(&resp2, "etag").trim_matches('"').to_string();
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &[(1, e1), (2, e2)],
        &[("x-amz-checksum-type", "COMPOSITE")],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400, "partial coverage must 400");
    let body = resp.text().await.expect("body");
    assert!(body.contains("without a checksum"), "{body}");

    // 全裸 + 显式 composite 声明
    let key = "bare.bin";
    let upload_id = initiate(&fx, &bucket, key).await;
    let a = pseudo_random(8);
    let resp2 = fx.upload_part(&bucket, key, &upload_id, 1, &a).await;
    let e1 = header(&resp2, "etag").trim_matches('"').to_string();
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &[(1, e1)],
        &[("x-amz-checksum-type", "COMPOSITE")],
    )
    .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "bare parts under composite claim must 400"
    );
}

/// 请求头形态边缘:type 野值/声明无值/SHA 配 composite/CRC64NVME 族面。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn multipart_complete_header_semantics_edges() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("mpckedge").await;
    let key = "edge.bin";
    let upload_id = initiate(&fx, &bucket, key).await;
    let fake_etags = [(1, "d41d8cd98f00b204e9800998ecf8427e".to_string())];

    let v = antfs_s3::checksum::ChecksumAlgo::Sha256.compute(b"x");
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &fake_etags,
        &[
            ("x-amz-checksum-type", "COMPOSITE"),
            ("x-amz-checksum-sha256", &v),
        ],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400, "sha under COMPOSITE must 400");

    let v = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(b"x");
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &fake_etags,
        &[
            ("x-amz-checksum-type", "BASE64"),
            ("x-amz-checksum-crc32", &v),
        ],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400, "bogus type value must 400");

    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &fake_etags,
        &[("x-amz-checksum-type", "FULL_OBJECT")],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 400, "type without value must 400");

    let nvme = "AAAAAAAAAAA=";
    let resp = complete_with_headers(
        &fx,
        &bucket,
        key,
        &upload_id,
        &fake_etags,
        &[
            ("x-amz-checksum-type", "FULL_OBJECT"),
            ("x-amz-checksum-crc64nvme", nvme),
        ],
    )
    .await;
    assert_eq!(resp.status().as_u16(), 501, "crc64nvme family stays 501");
}

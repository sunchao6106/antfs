//! Object-level S3 operations (task 13.1): PutObject / GetObject / HeadObject / DeleteObject.
//!
//! All tests are `#[ignore]`d — they need a live MetaStore database (dialect 由 `TEST_S3_DRIVER`
//! 选择,默认 postgres;见 `common/mod.rs` 头注)。

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

/// Overwrite 即刻遮蔽旧版本并回收旧 blob(handler 对 shadowed 逐个 router.delete;
/// 旧行 state=0)。坏点(blob 未删)会在此暴露:旧 blob 仍可读。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn overwrite_shadow_reclaims_old_blob() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("gc").await;

    let body1 = pseudo_random(64);
    let body2 = pseudo_random(128);
    assert_eq!(
        fx.put_object(&bucket, "over.bin", &body1)
            .await
            .status()
            .as_u16(),
        200
    );
    let v1 = fx
        .state
        .metastore
        .get_object(&bucket, "over.bin")
        .await
        .expect("metastore get")
        .expect("v1 present");
    assert_eq!(
        fx.put_object(&bucket, "over.bin", &body2)
            .await
            .status()
            .as_u16(),
        200
    );

    // 新版本可见、旧版本行已遮蔽(state=0 ⇒ get_object 只见 state=1)。
    let after = fx
        .state
        .metastore
        .get_object(&bucket, "over.bin")
        .await
        .expect("metastore get")
        .expect("v2 present");
    assert_ne!(after.blob_id, v1.blob_id);
    assert_eq!(after.size as usize, body2.len());

    // 旧 blob 在集群被删(墓碑 W=2 即回;R=1 可能先读到无墓碑副本 → 有界轮询等收敛)。
    poll_tombstoned(&fx, &v1.blob_id).await;

    // 共享测试库:GC 是全库语义,断言一律限本 fixture 的 bucket 域
    // (方言中性行级断言走夹具助手,PG/MySQL 同一套用例)。
    {
        let kept = fx
            .state
            .metastore
            .purge_expired_deletions(0, 100)
            .await
            .expect("purge within retention");
        // 远古 cutoff 不清任何行(语义边界);桶域行仍在。
        assert_eq!(kept, 0, "no row is older than the epoch cutoff");
        let n = fx.shadowed_rows_in_bucket(&bucket).await;
        assert_eq!(n, 1, "shadowed row inside retention must stay");
    }
    fx.state
        .metastore
        .purge_expired_deletions(i64::MAX, 100)
        .await
        .expect("purge past retention");
    let n = fx.shadowed_rows_in_bucket(&bucket).await;
    assert_eq!(n, 0, "shadowed row past retention must be purged");
}

/// GC 循环:`run_once_at` 时间注入驱动保留期(免 sleep)—— 保留期内行不动,
/// 未来时刻清行并落 `s3_gc_purged_rows_total` 到**网关真接入的 Registry**(CLAUDE.md §6)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn gc_cycle_purges_deleted_rows_after_retention() {
    use antfs_s3::config::GcConfig;

    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("gc").await;
    let body = pseudo_random(64);
    assert_eq!(
        fx.put_object(&bucket, "del.bin", &body)
            .await
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        fx.delete_object(&bucket, "del.bin").await.status().as_u16(),
        204
    );
    assert!(fx
        .state
        .metastore
        .get_object(&bucket, "del.bin")
        .await
        .expect("get")
        .is_none());

    // 共享测试库:GC 的 purge 语句是全库语义,行数断言必须限本 fixture 的 bucket 域
    // (方言中性行级断言走夹具助手,PG/MySQL 同一套用例)。

    let cfg = GcConfig {
        enabled: false,
        interval_secs: 1,
        deletion_retention_secs: 3600,
        upload_ttl_secs: 3600,
    };
    let now = chrono::Utc::now().timestamp_millis();

    antfs_s3::gc::run_once_at(&fx.state, &cfg, now).await;
    assert_eq!(
        fx.shadowed_rows_in_bucket(&bucket).await,
        1,
        "deleted row must survive its retention"
    );

    let after = now + 3601 * 1000;
    antfs_s3::gc::run_once_at(&fx.state, &cfg, after).await;
    assert_eq!(
        fx.shadowed_rows_in_bucket(&bucket).await,
        0,
        "row past retention must be purged"
    );
    let exported = antfs_metric::gather(&fx.state.registry).expect("gather");
    assert!(
        exported.contains("s3_gc_purged_rows_total"),
        "metric must land on the live registry:\n{exported}"
    );
}

/// 有界轮询等 blob 墓碑收敛(W=2 删除后,滞后副本靠拉复制补墓碑;R=1 首跳可能仍命中旧副本)。
/// 10s 上限、50ms 间隔 —— CLAUDE.md §6(确定性等待上限,禁裸 sleep)。
async fn poll_tombstoned(fx: &S3Fixture, blob: &antfs_blob::StoreKey) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if fx.state.router.get(blob).await.is_err() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "blob must converge to tombstoned within 10s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// 桶级配额全链路:守卫拒写(507 + QuotaExceeded + 死点墓碑)、净增量覆盖释放、
/// 调大恢复、删除释放、quota=0 解除;used 记账经 admin 闭环核对;拒绝计数落真 Registry。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn quota_gates_writes_and_accounts() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("quota").await;

    fx.set_bucket_quota(&bucket, 12).await;

    assert_eq!(
        fx.put_object(&bucket, "a.bin", &[1u8; 8])
            .await
            .status()
            .as_u16(),
        200
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (12, 8));

    let resp = fx.put_object(&bucket, "b.bin", &[2u8; 5]).await;
    assert_eq!(resp.status().as_u16(), 507, "13 > 12 必须被配额拒绝");
    let body = resp.text().await.expect("error body");
    assert!(
        body.contains("QuotaExceeded"),
        "错误体必须点名配额错误码: {body}"
    );
    assert!(
        fx.state
            .metastore
            .get_object(&bucket, "b.bin")
            .await
            .expect("get b")
            .is_none(),
        "被拒写不能落行(blob 已同请求墓碑)"
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (12, 8));

    // 覆盖大→小:净增量 4−8 = −4 次 → 放行,used 4。
    assert_eq!(
        fx.put_object(&bucket, "a.bin", &[3u8; 4])
            .await
            .status()
            .as_u16(),
        200
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (12, 4));

    // 调大恢复:同尺寸 5B 放行(used 9)。
    fx.set_bucket_quota(&bucket, 100).await;
    assert_eq!(
        fx.put_object(&bucket, "b.bin", &[4u8; 5])
            .await
            .status()
            .as_u16(),
        200
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (100, 9));

    // 删除释放(used 4)。
    assert_eq!(
        fx.delete_object(&bucket, "b.bin").await.status().as_u16(),
        204
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (100, 4));

    let dumped = antfs_metric::gather(&fx.state.registry).expect("gather");
    let want = format!("s3_quota_rejected_total{{bucket=\"{bucket}\"}}");
    let line = dumped
        .lines()
        .find(|l| l.starts_with(&want))
        .unwrap_or_else(|| panic!("配额拒绝指标必须落真 Registry:\n{dumped}"));
    assert!(line.ends_with(" 1"), "本桶拒绝计数应为 1: {line}");

    // quota=0 解除限制。
    fx.set_bucket_quota(&bucket, 0).await;
    assert_eq!(
        fx.put_object(&bucket, "c.bin", &[5u8; 200])
            .await
            .status()
            .as_u16(),
        200
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (0, 204));
}

/// 守卫原子性:并发同抢最后余量,恰一只成功(单语句条件 UPDATE 的行锁保证)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn quota_guard_exactly_one_passes_under_race() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("quota-race").await;

    fx.set_bucket_quota(&bucket, 10).await;
    assert_eq!(
        fx.put_object(&bucket, "setup.bin", &[1u8; 8])
            .await
            .status()
            .as_u16(),
        200
    );

    let f1 = fx.put_object(&bucket, "r1.bin", &[2u8; 2]);
    let f2 = fx.put_object(&bucket, "r2.bin", &[2u8; 2]);
    let (r1, r2) = tokio::join!(f1, f2);
    let (c1, c2) = (r1.status().as_u16(), r2.status().as_u16());
    assert!(
        (c1 == 200 && c2 == 507) || (c1 == 507 && c2 == 200),
        "恰一只成功、另一只被拒: {c1}/{c2}"
    );
    assert_eq!(fx.bucket_quota(&bucket).await, (10, 10));
}

/// 多租户私有桶(s3-bucket-acl):跨租户对象/列举/批量/multipart 全家族 403;
/// owner 一切照旧;ListBuckets 隔离;未知桶 404 NoSuchBucket。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn private_buckets_reject_cross_tenant_access() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("acl").await;
    assert_eq!(
        fx.put_object(&bucket, "a.bin", b"tenant-data")
            .await
            .status()
            .as_u16(),
        200
    );

    let (bob_ak, bob_sk) = fx.issue_credential("bob").await;

    let body = b"x".to_vec();
    let cases: Vec<(&str, String)> = vec![
        ("PUT", format!("/{bucket}/bob.bin")),
        ("GET", format!("/{bucket}/a.bin")),
        ("HEAD", format!("/{bucket}/a.bin")),
        ("DELETE", format!("/{bucket}/a.bin")),
        ("GET", format!("/{bucket}?list-type=2")),
        ("POST", format!("/{bucket}/m.bin?uploads=")),
    ];
    for (method, uri) in cases {
        let resp = fx.request_as(&bob_ak, &bob_sk, method, &uri, &body).await;
        assert_eq!(
            resp.status().as_u16(),
            403,
            "跨租户 {method} {uri} 必须 403"
        );
    }

    // 跨租户写未落任何行。
    assert!(fx
        .state
        .metastore
        .get_object(&bucket, "bob.bin")
        .await
        .expect("get bob.bin")
        .is_none());

    // owner(默认凭证)一切照旧。
    assert_eq!(fx.get_object(&bucket, "a.bin").await.status().as_u16(), 200);
    assert_eq!(
        fx.delete_object(&bucket, "a.bin").await.status().as_u16(),
        204
    );

    // ListBuckets 隔离:bob 看不到 default 的桶。
    let resp = fx.request_as(&bob_ak, &bob_sk, "GET", "/", &[]).await;
    assert_eq!(resp.status().as_u16(), 200);
    let text = resp.text().await.expect("list buckets body");
    assert!(
        !text.contains(&bucket),
        "bob 不得看见 default 的桶:\n{text}"
    );

    // 未知桶 → 404 NoSuchBucket(不存在优先于租户判定)。
    let resp = fx
        .request_as(&bob_ak, &bob_sk, "GET", "/no-such-xyz-42/k", &[])
        .await;
    assert_eq!(resp.status().as_u16(), 404);
}

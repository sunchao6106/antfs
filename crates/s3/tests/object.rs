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

    // 遮蔽行进保留期:cutoff=0(远古)清不动;未来时刻一口气清 1 行。
    let kept = fx
        .state
        .metastore
        .purge_expired_deletions(0, 100)
        .await
        .expect("purge within retention");
    assert_eq!(kept, 0, "row inside retention must not be purged");
    let purged = fx
        .state
        .metastore
        .purge_expired_deletions(i64::MAX, 100)
        .await
        .expect("purge past retention");
    assert_eq!(purged, 1, "shadowed row past retention must be purged");
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

    let pool = fx.pg_pool().await;
    let count = |pool: &sqlx::PgPool, cond: &str| {
        let pool = pool.clone();
        let cond = cond.to_string();
        async move {
            let r: (i64,) =
                sqlx::query_as(&format!("SELECT count(*) FROM named_objects WHERE {cond}"))
                    .fetch_one(&pool)
                    .await
                    .expect("count query");
            r.0
        }
    };

    let cfg = GcConfig {
        enabled: false,
        interval_secs: 1,
        deletion_retention_secs: 3600,
        upload_ttl_secs: 3600,
    };
    let now = chrono::Utc::now().timestamp_millis();

    antfs_s3::gc::run_once_at(&fx.state, &cfg, now).await;
    assert_eq!(
        count(&pool, "state = 0").await,
        1,
        "deleted row must survive its retention"
    );

    let after = now + 3601 * 1000;
    antfs_s3::gc::run_once_at(&fx.state, &cfg, after).await;
    assert_eq!(
        count(&pool, "state = 0").await,
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

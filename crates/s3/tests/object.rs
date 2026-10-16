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

/// canned ACL 桶级读写面(s3-canned-acl):public-read 开匿名/跨租户**读**
/// (GET/HEAD object、ListObjectsV2),写与 owner 面不动;切换即时生效;坏凭证
/// 绝不匿名化;XML body 形式显式 501。私有桶行为 = 上一测试的逐字不变量。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn canned_acl_public_read_grants_reads_but_not_writes() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("acl-pub").await;
    let body = pseudo_random(128);
    assert_eq!(
        fx.put_object(&bucket, "pub.bin", &body)
            .await
            .status()
            .as_u16(),
        200
    );
    let url_obj = fx.url(&format!("/{bucket}/pub.bin"));

    // 私有默认:匿名 GET/HEAD/ListV2 全 403(首轮语义,拒绝点从中间件挪到闸门)。
    assert_eq!(
        fx.http
            .get(&url_obj)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous GET on private bucket"
    );
    assert_eq!(
        fx.http
            .head(&url_obj)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous HEAD on private bucket"
    );
    assert_eq!(
        fx.http
            .get(fx.url(&format!("/{bucket}?list-type=2")))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous ListObjectsV2 on private bucket"
    );

    // owner 开公开(canned 头未列入签名集,SigV4 只验已列头 —— 同真 S3 口径)。
    let mut headers = fx.signed_headers("PUT", &format!("/{bucket}?acl"), b"");
    headers.insert("x-amz-acl", "public-read".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "PutBucketAcl public-read");

    // 匿名读三件全开,数据与列举为真。
    let resp = fx.http.get(&url_obj).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "anonymous GET on public bucket"
    );
    assert_eq!(resp.bytes().await.unwrap().to_vec(), body);
    assert_eq!(
        fx.http
            .head(&url_obj)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200,
        "anonymous HEAD on public bucket"
    );
    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}?list-type=2")))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "anonymous ListObjectsV2 on public bucket"
    );
    let listing = resp.text().await.unwrap();
    assert!(listing.contains("<Key>pub.bin</Key>"), "listing: {listing}");

    // 匿名写/删/会话目录依旧拒。
    assert_eq!(
        fx.http
            .put(&url_obj)
            .body("x")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous PUT stays owner-only"
    );
    assert_eq!(
        fx.http
            .delete(&url_obj)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous DELETE stays owner-only"
    );
    assert_eq!(
        fx.http
            .get(fx.url(&format!("/{bucket}?uploads")))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "anonymous ListMultipartUploads stays owner-only"
    );

    // 匿名未知桶 → 404(存在性判定先于 ACL)。
    assert_eq!(
        fx.http
            .get(fx.url("/no-such-bucket-aa/k"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        404,
        "unknown bucket is 404 regardless of ACL"
    );

    // 坏凭证 ≠ 匿名(public 桶上乱 Authorization 头仍 403,无降级通道)。
    let resp = fx
        .http
        .get(&url_obj)
        .header("authorization", "definitely-not-sigv4")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        403,
        "malformed credential is never treated as anonymous"
    );

    // 跨租户签名读:public → 200。
    let (bob_ak, bob_sk) = fx.issue_credential("bob").await;
    let resp = fx
        .request_as(&bob_ak, &bob_sk, "GET", &format!("/{bucket}/pub.bin"), &[])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "cross-tenant signed read granted on public-read"
    );

    // owner GetBucketAcl:XML 含 AllUsers READ grant 与 owner FULL_CONTROL。
    let headers = fx.signed_headers("GET", &format!("/{bucket}?acl"), b"");
    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "GetBucketAcl");
    let acl_xml = resp.text().await.unwrap();
    assert!(acl_xml.contains("AllUsers"), "public ACL XML: {acl_xml}");
    assert!(
        acl_xml.contains("<Permission>FULL_CONTROL</Permission>"),
        "owner grant present: {acl_xml}"
    );

    // 切回私有,即时生效;ACL XML 不再含 AllUsers。
    let mut headers = fx.signed_headers("PUT", &format!("/{bucket}?acl"), b"");
    headers.insert("x-amz-acl", "private".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "PutBucketAcl back to private");
    assert_eq!(
        fx.http
            .get(&url_obj)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403,
        "flip back to private is immediate"
    );
    let headers = fx.signed_headers("GET", &format!("/{bucket}?acl"), b"");
    let resp = fx
        .http
        .get(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    let acl_xml = resp.text().await.unwrap();
    assert!(!acl_xml.contains("AllUsers"), "private ACL XML: {acl_xml}");

    // CreateBucket 即带 x-amz-acl:public-read → 匿名列举立即可读。
    let bucket2 = fx.unique("acl-pub2");
    let mut headers = fx.signed_headers("PUT", &format!("/{bucket2}"), b"");
    headers.insert("x-amz-acl", "public-read".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket2}")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200, "CreateBucket with canned ACL");
    assert_eq!(
        fx.http
            .get(fx.url(&format!("/{bucket2}?list-type=2")))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200,
        "bucket created public is anonymously listable"
    );

    // 非法 canned 值 → 400 InvalidArgument。
    let bucket3 = fx.unique("acl-bad");
    let mut headers = fx.signed_headers("PUT", &format!("/{bucket3}"), b"");
    headers.insert("x-amz-acl", "public-read-write".parse().unwrap());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket3}")))
        .headers(headers)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        400,
        "unsupported x-amz-acl value is InvalidArgument"
    );

    // PUT ?acl 的 XML body 形式 → 501 NotImplemented(显式出界,不静默不误导)。
    let xml_body = "<AccessControlPolicy></AccessControlPolicy>";
    let headers = fx.signed_headers("PUT", &format!("/{bucket}?acl"), xml_body.as_bytes());
    let resp = fx
        .http
        .put(fx.url(&format!("/{bucket}?acl")))
        .headers(headers)
        .body(xml_body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        501,
        "XML-body PutBucketAcl is explicitly out of scope"
    );
}

// ── Range requests (s3-range-streaming) ─────────────────────────────────────

const SSE_HDR: (&str, &str) = ("x-amz-server-side-encryption", "AES256");

/// Full Range matrix on a 100-byte plaintext object: 206 / clamp / suffix / suffix-overflow /
/// unsatisfiable 416 / invalid-and-multi-range → 200 full / Accept-Ranges on 200 and HEAD.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn range_get_matrix_small_object() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("range").await;
    let key = "matrix.bin";
    let data = pseudo_random(100);
    let put = fx.put_object(&bucket, key, &data).await;
    assert_eq!(put.status().as_u16(), 200, "PUT seed must 200");

    let expected = |s: usize, e: usize| data[s..e].to_vec();

    // 基本闭区间。
    let resp = fx.get_object_range(&bucket, key, "bytes=10-19").await;
    assert_eq!(resp.status().as_u16(), 206, "closed range must 206");
    assert_eq!(header(&resp, "content-range"), "bytes 10-19/100");
    assert_eq!(header(&resp, "content-length"), "10");
    assert_eq!(header(&resp, "accept-ranges"), "bytes");
    assert!(
        !header(&resp, "etag").is_empty() && header(&resp, "etag").starts_with('\"'),
        "206 must still return the object ETag"
    );
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        expected(10, 20)
    );

    // end 超界截断。
    let resp = fx.get_object_range(&bucket, key, "bytes=90-1000").await;
    assert_eq!(resp.status().as_u16(), 206, "clamped end must 206");
    assert_eq!(header(&resp, "content-range"), "bytes 90-99/100");
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        expected(90, 100)
    );

    // 开区间。
    let resp = fx.get_object_range(&bucket, key, "bytes=90-").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        expected(90, 100)
    );

    // 单字节。
    let resp = fx.get_object_range(&bucket, key, "bytes=99-99").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        expected(99, 100)
    );

    // 后缀。
    let resp = fx.get_object_range(&bucket, key, "bytes=-7").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(header(&resp, "content-range"), "bytes 93-99/100");
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        expected(93, 100)
    );

    // 后缀超长 → 全对象 206;`bytes=0-` 同。
    let resp = fx.get_object_range(&bucket, key, "bytes=-1000").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(header(&resp, "content-range"), "bytes 0-99/100");
    assert_eq!(resp.bytes().await.expect("206 body").to_vec(), data.clone());

    // 首 byte 越界 → 416 InvalidRange。
    let resp = fx.get_object_range(&bucket, key, "bytes=100-").await;
    assert_eq!(resp.status().as_u16(), 416, "start past EOF must 416");
    let body = resp.text().await.expect("416 body");
    assert!(
        body.contains("InvalidRange"),
        "416 body must carry code, got: {body}"
    );

    // `bytes=-0`:RFC 不可满足。
    let resp = fx.get_object_range(&bucket, key, "bytes=-0").await;
    assert_eq!(resp.status().as_u16(), 416, "bytes=-0 is unsatisfiable");

    // 语法坏 / 多区间 → 忽略,回 200 全对象。
    for bad in [
        "bytes=5-1",
        "bytes=not-a-range",
        "bytes=0-1,3-4",
        "items=0-9",
    ] {
        let resp = fx.get_object_range(&bucket, key, bad).await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "invalid `{bad}` must be ignored, not failed"
        );
        assert_eq!(header(&resp, "content-length"), "100");
        assert_eq!(resp.bytes().await.expect("200 body").to_vec(), data);
    }

    // 无 Range 的 200 也声明 Accept-Ranges。
    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "accept-ranges"), "bytes");

    // HEAD:忽略 Range,全量 Content-Length + Accept-Ranges,无 body。
    let uri = format!("/{bucket}/{key}");
    let mut headers = fx.signed_headers("HEAD", &uri, &[]);
    headers.insert("range", "bytes=0-9".parse().unwrap());
    let resp = fx
        .send(reqwest::Method::HEAD, &uri, headers, Vec::new())
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "HEAD ignores Range (S3 semantics)"
    );
    assert_eq!(header(&resp, "content-length"), "100");
    assert_eq!(header(&resp, "accept-ranges"), "bytes");
    let got = resp.bytes().await.expect("read HEAD body");
    assert!(got.is_empty(), "HEAD must not carry a body");
}

/// 空对象:任何 range 不可满足(416),无 range 仍可 200 全读。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn range_get_empty_object_is_416() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("range-empty").await;
    let key = "empty.bin";
    let resp = fx.put_object(&bucket, key, b"").await;
    assert_eq!(resp.status().as_u16(), 200, "empty PUT must 200");

    for range in ["bytes=0-0", "bytes=0-", "bytes=-5"] {
        let resp = fx.get_object_range(&bucket, key, range).await;
        assert_eq!(
            resp.status().as_u16(),
            416,
            "range `{range}` on an empty object must be 416"
        );
    }

    let resp = fx.get_object(&bucket, key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(header(&resp, "content-length"), "0");
}

/// SSE 对象走同一 range 语义:解密后切明文窗口,SSE 回显头照在。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn range_get_sse_object() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("range-sse").await;
    let key = "secret.bin";
    let data = pseudo_random(100_001);

    let put = fx
        .put_object_with_headers(&bucket, key, &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200, "SSE PUT must 200");

    let resp = fx.get_object_range(&bucket, key, "bytes=50000-50199").await;
    assert_eq!(resp.status().as_u16(), 206, "SSE range must 206");
    assert_eq!(header(&resp, "content-range"), "bytes 50000-50199/100001");
    assert_eq!(header(&resp, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(
        resp.bytes().await.expect("206 body").to_vec(),
        data[50000..50200].to_vec()
    );

    let resp = fx.get_object_range(&bucket, key, "bytes=-100").await;
    assert_eq!(resp.status().as_u16(), 206);
    assert_eq!(
        resp.bytes().await.expect("suffix body").to_vec(),
        data[data.len() - 100..].to_vec()
    );

    let resp = fx.get_object_range(&bucket, key, "bytes=100001-").await;
    assert_eq!(
        resp.status().as_u16(),
        416,
        "SSE object 416 at EOF boundary"
    );
}

// ── 条件 GET/HEAD + If-Range + ?partNumber(s3-read-compat)────────────────────

async fn head_with(
    fx: &S3Fixture,
    bucket: &str,
    key: &str,
    pairs: &[(&str, &str)],
) -> reqwest::Response {
    use reqwest::Method;
    let uri = format!("/{bucket}/{key}");
    let mut headers = fx.signed_headers("HEAD", &uri, &[]);
    for (name, value) in pairs {
        headers.insert(
            name.parse::<reqwest::header::HeaderName>().expect("name"),
            value.parse().expect("value"),
        );
    }
    fx.send(Method::HEAD, &uri, headers, Vec::new()).await
}

const DAY_PAST: &str = "Mon, 13 Nov 2023 22:13:20 GMT";
const DAY_FUTURE: &str = "Fri, 01 Jan 2100 00:00:00 GMT";

/// RFC 7232 §6 条件矩阵:If-Match(412/短路)/ If-None-Match(304,`*` 通配,
/// 不匹配短路)/ If-Modified-Since(304)/ If-Unmodified-Since(412)/ 坏日期 400;
/// HEAD 同语义空体;GET 200/304 全贴 ETag + Last-Modified。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn conditional_get_head_matrix() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("cond").await;
    let key = "cond.bin";
    let data = pseudo_random(100);
    let put = fx.put_object(&bucket, key, &data).await;
    assert_eq!(put.status().as_u16(), 200, "PUT seed must 200");
    let etag = header(&put, "etag");

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-match", etag.as_str())])
        .await;
    assert_eq!(resp.status().as_u16(), 200, "If-Match pass must 200");
    assert!(
        !header(&resp, "last-modified").is_empty(),
        "200 carries Last-Modified"
    );
    assert_eq!(resp.bytes().await.expect("body").len(), 100);

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-match", "\"deadbeef\"")])
        .await;
    assert_eq!(resp.status().as_u16(), 412, "If-Match mismatch must 412");
    let body = resp.text().await.expect("412 body");
    assert!(body.contains("PreconditionFailed"), "{body}");

    let resp = fx
        .get_object_with_headers(
            &bucket,
            key,
            &[("if-match", &etag), ("if-modified-since", DAY_FUTURE)],
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "If-Match pass short-circuits If-Modified-Since"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-none-match", etag.as_str())])
        .await;
    assert_eq!(resp.status().as_u16(), 304, "If-None-Match hit must 304");
    assert_eq!(header(&resp, "etag"), etag, "304 carries ETag");
    assert!(
        !header(&resp, "last-modified").is_empty(),
        "304 carries Last-Modified"
    );
    assert!(
        resp.bytes().await.expect("304 body").is_empty(),
        "304 must be bodiless"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-none-match", "*")])
        .await;
    assert_eq!(resp.status().as_u16(), 304, "* matches any");

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-none-match", "\"deadbeef\"")])
        .await;
    assert_eq!(resp.status().as_u16(), 200, "If-None-Match miss must 200");
    assert_eq!(
        resp.bytes().await.expect("body").len(),
        100,
        "miss serves the object"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-modified-since", DAY_FUTURE)])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        304,
        "not modified since future → 304"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-modified-since", DAY_PAST)])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "modified after past date → 200"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-unmodified-since", DAY_PAST)])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        412,
        "modified after unmodified-since must 412"
    );

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-unmodified-since", DAY_FUTURE)])
        .await;
    assert_eq!(resp.status().as_u16(), 200);

    let resp = fx
        .get_object_with_headers(&bucket, key, &[("if-modified-since", "not-a-date")])
        .await;
    assert_eq!(
        resp.status().as_u16(),
        400,
        "malformed conditional date → 400"
    );
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("InvalidArgument"), "{body}");

    // HEAD:304 / 412 / 200 三态,空体。
    let resp = head_with(&fx, &bucket, key, &[("if-none-match", etag.as_str())]).await;
    assert_eq!(resp.status().as_u16(), 304, "HEAD 304");
    assert!(!header(&resp, "last-modified").is_empty());

    let resp = head_with(&fx, &bucket, key, &[("if-unmodified-since", DAY_PAST)]).await;
    assert_eq!(resp.status().as_u16(), 412, "HEAD 412");

    let resp = head_with(&fx, &bucket, key, &[]).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert!(
        !header(&resp, "last-modified").is_empty(),
        "HEAD 200 has Last-Modified"
    );
}

/// If-Range 闸:ETag/未来日期过 → 206 按窗;失配/过旧/解析失败 → 200 全对象。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn if_range_gating() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ifrange").await;
    let key = "gated.bin";
    let data = pseudo_random(100);
    let put = fx.put_object(&bucket, key, &data).await;
    assert_eq!(put.status().as_u16(), 200);
    let etag = header(&put, "etag");

    let resp = fx
        .get_object_with_headers(
            &bucket,
            key,
            &[("range", "bytes=10-19"), ("if-range", &etag)],
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        206,
        "fresh validator serves the range"
    );
    assert_eq!(header(&resp, "content-range"), "bytes 10-19/100");

    let resp = fx
        .get_object_with_headers(
            &bucket,
            key,
            &[("range", "bytes=10-19"), ("if-range", DAY_FUTURE)],
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        206,
        "date at least as recent serves the range"
    );

    for bad in ["\"deadbeef\"", DAY_PAST, "garbage"] {
        let resp = fx
            .get_object_with_headers(&bucket, key, &[("range", "bytes=10-19"), ("if-range", bad)])
            .await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "stale/invalid If-Range `{bad}` nullifies the window"
        );
        assert!(!resp.headers().contains_key("content-range"));
        assert_eq!(
            resp.bytes().await.expect("full body").to_vec(),
            data,
            "whole object must be returned"
        );
    }
}

/// ?partNumber 矩阵(单片对象):part 1 全幅 206 / 其余 416 / ×Range 交集 /
/// ×If-Range 失败提权 200 / HEAD 忽略 / 空对象 416。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn part_number_single_part_object() {
    use reqwest::Method;
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("part1").await;
    let key = "single.bin";
    let data = pseudo_random(100);
    let put = fx.put_object(&bucket, key, &data).await;
    assert_eq!(put.status().as_u16(), 200);
    let etag = header(&put, "etag");

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=1"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 206, "part 1 → 206");
    assert_eq!(header(&resp, "content-range"), "bytes 0-99/100");
    assert_eq!(header(&resp, "content-length"), "100");
    assert_eq!(resp.bytes().await.expect("body").to_vec(), data);

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=2"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 416, "no second part");
    let body = resp.text().await.expect("416 body");
    assert!(
        body.contains("InvalidRange") && body.contains("1 part"),
        "{body}"
    );

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=0"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 416, "partNumber=0 is out of range");

    let uri = format!("/{bucket}/{key}?partNumber=1");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("range", "bytes=10-19".parse().expect("range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(resp.status().as_u16(), 206, "part 1 ∩ Range window");
    assert_eq!(header(&resp, "content-range"), "bytes 10-19/100");
    assert_eq!(
        resp.bytes().await.expect("body").to_vec(),
        data[10..20].to_vec()
    );

    let uri = format!("/{bucket}/{key}?partNumber=2");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("range", "bytes=0-9".parse().expect("range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        416,
        "empty intersection 416(先 part 越界)"
    );

    let uri = format!("/{bucket}/{key}?partNumber=1");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("range", "bytes=10-19".parse().expect("range"));
    headers.insert("if-range", "\"deadbeef\"".parse().expect("if-range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "failed validator escalates to whole object"
    );
    assert!(!resp.headers().contains_key("content-range"));
    assert_eq!(resp.bytes().await.expect("body").len(), 100);

    let uri = format!("/{bucket}/{key}?partNumber=1");
    let mut headers = fx.signed_headers("GET", &uri, &[]);
    headers.insert("if-range", etag.parse().expect("if-range"));
    let resp = fx.send(Method::GET, &uri, headers, Vec::new()).await;
    assert_eq!(
        resp.status().as_u16(),
        206,
        "passing validator keeps the part window"
    );
    assert_eq!(header(&resp, "content-range"), "bytes 0-99/100");

    let resp = fx
        .request(
            Method::HEAD,
            &format!("/{bucket}/{key}?partNumber=1"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 200, "HEAD ignores partNumber");
    assert_eq!(header(&resp, "content-length"), "100");

    let resp = fx.put_object(&bucket, "empty.bin", b"").await;
    assert_eq!(resp.status().as_u16(), 200);
    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/empty.bin?partNumber=1"),
            Vec::new(),
        )
        .await;
    assert_eq!(
        resp.status().as_u16(),
        416,
        "empty object has no satisfiable part"
    );
}

/// SSE 单片对象:?partNumber=1 = 解密后全幅 206(带回加密头)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn part_number_sse_object() {
    use reqwest::Method;
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("partsse").await;
    let key = "enc.bin";
    let data = pseudo_random(80);
    let put = fx
        .put_object_with_headers(&bucket, key, &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200, "SSE PUT must 200");

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=1"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 206, "SSE part 1 → 206");
    assert_eq!(header(&resp, "content-range"), "bytes 0-79/80");
    assert_eq!(header(&resp, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(
        resp.bytes().await.expect("decrypted body").to_vec(),
        data,
        "part 1 of SSE = whole plaintext"
    );

    let resp = fx
        .request(
            Method::GET,
            &format!("/{bucket}/{key}?partNumber=2"),
            Vec::new(),
        )
        .await;
    assert_eq!(resp.status().as_u16(), 416, "single-part SSE has no part 2");
}

// ── flexible checksums(s3-checksums)──────────────────────────────────────────

use antfs_s3::checksum::ChecksumAlgo;

/// 四族回环:PUT 验证+回显、GET/HEAD 按 mode 回显、无 mode 无头。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn checksum_roundtrip_all_algos() {
    let cases = [
        (ChecksumAlgo::Crc32, "x-amz-checksum-crc32"),
        (ChecksumAlgo::Crc32c, "x-amz-checksum-crc32c"),
        (ChecksumAlgo::Sha1, "x-amz-checksum-sha1"),
        (ChecksumAlgo::Sha256, "x-amz-checksum-sha256"),
    ];
    for (algo, hdr) in cases {
        let fx = S3Fixture::start().await;
        let bucket = fx.create_unique_bucket("cksum").await;
        let key = "obj.bin";
        let data = pseudo_random(512);
        let value = algo.compute(&data);

        let put = fx
            .put_object_with_headers(&bucket, key, &data, &[(hdr, value.as_str())])
            .await;
        assert_eq!(put.status().as_u16(), 200, "PUT {hdr} must 200");
        assert_eq!(header(&put, hdr), value, "PUT 辗转回显算出值");
        assert_eq!(header(&put, "x-amz-checksum-type"), "FULL_OBJECT");

        let mode = [("x-amz-checksum-mode", "ENABLED")];
        let get = fx.get_object_with_headers(&bucket, key, &mode).await;
        assert_eq!(get.status().as_u16(), 200);
        assert_eq!(header(&get, hdr), value, "GET mode 回显行内值");
        assert_eq!(header(&get, "x-amz-checksum-type"), "FULL_OBJECT");
        assert!(get.bytes().await.expect("body").len() == 512);

        let head = head_with(&fx, &bucket, key, &mode).await;
        assert_eq!(head.status().as_u16(), 200);
        assert_eq!(header(&head, hdr), value, "HEAD mode 回显");

        let plain = fx.get_object(&bucket, key).await;
        assert_eq!(plain.status().as_u16(), 200);
        assert!(
            !plain
                .headers()
                .keys()
                .any(|k| k.as_str().starts_with("x-amz-checksum-")),
            "无 mode 不回任何校验头"
        );
    }
}

/// 拒绝面:失配 400 BadDigest 零副作用 / 坏 base64 / 长度不符 / 未实现族 501 /
/// checksum-type 非 FULL_OBJECT / mode 坏值。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn checksum_rejection_matrix() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("cksumbad").await;
    let data = pseudo_random(256);

    let wrong = ChecksumAlgo::Sha256.compute(b"other-data");
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "mismatch.bin",
            &data,
            &[("x-amz-checksum-sha256", wrong.as_str())],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "mismatch must 400");
    let body = resp.text().await.expect("400 body");
    assert!(body.contains("BadDigest"), "{body}");
    let resp = fx.get_object(&bucket, "mismatch.bin").await;
    assert_eq!(resp.status().as_u16(), 404, "failed write leaves nothing");

    for value in ["not-base64!!", "0KDAqMDA="] {
        let resp = fx
            .put_object_with_headers(
                &bucket,
                "badval.bin",
                &data,
                &[("x-amz-checksum-crc32", value)],
            )
            .await;
        assert_eq!(
            resp.status().as_u16(),
            400,
            "malformed Base64 `{value}` must 400"
        );
        let body = resp.text().await.expect("400 body");
        assert!(body.contains("InvalidArgument"), "{body}");
    }
    // 合法 base64 但 digest 长度不符(8 字节 ≠ CRC32 的 4 字节)。
    let len_mismatch = "0KDAqMDAwMDAwMDA=";
    let resp = fx
        .put_object_with_headers(
            &bucket,
            "badlen.bin",
            &data,
            &[("x-amz-checksum-crc32", len_mismatch)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "length mismatch must 400");

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "nvme.bin",
            &data,
            &[("x-amz-checksum-crc64nvme", "AAAAAA==")],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 501, "crc64nvme is out of scope");
    let body = resp.text().await.expect("501 body");
    assert!(body.contains("crc64nvme"), "{body}");

    let resp = fx
        .put_object_with_headers(
            &bucket,
            "badtype.bin",
            &data,
            &[
                (
                    "x-amz-checksum-crc32",
                    ChecksumAlgo::Crc32.compute(&data).as_str(),
                ),
                ("x-amz-checksum-type", "COMPOSITE"),
            ],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 400, "type must be FULL_OBJECT");

    let put = fx.put_object(&bucket, "plain.bin", &data).await;
    assert_eq!(put.status().as_u16(), 200);
    let resp = fx
        .get_object_with_headers(&bucket, "plain.bin", &[("x-amz-checksum-mode", "garbage")])
        .await;
    assert_eq!(resp.status().as_u16(), 400, "mode value must be ENABLED");
}

/// 多算法并存全验证、存最强;覆盖后回显消失。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn checksum_precedence_and_overwrite() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ckprec").await;
    let key = "both.bin";
    let data = pseudo_random(128);

    let extras_owned = [
        ("x-amz-checksum-crc32", ChecksumAlgo::Crc32.compute(&data)),
        ("x-amz-checksum-sha256", ChecksumAlgo::Sha256.compute(&data)),
    ];
    let extras: Vec<(&str, &str)> = extras_owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let resp = fx
        .put_object_with_headers(&bucket, key, &data, &extras)
        .await;
    assert_eq!(resp.status().as_u16(), 200, "both valid");
    assert_eq!(header(&resp, "x-amz-checksum-sha256"), extras_owned[1].1);
    assert!(
        !resp.headers().contains_key("x-amz-checksum-crc32"),
        "存一份最强"
    );

    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(header(&get, "x-amz-checksum-sha256"), extras_owned[1].1);
    assert!(!get.headers().contains_key("x-amz-checksum-crc32"));

    let overwrite = fx.put_object(&bucket, key, b"smaller").await;
    assert_eq!(overwrite.status().as_u16(), 200);
    let get = fx
        .get_object_with_headers(&bucket, key, &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert!(
        !get.headers()
            .keys()
            .any(|k| k.as_str().starts_with("x-amz-checksum-")),
        "无校验覆盖 → 行内校验翻空"
    );
}

/// GetObjectAttributes 的 Checksum 面:有值双元素;无值省略;ObjectParts 501 维持。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn checksum_attributes_face() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("ckattr").await;
    let data = pseudo_random(300);
    let value = ChecksumAlgo::Sha1.compute(&data);
    let put = fx
        .put_object_with_headers(
            &bucket,
            "sha1.bin",
            &data,
            &[("x-amz-checksum-sha1", value.as_str())],
        )
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let uri = format!("/{bucket}/sha1.bin?attributes");
    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert(
        "x-amz-object-attributes",
        "Checksum,ObjectSize".parse().expect("attrs value"),
    );
    let resp = fx
        .send(reqwest::Method::GET, &uri, headers, Vec::new())
        .await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.expect("attributes body");
    assert!(
        body.contains(&format!(
            "<Checksum><ChecksumSHA1>{value}</ChecksumSHA1><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>"
        )),
        "{body}"
    );
    assert!(body.contains("<ObjectSize>300</ObjectSize>"), "{body}");

    let plain_put = fx.put_object(&bucket, "plain.bin", &data).await;
    assert_eq!(plain_put.status().as_u16(), 200);
    let uri = format!("/{bucket}/plain.bin?attributes");
    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert(
        "x-amz-object-attributes",
        "Checksum".parse().expect("attrs value"),
    );
    let resp = fx
        .send(reqwest::Method::GET, &uri, headers, Vec::new())
        .await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.text().await.expect("attributes body");
    assert!(
        !body.contains("<Checksum"),
        "no value → element omitted: {body}"
    );

    let uri = format!("/{bucket}/plain.bin?attributes");
    let mut headers = fx.signed_headers("GET", &uri, b"");
    headers.insert(
        "x-amz-object-attributes",
        "ObjectParts".parse().expect("attrs value"),
    );
    let resp = fx
        .send(reqwest::Method::GET, &uri, headers, Vec::new())
        .await;
    assert_eq!(resp.status().as_u16(), 501, "ObjectParts stays 501");
}

/// SSE 对象:校验按明文算/存/回显。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn checksum_sse_plaintext_face() {
    let fx = S3Fixture::start().await;
    let bucket = fx.create_unique_bucket("cksse").await;
    let data = pseudo_random(77);
    let value = ChecksumAlgo::Crc32c.compute(&data);
    let extras = [
        ("x-amz-server-side-encryption", "AES256"),
        ("x-amz-checksum-crc32c", value.as_str()),
    ];
    let put = fx
        .put_object_with_headers(&bucket, "enc.bin", &data, &extras)
        .await;
    assert_eq!(put.status().as_u16(), 200, "SSE PUT with checksum");
    assert_eq!(header(&put, "x-amz-checksum-crc32c"), value);

    let get = fx
        .get_object_with_headers(&bucket, "enc.bin", &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(get.status().as_u16(), 200);
    assert_eq!(header(&get, "x-amz-checksum-crc32c"), value, "明文口径回显");
    assert_eq!(header(&get, "x-amz-server-side-encryption"), "AES256");
    assert_eq!(get.bytes().await.expect("解码体").to_vec(), data);
}

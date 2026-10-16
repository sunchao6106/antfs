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

/// 轮换速率无关的本地新钥材料:tmp 下随机 64 hex(与 fixture 同格式)→ 载入 MasterKey。
fn write_new_master(fx: &S3Fixture) -> std::sync::Arc<antfs_s3::sse::MasterKey> {
    let material = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let path = fx._tmp.path().join("sse-master-new.key");
    std::fs::write(&path, &material).expect("write new key material");
    std::sync::Arc::new(
        antfs_s3::sse::MasterKey::load(path.to_string_lossy().as_ref())
            .expect("new master key loads"),
    )
}

/// 有界轮询等 blob 墓碑收敛(样式同 tests/object.rs:10s 上限、50ms 间隔)。
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

/// 三个 SSE 行(含 >64KiB 多分块;batch=2 强制跨页)+ 一个明文对照:换钥 → 幂等重跑
/// 全跳过 → 「重启」网关(新主钥)→ 读回全等(etag/size/字节/回显头/checksum/元数据);
/// 旧 blob 墓碑收敛、WAL 无残行;新主钥下的新写正常。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_key_rotation_swaps_master_and_preserves_reads() {
    let mut fx = S3Fixture::start_with_sse(true).await;
    let old_master = fx.state.sse.clone().expect("fixture sse master");
    let bucket = fx.create_unique_bucket("sserot").await;

    let small = pseudo_random(1024);
    let big = pseudo_random(64 * 1024 + 55);
    let mid = pseudo_random(77);
    let ck = antfs_s3::checksum::ChecksumAlgo::Crc32.compute(&mid);
    let put = fx
        .put_object_with_headers(
            &bucket,
            "mid.bin",
            &mid,
            &[
                ("x-amz-server-side-encryption", "AES256"),
                ("x-amz-checksum-crc32", ck.as_str()),
                ("x-amz-meta-note", "kept"),
            ],
        )
        .await;
    assert_eq!(put.status().as_u16(), 200, "sse PUT mid must 200");
    for (key, data) in [("small.bin", &small), ("big.bin", &big)] {
        let put = fx
            .put_object_with_headers(&bucket, key, data, &[SSE_HDR])
            .await;
        assert_eq!(put.status().as_u16(), 200, "sse PUT {key} must 200");
    }
    let plain = pseudo_random(999);
    let put = fx.put_object(&bucket, "plain.bin", &plain).await;
    assert_eq!(put.status().as_u16(), 200, "plaintext control PUT must 200");

    let new_master = write_new_master(&fx);
    assert_ne!(new_master.key_id(), old_master.key_id());

    let mut old_blobs = Vec::new();
    for key in ["mid.bin", "small.bin", "big.bin"] {
        let meta = fx
            .state
            .metastore
            .get_object(&bucket, key)
            .await
            .expect("meta read")
            .expect("live row");
        old_blobs.push(meta.blob_id);
    }

    let report =
        antfs_s3::sse_rotation::rotate(&fx.state, &old_master, &new_master, 2, Some(&bucket))
            .await
            .expect("rotation completes");
    assert_eq!(
        report,
        antfs_s3::sse_rotation::RotationReport {
            scanned: 3,
            rotated: 3,
            skipped_other_key: 0,
        },
        "batch=2 forces pagination over 3 rows"
    );

    let again =
        antfs_s3::sse_rotation::rotate(&fx.state, &old_master, &new_master, 2, Some(&bucket))
            .await
            .expect("re-run completes");
    assert_eq!(
        again,
        antfs_s3::sse_rotation::RotationReport {
            scanned: 3,
            rotated: 0,
            skipped_other_key: 3,
        },
        "re-run with same pair: already-rotated rows skip as other-key"
    );

    for blob in &old_blobs {
        poll_tombstoned(&fx, blob).await;
    }
    let pending = fx
        .state
        .metastore
        .pending_reclaims_batch(100)
        .await
        .expect("pending read");
    assert!(
        !old_blobs.iter().any(|b| pending.contains(b)),
        "rotation shadow arc must leave no WAL rows for the old blobs"
    );

    fx.restart_with_sse_master(new_master.clone()).await;

    for (key, data) in [("mid.bin", &mid), ("small.bin", &small), ("big.bin", &big)] {
        let get = fx.get_object(&bucket, key).await;
        assert_eq!(get.status().as_u16(), 200, "GET {key} after restart");
        assert_eq!(header(&get, "etag"), format!("\"{}\"", md5_hex(data)));
        assert_eq!(header(&get, "content-length"), data.len().to_string());
        assert_eq!(header(&get, "x-amz-server-side-encryption"), "AES256");
        let body = get.bytes().await.expect("get body");
        assert_eq!(
            body.as_ref(),
            data.as_slice(),
            "bytes survive rotation ({key})"
        );
    }
    let get_mid = fx
        .get_object_with_headers(&bucket, "mid.bin", &[("x-amz-checksum-mode", "ENABLED")])
        .await;
    assert_eq!(get_mid.status().as_u16(), 200);
    assert_eq!(header(&get_mid, "x-amz-checksum-crc32"), ck);
    assert_eq!(header(&get_mid, "x-amz-checksum-type"), "FULL_OBJECT");
    let head_mid = fx.head_object(&bucket, "mid.bin").await;
    assert_eq!(head_mid.status().as_u16(), 200);
    assert_eq!(header(&head_mid, "x-amz-meta-note"), "kept");
    assert_eq!(header(&head_mid, "etag"), format!("\"{}\"", md5_hex(&mid)));

    let get_plain = fx.get_object(&bucket, "plain.bin").await;
    assert_eq!(get_plain.status().as_u16(), 200);
    let body = get_plain.bytes().await.expect("plaintext body");
    assert_eq!(body.as_ref(), plain.as_slice(), "plaintext rows untouched");

    let fresh = pseudo_random(1234);
    let put = fx
        .put_object_with_headers(&bucket, "post-rot.bin", &fresh, &[SSE_HDR])
        .await;
    assert_eq!(
        put.status().as_u16(),
        200,
        "new writes under the new master"
    );
    let get = fx.get_object(&bucket, "post-rot.bin").await;
    assert_eq!(get.status().as_u16(), 200);
    let body = get.bytes().await.expect("fresh body");
    assert_eq!(body.as_ref(), fresh.as_slice());
}

/// 他代 key_id 行:轮换只处理「当前旧钥」的行,过往代残留原样不动。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_key_rotation_skips_foreign_key_rows() {
    let fx = S3Fixture::start_with_sse(true).await;
    let old_master = fx.state.sse.clone().expect("fixture sse master");
    let bucket = fx.create_unique_bucket("sseforeign").await;

    let data = pseudo_random(500);
    let put = fx
        .put_object_with_headers(&bucket, "a-legit.bin", &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let junk = pseudo_random(300);
    let (junk_blob, _) = fx
        .state
        .router
        .put_auto_with_ttl(
            axum::body::Bytes::from(junk.clone()),
            antfs_router::NO_EXPIRATION,
        )
        .await
        .expect("junk blob put");
    fx.state
        .metastore
        .put_object(antfs_s3::metastore::types::PutObjectMeta {
            bucket: bucket.clone(),
            key: "b-foreign.bin".into(),
            blob_id: junk_blob,
            size: junk.len() as u64,
            etag: md5_hex(&junk),
            content_type: Some("application/octet-stream".into()),
            user_meta: Default::default(),
            parts_manifest: None,
            sse: Some(antfs_s3::sse::descriptor("0123456789abcdef")),
            checksum: None,
        })
        .await
        .expect("foreign row insert");
    let pre = fx
        .state
        .metastore
        .get_object(&bucket, "b-foreign.bin")
        .await
        .expect("meta read")
        .expect("foreign row live");

    let new_master = write_new_master(&fx);
    let report =
        antfs_s3::sse_rotation::rotate(&fx.state, &old_master, &new_master, 2, Some(&bucket))
            .await
            .expect("rotation completes");
    assert_eq!(
        report,
        antfs_s3::sse_rotation::RotationReport {
            scanned: 2,
            rotated: 1,
            skipped_other_key: 1,
        }
    );

    let post = fx
        .state
        .metastore
        .get_object(&bucket, "b-foreign.bin")
        .await
        .expect("meta read")
        .expect("foreign row still live");
    assert_eq!(post.version, pre.version, "foreign row version untouched");
    assert_eq!(post.blob_id, pre.blob_id, "foreign row blob untouched");
    assert_eq!(post.sse, pre.sse, "foreign row descriptor untouched");
    assert!(
        fx.state.router.get(&pre.blob_id).await.is_ok(),
        "foreign blob must not be tombstoned"
    );
}

/// 坏描述符(未知算法)响亮中止:带定位与已轮换进度;行不动。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_key_rotation_aborts_on_corrupt_descriptor() {
    let fx = S3Fixture::start_with_sse(true).await;
    let old_master = fx.state.sse.clone().expect("fixture sse master");
    let bucket = fx.create_unique_bucket("sserotbad").await;

    let data = pseudo_random(300);
    let put = fx
        .put_object_with_headers(&bucket, "a-ok.bin", &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let junk = pseudo_random(64);
    let (junk_blob, _) = fx
        .state
        .router
        .put_auto_with_ttl(
            axum::body::Bytes::from(junk.clone()),
            antfs_router::NO_EXPIRATION,
        )
        .await
        .expect("junk blob put");
    fx.state
        .metastore
        .put_object(antfs_s3::metastore::types::PutObjectMeta {
            bucket: bucket.clone(),
            key: "b-bad.bin".into(),
            blob_id: junk_blob,
            size: junk.len() as u64,
            etag: md5_hex(&junk),
            content_type: Some("application/octet-stream".into()),
            user_meta: Default::default(),
            parts_manifest: None,
            sse: Some(r#"{"alg":"DES","key_id":"deadbeefdeadbeef"}"#.into()),
            checksum: None,
        })
        .await
        .expect("corrupt descriptor row insert");

    let new_master = write_new_master(&fx);
    let failure =
        antfs_s3::sse_rotation::rotate(&fx.state, &old_master, &new_master, 2, Some(&bucket))
            .await
            .expect_err("unknown algorithm must abort");
    assert_eq!(failure.bucket, bucket);
    assert_eq!(failure.key, "b-bad.bin");
    assert!(
        failure.error.contains("unknown sse algorithm"),
        "error names the cause: {}",
        failure.error
    );
    assert_eq!(
        failure.report,
        antfs_s3::sse_rotation::RotationReport {
            scanned: 2,
            rotated: 1,
            skipped_other_key: 0,
        },
        "progress: a-ok rotated before the abort"
    );
}

/// SSE 行带 parts manifest = 不可达状态(multipart SSE 写侧 501/拷贝重钥产单 blob):
/// 守卫中止而非默默展开。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sse_key_rotation_aborts_on_manifest_combo() {
    let fx = S3Fixture::start_with_sse(true).await;
    let old_master = fx.state.sse.clone().expect("fixture sse master");
    let bucket = fx.create_unique_bucket("sseromanifest").await;

    let data = pseudo_random(200);
    let put = fx
        .put_object_with_headers(&bucket, "a-ok.bin", &data, &[SSE_HDR])
        .await;
    assert_eq!(put.status().as_u16(), 200);

    let junk = pseudo_random(64);
    let (junk_blob, _) = fx
        .state
        .router
        .put_auto_with_ttl(
            axum::body::Bytes::from(junk.clone()),
            antfs_router::NO_EXPIRATION,
        )
        .await
        .expect("junk blob put");
    fx.state
        .metastore
        .put_object(antfs_s3::metastore::types::PutObjectMeta {
            bucket: bucket.clone(),
            key: "b-manifest.bin".into(),
            blob_id: junk_blob,
            size: junk.len() as u64,
            etag: md5_hex(&junk),
            content_type: Some("application/octet-stream".into()),
            user_meta: Default::default(),
            parts_manifest: Some(r#"[{"etag":"d41d8c","n":1}]"#.into()),
            sse: Some(antfs_s3::sse::descriptor(old_master.key_id())),
            checksum: None,
        })
        .await
        .expect("manifest combo row insert");

    let new_master = write_new_master(&fx);
    let failure =
        antfs_s3::sse_rotation::rotate(&fx.state, &old_master, &new_master, 2, Some(&bucket))
            .await
            .expect_err("manifest+sse combination must abort");
    assert_eq!(failure.key, "b-manifest.bin");
    assert!(
        failure.error.contains("parts manifest"),
        "error names the cause: {}",
        failure.error
    );
    assert_eq!(
        failure.report,
        antfs_s3::sse_rotation::RotationReport {
            scanned: 2,
            rotated: 1,
            skipped_other_key: 0,
        },
        "progress: a-ok rotated before the abort"
    );
}

//! `store-composed-backfill` 集成测试:`POST /admin/backfill-composed` 的甄别、落位、
//! 报告与幂等。素材用**数据面普通 PUT**(无 `?composed=true`)落库,忠实模拟升级前的
//! 存量行;等待一律带条件断言,不用固定 sleep。

use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn store_config(segment_size_bytes: u64) -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

fn server(dir: &std::path::Path) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 4 << 20,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    StoreServer::start_with(cfg, store_config(1 << 20), &registry).expect("start")
}

async fn send(router: &axum::Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, body)
}

async fn create_replica(router: &axum::Router) {
    let (status, _) = send(
        router,
        Request::post("/admin/partitions/0")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

async fn put_plain(router: &axum::Router, key: &str, data: Vec<u8>) {
    let req = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(data))
        .unwrap();
    let (status, body) = send(router, req).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "put {key}: {}",
        String::from_utf8_lossy(&body)
    );
}

async fn backfill(router: &axum::Router) -> serde_json::Value {
    let req = Request::post("/admin/backfill-composed")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(router, req).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "backfill: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).expect("report json")
}

async fn metadata(router: &axum::Router, key: &str) -> serde_json::Value {
    let req = Request::get(format!("/partitions/0/blobs/{key}/metadata"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(router, req).await;
    assert_eq!(status, StatusCode::OK, "metadata {key}");
    serde_json::from_slice(&body).expect("metadata json")
}

/// 一条"真描述符"字节(两片,尺寸自洽;chunk id 指向本测试里另两个普通 blob)。
fn descriptor_bytes(key_a: &str, len_a: u64, key_b: &str, len_b: u64) -> Vec<u8> {
    let entry = |key: &str, size: u64| antfs_descriptor::ChunkRef {
        id: {
            let mut out = [0u8; 8];
            out.copy_from_slice(&hex::decode(key).expect("hex key"));
            out
        },
        size,
        expiration_ms: -1,
    };
    antfs_descriptor::encode(
        b"legacy-session",
        &[entry(key_a, len_a), entry(key_b, len_b)],
    )
    .expect("encode")
}

/// 主弧:存量无位描述符落位、ASTC 前缀普通件不落位、已删行不复活、二跑幂等
/// —— 报告计数与索引态双断言,GET 字节前后不变。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_migrates_legacy_descriptors_and_reports_honestly() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();
    create_replica(&router).await;

    // 三个素材(全部普通 PUT = 无位,尘封的存量行):
    // ① 片 a/b 与引用它们的无位真描述符;② ASTC 前缀普通件;③ 回填前已删的无位描述符。
    put_plain(&router, "000000000000000a", b"chunk-a-payload".to_vec()).await;
    put_plain(&router, "000000000000000b", b"chunk-b".to_vec()).await;
    let desc = descriptor_bytes("000000000000000a", 14, "000000000000000b", 7);
    put_plain(&router, "000000000000000d", desc.clone()).await;
    let mut astc = b"ASTC".to_vec();
    astc.extend_from_slice(b"plain file bytes, not a descriptor");
    put_plain(&router, "000000000000000e", astc.clone()).await;
    put_plain(
        &router,
        "000000000000000f",
        descriptor_bytes("000000000000000a", 14, "000000000000000b", 7),
    )
    .await;
    let (status, _) = send(
        &router,
        Request::delete("/partitions/0/blobs/000000000000000f")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // 一轮:报告中 migrated=1 / invalid=1 / deleted=1 / already=0;位与无位各归其位。
    let r = backfill(&router).await;
    assert_eq!(r["migrated"].as_u64().unwrap(), 1, "存量真描述符落位: {r}");
    assert_eq!(
        r["invalid"].as_u64().unwrap(),
        1,
        "ASTC 前缀普通件不落位: {r}"
    );
    assert_eq!(r["deleted"].as_u64().unwrap(), 1, "已删行如实计数: {r}");
    assert_eq!(r["already"].as_u64().unwrap(), 0);
    assert_eq!(
        r["candidates"].as_u64().unwrap(),
        2,
        "过甄别面的是未删无位小对象(d/e);f 在 deleted 分支早退: {r}"
    );
    let m = metadata(&router, "000000000000000d").await;
    assert_eq!(m["composed"], serde_json::json!(true), "位落索引: {m}");
    let m = metadata(&router, "000000000000000e").await;
    assert_eq!(m["composed"], serde_json::json!(false), "普通件维持无位");
    // 片是普通对象:不进甄别面(非 MAGIC 前缀),但不报 invalid。
    let m = metadata(&router, "000000000000000a").await;
    assert_eq!(m["composed"], serde_json::json!(false));

    // GET 字节不变 + 已删行仍 404(绝不复活)。
    let req = Request::get("/partitions/0/blobs/000000000000000d")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, desc, "回填前后字节逐位不变");
    let req = Request::get("/partitions/0/blobs/000000000000000f")
        .body(Body::empty())
        .unwrap();
    let (status, _) = send(&router, req).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "墓碑不被复活");

    // 二跑:幂等(migrated=0,already=1),报告即落位证明。
    let r = backfill(&router).await;
    assert_eq!(r["migrated"].as_u64().unwrap(), 0, "幂等: {r}");
    assert_eq!(r["already"].as_u64().unwrap(), 1);
    assert_eq!(r["invalid"].as_u64().unwrap(), 1, "普通件依旧不落位");
}

/// 组合写口写下的描述符(`?composed=true`)本就带位:扫面只计 already,不动它。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_leaves_flagged_blobs_at_already() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();
    create_replica(&router).await;

    put_plain(&router, "000000000000000a", b"chunk-a".to_vec()).await;
    let desc = descriptor_bytes("000000000000000a", 7, "000000000000000a", 7);
    let req = Request::put("/partitions/0/blobs/000000000000000d?composed=true")
        .body(Body::from(desc.clone()))
        .unwrap();
    let (status, body) = send(&router, req).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );

    let r = backfill(&router).await;
    assert_eq!(r["migrated"].as_u64().unwrap(), 0, "新世界不改: {r}");
    assert_eq!(r["already"].as_u64().unwrap(), 1);
    let m = metadata(&router, "000000000000000d").await;
    assert_eq!(m["composed"], serde_json::json!(true));
    let req = Request::get("/partitions/0/blobs/000000000000000d")
        .body(Body::empty())
        .unwrap();
    let (_, body) = send(&router, req).await;
    assert_eq!(body, desc);
}

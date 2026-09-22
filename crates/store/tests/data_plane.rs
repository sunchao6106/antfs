//! store-server S1 数据面集成测试:HTTP put/get 往返 + 错误码,进程内 `oneshot`(不起真端口)。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use antfs_store::{StoreServer, StoreServerConfig};
use tower::ServiceExt;

fn small_store_config() -> antfs_blob::StoreConfig {
    // 小段:避免测试预分配 8GiB 默认段。
    antfs_blob::StoreConfig { segment_size_bytes: 1 << 20, index_max_inmem_elements: 1000, io_error_count_to_trigger_shutdown: 0 }
}

fn server(dir: &std::path::Path) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 4 << 20,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    StoreServer::start_with(cfg, small_store_config(), &registry).expect("start")
}

async fn status_of(router: &axum::Router, req: Request<Body>) -> StatusCode {
    router.clone().oneshot(req).await.unwrap().status()
}

#[tokio::test]
async fn put_get_roundtrip_and_error_codes() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    // 建 replica。
    let create = Request::post("/admin/partitions/0").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, create).await, StatusCode::CREATED);

    let key = "01".repeat(8); // 8 字节 → 16 hex
    let data = b"hello ambry data plane".to_vec();

    // PUT。
    let put = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(data.clone()))
        .unwrap();
    assert_eq!(status_of(&router, put).await, StatusCode::CREATED);

    // GET 字节一致。
    let get = Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), data.as_slice(), "GET must return the exact bytes written");

    // 重复 PUT 同 key → 409。
    let dup = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(data.clone()))
        .unwrap();
    assert_eq!(status_of(&router, dup).await, StatusCode::CONFLICT);

    // 未知 key GET → 404。
    let missing = "ab".repeat(8);
    let get_missing =
        Request::get(format!("/partitions/0/blobs/{missing}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, get_missing).await, StatusCode::NOT_FOUND);

    // 非法 key(非 8 字节)→ 400。
    let bad = Request::get("/partitions/0/blobs/zz").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, bad).await, StatusCode::BAD_REQUEST);

    // 未建 replica 的分区 → 404。
    let no_part =
        Request::put(format!("/partitions/99/blobs/{key}")).body(Body::from(data)).unwrap();
    assert_eq!(status_of(&router, no_part).await, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn mutations_delete_undelete_ttl() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    let create = Request::post("/admin/partitions/0").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, create).await, StatusCode::CREATED);

    let key = "07".repeat(8);
    let data = b"mutable blob".to_vec();

    // put
    let put = Request::put(format!("/partitions/0/blobs/{key}")).body(Body::from(data.clone())).unwrap();
    assert_eq!(status_of(&router, put).await, StatusCode::CREATED);

    // delete -> 204, then get -> 404
    let del = Request::delete(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, del).await, StatusCode::NO_CONTENT);
    let get = Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, get).await, StatusCode::NOT_FOUND);

    // repeat delete -> 409
    let del2 = Request::delete(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, del2).await, StatusCode::CONFLICT);

    // undelete -> 200, then get -> 200
    let und = Request::post(format!("/partitions/0/blobs/{key}/undelete")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, und).await, StatusCode::OK);
    let get2 = Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, get2).await, StatusCode::OK);

    // update ttl -> 204, repeat -> 409
    let ttl = Request::post(format!("/partitions/0/blobs/{key}/ttl?expiration_ms=99999999999"))
        .body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, ttl).await, StatusCode::NO_CONTENT);
    let ttl2 = Request::post(format!("/partitions/0/blobs/{key}/ttl?expiration_ms=99999999999"))
        .body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, ttl2).await, StatusCode::CONFLICT);

    // 非法 key -> 400
    let bad = Request::delete("/partitions/0/blobs/zz").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, bad).await, StatusCode::BAD_REQUEST);
}

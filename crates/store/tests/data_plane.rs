//! store-server S1 数据面集成测试:HTTP put/get 往返 + 错误码,进程内 `oneshot`(不起真端口)。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use antfs_store::{StoreServer, StoreServerConfig};
use tower::ServiceExt;

fn store_config(segment_size_bytes: u64) -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn server_with(
    dir: &std::path::Path,
    store_config: antfs_blob::StoreConfig,
    partition_capacity_bytes: u64,
    max_body_bytes: u64,
) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes,
        max_body_bytes,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    StoreServer::start_with(cfg, store_config, &registry).expect("start")
}

fn server(dir: &std::path::Path) -> StoreServer {
    // 小段:避免测试预分配 8GiB 默认段;body 上限用默认 2 MiB。
    server_with(dir, store_config(1 << 20), 4 << 20, 2 * 1024 * 1024)
}

async fn status_of(router: &axum::Router, req: Request<Body>) -> StatusCode {
    router.clone().oneshot(req).await.unwrap().status()
}

/// 同 registry(被测服务自己的 `/metrics`)的 Prometheus 文本。
async fn metrics_text(router: &axum::Router) -> String {
    let req = Request::get("/metrics").body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

/// admin 端点的 JSON 请求。
fn json_req(method: &str, path: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// 建 pid=0 的本地副本。
async fn create_replica(router: &axum::Router) {
    let req = Request::post("/admin/partitions/0").body(Body::empty()).unwrap();
    assert_eq!(status_of(router, req).await, StatusCode::CREATED);
}

#[tokio::test]
async fn metrics_endpoint_exports_prometheus_text() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    let req = Request::get("/metrics").body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("store_server_put_total"),
        "/metrics must export registered store metrics, got:\n{text}"
    );
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

#[tokio::test]
async fn request_latency_and_in_flight_metrics_exported() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    let create = Request::post("/admin/partitions/0").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, create).await, StatusCode::CREATED);

    let key = "11".repeat(8);
    let put = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(b"latency probe".to_vec()))
        .unwrap();
    assert_eq!(status_of(&router, put).await, StatusCode::CREATED);
    let get = Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, get).await, StatusCode::OK);

    let req = Request::get("/metrics").body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        text.contains(r#"store_server_request_duration_seconds_count{op="put"} 1"#),
        "put latency must be observed once, got:\n{text}"
    );
    assert!(
        text.contains(r#"store_server_request_duration_seconds_count{op="get"} 1"#),
        "get latency must be observed once, got:\n{text}"
    );
    // 请求已返回,在途 gauge 必须归零(guard 在早退/正常出口都 dec)。
    assert!(
        text.contains(r#"store_server_in_flight_requests{op="put"} 0"#),
        "in-flight gauge must return to zero after the request finishes, got:\n{text}"
    );
}

#[tokio::test]
async fn manual_compaction_endpoint_reclaims_and_404s() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    // 无本地副本 → 404。
    let missing = Request::post("/admin/partitions/99/compact").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, missing).await, StatusCode::NOT_FOUND);

    let create = Request::post("/admin/partitions/0").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, create).await, StatusCode::CREATED);

    // 造死数据:同一 key 的历史版本(软删/复活/改 TTL 都会追加新记录,旧记录成为死数据)。
    let key = "22".repeat(8);
    let put = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(b"to be compacted".to_vec()))
        .unwrap();
    assert_eq!(status_of(&router, put).await, StatusCode::CREATED);
    let del = Request::delete(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, del).await, StatusCode::NO_CONTENT);
    let und =
        Request::post(format!("/partitions/0/blobs/{key}/undelete")).body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, und).await, StatusCode::OK);

    let compact = Request::post("/admin/partitions/0/compact").body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(compact).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["dead_bytes"].as_u64().unwrap_or(0) > 0, "superseded records must count as dead: {v}");
    assert!(v["live_bytes"].as_u64().unwrap_or(0) > 0, "the newest record must survive: {v}");
    assert!(
        v["segments_after"].as_u64().unwrap() <= v["segments_before"].as_u64().unwrap(),
        "compaction must not grow the segment count: {v}"
    );

    // compact 也计入指标。
    let req = Request::get("/metrics").body(Body::empty()).unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("store_server_manual_compactions_total 1"),
        "manual compaction must be counted, got:\n{text}"
    );
}

/// 任务 2:成功 blob 请求的延迟按对象大小分桶(对齐 Ambry 的 small/medium/large 分类)。
/// 边界:50 KiB / 1 MiB;大 blob(1.5 MiB)仍在默认 `max_body_bytes`(2 MiB)内。
#[tokio::test]
async fn blob_request_duration_is_bucketed_by_object_size() {
    let tmp = tempfile::tempdir().unwrap();
    // 段容量必须 > 大 blob 记录(否则引擎按"单条记录不跨段"拒收),故段设 4 MiB。
    let srv = server_with(tmp.path(), store_config(4 << 20), 32 << 20, 2 * 1024 * 1024);
    let router = srv.router();
    create_replica(&router).await;

    // (桶, 长度):1 KiB = small,100 KiB = medium,1.5 MiB = large。
    let cases: [(&str, usize); 3] =
        [("small", 1 << 10), ("medium", 100 << 10), ("large", 3 << 19)];
    for (i, (bucket, len)) in cases.iter().enumerate() {
        let key = format!("{:02x}", i + 1).repeat(8);
        let data = vec![u8::try_from(i + 1).unwrap(); *len];
        let put = Request::put(format!("/partitions/0/blobs/{key}"))
            .body(Body::from(data.clone()))
            .unwrap();
        assert_eq!(status_of(&router, put).await, StatusCode::CREATED, "{bucket} PUT");

        let get = Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(get).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{bucket} GET");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), *len, "{bucket} GET must return the full body");
    }

    let text = metrics_text(&router).await;
    for (bucket, _) in cases {
        for op in ["put", "get"] {
            let want =
                format!(r#"store_server_blob_request_duration_seconds_count{{op="{op}",size="{bucket}"}} 1"#);
            assert!(text.contains(&want), "expect `{want}`, got:\n{text}");
        }
    }
}

/// 任务 3:禁用 put → 503 + Retry-After;复制/运维端点豁免;re-enable 恢复;未知 op → 400。
#[tokio::test]
async fn request_control_silences_data_ops_but_spares_replication_and_admin() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();
    create_replica(&router).await;

    let key = "44".repeat(8);
    let put_req = || {
        Request::put(format!("/partitions/0/blobs/{key}"))
            .body(Body::from(b"gc probe".to_vec()))
            .unwrap()
    };
    assert_eq!(status_of(&router, put_req()).await, StatusCode::CREATED);

    // 节点级禁用 put(未给 ttl_secs → 用配置默认 1800s)。
    let resp = router
        .clone()
        .oneshot(json_req("POST", "/admin/request-control", r#"{"op":"put","enable":true}"#))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 直连 PUT 被 503 挡住,且告知何时可重试(有 TTL)。
    let blocked = "55".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{blocked}"))
                .body(Body::from(b"blocked".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(resp.headers().contains_key("retry-after"), "有 TTL 的禁用必须带 Retry-After");

    // 豁免清单:拉复制与运维端点必须照常 200(挡掉它们会让副本不收敛/掉数据)。
    for path in [
        "/partitions/0/replica-metadata?max_bytes=1024".to_string(),
        "/partitions/0/segments".to_string(),
        format!("/partitions/0/blobs/{key}?replication=1"),
        "/admin/request-control".to_string(),
        "/partitions/0/scrub-report".to_string(),
        "/partitions/0/verify".to_string(),
        "/metrics".to_string(),
    ] {
        let resp = router
            .clone()
            .oneshot(Request::get(path.clone()).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{path} 必须豁免");
    }
    // 只禁了 put:普通读不受影响。
    assert_eq!(
        status_of(
            &router,
            Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap()
        )
        .await,
        StatusCode::OK,
        "禁 put 不得影响 get"
    );

    let text = metrics_text(&router).await;
    assert!(
        text.contains(r#"store_server_rejected_requests_total{op="put",reason="disabled"} 1"#),
        "被拒请求必须计数, got:\n{text}"
    );
    // 被拒请求不进引擎:put 的 request_duration 只应记到那一次成功的 PUT。
    assert!(
        text.contains(r#"store_server_request_duration_seconds_count{op="put"} 1"#),
        "被拒的请求不得计入 request_duration, got:\n{text}"
    );

    // re-enable → 恢复正常。
    let resp = router
        .clone()
        .oneshot(json_req("POST", "/admin/request-control", r#"{"op":"put","enable":false}"#))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        status_of(
            &router,
            Request::put(format!("/partitions/0/blobs/{blocked}"))
                .body(Body::from(b"blocked".to_vec()))
                .unwrap()
        )
        .await,
        StatusCode::CREATED
    );
    // 禁用集已空。
    let resp = router
        .clone()
        .oneshot(Request::get("/admin/request-control").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "[]", "re-enable 后禁用集必须为空");

    // 未知 op → 400;本地无该副本的分区 → 404。
    assert_eq!(
        status_of(
            &router,
            json_req("POST", "/admin/request-control", r#"{"op":"bogus","enable":true}"#)
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        status_of(
            &router,
            json_req(
                "POST",
                "/admin/request-control",
                r#"{"op":"put","partition_id":99,"enable":true}"#
            )
        )
        .await,
        StatusCode::NOT_FOUND
    );
}

/// 任务 3:分区级禁用(只挡该分区)与惰性过期(0 秒 TTL 立即到期,不 sleep)。
#[tokio::test]
async fn request_control_partition_scope_and_lazy_ttl_expiry() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();
    create_replica(&router).await;

    let key = "66".repeat(8);
    let get_req =
        || Request::get(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap();
    let put = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(b"ttl probe".to_vec()))
        .unwrap();
    assert_eq!(status_of(&router, put).await, StatusCode::CREATED);
    assert_eq!(status_of(&router, get_req()).await, StatusCode::OK);

    // 分区级禁用 get(短 TTL)→ 该分区的读被挡。
    assert_eq!(
        status_of(
            &router,
            json_req(
                "POST",
                "/admin/request-control",
                r#"{"op":"get","partition_id":0,"enable":true,"ttl_secs":60}"#
            )
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(status_of(&router, get_req()).await, StatusCode::SERVICE_UNAVAILABLE);
    // 禁用集里能看到该条(分区级 + 剩余秒数)。
    let resp = router
        .clone()
        .oneshot(Request::get("/admin/request-control").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v[0]["op"], "get");
    assert_eq!(v[0]["partition_id"], 0);
    assert!(v[0]["remaining_secs"].as_u64().unwrap_or(0) <= 60);
    // 解除后恢复。
    assert_eq!(
        status_of(
            &router,
            json_req(
                "POST",
                "/admin/request-control",
                r#"{"op":"get","partition_id":0,"enable":false}"#
            )
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(status_of(&router, get_req()).await, StatusCode::OK);

    // ttl_secs = 0 = 0 秒有效期:判定时即视为已过期(惰性摘除,不需要 sleep)。
    assert_eq!(
        status_of(
            &router,
            json_req("POST", "/admin/request-control", r#"{"op":"delete","enable":true,"ttl_secs":0}"#)
        )
        .await,
        StatusCode::OK
    );
    assert_eq!(
        status_of(
            &router,
            Request::delete(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap()
        )
        .await,
        StatusCode::NO_CONTENT,
        "0 秒 TTL 必须立即过期(不挡请求)"
    );
    let resp = router
        .clone()
        .oneshot(Request::get("/admin/request-control").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        String::from_utf8(body.to_vec()).unwrap(),
        "[]",
        "过期条目必须被惰性摘除"
    );
}

/// `max_body_bytes` 经 `DefaultBodyLimit` 生效:边界内放行、超限在 extractor 前 413。
#[tokio::test]
async fn max_body_bytes_rejects_oversized_bodies() {
    let tmp = tempfile::tempdir().unwrap();
    // 段容量 > body 上限,确保被拒原因只能是 body 上限(不是"单条记录跨段")。
    let srv = server_with(tmp.path(), store_config(4 << 20), 16 << 20, 1 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let at_limit = "71".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{at_limit}"))
                .body(Body::from(vec![7u8; 1 << 20]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "等于上限的 body 必须放行");

    let over_limit = "72".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{over_limit}"))
                .body(Body::from(vec![7u8; (1 << 20) + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE, "超限 body 必须被拒");
}

/// 暂存目录(流式 put 的中转文件落点)里的残留文件名。
fn staging_leftovers(disk_dir: &std::path::Path) -> Vec<String> {
    std::fs::read_dir(disk_dir.join("tmp"))
        .map(|it| {
            it.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default()
}

async fn get_body(router: &axum::Router, path: &str) -> (StatusCode, Vec<u8>) {
    let resp = router
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, body)
}

/// 带 `Content-Length` 的 PUT:走**流式**路径(暂存到盘 + 引擎按长度顺序写),
/// 大 body 不再整个缓进内存;且成功/失败都不留暂存文件。
#[tokio::test]
async fn put_with_content_length_streams_body_and_leaves_no_staging_file() {
    let tmp = tempfile::tempdir().unwrap();
    // 段容量/盘容量/body 上限都放大到 3 MiB body 收得下(本测试只验流式路径本身)。
    let srv = server_with(tmp.path(), store_config(8 << 20), 64 << 20, 8 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let key = "a1".repeat(8);
    let data = vec![0x5Au8; 3 << 20];
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{key}"))
                .header("content-length", data.len().to_string())
                .body(Body::from(data.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "流式 put 必须成功");

    let (status, got) = get_body(&router, &format!("/partitions/0/blobs/{key}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(got, data, "流式写的对象必须与整块写等价(逐字节一致)");

    assert!(
        staging_leftovers(tmp.path()).is_empty(),
        "成功路径必须清掉暂存文件,实际残留 {:?}",
        staging_leftovers(tmp.path())
    );
}

/// 流式路径同样受 `store_server.max_body_bytes` 约束:声明长度超限 → 413。
#[tokio::test]
async fn put_with_content_length_over_limit_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(8 << 20), 64 << 20, 1 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let over = "b2".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{over}"))
                .header("content-length", ((1 << 20) + 1).to_string())
                .body(Body::from(vec![9u8; (1 << 20) + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(staging_leftovers(tmp.path()).is_empty(), "被拒的请求不该留下暂存文件");
}

/// 声明长度与实际字节数不符(客户端中途断开/超发):400 且**不产生任何可见记录**。
#[tokio::test]
async fn put_with_mismatched_content_length_creates_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(8 << 20), 64 << 20, 8 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let key = "c3".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{key}"))
                .header("content-length", "1000")
                .body(Body::from(vec![1u8; 10]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "短读必须报错而不是写半个对象");

    let (status, _) = get_body(&router, &format!("/partitions/0/blobs/{key}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "失败的上传不得留下可见对象");
    assert!(
        staging_leftovers(tmp.path()).is_empty(),
        "失败路径也要清掉暂存文件,实际残留 {:?}",
        staging_leftovers(tmp.path())
    );
}

/// `PUT ?expiration_ms=`:建对象时**出生即带 TTL**(此前只能先 PUT 再调 `/ttl`)。
#[tokio::test]
async fn put_honors_expiration_ms_query() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(1 << 20), 4 << 20, 1 << 20);
    let router = srv.router();
    create_replica(&router).await;

    // 已过去的过期时刻 → 立即可判定过期(不依赖 sleep,见 CLAUDE.md §5.6)。
    let key = "d4".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{key}?expiration_ms=1000"))
                .body(Body::from(vec![3u8; 64]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let (status, _) = get_body(&router, &format!("/partitions/0/blobs/{key}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "已过期对象的数据读必须 404");

    // 元数据仍可见,且 expired=true。
    let (status, body) = get_body(&router, &format!("/partitions/0/blobs/{key}/metadata")).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["expiration_ms"], 1000);
    assert_eq!(v["expired"], true);
    assert_eq!(v["deleted"], false);
}

/// 元数据端点:不读数据体即可拿状态;墓碑也可读(对齐 Ambry `GetBlobInfo`)。
#[tokio::test]
async fn metadata_endpoint_reports_state_without_reading_body() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(1 << 20), 4 << 20, 1 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let key = "e5".repeat(8);
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{key}"))
                .body(Body::from(vec![4u8; 128]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let (status, body) = get_body(&router, &format!("/partitions/0/blobs/{key}/metadata")).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["size"], 128, "size 是数据体字节数");
    assert_eq!(v["deleted"], false);
    assert_eq!(v["life_version"], 0);
    assert_eq!(v["expiration_ms"], -1, "-1 = 不过期");

    // 删除后:数据读 404,但元数据仍 200 且 deleted=true(墓碑可见)。
    let resp = router
        .clone()
        .oneshot(Request::delete(format!("/partitions/0/blobs/{key}")).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let (status, body) = get_body(&router, &format!("/partitions/0/blobs/{key}/metadata")).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["deleted"], true);

    // 从未写过的 key → 404;非法 key → 400。
    let (status, _) = get_body(&router, &format!("/partitions/0/blobs/{}/metadata", "f6".repeat(8))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get_body(&router, "/partitions/0/blobs/zz/metadata").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// 分区写满 → **507**(不再混在 500 里),让压测/运维一眼看出"该加分区了"。
#[tokio::test]
async fn store_full_maps_to_507() {
    let tmp = tempfile::tempdir().unwrap();
    // 盘容量极小:写满即 StoreFull。
    let srv = server_with(tmp.path(), store_config(1 << 20), 2 << 20, 1 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let mut saw_full = false;
    for i in 0..64u32 {
        let key = format!("{:08x}", i).repeat(2);
        let resp = router
            .clone()
            .oneshot(
                Request::put(format!("/partitions/0/blobs/{key}"))
                    .body(Body::from(vec![7u8; 128 * 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();
        if resp.status() == StatusCode::INSUFFICIENT_STORAGE {
            saw_full = true;
            break;
        }
    }
    assert!(saw_full, "写满定容 replica 必须返回 507 Insufficient Storage");
}

/// 上传被**中途放弃**时不得把暂存文件漏在盘上(真 socket:裸 TCP 发满 headers 与部分 body 后直接断开)。
///
/// 这是实测抓到的 bug:store 侧原本在 `.await` 之后手写 `remove_file`,而连接断开时 hyper 会 drop 掉
/// 在飞的 handler future → 清理代码永不执行(一次 2 MiB × 3000 次压测漏出 1600+ 个文件)。现在靠 RAII 兜住。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_upload_leaves_no_staging_file() {
    use tokio::io::AsyncWriteExt;

    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(8 << 20), 64 << 20, 8 << 20);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = srv.router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    // 建 replica(真 HTTP,走 admin 端点)。
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/partitions/0"))
        .send()
        .await
        .expect("admin create replica");
    assert_eq!(resp.status().as_u16(), 201);

    // 声明 4 MiB(> INLINE_BODY_MAX ⇒ 走暂存路径),只发 64 KiB 就断开。
    let key = "ee".repeat(8);
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "PUT /partitions/0/blobs/{key} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n\r\n",
        4 * 1024 * 1024
    );
    sock.write_all(head.as_bytes()).await.unwrap();
    sock.write_all(&vec![7u8; 64 * 1024]).await.unwrap();
    sock.flush().await.unwrap();
    // 给 handler 一点时间进入 staging,然后**直接断开**(不写完 body)。
    let staging = tmp.path().join("tmp");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if std::fs::read_dir(&staging).map(|it| it.count()).unwrap_or(0) > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    drop(sock);

    // 有界轮询:暂存目录最终必须为空。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let left: Vec<String> = std::fs::read_dir(&staging)
            .map(|it| {
                it.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        if left.is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "上传被放弃后暂存文件必须清掉,实际残留 {left:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// 单条记录放不进一个段(段容量 1 MiB、body 2 MiB 且上限放宽到 4 MiB):必须给 **400**(客户端该分片),
/// 而不是 500 —— 这类"客户端可控"的失败混进 500 会让压测/运维误判成服务端故障。
#[tokio::test]
async fn record_too_big_for_segment_is_400_not_500() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server_with(tmp.path(), store_config(1 << 20), 64 << 20, 4 << 20);
    let router = srv.router();
    create_replica(&router).await;

    let key = "dd".repeat(8);
    let data = vec![7u8; 2 << 20];
    let resp = router
        .clone()
        .oneshot(
            Request::put(format!("/partitions/0/blobs/{key}"))
                .header("content-length", data.len().to_string())
                .body(Body::from(data))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "记录超段容量是客户端问题 → 400");
    assert!(staging_leftovers(tmp.path()).is_empty(), "被拒的上传同样不得留下暂存文件");
}

#[tokio::test]
async fn scrub_report_summarises_replica_and_404s_unknown_partition() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    // 无本地副本 → 404
    let missing =
        Request::get("/partitions/9/scrub-report").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, missing).await, StatusCode::NOT_FOUND);

    assert_eq!(
        status_of(&router, Request::post("/admin/partitions/0").body(Body::empty()).unwrap()).await,
        StatusCode::CREATED
    );
    for (n, len) in [(1u8, 4usize), (2, 6), (3, 2)] {
        let key = format!("{:02x}", n).repeat(8);
        let put = Request::put(format!("/partitions/0/blobs/{key}"))
            .body(Body::from(vec![n; len]))
            .unwrap();
        assert_eq!(status_of(&router, put).await, StatusCode::CREATED);
    }
    // 删掉一个 → 墓碑
    let del = Request::delete(format!("/partitions/0/blobs/{}", "03".repeat(8)))
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_of(&router, del).await, StatusCode::NO_CONTENT);

    let resp = router
        .clone()
        .oneshot(Request::get("/partitions/0/scrub-report").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["partition_id"], 0);
    assert_eq!(v["live_records"], 2, "两个活 key: {v}");
    assert_eq!(v["tombstones"], 1, "一个墓碑: {v}");
    assert_eq!(v["live_bytes"], 10, "活数据字节 = 4 + 6: {v}");
    assert!(v["segments"].as_array().is_some(), "应带段清单: {v}");
    assert!(v.get("token_seg").is_some() && v.get("token_off").is_some(), "应带 log 末端游标: {v}");
    assert!(
        !v["token_seg"].is_null(),
        "写过后 log 应非空(有末端游标): {v}"
    );

    let text = metrics_text(&router).await;
    assert!(text.contains("store_server_scrub_reports_total 1"), "摘要调用应计数: {text}");
}

#[tokio::test]
async fn verify_endpoint_is_clean_then_detects_online_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    let missing = Request::get("/partitions/9/verify").body(Body::empty()).unwrap();
    assert_eq!(status_of(&router, missing).await, StatusCode::NOT_FOUND);

    assert_eq!(
        status_of(&router, Request::post("/admin/partitions/0").body(Body::empty()).unwrap()).await,
        StatusCode::CREATED
    );
    for n in 0..6u8 {
        let key = format!("{:02x}", n).repeat(8);
        let put = Request::put(format!("/partitions/0/blobs/{key}"))
            .body(Body::from(vec![n; 64]))
            .unwrap();
        assert_eq!(status_of(&router, put).await, StatusCode::CREATED);
    }

    // 干净副本:零坏记录、未截断
    let resp = router
        .clone()
        .oneshot(Request::get("/partitions/0/verify").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["bad_records"], 0, "干净副本: {v}");
    assert_eq!(v["index_mismatch"], 0, "索引应一致: {v}");
    assert_eq!(v["first_bad_offset"], serde_json::Value::Null);
    assert_eq!(v["truncated"], false);
    assert!(v["records"].as_u64().unwrap() >= 6, "应扫到至少 6 条记录: {v}");

    // 在线损坏:翻转首段第一条记录的 magic(物理 64 = 数据 offset 0)
    let seg = std::fs::read_dir(tmp.path().join("partition_0/log"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
        .expect("段文件");
    let mut bytes = std::fs::read(&seg).unwrap();
    assert!(bytes.len() > 128);
    bytes[64] ^= 0xff;
    std::fs::write(&seg, &bytes).unwrap();

    let resp = router
        .clone()
        .oneshot(Request::get("/partitions/0/verify").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["bad_records"], 1, "应检出 1 条坏记录: {v}");
    assert_eq!(v["first_bad_offset"]["segment"], 0);
    assert_eq!(v["first_bad_offset"]["offset"], 0);
    assert!(v["records"].as_u64().unwrap() >= 5, "坏记录之后应继续扫: {v}");

    let text = metrics_text(&router).await;
    assert!(text.contains("store_server_verify_runs_total 2"), "两次 verify 应计数: {text}");
    assert!(
        text.contains("store_server_verify_bad_records_total 1"),
        "坏记录应计数: {text}"
    );
}

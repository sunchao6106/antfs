//! A3 集成测试:手动 compaction 的**异步任务**端点与台账契约。
//!
//! 同步路径(`POST /admin/partitions/{pid}/compact`)的既有行为/响应体不变;`?async=1` 立即返回
//! `job_id`,结果经 `GET /admin/compactions[/{job_id}]` 查询。等待一律是**带超时的条件轮询**
//! (CLAUDE.md §6),不用固定 sleep。

use std::time::{Duration, Instant};

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
    }
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
    StoreServer::start_with(cfg, store_config(1 << 20), &registry).expect("start")
}

/// 发一个请求,返回 (状态码, body 字节)。
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

async fn get_json(router: &axum::Router, path: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::get(path).body(Body::empty()).unwrap();
    let (status, body) = send(router, req).await;
    let v = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// 建 pid=0 的本地副本,并造出死数据(同一 key 的历史版本:PUT 之后 DELETE,原记录被墓碑取代)。
async fn replica_with_dead_data(router: &axum::Router) {
    let (status, _) = send(
        router,
        Request::post("/admin/partitions/0")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // key:16 hex = 高 32bit 分区 0 + 低 32bit objectId(副本以默认前缀 0 建库)。
    let key = "0000000000000022";
    let put = Request::put(format!("/partitions/0/blobs/{key}"))
        .body(Body::from(b"to be compacted".to_vec()))
        .unwrap();
    assert_eq!(send(router, put).await.0, StatusCode::CREATED);
    let del = Request::delete(format!("/partitions/0/blobs/{key}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(router, del).await.0, StatusCode::NO_CONTENT);
}

/// 轮询任务到终态(`done`/`failed`);超时即失败(有界等待,不 sleep 拍脑袋)。
async fn wait_job(router: &axum::Router, job_id: u64) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (status, v) = get_json(router, &format!("/admin/compactions/{job_id}")).await;
        assert_eq!(status, StatusCode::OK, "已提交的 job 必须可查:{v}");
        if v["state"] == "done" || v["state"] == "failed" {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "job {job_id} 未在超时内到达终态:{v}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 提交异步 compaction:`?async=1` → 立即 200 + `job_id`。
async fn submit_async(router: &axum::Router) -> u64 {
    let req = Request::post("/admin/partitions/0/compact?async=1")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(router, req).await;
    assert_eq!(status, StatusCode::OK, "异步提交必须立即 200");
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    v["job_id"]
        .as_u64()
        .unwrap_or_else(|| panic!("响应体必须带 job_id:{v}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_compaction_reports_job_and_lands_in_the_jobs_table() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();
    replica_with_dead_data(&router).await;

    // 异步提交:立即拿 job_id;此刻尚未跑完(running)或已跑完(done),两者都合法。
    let job_id = submit_async(&router).await;
    let (status, submitted) = get_json(&router, &format!("/admin/compactions/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(submitted["partition_id"], 0);
    assert!(
        submitted["started_at_ms"].is_u64(),
        "running 必须带 started_at_ms:{submitted}"
    );
    assert!(
        matches!(submitted["state"].as_str(), Some("running" | "done")),
        "提交后只可能 running 或(小数据下已跑完的)done:{submitted}"
    );

    // 轮询到终态:结果字段与同步路径一致。
    let job = wait_job(&router, job_id).await;
    assert_eq!(job["state"], "done", "本用例的 compact 不该失败:{job}");
    let result = &job["result"];
    let mut keys: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "dead_bytes",
            "live_bytes",
            "segments_after",
            "segments_before"
        ],
        "异步结果字段必须与同步路径一致:{result}"
    );
    assert!(
        result["live_bytes"].as_u64().unwrap_or(0) > 0,
        "最新记录必须留下:{result}"
    );
    assert!(
        result["dead_bytes"].as_u64().unwrap_or(0) > 0,
        "被覆盖的历史版本算死数据:{result}"
    );
    assert!(
        result["segments_after"].as_u64().unwrap() <= result["segments_before"].as_u64().unwrap(),
        "compaction 不得让段数变多:{result}"
    );
    assert!(
        job["finished_at_ms"].is_u64(),
        "done 必须带 finished_at_ms:{job}"
    );
    assert!(job.get("error").is_none(), "done 不该带 error:{job}");

    // 台账列表:最新在前,含刚完成的任务。
    let (status, list) = get_json(&router, "/admin/compactions").await;
    assert_eq!(status, StatusCode::OK);
    let jobs = list["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), 1, "只提交了 1 个异步任务:{list}");
    assert_eq!(jobs[0]["job_id"].as_u64(), Some(job_id));

    // 同步路径:同一分区的字段与语义必须完全对齐(键集合逐项相等)。
    let sync = Request::post("/admin/partitions/0/compact")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(&router, sync).await;
    assert_eq!(status, StatusCode::OK);
    let sync_v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let mut sync_keys: Vec<&str> = sync_v
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    sync_keys.sort_unstable();
    assert_eq!(
        sync_keys, keys,
        "同步响应与异步 result 必须是同一组字段:sync={sync_v}"
    );

    // 指标:running/done 各计一次;手动 compact 调用计数把 sync + async 都算上。
    let text = String::from_utf8(
        send(
            &router,
            Request::get("/metrics").body(Body::empty()).unwrap(),
        )
        .await
        .1,
    )
    .unwrap();
    assert!(
        text.contains(r#"store_server_compaction_jobs_total{state="running"} 1"#),
        "异步提交必须计 running, got:\n{text}"
    );
    assert!(
        text.contains(r#"store_server_compaction_jobs_total{state="done"} 1"#),
        "完成必须计 done, got:\n{text}"
    );
    assert!(
        text.contains("store_server_manual_compactions_total 2"),
        "got:\n{text}"
    );
}

/// 未知 `job_id` → 404;无本地副本 → 404(与同步路径同码);同步路径仍可用。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_job_is_404_and_sync_path_is_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = server(tmp.path());
    let router = srv.router();

    // 还没建副本:sync / async 都是 404(不能凭空 compact)。
    let sync = Request::post("/admin/partitions/9/compact")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&router, sync).await.0, StatusCode::NOT_FOUND);
    let async_req = Request::post("/admin/partitions/9/compact?async=1")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(&router, async_req).await.0, StatusCode::NOT_FOUND);

    // 未知 job_id → 404;空台账 → jobs 为空数组。
    assert_eq!(
        get_json(&router, "/admin/compactions/12345").await.0,
        StatusCode::NOT_FOUND
    );
    let (status, list) = get_json(&router, "/admin/compactions").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        list["jobs"].as_array().unwrap().len(),
        0,
        "台账只记异步任务,同步 compact 不入账:{list}"
    );

    // 同步路径行为不变:200 + 四个统计字段。
    replica_with_dead_data(&router).await;
    let sync = Request::post("/admin/partitions/0/compact")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(&router, sync).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    for field in [
        "live_bytes",
        "dead_bytes",
        "segments_before",
        "segments_after",
    ] {
        assert!(v[field].is_u64(), "同步响应缺字段 {field}:{v}");
    }
    // 同步 compact 之后台账仍为空(它不产生 job)。
    let (_, list) = get_json(&router, "/admin/compactions").await;
    assert_eq!(list["jobs"].as_array().unwrap().len(), 0, "{list}");

    // `?async=0` 与不带参数等价:走同步路径(拿不到 job_id,直接拿统计)。
    let sync_zero = Request::post("/admin/partitions/0/compact?async=0")
        .body(Body::empty())
        .unwrap();
    let (status, body) = send(&router, sync_zero).await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(
        v["segments_before"].is_u64(),
        "?async=0 必须仍是同步路径:{v}"
    );
}

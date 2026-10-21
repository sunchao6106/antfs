//! 后台一致性巡检集成测试(`store-scrub-background`):真 loop 在真节点上蜗牛爬。
//!
//! 覆盖:① 循环自动推进(带界轮询指标,不动 sleep)、发现坏字节进后台专用指标(与手动
//! verify 计数分得清);② `/admin/scrub-control` 掐停后批数不动、恢复后继续;③ 手动
//! verify 端点回显续扫字段(`bytes_read` / `end_cursor`),且不受暂停开关影响(应急检查常在)。
//! 等待一律有界轮询(CLAUDE.md §6)。

mod common;

use std::time::Duration;

use antfs_blob::StoreKey;
use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::wait_until;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// 单条记录的磁盘占位:24B 记录开销 + 128B 数据。
// 5ded718 记录 v4 去 2B 帧 magic(24→22B 头)后,单条 128B 记录磁盘实占 = 150。
const REC: u64 = 22 + 128;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

/// 数据面 key:分区 0 + 低 32bit = `n`。
fn key(n: u8) -> StoreKey {
    StoreKey::from_parts(0, n as u32)
}

/// 起一个开了后台巡检的**独立**节点(不接 master,interval=1s 便于观察),建 pid=0 副本。
/// `serve()` 里才是循环的家(手动 axum::serve(router()) 不会起任何后台 task)。
/// 断言全部走 metrics 句柄与 router oneshot,不需要对外 URL(port 0 即可)。
async fn spawn_scrub_node(dir: &std::path::Path) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        scrub_background_enabled: true,
        scrub_background_interval_secs: 1,
        scrub_background_max_in_flight: 1,
        scrub_max_batch_bytes: REC,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(cfg, store_config(), &registry).expect("start");
    srv.replicas().ensure_replica(0).unwrap();
    srv.serve().await.expect("serve");
    srv
}

/// 写 3 条记录并落盘(副本目录 = 数据目录下唯一盘的 partitions/0 / 视布局而定,
/// 直接用 server 的 ReplicaManager 写,不需要再打开)。
async fn put_records(srv: &StoreServer, n: u8) {
    let h = srv.replicas().get(0).expect("副本应已建");
    for i in 0..n {
        h.store.put(key(i), &[i; 128]).unwrap();
    }
    h.store.flush().unwrap();
}

/// 经 `POST /admin/scrub-control` 开关,断言响应回显。
async fn set_scrub_paused(router: &axum::Router, paused: bool) {
    let req = Request::builder()
        .method("POST")
        .uri("/admin/scrub-control")
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"paused":{paused}}}"#)))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["paused"], paused, "POST 必须回显操作后的状态");
}

/// 循环真的在爬:批数与字节数自动增长(1s tick,iterval 抖动 ±25%),坏记录数为 0。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_loop_crawls_automatically() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = spawn_scrub_node(tmp.path()).await;
    put_records(&srv, 3).await;

    let metrics = srv.metrics();
    let covered = wait_until(Duration::from_secs(30), || {
        metrics.scrub_background_bytes.get() >= 3 * REC
            && metrics.scrub_background_batches.get() >= 3
    })
    .await;
    assert!(
        covered,
        "30s 内应爬完一轮(1s tick × 3 batch): batches={} bytes={}",
        metrics.scrub_background_batches.get(),
        metrics.scrub_background_bytes.get()
    );
    assert_eq!(
        metrics.scrub_background_bad_records.get(),
        0,
        "干净副本不该报坏记录"
    );
    assert_eq!(
        metrics.verify_runs.get(),
        0,
        "后台扫描不占用手动 verify 的计数口径"
    );
}

/// 盘上翻转字节:后台指标必须报警(与拉复制自愈无冲突 —— 这里只断言"发现"),
/// 且批数持续增长(碰到坏记录按声明长度跳过、续扫不停摆)。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_loop_finds_corrupted_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = spawn_scrub_node(tmp.path()).await;
    put_records(&srv, 3).await;

    // 翻坏第 2 条(offset = REC)的帧标识:物理 = 段头 64 + REC。
    let seg = std::fs::read_dir(tmp.path().join("partition_0").join("log"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
        .expect("至少一个段文件");
    let mut raw = std::fs::read(&seg).unwrap();
    raw[64 + REC as usize] ^= 0xff;
    std::fs::write(&seg, &raw).unwrap();

    let metrics = srv.metrics();
    let found = wait_until(Duration::from_secs(30), || {
        metrics.scrub_background_bad_records.get() >= 1
    })
    .await;
    assert!(found, "坏字节应当被后台巡检发现并计入专用指标");
    let batches_at_find = metrics.scrub_background_batches.get();
    let still_crawling = wait_until(Duration::from_secs(30), || {
        metrics.scrub_background_batches.get() > batches_at_find
    })
    .await;
    assert!(
        still_crawling,
        "发现坏字节后循环必须继续(按声明长度跳过,不停摆)"
    );
}

/// 掐停:批数停在原地(两倍 tick 间隔内零增长);恢复后继续爬。暂停期间手动 verify 不受影响。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrub_control_pauses_and_resumes_the_loop() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = spawn_scrub_node(tmp.path()).await;
    put_records(&srv, 3).await;

    let metrics = srv.metrics();
    let router = srv.router();
    let started = wait_until(Duration::from_secs(30), || {
        metrics.scrub_background_batches.get() >= 1
    })
    .await;
    assert!(started, "前置:循环应已开始爬");

    set_scrub_paused(&router, true).await;
    let froze_at = metrics.scrub_background_batches.get();
    // 负向断言必须花时间:等两个 tick(interval=1s,抖动后 ≤1.25s/拍)确认零增长。
    tokio::time::sleep(Duration::from_millis(2600)).await;
    assert_eq!(
        metrics.scrub_background_batches.get(),
        froze_at,
        "掐停后两个 tick 内不得有任何新批"
    );

    // 暂停期间手动 verify 照常可用且回显续扫字段(应急检查不被开关没收)。
    let resp = router
        .clone()
        .oneshot(
            Request::get(format!("/partitions/0/verify?max_bytes={REC}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(v["records"], 1, "手动单批照常");
    assert_eq!(v["end_cursor"]["offset"], REC);

    set_scrub_paused(&router, false).await;
    let resumed = wait_until(Duration::from_secs(30), || {
        metrics.scrub_background_batches.get() > froze_at
    })
    .await;
    assert!(resumed, "恢复后循环必须继续爬");
}

/// 手动 verify 端点契约:预算到点回 `truncated=true` + `bytes_read` + `end_cursor`;
/// 预算内扫完回 `end_cursor=null`。分批运维据此手动续扫。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_endpoint_echoes_resume_fields() {
    let tmp = tempfile::tempdir().unwrap();
    let srv = spawn_scrub_node(tmp.path()).await;
    put_records(&srv, 3).await;

    let router = srv.router();
    let one = |max: u64| {
        let router = router.clone();
        async move {
            let resp = router
                .oneshot(
                    Request::get(format!("/partitions/0/verify?max_bytes={max}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let v: serde_json::Value =
                serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            v
        }
    };

    let partial = one(REC).await;
    assert_eq!(partial["truncated"], true);
    assert_eq!(partial["bytes_read"], REC);
    assert_eq!(partial["end_cursor"]["segment"], 0);
    assert_eq!(partial["end_cursor"]["offset"], REC);

    let full = one(u64::MAX).await;
    assert_eq!(full["truncated"], false);
    assert_eq!(full["end_cursor"], serde_json::Value::Null);
    assert_eq!(full["records"], 3);
}

/// 没开会怎样:`scrub_background_enabled = false` 时不任何扫描(批数恒 0),
/// 指标与开关仍可读 —— 这是"默认无腿"契约。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_by_default_means_no_batches() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![tmp.path().to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(cfg, store_config(), &registry).expect("start");
    srv.replicas().ensure_replica(0).unwrap();
    srv.serve().await.expect("serve");
    put_records(&srv, 2).await;

    let metrics = srv.metrics();
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert_eq!(
        metrics.scrub_background_batches.get(),
        0,
        "没开就不能有扫描"
    );
    assert_eq!(metrics.scrub_background_paused.get(), 0, "开关读数默认正常");
}

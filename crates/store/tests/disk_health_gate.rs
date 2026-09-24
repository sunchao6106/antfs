//! **盘健康闸门(D2)在真 HTTP 数据面上的行为**:一块盘被判不可服务之后,落在这块盘上的请求
//! **快失败 503**,落在那块仍然健康的盘上的请求**完全不受影响**;`/metrics` 与 `/admin/**`
//! **永远照常**(不能做出一个无法远程恢复的节点)。
//!
//! ## 故障注入为什么走 `force_disk_unavailable`
//!
//! 真的把一块盘弄成 D 状态在测试里造不出来(也绝不该在开发机上造)。`StoreServer::force_disk_unavailable`
//! **不绕过任何判据** —— 它喂的是与生产探测/看门狗**完全相同**的迟滞状态机,只是合成观测而已。
//! 于是这些用例测的是真链路:合成观测 → 迟滞 → `DiskStatus` → 中间件 → HTTP 状态码。
//!
//! ## 这里每条用例守的是哪条性质
//!
//! | 用例 | 守的性质 |
//! |---|---|
//! | `unknown_disk_state_does_not_gate` | **默认部署回归**:探测默认关 ⇒ 每块盘都是 `UNKNOWN` ⇒ **一律放行** |
//! | `unavailable_disk_fails_fast_on_every_data_plane_route` | 数据面各条路由(含**拉复制读**/`replica-metadata`/`segments`)统一 503;`Retry-After` 与 reason 标签口径 |
//! | `a_bad_disk_does_not_take_down_requests_on_another_disk` | **盘的隔离性**:闸门按"副本所在的盘"判,不连坐 |
//! | `metrics_and_admin_stay_reachable_while_a_disk_is_unavailable` | **可远程恢复**:`/metrics` 与 `/admin/**` 不被闸门短路 |
//! | `unknown_partition_is_still_404` | 闸门不吞掉 404(没有本地副本 ≠ 盘坏) |
//! | `recovering_the_disk_restores_service` | 迟滞的**退出**方向:连续好轮 + 稳定期满 ⇒ 自动回归 |
//! | `background_worker_skips_the_unavailable_disk` | 本地维护(flush/compaction)跳过坏盘,且**跳过可见**(`disk_worker_skipped_total`) |

use std::path::Path;
use std::time::Duration;

use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

/// 盘 id = 目录 basename(`DiskManager::mount` 的口径)。
fn disk_id_of(dir: &Path) -> String {
    dir.file_name()
        .expect("tempdir has a name")
        .to_string_lossy()
        .into_owned()
}

/// 起一个**两盘**节点(本地维护 worker 关掉:这些用例只关心数据面与显式调用的后台动作)。
fn two_disk_node(dirs: &[&Path], cfg: StoreServerConfig) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: dirs
            .iter()
            .map(|d| d.to_string_lossy().into_owned().into())
            .collect(),
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 4 << 20,
        local_worker_enabled: false,
        ..cfg
    };
    StoreServer::start_with(cfg, store_config(), &antfs_metric::Registry::new()).expect("start")
}

fn data_key(object_id: u32) -> String {
    format!("{object_id:016x}")
}

async fn response_of(router: &axum::Router, req: Request<Body>) -> axum::response::Response {
    router.clone().oneshot(req).await.unwrap()
}

async fn status_of(router: &axum::Router, req: Request<Body>) -> StatusCode {
    response_of(router, req).await.status()
}

fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

fn put(path: &str, body: &[u8]) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(path)
        .body(Body::from(body.to_vec()))
        .unwrap()
}

/// 建 `pid` 的本地副本(经真 admin 端点)。
async fn create_replica(router: &axum::Router, pid: u32) {
    let req = Request::post(format!("/admin/partitions/{pid}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(status_of(router, req).await, StatusCode::CREATED);
}

async fn metrics_text(router: &axum::Router) -> String {
    let resp = response_of(router, get("/metrics")).await;
    assert_eq!(resp.status(), StatusCode::OK, "/metrics 必须始终可达");
    String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

/// 某条 series 的值(读不到 → 返回 `None`,让调用方自己给出可读的失败信息)。
fn series_value(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find(|l| l.starts_with(series) && l[series.len()..].starts_with(' '))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// 该副本落在哪块盘上,以及"另一块"是谁(不假设 `pick` 的顺序,由实际落点反推)。
fn home_and_other(srv: &StoreServer, dirs: &[&Path]) -> (String, String) {
    let home = srv.replicas().get(0).expect("replica exists").disk_id;
    let ids: Vec<String> = dirs.iter().map(|d| disk_id_of(d)).collect();
    let other = ids
        .into_iter()
        .find(|id| *id != home)
        .expect("两块盘里必有一块不是它的落点");
    (home, other)
}

/// **默认部署回归**(最要紧的一条):探测默认关 ⇒ 每块盘从未被观测 ⇒ `UNKNOWN` ⇒ **一律放行**。
///
/// 若哪天有人把"未知"当成"坏",默认部署会**一块盘都服务不了** —— 这条用例就是那道闸门。
#[tokio::test]
async fn unknown_disk_state_does_not_gate() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;

    let text = metrics_text(&router).await;
    for id in [disk_id_of(d0.path()), disk_id_of(d1.path())] {
        assert!(
            text.contains(&format!(
                r#"store_server_disk_health_state{{disk_id="{id}",state="UNKNOWN"}} 1"#
            )),
            "未观测的盘必须显式报 UNKNOWN(不是'一条 series 都没有'):\n{text}"
        );
    }
    assert_eq!(
        status_of(
            &router,
            put(&format!("/partitions/0/blobs/{}", data_key(1)), b"v")
        )
        .await,
        StatusCode::CREATED,
        "从未观测 ⇒ 必须放行(默认关探测的部署行为不变)"
    );
    assert_eq!(
        status_of(
            &router,
            get(&format!("/partitions/0/blobs/{}", data_key(1)))
        )
        .await,
        StatusCode::OK
    );
}

/// 数据面**每一条**落在该盘上的路由都必须 503,并带 `Retry-After`;`reason` 标签要能区分
/// "盘不可用"与"池满"/"人工静默"。
///
/// 特别覆盖**拉复制那三个端点**:`RequestControl` 刻意豁免它们(别让自己变成收不到数据的孤岛),
/// 但**盘闸门不沿用那个豁免** —— 从一块已经坏了的盘上拉数据本来就拉不动,放进去只会再漏一条线程。
#[tokio::test]
async fn unavailable_disk_fails_fast_on_every_data_plane_route() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;
    let (home, _other) = home_and_other(&srv, &[d0.path(), d1.path()]);

    let key = data_key(1);
    // 先写进去、再成功读一次:好让后面的断言有个**基线** ——
    // ① `GET` 本来是 200(否则 503 与 404 分不清);
    // ② 盘维度请求数本来就有一条 `get` 序列,后面才能断言"被挡下的请求没让它增长"。
    assert_eq!(
        status_of(
            &router,
            put(&format!("/partitions/0/blobs/{key}"), b"payload")
        )
        .await,
        StatusCode::CREATED
    );
    assert_eq!(
        status_of(&router, get(&format!("/partitions/0/blobs/{key}"))).await,
        StatusCode::OK,
        "坏盘注入之前,这条读本来是好的"
    );

    srv.force_disk_unavailable(&home);
    assert!(!antfs_store::disk_health::is_available(
        &srv.disk_health(),
        &home
    ));

    // (路径, 期望的 op 标签) —— op 名是 `request_control::path_op` 的真值。
    let routes: [(&str, &str); 7] = [
        (&format!("/partitions/0/blobs/{key}"), "get"),
        (&format!("/partitions/0/blobs/{key}/metadata"), "metadata"),
        ("/partitions/0/replica-metadata", "replica_metadata"),
        ("/partitions/0/segments", "segments"),
        ("/partitions/0/scrub-report", "scrub_report"),
        ("/partitions/0/verify", "verify"),
        (
            &format!("/partitions/0/blobs/{key}?replication=1"),
            "replication_get",
        ),
    ];
    for (path, _op) in routes {
        let resp = response_of(&router, get(path)).await;
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "坏盘上的 {path} 必须 503"
        );
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .map(|v| v.to_str().unwrap()),
            Some("1"),
            "{path} 应带 Retry-After: 1(提示上游立刻换副本)"
        );
    }
    // 写路径同样被挡,且**连 body 都不读**(中间件在 extractor 之前短路)。
    assert_eq!(
        status_of(
            &router,
            put(&format!("/partitions/0/blobs/{key}"), b"again")
        )
        .await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let text = metrics_text(&router).await;
    // 每条路由各留下一次拒绝,且 op 标签**逐个对得上**(打点标签错位会让看板把故障归错地方)。
    for op in routes
        .iter()
        .map(|(_, op)| *op)
        .chain(std::iter::once("put"))
        .collect::<Vec<_>>()
    {
        assert!(
            text.contains(&format!(
                r#"store_server_rejected_requests_total{{op="{op}",reason="disk_unavailable"}} 1"#
            )),
            "缺 {op} 的 disk_unavailable 计数:\n{text}"
        );
    }
    assert!(
        text.contains(&format!(
            r#"store_server_disk_health_state{{disk_id="{home}",state="UNAVAILABLE"}} 1"#
        )),
        "被摘的盘应如实上报 UNAVAILABLE:\n{text}"
    );
    // 被闸门挡下的请求**一个都没进引擎**:中间件在 handler 之前短路,故 `in_flight` 没有残留,
    // 盘维度"请求数"也不会增长(`with_disk` 只在 handler 里调)——
    // 也就是说闸门既没烧线程,也没污染盘的服务质量观测。
    let home_gets = format!(r#"store_server_disk_requests_total{{disk_id="{home}",op="get"}}"#);
    assert_eq!(
        series_value(&text, &home_gets),
        Some(1.0),
        "被闸门挡下的 GET 不该落进盘维度请求数(只有之前那一次成功的算):\n{text}"
    );
    assert_eq!(
        series_value(&text, r#"store_server_in_flight_requests{op="get"}"#),
        Some(0.0),
        "闸门短路不该留下在途计数:\n{text}"
    );
}

/// **盘的隔离性**:闸门按"该副本**所在的盘**"判 —— 另一块盘被判不可服务时,这块盘上的请求
/// **一点不受影响**(不连坐、也不把请求挪到别的盘上)。
#[tokio::test]
async fn a_bad_disk_does_not_take_down_requests_on_another_disk() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;
    let (_home, other) = home_and_other(&srv, &[d0.path(), d1.path()]);

    // 先把"另一块盘"判坏:本盘上的副本**必须照常服务**。
    srv.force_disk_unavailable(&other);
    let key = data_key(7);
    assert_eq!(
        status_of(
            &router,
            put(&format!("/partitions/0/blobs/{key}"), b"unaffected")
        )
        .await,
        StatusCode::CREATED,
        "坏的是**另一块**盘,这块盘上的写不该被拦"
    );
    let resp = response_of(&router, get(&format!("/partitions/0/blobs/{key}"))).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"unaffected");

    let text = metrics_text(&router).await;
    assert!(
        !text.contains(r#"reason="disk_unavailable""#),
        "另一块盘坏掉不该产生任何盘闸门拒绝:\n{text}"
    );
}

/// **不能做出一个无法远程恢复的节点**:盘不可服务时,`/metrics` 与 `/admin/**` 必须照常可达。
#[tokio::test]
async fn metrics_and_admin_stay_reachable_while_a_disk_is_unavailable() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;
    let (home, _other) = home_and_other(&srv, &[d0.path(), d1.path()]);
    srv.force_disk_unavailable(&home);

    assert_eq!(
        status_of(&router, get("/metrics")).await,
        StatusCode::OK,
        "/metrics 被闸门挡掉 = 自瞎双眼"
    );
    for path in ["/admin/replication-control", "/admin/compactions"] {
        assert_eq!(
            status_of(&router, get(path)).await,
            StatusCode::OK,
            "{path} 被闸门挡掉 = 无法远程恢复的节点"
        );
    }
    // 运维仍能对这块盘上的分区下发静默/恢复(admin 数据面之外的入口)。
    assert_eq!(
        status_of(
            &router,
            Request::post("/admin/replication-control")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"paused":true}"#))
                .unwrap()
        )
        .await,
        StatusCode::OK
    );
}

/// 闸门**不吞 404**:没有本地副本时,盘是谁都无从谈起 —— 该报 404 就报 404。
#[tokio::test]
async fn unknown_partition_is_still_404() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;
    let (home, other) = home_and_other(&srv, &[d0.path(), d1.path()]);
    // 两块盘都判坏:即便如此,pid=9 没有本地副本,也应是 404(与盘无关)。
    srv.force_disk_unavailable(&home);
    srv.force_disk_unavailable(&other);
    assert_eq!(
        status_of(
            &router,
            get(&format!("/partitions/9/blobs/{}", data_key(1)))
        )
        .await,
        StatusCode::NOT_FOUND,
        "没有本地副本 ⇒ 404;不能因为'某块盘坏'就报 503"
    );
}

/// 迟滞的**退出**方向:连续好轮 + 稳定期满 ⇒ 自动回归服务(不需要重启、不需要人工介入)。
///
/// 配置把 `recover_stable_secs` 置 0:本用例只验"轮数那一半";稳定期那一半由
/// `disk_health.rs` 的 `recovery_is_stricter_than_entry` 用注入时刻钉住(那里不需要真等 120s)。
#[tokio::test]
async fn recovering_the_disk_restores_service() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let cfg = StoreServerConfig {
        disk_health_recover_stable_secs: 0,
        ..Default::default()
    };
    let srv = two_disk_node(&[d0.path(), d1.path()], cfg);
    let router = srv.router();
    create_replica(&router, 0).await;
    let (home, _other) = home_and_other(&srv, &[d0.path(), d1.path()]);
    let key = data_key(3);
    assert_eq!(
        status_of(
            &router,
            put(&format!("/partitions/0/blobs/{key}"), b"before")
        )
        .await,
        StatusCode::CREATED
    );

    srv.force_disk_unavailable(&home);
    assert_eq!(
        status_of(&router, get(&format!("/partitions/0/blobs/{key}"))).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    // 只喂**不够**的好轮数:仍不可用(迟滞不能一好就复位)。
    let h = srv.config().hysteresis();
    for _ in 0..(h.good_rounds.max(1) - 1) {
        antfs_store::disk_health::observe_disk(
            &srv.disk_health(),
            &srv.metrics(),
            &home,
            antfs_store::health::DiskHealth::Healthy,
            &h,
        );
    }
    assert_eq!(
        status_of(&router, get(&format!("/partitions/0/blobs/{key}"))).await,
        StatusCode::SERVICE_UNAVAILABLE,
        "好轮数不够 ⇒ 不许复位"
    );

    // 补满:复位,服务自动回来,数据还在。
    srv.force_disk_available(&home);
    let resp = response_of(&router, get(&format!("/partitions/0/blobs/{key}"))).await;
    assert_eq!(resp.status(), StatusCode::OK, "复位后应自动回归服务");
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"before", "复位不该动数据");
    let text = metrics_text(&router).await;
    assert!(
        text.contains(&format!(
            r#"store_server_disk_health_transitions_total{{disk_id="{home}",from="UNAVAILABLE",to="HEALTHY"}} 1"#
        )),
        "复位要留下一条迁移记录:\n{text}"
    );
}

/// 本地维护(flush / compaction)必须**跳过不可服务的盘**,而且"跳过"这件事要**看得见**。
///
/// 为什么可见性也算性质:否则表现为"这块盘上的副本悄悄不 flush 了",运维只会看到持久化边界不再
/// 推进,却找不到原因。
#[tokio::test]
async fn background_worker_skips_the_unavailable_disk() {
    let d0 = tempfile::tempdir().unwrap();
    let d1 = tempfile::tempdir().unwrap();
    let srv = two_disk_node(&[d0.path(), d1.path()], StoreServerConfig::default());
    let router = srv.router();
    create_replica(&router, 0).await;
    let (home, _other) = home_and_other(&srv, &[d0.path(), d1.path()]);

    let replicas = srv.replicas();
    let pool = srv.disk_pools();
    let health = srv.disk_health();
    let metrics = srv.metrics();

    // 健康时:这块盘上的副本确实被 flush 到(基线,证明后面的 0 是"跳过"而不是"本来就没活干")。
    let flushed = tokio::time::timeout(
        Duration::from_secs(10),
        antfs_store::worker::flush_all(&pool, &replicas, &health, &metrics),
    )
    .await
    .expect("健康盘上 flush 不该卡住");
    assert_eq!(flushed, 1, "健康时应真的 flush 到那一个副本");

    srv.force_disk_unavailable(&home);
    let flushed = tokio::time::timeout(
        Duration::from_secs(10),
        antfs_store::worker::flush_all(&pool, &replicas, &health, &metrics),
    )
    .await
    .expect("坏盘上 flush 也不该卡住(整块跳过,不是等准入超时)");
    assert_eq!(flushed, 0, "坏盘上的副本应被整块跳过");

    let cfg = srv.config();
    let compacted = tokio::time::timeout(
        Duration::from_secs(10),
        antfs_store::worker::compact_all(&pool, &replicas, &health, &metrics, cfg, true),
    )
    .await
    .expect("坏盘上 compaction 不该卡住");
    assert_eq!(compacted, 0, "坏盘上的副本不该被压");

    let text = metrics_text(&router).await;
    for task in ["flush", "compaction"] {
        assert!(
            text.contains(&format!(
                r#"store_server_disk_worker_skipped_total{{disk_id="{home}",task="{task}"}} 1"#
            )),
            "{task} 的跳过必须可见:\n{text}"
        );
    }

    // 复位后维护自动回归:同一台节点上,盘一回来下一次 flush 就该照常干活。
    srv.force_disk_available(&home);
    let flushed = tokio::time::timeout(
        Duration::from_secs(10),
        antfs_store::worker::flush_all(&pool, &replicas, &health, &metrics),
    )
    .await
    .expect("复位后 flush 不该卡住");
    // 注意:默认配置的 `recover_stable_secs = 120` ⇒ 只喂好轮数**不足以**复位(那是刻意的,
    // 由 `recovering_the_disk_restores_service` 用 `recover_stable_secs = 0` 单独验证)。
    if antfs_store::disk_health::is_available(&health, &home) {
        assert_eq!(flushed, 1, "复位后应恢复维护");
    } else {
        assert_eq!(
            flushed, 0,
            "稳定期未满 ⇒ 仍被跳过(迟滞的退出方向比进入更严)"
        );
    }
}

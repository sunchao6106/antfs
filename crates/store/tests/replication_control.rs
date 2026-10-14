//! A7 集成测试:**运行期暂停本节点主动拉复制**(`/admin/replication-control`)。
//!
//! B 经假 master 发现 peer A,真的后台复制 loop 从 A 拉复制;暂停后 B 不再收敛、恢复后继续。
//! 同时验证复制开关的**作用域**:只停"我拉别人",不停"别人拉我"(`?replication=1` /
//! `replica-metadata` 照常),心跳也不受影响。等待一律有界轮询(CLAUDE.md §6)。

mod common;

use std::time::Duration;

use antfs_blob::StoreKey;
use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{peer_loc, wait_until, FakeMaster};
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tower::ServiceExt;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

/// 数据面 key:分区 0 + 低 32bit = `n`(副本由 `create_with` 建库,key 前缀 0)。
fn key(n: u8) -> StoreKey {
    StoreKey::from_parts(0, n as u32)
}

/// 起一个**独立**节点(不接 master),建 pid=0 副本,真端口上跑 axum;返回 (base_url, server)。
async fn spawn_standalone(dir: &std::path::Path) -> (String, StoreServer) {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(cfg, store_config(), &registry).expect("start");
    srv.replicas().ensure_replica(0).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = srv.router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), srv)
}

/// 经 `POST /admin/replication-control` 开关复制,断言响应体回显状态。
async fn set_paused(router: &axum::Router, paused: bool) -> serde_json::Value {
    let req = Request::builder()
        .method("POST")
        .uri("/admin/replication-control")
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"paused":{paused}}}"#)))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["paused"], paused, "POST 必须回显操作后的状态");
    v
}

async fn get_paused(router: &axum::Router) -> serde_json::Value {
    let resp = router
        .clone()
        .oneshot(
            Request::get("/admin/replication-control")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// 起一个节点:接假 master、开真后台复制 loop(与 [`paused_pull_replication_stops_converging_then_resumes`]
/// 同一装配),但**不预先建副本** —— 留给调用方在需要的时刻建,以便让"副本诞生前就已暂停"这件事确定发生。
async fn spawn_replicating(
    dir: &std::path::Path,
    node_id: u64,
    master_addr: String,
) -> (StoreServer, antfs_metric::Registry) {
    let registry = antfs_metric::Registry::new();
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        master_addr,
        heartbeat_interval_secs: 1,
        replication_enabled: true,
        replication_interval_secs: 1,
        ..Default::default()
    };
    let srv =
        StoreServer::start_with(cfg, store_config(), &registry).expect("start replicating node");
    (srv, registry)
}

/// 该分区副本的实然态(心跳上报的取值源 —— master 看到的就是它)。
/// 断言里用**字符串字面量**是刻意的:这正是 proto/心跳里的线上取值,master 侧按 `"ACTIVE"`/`"BOOTSTRAP"`
/// 字面量匹配(`crates/master/src/reconcile.rs`),常量改名不得悄悄改掉线上语义。
fn state_of(srv: &StoreServer, pid: u32) -> String {
    srv.replicas()
        .replica_states()
        .into_iter()
        .find(|s| s.partition_id == pid)
        .expect("本地必须有该分区的 replica")
        .current_state
}

/// A6 负例依赖的行为:**暂停拉复制时,新副本必须停在 `BOOTSTRAP`**。
///
/// 这正是 master 侧"先补后删"的关键:只要继任者还不是 `ACTIVE`,删源闸门(`plan_move`/
/// `drop_confirmed`)就不成立,不会摘掉源副本、更不会下发 `DROPPED` 删源数据。若新副本一出生
/// 就自称 `ACTIVE`,源是当时唯一有数据的副本时就会丢数据。
///
/// 顺序刻意做到确定性:先暂停(此时本地**还没有**该分区副本,复制 loop 无从拉取),再建副本并写数据,
/// 于是 paused 期间的 `BOOTSTRAP` 一定是"没追平"而非"没赶上暂停"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_pull_keeps_new_replica_bootstrapping() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path()).await;
    // 源:有数据的 pid 0。
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();

    let fake = FakeMaster::start(vec![peer_loc(1, base_a.trim_start_matches("http://"))]).await;
    let (srv_b, registry_b) = spawn_replicating(dir_b.path(), 2, fake.addr()).await;
    srv_b.serve().await.expect("serve b");

    // ---- 先暂停(本地尚无该分区副本 → 复制 loop 无事可做)----
    assert_eq!(set_paused(&srv_b.router(), true).await["paused"], true);

    // ---- 再建副本:起点必须是 BOOTSTRAP,且暂停期间一直如此 ----
    srv_b.replicas().ensure_replica(0).unwrap();
    let store_b = srv_b.replicas().get(0).unwrap().store;
    assert_eq!(state_of(&srv_b, 0), "BOOTSTRAP", "新建副本不得自称 ACTIVE");

    let rounds_paused = rounds(&registry_b);
    let beats_paused = fake.beat_count();
    // 时钟用**心跳 beat**(1s/次,与复制周期同为 1s):等 4 拍 ≫ 3 个复制周期。
    assert!(
        wait_until(Duration::from_secs(20), || fake.beat_count()
            >= beats_paused + 4)
        .await,
        "暂停期间心跳必须照常"
    );
    assert_eq!(
        state_of(&srv_b, 0),
        "BOOTSTRAP",
        "暂停拉复制时副本必须停在 BOOTSTRAP(否则 master 会据此删源)"
    );
    assert!(
        store_b.get(&key(1)).is_err(),
        "暂停期间不得拉到源的数据(负例有证明力)"
    );
    assert_eq!(rounds(&registry_b), rounds_paused, "暂停期间不得再跑复制轮");
    // 心跳上报的实然态(指标口径):一个副本仍未追平。
    assert!(
        wait_until(Duration::from_secs(10), || {
            text_value(
                &metrics_text(&registry_b),
                "store_server_replicas_bootstrapping",
            ) == 1
        })
        .await,
        "暂停期间的「同步中」副本数必须为 1:\n{}",
        metrics_text(&registry_b)
    );

    // ---- 恢复:拉复制收敛 → 真追平 → 转 ACTIVE ----
    set_paused(&srv_b.router(), false).await;
    assert!(
        wait_until(Duration::from_secs(20), || state_of(&srv_b, 0) == "ACTIVE").await,
        "恢复拉复制并追平后必须转 ACTIVE;beats={}",
        fake.beat_count()
    );
    assert_eq!(
        store_b.get(&key(1)).unwrap(),
        b"alpha",
        "转 ACTIVE 时数据必须已经真的在本地"
    );
    assert!(
        wait_until(Duration::from_secs(10), || {
            text_value(
                &metrics_text(&registry_b),
                "store_server_replicas_bootstrapping",
            ) == 0
        })
        .await,
        "追平后「同步中」计数必须归 0"
    );
}

/// 该节点的 Prometheus 文本(用**它自己接入的那个** Registry)。
fn metrics_text(registry: &antfs_metric::Registry) -> String {
    antfs_metric::gather(registry).expect("gather")
}

/// `store_server_replication_rounds_total` 的当前值(读文本解析;测试断言用)。
fn rounds(registry: &antfs_metric::Registry) -> u64 {
    text_value(
        &metrics_text(registry),
        "store_server_replication_rounds_total",
    )
}

fn text_value(text: &str, name: &str) -> u64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_pull_replication_stops_converging_then_resumes() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path()).await;

    // 假 master 只做副本发现:B 经它查到 A(peer),自己起真后台复制 loop。
    let peer = peer_loc(1, base_a.trim_start_matches("http://"));
    let fake = FakeMaster::start(vec![peer]).await;

    let registry_b = antfs_metric::Registry::new();
    let cfg_b = StoreServerConfig {
        node_id: 2,
        data_dirs: vec![dir_b.path().to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        master_addr: fake.addr(),
        heartbeat_interval_secs: 1,
        replication_enabled: true,
        replication_interval_secs: 1,
        ..Default::default()
    };
    let srv_b = StoreServer::start_with(cfg_b, store_config(), &registry_b).expect("start b");
    srv_b.replicas().ensure_replica(0).unwrap();
    let store_b = srv_b.replicas().get(0).unwrap().store;
    srv_b.serve().await.expect("serve b");

    // ---- 先证明复制链路本来是通的 ----
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();
    // 等**整轮跑完**才算通:数据是在 `sync_round` 里 apply 的,`replication_rounds` 由其后的复制
    // loop 累加 —— 只等"数据可见"会与计数差之毫厘(测试竞态,已实测到)。
    assert!(
        wait_until(Duration::from_secs(20), || store_b.get(&key(1)).is_ok()
            && rounds(&registry_b) > 0)
        .await,
        "B 必须先能通过后台复制收敛(否则本用例没有证明力)"
    );

    // ---- 暂停 ----
    set_paused(&srv_b.router(), true).await;
    assert_eq!(get_paused(&srv_b.router()).await["paused"], true);
    // 基线要落在"暂停已生效"之后:等 2 拍心跳(2s),暂停前最后在飞的那一轮早已收尾。
    let beats = fake.beat_count();
    assert!(wait_until(Duration::from_secs(10), || fake.beat_count() >= beats + 2).await);
    let rounds_paused = rounds(&registry_b);
    let beats_paused = fake.beat_count();

    // 暂停期间 A 写新对象:B 不得再收敛。
    store_a.put(key(2), b"beta").unwrap();

    // 时钟用**心跳 beat**(1s/次,与复制周期同为 1s):等 4 拍 ≫ 3 个复制周期。
    assert!(
        wait_until(Duration::from_secs(20), || fake.beat_count()
            >= beats_paused + 4)
        .await,
        "暂停期间心跳必须照常(它不受复制开关影响)"
    );
    assert!(
        store_b.get(&key(2)).is_err(),
        "暂停后本节点不得再拉取(新对象不应出现在本地)"
    );
    assert_eq!(rounds(&registry_b), rounds_paused, "暂停期间不得再跑复制轮");

    // 暂停只停"我拉别人":别人来拉我照常(复制读 + 元数据端点都不受影响)。
    let router_b = srv_b.router();
    let resp = router_b
        .clone()
        .oneshot(
            Request::get(format!(
                "/partitions/0/blobs/{}?replication=1",
                hex::encode(key(1).as_bytes())
            ))
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "暂停期间复制读端点必须照常服务"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), b"alpha");
    let resp = router_b
        .clone()
        .oneshot(
            Request::get("/partitions/0/replica-metadata?max_bytes=1024")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "暂停期间元数据端点必须照常服务"
    );

    // 复制开关与请求静默正交:暂停不影响数据面 put/get。
    let put = Request::put(format!(
        "/partitions/0/blobs/{}",
        hex::encode(key(3).as_bytes())
    ))
    .body(Body::from(b"local write".to_vec()))
    .unwrap();
    assert_eq!(
        router_b.clone().oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );
    let got = store_b.get(&key(3)).unwrap();
    assert_eq!(got.as_slice(), b"local write");

    // 指标:暂停 = 1。
    assert!(
        metrics_text(&registry_b).contains("store_server_replication_paused 1"),
        "暂停必须反映在指标上"
    );

    // ---- 恢复:从原游标续拉,自然收敛(含暂停期间 A 的新对象) ----
    set_paused(&router_b, false).await;
    assert_eq!(get_paused(&router_b).await["paused"], false);
    assert!(
        wait_until(Duration::from_secs(20), || store_b.get(&key(2)).is_ok()
            && rounds(&registry_b) > rounds_paused)
        .await,
        "恢复后必须继续收敛(拉到暂停期间的新对象)且复制轮继续计数"
    );
    assert!(
        metrics_text(&registry_b).contains("store_server_replication_paused 0"),
        "恢复后 gauge 必须归 0"
    );
}

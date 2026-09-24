//! 副本实然态(`BOOTSTRAP → ACTIVE`)集成测试:副本**如实**上报自己还没追平。
//!
//! 为什么这是数据安全问题:master 侧搬迁的删源闸门是"继任者已 `ACTIVE`"(`crates/master/src/
//! reconcile.rs` 的 `plan_move`/`drop_confirmed`)。新建副本若一出生就自称 `ACTIVE`,闸门瞬间成立
//! → 源副本被摘除 + 下发 `DROPPED` 删本地数据;源若是当时**唯一有数据**的副本,就是丢数据
//! (CLAUDE.md §3.1)。故这里逐个盯死要处理对的几个场景:全新空分区、单副本分区、对端有数据、
//! 物理 bootstrap、重启重开。
//!
//! 等待一律有界条件轮询(CLAUDE.md §6),不用 sleep 当同步。

mod common;

use std::time::Duration;

use antfs_blob::{FindToken, StoreKey};
use antfs_store::replica::{STATE_ACTIVE, STATE_BOOTSTRAP};
use antfs_store::replication::{bootstrap_from_peer, sync_round, HttpPeer, SyncOutcome};
use antfs_store::{StoreServer, StoreServerConfig};
use common::{peer_loc, wait_until, FakeMaster};
use tokio::net::TcpListener;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

/// 数据面 key:分区 0 + 低 32bit = `n`(副本由 `create_with` 建库,key 前缀 0)。
fn key(n: u8) -> StoreKey {
    StoreKey::from_parts(0, n as u32)
}

fn config(dir: &std::path::Path, node_id: u64) -> StoreServerConfig {
    StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        // 本地维护 worker 关掉:用例只关心副本实然态,别让后台 flush/compaction 掺进来。
        local_worker_enabled: false,
        ..Default::default()
    }
}

/// 起一个**独立**节点(不接 master)、建出给定分区副本,并在真端口上跑数据面/复制读端点
/// —— 拉复制走真 HTTP(`HttpPeer`),不是进程内特例。返回 (base_url, server)。
async fn spawn_serving(dir: &std::path::Path, pids: &[u32]) -> (String, StoreServer) {
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(config(dir, 1), store_config(), &registry).expect("start");
    for pid in pids {
        srv.replicas().ensure_replica(*pid).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = srv.router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), srv)
}

/// 该分区副本的实然态(心跳上报的取值源)。
fn state_of(srv: &StoreServer, pid: u32) -> String {
    srv.replicas()
        .replica_states()
        .into_iter()
        .find(|s| s.partition_id == pid)
        .expect("本地必须有该分区的 replica")
        .current_state
}

/// 在阻塞线程里跑一轮 `sync_round`(与后台复制器**同一入口**,故追平判定口径一致)。
async fn sync(
    srv: &StoreServer,
    base: String,
    prior: Option<FindToken>,
    bootstrap: bool,
) -> SyncOutcome {
    let replicas = srv.replicas();
    tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(10)).unwrap();
        sync_round(&replicas, 0, &peer, prior, None, u64::MAX, bootstrap)
    })
    .await
    .unwrap()
    .unwrap()
}

/// 单副本分区(RF=1 / 单节点集群):该分区在本节点之外**没有任何其他副本**,没有 peer 可同步 ——
/// 这是"所有 peer 都无数据"的**空集**情形,必须在有限轮内转 `ACTIVE`。
/// 否则单副本分区会永远停在 `BOOTSTRAP`,master 侧一直把它算作"欠副本/同步中"。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_replica_partition_reaches_active_without_peers() {
    let dir_b = tempfile::tempdir().unwrap();
    // 假 master 的 `LookupPartition` 返回**空副本列表**(placement 里只有本节点自己)。
    let fake = FakeMaster::start(Vec::new()).await;
    let registry_b = antfs_metric::Registry::new();
    let mut cfg_b = config(dir_b.path(), 2);
    cfg_b.master_addr = fake.addr();
    cfg_b.heartbeat_interval_secs = 1;
    cfg_b.replication_enabled = true;
    cfg_b.replication_interval_secs = 1;
    let srv_b = StoreServer::start_with(cfg_b, store_config(), &registry_b).expect("start b");
    srv_b.replicas().ensure_replica(0).unwrap();
    assert_eq!(state_of(&srv_b, 0), STATE_BOOTSTRAP, "起点仍是 BOOTSTRAP");
    srv_b.serve().await.expect("serve b");

    assert!(
        wait_until(Duration::from_secs(20), || state_of(&srv_b, 0)
            == STATE_ACTIVE)
        .await,
        "没有其他副本可供同步 = 没有数据要同步 → 必须在有限轮内转 ACTIVE"
    );
}

/// 场景 1(全新空分区):分区刚建时所有节点上都是空的,没有数据需要同步 —— 副本必须在**有限轮内**
/// 转 `ACTIVE`,否则会永远卡在 `BOOTSTRAP`(master 侧看着像"无健康副本",写请求/分配可能被拒)。
///
/// 两条"没有数据"的路各测一个分区:
/// - pid 0:双方都空(peer 有副本但一条没写 → 游标 `Uninitialized`,本地也空 → 一致);
/// - pid 1:peer 上**根本没有该分区的副本**(`replica-metadata` 404)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_partition_replica_reaches_active() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // A 只有 pid 0 的空副本;pid 1 在 A 上没有副本 → 拉复制拿到 404。
    let (base_a, _srv_a) = spawn_serving(dir_a.path(), &[0]).await;

    let fake = FakeMaster::start(vec![peer_loc(1, base_a.trim_start_matches("http://"))]).await;
    let registry_b = antfs_metric::Registry::new();
    let mut cfg_b = config(dir_b.path(), 2);
    cfg_b.master_addr = fake.addr();
    cfg_b.heartbeat_interval_secs = 1;
    cfg_b.replication_enabled = true;
    cfg_b.replication_interval_secs = 1;
    let srv_b = StoreServer::start_with(cfg_b, store_config(), &registry_b).expect("start b");
    srv_b.replicas().ensure_replica(0).unwrap();
    srv_b.replicas().ensure_replica(1).unwrap();

    // 建之初必须如实报 BOOTSTRAP(修的就是"一出生就自称 ACTIVE")。
    assert_eq!(
        state_of(&srv_b, 0),
        STATE_BOOTSTRAP,
        "新建空副本不得自称 ACTIVE"
    );
    assert_eq!(
        state_of(&srv_b, 1),
        STATE_BOOTSTRAP,
        "新建空副本不得自称 ACTIVE"
    );
    srv_b.serve().await.expect("serve b");

    assert!(
        wait_until(Duration::from_secs(20), || {
            state_of(&srv_b, 0) == STATE_ACTIVE && state_of(&srv_b, 1) == STATE_ACTIVE
        })
        .await,
        "空分区(peer 也空 / peer 无副本)必须在有限轮内转 ACTIVE;states=({}, {})",
        state_of(&srv_b, 0),
        state_of(&srv_b, 1)
    );

    // 指标:两个副本都已转正 → "同步中"计数归 0(用**本节点自己接入的** Registry 断言)。
    assert!(
        wait_until(Duration::from_secs(10), || {
            gauge_of(&registry_b, "store_server_replicas_bootstrapping") == 0
        })
        .await,
        "转正后 store_server_replicas_bootstrapping 必须归 0:\n{}",
        antfs_metric::gather(&registry_b).expect("gather")
    );
    assert_eq!(
        gauge_of(&registry_b, "store_server_replicas"),
        2,
        "两个副本都还在:\n{}",
        antfs_metric::gather(&registry_b).expect("gather")
    );
}

/// 现取某指标的值(每次调用重新 gather,供有界轮询用)。
fn gauge_of(registry: &antfs_metric::Registry, name: &str) -> i64 {
    text_value(&antfs_metric::gather(registry).expect("gather"), name)
}

/// 取 Prometheus 文本里某指标的值(gauge 内联标签的形态与本文件用到的指标一致)。
fn text_value(text: &str, name: &str) -> i64 {
    text.lines()
        .find_map(|l| {
            l.strip_prefix(name)?
                .split_whitespace()
                .next()?
                .parse::<i64>()
                .ok()
        })
        .unwrap_or(-1)
}

/// 场景 1/2 的负例依赖(**master 侧 A6 负例**):对端**有数据**而本地还没拉到时,副本必须停在
/// `BOOTSTRAP`;只有真追平(拉复制收敛)那一轮之后才 `ACTIVE`。
#[tokio::test]
async fn replica_with_peer_data_stays_bootstrapping_until_caught_up() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_serving(dir_a.path(), &[0]).await;
    let registry_b = antfs_metric::Registry::new();
    let srv_b = StoreServer::start_with(config(dir_b.path(), 2), store_config(), &registry_b)
        .expect("start b");
    srv_b.replicas().ensure_replica(0).unwrap();
    let store_b = srv_b.replicas().get(0).unwrap().store;

    assert_eq!(
        state_of(&srv_b, 0),
        STATE_BOOTSTRAP,
        "新建副本起点必须是 BOOTSTRAP"
    );

    // 对端写入数据(本地一条都还没有)。
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();
    store_a.put(key(2), b"beta").unwrap();

    // 关键断言:对端已有数据、本地尚未拉取时,**绝不能**已经是 ACTIVE。
    assert_eq!(
        state_of(&srv_b, 0),
        STATE_BOOTSTRAP,
        "对端有数据且本地没拉到时不得报 ACTIVE(否则 master 会据此删源)"
    );
    assert!(
        store_b.get(&key(1)).is_err(),
        "此时本地确实还没有数据(负例有证明力)"
    );

    // 一轮拉复制(与后台复制器同一入口)→ 追平 → 转 ACTIVE。
    let out = sync(&srv_b, base_a.clone(), None, false).await;
    assert_eq!(out.stats.applied, 2, "两条记录都该拉过来");
    assert_eq!(store_b.get(&key(1)).unwrap(), b"alpha");
    assert_eq!(state_of(&srv_b, 0), STATE_ACTIVE, "追平那一轮必须转 ACTIVE");

    // 再拉一轮(幂等):状态不抖回去。
    let again = sync(&srv_b, base_a, Some(out.token), false).await;
    assert_eq!(again.stats.applied, 0, "已追平 → 无新增 apply");
    assert_eq!(
        state_of(&srv_b, 0),
        STATE_ACTIVE,
        "ACTIVE 是单向的,不得回退"
    );
}

/// 场景 2(物理 bootstrap):`bootstrap_from_peer` 只拷封存段,**拷完不算追平** —— 副本仍是
/// `BOOTSTRAP`,要等增量拉把活跃尾补齐那一轮才转 `ACTIVE`。
#[tokio::test]
async fn physical_bootstrap_alone_stays_bootstrapping() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_serving(dir_a.path(), &[0]).await;
    let srv_b = StoreServer::start_with(
        config(dir_b.path(), 2),
        store_config(),
        &antfs_metric::Registry::new(),
    )
    .expect("start b");
    srv_b.replicas().ensure_replica(0).unwrap();

    // A 写大记录滚出多个封存段(每条 ~400KiB)。
    let store_a = srv_a.replicas().get(0).unwrap().store;
    let big = vec![3u8; 400 * 1024];
    for n in 0..6u8 {
        store_a.put(key(n), &big).unwrap();
    }
    store_a.flush().unwrap();
    assert!(store_a.sealed_segments().len() >= 2, "peer 应有多封存段");

    // 只做物理 bootstrap:封存段到位,但不算追平。
    let replicas_b = srv_b.replicas();
    let base = base_a.clone();
    let (_tok, boot) = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(10)).unwrap();
        bootstrap_from_peer(&replicas_b, 0, &peer)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(
        boot.segments >= 2,
        "应物理拷贝了封存段, got {}",
        boot.segments
    );
    assert_eq!(
        state_of(&srv_b, 0),
        STATE_BOOTSTRAP,
        "只拷了封存段(活跃尾还没补)不得报 ACTIVE"
    );

    // 逻辑补齐(活跃尾)→ 追平 → ACTIVE。
    let out = sync(&srv_b, base_a, None, false).await;
    assert_eq!(
        state_of(&srv_b, 0),
        STATE_ACTIVE,
        "bootstrap + 增量拉补齐后必须转 ACTIVE"
    );
    assert!(
        out.stats.applied <= 6,
        "不会重复 apply 已被封存段覆盖的记录"
    );
    let store_b = srv_b.replicas().get(0).unwrap().store;
    for n in 0..6u8 {
        assert_eq!(
            store_b.get(&key(n)).unwrap(),
            big,
            "bootstrap 后 key {n} 可读"
        );
    }
}

/// 场景 3(重启重开):盘上数据可能是旧的 → 重开后**故意**先报 `BOOTSTRAP`(重启期间显示
/// "同步中"是诚实状态),等一轮拉复制确认追平再 `ACTIVE`。
#[tokio::test]
async fn reopened_replica_starts_bootstrapping_then_active() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_serving(dir_a.path(), &[0]).await;
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();
    store_a.put(key(2), b"beta").unwrap();

    // 第一段进程:建副本 → 拉复制追平 → ACTIVE → 落盘后"关闭"(作用域结束时整棵 manager 析构)。
    {
        let srv = StoreServer::start_with(
            config(dir_b.path(), 2),
            store_config(),
            &antfs_metric::Registry::new(),
        )
        .expect("start b1");
        srv.replicas().ensure_replica(0).unwrap();
        let out = sync(&srv, base_a.clone(), None, false).await;
        assert_eq!(out.stats.applied, 2);
        assert_eq!(state_of(&srv, 0), STATE_ACTIVE, "首段:追平后 ACTIVE");
        srv.replicas().get(0).unwrap().store.flush().unwrap();
    }

    // 第二段进程:同目录重开(走 `dir.exists()` → `BlobStore::open_with`)。
    let srv2 = StoreServer::start_with(
        config(dir_b.path(), 2),
        store_config(),
        &antfs_metric::Registry::new(),
    )
    .expect("start b2");
    srv2.replicas().ensure_replica(0).unwrap();
    assert_eq!(
        state_of(&srv2, 0),
        STATE_BOOTSTRAP,
        "重开的副本数据可能是旧的 → 先报 BOOTSTRAP(不得沿用旧进程的 ACTIVE)"
    );
    let store_b2 = srv2.replicas().get(0).unwrap().store;
    assert_eq!(
        store_b2.get(&key(1)).unwrap(),
        b"alpha",
        "重开后数据必须还在"
    );

    // 一轮拉复制确认(对端没有新数据,幂等)→ 追平 → ACTIVE。
    let out = sync(&srv2, base_a, None, false).await;
    assert_eq!(out.stats.applied, 0, "本地已是最新 → 无新增 apply");
    assert_eq!(state_of(&srv2, 0), STATE_ACTIVE, "确认追平后必须转 ACTIVE");
}

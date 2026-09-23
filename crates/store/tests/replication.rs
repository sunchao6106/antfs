//! S3a 拉复制集成测试:两个 store-server 起**真 HTTP 端口**,B 经 `HttpPeer` + `replicate_from_peer`
//! 从 A 拉复制,验证元数据/复制读端点 + adapter + 收敛 + delete 传播。确定性(无 sleep)。

use std::time::Duration;

use antfs_blob::{FindToken, StoreKey};
use antfs_store::replica::ReplicaState;
use antfs_store::replication::{
    replicate_from_peer, sync_from_peer, sync_round, HttpPeer, ReplicateStats,
};
use antfs_store::{DataOp, StoreServer, StoreServerConfig};
use tokio::net::TcpListener;

fn small_store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn key(n: u8) -> StoreKey {
    let mut b = [0u8; 8];
    b[0] = n;
    StoreKey::from_slice(&b).unwrap()
}

/// 起一个 store-server,建 pid=0 的 replica,在真端口上跑 axum;返回 (addr, 该 server)。
async fn spawn(dir: &std::path::Path) -> (String, StoreServer) {
    spawn_with(dir, small_store_config(), 4 << 20).await
}

async fn spawn_with(
    dir: &std::path::Path,
    scfg: antfs_blob::StoreConfig,
    partition_capacity_bytes: u64,
) -> (String, StoreServer) {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(cfg, scfg, &registry).expect("start");
    srv.replicas().ensure_replica(0).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = srv.router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), srv)
}

#[tokio::test]
async fn pull_replication_converges_over_http() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn(dir_a.path()).await;
    let (_base_b, srv_b) = spawn(dir_b.path()).await;

    // 直接向 A 的 store 写入(A 的 metadata/blob HTTP 端点会看到)。
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();
    store_a.put(key(2), b"beta").unwrap();

    // B 经真 HTTP 从 A 拉复制(reqwest blocking 在 spawn_blocking 里跑)。
    let store_b = srv_b.replicas().get(0).unwrap().store;
    let base = base_a.clone();
    let sb = store_b.clone();
    let out = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(5)).unwrap();
        replicate_from_peer(&sb, &peer, FindToken::Uninitialized, u64::MAX)
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(out.stats.applied, 2, "两个 PUT 都被 apply");
    assert_eq!(store_b.get(&key(1)).unwrap(), b"alpha");
    assert_eq!(store_b.get(&key(2)).unwrap(), b"beta");

    // delete 在 A 上,再拉一轮 → B 也删除。
    store_a.delete(key(1)).unwrap();
    let base = base_a.clone();
    let sb = store_b.clone();
    tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(5)).unwrap();
        replicate_from_peer(&sb, &peer, FindToken::Uninitialized, u64::MAX)
    })
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(store_b.get(&key(1)), Err(antfs_blob::Error::Deleted)), "delete 传播");
    assert_eq!(store_b.get(&key(2)).unwrap(), b"beta", "未删的仍在");
}


/// 在阻塞线程里跑一轮 `sync_round`(与后台复制器**同一入口**,故滞后估算与追平判定口径一致)。
async fn pull(srv: &StoreServer, base: String, prior: Option<FindToken>) -> ReplicateStats {
    let replicas = srv.replicas();
    tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(10)).unwrap();
        sync_round(&replicas, 0, &peer, prior, u64::MAX, false)
    })
    .await
    .unwrap()
    .unwrap()
    .stats
}

/// 同 `pull` 但返回续拉游标(测试串联多轮用)。
async fn pull_token(srv: &StoreServer, base: String, prior: Option<FindToken>) -> FindToken {
    let replicas = srv.replicas();
    tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(10)).unwrap();
        sync_round(&replicas, 0, &peer, prior, u64::MAX, false)
    })
    .await
    .unwrap()
    .unwrap()
    .token
}

/// 该分区的心跳实况(心跳 beat 的填充源)。
fn beat_of(srv: &StoreServer, pid: u32) -> ReplicaState {
    srv.replicas()
        .replica_states()
        .into_iter()
        .find(|s| s.partition_id == pid)
        .expect("本地必须有该分区的 replica")
}

/// 任务 1:心跳上报 `used_bytes`(本副本实际占用)与 `lag_bytes`(拉复制滞后估算)。
/// 滞后口径:本轮从 peer 拿到的游标 − 本轮开始前本地已 apply 的游标,按段容量线性化;
/// 追平(无新数据可拉)后归 0。三段数据同大小(8 KiB),使该口径在稳态下等于本轮补上的字节
/// (游标指向记录**起点**,差值不含新游标处那条记录本身,见 [`antfs_store::replication::lag_bytes`])。
#[tokio::test]
async fn heartbeat_reports_used_bytes_and_lag_converges_to_zero() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn(dir_a.path()).await;
    let (_base_b, srv_b) = spawn(dir_b.path()).await;
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), &[1u8; 8192]).unwrap();
    store_a.put(key(2), &[2u8; 8192]).unwrap();

    // 首轮:本地无历史游标(Uninitialized)→ 无法线性化,滞后报 0。
    let tok = pull_token(&srv_b, base_a.clone(), None).await;
    let store_b = srv_b.replicas().get(0).unwrap().store;
    let beat = beat_of(&srv_b, 0);
    assert_eq!(beat.used_bytes, store_b.valid_data_size(), "used_bytes 必须取引擎的实际占用");
    assert!(
        beat.used_bytes >= 2 * 8192,
        "写入字节必须反映在心跳里, got {}",
        beat.used_bytes
    );
    assert_eq!(beat.lag_bytes, 0, "首轮无基线 → 滞后按 0 报");

    // 对端再写 8 KiB → 下一轮开始时落后 8 KiB(按段容量线性化)。
    store_a.put(key(3), &[3u8; 8192]).unwrap();
    let tok = pull_token(&srv_b, base_a.clone(), Some(tok)).await;
    let beat = beat_of(&srv_b, 0);
    assert!(
        beat.lag_bytes >= 8192,
        "滞后估算必须 ≥ 本轮补上的数据字节, got {}",
        beat.lag_bytes
    );
    assert!(
        beat.used_bytes >= 3 * 8192,
        "新数据 apply 后占用必须增长, got {}",
        beat.used_bytes
    );

    // 无新写再拉一轮:已追平 → 滞后归 0。
    let _ = pull_token(&srv_b, base_a, Some(tok)).await;
    assert_eq!(beat_of(&srv_b, 0).lag_bytes, 0, "追平后滞后必须归 0");
}

/// 任务 3 回归防线:源节点静默 `put` 与 `get` 后,拉复制**仍能收敛**
/// (证明 `classify` 的豁免清单生效——运维静默不会让副本停更/掉数据)。
#[tokio::test]
async fn replication_converges_while_source_data_ops_are_silenced() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn(dir_a.path()).await;
    let (_base_b, srv_b) = spawn(dir_b.path()).await;
    let store_a = srv_a.replicas().get(0).unwrap().store;
    store_a.put(key(1), b"alpha").unwrap();

    // 源节点节点级静默 put + get(1 小时)。
    let rc = srv_a.request_control();
    rc.set(DataOp::Put, None, true, Some(3600));
    rc.set(DataOp::Get, None, true, Some(3600));

    // 数据面 put 确实被挡(否则本测试没有证明力)。
    let base = base_a.clone();
    let blocked = tokio::task::spawn_blocking(move || {
        reqwest::blocking::Client::new()
            .put(format!("{base}/partitions/0/blobs/{}", hex::encode(key(4).as_bytes())))
            .body(vec![4u8; 16])
            .send()
            .expect("request")
            .status()
            .as_u16()
    })
    .await
    .unwrap();
    assert_eq!(blocked, 503, "源节点 put 必须被静默");

    // 静默期间源节点仍有写入与删除(直接写引擎,模拟写入不经数据面的情形)。
    store_a.put(key(2), b"beta").unwrap();
    store_a.delete(key(1)).unwrap();

    // 拉复制:元数据(replica-metadata)+ 数据体(`?replication=1`)+ 段清单都不能被挡。
    let tok = pull_token(&srv_b, base_a.clone(), None).await;
    let store_b = srv_b.replicas().get(0).unwrap().store;
    assert_eq!(store_b.get(&key(2)).unwrap(), b"beta", "存活对象必须拉到");
    assert!(
        matches!(store_b.get(&key(1)), Err(antfs_blob::Error::Deleted)),
        "delete 必须传播"
    );

    // 再拉一轮:幂等收敛,静默不产生累积错误。
    let again = pull(&srv_b, base_a, Some(tok)).await;
    assert_eq!(again.applied, 0, "已追平 → 无新增 apply");

    // 源节点心跳仍正常(静默不影响心跳上报)。
    let beat = beat_of(&srv_a, 0);
    assert_eq!(beat.used_bytes, store_a.valid_data_size());
}

/// S3b 混合 bootstrap:peer 造多封存段 + 活跃段新写 → 空副本物理拷贝封存段 + 逻辑补活跃尾。
#[tokio::test]
async fn hybrid_bootstrap_physical_then_logical() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // 小段容量,写大记录以滚动出封存段。
    let scfg = antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    };
    let a = spawn_with(dir_a.path(), scfg.clone(), 8 << 20).await;
    let b = spawn_with(dir_b.path(), scfg, 8 << 20).await;

    let store_a = a.1.replicas().get(0).unwrap().store;
    // 每条 ~400KiB;2 条即滚动一段。写 6 条 → 至少 2 个封存段 + 活跃段。
    let big = vec![3u8; 400 * 1024];
    for n in 0..6u8 {
        store_a.put(key(n), &big).unwrap();
    }
    store_a.flush().unwrap();
    assert!(store_a.sealed_segments().len() >= 2, "peer 应有多封存段");

    // 空副本 B:混合 bootstrap(物理拷贝封存段 + 逻辑补活跃尾)。
    let replicas_b = b.1.replicas();
    let base_a = a.0.clone();
    let out = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base_a, 0, Duration::from_secs(10)).unwrap();
        sync_from_peer(&replicas_b, 0, &peer, None, u64::MAX, true)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(out.bootstrap.segments >= 2, "应物理拷贝了封存段, got {}", out.bootstrap.segments);
    let store_b = b.1.replicas().get(0).unwrap().store;
    for n in 0..6u8 {
        assert_eq!(store_b.get(&key(n)).unwrap(), big, "bootstrap 后 key {n} 可读");
    }
}

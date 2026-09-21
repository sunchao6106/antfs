//! S3a 拉复制集成测试:两个 store-server 起**真 HTTP 端口**,B 经 `HttpPeer` + `replicate_from_peer`
//! 从 A 拉复制,验证元数据/复制读端点 + adapter + 收敛 + delete 传播。确定性(无 sleep)。

use std::time::Duration;

use blob::{FindToken, StoreKey};
use store::replication::{replicate_from_peer, sync_from_peer, HttpPeer};
use store::{StoreServer, StoreServerConfig};
use tokio::net::TcpListener;

fn small_store_config() -> blob::StoreConfig {
    blob::StoreConfig {
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
    scfg: blob::StoreConfig,
    partition_capacity_bytes: u64,
) -> (String, StoreServer) {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes,
        ..Default::default()
    };
    let registry = metric::Registry::new();
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
    let (_tok, stats) = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, 0, Duration::from_secs(5)).unwrap();
        replicate_from_peer(&sb, &peer, FindToken::Uninitialized, u64::MAX)
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(stats.applied, 2, "两个 PUT 都被 apply");
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
    assert!(matches!(store_b.get(&key(1)), Err(blob::Error::Deleted)), "delete 传播");
    assert_eq!(store_b.get(&key(2)).unwrap(), b"beta", "未删的仍在");
}

/// S3b 混合 bootstrap:peer 造多封存段 + 活跃段新写 → 空副本物理拷贝封存段 + 逻辑补活跃尾。
#[tokio::test]
async fn hybrid_bootstrap_physical_then_logical() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // 小段容量,写大记录以滚动出封存段。
    let scfg = blob::StoreConfig {
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
    let (_tok, _stats, boot) = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base_a, 0, Duration::from_secs(10)).unwrap();
        sync_from_peer(&replicas_b, 0, &peer, None, u64::MAX, true)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(boot.segments >= 2, "应物理拷贝了封存段, got {}", boot.segments);
    let store_b = b.1.replicas().get(0).unwrap().store;
    for n in 0..6u8 {
        assert_eq!(store_b.get(&key(n)).unwrap(), big, "bootstrap 后 key {n} 可读");
    }
}

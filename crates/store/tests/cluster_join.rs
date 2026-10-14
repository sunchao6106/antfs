//! S2 集成测试:store-server 接入 master —— 注册 → master 放置指令 → 心跳建 replica → HTTP put/get。
//!
//! 用进程内 `oneshot` 打数据面(命中同一 `ReplicaManager` Arc,即 directive 建出的 replica)。
//! 同步用带超时轮询(CLAUDE.md §6)。

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{CreatePartitionsRequest, ListPartitionsRequest};
use antfs_master::MasterNode;
use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use openraft::BasicNode;
use tower::ServiceExt;

const SECRET: &str = "s2-secret";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn small_store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cond()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn store_server_joins_master_and_serves_directed_replica() {
    let tmp = tempfile::tempdir().unwrap();

    // ---- 单节点 master ----
    let m_raft = free_port();
    let m_grpc = free_port();
    let mcfg = MasterConfig {
        node_id: 1,
        raft_addr: format!("127.0.0.1:{m_raft}"),
        grpc_addr: format!("127.0.0.1:{m_grpc}"),
        data_dir: tmp.path().join("master").to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 60,
        partition_capacity_bytes: 4 << 20,
        disk_near_full_ratio: 0.95,
        auto_grow_enabled: false,
        auto_grow_min_writable: 4,
        auto_grow_max_partitions: 100_000,
        auto_grow_interval_secs: 30,
        auto_grow_batch: 4,
        reconcile_interval_secs: 60, // 慢,避免干扰
        reconcile_startup_grace_secs: 0,
        id_alloc_segment_size: 1024,
        id_alloc_raft_batch_multiple: 2,
        balance_max_in_flight: 1,
        balance_max_moves: 100,
        balance_skew_threshold: 1.0,
        balance_move_timeout_secs: 300,
        // 其余配置项走默认值:新增配置项不该让每个用到 master 的测试文件都跟着改一遍。
        ..Default::default()
    };
    let mreg = antfs_metric::Registry::new();
    let master = MasterNode::start(mcfg.clone(), &mreg)
        .await
        .expect("master start");
    master.serve().expect("master serve");
    master
        .initialize(BTreeMap::from([(
            1u64,
            BasicNode::new(mcfg.raft_addr.clone()),
        )]))
        .await
        .expect("initialize");
    master
        .raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "leader")
        .await
        .expect("leader");

    // ---- store-server 接入 ----
    let ss_dir = tmp.path().join("ss");
    std::fs::create_dir_all(&ss_dir).unwrap();
    let sscfg = StoreServerConfig {
        node_id: 101,
        data_dirs: vec![ss_dir.to_string_lossy().into_owned().into()],
        http_addr: format!("127.0.0.1:{}", free_port()),
        partition_capacity_bytes: 4 << 20,
        master_addr: mcfg.grpc_addr.clone(),
        cluster_secret: SECRET.to_string(),
        heartbeat_interval_secs: 1,
        disk_health_enabled: false,
        disk_health_interval_secs: 60,
        disk_health_timeout_secs: 10,
        ..Default::default()
    };
    let ssreg = antfs_metric::Registry::new();
    let ss = StoreServer::start_with(sscfg, small_store_config(), &ssreg).expect("ss start");
    ss.serve().await.expect("ss serve"); // 注册 + 心跳后台任务

    // 注册后 master 应见到节点 101。
    let sm = master.sm_data.clone();
    assert!(
        wait_until(Duration::from_secs(15), || sm
            .read()
            .unwrap()
            .state
            .nodes
            .contains_key(&101))
        .await,
        "master must see registered store-server node 101"
    );

    // master 建 1 个 RF=1 分区(放到 101;需心跳已上报该盘余量)。重试到选盘成功。
    let mut mc = MasterClient::connect(format!("http://{}", mcfg.grpc_addr))
        .await
        .expect("mc");
    let mut pid = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        let resp = mc
            .create_partitions(CreatePartitionsRequest {
                version: 1,
                cluster_secret: SECRET.into(),
                count: 1,
                replication_factor: 1,
            })
            .await
            .expect("create_partitions")
            .into_inner();
        if resp.ok && !resp.partition_ids.is_empty() {
            pid = Some(resp.partition_ids[0]);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let pid = pid.expect("master must place a partition on the store-server");

    // store-server 应经心跳指令建出该 replica。
    let replicas = ss.replicas();
    assert!(
        wait_until(Duration::from_secs(15), || replicas.get(pid).is_some()).await,
        "store-server must create the directed replica {pid}"
    );

    // 经数据面 HTTP put/get(oneshot 命中同一 ReplicaManager)。
    let router = ss.router();
    // key:16 hex = 高 32bit 分区 0 + 低 32bit objectId(副本以默认前缀 0 建库)。
    let key = "000000000000000f";
    let data = b"s2 end-to-end blob".to_vec();
    let put = Request::put(format!("/partitions/{pid}/blobs/{key}"))
        .body(Body::from(data.clone()))
        .unwrap();
    assert_eq!(
        router.clone().oneshot(put).await.unwrap().status(),
        StatusCode::CREATED
    );
    let get = Request::get(format!("/partitions/{pid}/blobs/{key}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(get).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        body.as_ref(),
        data.as_slice(),
        "GET on directed replica returns written bytes"
    );

    // ListPartitions(admin dashboard 用)应列出该分区,含副本 placement 与(心跳上报的)实然态。
    // `state: ""` = 不过滤(proto 新增的按状态过滤字段)。
    let listed = mc
        .list_partitions(ListPartitionsRequest {
            version: 1,
            offset: 0,
            limit: 0,
            state: String::new(),
        })
        .await
        .expect("list_partitions")
        .into_inner();
    let entry = listed
        .partitions
        .iter()
        .find(|p| p.partition_id == pid)
        .expect("listed partitions must include the created partition");
    assert_eq!(entry.replication_factor, 1);
    assert_eq!(entry.replicas.len(), 1, "RF=1 one replica placement");
    assert_eq!(entry.replicas[0].node_id, 101);
}

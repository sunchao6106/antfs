//! client(router SDK)端到端集成测试:**真 master + 真 store(真 gRPC + 真 HTTP 端口)**。
//!
//! 覆盖判据(对齐 `docs/design/client.md` 与 `cluster.md §10`):
//!   1. 发号 → 写 → 读往返一致(`put`/`get`/`put_file`/`get_file`);
//!   2. **W = 实际副本数 / 2 + 1**:RF=3 掉一个副本仍能写成功(acks=2);RF=1 只要求 1 个 ack
//!      (证明门槛是"实算多数派",不是写死 2);
//!   3. **R = 1**:第一个副本不可达时自动换副本读成功;
//!   4. 号段:一次申请多个 id,互不重复、都落在 master 发的 partition 上,**且在可写分区之间轮转**;
//!   5. 元数据/删除/复活;不存在的对象报 `NotFound`(而不是笼统的传输错误);
//!   6. 后台刷新任务:可写分区增减(转 RO / 新建)后本地号段池跟着调账。
//!
//! 测试夹具刻意**不依赖心跳周期**:节点注册与分区建立直接经 master 的 Raft 提交,副本在 store 上
//! 经 admin 端点同步创建 —— 全部同步等待,不用 `sleep`(CLAUDE.md §5.6)。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_master::types::{
    DiskSpec, MasterCommand, MasterResponse, NodeInfo, PartitionState, ReplicaPlacement,
};
use antfs_master::{MasterConfig, MasterNode};
use antfs_router::{Error, Router, RouterConfig, StoreKey};
use antfs_store::{StoreServer, StoreServerConfig};
use openraft::BasicNode;

const SECRET: &str = "test-secret";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

fn addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// 起单节点 master(自举 → 等自己当选 leader),返回节点与 gRPC 地址。
async fn solo_master(dir: &std::path::Path) -> (MasterNode, String) {
    let raft_addr = addr(free_port());
    let grpc_addr = addr(free_port());
    let cfg = MasterConfig {
        node_id: 1,
        raft_addr: raft_addr.clone(),
        grpc_addr: grpc_addr.clone(),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        partition_capacity_bytes: 64 << 20,
        id_alloc_segment_size: 4, // 小号段:逼出多次取段,顺带验证号段边界
        id_alloc_raft_batch_multiple: 2,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg, &registry)
        .await
        .expect("start master");
    node.serve().expect("serve master");
    node.initialize(BTreeMap::from([(1, BasicNode::new(raft_addr))]))
        .await
        .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "leader elected")
        .await
        .expect("leader elected");
    (node, grpc_addr)
}

/// 起一个真数据节点(不接 master:注册走测试直连 Raft,避免心跳噪声/等待)。
async fn start_store(dir: &std::path::Path, node_id: u64) -> (StoreServer, String) {
    let http_addr = addr(free_port());
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: http_addr.clone(),
        master_addr: String::new(),
        partition_capacity_bytes: 64 << 20,
        max_body_bytes: 8 << 20,
        local_worker_enabled: false,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(
        cfg,
        antfs_blob::StoreConfig {
            segment_size_bytes: 8 << 20,
            index_max_inmem_elements: 100_000,
            io_error_count_to_trigger_shutdown: 0,
        },
        &registry,
    )
    .expect("start store");
    srv.serve().await.expect("serve store");
    (srv, http_addr)
}

/// 经 Raft 固化数据节点身份(测试自己知道地址)。
async fn register_node(node: &MasterNode, node_id: u64, http_addr: &str) {
    let info = NodeInfo {
        dc: "dc1".into(),
        rack: "r1".into(),
        addr: http_addr.to_string(),
        disks: BTreeMap::from([(
            "d0".to_string(),
            DiskSpec {
                capacity_bytes: 1 << 40,
            },
        )]),
        registered_at_ms: 1,
    };
    node.raft
        .client_write(MasterCommand::RegisterNode { node_id, info })
        .await
        .expect("register node");
}

fn placement(node_id: u64) -> ReplicaPlacement {
    ReplicaPlacement {
        node_id,
        disk_id: "d0".to_string(),
        target_state: "ACTIVE".to_string(),
        // 占位:真身份由 master 的 apply 发放(见 ReplicaPlacement::incarnation)。
        incarnation: 0,
    }
}

/// 经 Raft 建分区(测试精确控制副本集合),返回 partition id。
async fn create_partition(node: &MasterNode, replicas: Vec<ReplicaPlacement>) -> u32 {
    let rf = replicas.len() as u8;
    let resp = node
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: rf,
            replicas,
        })
        .await
        .expect("create partition");
    match resp.data {
        MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected apply response: {other:?}"),
    }
}

/// 在 store 上同步建出该分区的副本(数据面 admin 端点,已存在)。
async fn ensure_replica(store_addr: &str, pid: u32) {
    let resp = reqwest::Client::new()
        .post(format!("http://{store_addr}/admin/partitions/{pid}"))
        .send()
        .await
        .expect("admin create replica");
    assert_eq!(
        resp.status().as_u16(),
        201,
        "create replica on {store_addr}"
    );
}

fn client_cfg(master_addr: &str) -> RouterConfig {
    RouterConfig {
        master_addr: master_addr.to_string(),
        cluster_secret: SECRET.to_string(),
        request_timeout_secs: 10,
        id_segment_size: 4,
        // 1s:让后台刷新在测试里可观测(生产默认 30s)。
        partition_refresh_secs: 1,
    }
}

fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_roundtrip_and_w_is_actual_majority() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let dir_b = tmp.path().join("store-b");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    let (_sb, addr_b) = start_store(&dir_b, 102).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;

    // RF=2:多数派 = 2/2+1 = 2 → 两个副本都必须 ack。
    let pid = create_partition(&node, vec![placement(101), placement(102)]).await;
    ensure_replica(&addr_a, pid).await;
    ensure_replica(&addr_b, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect client");

    for (i, len) in [1usize, 4096, 100 * 1024].into_iter().enumerate() {
        let key = client.allocate_one().await.expect("allocate");
        assert_eq!(key.partition_id(), pid, "master 必须发我可写分区的号");
        let data = bytes::Bytes::from(payload(len, i as u8));
        let report = client.put(key, data.clone()).await.expect("put");
        assert_eq!(report.total, 2);
        assert_eq!(report.required, 2, "RF=2 → 多数派 2");
        assert_eq!(report.acks, 2, "两个真副本都应 ack");
        assert_eq!(
            client.get(&key).await.expect("get"),
            data,
            "往返必须逐字节一致"
        );
    }

    // 文件往返(流式):写本地文件 → 传 → 下载到另一个文件 → 比对。
    let src = tmp.path().join("src.bin");
    let dst = tmp.path().join("dst.bin");
    let bytes = payload(300 * 1024, 7);
    tokio::fs::write(&src, &bytes).await.unwrap();
    let (key, report) = client.put_file(&src).await.expect("put_file");
    assert_eq!(report.required, 2);
    let n = client.get_file(&key, &dst).await.expect("get_file");
    assert_eq!(n as usize, bytes.len());
    assert_eq!(
        tokio::fs::read(&dst).await.unwrap(),
        bytes,
        "文件往返必须一致"
    );

    // 元数据 / 删除(删除不可逆:删了就取不回来了)。
    let stat = client.stat(&key).await.expect("stat");
    assert_eq!(stat.size, bytes.len() as u64);
    assert!(!stat.deleted);
    client.delete(&key).await.expect("delete");
    assert!(matches!(
        client.get(&key).await,
        Err(Error::NotFound { .. })
    ));
    let after = client.stat(&key).await.expect("stat after delete");
    assert!(after.deleted, "墓碑的元数据仍可读(deleted=true)");
    assert!(!after.expired);

    // 重复删幂等:墓碑 key 再删仍成功(204),且不改变状态。
    client.delete(&key).await.expect("idempotent re-delete");
    let still = client.stat(&key).await.expect("stat after re-delete");
    assert!(still.deleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn write_survives_one_dead_replica_and_read_fails_over() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let dir_b = tmp.path().join("store-b");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    let (_sb, addr_b) = start_store(&dir_b, 102).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;

    // 第三个副本指向**没有监听**的端口(模拟宕机节点)。
    let dead_addr = addr(free_port());
    register_node(&node, 103, &dead_addr).await;

    // RF=3 → 多数派 2:2 真 + 1 死 恰好够写。
    let pid = create_partition(&node, vec![placement(101), placement(102), placement(103)]).await;
    ensure_replica(&addr_a, pid).await;
    ensure_replica(&addr_b, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");
    let key = client.allocate_one().await.expect("allocate");
    let data = bytes::Bytes::from(payload(64 * 1024, 3));
    let report = client.put(key, data.clone()).await.expect("W=2 必须写成功");
    assert_eq!(report.total, 3);
    assert_eq!(report.required, 2, "RF=3 → 多数派 2");
    assert_eq!(report.acks, 2, "恰好 2 个活副本 ack");

    // 读:R=1 —— 副本序里那个死节点失败后自动换到活副本。
    assert_eq!(client.get(&key).await.expect("R=1 必须换副本读成功"), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_replica_partition_requires_one_ack() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;

    // RF=1 → 多数派 = 1/2+1 = 1。若把 W 写死成 2,这里必然写不进去。
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");
    let key = client.allocate_one().await.expect("allocate");
    let data = bytes::Bytes::from(payload(2048, 5));
    let report = client
        .put(key, data.clone())
        .await
        .expect("RF=1 必须写得进");
    assert_eq!((report.total, report.required, report.acks), (1, 1, 1));
    assert_eq!(client.get(&key).await.expect("get"), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allocated_ids_are_unique_and_partitioned() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;

    // 两个可写分区:router 补段时给**每个**分区各申请一段,取号在两者之间轮转。
    let p0 = create_partition(&node, vec![placement(101)]).await;
    let p1 = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, p0).await;
    ensure_replica(&addr_a, p1).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");

    // 号段 4:申请 10 个 → 每个分区各发完两轮,中途再补一次段。
    let ids = client.allocate_batch(10).await.expect("allocate_batch");
    assert_eq!(ids.len(), 10);
    // 轮转发号:相邻写落在不同分区(进而是不同盘),而不是把一段号连续耗尽在一个分区上。
    let pids: Vec<u32> = ids.iter().map(|k| k.partition_id()).collect();
    assert_eq!(
        pids,
        vec![p0, p1, p0, p1, p0, p1, p0, p1, p0, p1],
        "取号必须在可写分区之间轮转"
    );
    let mut seen = std::collections::HashSet::new();
    for id in &ids {
        assert!(seen.insert(id.as_u64()), "blobId 不得重复: {id:?}");
        let pid = id.partition_id();
        assert!(
            pid == p0 || pid == p1,
            "号必须来自已建的可写分区,实际 {pid}"
        );
    }
    // 每个号都能真写进去(证明号段与副本位置自洽)。
    for (i, id) in ids.iter().enumerate() {
        client
            .put(*id, bytes::Bytes::from(payload(128, i as u8)))
            .await
            .unwrap_or_else(|e| panic!("put {id:?}: {e}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_blob_is_not_found_and_unknown_partition_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");

    // 没写过的号(在本分区内)→ 所有副本都 404 → 必须是 NotFound,不能报成传输故障。
    let key = StoreKey::from_parts(pid, 9999);
    assert!(matches!(
        client.get(&key).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        client.stat(&key).await,
        Err(Error::NotFound { .. })
    ));

    // 不存在的分区 → master 查不到副本 → 明确拒绝(不是静默失败)。
    let ghost = StoreKey::from_parts(9_999, 1);
    let err = client.get(&ghost).await.expect_err("不存在的分区必须报错");
    assert!(
        matches!(err, Error::Rejected(_) | Error::NotFound { .. }),
        "unexpected error: {err:?}"
    );
}

/// 并发发号:多个任务同时 `allocate_one`(号段只有 4 个 → 会频繁跨段向 master 要号),
/// 800 个号必须**互不重复**,且都落在可写分区上、都能真写进去。
///
/// 这条盯的是号段池的并发正确性:池耗尽时锁内单飞取段,任何一个环节漏掉都会给出重号(而重号 = 数据覆盖)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_allocations_are_unique_and_writable() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let router = std::sync::Arc::new(
        Router::connect(client_cfg(&grpc), &registry)
            .await
            .expect("connect"),
    );

    const TASKS: usize = 8;
    const PER_TASK: usize = 100;
    let mut tasks = Vec::new();
    for _ in 0..TASKS {
        let router = router.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::with_capacity(PER_TASK);
            for _ in 0..PER_TASK {
                got.push(router.allocate_one().await.map_err(|e| e.to_string())?);
            }
            Ok::<_, String>(got)
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.expect("task join").expect("allocate"));
    }
    assert_eq!(all.len(), TASKS * PER_TASK);

    let unique: std::collections::HashSet<u64> = all.iter().map(|k| k.as_u64()).collect();
    assert_eq!(unique.len(), all.len(), "并发发号给出重号(会覆盖数据)");
    for k in &all {
        assert_eq!(k.partition_id(), pid, "号必须来自可写分区");
    }

    // 抽查 20 个号真写进去(证明号段与副本位置自洽)。
    let mut written = Vec::new();
    for (i, k) in all.iter().enumerate().step_by(all.len() / 20) {
        let data = bytes::Bytes::from(payload(64, i as u8));
        router
            .put(*k, data.clone())
            .await
            .unwrap_or_else(|e| panic!("put {k:?}: {e}"));
        written.push((*k, data));
    }
    for (k, data) in written {
        assert_eq!(router.get(&k).await.expect("get"), data);
    }
}

/// 后台刷新任务(`partition_refresh_secs`):周期拉可写分区列表并调账本地号段 ——
/// **分区转 RO 后其号段被丢掉**(不再往里写)、**新出现的可写分区被补上段**(开始参与轮转)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_refresh_tracks_writable_partitions() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;

    let p0 = create_partition(&node, vec![placement(101)]).await;
    let p1 = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, p0).await;
    ensure_replica(&addr_a, p1).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");
    let held = || client.metrics().writable_partitions.get() as u64;

    // 后台任务启动即拉一次列表 → 两个可写分区各持一段。
    wait_for("两个可写分区各持一段", || held() == 2).await;

    // p0 转 RO:刷新后它的号段必须被丢掉,后续取号不再落在它上面。
    set_state(&node, p0, PartitionState::ReadOnly).await;
    wait_for("RO 分区的号段被丢掉", || held() == 1).await;
    for key in client.allocate_batch(4).await.expect("allocate_batch") {
        assert_eq!(key.partition_id(), p1, "转 RO 的分区不得再被发号");
    }

    // 新分区出现:刷新后应被补上段并参与轮转。
    let p2 = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, p2).await;
    wait_for("新可写分区被补段", || held() == 2).await;
    let ids = client.allocate_batch(8).await.expect("allocate_batch");
    assert!(
        ids.iter().any(|k| k.partition_id() == p2),
        "新出现的可写分区应参与发号"
    );
    assert!(
        ids.iter().all(|k| k.partition_id() != p0),
        "转 RO 的分区不得再被发号"
    );
}

/// 带超时的条件轮询(等后台任务,不靠固定 `sleep` 赌时长;CLAUDE.md §5.6)。
async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "等待超时:{what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// 改分区可写态(直接走 Raft,与 master `SetPartitionState` gRPC 同一条命令)。
async fn set_state(node: &MasterNode, pid: u32, state: PartitionState) {
    node.raft
        .client_write(MasterCommand::SetPartitionState {
            partition_id: pid,
            state,
        })
        .await
        .expect("set partition state");
}

//! client(router SDK)端到端集成测试:**真 master + 真 store(真 gRPC + 真 HTTP 端口)**。
//!
//! 覆盖判据(对齐 `docs/design/client.md` 与 `cluster.md §10`):
//!   1. 发号 → 写 → 读往返一致(`put`/`get`/`get_object_stream`;`put_file` 家族已移除);
//!   2. **W = 实际副本数 / 2 + 1**:RF=3 掉一个副本仍能写成功(acks=2);RF=1 只要求 1 个 ack
//!      (证明门槛是"实算多数派",不是写死 2);
//!   3. **R = 1**:第一个副本不可达时自动换副本读成功;
//!   4. 号段:一次申请多个 id,互不重复、都落在 master 发的 partition 上,**且在可写分区之间轮转**;
//!   5. 元数据/删除/复活;不存在的对象报 `NotFound`(而不是笼统的传输错误);
//!   6. 后台刷新任务:可写分区增减(转 RO / 新建)后本地号段池跟着调账;
//!   7. **写权重**:master 下发的 `write_weight`(剩余容量占比)决定发号怎么在分区之间分 —— 越空的分区
//!      拿到的号越多,占用对调后分布跟着翻转(软状态直注,不依赖心跳周期)。
//!
//! 测试夹具刻意**不依赖心跳周期**:节点注册与分区建立直接经 master 的 Raft 提交,副本在 store 上
//! 经 admin 端点同步创建 —— 全部同步等待,不用 `sleep`(CLAUDE.md §5.6)。

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::ListPartitionsRequest;
use antfs_master::soft::{DiskUsage, ReplicaState};
use antfs_master::types::{
    DiskSpec, MasterCommand, MasterResponse, NodeInfo, PartitionState, ReplicaPlacement,
};
use antfs_master::{MasterConfig, MasterNode};
use antfs_router::{Error, Router, RouterConfig, StoreKey, NO_EXPIRATION};
use antfs_store::{StoreServer, StoreServerConfig};
use openraft::BasicNode;
use tonic::transport::Channel;

const SECRET: &str = "test-secret";

/// 起单节点 master(自举 → 等自己当选 leader),返回节点与 gRPC 地址。
async fn solo_master(dir: &std::path::Path) -> (MasterNode, String) {
    let cfg = MasterConfig {
        node_id: 1,
        raft_addr: "127.0.0.1:0".to_string(),
        grpc_addr: "127.0.0.1:0".to_string(),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        partition_capacity_bytes: 64 << 20,
        // 与 store 段一致(段 ≥ 容量会被守卫拒启动;503 偶发根因,见 DEVLOG 步骤 48)
        segment_capacity_bytes: 8 << 20,
        id_alloc_segment_size: 4, // 小号段:逼出多次取段,顺带验证号段边界
        id_alloc_raft_batch_multiple: 2,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg, &registry)
        .await
        .expect("start master");
    node.serve().expect("serve master");
    let raft_addr = node.bound_raft_addr().expect("bound raft");
    node.initialize(BTreeMap::from([(1, BasicNode::new(raft_addr))]))
        .await
        .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "leader elected")
        .await
        .expect("leader elected");
    let grpc_addr = node.bound_grpc_addr().expect("bound grpc");
    (node, grpc_addr)
}

/// 起一个真数据节点(不接 master:注册走测试直连 Raft,避免心跳噪声/等待)。
async fn start_store(dir: &std::path::Path, node_id: u64) -> (StoreServer, String) {
    start_store_with(dir, node_id, 64 << 20, 8 << 20).await
}

/// 起一个真数据节点,可指定分区容量与段大小(写满 → 507 的测试用)。
async fn start_store_with(
    dir: &std::path::Path,
    node_id: u64,
    partition_capacity_bytes: u64,
    segment_size_bytes: u64,
) -> (StoreServer, String) {
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".to_string(),
        master_addr: String::new(),
        partition_capacity_bytes,
        max_body_bytes: 8 << 20,
        local_worker_enabled: false,
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(
        cfg,
        antfs_blob::StoreConfig {
            segment_size_bytes,
            index_max_inmem_elements: 100_000,
            io_error_count_to_trigger_shutdown: 0,
            defer_seal_flush: true,
        },
        &registry,
    )
    .expect("start store");
    srv.serve().await.expect("serve store");
    let http_addr = srv.bound_http_addr().expect("bound http");
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
            partition_capacity: 0,
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
        // 其余走默认值:新增配置项不该让每个构造 `RouterConfig` 的地方都跟着改一遍。
        ..Default::default()
    }
}

fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
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

    // 第三个副本指向**没有监听**的地址(模拟宕机节点;节点 id 占端口,零 probe)。
    let dead_addr = "127.0.0.1:103".to_string();
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
async fn duplicate_keyed_put_surfaces_conflict() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let dir_b = tmp.path().join("store-b");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    let (_sb, addr_b) = start_store(&dir_b, 102).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;

    // RF=2 → W=2:首写在两副本都成功;重发同 key ⇒ 两副本一律 409、无 ok ⇒ 多数派
    // 未达且失败全为冲突 —— 断言 SDK 把它透传成 AlreadyExists 而非折叠成 Quorum(503 面)。
    let pid = create_partition(&node, vec![placement(101), placement(102)]).await;
    ensure_replica(&addr_a, pid).await;
    ensure_replica(&addr_b, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");
    let key = client.allocate_one().await.expect("allocate");
    let first = client
        .put(key, bytes::Bytes::from_static(b"first"))
        .await
        .expect("首写必须双副本成功");
    assert_eq!((first.total, first.required, first.acks), (2, 2, 2));

    let err = client
        .put(key, bytes::Bytes::from_static(b"second"))
        .await
        .expect_err("重发同 key 必须失败");
    match &err {
        antfs_router::Error::AlreadyExists { .. } => {}
        other => panic!("期望 AlreadyExists(409 透传),得到 {other:?}"),
    }
    // 读回仍是首写内容(key 已被占,冲突不覆盖)。
    assert_eq!(
        client.get(&key).await.expect("get"),
        bytes::Bytes::from_static(b"first")
    );
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

/// **写权重生效**:master 下发的 `write_weight` 真的决定发号怎么在分区之间分 —— 越空的分区拿到的号越多。
///
/// 软状态**直注**(与 master 侧加权用例同款注入),故不依赖心跳周期、不用 `sleep`:分布的改变由
/// 「池子耗尽 → 按新权重补段」确定性地体现。号段 `id_segment_size = 64` × 2 分区 = 池 128,
/// 权重 1000:500 → 份额 85:43(余数按小数部分给 p1),两轮 256 个号即 170:86。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_write_weight_steers_allocation() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let dir_a = tmp.path().join("store-a");
    let (_sa, addr_a) = start_store(&dir_a, 101).await;
    register_node(&node, 101, &addr_a).await;

    let p0 = create_partition(&node, vec![placement(101)]).await;
    let p1 = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, p0).await;
    ensure_replica(&addr_a, p1).await;

    // p1 的副本报"用掉一半容量" → 权重 500;p0 没有上报 → 视为全空 → 1000。
    let cap = node.config.partition_capacity_bytes;
    report_used(&node, &[(p0, 0), (p1, cap / 2)]);

    // ① master 侧:权重真的**下发到 `ListPartitions`**(不是只在自动挑分区那一条路径里用)。
    let weights = list_write_weights(&grpc).await;
    assert_eq!(
        weights.get(&p1).copied(),
        Some(500),
        "半满 → 权重 500;实际 {weights:?}"
    );
    assert_eq!(
        weights.get(&p0).copied(),
        Some(1000),
        "全空 → 权重满值;实际 {weights:?}"
    );

    // ② client 侧:发号分布跟着权重走(p0 : p1 ≈ 2 : 1,而不是均权的 1 : 1)。
    let registry = antfs_metric::Registry::new();
    let client = Router::connect(
        RouterConfig {
            id_segment_size: 64, // 池子大一点,权重配比才不被量化吃掉
            ..client_cfg(&grpc)
        },
        &registry,
    )
    .await
    .expect("connect");
    let (p0_n, p1_n) = spread_by_partition(&client, p0, p1, 256).await;
    assert!(
        p0_n * 100 / 256 >= 60,
        "越空的分区应拿到大部分号:p0={p0_n}, p1={p1_n}"
    );
    assert!(
        p1_n * 100 / 256 <= 40,
        "半满的分区必须被降权:p0={p0_n}, p1={p1_n}"
    );

    // ③ 反向验证:把占用对调(现在 p0 半满)→ 分布跟着翻转 —— 证明它由**权重**驱动,与 pid 顺序无关。
    report_used(&node, &[(p0, cap / 2), (p1, 0)]);
    let (p0_n, p1_n) = spread_by_partition(&client, p0, p1, 256).await;
    assert!(
        p1_n * 100 / 256 >= 60,
        "占用对调后应轮到 p1 拿大头:p0={p0_n}, p1={p1_n}"
    );
    let snapshot: BTreeMap<u32, u32> = client.write_weights().await.into_iter().collect();
    assert_eq!(
        (snapshot.get(&p0).copied(), snapshot.get(&p1).copied()),
        (Some(500), Some(1000)),
        "client 侧权重快照必须跟着 master 更新"
    );
}

/// 申请 `n` 个号,返回它们落在两个分区上的个数(顺带断言"号只能来自这两个可写分区")。
async fn spread_by_partition(client: &Router, p0: u32, p1: u32, n: u32) -> (u64, u64) {
    let ids = client.allocate_batch(n).await.expect("allocate_batch");
    let p0_n = ids.iter().filter(|k| k.partition_id() == p0).count() as u64;
    let p1_n = ids.iter().filter(|k| k.partition_id() == p1).count() as u64;
    assert_eq!(p0_n + p1_n, u64::from(n), "号只能来自这两个可写分区");
    (p0_n, p1_n)
}

/// 直注软状态:节点 101 判活 + 各分区副本的实然态(占用)。心跳是**全量**上报,故一次给全该节点的副本
/// (分次调用会整体替换掉上一次的)。
fn report_used(node: &MasterNode, replicas: &[(u32, u64)]) {
    node.soft.record_beat(
        101,
        HashMap::from([(
            "d0".to_string(),
            DiskUsage {
                capacity_bytes: 1 << 40,
                free_bytes: 1 << 39,
                health: String::new(),
                adjusted_free: 1 << 39,
                last_heartbeat_time: std::time::Instant::now(),
            },
        )]),
        std::time::Duration::from_secs(120),
    );
    node.soft.record_replicas(
        101,
        replicas.iter().map(|(pid, used)| {
            (
                *pid,
                ReplicaState {
                    disk_id: "d0".to_string(),
                    current_state: "ACTIVE".to_string(),
                    lag_bytes: 0,
                    used_bytes: *used,
                },
            )
        }),
        &[],
        true,
    );
}

/// 读 master 的 `ListPartitions`(RW)→ `pid → write_weight`。
async fn list_write_weights(grpc_addr: &str) -> BTreeMap<u32, u32> {
    let mut client = MasterClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect master gRPC");
    list_write_weights_from(&mut client).await
}

async fn list_write_weights_from(client: &mut MasterClient<Channel>) -> BTreeMap<u32, u32> {
    client
        .list_partitions(ListPartitionsRequest {
            version: 1,
            offset: 0,
            limit: 0,
            state: "RW".to_string(),
        })
        .await
        .expect("list_partitions")
        .into_inner()
        .partitions
        .into_iter()
        .map(|p| (p.partition_id, p.write_weight))
        .collect()
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

/// 直连 store 写满一个分区(直到 507),证明该分区已不可写。
///
/// 用**高 objectId**(1_000_000 起)填充:master 发号从 0 起,测试里不会撞上这些号,
/// 避免与 router 后续分配的 key 冲突(AlreadyExists)。
///
/// 填充体取 **1024 B**(≤ 各测试里 router 的最小写入体):507 意味着活跃段剩余 < 24+1024,
/// 于是 router 任何 ≥ 1024 B 的写入都必然再吃 507 —— 否则"写满"后小写仍能塞进残段,测试失真。
async fn fill_partition_until_full(addr: &str, pid: u32) {
    let client = reqwest::Client::new();
    let mut saw_full = false;
    for i in 0..128u32 {
        let key = StoreKey::from_parts(pid, 1_000_000 + i);
        let url = format!(
            "http://{addr}/partitions/{pid}/blobs/{}",
            hex::encode(key.as_bytes())
        );
        let resp = client
            .put(&url)
            .body(vec![7u8; 1024])
            .send()
            .await
            .expect("fill put");
        if resp.status().as_u16() == 507 {
            saw_full = true;
            break;
        }
        assert_eq!(
            resp.status().as_u16(),
            201,
            "fill put should ack until full"
        );
    }
    assert!(saw_full, "partition {pid} must fill up and return 507");
}

/// **`put_with_ttl` 全副本 507 → 摘掉该分区号段 + 返回 StoreFull**(key 已绑定分区,不重试)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_with_ttl_evicts_full_partition_and_returns_store_full() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) =
        start_store_with(&tmp.path().join("store-a"), 101, 64 << 10, 64 << 10).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;
    fill_partition_until_full(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(
        RouterConfig {
            partition_refresh_secs: 0,
            ..client_cfg(&grpc)
        },
        &registry,
    )
    .await
    .expect("connect");

    let key = client.allocate_one().await.expect("allocate");
    assert_eq!(key.partition_id(), pid);
    let err = client
        .put_with_ttl(key, bytes::Bytes::from(payload(1024, 2)), NO_EXPIRATION)
        .await
        .expect_err("写已满分区必须失败");
    assert!(matches!(err, Error::StoreFull { .. }), "got {err:?}");
    // 分区被摘掉:写权重快照里不再有它(后台刷新已关,窗口内不会被重新补回)。
    let weights = client.write_weights().await;
    assert!(
        !weights.iter().any(|(wpid, _)| *wpid == pid),
        "已满分区必须从本地号段池摘掉,实际 {weights:?}"
    );
}

/// **部分副本 507 ≠ 分区满**:RF=3 中一个副本写满、其余两个正常 → 多数派(2/3)仍可达,
/// 写成功且**不触发**换分区重试。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_store_full_still_reaches_quorum() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) =
        start_store_with(&tmp.path().join("store-a"), 101, 64 << 10, 64 << 10).await;
    let (_sb, addr_b) = start_store_with(&tmp.path().join("store-b"), 102, 64 << 20, 8 << 20).await;
    let (_sc, addr_c) = start_store_with(&tmp.path().join("store-c"), 103, 64 << 20, 8 << 20).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;
    register_node(&node, 103, &addr_c).await;

    let pid = create_partition(&node, vec![placement(101), placement(102), placement(103)]).await;
    ensure_replica(&addr_a, pid).await;
    ensure_replica(&addr_b, pid).await;
    ensure_replica(&addr_c, pid).await;
    fill_partition_until_full(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry)
        .await
        .expect("connect");
    let key = client.allocate_one().await.expect("allocate");
    let report = client
        .put_with_ttl(key, bytes::Bytes::from(payload(4096, 3)), NO_EXPIRATION)
        .await
        .expect("两个健康副本必须凑够多数派");
    assert_eq!((report.total, report.required, report.acks), (3, 2, 2));
    assert_eq!(
        client.metrics().router_put_store_full_retries.get(),
        0,
        "部分 507 且多数派可达 → 不重试"
    );
}

/// **`put_auto` 分区写满自动换分区重试**:与 `put_file_retries_on_store_full_partition` 同一场景,
/// 但走**内存 body** 路径(`put_auto` / `put_auto_with_ttl`)。验证小 body 写也能在 507 时
/// 摘掉已满分区、重新发号到空分区,重试成功。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_auto_retries_on_store_full_partition() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) =
        start_store_with(&tmp.path().join("store-a"), 101, 64 << 10, 64 << 10).await;
    let (_sb, addr_b) = start_store_with(&tmp.path().join("store-b"), 102, 64 << 20, 8 << 20).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;

    let full_pid = create_partition(&node, vec![placement(101)]).await;
    let empty_pid = create_partition(&node, vec![placement(102)]).await;
    ensure_replica(&addr_a, full_pid).await;
    ensure_replica(&addr_b, empty_pid).await;
    fill_partition_until_full(&addr_a, full_pid).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(
        RouterConfig {
            id_segment_size: 64,
            partition_refresh_secs: 0,
            ..client_cfg(&grpc)
        },
        &registry,
    )
    .await
    .expect("connect");

    for _ in 0..20 {
        let (key, report) = client
            .put_auto(bytes::Bytes::from(payload(1024, 4)))
            .await
            .expect("put_auto must succeed via retry");
        assert_eq!(key.partition_id(), empty_pid, "成功写入必须落在空分区");
        assert_eq!(report.acks, 1);
    }
    assert!(
        client.metrics().router_put_store_full_retries.get() > 0,
        "至少一次命中已满分区触发换分区重试"
    );
}

/// router-read-topology-refresh:两 reason 的 metric 证据 ——
/// ttl(TTL 短旋钮轮询到重刷发生)/ evict(全员不可达分区触发驱逐)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topology_refresh_counts_ttl_and_evict_reasons() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    register_node(&node, 101, &addr_a).await;

    let mut cfg = client_cfg(&grpc);
    cfg.partition_refresh_secs = 1; // TTL 旋钮 1s(现批语义:读拓扑 TTL 同源)
    let registry = antfs_metric::Registry::new();
    let client = Router::connect(cfg, &registry).await.unwrap();

    // Live 副本分区:写入并读回,顺手填缓存。
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;
    let key = client.allocate_one().await.unwrap();
    client
        .put(key, bytes::Bytes::from_static(b"topo"))
        .await
        .unwrap();
    assert_eq!(
        client.get(&key).await.unwrap(),
        bytes::Bytes::from_static(b"topo")
    );

    // TTL(1s)过后读同一分区 → reason=ttl 实增(轮询带 deadline,期限内容忍 TD 竞争)。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
    loop {
        let n = client
            .metrics()
            .topology_refreshes
            .with_label_values(&["ttl"])
            .get();
        if n >= 1 || tokio::time::Instant::now() >= deadline {
            break;
        }
        // 读触发 TTL 检查(miss 与 hit 都查缓存时刻);等待 TTL 走过用带 deadline 的短夜轮询。
        let _ = client.get(&key).await;
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    assert!(
        client
            .metrics()
            .topology_refreshes
            .with_label_values(&["ttl"])
            .get()
            >= 1,
        "TTL 到期须触发重拉(metric reason=ttl)"
    );

    // evict:注册全员不可达的分区(假地址、无 store),读必然全员失败 → 驱逐面。
    register_node(&node, 977, "127.0.0.1:977").await;
    let dead_pid = create_partition(&node, vec![placement(977)]).await;
    // 取号直至拿到 dead_pid 的 key(该分区 RW、默认权重,发号轮转必至)。
    let mut dead_key = None;
    for _ in 0..64 {
        let k = client.allocate_one().await.expect("allocate");
        if k.partition_id() == dead_pid {
            dead_key = Some(k);
            break;
        }
    }
    let dead_key = dead_key.expect("发号必须覆盖 dead 分区");
    let before = client
        .metrics()
        .topology_refreshes
        .with_label_values(&["evict"])
        .get();
    // 读:全员失败(Http)→ 驱逐(metric+1);重拉后列表相同(死拓扑无新世界)→ 原错误上抛。
    let r = client.get(&dead_key).await;
    assert!(r.is_err(), "全 dead 分区读必须失败");
    let after = client
        .metrics()
        .topology_refreshes
        .with_label_values(&["evict"])
        .get();
    assert_eq!(
        after,
        before + 1,
        "全员不可达须触发一次驱逐重拉(reason=evict)"
    );
}

/// router-id-prefetch:极小段(id_segment_size=4)下连续高频发号 —— 预取续段/耗尽同步补
/// 两条路都通(行为不复发"必然停顿失败");预取错误恒 0,发完无 in-flight 残留。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn id_prefetch_keeps_allocation_fluent_on_tiny_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let mut cfg = client_cfg(&grpc);
    cfg.id_segment_size = 4; // 每段仅 4 号:号段边界高频出现,预取平滑面全暴露
    let registry = antfs_metric::Registry::new();
    let client = Router::connect(cfg, &registry).await.unwrap();

    // 连续 32 个号:预取(续段)/同步补任一路径必须供给 —— 任一失败即 NoWritable 上抛。
    let mut keys = Vec::new();
    for _ in 0..32 {
        keys.push(client.allocate_one().await.expect("发号必须连续可用"));
    }
    assert_eq!(keys.len(), 32);
    assert!(keys.windows(2).all(|w| w[0] != w[1]), "号互不重复");
    assert_eq!(
        client.metrics().prefetch_errors.get(),
        0,
        "真 master 下的预取须零错误"
    );
}
// ── router 层恒切下沉(upload-zero-spool §12 + 读侧裁决⑨/⑩/⑭ + blob-range-read)──────

/// 恒切全链:大 Bytes `put_auto`(17 MiB > 片宽 4 MiB)→ 5 片 + 描述符;
/// ① `get` 自动拼回原字节(⑨);② `stat_object` 逻辑尺寸/片数;③ `get_blob` 返回
/// 裸描述符(格式层 parse 可验,⑩ 的受控口);④ 片散向两个可写分区(SWRR);
/// ⑤ `get_object_stream` 流式整读恒等;⑥ 窗口读(5..7 MiB)只取覆盖片且字节恒等。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_auto_chunks_large_bytes_reads_back_and_spreads() {
    use futures::StreamExt as _;
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    let (_sb, addr_b) = start_store(&tmp.path().join("store-b"), 102).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;
    // 两个可写分区(RF=1 各占一节点):发号轮转必产生分片散布。
    let p0 = create_partition(&node, vec![placement(101)]).await;
    let p1 = create_partition(&node, vec![placement(102)]).await;
    ensure_replica(&addr_a, p0).await;
    ensure_replica(&addr_b, p1).await;

    let registry = antfs_metric::Registry::new();
    let client = Router::connect(client_cfg(&grpc), &registry).await.unwrap();
    assert_eq!(
        client.config().put_bytes_chunk_bytes,
        4 * 1024 * 1024,
        "默认片宽 4 MiB"
    );

    let data = bytes::Bytes::from(payload(17 * 1024 * 1024, 7));
    let (key, _report) = client.put_auto(data.clone()).await.expect("put_auto 恒切");

    // ① 逻辑整读自动拼装(⑨)。
    assert_eq!(client.get(&key).await.expect("get = 逻辑拼装"), data);

    // ② 逻辑元数据。
    let meta = client.stat_object(&key).await.expect("stat_object");
    assert_eq!(meta.size, data.len() as u64);
    assert_eq!(meta.chunks, Some(5), "17 MiB / 4 MiB → 5 片");

    // ③ 裸口 = 描述符本体(§格式层 parse 验证)。
    let raw = client.get_blob(&key).await.expect("get_blob");
    let refs = antfs_descriptor::parse(&raw).expect("描述符可解析");
    assert_eq!(refs.len(), 5);
    assert_eq!(refs[0].size, 4 * 1024 * 1024);
    assert_eq!(refs[4].size, 1024 * 1024, "末片欠整 = 1 MiB");

    // ④ 两分区(发号 SWRR 轮转)至少都被片覆盖。
    let pids: std::collections::HashSet<u32> = refs
        .iter()
        .filter_map(|r| antfs_blob::StoreKey::from_slice(&r.id).map(|k| k.partition_id()))
        .collect();
    assert!(
        pids.contains(&p0) && pids.contains(&p1),
        "片须散向两分区: {pids:?}"
    );

    // ⑤ 流式整读恒等。
    let (len, mut streamed) = client
        .get_object_stream(&key)
        .await
        .expect("get_object_stream");
    assert_eq!(len, data.len() as u64);
    let mut got = Vec::with_capacity(data.len());
    while let Some(chunk) = streamed.next().await {
        got.extend_from_slice(&chunk.expect("流片"));
    }
    assert_eq!(got, data, "流式整读 = 原字节");

    // ⑥ 窗口读(5..7 MiB:覆盖片 2 与 3,其中片 3 全采、片 2 裁头)。
    let start = 5 * 1024 * 1024u64;
    let want = &data[start as usize..(start as usize + 2 * 1024 * 1024)];
    let mut ranged = client
        .get_object_range_stream(&key, start, 2 * 1024 * 1024)
        .await
        .expect("range stream");
    let mut rgot = Vec::new();
    while let Some(chunk) = ranged.next().await {
        rgot.extend_from_slice(&chunk.expect("range 流片"));
    }
    assert_eq!(rgot.as_slice(), want, "窗口读 = 原字节切片");
    assert!(
        client.metrics().get_chunked_total.get() >= 3,
        "拼装读(整/流/窗)都计入 get_chunked_total"
    );
}

/// key 形恒切的失败回收:既有 key 冲突(描述符 409)→ 已入库片**尽力墓碑** +
/// `put_chunked_orphan_deletes_total` 计数;原 key 上的旧对象不受扰动。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_chunked_collision_tombstones_written_chunks() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let registry = antfs_metric::Registry::new();
    let mut cfg = client_cfg(&grpc);
    cfg.put_bytes_chunk_bytes = 64 * 1024; // 129 KiB → 3 片,测试快
    let client = Router::connect(cfg, &registry).await.unwrap();

    let key = client.allocate_one().await.unwrap();
    let first = bytes::Bytes::from_static(b"first-write");
    client.put(key, first.clone()).await.expect("占位写");

    let big = bytes::Bytes::from(payload(129 * 1024, 3));
    let err = client
        .put_with_ttl(key, big, NO_EXPIRATION)
        .await
        .expect_err("同 key 冲突必须失败");
    assert!(
        matches!(err, antfs_router::Error::AlreadyExists { .. }),
        "描述符落 key 409,得 {err:?}"
    );
    assert_eq!(
        client.metrics().put_chunked_orphan_deletes_total.get(),
        3,
        "3 片逐片墓碑"
    );
    // 旧对象原样幸存。
    assert_eq!(client.get_blob(&key).await.unwrap(), first);
}

/// 片宽 0(关):大 Bytes 单笔直写 —— `stat.composed = false`,`get`/`get_blob` 同字节。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_width_is_rejected_and_plain_windows_slice_raw() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    // 2026-10-22 裁决:恒切不可关 —— 片宽 0 必须在 Router 装配处即被拒。
    let mut cfg = client_cfg(&grpc);
    cfg.put_bytes_chunk_bytes = 0;
    let registry = antfs_metric::Registry::new();
    assert!(
        matches!(
            Router::connect(cfg, &registry).await,
            Err(e) if e.to_string().contains("put_bytes_chunk_bytes")
        ),
        "恒切禁 0:装配必须拒绝"
    );

    // 片宽以下 = 单笔普通 blob(不切),裸读与窗口直切语义不受影响。
    let mut cfg = client_cfg(&grpc);
    cfg.put_bytes_chunk_bytes = 4 * 1024 * 1024;
    let client = Router::connect(cfg, &registry).await.unwrap();

    let data = bytes::Bytes::from(payload(600 * 1024 + 7, 5));
    let (key, _) = client.put_auto(data.clone()).await.expect("单笔直写");
    let stat = client.stat(&key).await.expect("stat");
    assert!(!stat.composed, "片宽以下 = 普通 blob");
    assert_eq!(client.get(&key).await.unwrap(), data);
    assert_eq!(client.get_blob(&key).await.unwrap(), data);
    // 普通对象的窗口读 = 数据面 offset/length 直切。
    let mut ranged = client
        .get_object_range_stream(&key, 100, 400 * 1024)
        .await
        .expect("plain range");
    use futures::StreamExt as _;
    let mut got = Vec::new();
    while let Some(c) = ranged.next().await {
        got.extend_from_slice(&c.expect("plain range 片"));
    }
    assert_eq!(got.as_slice(), &data[100..(100 + 400 * 1024)]);
}

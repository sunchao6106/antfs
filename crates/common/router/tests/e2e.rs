//! client(router SDK)端到端集成测试:**真 master + 真 store(真 gRPC + 真 HTTP 端口)**。
//!
//! 覆盖判据(对齐 `docs/design/client.md` 与 `cluster.md §10`):
//!   1. 发号 → 写 → 读往返一致(`put`/`get`/`put_file`/`get_file`);
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

/// **分区写满(全副本 507)自动换分区重试**:`put_file_with_ttl` 内部发号,命中已满分区时
/// 摘掉该分区号段、重新发号到空分区,重试成功;`router_put_store_full_retries_total` 计数。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_file_retries_on_store_full_partition() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    // 节点 A:分区容量极小(64 KiB),写满即 507;节点 B:正常容量。
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

    // 关后台刷新:让"摘掉已满分区"在测试窗口内保持(否则刷新会把它重新补回来)。
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

    let src = tmp.path().join("upload.bin");
    tokio::fs::write(&src, payload(4096, 1)).await.unwrap();
    // 多次上传:首次命中已满分区 → 自动换分区重试;之后只从空分区取号。全部必须成功。
    for _ in 0..20 {
        let (key, report) = client
            .put_file_with_ttl(&src, NO_EXPIRATION)
            .await
            .expect("put_file_with_ttl must succeed via retry");
        assert_eq!(key.partition_id(), empty_pid, "成功写入的号必须落在空分区");
        assert_eq!(report.acks, 1);
    }
    assert!(
        client.metrics().router_put_store_full_retries.get() > 0,
        "至少一次命中已满分区触发换分区重试"
    );
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

/// router-blob-compression:三态语义(默认关逐字节零变化 / 开后表现为帧且读回恒等) +
/// get_file 流式解压 + 复制(RF=2)下帧字节透传保持一致。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compression_roundtrip_disabled_default_and_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    register_node(&node, 101, &addr_a).await;
    let pid = create_partition(&node, vec![placement(101)]).await;
    ensure_replica(&addr_a, pid).await;

    let compressible = || {
        // 高重复可压体(min_size 之上的确切可压模式)。
        let pat = b"compressible-pattern-0123456789-";
        bytes::Bytes::from(
            (0..64 * 1024)
                .map(|i| pat[i % pat.len()])
                .collect::<Vec<_>>(),
        )
    };

    // ── 态一:默认关 = 逐字节零变化(帧态 = 不存在)──
    let registry = antfs_metric::Registry::new();
    let off = Router::connect(client_cfg(&grpc), &registry).await.unwrap();
    let k1 = off.allocate_one().await.unwrap();
    let d1 = compressible();
    off.put(k1, d1.clone()).await.expect("关态首写");
    // 尺寸不缩(读回=写入;stored = 原文,admin 通道不做断言 —— get 恒等即可)。
    assert_eq!(off.get(&k1).await.unwrap(), d1);
    assert_eq!(
        off.metrics()
            .compression_attempts
            .with_label_values(&["disabled"])
            .get(),
        1,
        "关态应计 disabled 且不产生帧"
    );

    // ── 态二:开后 = 帧落盘 + bulk 读回恒等 ──
    // 开态用默认算法(gzip,2026-10-20 所有者终裁)。
    let mut cfg_on = client_cfg(&grpc);
    cfg_on.compression_enabled = true;
    assert_eq!(cfg_on.compression_algorithm, "gzip", "默认算法须为 gzip");
    let registry_on = antfs_metric::Registry::new();
    let on = Router::connect(cfg_on, &registry_on).await.unwrap();
    let k2 = on.allocate_one().await.unwrap();
    let d2 = compressible();
    let report = on.put(k2, d2.clone()).await.expect("开态首写");
    assert_eq!(report.acks, 1);
    assert_eq!(on.get(&k2).await.unwrap(), d2, "bulk 口透明解压恒等");
    let kept = on
        .metrics()
        .compression_attempts
        .with_label_values(&["kept"])
        .get();
    let saved = on.metrics().compression_saved_bytes.get();
    assert_eq!(kept, 1, "可压体过闸 kept");
    assert!(saved > 65536 / 2, "节省过半(高度重复文本),got {saved}");

    // ── 态三:get_file(流式口)对帧对象满地落盘恒等 ──
    let dst = tmp.path().join("out.bin");
    let n = on.get_file(&k2, &dst).await.expect("get_file");
    assert_eq!(n, d2.len() as u64, "流式口产出 = 逻辑长度(orig_len)");
    let read_back = std::fs::read(&dst).unwrap();
    assert_eq!(read_back, d2);

    // 同键零长前端(交叉):第二个小对象(低于 min_size)不压、原样 roundtrip。
    let k3 = on.allocate_one().await.unwrap();
    let tiny = bytes::Bytes::from_static(b"tiny-not-compressed");
    on.put(k3, tiny.clone()).await.unwrap();
    assert_eq!(on.get(&k3).await.unwrap(), tiny);
    assert_eq!(
        on.metrics()
            .compression_attempts
            .with_label_values(&["too_small"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compression_survives_replication_frame_passthrough() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc) = solo_master(&tmp.path().join("master")).await;
    let (_sa, addr_a) = start_store(&tmp.path().join("store-a"), 101).await;
    let (_sb, addr_b) = start_store(&tmp.path().join("store-b"), 102).await;
    register_node(&node, 101, &addr_a).await;
    register_node(&node, 102, &addr_b).await;
    // RF=2 ⇒ W=2:两副本都写;复制拉取(?replication=1)透传帧字节,两副本读回一致。
    let pid = create_partition(&node, vec![placement(101), placement(102)]).await;
    ensure_replica(&addr_a, pid).await;
    ensure_replica(&addr_b, pid).await;

    let mut cfg = client_cfg(&grpc);
    cfg.compression_enabled = true;
    let registry = antfs_metric::Registry::new();
    let client = Router::connect(cfg, &registry).await.unwrap();
    let pat = b"replicated-compressible-content-";
    let data = {
        let mut v = Vec::with_capacity(128 * 1024);
        for i in 0..128 * 1024 {
            v.push(pat[i % pat.len()]);
        }
        bytes::Bytes::from(v)
    };
    let key = client.allocate_one().await.unwrap();
    client
        .put(key, data.clone())
        .await
        .expect("W=2 双副本帧写入");
    assert_eq!(client.get(&key).await.unwrap(), data, "读回恒等");
}

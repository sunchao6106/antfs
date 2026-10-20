//! `GetBlobIds`(master 发号)集成测试。
//!
//! 断言的不变量(对齐 `openspec/changes/blobid-master-assign` D1–D4/D9):
//!   1. 号段**连续、不重叠、按 partition 单调**;两层批处理把 Raft 提交摊薄到「每 N×倍数 个号一次」;
//!   2. 未指定 partition 时**按剩余容量加权**挑可写分区(D10);
//!   3. **空池(切主/重启)从已提交水位续发,绝不重用已发出的号**(持久性红线 §3.1);
//!   4. 号段空间耗尽 → 明确报错且水位**不动**(绝不回绕复用);
//!   5. 非 leader 拒绝发号并回填 leader 地址。
//!
//! 指标断言一律打在**节点实际接入的那个 `Registry`** 上(CLAUDE.md §6)。

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;

use antfs_master::config::MasterConfig;
use antfs_master::idalloc::IdAllocator;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{GetBlobIdsRequest, GetBlobIdsResponse};
use antfs_master::soft::{DiskUsage, ReplicaState};
use antfs_master::types::{
    DiskSpec, MasterCommand, MasterResponse, NodeInfo, PartitionId, ReplicaPlacement,
};
use antfs_master::MasterNode;
use antfs_metric::Registry;
use openraft::BasicNode;
use tonic::transport::Channel;

const SECRET: &str = "test-secret";
const DISK: &str = "d0";
const SEGMENT: u32 = 4;
const MULTIPLE: u32 = 2;
/// 号段 4 × 倍数 2 → 每次抬水位预留 8 个号。
const BATCH: u32 = SEGMENT * MULTIPLE;

fn base_config(id: u64, dir: &Path, members: Vec<u64>) -> MasterConfig {
    let cfg = MasterConfig {
        node_id: id,
        raft_addr: "127.0.0.1:0".to_string(),
        grpc_addr: "127.0.0.1:0".to_string(),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: members,
        cluster_secret: SECRET.to_string(),
        // 测试不需要失败驱动的再复制(本文件不验证调谐),关掉宽限以免噪声干扰号段断言。
        reconcile_startup_grace_secs: 0,
        id_alloc_segment_size: SEGMENT,
        id_alloc_raft_batch_multiple: MULTIPLE,
        ..Default::default()
    };
    cfg
}

/// 起一个单节点 master(自举 → 等自己当选 leader),返回节点与 gRPC 地址。
async fn solo_master(dir: &Path) -> (MasterNode, String) {
    let cfg = base_config(1, dir, vec![1]);
    let registry = Registry::new();
    let node = MasterNode::start(cfg, &registry)
        .await
        .expect("start solo master");
    node.serve().expect("serve");
    let raft_addr = node.bound_raft_addr().expect("bound raft");
    node.initialize(BTreeMap::from([(1, BasicNode::new(raft_addr))]))
        .await
        .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "solo leader elected")
        .await
        .expect("leader elected");
    let grpc_addr = node.bound_grpc_addr().expect("bound grpc");
    (node, grpc_addr)
}

/// 直接经 Raft 固化一个数据节点身份(测试只要地址,不跑真 store)。
async fn register_store(node: &MasterNode, id: u64, addr: &str) {
    let info = NodeInfo {
        dc: "dc1".into(),
        rack: "r1".into(),
        addr: addr.into(),
        disks: BTreeMap::from([(
            DISK.to_string(),
            DiskSpec {
                capacity_bytes: 1 << 40,
            },
        )]),
        registered_at_ms: 1,
    };
    node.raft
        .client_write(MasterCommand::RegisterNode { node_id: id, info })
        .await
        .expect("register store");
}

/// 上报一次心跳(让节点判活 + 各盘有真实余量,避免调谐误判欠副本)。
fn beat(node: &MasterNode, node_id: u64, free: u64) {
    node.soft.record_beat(
        node_id,
        HashMap::from([(
            DISK.to_string(),
            DiskUsage {
                capacity_bytes: 1 << 40,
                free_bytes: free,
                health: String::new(),
                adjusted_free: free,
                last_heartbeat_time: std::time::Instant::now(),
            },
        )]),
        std::time::Duration::from_secs(120),
    );
}

/// 上报某节点各副本的实际占用(`used_bytes`),喂 D10 的容量加权。
///
/// 心跳是**全量**上报(store `full_report=true`)→ 一次给全该节点的副本,不能分次调用(会整体替换)。
fn beat_used(node: &MasterNode, node_id: u64, replicas: &[(PartitionId, u64)]) {
    node.soft.record_replicas(
        node_id,
        replicas.iter().map(|(pid, used)| {
            (
                *pid,
                ReplicaState {
                    disk_id: DISK.to_string(),
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

fn placement(node_id: u64) -> ReplicaPlacement {
    ReplicaPlacement {
        node_id,
        disk_id: DISK.to_string(),
        target_state: "ACTIVE".to_string(),
        incarnation: 0,
    }
}

/// 直接经 Raft 建分区(测试要精确控制副本数,绕开 leader 侧选盘对软状态的依赖)。
async fn create_partition(node: &MasterNode, replicas: Vec<ReplicaPlacement>) -> PartitionId {
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

/// 该 partition 的已提交水位(下一个可发放的 objectId)。
fn watermark(node: &MasterNode, pid: PartitionId) -> u32 {
    node.sm_data
        .read()
        .expect("sm lock")
        .state
        .object_next
        .get(&pid)
        .copied()
        .unwrap_or(0)
}

async fn connect(grpc_addr: &str) -> MasterClient<Channel> {
    MasterClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect master gRPC")
}

async fn get_ids(
    client: &mut MasterClient<Channel>,
    pid: Option<PartitionId>,
    count: u32,
) -> GetBlobIdsResponse {
    client
        .get_blob_ids(GetBlobIdsRequest {
            version: 1,
            cluster_secret: SECRET.to_string(),
            count,
            partition_id: pid,
        })
        .await
        .expect("get_blob_ids rpc")
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allocates_contiguous_segments_and_amortizes_raft_commits() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let pid = create_partition(&node, vec![placement(101)]).await;

    let mut client = connect(&grpc_addr).await;
    let mut starts = Vec::new();
    for _ in 0..8 {
        // count=0 → 用 master 默认号段大小(SEGMENT)。
        let r = get_ids(&mut client, Some(pid), 0).await;
        assert!(r.ok, "allocation must succeed: {}", r.error);
        assert_eq!(r.partition_id, pid);
        assert_eq!(r.count, SEGMENT);
        assert_eq!(r.replicas.len(), 1);
        assert_eq!(r.replicas[0].node_id, 101);
        assert_eq!(r.replicas[0].addr, "127.0.0.1:7200");
        assert!(r.replicas[0].up, "replica node heartbeated → must be up");
        starts.push(r.start_object_id as u32);
    }

    // 号段连续、不重叠、从 0 单调。
    assert_eq!(starts[0], 0);
    for w in starts.windows(2) {
        assert_eq!(
            w[1],
            w[0] + SEGMENT,
            "segments must be contiguous and non-overlapping"
        );
    }

    // 指标打在**真 Registry** 上。
    assert_eq!(node.metrics.blob_ids_requests.get(), 8);
    assert_eq!(node.metrics.blob_ids_allocated.get(), (8 * SEGMENT) as u64);
    assert_eq!(node.metrics.blob_ids_failures.get(), 0);
    // 8 次申请 × 4 个号 = 32 个号;每次抬水位预留 8 个 → 恰好 4 次 Raft 提交(两层批处理摊薄)。
    assert_eq!(node.metrics.id_alloc_raft_commits.get(), 4);
    assert_eq!(
        node.metrics.id_alloc_holes.get(),
        0,
        "恰好耗尽才换池 → 无空洞"
    );
    assert_eq!(watermark(&node, pid), 8 * SEGMENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segment_boundary_leaves_holes_which_are_counted_not_reused() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let pid = create_partition(&node, vec![placement(101)]).await;
    let mut client = connect(&grpc_addr).await;

    // 号段 4、倍数 2 → 每次抬水位预留 8 个。请求 3 个:
    //   第 1 次:池空 → 预留 [0,8),发 [0,3)
    //   第 2 次:池内还剩 5 个 → **直接取,零 Raft 提交**,发 [3,6)
    //   第 3 次:池内只剩 2 个 < 3 → 预留 [8,16),旧池剩的 2 个被丢弃(空洞),发 [8,11)
    let a = get_ids(&mut client, Some(pid), 3).await;
    let b = get_ids(&mut client, Some(pid), 3).await;
    let c = get_ids(&mut client, Some(pid), 3).await;
    assert_eq!((a.start_object_id, a.count), (0, 3));
    assert_eq!(
        (b.start_object_id, b.count),
        (3, 3),
        "池内还有号 → 热路径不碰 Raft"
    );
    assert_eq!(
        (c.start_object_id, c.count),
        (8, 3),
        "换池后从新预留段起点续发(4..8 未发完)"
    );

    assert_eq!(
        node.metrics.id_alloc_raft_commits.get(),
        2,
        "3 次申请只抬了 2 次水位"
    );
    assert_eq!(
        node.metrics.id_alloc_holes.get(),
        2,
        "池内未发完的 2 个号记为空洞"
    );
    assert_eq!(node.metrics.blob_ids_failures.get(), 0);

    // 空洞无害但绝不重叠:已发出的区间两两不交叉。
    for (x, y) in [(&a, &b), (&b, &c), (&a, &c)] {
        assert!(x.start_object_id + x.count as u64 <= y.start_object_id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn picks_writable_partition_weighted_by_remaining_capacity() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let p0 = create_partition(&node, vec![placement(101)]).await;
    let p1 = create_partition(&node, vec![placement(101)]).await;
    let cap = node.config.partition_capacity_bytes;

    // p0 的副本报到几乎写满(权重剩 1),p1 报未写(权重 = 整段容量)。
    beat_used(&node, 101, &[(p0, cap - 1), (p1, 0)]);

    let mut client = connect(&grpc_addr).await;
    let mut by_pid: HashMap<PartitionId, u32> = HashMap::new();
    for _ in 0..200 {
        // partition_id 缺省 = 让 master 按剩余容量加权自动挑。
        let r = get_ids(&mut client, None, 1).await;
        assert!(r.ok, "{}", r.error);
        *by_pid.entry(r.partition_id).or_default() += 1;
    }
    let near_full = by_pid.get(&p0).copied().unwrap_or(0);
    let empty = by_pid.get(&p1).copied().unwrap_or(0);
    assert_eq!(empty + near_full, 200);
    assert!(
        empty >= 195,
        "几乎写满的分区应极少被选中(实际 {near_full} 次),by_pid={by_pid:?}"
    );
    assert!(
        by_pid.keys().all(|p| *p == p0 || *p == p1),
        "只能从 RW 分区里挑"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_allocator_never_reuses_issued_ids() {
    // 「切主/重启后不复用」等价于「空池从已提交水位续发」:号段池只在 leader 内存里,
    // 新 leader / 重启进程的池必为空,故此处直接用一个全新的 IdAllocator 复现该场景。
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let pid = create_partition(&node, vec![placement(101)]).await;
    let mut client = connect(&grpc_addr).await;

    let mut issued = Vec::new();
    for _ in 0..3 {
        let r = get_ids(&mut client, Some(pid), SEGMENT).await;
        assert!(r.ok, "{}", r.error);
        for i in 0..r.count {
            issued.push(r.start_object_id as u32 + i);
        }
    }
    assert_eq!(issued, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let committed = watermark(&node, pid);
    assert_eq!(committed, BATCH * 2, "3 段共 12 个号 → 已提交水位 16");

    // 空池(等价于新 leader / 重启后的进程)续发:起点必须是已提交水位,绝不落在已发号区间内。
    let fresh = IdAllocator::default();
    let (start, n) = fresh
        .allocate(&node.raft, pid, SEGMENT, &node.config, &node.metrics)
        .await
        .expect("fresh allocator resumes");
    assert_eq!(start, committed, "必须从已提交水位续发");
    assert_eq!(n, SEGMENT);
    for i in 0..n {
        assert!(
            !issued.contains(&(start + i)),
            "id {} 已被发出过,绝不能重发",
            start + i
        );
    }
    assert_eq!(
        node.metrics.id_alloc_holes.get(),
        0,
        "空池无旧池剩余 → 无空洞"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exhausted_object_space_fails_loudly_and_freezes_watermark() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let pid = create_partition(&node, vec![placement(101)]).await;
    let mut client = connect(&grpc_addr).await;

    // 直接把水位顶到 u32::MAX(模拟该分区号段空间用尽)。
    node.raft
        .client_write(MasterCommand::AllocateObjectId {
            partition_id: pid,
            count: u32::MAX,
        })
        .await
        .expect("exhaust space");

    let r = get_ids(&mut client, Some(pid), SEGMENT).await;
    assert!(!r.ok, "耗尽必须 ok=false,不能悄悄回绕复用");
    assert!(
        r.error.contains("exhausted"),
        "error 要能一眼看出原因,实际:{}",
        r.error
    );
    assert_eq!(r.count, 0);
    assert_eq!(watermark(&node, pid), u32::MAX, "耗尽后水位不动(绝不回绕)");
    assert_eq!(node.metrics.blob_ids_failures.get(), 1);
    assert_eq!(node.metrics.blob_ids_allocated.get(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_refuses_allocation_and_reports_leader() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes: Vec<(MasterNode, String)> = Vec::new();
    let mut members = BTreeMap::new();
    let mut grpc: BTreeMap<u64, String> = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = base_config(id, &dir, vec![1, 2, 3]);
        let registry = Registry::new();
        let node = MasterNode::start(cfg, &registry).await.expect("start node");
        node.serve().expect("serve");
        let raft_addr = node.bound_raft_addr().expect("bound raft");
        members.insert(id, BasicNode::new(raft_addr));
        let grpc_addr = node.bound_grpc_addr().expect("bound grpc");
        grpc.insert(id, grpc_addr.clone());
        nodes.push((node, grpc_addr));
    }
    nodes[0].0.initialize(members).await.expect("initialize");
    for (n, _) in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(20)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("leader elected");
    }

    let leader_id = nodes[0]
        .0
        .raft
        .metrics()
        .borrow()
        .current_leader
        .expect("has leader");
    let leader = nodes
        .iter()
        .find(|(n, _)| n.config.node_id == leader_id)
        .expect("leader node")
        .0
        .clone();
    let follower_id = (1u64..=3)
        .find(|i| *i != leader_id)
        .expect("a follower exists");

    // 经 leader 固化一个数据节点 + 建分区(写命令只有 leader 能提交)。
    register_store(&leader, 101, "127.0.0.1:7200").await;
    beat(&leader, 101, 1 << 40);
    let pid = create_partition(&leader, vec![placement(101)]).await;

    // 向 follower 发号:必须拒绝并回填 leader 地址(由 client 重定向)。
    // leader 的自报注册(leader_addr 的唯一真值)经 Raft 异步收敛 —— 确定性轮询带 deadline,
    // 不许 sleep(§5.6)。
    let mut client = connect(&grpc[&follower_id]).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let r = loop {
        let r = get_ids(&mut client, Some(pid), SEGMENT).await;
        assert!(!r.ok);
        assert_eq!(r.error, "not leader");
        if !r.leader_addr.is_empty() || tokio::time::Instant::now() >= deadline {
            break r;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    let want = leader.bound_grpc_addr().expect("bound grpc");
    assert_eq!(
        r.leader_addr, want,
        "非 leader 必须回填 leader 的 gRPC 地址(master-leader-address 唯一真值)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_without_replica_is_never_offered() {
    // 迁移/下线过程中一个分区可能瞬时 0 副本:发号给它 = 让 client 拿到一个必然失败的号段。
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    register_store(&node, 101, "127.0.0.1:7200").await;
    beat(&node, 101, 1 << 40);
    let pid = create_partition(&node, vec![placement(101)]).await;
    let mut client = connect(&grpc_addr).await;
    assert!(get_ids(&mut client, Some(pid), SEGMENT).await.ok);

    // 移除该分区唯一副本(它仍是 RW 态,但已无处可写)。
    node.raft
        .client_write(MasterCommand::RemoveReplica {
            partition_id: pid,
            node_id: 101,
        })
        .await
        .expect("remove replica");
    assert!(
        node.sm_data.read().expect("sm").state.partitions[&pid]
            .replicas
            .is_empty(),
        "前置条件:该分区已无副本"
    );

    // ① 自动挑选:不作候选(且集群此时没有别的可写分区 → 明确报"没有可写分区")。
    // partition_id 缺省 = 由 master 自动挑(该分区已无副本,不该被选中)。
    let auto = get_ids(&mut client, None, SEGMENT).await;
    assert!(!auto.ok, "无副本的分区不得被选中");
    assert!(
        auto.error.contains("no writable partition"),
        "{}",
        auto.error
    );
    assert!(auto.replicas.is_empty());

    // ② 显式点名:同样拒绝,并说清原因(而不是发一段写不进去的号)。
    let named = get_ids(&mut client, Some(pid), SEGMENT).await;
    assert!(!named.ok);
    assert!(named.error.contains("no replica"), "{}", named.error);
    assert_eq!(named.count, 0);
}

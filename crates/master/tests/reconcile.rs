//! M3 集成测试:副本状态转移下发 + 失联检测再复制(3 节点 master,无 store server)。
//!
//! 同步用带超时的条件轮询(CLAUDE.md §6 允许"带超时的轮询"):`wait_until` 在超时内反复查条件。

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::types::{MasterCommand, NodeInfo, ReplicaPlacement};
use antfs_master::MasterNode;
use openraft::BasicNode;

const SECRET: &str = "test-secret";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn cfg(id: u64, dir: &std::path::Path, raft: u16, grpc: u16) -> MasterConfig {
    MasterConfig {
        node_id: id,
        raft_addr: format!("127.0.0.1:{raft}"),
        grpc_addr: format!("127.0.0.1:{grpc}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1, 2, 3],
        cluster_secret: SECRET.to_string(),
        // 长超时:单次心跳即可让节点在整个测试期"存活";103 从不心跳 → 判失联。
        heartbeat_timeout_secs: 60,
        partition_capacity_bytes: 100 * 1024 * 1024 * 1024,
        disk_near_full_ratio: 0.95,
        auto_grow_enabled: false,
        auto_grow_min_writable: 4,
        auto_grow_max_partitions: 100_000,
        auto_grow_interval_secs: 30,
        auto_grow_batch: 4,
        reconcile_interval_secs: 1,      // 快速调谐,便于测试
        reconcile_startup_grace_secs: 0, // 测试免宽限,立即调谐
        id_alloc_segment_size: 1024,
        id_alloc_raft_batch_multiple: 2,
        balance_max_in_flight: 1,
        balance_max_moves: 100,
        balance_skew_threshold: 1.0,
        balance_move_timeout_secs: 300,
    }
}

fn data_node(rack: &str) -> NodeInfo {
    NodeInfo {
        dc: "dc1".into(),
        rack: rack.into(),
        addr: format!("127.0.0.1:{}", free_port()),
        disks: BTreeMap::from([(
            "d0".to_string(),
            antfs_master::types::DiskSpec {
                capacity_bytes: 1_000_000_000_000,
            },
        )]),
        registered_at_ms: 1,
    }
}

/// 测试盘容量(1 TB)与默认余量(90% → 用量 10%,远低于 near-full 阈值)。
const DISK_CAPACITY: u64 = 1_000_000_000_000;
const DISK_FREE: u64 = 900_000_000_000;

/// 心跳,可指定 `d0` 的余量与健康上报(用于"盘将满"/"坏盘"场景)。
fn beat_disk(
    node_id: u64,
    replicas: Vec<antfs_master::pb::ReplicaBeat>,
    free_bytes: u64,
    health: &str,
) -> antfs_master::pb::NodeBeat {
    antfs_master::pb::NodeBeat {
        version: 1,
        cluster_secret: SECRET.into(),
        node_id,
        disks: vec![antfs_master::pb::DiskBeat {
            disk_id: "d0".into(),
            capacity_bytes: DISK_CAPACITY,
            free_bytes,
            health: health.into(),
        }],
        full_report: true,
        replicas,
    }
}

fn beat(node_id: u64, replicas: Vec<antfs_master::pb::ReplicaBeat>) -> antfs_master::pb::NodeBeat {
    beat_disk(node_id, replicas, DISK_FREE, "")
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
async fn reconcile_transitions_and_rereplication() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg(id, &dir, free_port(), free_port());
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader elected");
    }
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();

    // 注册 4 个数据节点(101..104),各不同机架。
    let racks = [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")];
    for (id, rack) in racks {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register data node");
    }

    // 显式建 1 个 RF=3 分区,副本落 101/102/103。
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 101,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        })
        .await
        .expect("create partition");
    let pid = match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    };

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");

    // (a) 转移下发:101 上报 pid current=BOOTSTRAP(≠target ACTIVE)→ 响应含转移指令。
    let mut hb101 = client
        .heartbeat(tokio_stream::iter(vec![beat(
            101,
            vec![antfs_master::pb::ReplicaBeat {
                partition_id: pid,
                disk_id: "d0".into(),
                current_state: "BOOTSTRAP".into(),
                lag_bytes: 10,
                used_bytes: 4_000,
            }],
        )]))
        .await
        .expect("hb101")
        .into_inner();
    let directive = hb101.message().await.expect("recv").expect("directive");
    assert!(
        directive
            .transitions
            .iter()
            .any(|t| t.partition_id == pid && t.target_state == "ACTIVE"),
        "heartbeat with current!=target must return a transition to ACTIVE"
    );

    // 102、104 心跳保持存活(103 从不心跳 → 失联)。104 是再复制的存活备选。
    for id in [102u64, 104] {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }

    // (b) 失联再复制:103 失联 → reconciler 在存活节点(104)补一个副本,placement 副本数增至 4。
    let sm = leader.sm_data.clone();
    let grew = wait_until(Duration::from_secs(20), || {
        let g = sm.read().unwrap();
        g.state
            .partitions
            .get(&pid)
            .map(|p| p.replicas.iter().any(|r| r.node_id == 104))
            .unwrap_or(false)
    })
    .await;
    let g = sm.read().unwrap();
    let part = &g.state.partitions[&pid];
    assert!(
        grew,
        "reconciler must add a replacement replica on live node 104; replicas={:?}",
        part.replicas.iter().map(|r| r.node_id).collect::<Vec<_>>()
    );
    assert!(
        part.replicas.len() >= 4,
        "placement must grow with the replacement replica"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decommission_evacuates_replicas_add_before_remove() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg(id, &dir, free_port(), free_port());
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader");
    }
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 101,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        })
        .await
        .expect("create partition");
    let pid = match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    };

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");

    // 全部 4 个数据节点心跳保持存活(含被下线的 103)。
    for id in [101u64, 102, 103, 104] {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }

    // 下线 103:evacuation 应先在 104 补副本,再移除 103。
    let d = client
        .decommission_node(antfs_master::pb::DecommissionRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            node_id: 103,
            force: true, // 本测试聚焦 evacuation 行为,跳过可行性前置校验
        })
        .await
        .expect("decommission")
        .into_inner();
    assert!(d.ok, "decommission must succeed: {}", d.error);

    let sm = leader.sm_data.clone();
    let done = wait_until(Duration::from_secs(20), || {
        let g = sm.read().unwrap();
        g.state
            .partitions
            .get(&pid)
            .map(|p| {
                let ids: Vec<u64> = p.replicas.iter().map(|r| r.node_id).collect();
                ids.contains(&104) && !ids.contains(&103)
            })
            .unwrap_or(false)
    })
    .await;
    let g = sm.read().unwrap();
    let mut ids: Vec<u64> = g.state.partitions[&pid]
        .replicas
        .iter()
        .map(|r| r.node_id)
        .collect();
    ids.sort();
    assert!(
        done,
        "evacuation must move replica off 103 onto a healthy node; got {ids:?}"
    );
    assert_eq!(
        ids,
        vec![101, 102, 104],
        "final placement excludes decommissioned 103, includes 104"
    );
}

/// 宽限期内 reconciler 不动作(用于确定性地观察 move 的中间态,不被"后删"竞态干扰)。
fn cfg_no_reconcile(id: u64, dir: &std::path::Path, raft: u16, grpc: u16) -> MasterConfig {
    let mut c = cfg(id, dir, raft, grpc);
    c.reconcile_startup_grace_secs = 3600;
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_and_move_replica_validation() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg_no_reconcile(id, &dir, free_port(), free_port());
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader");
    }
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 101,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        })
        .await
        .expect("create partition");
    let pid = match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    };

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");
    for id in [101u64, 102, 103, 104] {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }

    // ---- RepairPartition ----
    // 唯一候选是 104(其余节点已持有副本)→ 确定性落点。
    let r = client
        .repair_partition(antfs_master::pb::RepairPartitionRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
        })
        .await
        .expect("repair")
        .into_inner();
    assert!(r.ok, "repair must succeed: {}", r.error);
    assert_eq!(r.added_node_id, 104);
    assert_eq!(r.added_disk_id, "d0");

    // 分区不存在 → error。
    let r = client
        .repair_partition(antfs_master::pb::RepairPartitionRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: 999,
        })
        .await
        .expect("repair")
        .into_inner();
    assert!(
        !r.ok && r.error == "partition not found",
        "got: {}",
        r.error
    );

    // 全部 4 节点都已持有副本 → 无可放盘。
    let r = client
        .repair_partition(antfs_master::pb::RepairPartitionRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
        })
        .await
        .expect("repair")
        .into_inner();
    assert!(!r.ok && r.error == "no eligible disk", "got: {}", r.error);

    let dump = antfs_metric::gather(&leader.registry).expect("gather");
    assert!(
        dump.contains("master_repairs_triggered_total 3"),
        "3 repair requests must be counted, got:\n{dump}"
    );

    // ---- MoveReplica 校验(reconciler 宽限期内不动作,中间态确定)----
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 2,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 101,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        })
        .await
        .expect("create partition 2");
    let pid2 = match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    };

    // 自动选择:候选 {103,104},余量并列按 node_id 升序 → 103。
    let m = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid2,
            from_node_id: 102,
            to_node_id: 0,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(m.ok, "move must succeed: {}", m.error);
    assert_eq!(m.to_node_id, 103);
    assert_eq!(m.to_disk_id, "d0");

    // 中间态:新副本已补(3 副本),迁出源 102 记入 moving。
    {
        let g = leader.sm_data.read().unwrap();
        let p = &g.state.partitions[&pid2];
        assert_eq!(
            p.replicas.len(),
            3,
            "add-before-remove: new replica added first"
        );
        assert!(
            g.state.moving.contains(&(pid2, 102)),
            "source must be marked moving-out"
        );
    }

    // 幂等防护:重复 move 同一 (pid, from) → 报错。
    let dup = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid2,
            from_node_id: 102,
            to_node_id: 0,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(
        !dup.ok && dup.error == "move already in progress",
        "got: {}",
        dup.error
    );

    // from 不持有副本 → 报错。
    let bad = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid2,
            from_node_id: 999,
            to_node_id: 0,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(
        !bad.ok && bad.error.contains("does not hold a replica"),
        "got: {}",
        bad.error
    );

    // 指定目标已持有该分区副本 → 报错。
    let conflict = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid2,
            from_node_id: 101,
            to_node_id: 103,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(
        !conflict.ok && conflict.error.contains("already holds a replica"),
        "got: {}",
        conflict.error
    );

    // list_partitions 回填 moving_out_nodes。
    let list = client
        .list_partitions(antfs_master::pb::ListPartitionsRequest {
            version: 1,
            offset: 0,
            limit: 0,
            state: String::new(),
        })
        .await
        .expect("list_partitions")
        .into_inner();
    let entry = list
        .partitions
        .iter()
        .find(|p| p.partition_id == pid2)
        .expect("pid2 entry");
    assert_eq!(
        entry.moving_out_nodes,
        vec![102],
        "moving-out source must be listed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_replica_completes_add_before_remove() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg(id, &dir, free_port(), free_port());
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader");
    }
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement {
                    node_id: 101,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                },
            ],
        })
        .await
        .expect("create partition");
    let pid = match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    };

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");
    // 104 暂不心跳(不是候选):此时 move 无落点。
    for id in [101u64, 102, 103] {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    let m = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
            from_node_id: 103,
            to_node_id: 0,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(
        !m.ok && m.error == "no eligible disk",
        "no up target: {}",
        m.error
    );

    // 104 上线 → move 成功落到 104。
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat(104, vec![])]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive");
    let m = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
            from_node_id: 103,
            to_node_id: 104,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(m.ok, "move must succeed: {}", m.error);
    assert_eq!(m.to_node_id, 104);
    assert_eq!(m.to_disk_id, "d0");

    // 模拟 104 心跳上报新副本 ACTIVE(追平完成),保持节点存活。
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat(
            104,
            vec![antfs_master::pb::ReplicaBeat {
                partition_id: pid,
                disk_id: "d0".into(),
                current_state: "ACTIVE".into(),
                lag_bytes: 0,
                used_bytes: 1_000,
            }],
        )]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive");

    // reconciler"后删":健康副本(不含迁出源)≥ RF 后移除 103,moving 清空。
    let sm = leader.sm_data.clone();
    let done = wait_until(Duration::from_secs(20), || {
        let g = sm.read().unwrap();
        let Some(p) = g.state.partitions.get(&pid) else {
            return false;
        };
        let mut ids: Vec<u64> = p.replicas.iter().map(|r| r.node_id).collect();
        ids.sort();
        ids == vec![101, 102, 104] && g.state.moving.is_empty()
    })
    .await;
    let ids: Vec<u64> = {
        let g = leader.sm_data.read().unwrap();
        g.state.partitions[&pid]
            .replicas
            .iter()
            .map(|r| r.node_id)
            .collect()
    };
    assert!(
        done,
        "move must complete: source removed after replacement healthy; got {ids:?}"
    );

    // 指标断言:apply 可见先于 client_write 返回后的 inc(跨任务调度),故同样用带超时轮询;
    // 且并行测试下 leader 可能改选,"后删"由时任 leader 执行并计数,故跨全部节点 Registry 断言。
    let counted = wait_until(Duration::from_secs(10), || {
        nodes.iter().any(|n| {
            antfs_metric::gather(&n.registry)
                .map(|d| d.contains("master_replicas_moved_total 1"))
                .unwrap_or(false)
        })
    })
    .await;
    assert!(
        counted,
        "one moved replica must be counted on the acting leader"
    );
}

/// 观测指标:assign/lookup 打点 + reconciler 集群 gauges(断言打在 leader 实际接入的 Registry,CLAUDE.md §6)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assign_lookup_counters_and_cluster_gauges() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg(id, &dir, free_port(), free_port()); // reconcile 1s 周期、grace 0
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader");
    }
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();
    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");

    // 尚无分区:assign 必失败(ok=false);lookup 不存在的分区 → not found。两者都计入 requests。
    let a = client
        .assign_partition(antfs_master::pb::AssignRequest { version: 1 })
        .await
        .expect("assign")
        .into_inner();
    assert!(!a.ok, "no writable partition yet");
    let miss = client
        .lookup_partition(antfs_master::pb::LookupRequest {
            version: 1,
            partition_id: 999,
        })
        .await;
    assert_eq!(
        miss.expect_err("unknown partition").code(),
        tonic::Code::NotFound
    );

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register");
    }
    // 3 个分区(pid1 保持 RW,pid2 置 RO,pid3 置 SEALED)。
    let mut pids = Vec::new();
    for _ in 0..3 {
        let resp = leader
            .raft
            .client_write(MasterCommand::CreatePartition {
                replication_factor: 3,
                replicas: vec![
                    ReplicaPlacement {
                        node_id: 101,
                        disk_id: "d0".into(),
                        target_state: "ACTIVE".into(),
                    },
                    ReplicaPlacement {
                        node_id: 102,
                        disk_id: "d0".into(),
                        target_state: "ACTIVE".into(),
                    },
                    ReplicaPlacement {
                        node_id: 103,
                        disk_id: "d0".into(),
                        target_state: "ACTIVE".into(),
                    },
                ],
            })
            .await
            .expect("create partition");
        match resp.data {
            antfs_master::MasterResponse::PartitionId(id) => pids.push(id),
            other => panic!("unexpected: {other:?}"),
        }
    }
    for (pid, state) in [(pids[1], "RO"), (pids[2], "SEALED")] {
        let r = client
            .set_partition_state(antfs_master::pb::SetPartitionStateRequest {
                version: 1,
                cluster_secret: SECRET.into(),
                partition_id: pid,
                state: state.into(),
            })
            .await
            .expect("set state")
            .into_inner();
        assert!(r.ok, "set {state} must succeed: {}", r.error);
    }

    // 心跳:101 上报 pid1 ACTIVE(lag 12345);102/103 上报 pid1 ERROR
    // → pid1 健康副本=1 ≤ RF/2(majority down),且 lag 最大值为 12345。
    let replica_beat = |state: &str, lag: u64| antfs_master::pb::ReplicaBeat {
        partition_id: pids[0],
        disk_id: "d0".into(),
        current_state: state.into(),
        lag_bytes: lag,
        used_bytes: 0,
    };
    for (id, st) in [
        (101u64, replica_beat("ACTIVE", 12345)),
        (102, replica_beat("ERROR", 0)),
        (103, replica_beat("ERROR", 0)),
    ] {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, vec![st])]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }

    // 成功路径:2 次 assign + 2 次 lookup。
    for _ in 0..2 {
        let a = client
            .assign_partition(antfs_master::pb::AssignRequest { version: 1 })
            .await
            .expect("assign")
            .into_inner();
        assert!(a.ok, "assign must hit the RW partition: {}", a.error);
        assert_eq!(a.partition_id, pids[0]);
    }
    for pid in [pids[0], pids[1]] {
        client
            .lookup_partition(antfs_master::pb::LookupRequest {
                version: 1,
                partition_id: pid,
            })
            .await
            .expect("lookup");
    }

    // counter 在 handler 返回前已 inc,直接断言。
    let dump = antfs_metric::gather(&leader.registry).expect("gather");
    assert!(
        dump.contains("master_assign_requests_total 3"),
        "got:\n{dump}"
    );
    assert!(
        dump.contains("master_assign_failures_total 1"),
        "got:\n{dump}"
    );
    assert!(
        dump.contains("master_lookup_requests_total 3"),
        "got:\n{dump}"
    );

    // reconciler gauges:wait_until 轮询(禁 sleep);grace=0、周期 1s。
    let cap = 100u64 * 1024 * 1024 * 1024; // = cfg.partition_capacity_bytes
    let ok = wait_until(Duration::from_secs(20), || {
        let d = antfs_metric::gather(&leader.registry).unwrap_or_default();
        d.contains("master_partitions_rw 1")
            && d.contains("master_partitions_ro 1")
            && d.contains("master_partitions_sealed 1")
            && d.contains(&format!("master_writable_capacity_bytes {cap}"))
            && d.contains("master_nodes_up 3")
            && d.contains("master_cluster_free_bytes 2700000000000") // 3 × 900GB
            && d.contains("master_cluster_used_bytes 300000000000") // 3 × (1TB − 900GB)
            && d.contains("master_max_replica_lag_bytes 12345")
            && d.contains("master_majority_replicas_down_partitions 1")
    })
    .await;
    let dump = antfs_metric::gather(&leader.registry).expect("gather");
    assert!(ok, "reconciler gauges must converge, got:\n{dump}");
}

/// 起一个 3 节点 master 集群并等出 leader(真实 gRPC;`grace` = 调谐稳定期秒数)。
async fn start_masters(tmp: &std::path::Path, grace: u64) -> Vec<MasterNode> {
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let mut c = cfg(id, &dir, free_port(), free_port());
        c.reconcile_startup_grace_secs = grace;
        members.insert(id, BasicNode::new(c.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        nodes.push(node);
    }
    nodes[0].initialize(members).await.expect("initialize");
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("leader");
    }
    nodes
}

fn leader_of(nodes: &[MasterNode]) -> &MasterNode {
    let leader_id = nodes[0]
        .raft
        .metrics()
        .borrow()
        .current_leader
        .expect("leader elected");
    nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .expect("leader in set")
}

async fn connect(node: &MasterNode) -> MasterClient<tonic::transport::Channel> {
    MasterClient::connect(format!("http://{}", node.config.grpc_addr))
        .await
        .expect("connect")
}

async fn register_data_nodes(leader: &MasterNode, ids: &[(u64, &str)]) {
    for (id, rack) in ids {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: *id,
                info: data_node(rack),
            })
            .await
            .expect("register data node");
    }
}

/// 建一个 RF = `replicas.len()` 的分区,副本按给定节点顺序落在 `d0`。
async fn create_partition(leader: &MasterNode, replicas: &[u64]) -> u32 {
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: replicas.len() as u8,
            replicas: replicas
                .iter()
                .map(|n| ReplicaPlacement {
                    node_id: *n,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                })
                .collect(),
        })
        .await
        .expect("create partition");
    match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    }
}

/// 发一次心跳(单条消息的流:节点被标活一次,超时前一直算 up)并把节点标活。
async fn send_beat(
    client: &mut MasterClient<tonic::transport::Channel>,
    node_id: u64,
    replicas: Vec<antfs_master::pb::ReplicaBeat>,
) -> antfs_master::pb::MasterDirective {
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat(node_id, replicas)]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive")
}

fn replica(
    partition_id: u32,
    current_state: &str,
    used_bytes: u64,
) -> antfs_master::pb::ReplicaBeat {
    antfs_master::pb::ReplicaBeat {
        partition_id,
        disk_id: "d0".into(),
        current_state: current_state.into(),
        lag_bytes: 0,
        used_bytes,
    }
}

async fn set_maintenance(
    client: &mut MasterClient<tonic::transport::Channel>,
    paused: bool,
    pause_autogrow: bool,
) -> antfs_master::pb::SetMaintenanceResponse {
    client
        .set_maintenance(antfs_master::pb::SetMaintenanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            paused,
            pause_autogrow,
        })
        .await
        .expect("set_maintenance")
        .into_inner()
}

async fn set_partition_state(
    client: &mut MasterClient<tonic::transport::Channel>,
    partition_id: u32,
    state: &str,
) {
    let r = client
        .set_partition_state(antfs_master::pb::SetPartitionStateRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id,
            state: state.into(),
        })
        .await
        .expect("set state")
        .into_inner();
    assert!(r.ok, "set {state} must succeed: {}", r.error);
}

/// `ListPartitions`(offset + 状态过滤;limit=0 = 不限)。
async fn list_partitions(
    client: &mut MasterClient<tonic::transport::Channel>,
    state: &str,
    offset: u64,
) -> Result<antfs_master::pb::ListPartitionsResponse, tonic::Status> {
    client
        .list_partitions(antfs_master::pb::ListPartitionsRequest {
            version: 1,
            offset,
            limit: 0,
            state: state.into(),
        })
        .await
        .map(|r| r.into_inner())
}

/// 无 label 指标的值(未注册/未出现 = None)。
fn gauge(node: &MasterNode, name: &str) -> Option<i64> {
    let dump = antfs_metric::gather(&node.registry).ok()?;
    antfs_metric::parse(&dump)
        .into_iter()
        .find(|s| s.name == name && s.labels.is_empty())
        .map(|s| s.value as i64)
}

/// `master_replicas_by_disk{node_id,disk_id}` 的当前值。
fn replicas_on(node: &MasterNode, node_id: u64, disk_id: &str) -> Option<i64> {
    let dump = antfs_metric::gather(&node.registry).ok()?;
    let nid = node_id.to_string();
    antfs_metric::parse(&dump)
        .into_iter()
        .find(|s| {
            s.name == "master_replicas_by_disk"
                && s.labels.iter().any(|(k, v)| k == "node_id" && *v == nid)
                && s.labels.iter().any(|(k, v)| k == "disk_id" && v == disk_id)
        })
        .map(|s| s.value as i64)
}

fn placement(leader: &MasterNode, pid: u32) -> Vec<u64> {
    let g = leader.sm_data.read().expect("sm lock");
    let mut ids: Vec<u64> = g.state.partitions[&pid]
        .replicas
        .iter()
        .map(|r| r.node_id)
        .collect();
    ids.sort();
    ids
}

/// 维护模式(对标 Ceph noout/norecover):停自动动作、不停观测;由 `SetMaintenance` 开关。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_pauses_reconcile_until_disabled() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 0).await; // grace=0、周期 1s:立即调谐
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    // 先在"还没有分区"时开维护:此后 reconciler 不得执行任何自动动作(无先行动作的竞态)。
    let m = set_maintenance(&mut client, true, false).await;
    assert!(m.ok, "leader must accept set_maintenance: {}", m.error);
    assert!(
        m.leader_addr.is_empty(),
        "success response carries no redirect addr"
    );

    // 非 leader:follower 不落写,严格回 leader_addr 供 admin 重定向。
    let follower_id = nodes
        .iter()
        .map(|n| n.config.node_id)
        .find(|id| *id != leader.config.node_id)
        .expect("follower exists");
    let follower = nodes
        .iter()
        .find(|n| n.config.node_id == follower_id)
        .unwrap();
    let mut fclient = connect(follower).await;
    let f = set_maintenance(&mut fclient, false, false).await;
    assert!(
        !f.ok && f.error == "not leader",
        "got ok={} err={}",
        f.ok,
        f.error
    );
    assert_eq!(
        f.leader_addr, leader.config.raft_addr,
        "follower must redirect to the leader"
    );

    // 数据节点:101/102/104 心跳存活;103 从不心跳 → 失联(该分区欠副本)。
    register_data_nodes(
        leader,
        &[(101, "rA"), (102, "rB"), (103, "rC"), (104, "rD")],
    )
    .await;
    let pid = create_partition(leader, &[101, 102, 103]).await;
    for id in [101u64, 102] {
        send_beat(&mut client, id, vec![replica(pid, "ACTIVE", 1_000)]).await;
    }
    send_beat(&mut client, 104, vec![]).await;

    // 维护期观测照常:维护标志=1,且欠副本数=1(103 失联)仍被周期刷新。
    let observing = wait_until(Duration::from_secs(20), || {
        gauge(leader, "master_maintenance") == Some(1)
            && gauge(leader, "master_under_replicated_partitions") == Some(1)
    })
    .await;
    assert!(
        observing,
        "gauges must keep refreshing in maintenance, got:\n{}",
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );

    // 跨 ≥3 个 reconciler 周期(interval=1s)轮询"是否新增副本" → 始终不得新增。
    let added = wait_until(Duration::from_secs(3), || {
        let g = leader.sm_data.read().unwrap();
        g.state
            .partitions
            .get(&pid)
            .map(|p| p.replicas.iter().any(|r| r.node_id == 104))
            .unwrap_or(false)
    })
    .await;
    assert!(
        !added,
        "maintenance must pause re-replication (no replacement replica)"
    );
    assert_eq!(
        placement(leader, pid),
        vec![101, 102, 103],
        "placement untouched during maintenance"
    );
    assert_eq!(
        gauge(leader, "master_maintenance"),
        Some(1),
        "still in maintenance"
    );
    assert_eq!(
        gauge(leader, "master_under_replicated_partitions"),
        Some(1),
        "still under-replicated"
    );

    // 关闭维护 → 自动补副本收敛(104 是唯一存活候选),欠副本数回落 0。
    let m = set_maintenance(&mut client, false, false).await;
    assert!(m.ok, "disable must succeed: {}", m.error);
    let converged = wait_until(Duration::from_secs(20), || {
        placement(leader, pid).contains(&104)
            && gauge(leader, "master_maintenance") == Some(0)
            && gauge(leader, "master_under_replicated_partitions") == Some(0)
    })
    .await;
    assert!(
        converged,
        "reconcile must resume once maintenance is off; placement={:?}, dump:\n{}",
        placement(leader, pid),
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );
}

/// 分区实际占用:软状态 `used_bytes` → gauges(口径:每分区取副本最大值)+ `ListPartitions` 回填/过滤。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn used_bytes_gauges_and_list_partitions_state_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 0).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    // 105 注册但从不在任何分区上 → 其盘副本数 0(disk_skew 的 min 端)。
    register_data_nodes(
        leader,
        &[
            (101, "rA"),
            (102, "rB"),
            (103, "rC"),
            (104, "rD"),
            (105, "rE"),
        ],
    )
    .await;
    let pid_rw = create_partition(leader, &[101, 102, 103]).await;
    let pid_ro = create_partition(leader, &[101, 102, 103]).await;
    let pid_sealed = create_partition(leader, &[101, 102, 104]).await;
    set_partition_state(&mut client, pid_ro, "RO").await;
    set_partition_state(&mut client, pid_sealed, "SEALED").await;

    // 各副本上报实际占用:pid_rw 三副本 1000/3000/2000,pid_ro 500,pid_sealed 9999。
    send_beat(
        &mut client,
        101,
        vec![
            replica(pid_rw, "ACTIVE", 1_000),
            replica(pid_ro, "ACTIVE", 500),
        ],
    )
    .await;
    send_beat(
        &mut client,
        102,
        vec![
            replica(pid_rw, "ACTIVE", 3_000),
            replica(pid_ro, "ACTIVE", 500),
        ],
    )
    .await;
    send_beat(&mut client, 103, vec![replica(pid_rw, "ACTIVE", 2_000)]).await;
    send_beat(&mut client, 104, vec![replica(pid_sealed, "ACTIVE", 9_999)]).await;

    let cap = 100i64 * 1024 * 1024 * 1024; // = cfg.partition_capacity_bytes
                                           // 口径:可写已用 = 仅 RW 分区,每分区取各副本 used 最大值(3000,不是三副本之和 6000)再求和。
                                           // 放置:101×3、102×3、103×2、104×1、105×0 → skew = 3 − 0 = 3。
    let ok = wait_until(Duration::from_secs(20), || {
        gauge(leader, "master_partitions_rw") == Some(1)
            && gauge(leader, "master_writable_capacity_bytes") == Some(cap)
            && gauge(leader, "master_writable_used_bytes") == Some(3_000)
            && gauge(leader, "master_writable_free_bytes") == Some(cap - 3_000)
            && gauge(leader, "master_disk_skew") == Some(3)
            && replicas_on(leader, 101, "d0") == Some(3)
            && replicas_on(leader, 102, "d0") == Some(3)
            && replicas_on(leader, 103, "d0") == Some(2)
            && replicas_on(leader, 104, "d0") == Some(1)
            && replicas_on(leader, 105, "d0") == Some(0)
    })
    .await;
    assert!(
        ok,
        "used-bytes / per-disk gauges must converge, got:\n{}",
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );

    // ListPartitions:state 过滤 + 副本 used_bytes 回填。
    let rw = list_partitions(&mut client, "RW", 0)
        .await
        .expect("list RW");
    assert_eq!(rw.total, 1, "total counts only RW partitions");
    assert_eq!(rw.partitions.len(), 1);
    let entry = &rw.partitions[0];
    assert_eq!(entry.partition_id, pid_rw);
    let used = |node_id: u64| {
        entry
            .currents
            .iter()
            .find(|c| c.node_id == node_id)
            .expect("current")
            .used_bytes
    };
    assert_eq!(
        used(101),
        1_000,
        "current[101].used_bytes comes from the heartbeat"
    );
    assert_eq!(used(102), 3_000);
    assert_eq!(used(103), 2_000);

    // 过滤先于分页:total 是过滤后总数,offset 作用在过滤后的集合上(越界即空页)。
    let rw_page2 = list_partitions(&mut client, "RW", 1)
        .await
        .expect("list RW page2");
    assert_eq!(
        rw_page2.total, 1,
        "total is the filtered count, offset applies after filtering"
    );
    assert!(
        rw_page2.partitions.is_empty(),
        "offset past the filtered set yields an empty page"
    );

    let ro = list_partitions(&mut client, "RO", 0)
        .await
        .expect("list RO");
    assert_eq!((ro.total, ro.partitions[0].partition_id), (1, pid_ro));
    let sealed = list_partitions(&mut client, "SEALED", 0)
        .await
        .expect("list SEALED");
    assert_eq!(
        (sealed.total, sealed.partitions[0].partition_id),
        (1, pid_sealed)
    );
    assert_eq!(
        list_partitions(&mut client, "", 0)
            .await
            .expect("list all")
            .total,
        3,
        "empty state = no filter"
    );
    assert_eq!(
        list_partitions(&mut client, "BOGUS", 0)
            .await
            .expect_err("invalid state")
            .code(),
        tonic::Code::InvalidArgument,
        "invalid state must be rejected, not treated as 'all'"
    );
}

/// A5:盘健康上报 → `ListNodes` 回填 + 选盘避让(不健康盘不放新副本)+ `master_unhealthy_disks`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unhealthy_disk_is_reported_and_avoided_by_placement() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 0).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader, &[(101, "rA"), (102, "rB"), (103, "rC")]).await;

    // 101 的盘主动探测报 WRITE_TIMEOUT(容量/余量都正常)→ 该盘不得再放新副本。
    send_beat(&mut client, 101, vec![]).await; // 先活一次(下面的 beat_disk 覆盖盘健康)
    {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat_disk(
                101,
                vec![],
                DISK_FREE,
                "WRITE_TIMEOUT",
            )]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    for id in [102u64, 103] {
        send_beat(&mut client, id, vec![]).await;
    }

    // ListNodes 回填盘健康:101 有值(修掉恒空串),102 保持空串(未启用探测 ≠ 不健康)。
    let list = client
        .list_nodes(antfs_master::pb::ListNodesRequest { version: 1 })
        .await
        .expect("list_nodes")
        .into_inner();
    let health_of = |node_id: u64| {
        list.nodes
            .iter()
            .find(|n| n.node_id == node_id)
            .expect("node listed")
            .disks[0]
            .health
            .clone()
    };
    assert_eq!(
        health_of(101),
        "WRITE_TIMEOUT",
        "probe result must be surfaced in ListNodes"
    );
    assert_eq!(
        health_of(102),
        "",
        "probe disabled must stay empty, not be invented"
    );

    // 指标打在实际接入的 Registry 上(CLAUDE.md §6):恰有 1 块不健康盘(空串不计)。
    let counted = wait_until(Duration::from_secs(20), || {
        gauge(leader, "master_unhealthy_disks") == Some(1)
    })
    .await;
    assert!(
        counted,
        "master_unhealthy_disks must converge to 1, dump:\n{}",
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );

    // 选盘避让:RF=2 建分区 → 候选只剩 102/103(101 的盘不健康)。
    let r = client
        .create_partitions(antfs_master::pb::CreatePartitionsRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            count: 1,
            replication_factor: 2,
        })
        .await
        .expect("create_partitions")
        .into_inner();
    assert!(r.ok, "create must succeed: {}", r.error);
    let pid = r.partition_ids[0];
    assert_eq!(
        placement(leader, pid),
        vec![102, 103],
        "unhealthy disk (even with plenty of space) must not host a new replica"
    );
}

/// A4:副本所在盘将满 → 分区自动转 RO(**只置不回**);用量降回来也不自动恢复 RW。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn near_full_disk_forces_partition_read_only_without_auto_revert() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 0).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader, &[(101, "rA"), (102, "rB"), (103, "rC")]).await;
    let pid = create_partition(leader, &[101, 102, 103]).await;

    // 101 的 d0 只余 1%(用量 99% > disk_near_full_ratio=0.95)→ 其上的 RW 分区必须转 RO。
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat_disk(
            101,
            vec![replica(pid, "ACTIVE", 1_000)],
            DISK_CAPACITY / 100,
            "",
        )]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive");
    for id in [102u64, 103] {
        send_beat(&mut client, id, vec![replica(pid, "ACTIVE", 1_000)]).await;
    }

    let sm = leader.sm_data.clone();
    let state_of = |pid: u32| {
        sm.read()
            .expect("sm")
            .state
            .partitions
            .get(&pid)
            .map(|p| p.state)
    };
    let forced = wait_until(Duration::from_secs(20), || {
        state_of(pid) == Some(antfs_master::types::PartitionState::ReadOnly)
    })
    .await;
    assert!(
        forced,
        "a near-full replica disk must force the partition to ReadOnly; state={:?}",
        state_of(pid)
    );
    let counted = wait_until(Duration::from_secs(20), || {
        gauge(leader, "master_partitions_forced_ro_total") == Some(1)
            && gauge(leader, "master_partitions_ro") == Some(1)
            && gauge(leader, "master_partitions_rw") == Some(0)
    })
    .await;
    assert!(
        counted,
        "forced-RO must be counted once and reflected in the gauges, dump:\n{}",
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );

    // 空间释放(101 盘回到 90% 余量)+ 跨 ≥3 个调谐周期 → **不自动回 RW**(只置不回)。
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat_disk(
            101,
            vec![replica(pid, "ACTIVE", 1_000)],
            DISK_FREE,
            "",
        )]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive");
    let reverted = wait_until(Duration::from_secs(3), || {
        state_of(pid) == Some(antfs_master::types::PartitionState::ReadWrite)
    })
    .await;
    assert!(
        !reverted,
        "space freed must NOT auto-revert RO → RW (运维手工恢复)"
    );
    assert_eq!(
        gauge(leader, "master_partitions_forced_ro_total"),
        Some(1),
        "still set only once"
    );

    // 恢复走运维路径(现有的 SetPartitionState),且用量已正常 → 不会被再次自动转 RO。
    set_partition_state(&mut client, pid, "RW").await;
    assert_eq!(
        state_of(pid),
        Some(antfs_master::types::PartitionState::ReadWrite)
    );
    let re_forced = wait_until(Duration::from_secs(3), || {
        state_of(pid) == Some(antfs_master::types::PartitionState::ReadOnly)
    })
    .await;
    assert!(
        !re_forced,
        "manual RW restore with a healthy disk must stick"
    );
}

/// A6:DROPPED 的确认闸门。宽限期关掉 reconciler(只验证心跳路径的判定)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_requires_successor_active_and_keeps_retrying() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 3600).await; // 关掉自动调谐,中间态确定
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(
        leader,
        &[(101, "rA"), (102, "rB"), (103, "rC"), (104, "rD")],
    )
    .await;
    let pid = create_partition(leader, &[101, 102, 103]).await;
    // 101/103 存活并上报副本;102 **从不心跳**(= 失联);104 存活但不持有该分区。
    for id in [101u64, 103] {
        send_beat(&mut client, id, vec![replica(pid, "ACTIVE", 1_000)]).await;
    }
    send_beat(&mut client, 104, vec![]).await;

    // 迁移 103 → 104(手动 ops,不受维护/宽限影响)。
    let m = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
            from_node_id: 103,
            to_node_id: 104,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(m.ok, "move must succeed: {}", m.error);
    assert_eq!(m.to_node_id, 104);
    let targets = {
        let g = leader.sm_data.read().expect("sm");
        g.state.move_target_of(pid, 103)
    };
    assert_eq!(
        targets,
        Some(104),
        "successor must be recorded in move_targets"
    );

    // 模拟 reconciler 的"后删"已提交(本测试只验证下发闸门)。
    leader
        .raft
        .client_write(MasterCommand::RemoveReplica {
            partition_id: pid,
            node_id: 103,
        })
        .await
        .expect("remove source");
    let pending = |pid: u32| {
        leader
            .sm_data
            .read()
            .expect("sm")
            .state
            .pending_drop
            .contains(&(pid, 103))
    };
    assert!(pending(pid), "RemoveReplica must record a pending drop");

    let dropped_of = |d: &antfs_master::pb::MasterDirective| {
        d.transitions
            .iter()
            .filter(|t| t.target_state == "DROPPED")
            .cloned()
            .collect::<Vec<_>>()
    };

    // ① 继任者尚未 ACTIVE(104 仍 BOOTSTRAP)→ 源仍上报 → **不下发** DROPPED。
    send_beat(&mut client, 104, vec![replica(pid, "BOOTSTRAP", 0)]).await;
    let d = send_beat(&mut client, 103, vec![replica(pid, "ACTIVE", 1_000)]).await;
    assert!(
        dropped_of(&d).is_empty(),
        "successor not ACTIVE yet: must not drop the source"
    );
    assert!(pending(pid), "pending drop must be kept for the next round");

    // ② 继任者 ACTIVE → 下发 DROPPED(带源节点盘号);此时副本 102 失联也**不拖延**删除。
    send_beat(&mut client, 104, vec![replica(pid, "ACTIVE", 0)]).await;
    let d = send_beat(&mut client, 103, vec![replica(pid, "ACTIVE", 1_000)]).await;
    let dropped = dropped_of(&d);
    assert_eq!(
        dropped.len(),
        1,
        "successor ACTIVE + source removed ⇒ exactly one DROPPED"
    );
    assert_eq!(dropped[0].partition_id, pid);
    assert_eq!(
        dropped[0].disk_id, "d0",
        "DROPPED must target the disk the source holds"
    );
    assert_eq!(
        dropped[0].target_state, "DROPPED",
        "transition target_state carries the new DROPPED value (proto unchanged)"
    );
    assert!(
        antfs_metric::gather(&leader.registry)
            .unwrap_or_default()
            .contains("master_dropped_directives_total 1"),
        "dropped directive must be counted"
    );

    // 幂等重发:store 还没删完(仍上报该 pid)→ 每轮心跳继续下发。
    let d = send_beat(&mut client, 103, vec![replica(pid, "ACTIVE", 1_000)]).await;
    assert_eq!(
        dropped_of(&d).len(),
        1,
        "must keep re-sending until the source stops reporting"
    );

    // ③ 源不再上报该 pid → 不再下发(记账清理由 reconciler 负责,见下一个测试)。
    let d = send_beat(&mut client, 103, vec![]).await;
    assert!(
        dropped_of(&d).is_empty(),
        "source no longer reports the replica: nothing to drop"
    );
}

/// A6 全链路:迁移完成 → reconciler 后删 + 心跳下发 DROPPED → 源不再上报后清 `pending_drop`。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_completion_drops_source_dir_and_clears_pending_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 0).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(
        leader,
        &[(101, "rA"), (102, "rB"), (103, "rC"), (104, "rD")],
    )
    .await;
    let pid = create_partition(leader, &[101, 102, 103]).await;
    // 原 3 副本都上报 ACTIVE(避免被误判欠副本而触发再复制),104 存活但不持有该分区。
    for id in [101u64, 102, 103] {
        send_beat(&mut client, id, vec![replica(pid, "ACTIVE", 1_000)]).await;
    }
    send_beat(&mut client, 104, vec![]).await;

    let m = client
        .move_replica(antfs_master::pb::MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pid,
            from_node_id: 103,
            to_node_id: 104,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(m.ok, "move must succeed: {}", m.error);

    // 继任者 104 尚未上报该分区(未 ACTIVE)→ 后删闸门必须拦住源,跨 ≥3 个调谐周期不动(§3.1 真·先补后删)。
    let removed_early = wait_until(Duration::from_secs(3), || {
        !placement(leader, pid).contains(&103)
    })
    .await;
    assert!(
        !removed_early,
        "successor not ACTIVE yet: the source must survive"
    );
    assert!(
        placement(leader, pid).contains(&103),
        "source still in placement, data not dropped"
    );

    // 继任者 104 追平(上报 ACTIVE)→ reconciler 的后删闸门(目标 ACTIVE + 其余 up ≥ RF)才放行。
    send_beat(&mut client, 104, vec![replica(pid, "ACTIVE", 0)]).await;

    let removed = wait_until(Duration::from_secs(20), || {
        !placement(leader, pid).contains(&103)
    })
    .await;
    assert!(
        removed,
        "reconciler must remove the source once the successor is ACTIVE; placement={:?}",
        placement(leader, pid)
    );
    let recorded = wait_until(Duration::from_secs(20), || {
        leader
            .sm_data
            .read()
            .expect("sm")
            .state
            .pending_drop
            .contains(&(pid, 103))
    })
    .await;
    assert!(
        recorded,
        "the removal must be recorded as a pending drop (source dir still there)"
    );
    let pending_gauge = wait_until(Duration::from_secs(20), || {
        gauge(leader, "master_pending_drops") == Some(1)
    })
    .await;
    assert!(
        pending_gauge,
        "master_pending_drops must reflect the待删 pair"
    );

    // 源 103 心跳仍上报该副本 → 下发 DROPPED(带盘号),store 据此删本地副本/目录。
    let d = send_beat(&mut client, 103, vec![replica(pid, "ACTIVE", 1_000)]).await;
    let dropped: Vec<_> = d
        .transitions
        .iter()
        .filter(|t| t.target_state == "DROPPED")
        .collect();
    assert_eq!(
        dropped.len(),
        1,
        "source still holds it: DROPPED must be sent"
    );
    assert_eq!(
        (dropped[0].partition_id, dropped[0].disk_id.as_str()),
        (pid, "d0")
    );

    // store 删完 → 源心跳不再上报该 pid → reconciler 清记账(pending_drop 空、gauge 归零)。
    let d = send_beat(&mut client, 103, vec![]).await;
    assert!(
        d.transitions.iter().all(|t| t.target_state != "DROPPED"),
        "source no longer reports the replica: no more DROPPED"
    );
    let cleared = wait_until(Duration::from_secs(20), || {
        leader
            .sm_data
            .read()
            .expect("sm")
            .state
            .pending_drop
            .is_empty()
            && gauge(leader, "master_pending_drops") == Some(0)
    })
    .await;
    assert!(
        cleared,
        "pending_drop must be cleared once the source stops reporting it, dump:\n{}",
        antfs_metric::gather(&leader.registry).unwrap_or_default()
    );
    let targets_cleared = {
        let g = leader.sm_data.read().expect("sm");
        g.state.move_target_of(pid, 103)
    };
    assert_eq!(
        targets_cleared, None,
        "confirmed drop must also clear the successor bookkeeping"
    );
}

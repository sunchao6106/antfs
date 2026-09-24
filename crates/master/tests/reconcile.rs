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
        // 其余配置项走默认值:本用例只关心上面那几项,新增配置项不该让每个测试文件都跟着改一遍。
        ..Default::default()
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
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
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
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
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

    // 全部 4 个数据节点心跳保持存活(含被下线的 103),并**各自如实上报自己的副本为 `ACTIVE`**。
    //
    // 为什么必须报 ACTIVE:删源闸门已统一 —— 两条 evacuation 路径都要求"数得上的每一份**已追平**"
    // 才允许删源(否则会把还在 `BOOTSTRAP` 的那份算成已有数据,可能删掉盘上唯一一份可用数据)。
    // 上线的 `DecommissionNode` 在真实集群里也是这么工作的:store 会持续上报 `current_state`。
    let grpc = leader.config.grpc_addr.clone();
    let keepalive = tokio::spawn(async move {
        let mut c = MasterClient::connect(format!("http://{grpc}"))
            .await
            .expect("connect");
        for _ in 0..40 {
            for id in [101u64, 102, 104] {
                let replicas = vec![antfs_master::pb::ReplicaBeat {
                    partition_id: pid,
                    disk_id: "d0".into(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 100,
                }];
                if let Ok(resp) = c
                    .heartbeat(tokio_stream::iter(vec![beat(id, replicas)]))
                    .await
                {
                    let _ = resp.into_inner().message().await;
                }
            }
            // 103 也要保持"存活"(它在下线中,不应因为心跳缺失而被判 down —— 那会让测试偏离主题)。
            if let Ok(resp) = c
                .heartbeat(tokio_stream::iter(vec![beat(103, vec![])]))
                .await
            {
                let _ = resp.into_inner().message().await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    });

    // 下线 103:evacuation 应先在 104 补副本,再移除 103(等 104 报 ACTIVE 之后)。
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
    keepalive.abort();
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
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
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
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
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
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 102,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
                },
                ReplicaPlacement {
                    node_id: 103,
                    disk_id: "d0".into(),
                    target_state: "ACTIVE".into(),
                    incarnation: 0,
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
                        incarnation: 0,
                    },
                    ReplicaPlacement {
                        node_id: 102,
                        disk_id: "d0".into(),
                        target_state: "ACTIVE".into(),
                        incarnation: 0,
                    },
                    ReplicaPlacement {
                        node_id: 103,
                        disk_id: "d0".into(),
                        target_state: "ACTIVE".into(),
                        incarnation: 0,
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
                    incarnation: 0,
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

/// 分区可写态随心跳指令下发:store 侧靠它挑本地 compaction 候选集
/// (`crates/store/src/worker.rs`),两侧字段/消费点任一处对不上,`SEALED` 会被照压、
/// `RO` 又压不到 —— 而各自单测看不出这个接缝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_directive_carries_partition_states() {
    use antfs_master::pb::PartitionStateInfo;
    use antfs_master::types::PartitionState;

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

    // 注册一个数据节点并显式建 1 个 RF=1 分区(副本落在它上面)。
    leader
        .raft
        .client_write(MasterCommand::RegisterNode {
            node_id: 101,
            info: data_node("rA"),
        })
        .await
        .expect("register data node");
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 1,
            replicas: vec![ReplicaPlacement {
                node_id: 101,
                disk_id: "d0".into(),
                target_state: "ACTIVE".into(),
                incarnation: 0,
            }],
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

    // 新分区默认 RW:该节点上报承载副本 → 指令里必须带 (pid, RW)。
    let d = send_beat(&mut client, 101, vec![replica(pid, "ACTIVE", 0)]).await;
    let got = |d: &antfs_master::pb::MasterDirective| {
        d.partition_states
            .iter()
            .find(|s: &&PartitionStateInfo| s.partition_id == pid)
            .map(|s| s.state.clone())
    };
    assert_eq!(
        got(&d).as_deref(),
        Some("RW"),
        "本节点承载的分区状态应随指令下发: {:?}",
        d.partition_states
    );

    // 置 RO 后再发一拍 → 变成 RO(store 侧据此把它当回收候选)。
    leader
        .raft
        .client_write(MasterCommand::SetPartitionState {
            partition_id: pid,
            state: PartitionState::ReadOnly,
        })
        .await
        .expect("set RO");
    let d = send_beat(&mut client, 101, vec![replica(pid, "ACTIVE", 0)]).await;
    assert_eq!(got(&d).as_deref(), Some("RO"));

    // 该节点没上报的分区不下发(避免把"节点上没有的分区"塞进指令)。
    let d = send_beat(&mut client, 101, Vec::new()).await;
    assert!(
        d.partition_states.is_empty(),
        "无副本上报时不应下发任何分区状态"
    );
}

// ── D5 判活迟滞 / D6 再复制每 tick 预算(端到端:真 3 节点 master + 真心跳流)─────────────
//
// 这两条都是"防误判引发风暴"的护栏,而它们的**价值只在真链路里能看出来**:单元测试证明函数算得对,
// 这里证明**它被接上了**(配置 → reconcile loop → 实际不再一 tick 全量搬数据)。

/// D5 的配置:心跳超时与调谐周期都压到 1s、迟滞 3 拍、免稳定期 —— 让"过期 → 计数 → 判死"在几秒内可观测。
fn cfg_fast_liveness(id: u64, dir: &std::path::Path, raft: u16, grpc: u16) -> MasterConfig {
    MasterConfig {
        node_id: id,
        raft_addr: format!("127.0.0.1:{raft}"),
        grpc_addr: format!("127.0.0.1:{grpc}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1, 2, 3],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 1,
        reconcile_interval_secs: 1,
        reconcile_startup_grace_secs: 0,
        node_down_debounce_ticks: 3,
        ..Default::default()
    }
}

/// 起 3 节点 master、选出 leader、注册 4 个数据节点(n1..n3 + 一个备用 104),返回 (nodes, leader_id)。
async fn cluster_with_spare(tmp: &std::path::Path) -> Vec<MasterNode> {
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg_fast_liveness(id, &dir, free_port(), free_port());
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
    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: data_node(rack),
            })
            .await
            .expect("register data node");
    }
    nodes
}

/// 建一个副本落在 `on` 上的 RF=1 分区,返回 pid。
async fn create_partition_on(leader: &MasterNode, on: &[u64]) -> u32 {
    let replicas = on
        .iter()
        .map(|n| ReplicaPlacement {
            node_id: *n,
            disk_id: "d0".into(),
            target_state: "ACTIVE".into(),
            incarnation: 0,
        })
        .collect();
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: on.len() as u8,
            replicas,
        })
        .await
        .expect("create partition");
    match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    }
}

/// **D5:一次心跳抖动不该判节点死**(而持续静默**仍要**判死)。
///
/// 场景:RF=1 分区在 103 上;103 上报一次心跳之后**停止上报**(模拟一次 GC/调度停顿 / `statvfs` 卡顿);
/// 101、102 照常心跳(保证集群里有别的存活节点可作备选)。
///
/// 断言分两半,缺一不可:
/// 1. **早段**(节拍不到 3):`master_node_down_total == 0` 且分区**没有**被补到 104 ——
///    单窗口判活这时候早就判死了,迟滞必须挡住它;
/// 2. **晚段**(20s 内):连击达标 ⇒ 判死**确实发生**(`node_down == 1`)且副本补到 104 ——
///    "不再把抖动当死亡"不等于"永远不判死",真死了照样 30s 内被发现。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn liveness_debounce_absorbs_a_single_missed_beat_but_still_detects_real_death() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = cluster_with_spare(tmp.path()).await;
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();
    // RF=3 落在 101/102/103 ⇒ 103 一死,`try_add_replica` 的 exclude 就是 {101,102,103},
    // 唯一备选只剩 104 —— 断言因此是**确定**的(不用猜 placement 在平票时选谁)。
    let pid = create_partition_on(leader, &[101, 102, 103]).await;

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");

    // 三个节点各上报一次(让它们在软状态里"在册",并如实报出各自的副本);此后只有 101/102 继续上报。
    for id in [101u64, 102, 103] {
        let replicas = vec![antfs_master::pb::ReplicaBeat {
            partition_id: pid,
            disk_id: "d0".into(),
            current_state: "ACTIVE".into(),
            lag_bytes: 0,
            used_bytes: 100,
        }];
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, replicas)]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    // 104 也必须上报一次:选盘只看**软状态**里的盘余量,一次没报过的节点是"未上报 ⇒ 不可用"。
    // (这不是测试技巧,而是真实约束:备选节点得先活着报到,才有资格被补副本。)
    {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(104, vec![])]))
            .await
            .expect("hb104")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    let last_103_beat = Instant::now();

    // 101/102 持续心跳(在后台跑,保证它们不会跟着一起过期)。
    // 先把地址拷成 owned String —— `spawn` 要求 `'static`,不能借住 `nodes`。
    let grpc_addr = leader.config.grpc_addr.clone();
    let keepalive = tokio::spawn(async move {
        let mut c = MasterClient::connect(format!("http://{grpc_addr}"))
            .await
            .expect("connect");
        for _ in 0..12 {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            // 104 必须**持续**心跳:它既要做备选,就要一直是"存活健康盘",
            // 否则 1s 超时一到它自己就被排除出候选(与要修的"节点判死"是同一把尺子)。
            for id in [101u64, 102, 104] {
                if let Ok(resp) = c
                    .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
                    .await
                {
                    let _ = resp.into_inner().message().await;
                }
            }
        }
    });

    // ① 早段:等到"103 已过期但连击还没到 3 拍"的时刻。心跳超时 1s + 调谐周期 1s ⇒
    //    最坏连击进度 = floor((elapsed - 1s) / 1s);在 2.5s 时最多 1 拍,**远不到 3**。
    while last_103_beat.elapsed() < Duration::from_millis(2_500) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        gauge(leader, "master_node_down_total").unwrap_or(0),
        0,
        "抖一拍就被判死 = 迟滞没接上(单窗口判活这时候早就判死了)"
    );
    assert!(
        !placement(leader, pid).contains(&104),
        "还没判死就不该开始搬数据;placement={:?}",
        placement(leader, pid)
    );

    // ② 晚段:连击达标 ⇒ 判死发生 + 副本补到 104(证明"迟滞只是延后,不是永不")。
    let sm = leader.sm_data.clone();
    let grown = wait_until(Duration::from_secs(25), || {
        let g = sm.read().unwrap();
        g.state
            .partitions
            .get(&pid)
            .map(|p| p.replicas.iter().any(|r| r.node_id == 104))
            .unwrap_or(false)
    })
    .await;
    assert!(
        grown,
        "持续静默最终必须被发现并补副本;placement={:?}",
        placement(leader, pid)
    );
    assert_eq!(
        gauge(leader, "master_node_down_total").unwrap_or(0),
        1,
        "判死应恰好计一次(持续 down 不每拍重复计)"
    );

    keepalive.abort();
}

/// **D6:一个节点判死不能让它的全部分区在同一拍开始搬数据** —— 每 tick 只放行 K 个,
/// 但**最终**全部补齐(限速,不是停摆)。
///
/// 这条的失败形态正是"集群动荡":几万个分区同时开始搬 GB 级数据,把网络与盘写瞬间打满。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rereplication_budget_throttles_a_node_death_but_never_stalls() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = cluster_with_spare(tmp.path()).await;
    let leader_id = nodes[0].raft.metrics().borrow().current_leader.unwrap();
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .unwrap();

    // 6 个 RF=3 分区,副本统一落 101/102/103 ⇒ 103 一死,6 个分区都欠 1 个副本,
    // 而**唯一**备选是 104(确定),且 6 > 默认每 tick 预算 4 ⇒ 必然触发限速。
    let pids: Vec<u32> = {
        let mut v = Vec::new();
        for _ in 0..6 {
            v.push(create_partition_on(leader, &[101, 102, 103]).await);
        }
        v
    };

    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect");
    // 三个节点各上报一次(在册 + 如实报副本);此后 103 静止,101/102 保持心跳。
    for id in [101u64, 102, 103] {
        let replicas = pids
            .iter()
            .map(|pid| antfs_master::pb::ReplicaBeat {
                partition_id: *pid,
                disk_id: "d0".into(),
                current_state: "ACTIVE".into(),
                lag_bytes: 0,
                used_bytes: 10,
            })
            .collect();
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(id, replicas)]))
            .await
            .expect("hb")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    // 同上:104 先报一次盘余量,否则它不在候选里。
    {
        let mut hb = client
            .heartbeat(tokio_stream::iter(vec![beat(104, vec![])]))
            .await
            .expect("hb104")
            .into_inner();
        hb.message().await.expect("recv").expect("directive");
    }
    let grpc_addr = leader.config.grpc_addr.clone();
    let keepalive = tokio::spawn(async move {
        let mut c = MasterClient::connect(format!("http://{grpc_addr}"))
            .await
            .expect("connect");
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            // 104 必须**持续**心跳:它既要做备选,就要一直是"存活健康盘",
            // 否则 1s 超时一到它自己就被排除出候选(与要修的"节点判死"是同一把尺子)。
            for id in [101u64, 102, 104] {
                if let Ok(resp) = c
                    .heartbeat(tokio_stream::iter(vec![beat(id, vec![])]))
                    .await
                {
                    let _ = resp.into_inner().message().await;
                }
            }
        }
    });

    let sm = leader.sm_data.clone();
    // ① 限速可见:迟早会观测到"有分区被顺延到下一拍"。
    let throttled = wait_until(Duration::from_secs(25), || {
        gauge(leader, "master_rereplication_deferred").unwrap_or(0) > 0
    })
    .await;
    assert!(
        throttled,
        "6 个欠副本分区 > 每 tick 预算 4 ⇒ 限速必须可观测(否则运维看到的是'系统不动了')"
    );

    // ② 但绝不停摆:最终 6 个分区**全部**补齐到 104。
    let all_grown = wait_until(Duration::from_secs(30), || {
        let g = sm.read().unwrap();
        pids.iter().all(|pid| {
            g.state
                .partitions
                .get(pid)
                .map(|p| p.replicas.iter().any(|r| r.node_id == 104))
                .unwrap_or(false)
        })
    })
    .await;
    let g = sm.read().unwrap();
    let pending: Vec<u32> = pids
        .iter()
        .copied()
        .filter(|pid| {
            !g.state.partitions[pid]
                .replicas
                .iter()
                .any(|r| r.node_id == 104)
        })
        .collect();
    assert!(
        all_grown,
        "被顺延的分区必须在下几拍补上(限速 ≠ 停摆);仍缺: {pending:?}"
    );

    keepalive.abort();
}

/// **D4 坏盘撤离全链路**(加固需求 8.3):坏盘 → 先补副本 → **继任者追平(ACTIVE)后**才删源;
/// 断言两步之间**源副本一直还在**(任意时刻可用副本数不下降)。
///
/// ## 为什么这条断言最值钱
///
/// "先补后删"如果只按"存活副本数 ≥ RF"判,就会把**还在追平的那份**算成已有数据 ——
/// 于是删掉源盘上唯一一份可用数据 ⇒ **丢数据**(CLAUDE.md §3.1)。故本用例在"新副本已就位、
/// 但还没报 ACTIVE"的窗口里**反复断言源仍在**,把那道闸门钉死。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bad_disk_evacuation_copies_first_and_removes_only_after_successor_is_active() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = MasterConfig {
            node_id: id,
            raft_addr: format!("127.0.0.1:{}", free_port()),
            grpc_addr: format!("127.0.0.1:{}", free_port()),
            data_dir: dir.to_string_lossy().into_owned(),
            initial_members: vec![1, 2, 3],
            cluster_secret: SECRET.to_string(),
            heartbeat_timeout_secs: 5,
            reconcile_interval_secs: 1,
            reconcile_startup_grace_secs: 0,
            node_down_debounce_ticks: 3,
            // D4:显式打开自动撤离,并把"持续证据"压到 1s 便于观察。
            disk_evac_enabled: true,
            disk_evac_min_bad_secs: 1,
            disk_evac_max_in_flight: 1,
            ..Default::default()
        };
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
    // RF=2 落在 (101,d0)/(102,d0):(101,d0) 判坏后,候选只有 103/104(平票取小 ⇒ 103)。
    let pid = create_partition_on(leader, &[101, 102]).await;
    let grpc = leader.config.grpc_addr.clone();

    let sm = leader.sm_data.clone();

    // 该分区当前硬放置的节点集(**直接从被测 leader 的状态机读**,不经 API 加工)。
    let placement_now = |sm: &std::sync::Arc<
        std::sync::RwLock<antfs_master::store::StateMachineData>,
    >|
     -> Vec<u64> {
        let g = sm.read().expect("sm");
        let mut ids: Vec<u64> = g.state.partitions[&pid]
            .replicas
            .iter()
            .map(|r| r.node_id)
            .collect();
        ids.sort();
        ids
    };

    // `master_disk_evac_planned_total{result="planned"}` 的当前值。阶段 ⑤ 用它证明"盘恢复不再产生计划"。
    let planned_now = || -> f64 {
        antfs_metric::gather(&leader.registry)
            .ok()
            .and_then(|d| {
                antfs_metric::parse(&d)
                    .into_iter()
                    .find(|s| {
                        s.name == "master_disk_evac_planned_total"
                            && s.labels
                                .iter()
                                .any(|(k, v)| k == "result" && *v == "planned")
                    })
                    .map(|s| s.value)
            })
            .unwrap_or(0.0)
    };

    // 继任者是否已"追平"由测试控制:置位之后 103 才开始上报 ACTIVE。
    let successor_active = Arc::new(AtomicBool::new(false));
    let flag = successor_active.clone();
    // 101 的盘是否已"恢复"(只有阶段 ⑤ 才置位):在那之前它持续报 UNAVAILABLE。
    let disk_101_healthy = Arc::new(AtomicBool::new(false));
    let healthy101 = disk_101_healthy.clone();
    let keepalive = tokio::spawn(async move {
        let mut c = MasterClient::connect(format!("http://{grpc}"))
            .await
            .expect("connect");
        // 80 × 400ms ≈ 32s:够本用例所有阶段跑完(阶段变多时别把它压到比用例还短,否则心跳一停,
        // 软状态过期会把节点判死,后面的断言就会看到"计划外的迁移")。
        for _ in 0..80 {
            for id in [101u64, 102, 103, 104] {
                // 101 的盘**持续报 UNAVAILABLE**(直到阶段 ⑤ 把它改回健康);其余节点盘健康。
                let health = if id == 101 && !healthy101.load(Ordering::Relaxed) {
                    "UNAVAILABLE"
                } else {
                    ""
                };
                let mut replicas = Vec::new();
                if id == 101 || id == 102 {
                    replicas.push(antfs_master::pb::ReplicaBeat {
                        partition_id: pid,
                        disk_id: "d0".into(),
                        current_state: "ACTIVE".into(),
                        lag_bytes: 0,
                        used_bytes: 100,
                    });
                }
                if id == 103 && flag.load(Ordering::Relaxed) {
                    // 继任者补上来了:先报 BOOTSTRAP,等下面置位后才报 ACTIVE。
                    replicas.push(antfs_master::pb::ReplicaBeat {
                        partition_id: pid,
                        disk_id: "d0".into(),
                        current_state: "ACTIVE".into(),
                        lag_bytes: 0,
                        used_bytes: 100,
                    });
                }
                if let Ok(resp) = c
                    .heartbeat(tokio_stream::iter(vec![beat_disk(
                        id, replicas, DISK_FREE, health,
                    )]))
                    .await
                {
                    let _ = resp.into_inner().message().await;
                }
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    });

    // ① 先补:坏盘撤离应把副本补到 103。
    let sm1 = sm.clone();
    let added = wait_until(Duration::from_secs(25), || {
        placement_now(&sm1).contains(&103)
    })
    .await;
    assert!(
        added,
        "坏盘上的副本应被补到健康盘;placement={:?}",
        placement_now(&sm)
    );

    // ② **先补后删**:继任者还没报 ACTIVE 的这段时间里,源(101)必须**一直在**。
    //    跑够 ≥3 个调谐周期,给 reconciler 充分的"犯错机会"。
    for round in 0..4 {
        assert!(
            placement_now(&sm).contains(&101),
            "第 {round} 轮:继任者尚未 ACTIVE,源副本绝不能被删(删了就丢数据);placement={:?}",
            placement_now(&sm)
        );
        tokio::time::sleep(Duration::from_millis(700)).await;
    }
    assert_eq!(
        gauge(leader, "master_disk_evacuating").unwrap_or(0),
        1,
        "在途撤离数应如实暴露(受 disk_evac_max_in_flight 约束)"
    );
    let planned = planned_now();
    assert!(planned >= 1.0, "撤离计划应计数(planned)");

    // ③ 继任者追平 ⇒ 现在才允许删源。
    successor_active.store(true, Ordering::Relaxed);
    let sm2 = sm.clone();
    let removed = wait_until(Duration::from_secs(25), || {
        let ids = placement_now(&sm2);
        ids.contains(&103) && !ids.contains(&101)
    })
    .await;
    assert!(
        removed,
        "继任者 ACTIVE 之后才该删源;placement={:?}",
        placement_now(&sm)
    );
    // ④ 最终仍保有 RF=2 份可用副本(先补后删的可观测保证)。
    assert_eq!(
        placement_now(&sm),
        vec![102, 103],
        "最终放置:坏盘上的副本已搬走,冗余仍是 2"
    );
    // 在途 gauge 比硬放置**晚一拍**收敛(它在本 tick 的动作之前刷新),故这里是**有界等待**而不是立即断言。
    let drained = wait_until(Duration::from_secs(10), || {
        gauge(leader, "master_disk_evacuating") == Some(0)
    })
    .await;
    assert!(
        drained,
        "撤离走完后在途应归 0(实际 {:?})",
        gauge(leader, "master_disk_evacuating")
    );

    // ⑤ **盘恢复后不自动回填**(设计里明写的"只置不回"):把 101 的盘改回健康,再跑够几个调谐周期,
    //    放置必须**一动不动** —— 否则一块盘抖一下就会来回搬数据(搬一次就是搬 GB 级,且会反复)。
    let planned_before_recovery = planned_now();
    disk_101_healthy.store(true, Ordering::Relaxed);
    for round in 0..4 {
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            placement_now(&sm),
            vec![102, 103],
            "第 {round} 轮:盘恢复后副本绝不该自动搬回去(放置只由撤离 / 下线 / 显式均衡改)"
        );
        assert_eq!(
            planned_now(),
            planned_before_recovery,
            "盘恢复不该产生新的撤离计划"
        );
    }

    keepalive.abort();
}

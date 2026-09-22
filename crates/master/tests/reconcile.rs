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
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
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
        reconcile_interval_secs: 1, // 快速调谐,便于测试
        reconcile_startup_grace_secs: 0, // 测试免宽限,立即调谐
    }
}

fn data_node(rack: &str) -> NodeInfo {
    NodeInfo {
        dc: "dc1".into(),
        rack: rack.into(),
        addr: format!("127.0.0.1:{}", free_port()),
        disks: BTreeMap::from([(
            "d0".to_string(),
            antfs_master::types::DiskSpec { capacity_bytes: 1_000_000_000_000 },
        )]),
        registered_at_ms: 1,
    }
}

fn beat(node_id: u64, replicas: Vec<antfs_master::pb::ReplicaBeat>) -> antfs_master::pb::NodeBeat {
    antfs_master::pb::NodeBeat {
        version: 1,
        cluster_secret: SECRET.into(),
        node_id,
        disks: vec![antfs_master::pb::DiskBeat {
            disk_id: "d0".into(),
            capacity_bytes: 1_000_000_000_000,
            free_bytes: 900_000_000_000,
            health: String::new(),
        }],
        full_report: true,
        replicas,
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
    let leader = nodes.iter().find(|n| n.config.node_id == leader_id).unwrap();

    // 注册 4 个数据节点(101..104),各不同机架。
    let racks = [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")];
    for (id, rack) in racks {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode { node_id: id, info: data_node(rack) })
            .await
            .expect("register data node");
    }

    // 显式建 1 个 RF=3 分区,副本落 101/102/103。
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement { node_id: 101, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 102, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 103, disk_id: "d0".into(), target_state: "ACTIVE".into() },
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
            }],
        )]))
        .await
        .expect("hb101")
        .into_inner();
    let directive = hb101.message().await.expect("recv").expect("directive");
    assert!(
        directive.transitions.iter().any(|t| t.partition_id == pid && t.target_state == "ACTIVE"),
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
    assert!(grew, "reconciler must add a replacement replica on live node 104; replicas={:?}",
        part.replicas.iter().map(|r| r.node_id).collect::<Vec<_>>());
    assert!(part.replicas.len() >= 4, "placement must grow with the replacement replica");
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
    let leader = nodes.iter().find(|n| n.config.node_id == leader_id).unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode { node_id: id, info: data_node(rack) })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement { node_id: 101, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 102, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 103, disk_id: "d0".into(), target_state: "ACTIVE".into() },
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
    let mut ids: Vec<u64> = g.state.partitions[&pid].replicas.iter().map(|r| r.node_id).collect();
    ids.sort();
    assert!(done, "evacuation must move replica off 103 onto a healthy node; got {ids:?}");
    assert_eq!(ids, vec![101, 102, 104], "final placement excludes decommissioned 103, includes 104");
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
    let leader = nodes.iter().find(|n| n.config.node_id == leader_id).unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode { node_id: id, info: data_node(rack) })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement { node_id: 101, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 102, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 103, disk_id: "d0".into(), target_state: "ACTIVE".into() },
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
    assert!(!r.ok && r.error == "partition not found", "got: {}", r.error);

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
                ReplicaPlacement { node_id: 101, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 102, disk_id: "d0".into(), target_state: "ACTIVE".into() },
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
        assert_eq!(p.replicas.len(), 3, "add-before-remove: new replica added first");
        assert!(g.state.moving.contains(&(pid2, 102)), "source must be marked moving-out");
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
    assert!(!dup.ok && dup.error == "move already in progress", "got: {}", dup.error);

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
    assert!(!bad.ok && bad.error.contains("does not hold a replica"), "got: {}", bad.error);

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
    assert!(!conflict.ok && conflict.error.contains("already holds a replica"), "got: {}", conflict.error);

    // list_partitions 回填 moving_out_nodes。
    let list = client
        .list_partitions(antfs_master::pb::ListPartitionsRequest { version: 1, offset: 0, limit: 0 })
        .await
        .expect("list_partitions")
        .into_inner();
    let entry = list.partitions.iter().find(|p| p.partition_id == pid2).expect("pid2 entry");
    assert_eq!(entry.moving_out_nodes, vec![102], "moving-out source must be listed");
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
    let leader = nodes.iter().find(|n| n.config.node_id == leader_id).unwrap();

    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC"), (104, "rD")] {
        leader
            .raft
            .client_write(MasterCommand::RegisterNode { node_id: id, info: data_node(rack) })
            .await
            .expect("register");
    }
    let resp = leader
        .raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![
                ReplicaPlacement { node_id: 101, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 102, disk_id: "d0".into(), target_state: "ACTIVE".into() },
                ReplicaPlacement { node_id: 103, disk_id: "d0".into(), target_state: "ACTIVE".into() },
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
    assert!(!m.ok && m.error == "no eligible disk", "no up target: {}", m.error);

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
        let Some(p) = g.state.partitions.get(&pid) else { return false };
        let mut ids: Vec<u64> = p.replicas.iter().map(|r| r.node_id).collect();
        ids.sort();
        ids == vec![101, 102, 104] && g.state.moving.is_empty()
    })
    .await;
    let ids: Vec<u64> = {
        let g = leader.sm_data.read().unwrap();
        g.state.partitions[&pid].replicas.iter().map(|r| r.node_id).collect()
    };
    assert!(done, "move must complete: source removed after replacement healthy; got {ids:?}");

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
    assert!(counted, "one moved replica must be counted on the acting leader");
}

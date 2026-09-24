//! M1 集成测试:3 节点 Raft 起集群 → 选主 → 数据节点自注册 → 全节点 `ListNodes` 可见。
//!
//! 同步用 openraft `raft.wait(timeout)`(条件变量,非 `sleep`)+ 带超时轮询(CLAUDE.md §6)。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{ClusterStateRequest, ListNodesRequest, RegisterRequest};
use antfs_master::types::{MasterCommand, NodeInfo};
use antfs_master::MasterNode;
use openraft::BasicNode;

const SECRET: &str = "test-secret";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().expect("local addr").port()
}

fn node_config(id: u64, dir: &std::path::Path, raft_port: u16, grpc_port: u16) -> MasterConfig {
    MasterConfig {
        node_id: id,
        raft_addr: format!("127.0.0.1:{raft_port}"),
        grpc_addr: format!("127.0.0.1:{grpc_port}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1, 2, 3],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 15,
        partition_capacity_bytes: 100 * 1024 * 1024 * 1024,
        disk_near_full_ratio: 0.95,
        auto_grow_enabled: false,
        auto_grow_min_writable: 4,
        auto_grow_max_partitions: 100_000,
        auto_grow_interval_secs: 30,
        auto_grow_batch: 4,
        reconcile_interval_secs: 5,
        reconcile_startup_grace_secs: 0,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader_and_registers_node() {
    let tmp = tempfile::tempdir().unwrap();

    // 起 3 节点并各自监听 gRPC。每节点独立 Registry(生产中本就是独立进程)。
    let mut nodes = Vec::new();
    let mut registries = BTreeMap::new();
    let mut raft_addrs = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = node_config(id, &dir, free_port(), free_port());
        raft_addrs.insert(id, BasicNode::new(cfg.raft_addr.clone()));
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(cfg, &registry).await.expect("start node");
        node.serve().expect("serve");
        registries.insert(id, registry);
        nodes.push(node);
    }

    // 在 node1 上初始化 3 成员集群。
    nodes[0]
        .initialize(raft_addrs.clone())
        .await
        .expect("initialize");

    // 等待选出 leader(条件变量等待,非 sleep)。
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("leader elected within timeout");
    }

    let leader_id = nodes[0]
        .raft
        .metrics()
        .borrow()
        .current_leader
        .expect("has leader");
    let leader = nodes
        .iter()
        .find(|n| n.config.node_id == leader_id)
        .expect("leader node");

    // 数据节点(id=100)经 leader 自注册,走 Raft 提案。
    let info = NodeInfo {
        dc: "dc1".into(),
        rack: "r1".into(),
        addr: "127.0.0.1:9000".into(),
        disks: BTreeMap::from([(
            "d0".to_string(),
            antfs_master::types::DiskSpec {
                capacity_bytes: 16_000_000_000_000,
            },
        )]),
        registered_at_ms: 1,
    };
    let resp = leader
        .raft
        .client_write(MasterCommand::RegisterNode { node_id: 100, info })
        .await
        .expect("client_write register");
    let applied_index = resp.log_id.index;

    // 等待每个节点都应用到该提案,然后断言状态机可见该节点。
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .applied_index_at_least(Some(applied_index), "applied register")
            .await
            .expect("applied register within timeout");
        let g = n.sm_data.read().unwrap();
        assert!(
            g.state.nodes.contains_key(&100),
            "node {} must see registered node 100",
            n.config.node_id
        );
    }

    // 控制面 gRPC:ListNodes 与 GetClusterState 可读到注册结果。
    let mut client = MasterClient::connect(format!("http://{}", leader.config.grpc_addr))
        .await
        .expect("connect leader gRPC");
    let list = client
        .list_nodes(ListNodesRequest { version: 1 })
        .await
        .expect("list_nodes")
        .into_inner();
    assert!(
        list.nodes.iter().any(|e| e.node_id == 100),
        "ListNodes must include node 100"
    );

    let state = client
        .get_cluster_state(ClusterStateRequest { version: 1 })
        .await
        .expect("get_cluster_state")
        .into_inner();
    assert_eq!(state.leader, Some(leader_id));
    assert!(state.registered_nodes >= 1);

    // 指标断言:打在 leader 实际接入的那个 Registry 上(CLAUDE.md §6),不新建实例。
    let dump = antfs_metric::gather(&registries[&leader_id]).expect("gather metrics");
    assert!(
        dump.contains("master_registered_nodes 1"),
        "leader registry must report 1 registered node, got:\n{dump}"
    );
    assert!(
        dump.contains("master_applied_commands_total"),
        "applied-commands counter must be exported"
    );

    // 准入校验(Q1):错误密钥被拒。
    let bad = client
        .register_node(RegisterRequest {
            version: 1,
            cluster_secret: "wrong".into(),
            node_id: 200,
            dc: "dc1".into(),
            rack: "r1".into(),
            addr: "127.0.0.1:9001".into(),
            disks: vec![],
        })
        .await;
    assert_eq!(
        bad.expect_err("wrong secret must be rejected").code(),
        tonic::Code::Unauthenticated
    );

    // ---- M2b:放置 + 路由 ----
    // 注册 3 个数据节点(分处 rA/rB/rC,各一块 1TB 盘),经 leader 进 hard state。
    for (id, rack) in [(101u64, "rA"), (102, "rB"), (103, "rC")] {
        let dinfo = NodeInfo {
            dc: "dc1".into(),
            rack: rack.into(),
            addr: format!("127.0.0.1:90{id}"),
            disks: BTreeMap::from([(
                "d0".to_string(),
                antfs_master::types::DiskSpec {
                    capacity_bytes: 1_000_000_000_000,
                },
            )]),
            registered_at_ms: 1,
        };
        leader
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: id,
                info: dinfo,
            })
            .await
            .expect("register data node");
    }

    // 心跳上报各盘实时余量(软状态,供选盘);发给 leader 的 gRPC。
    let beats: Vec<antfs_master::pb::NodeBeat> = [101u64, 102, 103]
        .into_iter()
        .map(|id| antfs_master::pb::NodeBeat {
            version: 1,
            cluster_secret: SECRET.into(),
            node_id: id,
            disks: vec![antfs_master::pb::DiskBeat {
                disk_id: "d0".into(),
                capacity_bytes: 1_000_000_000_000,
                free_bytes: 900_000_000_000,
                health: String::new(),
            }],
            full_report: true,
            replicas: vec![],
        })
        .collect();
    let mut hb = client
        .heartbeat(tokio_stream::iter(beats))
        .await
        .expect("heartbeat stream")
        .into_inner();
    for _ in 0..3 {
        hb.message()
            .await
            .expect("directive")
            .expect("beat processed");
    }

    // 建 1 个 RF=3 分区,断言副本跨 3 个不同节点。
    let created = client
        .create_partitions(antfs_master::pb::CreatePartitionsRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            count: 1,
            replication_factor: 3,
        })
        .await
        .expect("create_partitions")
        .into_inner();
    assert!(
        created.ok,
        "create_partitions must succeed: {}",
        created.error
    );
    assert_eq!(created.partition_ids.len(), 1);
    let pid = created.partition_ids[0];

    let locs = client
        .lookup_partition(antfs_master::pb::LookupRequest {
            version: 1,
            partition_id: pid,
        })
        .await
        .expect("lookup_partition")
        .into_inner();
    assert_eq!(locs.state, "RW");
    assert_eq!(locs.replicas.len(), 3);
    let mut nodes_used: Vec<u64> = locs.replicas.iter().map(|r| r.node_id).collect();
    nodes_used.sort();
    assert_eq!(
        nodes_used,
        vec![101, 102, 103],
        "replicas must span the 3 data nodes"
    );

    let assigned = client
        .assign_partition(antfs_master::pb::AssignRequest { version: 1 })
        .await
        .expect("assign_partition")
        .into_inner();
    assert!(
        assigned.ok,
        "assign must return a writable partition: {}",
        assigned.error
    );
    assert_eq!(assigned.partition_id, pid);
    assert_eq!(assigned.replicas.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_metrics_exports_prometheus_text() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n1");
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = node_config(1, &dir, free_port(), free_port());
    let raft_addr = cfg.raft_addr.clone();
    let grpc_addr = cfg.grpc_addr.clone();
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg, &registry).await.expect("start");
    node.serve().expect("serve");
    // 单成员集群自举。
    node.initialize(BTreeMap::from([(1u64, BasicNode::new(raft_addr))]))
        .await
        .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "leader elected")
        .await
        .expect("leader elected within timeout");

    let mut client = MasterClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect");
    let resp = client
        .get_metrics(antfs_master::pb::MetricsRequest { version: 1 })
        .await
        .expect("get_metrics")
        .into_inner();
    assert!(
        resp.text.contains("master_heartbeats_total"),
        "metrics text must export heartbeats counter, got:\n{}",
        resp.text
    );
    assert!(
        resp.text.contains("master_is_leader"),
        "metrics text must export is_leader gauge, got:\n{}",
        resp.text
    );
    // 文本必须能被 Prometheus 文本解析器解析(common/metric parse)。
    let samples = antfs_metric::parse(&resp.text);
    assert!(
        samples
            .iter()
            .any(|s| s.name == "master_is_leader" && s.value == 1.0),
        "leader must export master_is_leader 1"
    );
}

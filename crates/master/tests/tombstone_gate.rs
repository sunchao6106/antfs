//! 墓碑回收闸门集成测试:心跳指令里的 `tombstones_reclaim_allowed` 随"副本是否追平"翻转。
//!
//! 为什么盯这条通路:store 侧 compaction 按这个 bool 决定能不能回收墓碑(删除标记)。若在还有副本没
//! 追平时放行,那份副本就永远学不到那条删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。
//!
//! 单节点 master + 1 个数据节点 + RF=1 分区:闸门的输入(副本实然态)全由本用例的心跳造出来,
//! 故每拍心跳的响应就是这一拍的状态决定的(handler 先 `record_replicas` 再算指令),断言无需轮询。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{CreatePartitionsRequest, MasterDirective, NodeBeat, ReplicaBeat};
use antfs_master::types::{MasterCommand, NodeInfo};
use antfs_master::MasterNode;
use openraft::BasicNode;

const SECRET: &str = "test-secret";
/// 测试盘容量 1 TB / 余量 900 GB(用量 10%,远低于 near-full 阈值)。
const DISK_CAPACITY: u64 = 1_000_000_000_000;
const DISK_FREE: u64 = 900_000_000_000;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 单节点 master(只需 leader 决策,不需要多副本 Raft):
/// 关掉调谐(宽限期极长),本用例只观察闸门,不让 reconciler 动 placement。
fn cfg(node_id: u64, dir: &std::path::Path, raft: u16, grpc: u16) -> MasterConfig {
    MasterConfig {
        node_id,
        raft_addr: format!("127.0.0.1:{raft}"),
        grpc_addr: format!("127.0.0.1:{grpc}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![node_id],
        cluster_secret: SECRET.to_string(),
        // 心跳超时长:单次心跳即让节点在整个测试期算 up(不引入"失联"这一维)。
        heartbeat_timeout_secs: 60,
        reconcile_startup_grace_secs: 3600,
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
                capacity_bytes: DISK_CAPACITY,
            },
        )]),
        registered_at_ms: 1,
    }
}

fn beat(node_id: u64, replicas: Vec<ReplicaBeat>) -> NodeBeat {
    NodeBeat {
        version: 1,
        cluster_secret: SECRET.into(),
        node_id,
        disks: vec![antfs_master::pb::DiskBeat {
            disk_id: "d0".into(),
            capacity_bytes: DISK_CAPACITY,
            free_bytes: DISK_FREE,
            health: String::new(),
        }],
        full_report: true,
        replicas,
        replica_digest: 0,
        removed_replicas: vec![],
    }
}

/// 发一次心跳(单条消息的流)并把节点标活,返回这一拍的指令。
async fn send_beat(
    client: &mut MasterClient<tonic::transport::Channel>,
    node_id: u64,
    replicas: Vec<ReplicaBeat>,
) -> MasterDirective {
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat(node_id, replicas)]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive")
}

fn replica(partition_id: u32, current_state: &str, lag_bytes: u64) -> ReplicaBeat {
    ReplicaBeat {
        partition_id,
        disk_id: "d0".into(),
        current_state: current_state.into(),
        lag_bytes,
        used_bytes: 0,
    }
}

/// 该拍指令里某分区的墓碑回收闸门。
fn gate(d: &MasterDirective, pid: u32) -> bool {
    d.partition_states
        .iter()
        .find(|s| s.partition_id == pid)
        .unwrap_or_else(|| {
            panic!(
                "partition {pid} 的分区状态应随指令下发,gots: {:?}",
                d.partition_states
            )
        })
        .tombstones_reclaim_allowed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstones_reclaim_gate_follows_replica_catch_up() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("n1");
    std::fs::create_dir_all(&dir).unwrap();
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg(1, &dir, free_port(), free_port()), &registry)
        .await
        .expect("start");
    node.serve().expect("serve");
    node.initialize(BTreeMap::from([(
        1,
        BasicNode::new(node.config.raft_addr.clone()),
    )]))
    .await
    .expect("initialize");
    node.raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader.is_some(), "leader")
        .await
        .expect("leader elected");

    // 注册唯一的数据节点,并先报一拍把盘余量写进软状态(建分区选盘要用余量)。
    node.raft
        .client_write(MasterCommand::RegisterNode {
            node_id: 101,
            info: data_node("rA"),
        })
        .await
        .expect("register data node");
    let mut client = MasterClient::connect(format!("http://{}", node.config.grpc_addr))
        .await
        .expect("connect");
    send_beat(&mut client, 101, Vec::new()).await;

    // RF=1 分区:唯一副本落在 101 的 d0(集群只有这一块可用盘)。
    let r = client
        .create_partitions(CreatePartitionsRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            count: 1,
            replication_factor: 1,
        })
        .await
        .expect("create_partitions")
        .into_inner();
    assert!(r.ok, "create_partitions must succeed: {}", r.error);
    let pid = r.partition_ids[0];

    // 唯一副本上报 ACTIVE 且 lag=0(= 已追平)→ 闸门开:允许 store 回收墓碑。
    let d = send_beat(&mut client, 101, vec![replica(pid, "ACTIVE", 0)]).await;
    assert!(
        gate(&d, pid),
        "全副本已追平 → tombstones_reclaim_allowed 应为 true: {:?}",
        d.partition_states
    );

    // 同一副本变成"还在追平"(lag_bytes > 0)→ 闸门关:墓碑一收它就永远学不到那条删除。
    let d = send_beat(&mut client, 101, vec![replica(pid, "ACTIVE", 1)]).await;
    assert!(
        !gate(&d, pid),
        "有副本滞后 → 不得回收墓碑: {:?}",
        d.partition_states
    );

    // 上报 BOOTSTRAP(追平中)→ 同样关。
    let d = send_beat(&mut client, 101, vec![replica(pid, "BOOTSTRAP", 0)]).await;
    assert!(
        !gate(&d, pid),
        "副本还在 BOOTSTRAP → 不得回收墓碑: {:?}",
        d.partition_states
    );

    // 追平回来 → 闸门重新打开(每拍重算,不是一次性)。
    let d = send_beat(&mut client, 101, vec![replica(pid, "ACTIVE", 0)]).await;
    assert!(
        gate(&d, pid),
        "追平后闸门应重新打开: {:?}",
        d.partition_states
    );
}

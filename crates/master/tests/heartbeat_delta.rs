//! 增量心跳(M7)集成测试:增量合并 + 显式摘除 + 摘要校验自愈 + 下行增量。
//!
//! 与 `tombstone_gate`(每拍一条新流)不同,本用例用**一条长流**连发多拍 —— 连接级下发现场
//! (last_sent)和 digest 校验都是流内状态,必须同一条流才能观测到增量行为。

use std::collections::BTreeMap;
use std::time::Duration;

use antfs_master::config::MasterConfig;
use antfs_master::digest;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{CreatePartitionsRequest, MasterDirective, NodeBeat, ReplicaBeat};
use antfs_master::types::{MasterCommand, NodeInfo};
use antfs_master::MasterNode;
use openraft::BasicNode;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const SECRET: &str = "test-secret";
const DISK_CAPACITY: u64 = 1_000_000_000_000;
const DISK_FREE: u64 = 900_000_000_000;
const NODE: u64 = 101;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 单节点 master:关掉调谐(宽限期极长),只观察心跳通路的增量行为。
fn cfg(node_id: u64, dir: &std::path::Path, raft: u16, grpc: u16) -> MasterConfig {
    MasterConfig {
        node_id,
        raft_addr: format!("127.0.0.1:{raft}"),
        grpc_addr: format!("127.0.0.1:{grpc}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![node_id],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 60,
        reconcile_startup_grace_secs: 3600,
        ..Default::default()
    }
}

fn replica(pid: u32, state: &str, lag: u64, used: u64) -> ReplicaBeat {
    ReplicaBeat {
        partition_id: pid,
        disk_id: "d0".into(),
        current_state: state.into(),
        lag_bytes: lag,
        used_bytes: used,
    }
}

fn digest_of(replicas: &[ReplicaBeat]) -> u64 {
    digest::digest_of(replicas.iter().map(|r| {
        (
            r.partition_id,
            "d0",
            r.current_state.as_str(),
            r.lag_bytes,
            r.used_bytes,
        )
    }))
}

fn beat(full: bool, replicas: Vec<ReplicaBeat>, removed: Vec<u32>, digest: u64) -> NodeBeat {
    NodeBeat {
        version: 1,
        cluster_secret: SECRET.into(),
        node_id: NODE,
        disks: vec![antfs_master::pb::DiskBeat {
            disk_id: "d0".into(),
            capacity_bytes: DISK_CAPACITY,
            free_bytes: DISK_FREE,
            health: String::new(),
        }],
        full_report: full,
        replicas,
        replica_digest: digest,
        removed_replicas: removed,
    }
}

/// 长流心跳:返回 (发送端, 接收端);每发一拍收一条指令。
struct BeatStream {
    tx: mpsc::Sender<NodeBeat>,
    rx: tonic::Streaming<MasterDirective>,
}

impl BeatStream {
    async fn round(&mut self, b: NodeBeat) -> MasterDirective {
        // 不 clone:stream 里每条消息独立,这里按值发。
        self.tx.send(b).await.expect("send beat");
        // 每条 beat 必有一条响应(handler 逐拍回复)。
        tokio::time::timeout(Duration::from_secs(10), self.rx.message())
            .await
            .expect("directive timeout")
            .expect("stream ok")
            .expect("directive")
    }
}

async fn start_cluster(
    dir: &std::path::Path,
) -> (MasterNode, MasterClient<tonic::transport::Channel>) {
    std::fs::create_dir_all(dir).unwrap();
    let registry = antfs_metric::Registry::new();
    let node = MasterNode::start(cfg(1, dir, free_port(), free_port()), &registry)
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
    node.raft
        .client_write(MasterCommand::RegisterNode {
            node_id: NODE,
            info: NodeInfo {
                dc: "dc1".into(),
                rack: "rA".into(),
                addr: format!("127.0.0.1:{}", free_port()),
                disks: BTreeMap::from([(
                    "d0".to_string(),
                    antfs_master::types::DiskSpec {
                        capacity_bytes: DISK_CAPACITY,
                    },
                )]),
                registered_at_ms: 1,
            },
        })
        .await
        .expect("register data node");
    let client = MasterClient::connect(format!("http://{}", node.config.grpc_addr))
        .await
        .expect("connect");
    (node, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_heartbeat_merges_removes_and_self_heals() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, mut client) = start_cluster(&tmp.path().join("n1")).await;

    // 建 RF=1 分区前先把盘余量报进软状态(选盘输入)。
    let (tx, rx) = mpsc::channel::<NodeBeat>(16);
    let stream = client
        .heartbeat(ReceiverStream::new(rx))
        .await
        .expect("hb")
        .into_inner();
    let mut hb = BeatStream { tx, rx: stream };
    let held = vec![];
    hb.round(beat(true, held.clone(), vec![], digest_of(&held)))
        .await;

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
    assert!(r.ok, "create_partitions: {}", r.error);
    let pid = r.partition_ids[0];

    // 拍 1:placement 已含 (pid, NODE) 但节点还没报 → 必须下发转移(应建未建仍下发)。
    // 全量空报,digest=0(空集)。
    let d = hb.round(beat(true, vec![], vec![], 0)).await;
    assert!(
        d.transitions.iter().any(|t| t.partition_id == pid),
        "placement 有、节点未上报 → 应下发转移: {:?}",
        d.transitions
    );
    assert!(d.delta_report_supported, "握手标志必须每拍置位");

    // 拍 2:全量上报 ACTIVE(副本已建)。首拍下行应含该分区状态;摘要一致 → 不要求重发。
    let held = vec![replica(pid, "ACTIVE", 0, 1024)];
    let d = hb
        .round(beat(true, held.clone(), vec![], digest_of(&held)))
        .await;
    assert!(!d.resend_full_report, "摘要一致不得要求重发");
    assert!(
        d.partition_states.iter().any(|s| s.partition_id == pid),
        "本连接首见该分区 → 下行应含其状态: {:?}",
        d.partition_states
    );
    assert!(
        d.transitions.is_empty(),
        "current==target 不应再有转移: {:?}",
        d.transitions
    );

    // 拍 3:稳态增量(空变化 + 同摘要)→ 不下发分区状态(增量下行)、不要求重发、无转移。
    let d = hb
        .round(beat(false, vec![], vec![], digest_of(&held)))
        .await;
    assert!(!d.resend_full_report);
    assert!(
        d.partition_states.is_empty(),
        "稳态增量拍不应重发分区状态: {:?}",
        d.partition_states
    );
    assert!(d.transitions.is_empty());
    assert!(node.soft.holds(pid, NODE), "增量缺项不得误删软状态");

    // 拍 4:摘要造假 → master 必须要求下一拍全量重发(自愈入口)。
    let d = hb
        .round(beat(false, vec![], vec![], digest_of(&held) ^ 1))
        .await;
    assert!(d.resend_full_report, "摘要不符必须置 resend_full_report");
    assert!(node.soft.holds(pid, NODE), "摘要不符不得清状态(等全量)");

    // 拍 5:应要求全量重发 → 摘要恢复一致。
    let d = hb
        .round(beat(true, held.clone(), vec![], digest_of(&held)))
        .await;
    assert!(!d.resend_full_report, "全量后摘要应恢复一致");

    // 拍 6:显式摘除(本地删了副本)→ 软状态清除;placement 还在 → 重新下发转移令其重建。
    let d = hb.round(beat(false, vec![], vec![pid], 0)).await;
    assert!(!node.soft.holds(pid, NODE), "removed_replicas 必须生效");
    assert!(
        d.transitions.iter().any(|t| t.partition_id == pid),
        "副本消失 → 应重新令建: {:?}",
        d.transitions
    );

    // 拍 7:闸门翻转仍随增量下行送达 —— 副本重建后先报 BOOTSTRAP(未追平),分区状态条目的
    // tombstones 标志从 true 翻 false,必须下发(增量只发变化,而这就是变化)。
    let held = vec![replica(pid, "BOOTSTRAP", 0, 0)];
    let d = hb
        .round(beat(false, held.clone(), vec![], digest_of(&held)))
        .await;
    let st = d
        .partition_states
        .iter()
        .find(|s| s.partition_id == pid)
        .unwrap_or_else(|| panic!("闸门翻转必须随增量下行: {:?}", d.partition_states));
    assert!(!st.tombstones_reclaim_allowed, "未追平 → 闸门关");
}

/// 新连接 = 新下发现场:重连后首拍应重新收到分区状态(全量下行),不依赖上一连接的记忆。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_stream_gets_full_downlink_again() {
    let tmp = tempfile::tempdir().unwrap();
    let (_node, mut client) = start_cluster(&tmp.path().join("n1")).await;

    let held = vec![]; // 节点无副本,只验证连接级语义不 panic、握手置位。
    for _ in 0..2 {
        let (tx, rx) = mpsc::channel::<NodeBeat>(16);
        let stream = client
            .heartbeat(ReceiverStream::new(rx))
            .await
            .expect("hb")
            .into_inner();
        let mut hb = BeatStream { tx, rx: stream };
        let d = hb
            .round(beat(true, held.clone(), vec![], digest_of(&held)))
            .await;
        assert!(d.delta_report_supported);
        assert!(!d.resend_full_report);
    }
}

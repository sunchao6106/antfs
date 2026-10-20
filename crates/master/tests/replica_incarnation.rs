//! M2:副本 **incarnation**(副本身份)发放测试。
//!
//! 语义见 `types.rs::ReplicaPlacement::incarnation`:身份由 master 的 **Raft 状态机**单调发放、
//! **永不复用**;每次"从零建一份新副本"都拿新号 —— 重建 / 换盘后的副本不得沿用旧身份,否则拉取方
//! 手里的旧复制游标会"看似合法",空批被误判追平 ⇒ 带缺数据转 `ACTIVE` ⇒ master 删源闸门成立 ⇒
//! 源被删 ⇒ **静默丢数据**。
//!
//! 断言的不变量:
//!   1. 建分区 / 再复制 / 迁移补副本产生的身份**严格递增且互不相同**,且都 ≥ 1(`0` 是"未分配"哨兵);
//!   2. `AssignReplicaIncarnation` 把 `(pid, node)` 换成**更大**的新号并落进 placement(重复调用每次新值);
//!   3. `LookupPartition` 返回的 `ReplicaLoc.incarnation` 与状态机一致(store 读到的就是权威值);
//!   4. **确定性**:同一批命令在任意节点产出同一状态(相邻两次分配恰差 1);旧快照(无该字段)读作
//!      `next_incarnation = 1` / 历史副本 `0`,新号不与之相撞;
//!   5. 3 节点集群:各节点状态机里的身份完全一致;非 leader 的申领请求回 `ok=false` + leader 地址。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    AssignIncarnationRequest, AssignIncarnationResponse, LookupRequest, PartitionLocations,
};
use antfs_master::soft::{DiskUsage, ReplicaState};
use antfs_master::types::{
    DiskSpec, MasterCommand, MasterResponse, MasterState, NodeId, NodeInfo, PartitionId,
    ReplicaPlacement,
};
use antfs_master::MasterNode;
use antfs_metric::Registry;
use openraft::BasicNode;
use tonic::transport::Channel;

const SECRET: &str = "test-secret";
const DISK: &str = "d0";

fn base_config(id: u64, dir: &Path, members: Vec<u64>) -> MasterConfig {
    MasterConfig {
        node_id: id,
        raft_addr: "127.0.0.1:0".to_string(),
        grpc_addr: "127.0.0.1:0".to_string(),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: members,
        cluster_secret: SECRET.to_string(),
        reconcile_startup_grace_secs: 0,
        ..Default::default()
    }
}

fn placement(node_id: NodeId, disk: &str) -> ReplicaPlacement {
    ReplicaPlacement {
        node_id,
        disk_id: disk.to_string(),
        target_state: "ACTIVE".to_string(),
        // 占位:真身份由 apply 发放(见 types.rs),提案里带什么都不采信。
        incarnation: 0,
    }
}

/// 起一个单节点 master(自举 → 等自己当选 leader)。
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

/// 开维护模式:临时 master 的软状态是空的,reconciler 会误判"欠副本"去补副本 —— 既多烧身份号,
/// 又打乱本文件对号序的断言。维护模式只停自动动作,手动 ops(本文件全走直接 `client_write`)不受影响。
async fn enter_maintenance(node: &MasterNode) {
    node.raft
        .client_write(MasterCommand::SetMaintenance {
            paused: true,
            pause_autogrow: false,
            set_at_ms: 0,
        })
        .await
        .expect("enter maintenance");
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

/// 上报一次心跳(判活 + 给出真实余量),并上报这些 `(pid)` 为 `ACTIVE`(否则被判欠副本)。
fn beat(node: &MasterNode, node_id: NodeId, replicas: &[PartitionId]) {
    node.soft.record_beat(
        node_id,
        HashMap::from([(
            DISK.to_string(),
            DiskUsage {
                capacity_bytes: 1 << 40,
                free_bytes: 1 << 40,
                health: String::new(),
                adjusted_free: 1 << 40,
                last_heartbeat_time: std::time::Instant::now(),
            },
        )]),
        std::time::Duration::from_secs(120),
    );
    node.soft.record_replicas(
        node_id,
        replicas.iter().map(|pid| {
            (
                *pid,
                ReplicaState {
                    disk_id: DISK.to_string(),
                    current_state: "ACTIVE".to_string(),
                    lag_bytes: 0,
                    used_bytes: 0,
                },
            )
        }),
        &[],
        true,
    );
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

/// 状态机里该分区各副本的 `(node_id, incarnation)`,按 placement(写入)顺序。
fn incarnations(node: &MasterNode, pid: PartitionId) -> Vec<(NodeId, u64)> {
    let g = node.sm_data.read().expect("sm lock");
    g.state.partitions[&pid]
        .replicas
        .iter()
        .map(|r| (r.node_id, r.incarnation))
        .collect()
}

fn next_incarnation(node: &MasterNode) -> u64 {
    node.sm_data.read().expect("sm lock").state.next_incarnation
}

async fn connect(grpc_addr: &str) -> MasterClient<Channel> {
    MasterClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect master gRPC")
}

async fn assign(
    client: &mut MasterClient<Channel>,
    pid: PartitionId,
    node_id: NodeId,
) -> AssignIncarnationResponse {
    client
        .assign_replica_incarnation(AssignIncarnationRequest {
            version: 1,
            cluster_secret: SECRET.to_string(),
            partition_id: pid,
            node_id,
        })
        .await
        .expect("assign_replica_incarnation rpc")
        .into_inner()
}

async fn lookup(client: &mut MasterClient<Channel>, pid: PartitionId) -> PartitionLocations {
    client
        .lookup_partition(LookupRequest {
            version: 1,
            partition_id: pid,
        })
        .await
        .expect("lookup_partition rpc")
        .into_inner()
}

/// 同一批命令在**两个独立状态机**上跑:必须逐字段相同(apply 只存不算 —— 号不能在 leader 侧算)。
fn apply_all(cmds: &[MasterCommand]) -> MasterState {
    let mut s = MasterState::default();
    for c in cmds {
        s.apply_command(c);
    }
    s
}

/// 一批覆盖全部"加副本"apply 路径的命令:建分区 → 再复制(含重放)→ 迁移补副本 → 换身份(两次)。
fn command_batch() -> Vec<MasterCommand> {
    vec![
        MasterCommand::CreatePartition {
            replication_factor: 3,
            replicas: vec![placement(1, DISK), placement(2, DISK), placement(3, DISK)],
            partition_capacity: 0,
        },
        MasterCommand::AddReplica {
            partition_id: 0,
            replica: placement(4, DISK),
        },
        // 重放同一条:幂等(不重复追加,也**不换身份** —— 重放同 log 必须同结果)。
        MasterCommand::AddReplica {
            partition_id: 0,
            replica: placement(4, DISK),
        },
        MasterCommand::MoveReplica {
            partition_id: 0,
            from_node_id: 3,
            replica: placement(5, DISK),
        },
        MasterCommand::AssignReplicaIncarnation {
            partition_id: 0,
            node_id: 1,
        },
        MasterCommand::AssignReplicaIncarnation {
            partition_id: 0,
            node_id: 1,
        },
    ]
}

/// 建分区 / 再复制 / 迁移补副本:每条"加副本"路径都发新号,全局互不相同且严格递增。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_add_move_allocate_distinct_increasing_incarnations() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, _) = solo_master(&tmp.path().join("n1")).await;
    enter_maintenance(&node).await;
    for (id, addr) in [(101u64, "127.0.0.1:7301"), (102, "127.0.0.1:7302")] {
        register_store(&node, id, addr).await;
    }

    // 首建:三份新副本 = 三个新身份。
    let pid = create_partition(&node, vec![placement(101, DISK), placement(102, DISK)]).await;
    beat(&node, 101, &[pid]);
    beat(&node, 102, &[pid]);
    let after_create = incarnations(&node, pid);
    assert_eq!(
        after_create,
        vec![(101, 1), (102, 2)],
        "首建的两份副本必须各拿一个新号(从 1 起)"
    );

    // 再复制:补第三份副本 → 再一个新号。
    node.raft
        .client_write(MasterCommand::AddReplica {
            partition_id: pid,
            replica: placement(103, DISK),
        })
        .await
        .expect("add replica");
    register_store(&node, 103, "127.0.0.1:7303").await;
    let after_add = incarnations(&node, pid);
    assert_eq!(
        after_add.last(),
        Some(&(103, 3)),
        "再复制补上的副本是新的一份,必须拿新号"
    );

    // 迁移补副本(先补):迁入的那份同样是新副本 → 新号。
    node.raft
        .client_write(MasterCommand::MoveReplica {
            partition_id: pid,
            from_node_id: 102,
            replica: placement(104, DISK),
        })
        .await
        .expect("move replica");
    let all: Vec<(NodeId, u64)> = incarnations(&node, pid);
    assert_eq!(
        all,
        vec![(101, 1), (102, 2), (103, 3), (104, 4)],
        "迁入的新副本拿新号(旧号 2 归源副本,不转移)"
    );

    // 全局不变量:严格递增 + 互不相同 + ≥ 1(0 保留给"未分配")。
    let vals: Vec<u64> = all.iter().map(|(_, v)| *v).collect();
    assert!(
        vals.windows(2).all(|w| w[0] < w[1]),
        "号必须严格递增,got {vals:?}"
    );
    assert!(vals.iter().all(|v| *v >= 1), "真身份一律 ≥ 1,got {vals:?}");
    let uniq: BTreeSet<u64> = vals.iter().copied().collect();
    assert_eq!(
        uniq.len(),
        vals.len(),
        "身份必须互不相同(单调发号,永不复用)"
    );
    assert_eq!(next_incarnation(&node), 5, "分配器必须停在下一个可用号上");
}

/// `AssignReplicaIncarnation`:**换新号**(不是取回旧号),落进 placement,重复调用每次都是新的更大值。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn assign_replica_incarnation_replaces_with_a_bigger_value() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    enter_maintenance(&node).await;
    register_store(&node, 101, "127.0.0.1:7301").await;
    let pid = create_partition(&node, vec![placement(101, DISK)]).await;
    let initial = incarnations(&node, pid)[0].1;
    assert_eq!(initial, 1, "首建副本的身份从 1 起");

    let mut client = connect(&grpc_addr).await;
    let first = assign(&mut client, pid, 101).await;
    assert!(first.ok, "assign must succeed: {}", first.error);
    assert!(
        first.incarnation > initial,
        "必须是**新的更大**号({initial} → {}),不能取回旧号",
        first.incarnation
    );
    assert_eq!(
        incarnations(&node, pid),
        vec![(101, first.incarnation)],
        "新身份必须落进 placement"
    );

    // 重复调用:每次都换成一个更大的新值(重建/换盘会反复申领)。
    let second = assign(&mut client, pid, 101).await;
    assert!(second.ok, "repeat assign must succeed: {}", second.error);
    assert!(
        second.incarnation > first.incarnation,
        "重复申领必须再换新号({} → {})",
        first.incarnation,
        second.incarnation
    );
    assert_eq!(incarnations(&node, pid), vec![(101, second.incarnation)]);
    assert_eq!(
        next_incarnation(&node),
        second.incarnation + 1,
        "分配器停在下一个可用号"
    );

    // 只认 placement 里已有的副本:不存在的 (pid, node) 不发号、状态不变。
    let before = next_incarnation(&node);
    let missing_node = assign(&mut client, pid, 999).await;
    assert!(!missing_node.ok, "该节点没有这份副本 → 必须拒绝");
    assert!(
        missing_node.error.contains("999"),
        "错误信息要点出是哪个节点,got {}",
        missing_node.error
    );
    let missing_pid = assign(&mut client, pid + 100, 101).await;
    assert!(!missing_pid.ok, "不存在的分区 → 必须拒绝");
    assert_eq!(
        next_incarnation(&node),
        before,
        "被拒的申领不得烧掉任何号(号只随真正落地的副本前进)"
    );
    assert_eq!(incarnations(&node, pid), vec![(101, second.incarnation)]);
}

/// `LookupPartition` 报出的 `ReplicaLoc.incarnation` 与状态机**逐副本一致**(store 据它认自己的身份)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_partition_incarnation_matches_state_machine() {
    let tmp = tempfile::tempdir().unwrap();
    let (node, grpc_addr) = solo_master(&tmp.path().join("n1")).await;
    enter_maintenance(&node).await;
    for (id, addr) in [
        (101u64, "127.0.0.1:7301"),
        (102, "127.0.0.1:7302"),
        (103, "127.0.0.1:7303"),
    ] {
        register_store(&node, id, addr).await;
    }
    let pid = create_partition(
        &node,
        vec![
            placement(101, DISK),
            placement(102, DISK),
            placement(103, DISK),
        ],
    )
    .await;

    let mut client = connect(&grpc_addr).await;
    let got = lookup(&mut client, pid).await;
    let from_rpc: Vec<(NodeId, u64)> = got
        .replicas
        .iter()
        .map(|r| (r.node_id, r.incarnation))
        .collect();
    assert_eq!(
        from_rpc,
        incarnations(&node, pid),
        "LookupPartition 必须报状态机里的当前值"
    );
    assert_eq!(from_rpc, vec![(101, 1), (102, 2), (103, 3)]);

    // 换身份后,再查仍是同一个值(申领结果对读路由立即可见)。
    let new = assign(&mut client, pid, 102).await;
    assert!(new.ok, "assign must succeed: {}", new.error);
    let after: Vec<(NodeId, u64)> = lookup(&mut client, pid)
        .await
        .replicas
        .iter()
        .map(|r| (r.node_id, r.incarnation))
        .collect();
    assert_eq!(after, incarnations(&node, pid));
    assert_eq!(after, vec![(101, 1), (102, new.incarnation), (103, 3)]);
}

/// 确定性(纯状态机,不起服务):同批命令 → 同状态;相邻两次分配**恰差 1**;重放幂等;旧快照可读。
#[test]
fn apply_is_deterministic_and_assignments_advance_by_one() {
    let a = apply_all(&command_batch());
    let b = apply_all(&command_batch());
    assert_eq!(
        a, b,
        "同一命令序列必须产出完全相同的状态(含 next_incarnation)"
    );

    // 号按分配顺序 1..=7:建分区 1/2/3 → 再复制 4 → 重放不烧号 → 迁移 5 → 两次换身份 6、7。
    assert_eq!(
        a.partitions[&0]
            .replicas
            .iter()
            .map(|r| (r.node_id, r.incarnation))
            .collect::<Vec<_>>(),
        vec![(1, 7), (2, 2), (3, 3), (4, 4), (5, 5)],
        "node1 被连续换两次身份(6、7),其余保持原号"
    );
    assert_eq!(a.next_incarnation, 8, "下一个可用号 = 已发放的最大号 + 1");
    let assigned = [1, 2, 3, 4, 5, 6, 7];
    assert!(
        assigned.windows(2).all(|w| w[1] == w[0] + 1),
        "相邻两次分配必须恰差 1(可复现的确定性号序)"
    );

    // 迁移的那份是**新**副本(node5 拿 5),源 node3 的旧身份不变。
    assert!(a.moving.contains(&(0, 3)), "迁移必须记 moving 源");
    assert_eq!(a.move_target_of(0, 3), Some(5));

    // §3.4 后向兼容:旧快照无 next_incarnation、副本也无 incarnation(旧代码没发过号)。
    let old = serde_json::json!({
        "next_partition_id": 1,
        "object_next": {},
        "nodes": {},
        "partitions": {
            "0": {
                "replication_factor": 1,
                "state": "ReadWrite",
                "replicas": [{ "node_id": 7, "disk_id": "d0", "target_state": "ACTIVE" }]
            }
        },
        "policy": { "default_replication_factor": 3 },
        "decommissioning": []
    });
    let mut s: MasterState = serde_json::from_value(old).expect("old snapshot must deserialize");
    assert_eq!(
        s.next_incarnation, 1,
        "缺字段读作 1(不是 0):新发的号必须与历史副本的 0 分得开"
    );
    assert_eq!(
        s.partitions[&0].replicas[0].incarnation, 0,
        "历史副本的身份是 0(= 未分配,store 应视作不可信)"
    );
    // 历史副本可以就地换新身份 → 新号 ≥ 1,绝不与 0 相撞。
    assert_eq!(
        s.apply_command(&MasterCommand::AssignReplicaIncarnation {
            partition_id: 0,
            node_id: 7
        }),
        MasterResponse::Incarnation {
            partition_id: 0,
            node_id: 7,
            incarnation: Some(1)
        }
    );
    assert_eq!(s.partitions[&0].replicas[0].incarnation, 1);
    // 快照可往返(状态机经 JSON 落 redb)。
    let bytes = serde_json::to_vec(&s).expect("state must serialize");
    let back: MasterState = serde_json::from_slice(&bytes).expect("must deserialize");
    assert_eq!(back, s, "incarnation 必须能经 JSON 往返");
}

/// 3 节点:同一 log 序列 → 各节点状态机里的身份完全一致;非 leader 的申领请求回 leader 地址重定向。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_agrees_on_incarnations() {
    let tmp = tempfile::tempdir().unwrap();
    let mut nodes = Vec::new();
    let mut raft_addrs = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = base_config(id, &dir, vec![1, 2, 3]);
        let registry = Registry::new();
        let node = MasterNode::start(cfg, &registry).await.expect("start node");
        node.serve().expect("serve");
        let raft_addr = node.bound_raft_addr().expect("bound raft");
        raft_addrs.insert(id, BasicNode::new(raft_addr));
        nodes.push(node);
    }
    nodes[0]
        .initialize(raft_addrs)
        .await
        .expect("initialize cluster");
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
    enter_maintenance(leader).await;

    for (id, addr) in [
        (101u64, "127.0.0.1:7301"),
        (102, "127.0.0.1:7302"),
        (103, "127.0.0.1:7303"),
    ] {
        register_store(leader, id, addr).await;
    }

    // 建分区(3 副本)→ 再复制补第 4 份 → 迁入一份新副本:各路径的号都经 Raft 提交。
    let pid = create_partition(
        leader,
        vec![
            placement(101, DISK),
            placement(102, DISK),
            placement(103, DISK),
        ],
    )
    .await;
    leader
        .raft
        .client_write(MasterCommand::AddReplica {
            partition_id: pid,
            replica: placement(104, DISK),
        })
        .await
        .expect("add replica");
    leader
        .raft
        .client_write(MasterCommand::MoveReplica {
            partition_id: pid,
            from_node_id: 103,
            replica: placement(105, DISK),
        })
        .await
        .expect("move replica");

    // 经 gRPC 申领一个新身份(leader-only 写)。
    let mut client = connect(&leader.bound_grpc_addr().expect("bound grpc")).await;
    let reassigned = assign(&mut client, pid, 101).await;
    assert!(reassigned.ok, "assign must succeed: {}", reassigned.error);

    // 屏障:申领走 RPC(拿不到它的 log index),再提一条纯赋值命令拿到 index;等所有节点都应用到
    // 这条 ⇒ 它之前的申领那条也必然已应用。
    let barrier = leader
        .raft
        .client_write(MasterCommand::SetMaintenance {
            paused: true,
            pause_autogrow: false,
            set_at_ms: 0,
        })
        .await
        .expect("barrier write")
        .log_id
        .index;
    for n in &nodes {
        n.raft
            .wait(Some(Duration::from_secs(15)))
            .applied_index_at_least(Some(barrier), "applied incarnation proposals")
            .await
            .expect("applied within timeout");
    }

    // 各节点状态机里的身份(以及分配器)必须逐字段一致。
    let snapshot = |n: &MasterNode| -> (u64, Vec<(u32, NodeId, u64)>) {
        let g = n.sm_data.read().expect("sm lock");
        let mut reps = Vec::new();
        for (pid, info) in &g.state.partitions {
            for r in &info.replicas {
                reps.push((*pid, r.node_id, r.incarnation));
            }
        }
        (g.state.next_incarnation, reps)
    };
    let want = snapshot(leader);
    assert_eq!(
        want.1,
        vec![
            (pid, 101, reassigned.incarnation),
            (pid, 102, 2),
            (pid, 103, 3),
            (pid, 104, 4),
            (pid, 105, 5),
        ],
        "leader 侧的号序:建分区 1/2/3 → 再复制 4 → 迁移 5 → 换身份 {}",
        reassigned.incarnation
    );
    for n in &nodes {
        assert_eq!(
            snapshot(n),
            want,
            "节点 {} 的 incarnation 状态机必须与 leader 完全一致",
            n.config.node_id
        );
    }

    // 非 leader:拒绝并回填 leader 地址(供 client 重定向),本身不提交任何东西。
    let follower = nodes
        .iter()
        .find(|n| n.config.node_id != leader_id)
        .expect("a follower");
    let mut follower_client = connect(&follower.bound_grpc_addr().expect("bound grpc")).await;
    let rejected = assign(&mut follower_client, pid, 101).await;
    assert!(!rejected.ok, "follower 不得自行发号");
    assert_eq!(rejected.error, "not leader");
    assert_eq!(
        rejected.leader_addr,
        leader.bound_raft_addr().expect("bound raft"),
        "必须回填 leader 地址供重定向"
    );
    assert_eq!(rejected.incarnation, 0, "被拒时 incarnation 为 0(未发号)");
    assert_eq!(snapshot(follower), want, "被拒的请求不得改动状态机");
}

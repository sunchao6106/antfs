//! 计划式均衡(对标 Ceph balancer:eval → start → cancel → state)集成测试:真 3 节点 master + 真 gRPC,
//! 数据节点用心跳模拟(无 store server)。同步一律用带超时的轮询(CLAUDE.md §6),不用裸 sleep。

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use antfs_master::config::MasterConfig;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    BalanceStateRequest, CancelBalanceRequest, EvalBalanceRequest, MoveReplicaRequest, ReplicaBeat,
    StartBalanceRequest,
};
use antfs_master::types::{DiskSpec, MasterCommand, NodeInfo, ReplicaPlacement};
use antfs_master::MasterNode;
use openraft::BasicNode;

const SECRET: &str = "test-secret";

/// 测试盘容量(1 TB)与余量(900 GB → 用量比 10%,远低于 near-full 阈值)。
const DISK_CAPACITY: u64 = 1_000_000_000_000;
const DISK_FREE: u64 = 900_000_000_000;

/// 数据节点 id 集合:101..104(104 是均衡的"空节点"目标)。
const DATA_NODES: [u64; 4] = [101, 102, 103, 104];

/// 各分区上报的占用(字节):让 planner 的"按占用从大到小"有确定顺序(pid2 > pid1 > pid0)。
fn used_of(pid: u32) -> u64 {
    (pid as u64 + 1) * 10_000
}

fn cfg(
    id: u64,
    dir: &std::path::Path,
    raft: u16,
    grpc: u16,
    move_timeout_secs: u64,
) -> MasterConfig {
    MasterConfig {
        node_id: id,
        raft_addr: format!("127.0.0.1:{raft}"),
        grpc_addr: format!("127.0.0.1:{grpc}"),
        data_dir: dir.to_string_lossy().into_owned(),
        initial_members: vec![1, 2, 3],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 60, // 单次心跳即可让节点在整个测试期"存活"
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
        balance_max_in_flight: 1, // 默认并发 1:便于断言"同一时刻只 1 条在途"
        balance_max_moves: 100,
        balance_skew_threshold: 1.0,
        balance_move_timeout_secs: move_timeout_secs,
        // 其余配置项走默认值:本用例只关心上面那几项,新增配置项不该让每个测试文件都跟着改一遍。
        ..Default::default()
    }
}

/// 起一个 3 节点 master 集群并等出 leader(真实 gRPC)。
async fn start_masters(tmp: &std::path::Path, move_timeout_secs: u64) -> Vec<MasterNode> {
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1u64..=3 {
        let dir = tmp.join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let c = cfg(id, &dir, 0, 0, move_timeout_secs);
        let registry = antfs_metric::Registry::new();
        let node = MasterNode::start(c, &registry).await.expect("start");
        node.serve().expect("serve");
        let raft_addr = node.bound_raft_addr().expect("bound raft");
        members.insert(id, BasicNode::new(raft_addr));
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
    let addr = node.bound_grpc_addr().expect("bound grpc");
    MasterClient::connect(format!("http://{addr}"))
        .await
        .expect("connect")
}

/// 注册 101..104(各一块 d0 盘,不同 rack)。
async fn register_data_nodes(leader: &MasterNode) {
    for (i, id) in DATA_NODES.iter().enumerate() {
        let info = NodeInfo {
            dc: "dc1".into(),
            rack: format!("r{}", (b'A' + i as u8) as char),
            addr: format!("127.0.0.1:{id}"),
            disks: BTreeMap::from([(
                "d0".to_string(),
                DiskSpec {
                    capacity_bytes: DISK_CAPACITY,
                },
            )]),
            registered_at_ms: 1,
        };
        leader
            .raft
            .client_write(MasterCommand::RegisterNode { node_id: *id, info })
            .await
            .expect("register data node");
    }
}

/// 建一个 RF=3 分区,副本按给定节点落在 `d0`(绕过随机选盘,人为造倾斜)。
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
            partition_capacity: 0,
        })
        .await
        .expect("create partition");
    match resp.data {
        antfs_master::MasterResponse::PartitionId(id) => id,
        other => panic!("unexpected: {other:?}"),
    }
}

/// 造倾斜:3 个 RF=3 分区全落 101/102/103(各 3 副本),104 空 → 返回 [pid0, pid1, pid2]。
async fn make_skew(leader: &MasterNode) -> [u32; 3] {
    let mut pids = [0; 3];
    for slot in &mut pids {
        *slot = create_partition(leader, &[101, 102, 103]).await;
    }
    pids
}

fn beat(node_id: u64, replicas: Vec<ReplicaBeat>) -> antfs_master::pb::NodeBeat {
    antfs_master::pb::NodeBeat {
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

async fn send_beat(
    client: &mut MasterClient<tonic::transport::Channel>,
    node_id: u64,
    replicas: Vec<ReplicaBeat>,
) {
    let mut hb = client
        .heartbeat(tokio_stream::iter(vec![beat(node_id, replicas)]))
        .await
        .expect("hb")
        .into_inner();
    hb.message().await.expect("recv").expect("directive");
}

/// 模拟理想 store 的心跳:每节点上报"当前 placement 里它持有的全部副本",
/// 副本态由 `state_of(node_id)` 决定(104 报 BOOTSTRAP 可用来按住"后删"闸门)。
async fn drive_beats(
    client: &mut MasterClient<tonic::transport::Channel>,
    leader: &MasterNode,
    state_of: &dyn Fn(u64) -> &'static str,
) {
    let reports: Vec<(u64, Vec<ReplicaBeat>)> = {
        let g = leader.sm_data.read().expect("sm");
        DATA_NODES
            .into_iter()
            .map(|id| {
                let reps = g
                    .state
                    .partitions
                    .iter()
                    .filter(|(_, p)| p.replicas.iter().any(|r| r.node_id == id))
                    .map(|(pid, _)| ReplicaBeat {
                        partition_id: *pid,
                        disk_id: "d0".into(),
                        current_state: state_of(id).into(),
                        lag_bytes: 0,
                        used_bytes: used_of(*pid),
                    })
                    .collect();
                (id, reps)
            })
            .collect()
    };
    for (id, reps) in reports {
        send_beat(client, id, reps).await;
    }
}

async fn balance_state(
    client: &mut MasterClient<tonic::transport::Channel>,
) -> antfs_master::pb::BalanceStateResponse {
    client
        .get_balance_state(BalanceStateRequest { version: 1 })
        .await
        .expect("get_balance_state")
        .into_inner()
}

/// 无 label 指标的值(未注册/未出现 = None)。
fn gauge(node: &MasterNode, name: &str) -> Option<i64> {
    let dump = antfs_metric::gather(&node.registry).ok()?;
    antfs_metric::parse(&dump)
        .into_iter()
        .find(|s| s.name == name && s.labels.is_empty())
        .map(|s| s.value as i64)
}

/// 任一 master 的 Registry 上某无 label 指标达到 `want`(打点由时任 leader 完成,切主后跨节点找)。
fn any_node_has(nodes: &[MasterNode], name: &str, want: i64) -> bool {
    nodes.iter().any(|n| gauge(n, name) == Some(want))
}

/// 每节点当前持有的分区集合(按 placement)。
fn holdings(leader: &MasterNode) -> BTreeMap<u64, Vec<u32>> {
    let g = leader.sm_data.read().expect("sm");
    DATA_NODES
        .into_iter()
        .map(|id| {
            let mut pids: Vec<u32> = g
                .state
                .partitions
                .iter()
                .filter(|(_, p)| p.replicas.iter().any(|r| r.node_id == id))
                .map(|(pid, _)| *pid)
                .collect();
            pids.sort_unstable();
            (id, pids)
        })
        .collect()
}

/// eval →(plan_id 校验)→ start → 按并发上限逐条推进 → 全部 done、分布变均衡。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balance_eval_start_executes_to_done() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 300).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader).await;
    let pids = make_skew(leader).await;
    let all_active = |_| "ACTIVE";
    drive_beats(&mut client, leader, &all_active).await;

    // 非 leader:回 leader_addr 供重定向。
    let follower = nodes
        .iter()
        .find(|n| n.config.node_id != leader.config.node_id)
        .expect("follower");
    let mut fclient = connect(follower).await;
    // leader 的自报注册(leader_addr 唯一真值)经 Raft 异步收敛 —— 轮询带 deadline,禁 sleep(§5.6)。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let f = loop {
        let f = fclient
            .eval_balance(EvalBalanceRequest {
                version: 1,
                cluster_secret: SECRET.into(),
                max_moves: 0,
            })
            .await
            .expect("eval on follower")
            .into_inner();
        assert!(
            !f.ok && f.error == "not leader",
            "got ok={} err={}",
            f.ok,
            f.error
        );
        if !f.leader_addr.is_empty() || tokio::time::Instant::now() >= deadline {
            break f;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert_eq!(
        f.leader_addr,
        leader.bound_grpc_addr().expect("bound grpc"),
        "follower must redirect to the leader (gRPC 唯一真值,master-leader-address)"
    );

    // max_moves=1 → 截断为 1 条(顺带覆盖截断语义;随后重新 eval 覆盖该计划)。
    let e1 = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 1,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(e1.ok, "eval must succeed: {}", e1.error);
    assert_eq!(
        e1.plan.as_ref().expect("plan").total,
        1,
        "max_moves must truncate"
    );

    // 正式评估:9 副本 4 节点(3,3,3,0)→ 恰 2 条 move(2,2,3,2 即收敛,改善护栏拦下第 3 条)。
    let e = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 0,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(e.ok, "eval must succeed: {}", e.error);
    let plan = e.plan.expect("plan");
    assert_eq!(plan.status, "PREVIEW");
    assert_eq!(
        (
            plan.total,
            plan.pending,
            plan.in_flight,
            plan.done,
            plan.failed
        ),
        (2, 2, 0, 0, 0)
    );
    // 确定性:源盘按 node asc、同盘按占用 desc → (pid2: 101→104),(pid1: 102→104)。
    assert_eq!(
        (
            plan.moves[0].partition_id,
            plan.moves[0].from_node_id,
            plan.moves[0].to_node_id
        ),
        (pids[2], 101, 104)
    );
    assert_eq!(
        (
            plan.moves[1].partition_id,
            plan.moves[1].from_node_id,
            plan.moves[1].to_node_id
        ),
        (pids[1], 102, 104)
    );
    assert!(plan.moves.iter().all(|m| m.from_disk_id == "d0"
        && m.to_disk_id == "d0"
        && m.status == "pending"
        && m.error.is_empty()));

    // plan_id 不匹配 → 拒绝(防陈旧确认)。
    let s = client
        .start_balance(StartBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id + 1,
        })
        .await
        .expect("start")
        .into_inner();
    assert!(
        !s.ok && s.error.contains("not found"),
        "got ok={} err={}",
        s.ok,
        s.error
    );

    // 确认启动。
    let s = client
        .start_balance(StartBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id,
        })
        .await
        .expect("start")
        .into_inner();
    assert!(s.ok, "start must succeed: {}", s.error);

    // 推进:轮询驱动心跳(副本即时报 ACTIVE),全程断言在途 ≤ max_in_flight(=1)。
    let start = Instant::now();
    let mut saw_in_flight = false;
    let mut final_state = None;
    while start.elapsed() < Duration::from_secs(40) {
        drive_beats(&mut client, leader, &all_active).await;
        let st = balance_state(&mut client).await;
        assert!(st.present, "plan must stay present while running");
        let p = st.plan.expect("plan");
        assert!(
            p.in_flight <= 1,
            "max_in_flight=1 must cap concurrency, got {}",
            p.in_flight
        );
        saw_in_flight |= p.in_flight == 1;
        if p.status == "DONE" {
            final_state = Some(p);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let p = final_state.expect("plan must reach DONE within timeout");
    assert!(
        saw_in_flight,
        "at least one move must have been observed in flight"
    );
    assert_eq!(
        (p.done, p.failed, p.pending, p.in_flight),
        (2, 0, 0, 0),
        "all moves done: {p:?}"
    );
    assert!(p.moves.iter().all(|m| m.status == "done"));

    // 分布收敛:pid2 从 101、pid1 从 102 迁到 104 → 每节点 2,2,3,2(不可整除均分的收敛态)。
    let h = holdings(leader);
    assert_eq!(h[&101], vec![pids[0], pids[1]]);
    assert_eq!(h[&102], vec![pids[0], pids[2]]);
    assert_eq!(h[&103], vec![pids[0], pids[1], pids[2]]);
    assert_eq!(h[&104], vec![pids[1], pids[2]]);
    // A6 记账清理生效:moving / pending_drop 全空。
    {
        let g = leader.sm_data.read().expect("sm");
        assert!(
            g.state.moving.is_empty(),
            "moving must drain, got {:?}",
            g.state.moving
        );
        assert!(g.state.pending_drop.is_empty(), "pending_drop must drain");
    }

    // 指标:gauge 归零 + 完成计数 = 2(reconciler 周期刷新,轮询等待)。
    let start = Instant::now();
    loop {
        let gauges_ok = gauge(leader, "master_balance_pending") == Some(0)
            && gauge(leader, "master_balance_in_flight") == Some(0);
        if gauges_ok && any_node_has(&nodes, "master_balance_moves_total", 2) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "balance metrics must converge, dump:\n{}",
            antfs_metric::gather(&leader.registry).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !any_node_has(&nodes, "master_balance_failed_total", 1),
        "no move may fail"
    );
}

/// cancel:计划清空(present=false)、在途 move **不回滚**、由既有 reconciler 自然收敛。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balance_cancel_clears_plan_and_in_flight_converges() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 300).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader).await;
    let pids = make_skew(leader).await;
    // 104 上的新副本只报 BOOTSTRAP(追平中)→ "后删"闸门按住,move 停在在途。
    let target_bootstrapping = |id: u64| if id == 104 { "BOOTSTRAP" } else { "ACTIVE" };
    drive_beats(&mut client, leader, &target_bootstrapping).await;

    let e = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 0,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(e.ok, "eval: {}", e.error);
    let plan = e.plan.expect("plan");
    let s = client
        .start_balance(StartBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id,
        })
        .await
        .expect("start")
        .into_inner();
    assert!(s.ok, "start: {}", s.error);

    // 等第一条 move 在途(max_in_flight=1 → 恰 1 条 in_flight + 1 条 pending)。
    let start = Instant::now();
    loop {
        drive_beats(&mut client, leader, &target_bootstrapping).await;
        let p = balance_state(&mut client).await.plan.expect("plan");
        if p.in_flight == 1 {
            assert_eq!(p.pending, 1, "second move must wait for the budget");
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "first move must go in flight"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let in_flight_pid = pids[2]; // 第一条 move(见上一测试的确定性顺序)
    assert!(
        leader
            .sm_data
            .read()
            .expect("sm")
            .state
            .moving
            .contains(&(in_flight_pid, 101)),
        "move must be accepted (source marked moving-out)"
    );

    // plan_id 不匹配 → 拒绝;匹配 → 清计划。
    let c = client
        .cancel_balance(CancelBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id + 9,
        })
        .await
        .expect("cancel")
        .into_inner();
    assert!(
        !c.ok && c.error.contains("not found"),
        "got ok={} err={}",
        c.ok,
        c.error
    );
    let c = client
        .cancel_balance(CancelBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id,
        })
        .await
        .expect("cancel")
        .into_inner();
    assert!(c.ok, "cancel: {}", c.error);
    assert!(
        !balance_state(&mut client).await.present,
        "cancel must clear the plan"
    );

    // 计划已清:第二条 move 不再推进(仍 pending 的事不会发生——在途那条继续自然收敛)。
    // 让 104 追平(报 ACTIVE)→ 在途 move 走既有"后删 + DROPPED + 记账清理"收敛。
    let all_active = |_| "ACTIVE";
    let start = Instant::now();
    loop {
        drive_beats(&mut client, leader, &all_active).await;
        let h = holdings(leader);
        let converged = {
            let g = leader.sm_data.read().expect("sm");
            h[&104] == vec![in_flight_pid]
                && !h[&101].contains(&in_flight_pid)
                && g.state.moving.is_empty()
                && g.state.pending_drop.is_empty()
        };
        if converged {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "in-flight move must converge naturally after cancel; holdings={:?}",
            holdings(leader)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // 计划保持清空;第二条 move 从未投递:102 仍持有 pid1,104 只有 pid2。
    assert!(!balance_state(&mut client).await.present);
    let h = holdings(leader);
    assert!(
        h[&102].contains(&pids[1]),
        "pending move must NOT have been dispatched after cancel"
    );
    assert!(!h[&104].contains(&pids[1]));
}

/// 无倾斜:eval 返回 ok + 空 moves;start 后空计划立即收官 DONE。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balance_eval_without_skew_returns_empty_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 300).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader).await;
    // 4 个 RF=3 分区轮转放置:每节点恰 3 副本,无倾斜。
    for replicas in [
        [101, 102, 103],
        [102, 103, 104],
        [103, 104, 101],
        [104, 101, 102],
    ] {
        create_partition(leader, &replicas).await;
    }
    let all_active = |_| "ACTIVE";
    drive_beats(&mut client, leader, &all_active).await;

    let e = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 0,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(
        e.ok,
        "eval must succeed even with nothing to do: {}",
        e.error
    );
    let plan = e.plan.expect("plan");
    assert_eq!(plan.status, "PREVIEW");
    assert_eq!(plan.total, 0, "uniform cluster must yield an empty plan");
    assert!(plan.moves.is_empty());

    let s = client
        .start_balance(StartBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id,
        })
        .await
        .expect("start")
        .into_inner();
    assert!(s.ok, "start: {}", s.error);

    let start = Instant::now();
    loop {
        drive_beats(&mut client, leader, &all_active).await;
        let p = balance_state(&mut client).await.plan.expect("plan");
        if p.status == "DONE" {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "empty plan must complete immediately"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// stuck:在途超 `balance_move_timeout_secs` 未摘源 → 判败、释放预算、不再重试;全部判败后计划 DONE。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balance_stuck_move_fails_and_plan_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 2).await; // 在途 2s 未摘源即判败
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader).await;
    make_skew(leader).await;
    // 104 的新副本永远 BOOTSTRAP(追不平)→ 在途卡死。
    let target_bootstrapping = |id: u64| if id == 104 { "BOOTSTRAP" } else { "ACTIVE" };
    drive_beats(&mut client, leader, &target_bootstrapping).await;

    let e = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 0,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(e.ok, "eval: {}", e.error);
    let plan = e.plan.expect("plan");
    assert_eq!(plan.total, 2);
    let s = client
        .start_balance(StartBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            plan_id: plan.plan_id,
        })
        .await
        .expect("start")
        .into_inner();
    assert!(s.ok, "start: {}", s.error);

    // 两条 move 依次:在途 → 超时判败(预算释放)→ 下一条投递 → 同样判败 → DONE(failed=2)。
    let start = Instant::now();
    let mut final_plan = None;
    while start.elapsed() < Duration::from_secs(40) {
        drive_beats(&mut client, leader, &target_bootstrapping).await;
        let p = balance_state(&mut client).await.plan.expect("plan");
        if p.status == "DONE" {
            final_plan = Some(p);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let p = final_plan.expect("plan must reach DONE via failures");
    assert_eq!(
        (p.failed, p.done),
        (2, 0),
        "both moves must fail as stuck: {p:?}"
    );
    assert!(p
        .moves
        .iter()
        .all(|m| m.status == "failed" && m.error.contains("stuck")));

    let start = Instant::now();
    while !any_node_has(&nodes, "master_balance_failed_total", 2) {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "failed counter must reach 2"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !any_node_has(&nodes, "master_balance_moves_total", 1),
        "no move completed"
    );
}

/// 手动 `MoveReplica` 落进"搬迁中间态"镜头(admin-balance-midstate 核心契约):
/// 无计划(present=false)也必须返回 moving 条目 → 状态 `unplanned`、继任者如实;
/// 重新 eval 出 PREVIEW 计划也不认领它(planner 跳过 moving 中的分区,镜头不撒谎);
/// 全员 ACTIVE 后走既有"后删 + ConfirmDrop"自然收敛 → 条目清零、placement 如愿搬完。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_move_visible_as_unplanned_then_converges_to_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let nodes = start_masters(tmp.path(), 300).await;
    let leader = leader_of(&nodes);
    let mut client = connect(leader).await;

    register_data_nodes(leader).await;
    let pids = make_skew(leader).await;
    let all_active = |_| "ACTIVE";
    drive_beats(&mut client, leader, &all_active).await;

    // 无计划时手动搬 pids[0]:101 → 104。
    let m = client
        .move_replica(MoveReplicaRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            partition_id: pids[0],
            from_node_id: 101,
            to_node_id: 104,
        })
        .await
        .expect("move")
        .into_inner();
    assert!(m.ok, "manual move: {}", m.error);
    assert_eq!(m.to_node_id, 104);

    // 镜头:present=false(无计划)但 moving 恰 1 条,unplanned、to=104。
    // (此刻 104 对新副本尚无心跳上报 → 后删闸门按住,条目短期稳定。)
    let s = balance_state(&mut client).await;
    assert!(!s.present, "no balance plan exists yet");
    assert_eq!(s.moving.len(), 1, "manual move must be visible");
    let e = &s.moving[0];
    assert_eq!(e.to_node_id, 104, "successor node as recorded");
    assert_eq!(e.from_node_id, 101);
    assert_eq!(e.partition_id, pids[0]);
    assert_eq!(e.status, "unplanned", "plan-outside entry: manual move");
    assert!(e.error.is_empty());

    // 重新 eval:计划必须跳过 moving 中的分区(重 eval 清不掉搬运中条目)→ 呈 PREVIEW 计划,
    // 但镜头里这条 move 仍是 unplanned(PREVIEW 未投递不认领)。
    let ev = client
        .eval_balance(EvalBalanceRequest {
            version: 1,
            cluster_secret: SECRET.into(),
            max_moves: 0,
        })
        .await
        .expect("eval")
        .into_inner();
    assert!(ev.ok, "eval: {}", ev.error);
    let plan = ev.plan.expect("preview plan");
    assert!(
        plan.moves.iter().all(|m| m.partition_id != pids[0]),
        "planner must skip partitions currently in the moving set"
    );
    let s = balance_state(&mut client).await;
    assert!(s.present);
    assert_eq!(s.moving.len(), 1);
    assert_eq!(
        s.moving[0].status, "unplanned",
        "a PREVIEW plan has not dispatched anything and must not claim the entry"
    );

    // 收敛:全员 ACTIVE(104 补上即报 ACTIVE = 追平)→ 后删摘源 → DROPPED → 记账清空。
    let start = Instant::now();
    loop {
        drive_beats(&mut client, leader, &all_active).await;
        let converged = {
            let g = leader.sm_data.read().expect("sm");
            g.state.moving.is_empty() && g.state.pending_drop.is_empty()
        };
        if converged {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "manual move must converge naturally; moving/pending_drop stuck"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let s = balance_state(&mut client).await;
    assert!(
        s.moving.is_empty(),
        "the lens reflects hard state: converged ⟹ empty"
    );
    let h = holdings(leader);
    assert!(!h[&101].contains(&pids[0]), "source dropped");
    assert!(h[&104].contains(&pids[0]), "successor holds the replica");
}

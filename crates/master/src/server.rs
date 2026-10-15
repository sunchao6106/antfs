//! master 节点装配 + gRPC 服务实现(控制面 `Master` + `RaftInternal`)。
//!
//! - `RaftInternal`:承接对端 master 的 Raft RPC,反序列化 payload → 交本地 `Raft` → 序列化结果。
//! - `Master`:数据节点接入(自注册 Q1 + 准入校验)、心跳(软状态重建)、只读查询。
//! - 只有 leader 能提交写(RegisterNode);follower 回 leader 地址供 client 重定向。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::{BasicNode, Config, Raft};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::config::MasterConfig;
use crate::idalloc::IdAllocator;
use crate::metrics::MasterMetrics;
use crate::network::GrpcNetworkFactory;
use crate::pb::master_server::{Master, MasterServer};
use crate::pb::raft_internal_server::{RaftInternal, RaftInternalServer};
use crate::pb::{
    AssignIncarnationRequest, AssignIncarnationResponse, AssignRequest, AssignResponse,
    BalanceStateRequest, BalanceStateResponse, CancelBalanceRequest, CancelBalanceResponse,
    ClusterState, ClusterStateRequest, CreatePartitionsRequest, CreatePartitionsResponse,
    DecommissionRequest, DecommissionResponse, EvalBalanceRequest, EvalBalanceResponse,
    GetBlobIdsRequest, GetBlobIdsResponse, ListNodesRequest, ListPartitionsRequest,
    ListPartitionsResponse, LookupRequest, MasterDirective, MetricsRequest, MetricsResponse,
    MoveReplicaRequest, MoveReplicaResponse, NodeBeat, NodeEntry, NodeList, PartitionEntry,
    PartitionLocations, RaftMessage, RegisterRequest, RegisterResponse, RepairPartitionRequest,
    RepairPartitionResponse, ReplicaCurrent, ReplicaLoc, SetMaintenanceRequest,
    SetMaintenanceResponse, SetPartitionStateRequest, SetPartitionStateResponse,
    StartBalanceRequest, StartBalanceResponse,
};
use crate::placement::{select_disks, usable_capacity, DiskFree, PlacementError};
use crate::reconcile::{
    drop_confirmed, plan_evacuation, plan_move, plan_partition_states, plan_rereplication,
    plan_transitions, tombstones_reclaim_allowed, EvacAction, SuccessorConfirm,
};
use crate::soft::{DiskUsage, SoftState};
use crate::store::{open, StateMachineData};
use crate::types::{
    BalancePlanRecord, BalanceStatus, DiskId, DiskSpec, MasterCommand, MasterState, NodeId,
    NodeInfo, PartitionId, PartitionState, ReplicaPlacement, TypeConfig,
};

const PROTO_VERSION: u32 = 1;

/// 删除源副本的指令目标态(A6):源节点收到即删本地副本(含目录)。
///
/// proto 不改(`Transition.target_state` 复用新取值);只在三条确认全满足后才下发,见
/// [`MasterService::heartbeat`] 与 [`drop_confirmed`]。
const DROPPED: &str = "DROPPED";

/// 选盘输入:软状态(有效余量 + 盘健康)+ Raft partition table(逻辑承诺)→ [`DiskFree`] 快照
///(re-replication / repair / move / 建分区共用)。
///
/// - **物理余量**:取 `adjusted_free`(心跳原始值叠加建分区即时扣减,见 [`SoftState::record_beat`])。
/// - **逻辑承诺**(D1):扫描 `state.partitions`,把每个副本的 `capacity_bytes` 累加到其所在盘。
///   RW + RO + SEALED 全计入(DROPPED 已从 table 移除);`capacity_bytes == 0`(旧分区)→ 回退
///   `config.partition_capacity_bytes`。
/// - **可用容量上限**:`capacity × (1 − reserved_ratio)`(见 [`usable_capacity`])。
///
/// 盘健康由此进入放置决策:非空且 ≠ `HEALTHY` 的盘会被 `select_disks` 排除(见其文档)。
fn placement_usage(
    soft: &SoftState,
    state: &MasterState,
    config: &MasterConfig,
) -> BTreeMap<(NodeId, DiskId), DiskFree> {
    let used_map = soft.used_bytes_by_partition();
    let mut committed: HashMap<(NodeId, DiskId), u64> = HashMap::new();
    for (pid, info) in &state.partitions {
        let cap = if info.capacity_bytes > 0 {
            info.capacity_bytes
        } else {
            config.partition_capacity_bytes
        };
        let contribution = match info.state {
            PartitionState::ReadWrite => cap,
            PartitionState::ReadOnly | PartitionState::Sealed => match used_map.get(pid) {
                Some(&used) if used > 0 => used,
                _ => cap,
            },
        };
        for r in &info.replicas {
            let e = committed.entry((r.node_id, r.disk_id.clone())).or_insert(0);
            *e = (*e).saturating_add(contribution);
        }
    }
    soft.usage_snapshot()
        .into_iter()
        .map(|(k, u)| {
            (
                k.clone(),
                DiskFree {
                    free_bytes: u.adjusted_free,
                    health: u.health,
                    committed_bytes: committed.get(&k).copied().unwrap_or(0),
                    usable_capacity: usable_capacity(u.capacity_bytes, config.disk_reserved_ratio),
                },
            )
        })
        .collect()
}

/// 一块盘的已用比例(`free/capacity`;capacity=0 视为不可判 → `None`)。
fn used_ratio(u: &DiskUsage) -> Option<f64> {
    (u.capacity_bytes > 0).then(|| 1.0 - (u.free_bytes as f64 / u.capacity_bytes as f64))
}

/// 分区级"将满"判定:分区已用 ≥ `capacity − segment_capacity` 即应转 RO。
///
/// 阈值留出**一个段容量**:当前活跃段封存/滚动仍需整段空间,逼近上限时提前止损。`capacity == 0`
/// (未配上限,无意见)返回 `false`,不触发。
fn should_convert_to_ro(capacity_bytes: u64, segment_capacity: u64, max_used: u64) -> bool {
    if capacity_bytes == 0 {
        return false;
    }
    max_used >= capacity_bytes.saturating_sub(segment_capacity)
}

/// 写权重的上界(千分比):1000 = 该分区完全空。下界固定 1(见 [`capacity_weight_permille`])。
const WEIGHT_PERMILLE_MAX: u32 = 1000;

/// 分区的**写权重**(千分比,`1..=1000`)= 剩余容量占分区容量上限的比例(1000 = 完全空,越接近 1 越满)。
///
/// 两处用途**共用这一个定义**(否则集群里会出现两套自相矛盾的"容量权重"):
/// ① 发号时 `partition_id` 缺省 → [`MasterService::pick_writable_partition`] 按它加权随机挑分区;
/// ② [`MasterService::list_partitions`] 把它下发给 client(`PartitionEntry.write_weight`)——
///    client(router SDK)只当**相对**权重用,不解释容量语义。
///
/// - **下限 1**:上报为满/超卖(余量 0)的分区仍留一丝机会,不被永久饿死;正因如此 `0` 可以让给
///   "未下发/无意见"(`cap == 0` 即未配容量上限时返回 1000 = 无意见),二者不冲突。
/// - 口径:`used` 由调用方按"每分区取各副本最大值"给(见 `SoftState::used_bytes_by_partition`)。
fn capacity_weight_permille(cap: u64, used: u64) -> u32 {
    if cap == 0 {
        return WEIGHT_PERMILLE_MAX;
    }
    // u128 做乘法:cap 取到 u64 上界时 remaining × 1000 会溢出 u64。
    let ratio =
        u128::from(cap.saturating_sub(used)) * u128::from(WEIGHT_PERMILLE_MAX) / u128::from(cap);
    u32::try_from(ratio)
        .unwrap_or(WEIGHT_PERMILLE_MAX)
        .clamp(1, WEIGHT_PERMILLE_MAX)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 轻量伪随机(u64,仅用于"按权重挑分区"这类散列用途,**非安全用途**)。
///
/// 不引入 `rand` 依赖:时间 + 自增序号经 splitmix/xorshift 混合 —— 同一毫秒内的连续调用也得到不同值
/// (序号不同),避免高并发下所有请求都落到同一个分区。
fn pseudo_random() -> u64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut x = now_ms() ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 29;
    x
}

/// placement 派生索引(M7):`node -> placement 中该节点涉及的分区集`,epoch 懒重建。
///
/// 心跳每拍的 `plan_transitions` 需要"该节点在 placement 里承载哪些分区";从 `MasterState.partitions`
/// 现算是 O(全集群)/拍。这里缓存一份反查索引,只在 `placement_epoch`(见
/// [`crate::store::RedbStateMachine`])变化后第一拍重建一次:稳态零重建、每拍 O(节点),
/// 变更后摊还一次 O(集群) 重建。**不进 Raft 状态**(纯派生,可从 partitions 重建)。
/// 索引本体:(重建时纪元, node -> 分区集)。
type IndexInner = Arc<RwLock<(u64, HashMap<NodeId, BTreeSet<PartitionId>>)>>;

#[derive(Clone, Default)]
pub struct PlacementIndex {
    inner: IndexInner,
}

impl PlacementIndex {
    /// 取某节点的 placement 分区集;`epoch` 与缓存不一致时按 `state` 重建(调用方持 sm_data 读锁)。
    fn node_pids(&self, node: NodeId, epoch: u64, state: &MasterState) -> BTreeSet<PartitionId> {
        {
            let g = self.inner.read().expect("placement index lock poisoned");
            if g.0 == epoch {
                return g.1.get(&node).cloned().unwrap_or_default();
            }
        }
        let mut g = self.inner.write().expect("placement index lock poisoned");
        if g.0 != epoch {
            let mut by_node: HashMap<NodeId, BTreeSet<PartitionId>> = HashMap::new();
            for (pid, info) in &state.partitions {
                for r in &info.replicas {
                    by_node.entry(r.node_id).or_default().insert(*pid);
                }
            }
            *g = (epoch, by_node);
        }
        g.1.get(&node).cloned().unwrap_or_default()
    }
}

/// 装配好的 master 节点:持有 Raft 句柄、共享状态机内存态、软状态、指标、配置。
#[derive(Clone)]
pub struct MasterNode {
    pub raft: Raft<TypeConfig>,
    pub sm_data: Arc<RwLock<StateMachineData>>,
    pub soft: SoftState,
    pub metrics: MasterMetrics,
    pub config: MasterConfig,
    /// 指标注册表(GetMetrics 导出用;与 `metrics` 接入的同一 `Registry`)。
    pub registry: antfs_metric::Registry,
    /// placement 变更纪元(状态机 apply 侧 bump;派生索引按它懒重建)。
    pub placement_epoch: Arc<AtomicU64>,
    /// placement 派生索引(心跳侧 `node -> pids`;见 [`PlacementIndex`])。
    pub placement_index: PlacementIndex,
}

impl MasterNode {
    /// 打开存储、构建 Raft、装配节点(不建 gRPC 监听;监听见 [`serve`](Self::serve))。
    pub async fn start(
        config: MasterConfig,
        registry: &antfs_metric::Registry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let metrics = MasterMetrics::new(registry)?;
        std::fs::create_dir_all(&config.data_dir)?;
        let path = std::path::Path::new(&config.data_dir).join("master.redb");
        let (log_store, sm) = open(&path, metrics.clone())?;
        let sm_data = sm.shared_data();
        let placement_epoch = sm.placement_epoch();

        let raft_config = Arc::new(
            Config {
                cluster_name: "ambry-master".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 500,
                election_timeout_max: 1000,
                ..Default::default()
            }
            .validate()?,
        );
        let raft = Raft::new(
            config.node_id,
            raft_config,
            GrpcNetworkFactory,
            log_store,
            sm,
        )
        .await?;

        Ok(MasterNode {
            raft,
            sm_data,
            soft: SoftState::default(),
            metrics,
            config,
            registry: registry.clone(),
            placement_epoch,
            placement_index: PlacementIndex::default(),
        })
    }

    /// 用给定成员集初始化一个全新集群(在其中一个节点上调一次即可)。
    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.raft.initialize(members).await?;
        Ok(())
    }

    /// 绑定并后台运行两个 gRPC 服务:`RaftInternal`(raft_addr)与 `Master`(grpc_addr)。
    pub fn serve(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let raft_addr: std::net::SocketAddr = self.config.raft_addr.parse()?;
        let grpc_addr: std::net::SocketAddr = self.config.grpc_addr.parse()?;

        let raft_svc = RaftInternalServer::new(RaftInternalService {
            raft: self.raft.clone(),
        });
        let master_svc = MasterServer::new(MasterService {
            raft: self.raft.clone(),
            sm_data: self.sm_data.clone(),
            soft: self.soft.clone(),
            metrics: self.metrics.clone(),
            config: self.config.clone(),
            registry: self.registry.clone(),
            id_alloc: IdAllocator::default(),
            placement_epoch: self.placement_epoch.clone(),
            placement_index: self.placement_index.clone(),
        });

        // 绑定端口必须在 serve() 返回前完成:spawn 后再 bind 曾留下「服务已启动但端口未监听」
        // 窗口,客户端首个懒连接 RPC 撞 ConnectionRefused → 503(CI 偶发第二族根因,DEVLOG 50)。
        let raft_listener = std::net::TcpListener::bind(raft_addr)?;
        raft_listener.set_nonblocking(true)?;
        let grpc_listener = std::net::TcpListener::bind(grpc_addr)?;
        grpc_listener.set_nonblocking(true)?;
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(raft_listener)
                    .expect("serve() 须在 tokio runtime 上下文中调用"),
            );
            let _ = Server::builder()
                .add_service(raft_svc)
                .serve_with_incoming(incoming)
                .await;
        });
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(
                tokio::net::TcpListener::from_std(grpc_listener)
                    .expect("serve() 须在 tokio runtime 上下文中调用"),
            );
            let _ = Server::builder()
                .add_service(master_svc)
                .serve_with_incoming(incoming)
                .await;
        });

        // 后台把 raft 的 term/leader 同步到 gauge(观测软状态,不进 Raft)。
        let mut rx = self.raft.metrics();
        let metrics = self.metrics.clone();
        let this_node = self.config.node_id;
        tokio::spawn(async move {
            loop {
                {
                    let m = rx.borrow_and_update();
                    metrics.raft_term.set(m.current_term as i64);
                    metrics
                        .is_leader
                        .set(i64::from(m.current_leader == Some(this_node)));
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });

        if self.config.auto_grow_enabled {
            self.spawn_auto_grow();
        }
        self.spawn_reconciler();
        Ok(())
    }

    /// Q5 自动扩容后台 loop:仅 leader 生效,按低水位补建分区,护栏见 [`crate::autogrow::plan_auto_grow`]。
    fn spawn_auto_grow(&self) {
        let raft = self.raft.clone();
        let sm_data = self.sm_data.clone();
        let soft = self.soft.clone();
        let metrics = self.metrics.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(Duration::from_secs(config.auto_grow_interval_secs));
            loop {
                tick.tick().await;
                if raft.metrics().borrow().current_leader != Some(config.node_id) {
                    continue; // 仅 leader 决策扩容
                }
                let (rw, total, rf, maintenance, pause_autogrow) = {
                    let g = match sm_data.read() {
                        Ok(g) => g,
                        Err(_) => continue,
                    };
                    let rw = g
                        .state
                        .partitions
                        .values()
                        .filter(|p| p.state == PartitionState::ReadWrite)
                        .count() as u64;
                    (
                        rw,
                        g.state.partitions.len() as u64,
                        g.state.policy.default_replication_factor,
                        g.state.maintenance,
                        g.state.pause_autogrow,
                    )
                };
                // 维护模式仅在显式要求连带暂停扩容时停(默认 false = 维护不影响扩容)。
                if maintenance && pause_autogrow {
                    continue;
                }
                let n = crate::autogrow::plan_auto_grow(
                    rw,
                    total,
                    config.auto_grow_min_writable,
                    config.auto_grow_max_partitions,
                    config.auto_grow_batch,
                );
                for _ in 0..n {
                    if let Err(e) =
                        place_one_partition(&raft, &sm_data, &soft, &config, &metrics, rf).await
                    {
                        tracing::warn!(error = %e, "auto-grow: create partition failed");
                        break; // 选盘/提交失败,等下个周期再试(不空转)
                    }
                }
            }
        });
    }

    /// M3 调谐后台 loop:仅 leader,失联检测 → 再复制补副本(承 §7.3)。
    fn spawn_reconciler(&self) {
        let raft = self.raft.clone();
        let sm_data = self.sm_data.clone();
        let soft = self.soft.clone();
        let metrics = self.metrics.clone();
        let config = self.config.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(Duration::from_secs(config.reconcile_interval_secs));
            let timeout = Duration::from_secs(config.heartbeat_timeout_secs);
            let grace = Duration::from_secs(config.reconcile_startup_grace_secs);
            let mut leader_since: Option<Instant> = None;
            let mut last_unsatisfiable: std::collections::BTreeSet<PartitionId> =
                std::collections::BTreeSet::new();
            // 均衡执行器的 leader 本地簿记(不进 Raft):在途计时与终态计数都只需 leader 侧口径,
            // 切主后由新 leader 重建(计时重新起算、已在终态的条目不重复计数)。
            let mut balance_dispatched_at: HashMap<(u64, PartitionId, NodeId), Instant> =
                HashMap::new();
            let mut balance_counted: std::collections::HashSet<(u64, PartitionId, NodeId)> =
                std::collections::HashSet::new();
            let mut balance_plan_seen: Option<u64> = None;
            // D5 判活迟滞的连击状态(`node -> 连续多少拍观察到心跳过期`)。leader 本地簿记:
            // 切主后新 leader 从零重算(反正它也要先过稳定期),不需要进 Raft。
            let mut liveness_misses: HashMap<NodeId, u64> = HashMap::new();
            // 上一拍判定为 up 的集合:只用来算"本拍**新**判死了几个"(计数不重复刷)。
            let mut prev_up: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            // D4 持续证据:`(node,disk) -> (最近健康值, 该值自何时起未变)`。值一变就重置计时。
            let mut disk_health_since: std::collections::BTreeMap<
                (NodeId, DiskId),
                (String, Instant),
            > = std::collections::BTreeMap::new();
            // D4 在途撤离:`已为哪些分区开过撤离`(坏盘上那份副本还在 ⇒ 仍未走完)。leader 本地簿记。
            let mut evac_started: std::collections::BTreeSet<PartitionId> =
                std::collections::BTreeSet::new();
            loop {
                tick.tick().await;
                if raft.metrics().borrow().current_leader != Some(config.node_id) {
                    leader_since = None; // 卸任 leader:下次重新计稳定期
                    last_unsatisfiable.clear();
                    liveness_misses.clear();
                    prev_up.clear();
                    continue;
                }
                // 启动/切主稳定期:软状态靠心跳从零重建,期内不据"暂无心跳"误判失联(防再复制风暴)。
                let since = *leader_since.get_or_insert_with(Instant::now);
                if !crate::reconcile::reconcile_ready(since.elapsed(), grace) {
                    continue;
                }
                // D5:先拿**原始**判活(单窗口)与"本拍谁过期",再过迟滞得到**决策用**的 up 集。
                //
                // 两者分工刻意不同:**观测/展示**口径(上面的 gauge、`ListNodes` 的 up 字段)用原始的 ——
                // "最后一拍心跳什么时候来的"是事实;而**执行动作**(再复制/撤离/迁移后删)用迟滞后的 ——
                // 判死的代价是几万个分区一起搬数据,一次抖动不该触发它。
                let up_raw = soft.up_nodes(timeout);
                let known: Vec<NodeId> = soft.known_nodes().into_iter().collect();
                let expired: std::collections::HashSet<NodeId> = known
                    .iter()
                    .copied()
                    .filter(|n| !up_raw.contains(n))
                    .collect();
                let up = crate::reconcile::up_nodes_debounced(
                    known.iter().copied(),
                    &expired,
                    config.node_down_debounce_ticks,
                    &mut liveness_misses,
                );
                // 只在"本拍新判死"时计数(持续 down 不每拍重复计)。
                for n in prev_up.difference(&up) {
                    metrics.node_down.inc();
                    tracing::warn!(
                        node = *n,
                        debounce_ticks = config.node_down_debounce_ticks,
                        "reconcile: node declared down after liveness debounce"
                    );
                }
                prev_up = up.clone();
                let state = {
                    let g = match sm_data.read() {
                        Ok(g) => g,
                        Err(_) => continue,
                    };
                    g.state.clone()
                };
                let partitions = &state.partitions;
                let all_nodes = &state.nodes;
                let decom = &state.decommissioning;
                let moving = &state.moving;
                let move_targets = &state.move_targets;
                let pending_drop = state.pending_drop.clone();
                let maintenance = state.maintenance;
                let balance = state.balance.clone();

                // 补副本目标:失联再复制(M3)∪ 下线 evacuation 的"先补"(M4)∪ **坏盘撤离**的"先补"(D4)。
                // 动作相同,合并去重。
                let active = soft.active_replicas();
                let errored = soft.errored_replicas();
                let usage = soft.usage_snapshot();

                // ---- D4:坏盘抖动集的**持续证据**维护 ----
                //
                // 逐 tick 记"每个 (node,disk) 的健康值**从什么时候起没变过**":值一变就重置计时。
                // 于是 `pick_evicted_disks` 能回答"它保持 UNAVAILABLE 满 min_bad_secs 了吗" ——
                // 抖动中的盘永远攒不满(这是刻意的,见 design §6/§8)。
                {
                    let now = Instant::now();
                    let mut alive: std::collections::HashSet<(NodeId, DiskId)> =
                        std::collections::HashSet::new();
                    for ((node, disk), u) in &usage {
                        let key = (*node, disk.clone());
                        alive.insert(key.clone());
                        match disk_health_since.get_mut(&key) {
                            Some((h, since)) => {
                                if *h != u.health {
                                    *h = u.health.clone();
                                    *since = now;
                                }
                            }
                            None => {
                                disk_health_since.insert(key, (u.health.clone(), now));
                            }
                        }
                    }
                    // 盘从软状态里消失(节点注销/换盘):记账一并清掉,免得留下陈旧计时。
                    disk_health_since.retain(|k, _| alive.contains(k));
                }
                let evicted_disks: std::collections::BTreeSet<(NodeId, DiskId)> =
                    if config.disk_evac_enabled {
                        crate::reconcile::pick_evicted_disks(
                            &disk_health_since,
                            config.disk_evac_min_bad_secs,
                            Instant::now(),
                        )
                    } else {
                        std::collections::BTreeSet::new() // 关着 ⇒ 什么都不抖出(既有行为)
                    };
                let evac = plan_evacuation(partitions, decom, &evicted_disks, &up, &active);

                // 集群级观测 gauge(周期刷新,仅 leader;对齐 under_replicated_partitions 的更新时机)。
                {
                    let (mut n_rw, mut n_ro, mut n_sealed, mut n_majority_down) =
                        (0i64, 0i64, 0i64, 0i64);
                    for (pid, info) in partitions {
                        match info.state {
                            PartitionState::ReadWrite => n_rw += 1,
                            PartitionState::ReadOnly => n_ro += 1,
                            PartitionState::Sealed => n_sealed += 1,
                        }
                        // Ambry isMajorityReplicasDown:健康副本(存活且未上报 ERROR)≤ RF/2。
                        let healthy = info
                            .replicas
                            .iter()
                            .filter(|r| {
                                up.contains(&r.node_id) && !errored.contains(&(*pid, r.node_id))
                            })
                            .count();
                        if healthy * 2 <= info.replication_factor as usize {
                            n_majority_down += 1;
                        }
                    }
                    metrics.partitions_rw.set(n_rw);
                    metrics.partitions_ro.set(n_ro);
                    metrics.partitions_sealed.set(n_sealed);
                    let writable_capacity =
                        n_rw.saturating_mul(config.partition_capacity_bytes as i64);
                    metrics.writable_capacity_bytes.set(writable_capacity);
                    // 可写已用:每分区取各副本 used 最大值后求和(RF 副本不重复计,口径见 soft)。
                    let used = soft.sum_used_bytes(
                        partitions
                            .iter()
                            .filter(|(_, p)| p.state == PartitionState::ReadWrite)
                            .map(|(pid, _)| *pid),
                    ) as i64;
                    metrics.writable_used_bytes.set(used);
                    metrics
                        .writable_free_bytes
                        .set(writable_capacity.saturating_sub(used));
                    metrics.nodes_up.set(up.len() as i64);
                    let (capacity, free) = soft.capacity_totals();
                    metrics.cluster_free_bytes.set(free as i64);
                    metrics
                        .cluster_used_bytes
                        .set(capacity.saturating_sub(free) as i64);
                    metrics
                        .max_replica_lag_bytes
                        .set(soft.max_replica_lag_bytes() as i64);
                    metrics
                        .majority_replicas_down_partitions
                        .set(n_majority_down);
                    metrics.maintenance.set(i64::from(maintenance));
                    // A5:主动探测报出不健康的盘数(空串 = 未启用探测,不计)。
                    let unhealthy = usage
                        .values()
                        .filter(|u| !u.health.is_empty() && u.health != "HEALTHY")
                        .count();
                    metrics.unhealthy_disks.set(unhealthy as i64);
                    // A6:待删源副本对数(等源节点确认后下发 DROPPED)。
                    metrics.pending_drops.set(pending_drop.len() as i64);
                    // 每盘承载的副本数(hard placement 口径):已注册盘(含 0 副本)∪ placement 涉及的盘。
                    let mut per_disk: BTreeMap<(NodeId, DiskId), i64> = BTreeMap::new();
                    for (id, info) in all_nodes {
                        for disk_id in info.disks.keys() {
                            per_disk.insert((*id, disk_id.clone()), 0);
                        }
                    }
                    for info in partitions.values() {
                        for r in &info.replicas {
                            *per_disk.entry((r.node_id, r.disk_id.clone())).or_insert(0) += 1;
                        }
                    }
                    for ((node_id, disk_id), n) in &per_disk {
                        metrics
                            .replicas_by_disk
                            .with_label_values(&[&node_id.to_string(), disk_id])
                            .set(*n);
                    }
                    let skew = match (per_disk.values().max(), per_disk.values().min()) {
                        (Some(max), Some(min)) => max - min,
                        _ => 0,
                    };
                    metrics.disk_skew.set(skew);
                }

                let mut add_targets: std::collections::BTreeSet<PartitionId> =
                    plan_rereplication(partitions, &up, &errored)
                        .into_iter()
                        .collect();
                // D4 的"先补"受**在途预算**约束:在途 = 该分区在硬放置里**还带着坏盘上那个副本**
                // (搬迁未完成)。预算满了就把本拍新腾出来的名额让给已在途的那些,不再开新的。
                let evac_adds: std::collections::BTreeSet<PartitionId> = evac
                    .iter()
                    .filter_map(|a| match a {
                        EvacAction::Add(pid) => Some(*pid),
                        EvacAction::Remove(..) => None,
                    })
                    .collect();
                // 在途 = **本 leader 已为它开过撤离**、且坏盘上那份副本还在 placement 里(搬迁没走完)。
                // 用显式的 `evac_started` 而不是"包含被抖出盘的分区" —— 后者在**开出第一发之前**就已经成立,
                // 会把预算一开始就占满,导致永远开不出第一发(实测踩到)。
                evac_started.retain(|pid| {
                    partitions.get(pid).is_some_and(|info| {
                        info.replicas
                            .iter()
                            .any(|r| evicted_disks.contains(&(r.node_id, r.disk_id.clone())))
                    })
                });
                metrics.disk_evacuating.set(evac_started.len() as i64);
                let budget =
                    (config.disk_evac_max_in_flight as usize).saturating_sub(evac_started.len());
                let mut opened = 0usize;
                let mut newly_opened = 0usize;
                for pid in &evac_adds {
                    // 已经开过的**照旧重发**:上一发可能因为"当时没有健康盘可落"而失败,重发才能自愈;
                    // 它不占新名额(名额是"同时在途多少条",不是"发了多少次")。重发不会造成重复副本 —
                    // 一旦补上来的那份进了 placement,这个 pid 就不再出现在 `evac_adds` 里了。
                    if evac_started.contains(pid) {
                        add_targets.insert(*pid);
                        continue;
                    }
                    if opened < budget {
                        opened += 1;
                        newly_opened += 1;
                        evac_started.insert(*pid);
                        add_targets.insert(*pid);
                    }
                }
                if config.disk_evac_enabled && !evac_adds.is_empty() {
                    // `unsatisfiable`:本拍该补的比"还没开过的名额"多 —— 说明被预算挡住了(不是没盘可落),
                    // 持续为 `unsatisfiable` 就是"该把 disk_evac_max_in_flight 调大"的信号。
                    let result = if newly_opened + evac_started.len().min(evac_adds.len())
                        >= evac_adds.len()
                    {
                        "planned"
                    } else {
                        "unsatisfiable"
                    };
                    metrics.disk_evac_planned.with_label_values(&[result]).inc();
                }
                metrics
                    .under_replicated_partitions
                    .set(add_targets.len() as i64);

                // A4 盘将满 → 自动转 RO(**只置不回**,止损)。位置:gauge 刷新**之后**(让本周期观测先
                // 反映上一轮状态)、维护模式 skip **之前** —— 盘满会立刻挡住写入,这是止损而非"恢复动作",
                // 维护期也应生效(维护窗口换盘正是要它先兜住)。只在当前为 RW 时写,故天然幂等:转 RO 后
                // 下个周期不再命中,不会每 tick 空转 raft。空间释放后**不自动恢复 RW**(恢复靠运维
                // `POST /api/partitions/{id}/state`,避免"抖动即回"把写流量又引回将满的盘)。
                let mut ro_this_tick: std::collections::BTreeSet<PartitionId> =
                    std::collections::BTreeSet::new();
                for (pid, info) in partitions {
                    if info.state != PartitionState::ReadWrite {
                        continue;
                    }
                    // 任一副本所在盘用量超阈值即转(取第一个命中的,记日志用)。
                    let Some((node_id, disk_id, ratio)) = info.replicas.iter().find_map(|r| {
                        let u = usage.get(&(r.node_id, r.disk_id.clone()))?;
                        let ratio = used_ratio(u)?;
                        (ratio > config.disk_near_full_ratio)
                            .then(|| (r.node_id, r.disk_id.clone(), ratio))
                    }) else {
                        continue;
                    };
                    if raft
                        .client_write(MasterCommand::SetPartitionState {
                            partition_id: *pid,
                            state: PartitionState::ReadOnly,
                        })
                        .await
                        .is_ok()
                    {
                        metrics.partitions_forced_ro.inc();
                        ro_this_tick.insert(*pid);
                        tracing::warn!(
                            partition = *pid,
                            node = node_id,
                            disk = %disk_id,
                            used_ratio = ratio,
                            threshold = config.disk_near_full_ratio,
                            "磁盘将满:分区自动转只读(只置不回,恢复请走 SetPartitionState)"
                        );
                    }
                }

                // D4 分区级写满 → 转 RO(**只置不回**,同上)。与上面的盘级判定互补:盘级看整块盘的余量,
                // 分区级看单个分区自身的已用。已在本拍被盘级判定转过的分区跳过,避免重复提案。
                let used_map = soft.used_bytes_by_partition();
                for (pid, info) in partitions {
                    if info.state != PartitionState::ReadWrite || ro_this_tick.contains(pid) {
                        continue;
                    }
                    let cap = if info.capacity_bytes > 0 {
                        info.capacity_bytes
                    } else {
                        config.partition_capacity_bytes
                    };
                    let max_used = used_map.get(pid).copied().unwrap_or(0);
                    if !should_convert_to_ro(cap, config.segment_capacity_bytes, max_used) {
                        continue;
                    }
                    if raft
                        .client_write(MasterCommand::SetPartitionState {
                            partition_id: *pid,
                            state: PartitionState::ReadOnly,
                        })
                        .await
                        .is_ok()
                    {
                        metrics.partitions_full_ro.inc();
                        tracing::info!(
                            partition = *pid,
                            max_used,
                            threshold = cap.saturating_sub(config.segment_capacity_bytes),
                            capacity = cap,
                            "partition approaching capacity: converted to read-only"
                        );
                    }
                }

                // 维护模式(对标 Ceph noout/norecover):观测(上面的 gauge/欠副本数)照常刷新,但
                // **不执行自动动作**——再复制 / 迁移后删 / 下线 evacuation 全部跳过;心跳里的
                // plan_transitions 下发保持运行(否则 move 的新副本永不落地)。手动 ops 走 RPC,不受影响。
                if maintenance {
                    continue;
                }
                let mut unsatisfiable: std::collections::BTreeSet<PartitionId> =
                    std::collections::BTreeSet::new();
                // D6:每 tick 只发起前 K 个(按 `PartitionId` 确定性排序),其余下 tick 继续 ——
                // 否则"一个节点判死"会让它承载的所有分区**同时**开始搬 GB 级数据。
                // 被压下的数量暴露成 gauge:让"我们正在限速"可见,而不是表现为"系统不动了"。
                let (this_tick, deferred) = crate::reconcile::take_rereplication_budget(
                    &add_targets,
                    config.rereplication_max_per_tick,
                );
                metrics.rereplication_deferred.set(deferred as i64);
                if deferred > 0 {
                    tracing::info!(
                        deferred,
                        max_per_tick = config.rereplication_max_per_tick,
                        "reconcile: 再复制按每 tick 预算限速,其余顺延到下一拍"
                    );
                }
                for pid in this_tick {
                    // 排除该分区已有节点 + 所有下线节点(故障域打散,只落存活健康盘)。
                    let mut exclude: std::collections::HashSet<NodeId> = partitions[&pid]
                        .replicas
                        .iter()
                        .map(|r| r.node_id)
                        .collect();
                    exclude.extend(decom.iter().copied());
                    if try_add_replica(
                        &raft, &soft, &state, all_nodes, &config, &metrics, pid, &exclude, &up,
                    )
                    .await
                    .is_err()
                    {
                        unsatisfiable.insert(pid);
                    }
                }
                // 不可满足的补副本(如 RF>健康节点数)只在集合变化时记一次,避免每周期刷屏;
                // 后台照常每周期重算,有新节点/盘加入即自动补齐。
                if unsatisfiable != last_unsatisfiable {
                    if unsatisfiable.is_empty() {
                        tracing::info!("reconcile: 之前无法补的欠副本已全部满足");
                    } else {
                        tracing::warn!(
                            count = unsatisfiable.len(),
                            partitions = ?unsatisfiable,
                            "reconcile: 欠副本且无空闲盘可补(需加节点/盘),将持续重试"
                        );
                    }
                    last_unsatisfiable = unsatisfiable;
                }

                // 下线 evacuation 的"后删":健康副本已达 RF 才移除下线节点旧副本(守 §3.1)。
                for a in evac {
                    if let EvacAction::Remove(pid, node) = a {
                        if raft
                            .client_write(MasterCommand::RemoveReplica {
                                partition_id: pid,
                                node_id: node,
                            })
                            .await
                            .is_ok()
                        {
                            metrics.replicas_removed.inc();
                        }
                    }
                }

                // 副本迁移(MoveReplica)的"后删":新副本补上、继任者已 ACTIVE、且非迁出源的存活副本
                // 数 ≥ RF,才移除迁出源(真正的"先补后删",见 `plan_move`)。
                // (`active` 已在前面取过 —— 撤离判定也要用它,不重复取。)
                let successor_active: std::collections::HashSet<(PartitionId, NodeId)> = moving
                    .iter()
                    .filter(|(pid, from)| {
                        move_targets
                            .get(pid)
                            .and_then(|m| m.get(from))
                            .is_some_and(|to| active.contains(&(*pid, *to)))
                    })
                    .copied()
                    .collect();
                for (pid, node) in plan_move(partitions, moving, &up, &successor_active) {
                    if raft
                        .client_write(MasterCommand::RemoveReplica {
                            partition_id: pid,
                            node_id: node,
                        })
                        .await
                        .is_ok()
                    {
                        metrics.replicas_moved.inc();
                    }
                }

                // A6 记账清理:源节点**已不再上报**该 pid(= 盘上已删/已不再持有)→ 清 `pending_drop`
                // 与 `move_targets`。判据:节点存活且软状态显示它不再持有(心跳是全量上报,缺项即"没有");
                // **仍在上报时绝不清**,否则会丢掉对源的删除催促。与其它自动动作同在维护 skip 之后。
                for (pid, node) in pending_drop {
                    if !up.contains(&node) || soft.holds(pid, node) {
                        continue;
                    }
                    if raft
                        .client_write(MasterCommand::ConfirmDrop {
                            partition_id: pid,
                            node_id: node,
                        })
                        .await
                        .is_err()
                    {
                        break; // 提交失败(切主等):下个周期重来,不刷日志
                    }
                }

                // ---- 计划式均衡执行器(§7.3;维护模式 skip 之后,与其它自动动作并列)----
                // 计划是 hard state(可跨切主存活);这里只推进 status==RUNNING 的计划。
                // Cancel/替换旧计划后:状态不匹配 → 本块不再碰它;在途 move 的 moving 记账仍由上面的
                // `plan_move` 后删自然收敛(**不回滚**,注释见 `SetBalancePlan`)。
                {
                    // 计划切换(含被清)→ 重置 leader 本地簿记,旧计划的 move 绝不再推进。
                    let cur_id = balance.as_ref().map(|p| p.plan_id);
                    if cur_id != balance_plan_seen {
                        balance_plan_seen = cur_id;
                        balance_dispatched_at.clear();
                        balance_counted.clear();
                        // 预填终态条目:切主/换计划时已在终态的 move 不重复计数(老 leader 已计过)。
                        if let Some(p) = &balance {
                            for m in &p.moves {
                                let (phase, _) =
                                    crate::balance::move_phase(p, m, partitions, moving);
                                if matches!(
                                    phase,
                                    crate::balance::MovePhase::Done
                                        | crate::balance::MovePhase::Failed
                                ) {
                                    balance_counted.insert((
                                        p.plan_id,
                                        m.partition_id,
                                        m.from_node_id,
                                    ));
                                }
                            }
                        }
                    }
                    // gauge:任何在册计划都刷新(无计划 = 0)。
                    match &balance {
                        Some(plan) => {
                            let phases: Vec<crate::balance::MovePhase> = plan
                                .moves
                                .iter()
                                .map(|m| crate::balance::move_phase(plan, m, partitions, moving).0)
                                .collect();
                            let count = |want: crate::balance::MovePhase| {
                                phases.iter().filter(|p| **p == want).count()
                            };
                            let (n_pending, n_in_flight) = (
                                count(crate::balance::MovePhase::Pending),
                                count(crate::balance::MovePhase::InFlight),
                            );
                            metrics.balance_pending.set(n_pending as i64);
                            metrics.balance_in_flight.set(n_in_flight as i64);
                            // 终态计数(每条只计一次;counter 口径见 metrics.rs)。
                            for (m, phase) in plan.moves.iter().zip(&phases) {
                                let key = (plan.plan_id, m.partition_id, m.from_node_id);
                                if balance_counted.contains(&key) {
                                    continue;
                                }
                                match phase {
                                    crate::balance::MovePhase::Done => {
                                        balance_counted.insert(key);
                                        metrics.balance_moves.inc();
                                    }
                                    crate::balance::MovePhase::Failed => {
                                        balance_counted.insert(key);
                                        metrics.balance_failed.inc();
                                    }
                                    _ => {}
                                }
                            }
                            if plan.status != BalanceStatus::Running {
                                continue; // PREVIEW 待确认 / DONE 已收官:不推进
                            }
                            // 全部 move 进入终态 → 收官 DONE(apply 幂等;下周期 status!=RUNNING 不再写)。
                            if phases.iter().all(|p| {
                                matches!(
                                    p,
                                    crate::balance::MovePhase::Done
                                        | crate::balance::MovePhase::Failed
                                )
                            }) {
                                let _ = raft
                                    .client_write(MasterCommand::SetBalanceStatus {
                                        plan_id: plan.plan_id,
                                        status: BalanceStatus::Done,
                                    })
                                    .await;
                                continue;
                            }
                            // stuck 判据:在途超过 balance_move_timeout_secs 仍未摘源 → 判败释放预算,
                            // 不再重试该条(同一落点重试大概率再卡;重搬由运维重新 eval 决策)。
                            for (m, phase) in plan.moves.iter().zip(&phases) {
                                let key = (plan.plan_id, m.partition_id, m.from_node_id);
                                if *phase == crate::balance::MovePhase::InFlight {
                                    let since = balance_dispatched_at
                                        .entry(key)
                                        .or_insert_with(Instant::now);
                                    if since.elapsed()
                                        >= Duration::from_secs(config.balance_move_timeout_secs)
                                        && raft
                                            .client_write(MasterCommand::FailBalanceMove {
                                                plan_id: plan.plan_id,
                                                partition_id: m.partition_id,
                                                from_node_id: m.from_node_id,
                                                error: format!(
                                                    "in-flight move stuck for over {}s",
                                                    config.balance_move_timeout_secs
                                                ),
                                            })
                                            .await
                                            .is_ok()
                                    {
                                        balance_dispatched_at.remove(&key);
                                    }
                                } else {
                                    balance_dispatched_at.remove(&key); // 非在途:计时无意义
                                }
                            }
                            // 并发预算 = max_in_flight − 在途数(在途口径:派生为 InFlight 的本计划 move)。
                            let mut budget =
                                (plan.max_in_flight as usize).saturating_sub(n_in_flight);
                            if budget == 0 {
                                continue;
                            }
                            for (m, phase) in plan.moves.iter().zip(&phases) {
                                if budget == 0 {
                                    break;
                                }
                                if *phase != crate::balance::MovePhase::Pending {
                                    continue;
                                }
                                let Some(part) = partitions.get(&m.partition_id) else {
                                    continue;
                                };
                                // 执行前再校验(计划是评估时快照,执行期集群可能已变):
                                let fail_reason =
                                    if !part.replicas.iter().any(|r| r.node_id == m.from_node_id) {
                                        Some("source no longer holds the replica".to_string())
                                    } else if !up.contains(&m.to_node_id)
                                        || decom.contains(&m.to_node_id)
                                    {
                                        Some(format!(
                                            "target node {} not up/decommissioning",
                                            m.to_node_id
                                        ))
                                    } else {
                                        None
                                    };
                                // 幂等陷阱:目标已持有该副本 = 上次已投递受理(apply 只在目标不存在时
                                // 才追加+记 moving)→ **不当失败重投**,等 plan_move 摘源即可。
                                let target_holds =
                                    part.replicas.iter().any(|r| r.node_id == m.to_node_id);
                                if fail_reason.is_none()
                                    && !target_holds
                                    && !moving.contains(&(m.partition_id, m.from_node_id))
                                {
                                    // 目标盘再校验/重选:评估时选的盘此刻可能已不可用(将满/坏盘)。
                                    let picked = match all_nodes.get(&m.to_node_id) {
                                        Some(info) => {
                                            let candidates =
                                                BTreeMap::from([(m.to_node_id, info.clone())]);
                                            select_disks(
                                                &candidates,
                                                &placement_usage(&soft, &state, &config),
                                                1,
                                                config.partition_capacity_bytes,
                                                config.disk_near_full_ratio,
                                            )
                                            .ok()
                                            .map(|mut reps| reps.remove(0))
                                        }
                                        None => None,
                                    };
                                    match picked {
                                        Some(replica) => {
                                            if raft
                                                .client_write(MasterCommand::MoveReplica {
                                                    partition_id: m.partition_id,
                                                    from_node_id: m.from_node_id,
                                                    replica,
                                                })
                                                .await
                                                .is_ok()
                                            {
                                                balance_dispatched_at.insert(
                                                    (plan.plan_id, m.partition_id, m.from_node_id),
                                                    Instant::now(),
                                                );
                                                budget -= 1;
                                            }
                                        }
                                        None => {
                                            let _ = raft
                                                .client_write(MasterCommand::FailBalanceMove {
                                                    plan_id: plan.plan_id,
                                                    partition_id: m.partition_id,
                                                    from_node_id: m.from_node_id,
                                                    error: format!(
                                                        "target node {} has no eligible disk at execution time",
                                                        m.to_node_id
                                                    ),
                                                })
                                                .await;
                                        }
                                    }
                                } else if let Some(reason) = fail_reason {
                                    // 落点已失效(目标 down/下线/源已不在)→ 判败,不再重试。
                                    let _ = raft
                                        .client_write(MasterCommand::FailBalanceMove {
                                            plan_id: plan.plan_id,
                                            partition_id: m.partition_id,
                                            from_node_id: m.from_node_id,
                                            error: reason,
                                        })
                                        .await;
                                }
                                // target_holds / moving 已含该 (pid,from):已受理,不占预算投递,等摘源。
                            }
                        }
                        None => {
                            metrics.balance_pending.set(0);
                            metrics.balance_in_flight.set(0);
                        }
                    }
                }
            }
        });
    }
}

/// 在健康、非排除的节点里为一个新副本选盘(re-replication / repair / move 共用)。
fn pick_replica_disk(
    soft: &SoftState,
    state: &MasterState,
    all_nodes: &BTreeMap<NodeId, NodeInfo>,
    config: &MasterConfig,
    exclude: &std::collections::HashSet<NodeId>,
    up: &std::collections::HashSet<NodeId>,
) -> Result<ReplicaPlacement, PlacementError> {
    let candidates: BTreeMap<NodeId, NodeInfo> = all_nodes
        .iter()
        .filter(|(id, _)| !exclude.contains(id) && up.contains(id))
        .map(|(id, info)| (*id, info.clone()))
        .collect();
    let mut reps = select_disks(
        &candidates,
        &placement_usage(soft, state, config),
        1,
        config.partition_capacity_bytes,
        config.disk_near_full_ratio,
    )?;
    Ok(reps.remove(0))
}

/// (A6) 算出该节点本轮该下发的 `DROPPED` 指令:遍历 `pending_drop`,**三条确认全满足**才发。
///
/// 1. hard placement 已不含该 `(pid, node)`(reconciler 的 `RemoveReplica` 已提交);
/// 2. 继任者确认([`drop_confirmed`]):有 move 记账时要求其目标节点已上报 `ACTIVE`(= 追平,转移成功);
///    无记账(evacuation 的"后删")退化为"其余副本数 ≥ RF 且无 ERROR";
/// 3. 源节点存活(正在心跳)**且仍上报该 pid** —— 否则无需删(记账清理由 reconciler 的 `ConfirmDrop` 负责)。
///
/// **安全不变量**:全仓只有 reconciler 写 `RemoveReplica`,没有运维 RPC 能直接移除副本,故
/// "placement 已移除 + 继任者已 ACTIVE + 源仍持有"三条齐了才删 = 安全下界(最坏也只是源上多留一份数据)。
/// 每轮心跳重发直到 store 不再上报该 pid(幂等)。
fn plan_drops(
    soft: &SoftState,
    state: &MasterState,
    node_id: NodeId,
) -> Vec<crate::pb::Transition> {
    // 源是否仍持有该副本:查软状态(增量心跳下 beat 内容不代表全集,不能用当拍上报列表判断)。
    let mine: Vec<(PartitionId, NodeId)> = state
        .pending_drop
        .iter()
        .copied()
        .filter(|(pid, node)| *node == node_id && soft.holds(*pid, node_id))
        .collect();
    if mine.is_empty() {
        return Vec::new(); // 常态:该节点没有待删项 → 不碰软状态
    }
    let active = soft.active_replicas();
    let errored = soft.errored_replicas();
    let mut out = Vec::new();
    for (pid, node) in mine {
        let Some(info) = state.partitions.get(&pid) else {
            continue;
        };
        let successor = match state.move_target_of(pid, node) {
            Some(target) if active.contains(&(pid, target)) => SuccessorConfirm::Active,
            Some(_) => SuccessorConfirm::NotYetActive,
            None => SuccessorConfirm::Unrecorded,
        };
        if !drop_confirmed(pid, info, node, successor, &errored) {
            continue;
        }
        // disk_id 从软状态取该副本所在盘;拿不到落点就不发(宁可留一份数据,不猜是哪块盘)。
        let Some(st) = soft.replica_state(pid, node) else {
            continue;
        };
        out.push(crate::pb::Transition {
            partition_id: pid,
            disk_id: st.disk_id,
            target_state: DROPPED.to_string(),
        });
    }
    out
}

/// 某分区某副本是否"已追平"(墓碑回收闸门的单个副本判据,喂 [`tombstones_reclaim_allowed`]):
/// 节点存活 **且** 上报 `ACTIVE` 且 `lag_bytes == 0`。
///
/// 任一不满足都算未追平:节点失联 / 缺上报(如副本刚补上还没报过) / 报 BOOTSTRAP/ERROR/OFFLINE /
/// 仍有滞后字节。判据一律取软状态最新一拍(心跳每拍先 `record_replicas` 再算指令)。
fn replica_caught_up(
    soft: &SoftState,
    pid: PartitionId,
    node: NodeId,
    up_timeout: Duration,
) -> bool {
    soft.is_up(node, up_timeout)
        && soft
            .replica_state(pid, node)
            .is_some_and(|st| st.current_state == "ACTIVE" && st.lag_bytes == 0)
}

/// 在一个健康、非排除的节点上给分区补一个副本(re-replication / evacuation / repair 共用)。
///
/// 返回:`Ok(Some((node, disk)))` = 提案成功及落点;`Ok(None)` = 提案失败(可重试);
/// `Err(PlacementError)` = 无可放的盘(unsatisfiable)。
#[allow(clippy::too_many_arguments)]
async fn try_add_replica(
    raft: &Raft<TypeConfig>,
    soft: &SoftState,
    state: &MasterState,
    all_nodes: &BTreeMap<NodeId, NodeInfo>,
    config: &MasterConfig,
    metrics: &MasterMetrics,
    pid: crate::types::PartitionId,
    exclude: &std::collections::HashSet<NodeId>,
    up: &std::collections::HashSet<NodeId>,
) -> Result<Option<(NodeId, DiskId)>, PlacementError> {
    let replica = match pick_replica_disk(soft, state, all_nodes, config, exclude, up) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(partition = pid, error = %e, "no eligible disk to add replica");
            return Err(e);
        }
    };
    let at = (replica.node_id, replica.disk_id.clone());
    if raft
        .client_write(MasterCommand::AddReplica {
            partition_id: pid,
            replica,
        })
        .await
        .is_ok()
    {
        metrics.rereplications.inc();
        return Ok(Some(at));
    }
    Ok(None)
}

/// 是否存在可放新副本的空闲健康节点(re-replication / healable / 下线可行性共用)。
fn has_spare_disk(
    all_nodes: &BTreeMap<NodeId, NodeInfo>,
    soft: &SoftState,
    state: &MasterState,
    config: &MasterConfig,
    exclude: &std::collections::HashSet<NodeId>,
    up: &std::collections::HashSet<NodeId>,
) -> bool {
    let candidates: BTreeMap<NodeId, NodeInfo> = all_nodes
        .iter()
        .filter(|(id, _)| !exclude.contains(id) && up.contains(id))
        .map(|(id, info)| (*id, info.clone()))
        .collect();
    select_disks(
        &candidates,
        &placement_usage(soft, state, config),
        1,
        config.partition_capacity_bytes,
        config.disk_near_full_ratio,
    )
    .is_ok()
}

fn enc<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(v).map_err(|e| Status::internal(format!("encode: {e}")))
}
fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(b).map_err(|e| Status::internal(format!("decode: {e}")))
}
fn reply(payload: Vec<u8>) -> Response<RaftMessage> {
    Response::new(RaftMessage {
        version: PROTO_VERSION,
        payload,
    })
}

/// Raft 内部 RPC 服务端:反序列化 → 本地 Raft → 序列化 `Result<Resp, RaftError>`。
struct RaftInternalService {
    raft: Raft<TypeConfig>,
}

#[tonic::async_trait]
impl RaftInternal for RaftInternalService {
    async fn append_entries(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let req = dec(&request.into_inner().payload)?;
        let res = self.raft.append_entries(req).await;
        Ok(reply(enc(&res)?))
    }

    async fn vote(&self, request: Request<RaftMessage>) -> Result<Response<RaftMessage>, Status> {
        let req = dec(&request.into_inner().payload)?;
        let res = self.raft.vote(req).await;
        Ok(reply(enc(&res)?))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessage>, Status> {
        let req = dec(&request.into_inner().payload)?;
        let res = self.raft.install_snapshot(req).await;
        Ok(reply(enc(&res)?))
    }
}

/// 控制面服务端。
struct MasterService {
    raft: Raft<TypeConfig>,
    sm_data: Arc<RwLock<StateMachineData>>,
    soft: SoftState,
    metrics: MasterMetrics,
    config: MasterConfig,
    registry: antfs_metric::Registry,
    /// objectId 号段发号器(leader 内存池 + Raft 水位;见 [`crate::idalloc`])。
    id_alloc: IdAllocator,
    /// placement 变更纪元 + 派生索引(心跳每拍 O(节点) 的关键,见 [`PlacementIndex`])。
    placement_epoch: Arc<AtomicU64>,
    placement_index: PlacementIndex,
}

impl MasterService {
    /// 准入校验(Q1 安全点):密钥非空时校验;失败计数 + 拒绝。
    fn check_secret(&self, provided: &str) -> Result<(), Status> {
        if self.config.cluster_secret.is_empty() || provided == self.config.cluster_secret {
            Ok(())
        } else {
            self.metrics.admission_rejected.inc();
            Err(Status::unauthenticated("invalid cluster secret"))
        }
    }

    /// 当前 leader 的对外地址(从 raft membership 里取 Node.addr)。
    fn leader_addr(&self) -> Option<String> {
        let m = self.raft.metrics().borrow().clone();
        let leader = m.current_leader?;
        let addrs: BTreeMap<u64, String> = m
            .membership_config
            .nodes()
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect();
        addrs.get(&leader).cloned()
    }

    /// 发号失败响应:统一 `ok=false` + 失败计数 + 回填 leader 地址(写路径,非 leader 时 client 据此重定向)。
    fn blob_ids_fail(&self, error: impl Into<String>) -> GetBlobIdsResponse {
        self.metrics.blob_ids_failures.inc();
        GetBlobIdsResponse {
            version: PROTO_VERSION,
            ok: false,
            leader_addr: self.leader_addr().unwrap_or_default(),
            error: error.into(),
            partition_id: 0,
            start_object_id: 0,
            count: 0,
            replicas: Vec::new(),
        }
    }

    /// 按**剩余容量加权**随机挑一个可写(RW)分区(blobid-master-assign D10:写路由由 master 选,
    /// 换来全局容量感知均衡 + 不撞陈旧的满/RO 分区 + 薄 client)。
    ///
    /// - 权重 = [`capacity_weight_permille`](千分比;与下发给 client 的 `write_weight` 同一口径与同一函数),
    ///   下界 1(留一丝机会,不让"上报为满"的分区被永久饿死)。`used_bytes` 取自心跳软状态,缺上报按 0 计
    ///   (即视为"还没写过")。
    /// - 候选**优先**取「至少一个副本所在节点存活」的分区;若一个都没有(典型是 master 刚重启、
    ///   软状态还没被心跳重建),退回全部 RW 分区 —— 否则会出现"心跳空窗期完全不可写"的假故障。
    /// - 随机只在 leader 侧发生(不进 `apply`,守状态机确定性铁律)。
    fn pick_writable_partition(
        &self,
        g: &StateMachineData,
        used: &BTreeMap<PartitionId, u64>,
        timeout: Duration,
    ) -> Option<PartitionId> {
        let cap = self.config.partition_capacity_bytes;
        let weight =
            |pid: PartitionId| capacity_weight_permille(cap, used.get(&pid).copied().unwrap_or(0));

        let mut cands: Vec<PartitionId> = g
            .state
            .partitions
            .iter()
            .filter(|(_, p)| p.state == PartitionState::ReadWrite)
            // 没有副本的分区写不进去(避让:迁移/下线过程中可能出现瞬时 0 副本),
            // 挑了它只会让 client 拿到一个必然失败的号段。
            .filter(|(_, p)| !p.replicas.is_empty())
            .map(|(pid, _)| *pid)
            .collect();
        if cands.is_empty() {
            return None;
        }
        let live: Vec<PartitionId> = cands
            .iter()
            .copied()
            .filter(|pid| {
                g.state.partitions[pid]
                    .replicas
                    .iter()
                    .any(|r| self.soft.is_up(r.node_id, timeout))
            })
            .collect();
        if !live.is_empty() {
            cands = live;
        }
        // 稳定顺序:权重相同时结果可复现,便于测试与排障(随机只体现在落点上)。
        cands.sort_unstable();

        let total: u64 = cands.iter().map(|pid| u64::from(weight(*pid))).sum();
        let mut point = pseudo_random() % total;
        for pid in &cands {
            let w = u64::from(weight(*pid));
            if point < w {
                return Some(*pid);
            }
            point -= w;
        }
        cands.last().copied()
    }
}

#[tonic::async_trait]
impl Master for MasterService {
    async fn register_node(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(RegisterResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
            }));
        }

        let disks = req
            .disks
            .into_iter()
            .map(|d| {
                (
                    d.disk_id,
                    DiskSpec {
                        capacity_bytes: d.capacity_bytes,
                    },
                )
            })
            .collect();
        let info = NodeInfo {
            dc: req.dc,
            rack: req.rack,
            addr: req.addr,
            disks,
            registered_at_ms: now_ms(),
        };
        match self
            .raft
            .client_write(MasterCommand::RegisterNode {
                node_id: req.node_id,
                info,
            })
            .await
        {
            Ok(_) => Ok(Response::new(RegisterResponse {
                version: PROTO_VERSION,
                ok: true,
                leader_addr: String::new(),
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(RegisterResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: format!("{e}"),
            })),
        }
    }

    type HeartbeatStream =
        Pin<Box<dyn futures::Stream<Item = Result<MasterDirective, Status>> + Send>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<NodeBeat>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut in_stream = request.into_inner();
        let soft = self.soft.clone();
        let sm_data = self.sm_data.clone();
        let metrics = self.metrics.clone();
        let secret = self.config.cluster_secret.clone();
        let up_timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let adjusted_free_timeout = Duration::from_secs(self.config.adjusted_free_timeout_secs);
        let placement_epoch = self.placement_epoch.clone();
        let placement_index = self.placement_index.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // 连接级下发现场(M7 增量下行):pid -> 上次已下发的 (分区状态, 墓碑闸门)。
            // 重连 = 新流 = 新场 → 首拍自然全量下发;store 端按 pid 合并应用,无删除语义。
            let mut last_sent: HashMap<PartitionId, (String, bool)> = HashMap::new();
            while let Ok(Some(beat)) = in_stream.message().await {
                if !secret.is_empty() && beat.cluster_secret != secret {
                    let _ = tx
                        .send(Err(Status::unauthenticated("invalid cluster secret")))
                        .await;
                    break;
                }
                metrics.heartbeats.inc();
                let node_id = beat.node_id;
                let full_report = beat.full_report;
                let reported_digest = beat.replica_digest;
                let removed = beat.removed_replicas;
                let disks: HashMap<String, DiskUsage> = beat
                    .disks
                    .into_iter()
                    .map(|d| {
                        (
                            d.disk_id,
                            DiskUsage {
                                capacity_bytes: d.capacity_bytes,
                                free_bytes: d.free_bytes,
                                health: d.health,
                                // 占位:adjusted_free / last_heartbeat_time 由 record_beat 的守卫逻辑重算。
                                adjusted_free: d.free_bytes,
                                last_heartbeat_time: Instant::now(),
                            },
                        )
                    })
                    .collect();
                soft.record_beat(node_id, disks, adjusted_free_timeout);

                // M3/M7:记录副本实然态(全量替换 / 增量合并 + 显式摘除)。
                if full_report {
                    metrics.heartbeat_full_reports.inc();
                }
                soft.record_replicas(
                    node_id,
                    beat.replicas.into_iter().map(|r| {
                        (
                            r.partition_id,
                            crate::soft::ReplicaState {
                                disk_id: r.disk_id,
                                current_state: r.current_state,
                                lag_bytes: r.lag_bytes,
                                used_bytes: r.used_bytes,
                            },
                        )
                    }),
                    &removed,
                    full_report,
                );

                // 节点视图取**软状态**(刚合并完这一拍):增量 beat 的内容不代表全集,规划一律以此为准。
                let reported: BTreeMap<PartitionId, String> = soft
                    .node_replicas(node_id)
                    .into_iter()
                    .map(|(pid, st)| (pid, st.current_state))
                    .collect();

                // 摘要校验(M7):不符 → 本拍响应要求下一拍全量重发(增量丢项/错项一拍内自愈)。
                // digest==0 跳过:旧版 store 恒不报摘要(字段缺省 0),不对它误报;新版 store 持有副本时
                // 摘要为 0 的概率 2^-64(XOR 恰好抵消),放过一拍无妨。
                let resend_full_report =
                    reported_digest != 0 && soft.node_digest(node_id) != reported_digest;
                if resend_full_report {
                    metrics.heartbeat_resend_requests.inc();
                }

                let (transitions, partition_states) = {
                    let g = match sm_data.read() {
                        Ok(g) => g,
                        Err(_) => break,
                    };
                    let epoch = placement_epoch.load(Ordering::SeqCst);
                    let node_pids = placement_index.node_pids(node_id, epoch, &g.state);
                    let mut out: Vec<crate::pb::Transition> =
                        plan_transitions(node_id, &node_pids, &g.state.partitions, &reported)
                            .into_iter()
                            .map(|t| crate::pb::Transition {
                                partition_id: t.partition_id,
                                disk_id: t.disk_id,
                                target_state: t.target_state,
                            })
                            .collect();
                    out.extend(plan_drops(&soft, &g.state, node_id));
                    // 下发现场按当前持有集收敛:副本摘除后旧条目作废(将来重建句柄要重新收到状态)。
                    last_sent.retain(|pid, _| reported.contains_key(pid));
                    // 分区状态与墓碑闸门**每拍重算**(闸门依赖其它节点追平态,不能缓存),但只下发
                    // 与上拍不同的条目(M7 增量下行);store 端按 pid 合并应用。
                    let states = plan_partition_states(&g.state.partitions, &reported)
                        .into_iter()
                        .filter_map(|(partition_id, state)| {
                            // 墓碑回收闸门:该分区**所有副本**都已追平才放行(见
                            // [`tombstones_reclaim_allowed`]) —— 还有副本没追平时回收墓碑,它
                            // 就永远学不到那条删除,已删对象会以"活数据"永久留在它上面。
                            let flag = g.state.partitions.get(&partition_id).is_some_and(|info| {
                                tombstones_reclaim_allowed(info, |n| {
                                    replica_caught_up(&soft, partition_id, n, up_timeout)
                                })
                            });
                            let entry = (pstate(state).to_string(), flag);
                            if last_sent.get(&partition_id) == Some(&entry) {
                                return None;
                            }
                            last_sent.insert(partition_id, entry.clone());
                            Some(crate::pb::PartitionStateInfo {
                                partition_id,
                                state: entry.0,
                                tombstones_reclaim_allowed: entry.1,
                            })
                        })
                        .collect();
                    (out, states)
                };
                if !transitions.is_empty() {
                    metrics.transitions_issued.inc_by(transitions.len() as u64);
                }
                let dropped = transitions
                    .iter()
                    .filter(|t| t.target_state == DROPPED)
                    .count();
                if dropped > 0 {
                    metrics.dropped_directives.inc_by(dropped as u64);
                }
                if tx
                    .send(Ok(MasterDirective {
                        version: PROTO_VERSION,
                        transitions,
                        tasks: vec![],
                        partition_states,
                        resend_full_report,
                        delta_report_supported: true,
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<NodeList>, Status> {
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self
            .sm_data
            .read()
            .map_err(|_| Status::internal("sm lock poisoned"))?;
        let nodes = g
            .state
            .nodes
            .iter()
            .map(|(id, info)| {
                let usage = self.soft.disks_of(*id);
                let disks = info
                    .disks
                    .iter()
                    .map(|(disk_id, spec)| crate::pb::DiskBeat {
                        disk_id: disk_id.clone(),
                        capacity_bytes: spec.capacity_bytes,
                        free_bytes: usage.get(disk_id).map(|u| u.free_bytes).unwrap_or(0),
                        // 主动探测结果回填(空 = 尚未探测/未启用):admin 节点页据此显示健康徽标。
                        health: usage
                            .get(disk_id)
                            .map(|u| u.health.clone())
                            .unwrap_or_default(),
                    })
                    .collect();
                NodeEntry {
                    node_id: *id,
                    dc: info.dc.clone(),
                    rack: info.rack.clone(),
                    addr: info.addr.clone(),
                    up: self.soft.is_up(*id, timeout),
                    disks,
                }
            })
            .collect();
        Ok(Response::new(NodeList {
            version: PROTO_VERSION,
            nodes,
        }))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterState>, Status> {
        let m = self.raft.metrics().borrow().clone();
        let g = self
            .sm_data
            .read()
            .map_err(|_| Status::internal("sm lock poisoned"))?;
        Ok(Response::new(ClusterState {
            version: PROTO_VERSION,
            this_node: self.config.node_id,
            leader: m.current_leader,
            term: m.current_term,
            registered_nodes: g.state.nodes.len() as u64,
            next_partition_id: g.state.next_partition_id,
            maintenance: g.state.maintenance,
            pause_autogrow: g.state.pause_autogrow,
        }))
    }

    async fn create_partitions(
        &self,
        request: Request<CreatePartitionsRequest>,
    ) -> Result<Response<CreatePartitionsResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(CreatePartitionsResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
                partition_ids: vec![],
            }));
        }

        let rf_default = {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            g.state.policy.default_replication_factor
        };
        let rf = if req.replication_factor == 0 {
            rf_default
        } else {
            req.replication_factor as u8
        };

        let mut ids = Vec::new();
        for _ in 0..req.count {
            match place_one_partition(
                &self.raft,
                &self.sm_data,
                &self.soft,
                &self.config,
                &self.metrics,
                rf,
            )
            .await
            {
                Ok(id) => ids.push(id),
                Err(e) => {
                    return Ok(Response::new(CreatePartitionsResponse {
                        version: PROTO_VERSION,
                        ok: false,
                        leader_addr: self.leader_addr().unwrap_or_default(),
                        error: e,
                        partition_ids: ids,
                    }));
                }
            }
        }
        Ok(Response::new(CreatePartitionsResponse {
            version: PROTO_VERSION,
            ok: true,
            leader_addr: String::new(),
            error: String::new(),
            partition_ids: ids,
        }))
    }

    async fn lookup_partition(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<PartitionLocations>, Status> {
        self.metrics.lookup_requests.inc();
        let pid = request.into_inner().partition_id;
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self
            .sm_data
            .read()
            .map_err(|_| Status::internal("sm lock poisoned"))?;
        let part = g
            .state
            .partitions
            .get(&pid)
            .ok_or_else(|| Status::not_found(format!("partition {pid} not found")))?;
        let replicas = part
            .replicas
            .iter()
            .map(|r| ReplicaLoc {
                node_id: r.node_id,
                addr: g
                    .state
                    .nodes
                    .get(&r.node_id)
                    .map(|n| n.addr.clone())
                    .unwrap_or_default(),
                disk_id: r.disk_id.clone(),
                target_state: r.target_state.clone(),
                incarnation: r.incarnation,
                up: self.soft.is_up(r.node_id, timeout),
            })
            .collect();
        Ok(Response::new(PartitionLocations {
            version: PROTO_VERSION,
            partition_id: pid,
            state: pstate(part.state).to_string(),
            replicas,
        }))
    }

    async fn list_partitions(
        &self,
        request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        let req = request.into_inner();
        // 状态过滤(空 = 全部)。边界校验:非法状态串直接拒(admin 传错时不静默当"全部")。
        let want = match req.state.as_str() {
            "" => None,
            "RW" => Some(PartitionState::ReadWrite),
            "RO" => Some(PartitionState::ReadOnly),
            "SEALED" => Some(PartitionState::Sealed),
            other => {
                return Err(Status::invalid_argument(format!(
                    "invalid state {other} (want RW/RO/SEALED)"
                )))
            }
        };
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self
            .sm_data
            .read()
            .map_err(|_| Status::internal("sm lock poisoned"))?;
        let keep = |p: &crate::types::PartitionInfo| want.is_none_or(|s| p.state == s);
        // 过滤先于分页:`total` 是过滤后的总数,offset/limit 作用在过滤后的集合上。
        let total = g.state.partitions.values().filter(|p| keep(p)).count() as u64;
        let up_set = self.soft.up_nodes(timeout);
        // BTreeMap 按 partition_id 升序,offset/limit 稳定切片(limit=0 表示不限)。
        let take = if req.limit == 0 {
            usize::MAX
        } else {
            req.limit as usize
        };
        // 写权重(千分比)按软状态现算:**派生视图,不进 hard state/apply**(切主后由心跳重建)。
        let used = self.soft.used_bytes_by_partition();
        let partitions = g
            .state
            .partitions
            .iter()
            .filter(|(_, part)| keep(part))
            .skip(req.offset as usize)
            .take(take)
            .map(|(pid, part)| {
                let replicas = part
                    .replicas
                    .iter()
                    .map(|r| ReplicaLoc {
                        node_id: r.node_id,
                        addr: g
                            .state
                            .nodes
                            .get(&r.node_id)
                            .map(|n| n.addr.clone())
                            .unwrap_or_default(),
                        disk_id: r.disk_id.clone(),
                        target_state: r.target_state.clone(),
                        incarnation: r.incarnation,
                        up: self.soft.is_up(r.node_id, timeout),
                    })
                    .collect();
                let currents = part
                    .replicas
                    .iter()
                    .filter_map(|r| {
                        self.soft
                            .replica_state(*pid, r.node_id)
                            .map(|st| ReplicaCurrent {
                                node_id: r.node_id,
                                current_state: st.current_state,
                                lag_bytes: st.lag_bytes,
                                used_bytes: st.used_bytes,
                            })
                    })
                    .collect();
                // 健康分类(承 reconcile::classify_partition):健康副本数 + 欠副本主因。
                let health_inputs: Vec<crate::reconcile::ReplicaHealth> = part
                    .replicas
                    .iter()
                    .map(|r| crate::reconcile::ReplicaHealth {
                        up: self.soft.is_up(r.node_id, timeout),
                        current_state: self
                            .soft
                            .replica_state(*pid, r.node_id)
                            .map(|st| st.current_state)
                            .unwrap_or_default(),
                        decommissioning: g.state.decommissioning.contains(&r.node_id),
                    })
                    .collect();
                let (healthy_replicas, reason) =
                    crate::reconcile::classify_partition(&health_inputs, part.replication_factor);
                // 欠副本时:是否有空闲健康节点可补(排除已持有 + 下线节点)。
                let healable = match reason {
                    None => true,
                    Some(_) => {
                        let mut exclude: std::collections::HashSet<NodeId> =
                            part.replicas.iter().map(|r| r.node_id).collect();
                        exclude.extend(g.state.decommissioning.iter().copied());
                        has_spare_disk(
                            &g.state.nodes,
                            &self.soft,
                            &g.state,
                            &self.config,
                            &exclude,
                            &up_set,
                        )
                    }
                };
                PartitionEntry {
                    partition_id: *pid,
                    state: pstate(part.state).to_string(),
                    replication_factor: part.replication_factor as u32,
                    replicas,
                    currents,
                    healthy_replicas,
                    healable,
                    under_replicated_reason: reason.unwrap_or("").to_string(),
                    moving_out_nodes: g
                        .state
                        .moving
                        .iter()
                        .filter(|(p, _)| p == pid)
                        .map(|&(_, n)| n)
                        .collect(),
                    write_weight: capacity_weight_permille(
                        self.config.partition_capacity_bytes,
                        used.get(pid).copied().unwrap_or(0),
                    ),
                }
            })
            .collect();
        Ok(Response::new(ListPartitionsResponse {
            version: PROTO_VERSION,
            partitions,
            total,
        }))
    }

    async fn set_partition_state(
        &self,
        request: Request<SetPartitionStateRequest>,
    ) -> Result<Response<SetPartitionStateResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let state = match req.state.as_str() {
            "RW" => PartitionState::ReadWrite,
            "RO" => PartitionState::ReadOnly,
            "SEALED" => PartitionState::Sealed,
            other => {
                return Ok(Response::new(SetPartitionStateResponse {
                    version: PROTO_VERSION,
                    ok: false,
                    leader_addr: String::new(),
                    error: format!("invalid state {other} (want RW/RO/SEALED)"),
                }))
            }
        };
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(SetPartitionStateResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
            }));
        }
        match self
            .raft
            .client_write(MasterCommand::SetPartitionState {
                partition_id: req.partition_id,
                state,
            })
            .await
        {
            Ok(_) => Ok(Response::new(SetPartitionStateResponse {
                version: PROTO_VERSION,
                ok: true,
                leader_addr: String::new(),
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SetPartitionStateResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: format!("{e}"),
            })),
        }
    }

    async fn set_maintenance(
        &self,
        request: Request<SetMaintenanceRequest>,
    ) -> Result<Response<SetMaintenanceResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(SetMaintenanceResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
            }));
        }
        // 时间戳在 leader 侧取,作为提案数据进 Raft(apply 不取 now,确定性铁律见 types.rs 头)。
        let set_at_ms = now_ms();
        match self
            .raft
            .client_write(MasterCommand::SetMaintenance {
                paused: req.paused,
                pause_autogrow: req.pause_autogrow,
                set_at_ms,
            })
            .await
        {
            Ok(_) => {
                tracing::info!(
                    paused = req.paused,
                    pause_autogrow = req.pause_autogrow,
                    set_at_ms,
                    "maintenance mode updated"
                );
                // ok=true ⟺ 已提交的 `ClusterState.maintenance` == req.paused(apply 为纯赋值)。
                Ok(Response::new(SetMaintenanceResponse {
                    version: PROTO_VERSION,
                    ok: true,
                    leader_addr: String::new(),
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(SetMaintenanceResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: format!("{e}"),
            })),
        }
    }

    async fn assign_partition(
        &self,
        _request: Request<AssignRequest>,
    ) -> Result<Response<AssignResponse>, Status> {
        self.metrics.assign_requests.inc();
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = match self.sm_data.read() {
            Ok(g) => g,
            Err(_) => {
                self.metrics.assign_failures.inc();
                return Err(Status::internal("sm lock poisoned"));
            }
        };
        let writable = g
            .state
            .partitions
            .iter()
            .find(|(_, p)| p.state == PartitionState::ReadWrite);
        match writable {
            Some((pid, part)) => {
                let replicas = part
                    .replicas
                    .iter()
                    .map(|r| ReplicaLoc {
                        node_id: r.node_id,
                        addr: g
                            .state
                            .nodes
                            .get(&r.node_id)
                            .map(|n| n.addr.clone())
                            .unwrap_or_default(),
                        disk_id: r.disk_id.clone(),
                        target_state: r.target_state.clone(),
                        incarnation: r.incarnation,
                        up: self.soft.is_up(r.node_id, timeout),
                    })
                    .collect();
                Ok(Response::new(AssignResponse {
                    version: PROTO_VERSION,
                    ok: true,
                    partition_id: *pid,
                    replicas,
                    error: String::new(),
                }))
            }
            None => {
                self.metrics.assign_failures.inc();
                Ok(Response::new(AssignResponse {
                    version: PROTO_VERSION,
                    ok: false,
                    partition_id: 0,
                    replicas: vec![],
                    error: "no writable partition".to_string(),
                }))
            }
        }
    }

    /// 发号:client/网关写路径的入口(blobid-master-assign)。
    ///
    /// leader-only + 需准入密钥(写路径,要经 Raft 抬水位)。返回**可写分区 + objectId 号段 + 副本位置**,
    /// client 据此拼 `blobId = partitionId << 32 | objectId` 并直连副本写。
    async fn get_blob_ids(
        &self,
        request: Request<GetBlobIdsRequest>,
    ) -> Result<Response<GetBlobIdsResponse>, Status> {
        let req = request.into_inner();
        self.metrics.blob_ids_requests.inc();
        self.check_secret(&req.cluster_secret)?;

        // count = 0 → 用 master 默认号段大小(薄 client:默认值由服务端定)。
        let count = if req.count == 0 {
            self.config.id_alloc_segment_size
        } else {
            req.count
        };
        if count == 0 {
            return Ok(Response::new(
                self.blob_ids_fail("id_alloc_segment_size must be > 0"),
            ));
        }

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(self.blob_ids_fail("not leader")));
        }

        // 选分区 + 取副本位置。**读锁必须在 await 前释放**(std guard 非 Send,跨 await 会让
        // 该 handler 的 future 不再是 Send),故这里用一个显式作用域把它圈住。
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let (pid, replicas) = {
            let g = match self.sm_data.read() {
                Ok(g) => g,
                Err(_) => return Err(Status::internal("sm lock poisoned")),
            };
            let pid = match req.partition_id {
                Some(p) => match g.state.partitions.get(&p) {
                    Some(part) if part.state != PartitionState::ReadWrite => {
                        let state = pstate(part.state).to_string();
                        return Ok(Response::new(self.blob_ids_fail(format!(
                            "partition {p} is not writable (state {state})"
                        ))));
                    }
                    // 显式点名一个没有副本的分区:号段发得出来也写不进去,直接拒(与自动挑选同一判据)。
                    Some(part) if part.replicas.is_empty() => {
                        return Ok(Response::new(
                            self.blob_ids_fail(format!("partition {p} has no replica")),
                        ))
                    }
                    Some(_) => p,
                    None => {
                        return Ok(Response::new(
                            self.blob_ids_fail(format!("unknown partition {p}")),
                        ))
                    }
                },
                None => match self.pick_writable_partition(
                    &g,
                    &self.soft.used_bytes_by_partition(),
                    timeout,
                ) {
                    Some(p) => p,
                    None => return Ok(Response::new(self.blob_ids_fail("no writable partition"))),
                },
            };
            let replicas = g
                .state
                .partitions
                .get(&pid)
                .map(|p| {
                    p.replicas
                        .iter()
                        .map(|r| ReplicaLoc {
                            node_id: r.node_id,
                            addr: g
                                .state
                                .nodes
                                .get(&r.node_id)
                                .map(|n| n.addr.clone())
                                .unwrap_or_default(),
                            disk_id: r.disk_id.clone(),
                            target_state: r.target_state.clone(),
                            incarnation: r.incarnation,
                            up: self.soft.is_up(r.node_id, timeout),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (pid, replicas)
        };

        // 发号:池内直接取;不够则先经 Raft 抬水位(失败 → ok=false,err 说明原因)。
        match self
            .id_alloc
            .allocate(&self.raft, pid, count, &self.config, &self.metrics)
            .await
        {
            Ok((start, n)) => {
                self.metrics.blob_ids_allocated.inc_by(n as u64);
                Ok(Response::new(GetBlobIdsResponse {
                    version: PROTO_VERSION,
                    ok: true,
                    leader_addr: String::new(),
                    error: String::new(),
                    partition_id: pid,
                    start_object_id: start as u64,
                    count: n,
                    replicas,
                }))
            }
            Err(e) => Ok(Response::new(self.blob_ids_fail(e.to_string()))),
        }
    }

    async fn decommission_node(
        &self,
        request: Request<DecommissionRequest>,
    ) -> Result<Response<DecommissionResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(DecommissionResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
                affected_partitions: vec![],
            }));
        }
        // 前置可行性校验:下线后该节点承载的分区,若无空闲健康节点可补回 RF → 拒绝(除非 force)。
        if !req.force {
            let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
            let affected: Vec<u32> = {
                let g = self
                    .sm_data
                    .read()
                    .map_err(|_| Status::internal("sm lock poisoned"))?;
                let up_set = self.soft.up_nodes(timeout);
                g.state
                    .partitions
                    .iter()
                    .filter(|(_, part)| part.replicas.iter().any(|r| r.node_id == req.node_id))
                    .filter(|(_, part)| {
                        let mut exclude: std::collections::HashSet<NodeId> =
                            part.replicas.iter().map(|r| r.node_id).collect();
                        exclude.insert(req.node_id);
                        exclude.extend(g.state.decommissioning.iter().copied());
                        !has_spare_disk(
                            &g.state.nodes,
                            &self.soft,
                            &g.state,
                            &self.config,
                            &exclude,
                            &up_set,
                        )
                    })
                    .map(|(pid, _)| *pid)
                    .collect()
            };
            if !affected.is_empty() {
                return Ok(Response::new(DecommissionResponse {
                    version: PROTO_VERSION,
                    ok: false,
                    leader_addr: String::new(),
                    error: format!(
                        "下线将使 {} 个分区欠副本且无空闲盘可补齐;确认后可用 force 强制下线",
                        affected.len()
                    ),
                    affected_partitions: affected,
                }));
            }
        }
        match self
            .raft
            .client_write(MasterCommand::DecommissionNode {
                node_id: req.node_id,
            })
            .await
        {
            Ok(_) => Ok(Response::new(DecommissionResponse {
                version: PROTO_VERSION,
                ok: true,
                leader_addr: String::new(),
                error: String::new(),
                affected_partitions: vec![],
            })),
            Err(e) => Ok(Response::new(DecommissionResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: format!("{e}"),
                affected_partitions: vec![],
            })),
        }
    }

    async fn get_metrics(
        &self,
        _request: Request<MetricsRequest>,
    ) -> Result<Response<MetricsResponse>, Status> {
        // 任何节点(含 follower)都可答:导出本节点 Registry 的 Prometheus 文本。
        let text =
            antfs_metric::gather(&self.registry).map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(MetricsResponse {
            version: PROTO_VERSION,
            text,
        }))
    }

    async fn repair_partition(
        &self,
        request: Request<RepairPartitionRequest>,
    ) -> Result<Response<RepairPartitionResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        self.metrics.repairs_triggered.inc();
        let fail = |error: String| {
            Ok(Response::new(RepairPartitionResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: String::new(),
                error,
                added_node_id: 0,
                added_disk_id: String::new(),
            }))
        };

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(RepairPartitionResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
                added_node_id: 0,
                added_disk_id: String::new(),
            }));
        }

        // 排除该分区已有副本节点 + 所有下线节点(同 reconciler 的补副本约束)。
        let (state, exclude) = {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            let Some(part) = g.state.partitions.get(&req.partition_id) else {
                return fail("partition not found".to_string());
            };
            let mut exclude: std::collections::HashSet<NodeId> =
                part.replicas.iter().map(|r| r.node_id).collect();
            exclude.extend(g.state.decommissioning.iter().copied());
            (g.state.clone(), exclude)
        };
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let up = self.soft.up_nodes(timeout);
        match try_add_replica(
            &self.raft,
            &self.soft,
            &state,
            &state.nodes,
            &self.config,
            &self.metrics,
            req.partition_id,
            &exclude,
            &up,
        )
        .await
        {
            Ok(Some((node, disk))) => Ok(Response::new(RepairPartitionResponse {
                version: PROTO_VERSION,
                ok: true,
                leader_addr: String::new(),
                error: String::new(),
                added_node_id: node,
                added_disk_id: disk,
            })),
            Ok(None) => fail("raft write failed".to_string()),
            Err(_) => fail("no eligible disk".to_string()),
        }
    }

    async fn move_replica(
        &self,
        request: Request<MoveReplicaRequest>,
    ) -> Result<Response<MoveReplicaResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let fail = |error: String| {
            Ok(Response::new(MoveReplicaResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: String::new(),
                error,
                to_node_id: 0,
                to_disk_id: String::new(),
            }))
        };

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return Ok(Response::new(MoveReplicaResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: self.leader_addr().unwrap_or_default(),
                error: "not leader".to_string(),
                to_node_id: 0,
                to_disk_id: String::new(),
            }));
        }

        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let up = self.soft.up_nodes(timeout);
        let (state, part_replicas, already_moving) = {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            let Some(part) = g.state.partitions.get(&req.partition_id) else {
                return fail("partition not found".to_string());
            };
            (
                g.state.clone(),
                part.replicas.clone(),
                g.state
                    .moving
                    .contains(&(req.partition_id, req.from_node_id)),
            )
        };
        if !part_replicas.iter().any(|r| r.node_id == req.from_node_id) {
            return fail(format!(
                "node {} does not hold a replica of partition {}",
                req.from_node_id, req.partition_id
            ));
        }
        if already_moving {
            return fail("move already in progress".to_string());
        }

        // 候选排除:现有副本节点 ∪ 下线节点(自动选择时;指定目标时另作校验)。
        let mut exclude: std::collections::HashSet<NodeId> =
            part_replicas.iter().map(|r| r.node_id).collect();
        exclude.extend(state.decommissioning.iter().copied());
        let replica = if req.to_node_id != 0 {
            if !up.contains(&req.to_node_id) {
                return fail(format!("target node {} is not up", req.to_node_id));
            }
            if exclude.contains(&req.to_node_id) {
                return fail(format!(
                    "target node {} already holds a replica or is decommissioning",
                    req.to_node_id
                ));
            }
            let Some(info) = state.nodes.get(&req.to_node_id) else {
                return fail(format!("target node {} not registered", req.to_node_id));
            };
            // 选盘限定在该节点。
            let candidates = BTreeMap::from([(req.to_node_id, info.clone())]);
            match select_disks(
                &candidates,
                &placement_usage(&self.soft, &state, &self.config),
                1,
                self.config.partition_capacity_bytes,
                self.config.disk_near_full_ratio,
            ) {
                Ok(mut reps) => reps.remove(0),
                Err(_) => return fail("no eligible disk".to_string()),
            }
        } else {
            match pick_replica_disk(
                &self.soft,
                &state,
                &state.nodes,
                &self.config,
                &exclude,
                &up,
            ) {
                Ok(r) => r,
                Err(_) => return fail("no eligible disk".to_string()),
            }
        };
        let to_node_id = replica.node_id;
        let to_disk_id = replica.disk_id.clone();
        match self
            .raft
            .client_write(MasterCommand::MoveReplica {
                partition_id: req.partition_id,
                from_node_id: req.from_node_id,
                replica,
            })
            .await
        {
            Ok(_) => Ok(Response::new(MoveReplicaResponse {
                version: PROTO_VERSION,
                ok: true,
                leader_addr: String::new(),
                error: String::new(),
                to_node_id,
                to_disk_id,
            })),
            Err(e) => fail(format!("{e}")),
        }
    }

    /// AssignReplicaIncarnation:为某 `(partition, node)` 上已有的副本**发放一个全新身份**。
    ///
    /// store 每次**从零建一个副本目录**时调用(重建 / 换盘后的副本不得沿用旧身份:旧身份会让拉取方
    /// 手里的复制游标看似合法,进而误判追平 → 带缺数据转 `ACTIVE` → master 删源 → 丢数据)。
    /// **每次调用都是更大的新号**,不是取回旧号。号由 apply 发放(确定性),返回的值已落进 placement
    /// —— 随后的 `LookupPartition` 会看到同一个值。
    ///
    /// leader-only + 需准入密钥:非 leader 回 `ok=false` + `leader_addr` 供重定向(不代转)。
    /// 副本不在 placement(分区不存在 / 该节点没这份副本)→ `ok=false`,**不发号也不改状态**。
    async fn assign_replica_incarnation(
        &self,
        request: Request<AssignIncarnationRequest>,
    ) -> Result<Response<AssignIncarnationResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let fail = |leader_addr: String, error: String| {
            Ok(Response::new(AssignIncarnationResponse {
                version: PROTO_VERSION,
                ok: false,
                incarnation: 0,
                leader_addr,
                error,
            }))
        };

        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return fail(
                self.leader_addr().unwrap_or_default(),
                "not leader".to_string(),
            );
        }

        match self
            .raft
            .client_write(MasterCommand::AssignReplicaIncarnation {
                partition_id: req.partition_id,
                node_id: req.node_id,
            })
            .await
        {
            Ok(resp) => match resp.data {
                crate::types::MasterResponse::Incarnation {
                    incarnation: Some(v),
                    ..
                } => Ok(Response::new(AssignIncarnationResponse {
                    version: PROTO_VERSION,
                    ok: true,
                    incarnation: v,
                    leader_addr: String::new(),
                    error: String::new(),
                })),
                crate::types::MasterResponse::Incarnation {
                    incarnation: None, ..
                } => fail(
                    String::new(),
                    format!(
                        "node {} holds no replica of partition {}",
                        req.node_id, req.partition_id
                    ),
                ),
                other => fail(
                    String::new(),
                    format!("unexpected apply response: {other:?}"),
                ),
            },
            Err(e) => fail(self.leader_addr().unwrap_or_default(), format!("{e}")),
        }
    }

    /// EvalBalance:leader 侧算计划(读软状态,非确定)→ `SetBalancePlan` 落 Raft(status PREVIEW)。
    /// 新评估**整体替换**旧计划(无论其状态);无搬迁需求也返回 ok=true + 空 moves。
    async fn eval_balance(
        &self,
        request: Request<EvalBalanceRequest>,
    ) -> Result<Response<EvalBalanceResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let fail = |svc: &MasterService, error: String| {
            Ok(Response::new(EvalBalanceResponse {
                version: PROTO_VERSION,
                ok: false,
                leader_addr: svc.leader_addr().unwrap_or_default(),
                error,
                plan: None,
            }))
        };
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return fail(self, "not leader".to_string());
        }

        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let up = self.soft.up_nodes(timeout);
        let usage = self.soft.usage_snapshot();
        let used = self.soft.used_bytes_by_partition();
        let (nodes, partitions, moving, decom, prev_plan_id) = {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            (
                g.state.nodes.clone(),
                g.state.partitions.clone(),
                g.state.moving.clone(),
                g.state.decommissioning.clone(),
                g.state.balance.as_ref().map(|p| p.plan_id).unwrap_or(0),
            )
        };
        let max_moves = if req.max_moves == 0 {
            self.config.balance_max_moves
        } else {
            req.max_moves
        };
        let moves = crate::balance::plan_balance(&crate::balance::BalanceInputs {
            partitions: &partitions,
            nodes: &nodes,
            usage: &usage,
            used_bytes: &used,
            moving: &moving,
            decommissioning: &decom,
            up: &up,
            max_moves,
            skew_threshold: self.config.balance_skew_threshold,
            partition_capacity_bytes: self.config.partition_capacity_bytes,
            near_full_ratio: self.config.disk_near_full_ratio,
            reserved_ratio: self.config.disk_reserved_ratio,
        });
        let record = BalancePlanRecord {
            // plan_id 单调:旧计划 id + 1(读自已提交状态,切主后不回头;时间戳同理由提案带入)。
            plan_id: prev_plan_id + 1,
            status: BalanceStatus::Preview,
            created_at_ms: now_ms(),
            max_in_flight: self.config.balance_max_in_flight,
            moves,
            failed: Vec::new(),
        };
        match self
            .raft
            .client_write(MasterCommand::SetBalancePlan {
                plan: Some(record.clone()),
            })
            .await
        {
            Ok(_) => {
                // 新计划全部 move 处于 pending(派生口径),直接构造视图返回。
                let plan = crate::balance::plan_view(&record, &partitions, &moving);
                tracing::info!(
                    plan_id = record.plan_id,
                    moves = plan.total,
                    "balance plan evaluated (PREVIEW)"
                );
                Ok(Response::new(EvalBalanceResponse {
                    version: PROTO_VERSION,
                    ok: true,
                    leader_addr: String::new(),
                    error: String::new(),
                    plan: Some(plan),
                }))
            }
            Err(e) => fail(self, format!("{e}")),
        }
    }

    /// StartBalance:确认当前 PREVIEW 计划 → RUNNING(reconciler 执行器随后按并发上限推进)。
    async fn start_balance(
        &self,
        request: Request<StartBalanceRequest>,
    ) -> Result<Response<StartBalanceResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let resp = |ok: bool, leader_addr: String, error: String| {
            Ok(Response::new(StartBalanceResponse {
                version: PROTO_VERSION,
                ok,
                leader_addr,
                error,
            }))
        };
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return resp(
                false,
                self.leader_addr().unwrap_or_default(),
                "not leader".to_string(),
            );
        }
        {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            let cur = g.state.balance.as_ref();
            // 必须是当前计划且处于 PREVIEW:plan_id 不匹配 = 陈旧确认(计划已被替换/取消)。
            match cur {
                Some(p) if p.plan_id == req.plan_id && p.status == BalanceStatus::Preview => {}
                Some(p) if p.plan_id == req.plan_id => {
                    return resp(
                        false,
                        String::new(),
                        format!(
                            "plan {} is not preview (status {:?})",
                            req.plan_id, p.status
                        ),
                    );
                }
                _ => {
                    return resp(
                        false,
                        String::new(),
                        format!("plan {} not found", req.plan_id),
                    )
                }
            }
        }
        match self
            .raft
            .client_write(MasterCommand::SetBalanceStatus {
                plan_id: req.plan_id,
                status: BalanceStatus::Running,
            })
            .await
        {
            Ok(_) => resp(true, String::new(), String::new()),
            Err(e) => resp(
                false,
                self.leader_addr().unwrap_or_default(),
                format!("{e}"),
            ),
        }
    }

    /// CancelBalance:清掉当前计划(`SetBalancePlan{None}`)。**不回滚**已受理的 move:
    /// 在途迁移的 `moving` 记账仍在,由 reconciler 既有的"后删 + DROPPED + 记账清理"自然收敛
    /// (等价于一次手动 MoveReplica 走到完,守 §3.1 先补后删)。
    async fn cancel_balance(
        &self,
        request: Request<CancelBalanceRequest>,
    ) -> Result<Response<CancelBalanceResponse>, Status> {
        let req = request.into_inner();
        self.check_secret(&req.cluster_secret)?;
        let resp = |ok: bool, leader_addr: String, error: String| {
            Ok(Response::new(CancelBalanceResponse {
                version: PROTO_VERSION,
                ok,
                leader_addr,
                error,
            }))
        };
        let is_leader = self.raft.metrics().borrow().current_leader == Some(self.config.node_id);
        if !is_leader {
            return resp(
                false,
                self.leader_addr().unwrap_or_default(),
                "not leader".to_string(),
            );
        }
        {
            let g = self
                .sm_data
                .read()
                .map_err(|_| Status::internal("sm lock poisoned"))?;
            match g.state.balance.as_ref() {
                Some(p) if p.plan_id == req.plan_id => {}
                _ => {
                    return resp(
                        false,
                        String::new(),
                        format!("plan {} not found", req.plan_id),
                    )
                }
            }
        }
        match self
            .raft
            .client_write(MasterCommand::SetBalancePlan { plan: None })
            .await
        {
            Ok(_) => {
                tracing::info!(
                    plan_id = req.plan_id,
                    "balance plan cancelled (in-flight moves converge naturally)"
                );
                resp(true, String::new(), String::new())
            }
            Err(e) => resp(
                false,
                self.leader_addr().unwrap_or_default(),
                format!("{e}"),
            ),
        }
    }

    /// GetBalanceState:任何节点可答(只读);返回计划 + 逐 move 的派生进度。
    async fn get_balance_state(
        &self,
        _request: Request<BalanceStateRequest>,
    ) -> Result<Response<BalanceStateResponse>, Status> {
        let g = self
            .sm_data
            .read()
            .map_err(|_| Status::internal("sm lock poisoned"))?;
        match g.state.balance.as_ref() {
            Some(plan) => {
                let view = crate::balance::plan_view(plan, &g.state.partitions, &g.state.moving);
                Ok(Response::new(BalanceStateResponse {
                    version: PROTO_VERSION,
                    present: true,
                    plan: Some(view),
                }))
            }
            None => Ok(Response::new(BalanceStateResponse {
                version: PROTO_VERSION,
                present: false,
                plan: None,
            })),
        }
    }
}

fn pstate(s: PartitionState) -> &'static str {
    match s {
        PartitionState::ReadWrite => "RW",
        PartitionState::ReadOnly => "RO",
        PartitionState::Sealed => "SEALED",
    }
}

/// 建**一个**分区(手动 `CreatePartitions` 与自动扩容 loop 共用):
/// leader 侧选盘(读软状态,非确定)→ `client_write` 写 placement(确定性 apply)。返回新分区 id。
async fn place_one_partition(
    raft: &Raft<TypeConfig>,
    sm_data: &Arc<RwLock<StateMachineData>>,
    soft: &SoftState,
    config: &MasterConfig,
    metrics: &MasterMetrics,
    rf: u8,
) -> Result<crate::types::PartitionId, String> {
    let state = {
        let g = sm_data.read().map_err(|_| "sm lock poisoned".to_string())?;
        g.state.clone()
    };
    let replicas = select_disks(
        &state.nodes,
        &placement_usage(soft, &state, config),
        rf,
        config.partition_capacity_bytes,
        config.disk_near_full_ratio,
    )
    .map_err(|e| {
        metrics.partition_create_failed.inc();
        format!("placement: {e}")
    })?;
    let placed: Vec<(NodeId, DiskId)> = replicas
        .iter()
        .map(|r| (r.node_id, r.disk_id.clone()))
        .collect();
    match raft
        .client_write(MasterCommand::CreatePartition {
            replication_factor: rf,
            replicas,
            partition_capacity: config.partition_capacity_bytes,
        })
        .await
    {
        Ok(resp) => match resp.data {
            crate::types::MasterResponse::PartitionId(id) => {
                metrics.partitions_created.inc();
                for (node, disk) in &placed {
                    soft.deduct_disk_free(*node, disk, config.partition_capacity_bytes);
                }
                Ok(id)
            }
            other => Err(format!("unexpected apply response: {other:?}")),
        },
        Err(e) => {
            metrics.partition_create_failed.inc();
            Err(format!("{e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 写权重(千分比)的边界与单调性:空 = 1000、半满 = 500、满/超卖 = 下限 1、未配上限 = 1000。
    #[test]
    fn capacity_weight_is_remaining_ratio_with_floor_one() {
        let cap = 64 << 20;
        assert_eq!(capacity_weight_permille(cap, 0), 1000, "完全空 → 满权重");
        assert_eq!(
            capacity_weight_permille(cap, cap / 2),
            500,
            "用掉一半 → 500"
        );
        assert_eq!(
            capacity_weight_permille(cap, cap),
            1,
            "写满 → 下限 1(不被饿死)"
        );
        assert_eq!(capacity_weight_permille(cap, cap * 2), 1, "超卖 → 仍 1");
        assert_eq!(
            capacity_weight_permille(cap, cap - 1),
            1,
            "只剩 1 字节 → 量化后落到下限,不能是 0(0 表示'未下发')"
        );
        assert_eq!(
            capacity_weight_permille(0, 0),
            1000,
            "未配容量上限 → 无意见(均权)"
        );
        assert_eq!(capacity_weight_permille(0, 123), 1000);
        // cap 取到 u64 上界也不许溢出/panic(u128 内部算)。
        assert_eq!(capacity_weight_permille(u64::MAX, 0), 1000);
        assert_eq!(capacity_weight_permille(u64::MAX, u64::MAX / 2), 500);
    }

    /// 分区级写满阈值:已用 ≥ `容量 − 一个段容量` 才转 RO;未配容量(cap=0)永不触发。
    #[test]
    fn should_convert_to_ro_uses_segment_headroom_threshold() {
        let gib = 1024 * 1024 * 1024u64;
        let cap = 100 * gib;
        let seg = 4 * gib;
        let threshold = cap - seg; // 96 GiB

        assert!(
            !should_convert_to_ro(cap, seg, threshold - 1),
            "阈值下一字节不转"
        );
        assert!(should_convert_to_ro(cap, seg, threshold), "恰达阈值即转");
        assert!(should_convert_to_ro(cap, seg, cap), "写满必转");
        assert!(should_convert_to_ro(cap, seg, cap * 2), "超卖也转");
        assert!(!should_convert_to_ro(cap, seg, 0), "空分区不转");
        assert!(
            !should_convert_to_ro(0, seg, u64::MAX),
            "cap=0 无意见,永不转"
        );
    }

    /// 分区容量小于一个段容量时,剩余余量 `saturating_sub` 落到 0:任何非空用量都视为将满。
    #[test]
    fn should_convert_to_ro_saturates_when_capacity_below_segment() {
        assert!(should_convert_to_ro(1024, 4096, 1), "cap<seg 时非空即转");
        assert!(
            should_convert_to_ro(1024, 4096, 0),
            "cap<seg 时 0 用已达阈值 0"
        );
    }

    #[test]
    fn placement_usage_mixed_committed_accounting() {
        use crate::soft::{DiskUsage, ReplicaState, SoftState};
        use crate::types::{
            MasterState, NodeInfo, PartitionInfo, PartitionState, Policy, ReplicaPlacement,
        };
        use std::collections::{BTreeMap, BTreeSet, HashMap};

        let gib = 1024u64 * 1024 * 1024;
        let config = MasterConfig {
            partition_capacity_bytes: 100 * gib,
            disk_reserved_ratio: 0.0, // no reserve, simplify math
            ..MasterConfig::default()
        };

        // 1 node, 1 disk
        let node_id = 1u64;
        let disk_id = "d0".to_string();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id,
            NodeInfo {
                dc: "dc1".into(),
                rack: "r1".into(),
                addr: "127.0.0.1:1".into(),
                disks: BTreeMap::from([(
                    disk_id.clone(),
                    crate::types::DiskSpec {
                        capacity_bytes: 800 * gib,
                    },
                )]),
                registered_at_ms: 0,
            },
        );

        let mk_replica = |nid, did: &str| ReplicaPlacement {
            node_id: nid,
            disk_id: did.to_string(),
            target_state: "ACTIVE".into(),
            incarnation: 1,
        };

        let mut partitions = BTreeMap::new();
        // 2 RW partitions (committed = capacity_bytes each)
        partitions.insert(
            0,
            PartitionInfo {
                replication_factor: 1,
                state: PartitionState::ReadWrite,
                replicas: vec![mk_replica(node_id, &disk_id)],
                capacity_bytes: 100 * gib,
            },
        );
        partitions.insert(
            1,
            PartitionInfo {
                replication_factor: 1,
                state: PartitionState::ReadWrite,
                replicas: vec![mk_replica(node_id, &disk_id)],
                capacity_bytes: 100 * gib,
            },
        );
        // 3 RO partitions (committed = used_bytes if available)
        for i in 2..5 {
            partitions.insert(
                i,
                PartitionInfo {
                    replication_factor: 1,
                    state: PartitionState::ReadOnly,
                    replicas: vec![mk_replica(node_id, &disk_id)],
                    capacity_bytes: 100 * gib,
                },
            );
        }
        // 1 SEALED partition
        partitions.insert(
            5,
            PartitionInfo {
                replication_factor: 1,
                state: PartitionState::Sealed,
                replicas: vec![mk_replica(node_id, &disk_id)],
                capacity_bytes: 100 * gib,
            },
        );

        let state = MasterState {
            next_partition_id: 6,
            next_incarnation: 10,
            object_next: BTreeMap::new(),
            nodes,
            partitions,
            policy: Policy::default(),
            decommissioning: BTreeSet::new(),
            moving: BTreeSet::new(),
            move_targets: BTreeMap::new(),
            pending_drop: BTreeSet::new(),
            maintenance: false,
            pause_autogrow: false,
            balance: None,
        };

        // Soft state: report used_bytes for RO/SEALED partitions
        let soft = SoftState::default();
        let mut disks = HashMap::new();
        disks.insert(
            disk_id.clone(),
            DiskUsage {
                capacity_bytes: 800 * gib,
                free_bytes: 500 * gib,
                health: String::new(),
                adjusted_free: 500 * gib,
                last_heartbeat_time: std::time::Instant::now(),
            },
        );
        soft.record_beat(
            node_id,
            disks,
            std::time::Duration::from_secs(config.adjusted_free_timeout_secs),
        );

        // Report used_bytes: RO partitions 2,3,4 at 40G each; SEALED partition 5 at 80G
        // RW partitions 0,1 at 50G each (should NOT affect committed)
        let replicas_data: Vec<(crate::types::PartitionId, ReplicaState)> = vec![
            (
                0,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 50 * gib,
                },
            ),
            (
                1,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 50 * gib,
                },
            ),
            (
                2,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 40 * gib,
                },
            ),
            (
                3,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 40 * gib,
                },
            ),
            (
                4,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 40 * gib,
                },
            ),
            (
                5,
                ReplicaState {
                    disk_id: disk_id.clone(),
                    current_state: "ACTIVE".into(),
                    lag_bytes: 0,
                    used_bytes: 80 * gib,
                },
            ),
        ];
        soft.record_replicas(node_id, replicas_data, &[], true);

        let usage = placement_usage(&soft, &state, &config);
        let disk_free = &usage[&(node_id, disk_id.clone())];
        let expected_committed = 2 * 100 * gib + 3 * 40 * gib + 80 * gib;
        assert_eq!(
            disk_free.committed_bytes,
            expected_committed,
            "mixed committed: RW=cap, RO/SEALED=used_bytes; got {} expected {}",
            disk_free.committed_bytes / gib,
            expected_committed / gib
        );

        // Without mixed accounting it would be 6×100G = 600G
        assert!(
            disk_free.committed_bytes < 6 * 100 * gib,
            "mixed accounting must be less than all-capacity"
        );
    }

    #[test]
    fn placement_usage_fallback_when_used_bytes_missing() {
        use crate::soft::{DiskUsage, SoftState};
        use crate::types::{
            MasterState, NodeInfo, PartitionInfo, PartitionState, Policy, ReplicaPlacement,
        };
        use std::collections::{BTreeMap, BTreeSet, HashMap};

        let gib = 1024u64 * 1024 * 1024;
        let config = MasterConfig {
            partition_capacity_bytes: 100 * gib,
            disk_reserved_ratio: 0.0,
            ..MasterConfig::default()
        };

        let node_id = 1u64;
        let disk_id = "d0".to_string();
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id,
            NodeInfo {
                dc: "dc1".into(),
                rack: "r1".into(),
                addr: "127.0.0.1:1".into(),
                disks: BTreeMap::from([(
                    disk_id.clone(),
                    crate::types::DiskSpec {
                        capacity_bytes: 800 * gib,
                    },
                )]),
                registered_at_ms: 0,
            },
        );

        let mk_replica = |nid, did: &str| ReplicaPlacement {
            node_id: nid,
            disk_id: did.to_string(),
            target_state: "ACTIVE".into(),
            incarnation: 1,
        };

        let mut partitions = BTreeMap::new();
        // 1 RO partition — no used_bytes will be reported → fallback to capacity_bytes
        partitions.insert(
            0,
            PartitionInfo {
                replication_factor: 1,
                state: PartitionState::ReadOnly,
                replicas: vec![mk_replica(node_id, &disk_id)],
                capacity_bytes: 100 * gib,
            },
        );

        let state = MasterState {
            next_partition_id: 1,
            next_incarnation: 2,
            object_next: BTreeMap::new(),
            nodes,
            partitions,
            policy: Policy::default(),
            decommissioning: BTreeSet::new(),
            moving: BTreeSet::new(),
            move_targets: BTreeMap::new(),
            pending_drop: BTreeSet::new(),
            maintenance: false,
            pause_autogrow: false,
            balance: None,
        };

        let soft = SoftState::default();
        let mut disks = HashMap::new();
        disks.insert(
            disk_id.clone(),
            DiskUsage {
                capacity_bytes: 800 * gib,
                free_bytes: 700 * gib,
                health: String::new(),
                adjusted_free: 700 * gib,
                last_heartbeat_time: std::time::Instant::now(),
            },
        );
        soft.record_beat(
            node_id,
            disks,
            std::time::Duration::from_secs(config.adjusted_free_timeout_secs),
        );
        // No replica used_bytes reported → used_map has no entry for partition 0

        let usage = placement_usage(&soft, &state, &config);
        let disk_free = &usage[&(node_id, disk_id)];

        // Fallback: RO with no used_bytes → capacity_bytes = 100G
        assert_eq!(
            disk_free.committed_bytes,
            100 * gib,
            "RO without used_bytes must fallback to capacity_bytes"
        );
    }
}

//! master 节点装配 + gRPC 服务实现(控制面 `Master` + `RaftInternal`)。
//!
//! - `RaftInternal`:承接对端 master 的 Raft RPC,反序列化 payload → 交本地 `Raft` → 序列化结果。
//! - `Master`:数据节点接入(自注册 Q1 + 准入校验)、心跳(软状态重建)、只读查询。
//! - 只有 leader 能提交写(RegisterNode);follower 回 leader 地址供 client 重定向。

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::{BasicNode, Config, Raft};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::config::MasterConfig;
use crate::metrics::MasterMetrics;
use crate::network::GrpcNetworkFactory;
use crate::pb::master_server::{Master, MasterServer};
use crate::pb::raft_internal_server::{RaftInternal, RaftInternalServer};
use crate::pb::{
    AssignRequest, AssignResponse, ClusterState, ClusterStateRequest, CreatePartitionsRequest,
    CreatePartitionsResponse, DecommissionRequest, DecommissionResponse, ListNodesRequest,
    ListPartitionsRequest, ListPartitionsResponse, LookupRequest, MasterDirective, NodeBeat,
    NodeEntry, NodeList, PartitionEntry, PartitionLocations, RaftMessage, RegisterRequest,
    RegisterResponse, ReplicaCurrent, ReplicaLoc, SetPartitionStateRequest,
    SetPartitionStateResponse,
};
use crate::placement::{select_disks, DiskFree};
use crate::reconcile::{plan_evacuation, plan_rereplication, plan_transitions, EvacAction};
use crate::soft::{DiskUsage, SoftState};
use crate::store::{open, StateMachineData};
use crate::types::{DiskSpec, MasterCommand, NodeId, NodeInfo, PartitionId, PartitionState, TypeConfig};

const PROTO_VERSION: u32 = 1;

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// 装配好的 master 节点:持有 Raft 句柄、共享状态机内存态、软状态、指标、配置。
#[derive(Clone)]
pub struct MasterNode {
    pub raft: Raft<TypeConfig>,
    pub sm_data: Arc<RwLock<StateMachineData>>,
    pub soft: SoftState,
    pub metrics: MasterMetrics,
    pub config: MasterConfig,
}

impl MasterNode {
    /// 打开存储、构建 Raft、装配节点(不建 gRPC 监听;监听见 [`serve`](Self::serve))。
    pub async fn start(
        config: MasterConfig,
        registry: &metric::Registry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let metrics = MasterMetrics::new(registry)?;
        std::fs::create_dir_all(&config.data_dir)?;
        let path = std::path::Path::new(&config.data_dir).join("master.redb");
        let (log_store, sm) = open(&path, metrics.clone())?;
        let sm_data = sm.shared_data();

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

        Ok(MasterNode { raft, sm_data, soft: SoftState::default(), metrics, config })
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
        let raft_addr = self.config.raft_addr.parse()?;
        let grpc_addr = self.config.grpc_addr.parse()?;

        let raft_svc = RaftInternalServer::new(RaftInternalService { raft: self.raft.clone() });
        let master_svc = MasterServer::new(MasterService {
            raft: self.raft.clone(),
            sm_data: self.sm_data.clone(),
            soft: self.soft.clone(),
            metrics: self.metrics.clone(),
            config: self.config.clone(),
        });

        tokio::spawn(async move {
            let _ = Server::builder().add_service(raft_svc).serve(raft_addr).await;
        });
        tokio::spawn(async move {
            let _ = Server::builder().add_service(master_svc).serve(grpc_addr).await;
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
                    metrics.is_leader.set(i64::from(m.current_leader == Some(this_node)));
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
                let (rw, total, rf) = {
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
                    (rw, g.state.partitions.len() as u64, g.state.policy.default_replication_factor)
                };
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
            loop {
                tick.tick().await;
                if raft.metrics().borrow().current_leader != Some(config.node_id) {
                    leader_since = None; // 卸任 leader:下次重新计稳定期
                    last_unsatisfiable.clear();
                    continue;
                }
                // 启动/切主稳定期:软状态靠心跳从零重建,期内不据"暂无心跳"误判失联(防再复制风暴)。
                let since = *leader_since.get_or_insert_with(Instant::now);
                if !crate::reconcile::reconcile_ready(since.elapsed(), grace) {
                    continue;
                }
                let up = soft.up_nodes(timeout);
                let (partitions, all_nodes, decom) = {
                    let g = match sm_data.read() {
                        Ok(g) => g,
                        Err(_) => continue,
                    };
                    (g.state.partitions.clone(), g.state.nodes.clone(), g.state.decommissioning.clone())
                };

                // 补副本目标:失联再复制(M3)∪ 下线 evacuation 的"先补"(M4)。动作相同,合并去重。
                let evac = plan_evacuation(&partitions, &decom, &up);
                let errored = soft.errored_replicas();
                let mut add_targets: std::collections::BTreeSet<PartitionId> =
                    plan_rereplication(&partitions, &up, &errored).into_iter().collect();
                for a in &evac {
                    if let EvacAction::Add(pid) = a {
                        add_targets.insert(*pid);
                    }
                }
                metrics.under_replicated_partitions.set(add_targets.len() as i64);
                let mut unsatisfiable: std::collections::BTreeSet<PartitionId> =
                    std::collections::BTreeSet::new();
                for pid in add_targets {
                    // 排除该分区已有节点 + 所有下线节点(故障域打散,只落存活健康盘)。
                    let mut exclude: std::collections::HashSet<NodeId> =
                        partitions[&pid].replicas.iter().map(|r| r.node_id).collect();
                    exclude.extend(decom.iter().copied());
                    if try_add_replica(&raft, &soft, &all_nodes, &config, &metrics, pid, &exclude, &up)
                        .await
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
                            .client_write(MasterCommand::RemoveReplica { partition_id: pid, node_id: node })
                            .await
                            .is_ok()
                        {
                            metrics.replicas_removed.inc();
                        }
                    }
                }
            }
        });
    }
}

/// 在一个健康、非排除的节点上给分区补一个副本(re-replication / evacuation 共用)。
#[allow(clippy::too_many_arguments)]
async fn try_add_replica(
    raft: &Raft<TypeConfig>,
    soft: &SoftState,
    all_nodes: &BTreeMap<NodeId, NodeInfo>,
    config: &MasterConfig,
    metrics: &MasterMetrics,
    pid: crate::types::PartitionId,
    exclude: &std::collections::HashSet<NodeId>,
    up: &std::collections::HashSet<NodeId>,
) -> bool {
    let candidates: BTreeMap<NodeId, NodeInfo> = all_nodes
        .iter()
        .filter(|(id, _)| !exclude.contains(id) && up.contains(id))
        .map(|(id, info)| (*id, info.clone()))
        .collect();
    let usage = soft
        .free_snapshot()
        .into_iter()
        .map(|(k, free_bytes)| (k, DiskFree { free_bytes }))
        .collect();
    match select_disks(
        &candidates,
        &usage,
        1,
        config.partition_capacity_bytes,
        config.disk_near_full_ratio,
    ) {
        Ok(mut reps) => {
            let replica = reps.remove(0);
            if raft
                .client_write(MasterCommand::AddReplica { partition_id: pid, replica })
                .await
                .is_ok()
            {
                metrics.rereplications.inc();
            }
            false
        }
        Err(e) => {
            tracing::debug!(partition = pid, error = %e, "reconcile: no eligible disk to add replica");
            true
        }
    }
}

/// 是否存在可放新副本的空闲健康节点(re-replication / healable / 下线可行性共用)。
fn has_spare_disk(
    all_nodes: &BTreeMap<NodeId, NodeInfo>,
    soft: &SoftState,
    config: &MasterConfig,
    exclude: &std::collections::HashSet<NodeId>,
    up: &std::collections::HashSet<NodeId>,
) -> bool {
    let candidates: BTreeMap<NodeId, NodeInfo> = all_nodes
        .iter()
        .filter(|(id, _)| !exclude.contains(id) && up.contains(id))
        .map(|(id, info)| (*id, info.clone()))
        .collect();
    let usage = soft
        .free_snapshot()
        .into_iter()
        .map(|(k, free_bytes)| (k, DiskFree { free_bytes }))
        .collect();
    select_disks(&candidates, &usage, 1, config.partition_capacity_bytes, config.disk_near_full_ratio)
        .is_ok()
}

fn enc<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(v).map_err(|e| Status::internal(format!("encode: {e}")))
}
fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(b).map_err(|e| Status::internal(format!("decode: {e}")))
}
fn reply(payload: Vec<u8>) -> Response<RaftMessage> {
    Response::new(RaftMessage { version: PROTO_VERSION, payload })
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
        let addrs: BTreeMap<u64, String> =
            m.membership_config.nodes().map(|(id, n)| (*id, n.addr.clone())).collect();
        addrs.get(&leader).cloned()
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
            .map(|d| (d.disk_id, DiskSpec { capacity_bytes: d.capacity_bytes }))
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
            .client_write(MasterCommand::RegisterNode { node_id: req.node_id, info })
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

    type HeartbeatStream = Pin<Box<dyn futures::Stream<Item = Result<MasterDirective, Status>> + Send>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<NodeBeat>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let mut in_stream = request.into_inner();
        let soft = self.soft.clone();
        let sm_data = self.sm_data.clone();
        let metrics = self.metrics.clone();
        let secret = self.config.cluster_secret.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            while let Ok(Some(beat)) = in_stream.message().await {
                if !secret.is_empty() && beat.cluster_secret != secret {
                    let _ = tx.send(Err(Status::unauthenticated("invalid cluster secret"))).await;
                    break;
                }
                metrics.heartbeats.inc();
                let disks: HashMap<String, DiskUsage> = beat
                    .disks
                    .into_iter()
                    .map(|d| (d.disk_id, DiskUsage { capacity_bytes: d.capacity_bytes, free_bytes: d.free_bytes }))
                    .collect();
                soft.record_beat(beat.node_id, disks);

                // M3:记录副本实然态,算出该节点待下发的转移(current != target)。
                let reported: BTreeMap<PartitionId, String> =
                    beat.replicas.iter().map(|r| (r.partition_id, r.current_state.clone())).collect();
                soft.record_replicas(
                    beat.node_id,
                    beat.replicas.into_iter().map(|r| {
                        (r.partition_id, crate::soft::ReplicaState { current_state: r.current_state, lag_bytes: r.lag_bytes })
                    }),
                );
                let transitions: Vec<crate::pb::Transition> = {
                    let g = match sm_data.read() {
                        Ok(g) => g,
                        Err(_) => break,
                    };
                    plan_transitions(beat.node_id, &g.state.partitions, &reported)
                        .into_iter()
                        .map(|t| crate::pb::Transition {
                            partition_id: t.partition_id,
                            disk_id: t.disk_id,
                            target_state: t.target_state,
                        })
                        .collect()
                };
                if !transitions.is_empty() {
                    metrics.transitions_issued.inc_by(transitions.len() as u64);
                }
                if tx
                    .send(Ok(MasterDirective { version: PROTO_VERSION, transitions, tasks: vec![] }))
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
        let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
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
                        health: String::new(),
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
        Ok(Response::new(NodeList { version: PROTO_VERSION, nodes }))
    }

    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterState>, Status> {
        let m = self.raft.metrics().borrow().clone();
        let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
        Ok(Response::new(ClusterState {
            version: PROTO_VERSION,
            this_node: self.config.node_id,
            leader: m.current_leader,
            term: m.current_term,
            registered_nodes: g.state.nodes.len() as u64,
            next_partition_id: g.state.next_partition_id,
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
            let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
            g.state.policy.default_replication_factor
        };
        let rf = if req.replication_factor == 0 { rf_default } else { req.replication_factor as u8 };

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
        let pid = request.into_inner().partition_id;
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
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
                addr: g.state.nodes.get(&r.node_id).map(|n| n.addr.clone()).unwrap_or_default(),
                disk_id: r.disk_id.clone(),
                target_state: r.target_state.clone(),
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
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
        let total = g.state.partitions.len() as u64;
        let up_set = self.soft.up_nodes(timeout);
        // BTreeMap 按 partition_id 升序,offset/limit 稳定切片(limit=0 表示不限)。
        let take = if req.limit == 0 { usize::MAX } else { req.limit as usize };
        let partitions = g
            .state
            .partitions
            .iter()
            .skip(req.offset as usize)
            .take(take)
            .map(|(pid, part)| {
                let replicas = part
                    .replicas
                    .iter()
                    .map(|r| ReplicaLoc {
                        node_id: r.node_id,
                        addr: g.state.nodes.get(&r.node_id).map(|n| n.addr.clone()).unwrap_or_default(),
                        disk_id: r.disk_id.clone(),
                        target_state: r.target_state.clone(),
                        up: self.soft.is_up(r.node_id, timeout),
                    })
                    .collect();
                let currents = part
                    .replicas
                    .iter()
                    .filter_map(|r| {
                        self.soft.replica_state(*pid, r.node_id).map(|st| ReplicaCurrent {
                            node_id: r.node_id,
                            current_state: st.current_state,
                            lag_bytes: st.lag_bytes,
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
                        has_spare_disk(&g.state.nodes, &self.soft, &self.config, &exclude, &up_set)
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
                }
            })
            .collect();
        Ok(Response::new(ListPartitionsResponse { version: PROTO_VERSION, partitions, total }))
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
            .client_write(MasterCommand::SetPartitionState { partition_id: req.partition_id, state })
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

    async fn assign_partition(
        &self,
        _request: Request<AssignRequest>,
    ) -> Result<Response<AssignResponse>, Status> {
        let timeout = Duration::from_secs(self.config.heartbeat_timeout_secs);
        let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
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
            None => Ok(Response::new(AssignResponse {
                version: PROTO_VERSION,
                ok: false,
                partition_id: 0,
                replicas: vec![],
                error: "no writable partition".to_string(),
            })),
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
                let g = self.sm_data.read().map_err(|_| Status::internal("sm lock poisoned"))?;
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
                        !has_spare_disk(&g.state.nodes, &self.soft, &self.config, &exclude, &up_set)
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
        match self.raft.client_write(MasterCommand::DecommissionNode { node_id: req.node_id }).await {
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
    let nodes = {
        let g = sm_data.read().map_err(|_| "sm lock poisoned".to_string())?;
        g.state.nodes.clone()
    };
    let usage = soft
        .free_snapshot()
        .into_iter()
        .map(|(k, free_bytes)| (k, DiskFree { free_bytes }))
        .collect();
    let replicas = select_disks(
        &nodes,
        &usage,
        rf,
        config.partition_capacity_bytes,
        config.disk_near_full_ratio,
    )
    .map_err(|e| {
        metrics.partition_create_failed.inc();
        format!("placement: {e}")
    })?;
    match raft
        .client_write(MasterCommand::CreatePartition { replication_factor: rf, replicas })
        .await
    {
        Ok(resp) => match resp.data {
            crate::types::MasterResponse::PartitionId(id) => {
                metrics.partitions_created.inc();
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

//! master 只读/运维的抽象与 gRPC 实现。
//!
//! `MasterApi` trait 让 axum handler 与具体 gRPC 客户端解耦(测试注入 mock,免起真 master + raft)。
//! DTO 为对外 JSON 契约(serde);由 master pb 消息转换而来。

use async_trait::async_trait;
use serde::Serialize;
use tonic::transport::Channel;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    BalanceStateRequest, CancelBalanceRequest, ClusterStateRequest, CreatePartitionsRequest,
    DecommissionRequest, EvalBalanceRequest, ListNodesRequest, ListPartitionsRequest,
    MetricsRequest, MoveReplicaRequest, RepairPartitionRequest, SetMaintenanceRequest,
    SetPartitionStateRequest, StartBalanceRequest,
};

use crate::error::{Error, Result};

/// 集群概览(`GET /api/cluster`)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ClusterDto {
    pub this_node: u64,
    pub leader: Option<u64>,
    pub term: u64,
    pub registered_nodes: u64,
    pub next_partition_id: u32,
    /// 全集群各盘容量之和(字节)。
    pub total_capacity_bytes: u64,
    /// 全集群各盘可用之和(字节)。
    pub free_bytes: u64,
    /// 已用 = 总 - 可用(字节)。
    pub used_bytes: u64,
    /// 存活节点数 / 已注册节点数(存活由心跳超时判定)。
    pub nodes_up: u64,
    pub nodes_total: u64,
    /// 欠副本(健康副本 < RF)分区数。
    pub under_replicated_partitions: u64,
    /// 维护模式:已暂停 master 的自动动作(再复制/迁移/下线 evacuation)。
    pub maintenance: bool,
    /// 维护模式是否连带暂停自动扩容(仅 `maintenance` 为真时有意义)。
    pub pause_autogrow: bool,
}

/// 一块盘(节点视图内)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiskDto {
    pub disk_id: String,
    pub capacity_bytes: u64,
    pub free_bytes: u64,
    pub health: String,
}

/// 一个数据节点(`GET /api/nodes`)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NodeDto {
    pub node_id: u64,
    pub dc: String,
    pub rack: String,
    pub addr: String,
    pub up: bool,
    pub disks: Vec<DiskDto>,
}

/// 一个副本(placement + 实然态联表)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicaDto {
    pub node_id: u64,
    pub addr: String,
    pub disk_id: String,
    pub target_state: String,
    pub up: bool,
    pub current_state: Option<String>,
    pub lag_bytes: Option<u64>,
    /// 该副本上报的分区实际占用字节(软状态;未上报过则为 0)。
    pub used_bytes: u64,
}

/// 一个分区(`GET /api/partitions`)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PartitionDto {
    pub partition_id: u32,
    pub state: String,
    pub replication_factor: u32,
    pub replicas: Vec<ReplicaDto>,
    /// 该分区实际占用字节 = 各副本上报 `used_bytes` 的**最大值**(副本间不重复计数)。
    pub used_bytes: u64,
    /// 存活且 ACTIVE 的副本数(< RF 即欠副本)。
    pub healthy_replicas: u32,
    /// 欠副本时是否有空闲健康节点可补(false = 卡住,需加容量)。
    pub healable: bool,
    /// 欠副本原因码(healthy 时空):node_down/replica_error/decommissioning/bootstrapping。
    pub under_replicated_reason: String,
    /// 迁移中的"待迁出"源节点(add-before-remove,新副本健康后由 reconciler 移除)。
    #[serde(default)]
    pub moving_out_nodes: Vec<u64>,
}

/// `POST /api/partitions/{id}/repair` 的结果(master 侧 ok=false 也走 200 + ok:false,错误交前端展示)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RepairOutcome {
    pub ok: bool,
    pub error: String,
    pub added_node_id: u64,
    pub added_disk_id: String,
}

/// `POST /api/partitions/{id}/move` 的结果(语义同 [`RepairOutcome`];to_node_id=0 入参 = 自动选目标)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MoveOutcome {
    pub ok: bool,
    pub error: String,
    pub to_node_id: u64,
    pub to_disk_id: String,
}

/// 一条搬迁(均衡计划内;`status` = pending / in_flight / done / failed / infeasible)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BalanceMoveDto {
    pub partition_id: u32,
    pub from_node_id: u64,
    pub from_disk_id: String,
    pub to_node_id: u64,
    pub to_disk_id: String,
    pub status: String,
    pub error: String,
}

/// 均衡计划视图(含 master 派生的进度;`status` = PREVIEW / RUNNING / CANCELLED / DONE)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BalancePlanDto {
    pub plan_id: u64,
    pub status: String,
    pub created_at_ms: u64,
    pub max_in_flight: u32,
    pub total: u32,
    pub pending: u32,
    pub in_flight: u32,
    pub done: u32,
    pub failed: u32,
    pub moves: Vec<BalanceMoveDto>,
}

/// `GET /api/balance` 的响应;`present=false` 时 `plan` 为 null。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BalanceStateDto {
    pub present: bool,
    pub plan: Option<BalancePlanDto>,
}

/// `POST /api/balance/{eval,start,cancel}` 的结果(master 侧 ok=false 也走 200 + ok:false)。
/// 只有 `eval` 会带 `plan`(start/cancel 省略该字段)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BalanceOpOutcome {
    pub ok: bool,
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<BalancePlanDto>,
}

/// admin 对 master 的只读 + 运维能力(handler 依赖此 trait,便于测试注入)。
#[async_trait]
pub trait MasterApi: Send + Sync {
    async fn cluster(&self) -> Result<ClusterDto>;
    async fn nodes(&self) -> Result<Vec<NodeDto>>;
    /// 分页列分区,返回 `(本页, 总数)`;`limit=0` 表示不限;`state` 空 = 全部,否则 RW/RO/SEALED。
    async fn partitions(
        &self,
        state: &str,
        offset: u64,
        limit: u32,
    ) -> Result<(Vec<PartitionDto>, u64)>;
    async fn create_partitions(&self, count: u32, replication_factor: u32) -> Result<Vec<u32>>;
    async fn decommission(&self, node_id: u64, force: bool) -> Result<()>;
    async fn set_partition_state(&self, partition_id: u32, state: &str) -> Result<()>;
    /// 维护模式开关(暂停/恢复 master 的自动收敛动作;`pause_autogrow` 仅暂停时有意义)。
    async fn set_maintenance(&self, paused: bool, pause_autogrow: bool) -> Result<()>;
    /// master 节点的 Prometheus 文本(`GetMetrics`,任何节点可答)。
    async fn master_metrics_text(&self) -> Result<String>;
    /// 单个分区详情;不存在返回 [`Error::PartitionNotFound`](crate::error::Error::PartitionNotFound)。
    async fn partition_detail(&self, partition_id: u32) -> Result<PartitionDto>;
    /// 手动立即补一个副本(`RepairPartition`);gRPC ok=false 不算传输错,回 Ok(outcome{ok:false})。
    async fn repair_partition(&self, partition_id: u32) -> Result<RepairOutcome>;
    /// 副本迁移(`MoveReplica`,add-before-remove);to_node_id=0 = 自动选目标。
    async fn move_replica(
        &self,
        partition_id: u32,
        from_node_id: u64,
        to_node_id: u64,
    ) -> Result<MoveOutcome>;
    /// 当前均衡计划 + 派生进度(`GetBalanceState`;任何节点可答)。
    async fn balance_state(&self) -> Result<BalanceStateDto>;
    /// 评估并落一份 PREVIEW 计划(`EvalBalance`);`max_moves=0` = 用 master 配置默认。
    async fn balance_eval(&self, max_moves: u32) -> Result<BalanceOpOutcome>;
    /// 确认执行指定计划(`StartBalance`);plan_id 不匹配 → Ok(ok=false)。
    async fn balance_start(&self, plan_id: u64) -> Result<BalanceOpOutcome>;
    /// 取消计划(`CancelBalance`,不回滚已 apply 的搬迁);plan_id 不匹配 → Ok(ok=false)。
    async fn balance_cancel(&self, plan_id: u64) -> Result<BalanceOpOutcome>;
}

/// 基于 tonic 的 `MasterApi` 实现。持有一个 `Channel`,每次调用克隆客户端(共享连接池)。
#[derive(Clone)]
pub struct GrpcMasterApi {
    client: MasterClient<Channel>,
    cluster_secret: String,
}

impl GrpcMasterApi {
    pub async fn connect(master_addr: &str, cluster_secret: String) -> Result<Self> {
        let client = MasterClient::connect(format!("http://{master_addr}"))
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?;
        Ok(GrpcMasterApi {
            client,
            cluster_secret,
        })
    }
}

#[async_trait]
impl MasterApi for GrpcMasterApi {
    async fn cluster(&self) -> Result<ClusterDto> {
        let mut c = self.client.clone();
        let s = c
            .get_cluster_state(ClusterStateRequest { version: 1 })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        // 容量/健康汇总:master 无现成聚合,由 admin 汇总节点盘容量 + 分区健康。
        let nodes = self.nodes().await?;
        let (partitions, _) = self.partitions("", 0, 0).await?;
        let total_capacity_bytes: u64 = nodes
            .iter()
            .flat_map(|n| n.disks.iter())
            .map(|d| d.capacity_bytes)
            .sum();
        let free_bytes: u64 = nodes
            .iter()
            .flat_map(|n| n.disks.iter())
            .map(|d| d.free_bytes)
            .sum();
        let nodes_up = nodes.iter().filter(|n| n.up).count() as u64;
        let under_replicated_partitions = partitions
            .iter()
            .filter(|p| !p.under_replicated_reason.is_empty())
            .count() as u64;
        Ok(ClusterDto {
            this_node: s.this_node,
            leader: s.leader,
            term: s.term,
            registered_nodes: s.registered_nodes,
            next_partition_id: s.next_partition_id,
            total_capacity_bytes,
            free_bytes,
            used_bytes: total_capacity_bytes.saturating_sub(free_bytes),
            nodes_up,
            nodes_total: nodes.len() as u64,
            under_replicated_partitions,
            maintenance: s.maintenance,
            pause_autogrow: s.pause_autogrow,
        })
    }

    async fn nodes(&self) -> Result<Vec<NodeDto>> {
        let mut c = self.client.clone();
        let list = c
            .list_nodes(ListNodesRequest { version: 1 })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        Ok(list
            .nodes
            .into_iter()
            .map(|n| NodeDto {
                node_id: n.node_id,
                dc: n.dc,
                rack: n.rack,
                addr: n.addr,
                up: n.up,
                disks: n
                    .disks
                    .into_iter()
                    .map(|d| DiskDto {
                        disk_id: d.disk_id,
                        capacity_bytes: d.capacity_bytes,
                        free_bytes: d.free_bytes,
                        health: d.health,
                    })
                    .collect(),
            })
            .collect())
    }

    async fn partitions(
        &self,
        state: &str,
        offset: u64,
        limit: u32,
    ) -> Result<(Vec<PartitionDto>, u64)> {
        let mut c = self.client.clone();
        let resp = c
            .list_partitions(ListPartitionsRequest {
                version: 1,
                offset,
                limit,
                state: state.to_string(),
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let total = resp.total;
        let items = resp
            .partitions
            .into_iter()
            .map(|p| {
                let replicas = p
                    .replicas
                    .into_iter()
                    .map(|r| {
                        let cur = p.currents.iter().find(|c| c.node_id == r.node_id);
                        ReplicaDto {
                            node_id: r.node_id,
                            addr: r.addr,
                            disk_id: r.disk_id,
                            target_state: r.target_state,
                            up: r.up,
                            current_state: cur.map(|c| c.current_state.clone()),
                            lag_bytes: cur.map(|c| c.lag_bytes),
                            used_bytes: cur.map(|c| c.used_bytes).unwrap_or(0),
                        }
                    })
                    .collect::<Vec<_>>();
                PartitionDto {
                    partition_id: p.partition_id,
                    state: p.state,
                    replication_factor: p.replication_factor,
                    // 副本间是同一份数据的拷贝,取最大而不是求和(与 master `sum_used_bytes` 口径一致)。
                    used_bytes: replicas.iter().map(|r| r.used_bytes).max().unwrap_or(0),
                    replicas,
                    healthy_replicas: p.healthy_replicas,
                    healable: p.healable,
                    under_replicated_reason: p.under_replicated_reason,
                    moving_out_nodes: p.moving_out_nodes,
                }
            })
            .collect();
        Ok((items, total))
    }

    async fn create_partitions(&self, count: u32, replication_factor: u32) -> Result<Vec<u32>> {
        let mut c = self.client.clone();
        let resp = c
            .create_partitions(CreatePartitionsRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                count,
                replication_factor,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        if !resp.ok {
            return Err(Error::MasterRejected(if resp.error.is_empty() {
                format!("not leader (leader={})", resp.leader_addr)
            } else {
                resp.error
            }));
        }
        Ok(resp.partition_ids)
    }

    async fn decommission(&self, node_id: u64, force: bool) -> Result<()> {
        let mut c = self.client.clone();
        let resp = c
            .decommission_node(DecommissionRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                node_id,
                force,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        if !resp.ok {
            let msg = if resp.error.is_empty() {
                format!("not leader (leader={})", resp.leader_addr)
            } else if resp.affected_partitions.is_empty() {
                resp.error
            } else {
                format!(
                    "{} (受影响分区: {:?})",
                    resp.error, resp.affected_partitions
                )
            };
            return Err(Error::MasterRejected(msg));
        }
        Ok(())
    }

    async fn set_partition_state(&self, partition_id: u32, state: &str) -> Result<()> {
        let mut c = self.client.clone();
        let resp = c
            .set_partition_state(SetPartitionStateRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                partition_id,
                state: state.to_string(),
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        if !resp.ok {
            return Err(Error::MasterRejected(if resp.error.is_empty() {
                format!("not leader (leader={})", resp.leader_addr)
            } else {
                resp.error
            }));
        }
        Ok(())
    }

    async fn set_maintenance(&self, paused: bool, pause_autogrow: bool) -> Result<()> {
        let mut c = self.client.clone();
        let resp = c
            .set_maintenance(SetMaintenanceRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                paused,
                pause_autogrow,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        if !resp.ok {
            return Err(Error::MasterRejected(if resp.error.is_empty() {
                format!("not leader (leader={})", resp.leader_addr)
            } else {
                resp.error
            }));
        }
        Ok(())
    }

    async fn master_metrics_text(&self) -> Result<String> {
        let mut c = self.client.clone();
        let resp = c
            .get_metrics(MetricsRequest { version: 1 })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        Ok(resp.text)
    }

    async fn partition_detail(&self, partition_id: u32) -> Result<PartitionDto> {
        // 用 ListPartitions 联表视图而非 LookupPartition:后者只有路由用 ReplicaLoc,
        // 缺 replication_factor/healthy/moving_out_nodes 等 dashboard 字段。
        let (items, _) = self.partitions("", 0, 0).await?;
        items
            .into_iter()
            .find(|p| p.partition_id == partition_id)
            .ok_or(Error::PartitionNotFound(partition_id))
    }

    async fn repair_partition(&self, partition_id: u32) -> Result<RepairOutcome> {
        let mut c = self.client.clone();
        let resp = c
            .repair_partition(RepairPartitionRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                partition_id,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let error = op_error(resp.ok, &resp.error, &resp.leader_addr);
        Ok(RepairOutcome {
            ok: resp.ok,
            error,
            added_node_id: resp.added_node_id,
            added_disk_id: resp.added_disk_id,
        })
    }

    async fn move_replica(
        &self,
        partition_id: u32,
        from_node_id: u64,
        to_node_id: u64,
    ) -> Result<MoveOutcome> {
        let mut c = self.client.clone();
        let resp = c
            .move_replica(MoveReplicaRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                partition_id,
                from_node_id,
                to_node_id,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let error = op_error(resp.ok, &resp.error, &resp.leader_addr);
        Ok(MoveOutcome {
            ok: resp.ok,
            error,
            to_node_id: resp.to_node_id,
            to_disk_id: resp.to_disk_id,
        })
    }
    async fn balance_state(&self) -> Result<BalanceStateDto> {
        let mut c = self.client.clone();
        let resp = c
            .get_balance_state(BalanceStateRequest { version: 1 })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        Ok(BalanceStateDto {
            present: resp.present,
            plan: resp.plan.map(plan_dto),
        })
    }

    async fn balance_eval(&self, max_moves: u32) -> Result<BalanceOpOutcome> {
        let mut c = self.client.clone();
        let resp = c
            .eval_balance(EvalBalanceRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                max_moves,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let error = op_error(resp.ok, &resp.error, &resp.leader_addr);
        Ok(BalanceOpOutcome {
            ok: resp.ok,
            error,
            plan: resp.plan.map(plan_dto),
        })
    }

    async fn balance_start(&self, plan_id: u64) -> Result<BalanceOpOutcome> {
        let mut c = self.client.clone();
        let resp = c
            .start_balance(StartBalanceRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                plan_id,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let error = op_error(resp.ok, &resp.error, &resp.leader_addr);
        Ok(BalanceOpOutcome {
            ok: resp.ok,
            error,
            plan: None,
        })
    }

    async fn balance_cancel(&self, plan_id: u64) -> Result<BalanceOpOutcome> {
        let mut c = self.client.clone();
        let resp = c
            .cancel_balance(CancelBalanceRequest {
                version: 1,
                cluster_secret: self.cluster_secret.clone(),
                plan_id,
            })
            .await
            .map_err(|e| Error::MasterRpc(e.to_string()))?
            .into_inner();
        let error = op_error(resp.ok, &resp.error, &resp.leader_addr);
        Ok(BalanceOpOutcome {
            ok: resp.ok,
            error,
            plan: None,
        })
    }
}

/// 运维类响应的错误文本:ok 时为空;error 空但 !ok 通常是打到了 follower,回填 leader 地址。
fn op_error(ok: bool, error: &str, leader_addr: &str) -> String {
    if ok {
        String::new()
    } else if error.is_empty() {
        format!("not leader (leader={leader_addr})")
    } else {
        error.to_string()
    }
}

/// pb `BalancePlan` → 对外 DTO。
fn plan_dto(p: antfs_master::pb::BalancePlan) -> BalancePlanDto {
    BalancePlanDto {
        plan_id: p.plan_id,
        status: p.status,
        created_at_ms: p.created_at_ms,
        max_in_flight: p.max_in_flight,
        total: p.total,
        pending: p.pending,
        in_flight: p.in_flight,
        done: p.done,
        failed: p.failed,
        moves: p
            .moves
            .into_iter()
            .map(|m| BalanceMoveDto {
                partition_id: m.partition_id,
                from_node_id: m.from_node_id,
                from_disk_id: m.from_disk_id,
                to_node_id: m.to_node_id,
                to_disk_id: m.to_disk_id,
                status: m.status,
                error: m.error,
            })
            .collect(),
    }
}

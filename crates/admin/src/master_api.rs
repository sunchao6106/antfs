//! master 只读/运维的抽象与 gRPC 实现。
//!
//! `MasterApi` trait 让 axum handler 与具体 gRPC 客户端解耦(测试注入 mock,免起真 master + raft)。
//! DTO 为对外 JSON 契约(serde);由 master pb 消息转换而来。

use async_trait::async_trait;
use serde::Serialize;
use tonic::transport::Channel;

use master::pb::master_client::MasterClient;
use master::pb::{
    ClusterStateRequest, CreatePartitionsRequest, DecommissionRequest, ListNodesRequest,
    ListPartitionsRequest, SetPartitionStateRequest,
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
}

/// 一个分区(`GET /api/partitions`)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PartitionDto {
    pub partition_id: u32,
    pub state: String,
    pub replication_factor: u32,
    pub replicas: Vec<ReplicaDto>,
    /// 存活且 ACTIVE 的副本数(< RF 即欠副本)。
    pub healthy_replicas: u32,
    /// 欠副本时是否有空闲健康节点可补(false = 卡住,需加容量)。
    pub healable: bool,
    /// 欠副本原因码(healthy 时空):node_down/replica_error/decommissioning/bootstrapping。
    pub under_replicated_reason: String,
}

/// admin 对 master 的只读 + 运维能力(handler 依赖此 trait,便于测试注入)。
#[async_trait]
pub trait MasterApi: Send + Sync {
    async fn cluster(&self) -> Result<ClusterDto>;
    async fn nodes(&self) -> Result<Vec<NodeDto>>;
    /// 分页列分区,返回 `(本页, 总数)`;`limit=0` 表示不限。
    async fn partitions(&self, offset: u64, limit: u32) -> Result<(Vec<PartitionDto>, u64)>;
    async fn create_partitions(&self, count: u32, replication_factor: u32) -> Result<Vec<u32>>;
    async fn decommission(&self, node_id: u64, force: bool) -> Result<()>;
    async fn set_partition_state(&self, partition_id: u32, state: &str) -> Result<()>;
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
        Ok(GrpcMasterApi { client, cluster_secret })
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
        let (partitions, _) = self.partitions(0, 0).await?;
        let total_capacity_bytes: u64 =
            nodes.iter().flat_map(|n| n.disks.iter()).map(|d| d.capacity_bytes).sum();
        let free_bytes: u64 =
            nodes.iter().flat_map(|n| n.disks.iter()).map(|d| d.free_bytes).sum();
        let nodes_up = nodes.iter().filter(|n| n.up).count() as u64;
        let under_replicated_partitions =
            partitions.iter().filter(|p| !p.under_replicated_reason.is_empty()).count() as u64;
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

    async fn partitions(&self, offset: u64, limit: u32) -> Result<(Vec<PartitionDto>, u64)> {
        let mut c = self.client.clone();
        let resp = c
            .list_partitions(ListPartitionsRequest { version: 1, offset, limit })
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
                        }
                    })
                    .collect();
                PartitionDto {
                    partition_id: p.partition_id,
                    state: p.state,
                    replication_factor: p.replication_factor,
                    replicas,
                    healthy_replicas: p.healthy_replicas,
                    healable: p.healable,
                    under_replicated_reason: p.under_replicated_reason,
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
                format!("{} (受影响分区: {:?})", resp.error, resp.affected_partitions)
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
}

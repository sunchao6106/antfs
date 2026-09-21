//! Raft 内部传输的 gRPC 客户端(D2):[`RaftNetworkFactory`] + [`RaftNetwork`]。
//!
//! openraft 的请求/响应经 JSON 序列化放进 [`pb::RaftMessage`] 的 `payload`,避免在 proto 里镜像
//! openraft 内部类型(其跨版本演进快)。未启用 `generic-snapshot-data`,故实现
//! `append_entries` / `vote` / `install_snapshot` 三个方法;`full_snapshot` 用默认分片实现走
//! `install_snapshot`。

use openraft::error::{NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RaftNetwork, RaftNetworkFactory, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::error::{InstallSnapshotError, RaftError};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::transport::Channel;

use crate::pb::raft_internal_client::RaftInternalClient;
use crate::pb::RaftMessage;
use crate::types::{NodeId, TypeConfig};

const PROTO_VERSION: u32 = 1;

/// 为每个目标节点创建 [`GrpcNetwork`] 客户端(懒连接,不在此建连)。
#[derive(Clone, Default)]
pub struct GrpcNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        GrpcNetwork { target, addr: node.addr.clone(), client: None }
    }
}

/// 指向单个目标 master 的 Raft 内部 RPC 客户端。
pub struct GrpcNetwork {
    target: NodeId,
    addr: String,
    client: Option<RaftInternalClient<Channel>>,
}

impl GrpcNetwork {
    async fn connect(&mut self) -> Result<RaftInternalClient<Channel>, Unreachable> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let dst = format!("http://{}", self.addr);
        let c = RaftInternalClient::connect(dst)
            .await
            .map_err(|e| Unreachable::new(&e))?;
        self.client = Some(c.clone());
        Ok(c)
    }
}

fn enc<T: Serialize>(v: &T) -> Result<Vec<u8>, NetworkError> {
    serde_json::to_vec(v).map_err(|e| NetworkError::new(&e))
}

fn dec<T: DeserializeOwned>(b: &[u8]) -> Result<T, NetworkError> {
    serde_json::from_slice(b).map_err(|e| NetworkError::new(&e))
}

fn msg(payload: Vec<u8>) -> RaftMessage {
    RaftMessage { version: PROTO_VERSION, payload }
}

impl RaftNetwork<TypeConfig> for GrpcNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let mut client = self.connect().await?;
        let payload = enc(&rpc)?;
        let resp = client
            .append_entries(msg(payload))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let inner: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            dec(&resp.into_inner().payload)?;
        inner.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let mut client = self.connect().await?;
        let payload = enc(&rpc)?;
        let resp = client
            .vote(msg(payload))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let inner: Result<VoteResponse<NodeId>, RaftError<NodeId>> =
            dec(&resp.into_inner().payload)?;
        inner.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let mut client = self.connect().await?;
        let payload = enc(&rpc)?;
        let resp = client
            .install_snapshot(msg(payload))
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let inner: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> =
            dec(&resp.into_inner().payload)?;
        inner.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

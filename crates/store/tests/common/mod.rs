//! store 集成测试共用的**假 master**(进程内 gRPC):只真正实现 store-server 用到的三个 RPC
//! (`RegisterNode` / `Heartbeat` / `LookupPartition`),其余走 `unimplemented`。
//!
//! 为什么需要它:真 master 只下发 `ACTIVE` 目标态(M2/M3 尚未产生 `DROPPED`,也不在本 crate 的
//! 改动范围内),而心跳指令/DROPPED/副本发现的**通路**必须在 store 侧被验证 —— 这里把 master
//! 那一端换成可控的假体,store 侧仍走真 gRPC + 真心跳 loop + 真拉复制 loop。
//!
//! 本文件被多个 integration test crate 各自 `mod common;` 引入,故允许未被某个用例用到的辅助项。

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use antfs_master::pb::master_server::{Master, MasterServer};
use antfs_master::pb::{
    AssignRequest, AssignResponse, BalanceStateRequest, BalanceStateResponse, CancelBalanceRequest,
    CancelBalanceResponse, ClusterState, ClusterStateRequest, CreatePartitionsRequest,
    CreatePartitionsResponse, DecommissionRequest, DecommissionResponse, EvalBalanceRequest,
    EvalBalanceResponse, GetBlobIdsRequest, GetBlobIdsResponse, ListNodesRequest,
    ListPartitionsRequest, ListPartitionsResponse, LookupRequest, MasterDirective, MetricsRequest,
    MetricsResponse, MoveReplicaRequest, MoveReplicaResponse, NodeBeat, NodeList,
    PartitionLocations, RegisterRequest, RegisterResponse, RepairPartitionRequest,
    RepairPartitionResponse, ReplicaLoc, SetMaintenanceRequest, SetMaintenanceResponse,
    SetPartitionStateRequest, SetPartitionStateResponse, StartBalanceRequest, StartBalanceResponse,
    Transition,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// 未被 store 用到的 RPC 的统一回复(测试里永远不会被调到)。
///
/// 注:这些方法必须**逐个写成字面 `async fn`** —— `#[tonic::async_trait]` 只能重写它自己看到的
/// `async fn` 项,宏展开出来的方法签名与 trait 对不上(E0195)。
const UNUSED: &str = "not implemented by the store test double";

/// 进程内假 master。
///
/// 两种用法(都以 `let fake = FakeMaster::start(peers).await;` 起服务):
/// - 注入心跳指令:先 [`push_directive`](Self::push_directive),节点心跳流建立且收到**首条 beat**
///   后下发(对齐真 master「先看到实然态上报,再算转移」);
/// - 提供副本发现(拉复制):`peers` 即 `LookupPartition` 固定返回的副本定位。
#[derive(Clone)]
pub struct FakeMaster {
    /// 已排队的指令(心跳流建立时一次性下发,队列随之清空)。
    directives: Arc<Mutex<Vec<MasterDirective>>>,
    /// `LookupPartition` 固定返回的副本定位。
    peers: Arc<Vec<ReplicaLoc>>,
    /// 收到的心跳 beat 数(用例用它同步「节点心跳仍在跑」,免 sleep 拍脑袋)。
    beats: Arc<AtomicU64>,
    addr: SocketAddr,
}

impl FakeMaster {
    /// 起 gRPC 服务并等到端口可连(避免 store 侧 `connect` 抢跑失败)。
    pub async fn start(peers: Vec<ReplicaLoc>) -> FakeMaster {
        let addr = free_addr();
        let fake = FakeMaster {
            directives: Arc::new(Mutex::new(Vec::new())),
            peers: Arc::new(peers),
            beats: Arc::new(AtomicU64::new(0)),
            addr,
        };
        let svc = MasterServer::new(fake.clone());
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(svc)
                .serve(addr)
                .await;
        });
        wait_until_connectable(addr).await;
        fake
    }

    /// gRPC 地址(store 的 `master_addr` 用它,格式 `127.0.0.1:port`)。
    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    /// 排队一条指令:在**下一条**心跳流建立并收到首条 beat 后原样下发。
    pub fn push_directive(&self, directive: MasterDirective) {
        self.directives
            .lock()
            .expect("fake master directives lock")
            .push(directive);
    }

    /// 排队一条「把某分区推到 `target_state`」的转移指令(单条 transition)。
    pub fn push_transition(&self, pid: u32, target_state: &str) {
        self.push_directive(MasterDirective {
            version: 1,
            transitions: vec![Transition {
                partition_id: pid,
                disk_id: String::new(),
                target_state: target_state.to_string(),
            }],
            tasks: Vec::new(),
        });
    }

    /// 已收到的心跳 beat 数(单调递增)。
    pub fn beat_count(&self) -> u64 {
        self.beats.load(Ordering::Relaxed)
    }
}

#[tonic::async_trait]
impl Master for FakeMaster {
    async fn register_node(
        &self,
        _request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        Ok(Response::new(RegisterResponse {
            version: 1,
            ok: true,
            leader_addr: String::new(),
            error: String::new(),
        }))
    }

    type HeartbeatStream = ReceiverStream<Result<MasterDirective, Status>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<NodeBeat>>,
    ) -> Result<Response<Self::HeartbeatStream>, Status> {
        let queued: Vec<MasterDirective> =
            std::mem::take(&mut *self.directives.lock().expect("fake master directives lock"));
        let beats = self.beats.clone();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            // 首条 beat = 节点已注册并上报实然态(master 据此算转移)。收不到就什么都不发。
            if !matches!(inbound.message().await, Ok(Some(_))) {
                return;
            }
            beats.fetch_add(1, Ordering::Relaxed);
            for d in queued {
                if tx.send(Ok(d)).await.is_err() {
                    return;
                }
            }
            while let Ok(Some(_)) = inbound.message().await {
                beats.fetch_add(1, Ordering::Relaxed);
            }
            // 节点断开后仍挂住 sender:流不结束,行为与常驻 master 一致。
            std::future::pending::<()>().await
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lookup_partition(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<PartitionLocations>, Status> {
        Ok(Response::new(PartitionLocations {
            version: 1,
            partition_id: request.into_inner().partition_id,
            state: "RW".to_string(),
            replicas: (*self.peers).clone(),
        }))
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<NodeList>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn get_cluster_state(
        &self,
        _request: Request<ClusterStateRequest>,
    ) -> Result<Response<ClusterState>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn create_partitions(
        &self,
        _request: Request<CreatePartitionsRequest>,
    ) -> Result<Response<CreatePartitionsResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn assign_partition(
        &self,
        _request: Request<AssignRequest>,
    ) -> Result<Response<AssignResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn get_blob_ids(
        &self,
        _request: Request<GetBlobIdsRequest>,
    ) -> Result<Response<GetBlobIdsResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn decommission_node(
        &self,
        _request: Request<DecommissionRequest>,
    ) -> Result<Response<DecommissionResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn list_partitions(
        &self,
        _request: Request<ListPartitionsRequest>,
    ) -> Result<Response<ListPartitionsResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn set_partition_state(
        &self,
        _request: Request<SetPartitionStateRequest>,
    ) -> Result<Response<SetPartitionStateResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn set_maintenance(
        &self,
        _request: Request<SetMaintenanceRequest>,
    ) -> Result<Response<SetMaintenanceResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn get_metrics(
        &self,
        _request: Request<MetricsRequest>,
    ) -> Result<Response<MetricsResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn repair_partition(
        &self,
        _request: Request<RepairPartitionRequest>,
    ) -> Result<Response<RepairPartitionResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn move_replica(
        &self,
        _request: Request<MoveReplicaRequest>,
    ) -> Result<Response<MoveReplicaResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn eval_balance(
        &self,
        _request: Request<EvalBalanceRequest>,
    ) -> Result<Response<EvalBalanceResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn start_balance(
        &self,
        _request: Request<StartBalanceRequest>,
    ) -> Result<Response<StartBalanceResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn cancel_balance(
        &self,
        _request: Request<CancelBalanceRequest>,
    ) -> Result<Response<CancelBalanceResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
    async fn get_balance_state(
        &self,
        _request: Request<BalanceStateRequest>,
    ) -> Result<Response<BalanceStateResponse>, Status> {
        Err(Status::unimplemented(UNUSED))
    }
}
/// 一条指向 `addr` 的存活副本定位(供 `LookupPartition` 返回)。
pub fn peer_loc(node_id: u64, addr: &str) -> ReplicaLoc {
    ReplicaLoc {
        node_id,
        addr: addr.to_string(),
        disk_id: String::new(),
        target_state: "ACTIVE".to_string(),
        up: true,
    }
}

/// 当前可用端口(绑 0 取端口后即释放;与既有测试同一手法)。
fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    l.local_addr().expect("local_addr")
}

/// 等到 `addr` 可连通(有界;gRPC 服务起来前的空窗)。
async fn wait_until_connectable(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "fake master 未在超时内监听 {addr}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// 带超时的条件轮询(CLAUDE.md §6:确定性等待,不用 sleep 当同步)。
pub async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

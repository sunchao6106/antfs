//! 接入 master:注册 + 双向流心跳 + 消费 `MasterDirective` 的 ReplicaLifecycle(S2)。
//!
//! 复用 `master` crate 的 gRPC pb(控制面契约单一来源)。同步 store 调用(`ensure_replica`)经
//! `spawn_blocking` offload。

use std::sync::Arc;
use std::time::Duration;

use master::pb::master_client::MasterClient;
use master::pb::{DiskBeat, NodeBeat, RegisterRequest, ReplicaBeat};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::StoreServerConfig;
use crate::disk::DiskManager;
use crate::health::HealthMap;
use crate::metrics::StoreServerMetrics;
use crate::replica::ReplicaManager;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

fn disk_beats(disks: &DiskManager, health: &HealthMap) -> Vec<DiskBeat> {
    let hm = health.read().expect("health map poisoned");
    disks
        .disks()
        .iter()
        .map(|d| DiskBeat {
            disk_id: d.id.clone(),
            capacity_bytes: d.capacity_bytes,
            free_bytes: d.free_bytes,
            health: hm.get(&d.id).map(|h| h.as_str().to_string()).unwrap_or_default(),
        })
        .collect()
}

/// 启动后台任务:注册 + 心跳(仅当配置了 `master_addr`)。
pub fn spawn_cluster_client(
    config: StoreServerConfig,
    disks: DiskManager,
    replicas: Arc<ReplicaManager>,
    health: HealthMap,
    metrics: Arc<StoreServerMetrics>,
) {
    tokio::spawn(async move {
        if let Err(e) = run(config, disks, replicas, health, metrics).await {
            tracing::warn!(error = %e, "store-server cluster client stopped");
        }
    });
}

async fn run(
    config: StoreServerConfig,
    disks: DiskManager,
    replicas: Arc<ReplicaManager>,
    health: HealthMap,
    metrics: Arc<StoreServerMetrics>,
) -> Result<(), BoxErr> {
    let mut client = MasterClient::connect(format!("http://{}", config.master_addr)).await?;

    // 注册(node_id + 各盘 statfs 容量)。
    client
        .register_node(RegisterRequest {
            version: 1,
            cluster_secret: config.cluster_secret.clone(),
            node_id: config.node_id,
            dc: "dc1".to_string(),
            rack: "r1".to_string(),
            addr: config.http_addr.clone(),
            disks: disk_beats(&disks, &health),
        })
        .await?;

    // 双向流心跳:sender 周期推 NodeBeat;receiver 应用 transitions。
    let (tx, rx) = mpsc::channel::<NodeBeat>(16);
    let mut inbound = client.heartbeat(ReceiverStream::new(rx)).await?.into_inner();

    {
        let (config, disks, replicas, health) =
            (config.clone(), disks.clone(), replicas.clone(), health.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(config.heartbeat_interval_secs));
            loop {
                tick.tick().await;
                let mut dm = disks.clone();
                dm.refresh();
                let beat = NodeBeat {
                    version: 1,
                    cluster_secret: config.cluster_secret.clone(),
                    node_id: config.node_id,
                    disks: disk_beats(&dm, &health),
                    full_report: true,
                    replicas: replicas
                        .replica_states()
                        .into_iter()
                        .map(|(pid, disk_id, current_state)| ReplicaBeat {
                            partition_id: pid,
                            disk_id,
                            current_state,
                            lag_bytes: 0,
                        })
                        .collect(),
                };
                if tx.send(beat).await.is_err() {
                    break;
                }
            }
        });
    }

    while let Some(directive) = inbound.message().await? {
        metrics.heartbeats.inc();
        for t in directive.transitions {
            match t.target_state.as_str() {
                "ACTIVE" | "BOOTSTRAP" => {
                    let r = replicas.clone();
                    let pid = t.partition_id;
                    // ensure_replica 同步阻塞(建 BlobStore),offload 到阻塞池。
                    let _ = tokio::task::spawn_blocking(move || r.ensure_replica(pid)).await;
                }
                "OFFLINE" | "DROPPED" => replicas.remove_replica(t.partition_id),
                _ => {}
            }
        }
    }
    Ok(())
}

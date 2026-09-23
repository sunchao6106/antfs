//! 接入 master:注册 + 双向流心跳 + 消费 `MasterDirective` 的 ReplicaLifecycle(S2)。
//!
//! 复用 `master` crate 的 gRPC pb(控制面契约单一来源)。同步 store 调用(`ensure_replica`)经
//! `spawn_blocking` offload。

use std::sync::Arc;
use std::time::Duration;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{DiskBeat, NodeBeat, RegisterRequest, ReplicaBeat};
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
        let (config, disks, replicas, health, metrics) =
            (config.clone(), disks.clone(), replicas.clone(), health.clone(), metrics.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(config.heartbeat_interval_secs));
            loop {
                tick.tick().await;
                let mut dm = disks.clone();
                dm.refresh();
                let states = replicas.replica_states();
                // 每拍顺带刷新副本相关 gauge(实然态会随拉复制转正而变,见 `replication::sync_round`)。
                metrics.set_replica_gauges(&states);
                let beat = NodeBeat {
                    version: 1,
                    cluster_secret: config.cluster_secret.clone(),
                    node_id: config.node_id,
                    disks: disk_beats(&dm, &health),
                    full_report: true,
                    replicas: states
                        .into_iter()
                        .map(|s| ReplicaBeat {
                            partition_id: s.partition_id,
                            disk_id: s.disk_id,
                            current_state: s.current_state,
                            // 滞后字节为**估算**(口径与误差见 `replication::lag_bytes`):
                            // 每轮复制开始时 peer 游标与本地已 apply 游标之差,按段容量线性化。
                            lag_bytes: s.lag_bytes,
                            // 实际占用 = blob 活数据(墓碑不计),供 master 汇总可写容量与均衡判据。
                            used_bytes: s.used_bytes,
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
                // OFFLINE = 临时摘除(只摘内存句柄,盘上数据保留 —— 副本可能再回来),
                // **不删数据**:删盘只在 DROPPED 时做(见下)。
                "OFFLINE" => replicas.remove_replica(t.partition_id),
                // DROPPED = 该副本已从集群 placement 正式移除且继任者已 ACTIVE(master 确认后才下发),
                // 可以删盘上数据。`remove_dir_all` 是同步阻塞 IO,offload 到阻塞池,别卡心跳 loop。
                "DROPPED" => {
                    let r = replicas.clone();
                    let pid = t.partition_id;
                    match tokio::task::spawn_blocking(move || r.drop_replica(pid)).await {
                        // 真的删了才计数;`false` = 本地本来就没有(重复指令/重放)→ 幂等忽略。
                        Ok(Ok(true)) => metrics.replicas_dropped.inc(),
                        Ok(Ok(false)) => {}
                        Ok(Err(e)) => {
                            tracing::warn!(partition = pid, error = %e, "drop local replica failed")
                        }
                        Err(_) => tracing::warn!(partition = pid, "drop local replica join error"),
                    }
                }
                _ => {}
            }
        }
        // 副本数与实然态可能经本路径(心跳指令)而非 `POST /admin/partitions/{pid}` 变化,每轮刷新一次,
        // 否则 `store_server_replicas` 会恒为 0(既有缺口)。实然态照旧上报(只是值更真实:
        // 新建/重开/重装出来的副本先报 `BOOTSTRAP`,拉复制追平后由复制器转 `ACTIVE`)。
        metrics.set_replica_gauges(&replicas.replica_states());
    }
    Ok(())
}

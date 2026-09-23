//! 接入 master:注册 + 双向流心跳 + 消费 `MasterDirective` 的 ReplicaLifecycle(S2)。
//!
//! 还负责"从零建副本时向 master 申领副本身份(incarnation)"的 RPC(见 [`claim_incarnation`])——
//! 与心跳共用同一个 `MasterClient`(连接复用),拉复制侧(`install_bootstrap` 之后)也复用它。
//!
//! 复用 `master` crate 的 gRPC pb(控制面契约单一来源)。同步 store 调用(`ensure_replica`)经
//! `spawn_blocking` offload。

use std::sync::Arc;
use std::time::Duration;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    AssignIncarnationRequest, DiskBeat, NodeBeat, RegisterRequest, ReplicaBeat,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::StoreServerConfig;
use crate::disk::DiskManager;
use crate::health::HealthMap;
use crate::metrics::StoreServerMetrics;
use crate::replica::{ReplicaHandle, ReplicaManager};
use crate::PartitionId;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// 「从零建副本」时向 master 申领这份副本的**全新身份**(incarnation)并采纳到本地句柄。
///
/// 两条调用路径:心跳里 `ensure_replica_with_origin` 返回 `true`(**盘上原本没有**这份副本,本次新建)
/// 与 `install_bootstrap`(物理重装 —— 目录被整体重建)。**重开已有副本(OFFLINE 挂回 / 进程重启)
/// 不走这里**:数据与身份文件都在,换号只会白白作废对端的游标。
/// 为什么必须申领:重建/换盘后的副本**不得沿用旧身份** —— 其它节点手里那条旧复制游标会看似合法,
/// 于是带着缺数据被误判"已追平"(`ACTIVE` 是 master 删源的闸门),那就是丢数据(CLAUDE.md §3.1)。
/// master 侧语义是"每次调用发一个全新的更大的号"(已落进 placement,拉取方 `LookupPartition`
/// 看到的是同一个值)。
///
/// **失败绝不影响建副本**:网络不通 / 非 leader / 该 (partition, node) 不在 placement / 老 master
/// 未实现 —— 一律只 `warn` 并**保留本地兜底值**(建库时从首段诞生时刻推出的单调值,已落盘在
/// `LOG_INCARNATION`),副本照常建、照常服务。单机模式没有 master,这条兜底就是最终值。
/// 非 leader 时 master 会回 `leader_addr`,这里**不代转**(store 侧的控制面调用一贯只记不追)——
/// 下次从零建这份副本时再申领。
///
/// 用**已有的** `MasterClient`(连接复用),不为罕路径另建连接。
pub async fn claim_incarnation(
    client: &mut MasterClient<tonic::transport::Channel>,
    config: &StoreServerConfig,
    pid: PartitionId,
    handle: &ReplicaHandle,
) {
    let request = AssignIncarnationRequest {
        version: 1,
        cluster_secret: config.cluster_secret.clone(),
        partition_id: pid,
        node_id: config.node_id,
    };
    match client.assign_replica_incarnation(request).await {
        Ok(resp) => {
            let resp = resp.into_inner();
            if !resp.ok {
                tracing::warn!(
                    partition = pid,
                    error = %resp.error,
                    leader = %resp.leader_addr,
                    "master refused to assign a replica incarnation; keeping the local one"
                );
                return;
            }
            if let Err(e) = handle.store.adopt_incarnation(resp.incarnation) {
                tracing::warn!(
                    partition = pid,
                    incarnation = resp.incarnation,
                    error = %e,
                    "adopting the assigned replica incarnation failed"
                );
            }
        }
        Err(e) => tracing::warn!(
            partition = pid,
            error = %e,
            "assigning a replica incarnation failed; keeping the local one"
        ),
    }
}

fn disk_beats(disks: &DiskManager, health: &HealthMap) -> Vec<DiskBeat> {
    let hm = health.read().expect("health map poisoned");
    disks
        .disks()
        .iter()
        .map(|d| DiskBeat {
            disk_id: d.id.clone(),
            capacity_bytes: d.capacity_bytes,
            free_bytes: d.free_bytes,
            health: hm
                .get(&d.id)
                .map(|h| h.as_str().to_string())
                .unwrap_or_default(),
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
    let mut inbound = client
        .heartbeat(ReceiverStream::new(rx))
        .await?
        .into_inner();

    {
        let (config, disks, replicas, health, metrics) = (
            config.clone(),
            disks.clone(),
            replicas.clone(),
            health.clone(),
            metrics.clone(),
        );
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(Duration::from_secs(config.heartbeat_interval_secs));
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
        // 分区可写态(master 权威,每拍全量):本地维护(定时 compaction)按它挑候选集。
        // 先吸收再处理转移 —— 转移会让副本增删,状态记在"当时存在的副本"上更有意义。
        for st in &directive.partition_states {
            replicas.set_partition_state(st.partition_id, &st.state);
            // 墓碑回收闸门(同一份权威状态里的另一个字段):记到句柄上供 compaction 判活
            // (见 `crate::worker::tombstones_allowed`)。本地没该副本时忽略 —— 下一拍还会重发。
            replicas.set_tombstones_reclaim_allowed(st.partition_id, st.tombstones_reclaim_allowed);
        }
        for t in directive.transitions {
            match t.target_state.as_str() {
                "ACTIVE" | "BOOTSTRAP" => {
                    // **只有从零建副本才申领身份** —— 判据是"盘上原本有没有这份副本"(不是"内存里有没有
                    // 句柄"):OFFLINE 摘除后挂回、进程重启重开、以及**幂等心跳重复下发**都不算从零建,
                    // 身份必须保持;否则对端手里绑着这份副本的游标会被反复作废(甚至判不出追平)。
                    let r = replicas.clone();
                    let pid = t.partition_id;
                    // ensure_replica 同步阻塞(建 BlobStore),offload 到阻塞池。
                    match tokio::task::spawn_blocking(move || r.ensure_replica_with_origin(pid))
                        .await
                    {
                        Ok(Ok((h, true))) => {
                            claim_incarnation(&mut client, &config, pid, &h).await;
                        }
                        Ok(Ok((_, false))) => {}
                        Ok(Err(e)) => {
                            tracing::warn!(partition = pid, error = %e, "ensure local replica failed")
                        }
                        Err(_) => {
                            tracing::warn!(partition = pid, "ensure local replica join error")
                        }
                    }
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

//! 接入 master:注册 + 双向流心跳 + 消费 `MasterDirective` 的 ReplicaLifecycle(S2)。
//!
//! 还负责"从零建副本时向 master 申领副本身份(incarnation)"的 RPC(见 [`claim_incarnation`])——
//! 与心跳共用同一个 `MasterClient`(连接复用),拉复制侧(`install_bootstrap` 之后)也复用它。
//!
//! 复用 `master` crate 的 gRPC pb(控制面契约单一来源)。同步 store 调用(`ensure_replica`)经
//! `spawn_blocking` offload。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    AssignIncarnationRequest, DiskBeat, NodeBeat, RegisterRequest, ReplicaBeat,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::config::StoreServerConfig;
use crate::disk::{Disk, DiskManager};
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

/// 每盘的 `DiskBeat`(容量 + 健康值)。
///
/// 健康值来自 [`crate::disk_health::reported_state`],值域是
/// `HEALTHY` / 具体故障枚举 / `UNAVAILABLE`(迟滞隔离态)/ **`UNKNOWN`(从未观测过)**。
///
/// **"没探过"绝不再报空串**:空串在 master 侧被当成"可用",于是"从没检查过"与"检查过且健康"
/// 混为一谈(D2 的硬规则 1)。改成显式 `UNKNOWN` 之后,master 侧必须把 `UNKNOWN` 也算作可放新副本
/// —— 否则默认关探测的部署会一块盘都放不了(`crates/master/src/placement.rs` 有对应单测)。
fn disk_beats(disks: &DiskManager, health: &HealthMap) -> Vec<DiskBeat> {
    disks
        .disks()
        .iter()
        .map(|d| DiskBeat {
            disk_id: d.id.clone(),
            capacity_bytes: d.capacity_bytes,
            free_bytes: d.free_bytes,
            health: crate::disk_health::reported_state(health, &d.id).to_string(),
        })
        .collect()
}

/// 每盘至多一发在飞的容量刷新:`disk_id -> 上一发未归的 `statvfs` 任务`。
///
/// 为什么要这么绕:`DiskManager::refresh` 是同步 `statvfs`,直接放在异步心跳任务里会把 **reactor 线程**
/// 卡住 —— 盘挂死时心跳就停,而 master 判节点死的默认门槛只有 15s(`heartbeat_timeout_secs`),一停就是
/// **全节点再复制**。挪进阻塞池后又多一条约束:卡住的阻塞线程收不回来(`spawn_blocking` 不可取消),
/// 所以**每盘至多一发在飞**,否则每拍漏一个,很快把进程共享的阻塞池吃光(与 `health.rs` 的探测守卫
/// 同一套道理)。
type RefreshSlots = HashMap<String, JoinHandle<StatfsResult>>;

/// 一发 `statvfs` 刷新的结果:`(disk_id, 容量/余量)`;`None` = 该盘 `statvfs` 失败(保留旧值)。
type StatfsResult = (String, Option<(u64, u64)>);

/// 收集**已完成**的刷新并写回 `dm`;未完成的一发留在槽位里(本轮不重发,心跳照发)。
async fn collect_refresh(slots: &mut RefreshSlots, dm: &mut DiskManager) {
    let done: Vec<String> = slots
        .iter()
        .filter(|(_, h)| h.is_finished())
        .map(|(id, _)| id.clone())
        .collect();
    for id in done {
        let Some(h) = slots.remove(&id) else { continue };
        // 借 `&mut` 之外已无需句柄,直接 await;join 失败只可能是 panic。
        if let Ok((disk_id, Some((cap, free)))) = h.await {
            dm.apply_statfs(&disk_id, cap, free);
        }
    }
}

/// 给还没有在飞刷新的盘各发一发(`statvfs` 在阻塞池里跑)。
fn spawn_refresh(slots: &mut RefreshSlots, disks: &[Disk]) {
    for d in disks {
        if slots.contains_key(&d.id) {
            continue;
        }
        let id = d.id.clone();
        let dir = d.dir.clone();
        slots.insert(
            id.clone(),
            tokio::task::spawn_blocking(move || (id, crate::disk::statfs(&dir).ok())),
        );
    }
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
            let mut refresh_slots: RefreshSlots = HashMap::new();
            loop {
                tick.tick().await;
                let mut dm = disks.clone();
                // 盘容量刷新:①**挪出 reactor** —— `statvfs` 是同步调用,盘挂死时直调会卡住 reactor 线程,
                // 心跳随之停摆 → master 判节点死 → 全节点再复制;②**每盘至多一发在飞** —— 卡住的阻塞线程
                // 收不回来,重复发只会把共享阻塞池吃光。刷新未归的盘本轮沿用上一拍的容量值:容量只喂放置
                // 启发式与"盘将满转 RO"判定,陈旧一拍无害;而"心跳必须发出去"是硬要求。
                collect_refresh(&mut refresh_slots, &mut dm).await;
                spawn_refresh(&mut refresh_slots, dm.disks());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(dir: &std::path::Path) -> DiskManager {
        DiskManager::mount(&[dir.to_string_lossy().into_owned().into()]).expect("mount")
    }

    /// **上报值域**(D2 的核心规则):"没探过"绝不报空串/`HEALTHY`,而是 `UNKNOWN`;迟滞隔离后报
    /// `UNAVAILABLE`;单轮故障如实报具体值。
    ///
    /// 空串是**改造前的行为**:master 侧把它当"可用",于是"从没检查过"与"检查过且健康"混为一谈。
    #[tokio::test]
    async fn disk_beats_report_unknown_and_unavailable_never_a_blank_string() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dm = manager(tmp.path());
        let id = dm.disks()[0].id.clone();
        let health = crate::disk_health::new_map();

        // 从未观测:UNKNOWN(不是空串,也不是 HEALTHY)。
        let beats = disk_beats(&dm, &health);
        assert_eq!(beats.len(), 1);
        assert_eq!(beats[0].health, "UNKNOWN");
        assert_ne!(
            beats[0].health, "",
            "空串会被 master 当成'可用',必须显式 UNKNOWN"
        );
        assert_ne!(beats[0].health, "HEALTHY", "未知 ≠ 健康");

        // 单轮坏:如实报故障值(让 master 先避开新放置),但**不**谎报隔离。
        let h = crate::disk_health::Hysteresis {
            bad_rounds: 3,
            bad_secs: 30,
            good_rounds: 3,
            recover_stable_secs: 120,
        };
        let reg = antfs_metric::Registry::new();
        let metrics = StoreServerMetrics::new(&reg).expect("metrics");
        crate::disk_health::observe_disk(
            &health,
            &metrics,
            &id,
            crate::health::DiskHealth::WriteTimeout,
            &h,
        );
        assert_eq!(disk_beats(&dm, &health)[0].health, "WRITE_TIMEOUT");

        // 凑满迟滞 ⇒ 派生的隔离态压过具体故障值。
        for _ in 0..2 {
            crate::disk_health::observe_disk(
                &health,
                &metrics,
                &id,
                crate::health::DiskHealth::WriteTimeout,
                &h,
            );
        }
        assert_eq!(disk_beats(&dm, &health)[0].health, "UNAVAILABLE");
    }

    /// 有界轮询把在飞的刷新收干净(不用 `sleep`:让出调度即可 —— 临时目录上的 `statvfs` 是瞬时的)。
    async fn drain(slots: &mut RefreshSlots, dm: &mut DiskManager) -> bool {
        for _ in 0..10_000 {
            collect_refresh(slots, dm).await;
            if slots.is_empty() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    /// 每盘至多一发在飞:已有未归的一发时**不再叠加**(叠加就是往共享阻塞池里漏线程),并且
    /// 结果回来后经 `collect_refresh` 写回容量。
    #[tokio::test]
    async fn refresh_is_single_flight_per_disk_and_writes_back() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut dm = manager(tmp.path());
        let id = dm.disks()[0].id.clone();

        let mut slots: RefreshSlots = HashMap::new();
        spawn_refresh(&mut slots, dm.disks());
        assert_eq!(slots.len(), 1, "首拍给该盘发一发");
        assert!(slots.contains_key(&id));

        spawn_refresh(&mut slots, dm.disks());
        assert_eq!(slots.len(), 1, "已有在飞的一发 ⇒ 不再叠加");

        // 先用假值冒充"陈旧容量",再看这一发回来是否把它改正。
        dm.apply_statfs(&id, 1, 1);
        assert!(drain(&mut slots, &mut dm).await, "刷新应在一万次让出内完成");
        assert!(dm.disks()[0].capacity_bytes > 1, "容量应被刷新写回");
        assert_eq!(dm.disks()[0].id, id);
    }

    /// 已完成的槽位会被收掉、`statvfs` 在本机临时目录上必然成功;完成后应能再发下一发。
    #[tokio::test]
    async fn drained_refresh_allows_the_next_round() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut dm = manager(tmp.path());
        let mut slots: RefreshSlots = HashMap::new();

        spawn_refresh(&mut slots, dm.disks());
        assert!(drain(&mut slots, &mut dm).await);
        assert!(slots.is_empty(), "收干净后槽位应清空");

        spawn_refresh(&mut slots, dm.disks());
        assert_eq!(slots.len(), 1, "下一拍应再发一发");
        assert!(drain(&mut slots, &mut dm).await);
    }

    /// 盘 id 不在列表里 = 无操作(不 panic、不改动任何盘)。
    #[test]
    fn apply_statfs_ignores_unknown_disk() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut dm = manager(tmp.path());
        let before = (dm.disks()[0].capacity_bytes, dm.disks()[0].free_bytes);
        dm.apply_statfs("no-such-disk", 1, 2);
        assert_eq!(
            (dm.disks()[0].capacity_bytes, dm.disks()[0].free_bytes),
            before
        );
    }

    /// **本拍绝不等未归的刷新**(openspec 8.4 的核心不变式):盘真挂死时 `statvfs` 不会返回 —— 若这里
    /// `await` 了未完成的句柄,心跳循环就停在这一拍,master 见不到心跳 ⇒ 判节点死 ⇒ 全节点再复制。
    /// 用 `timeout` 兜底:一旦退化成"等",测试**红**而不是挂死。
    #[tokio::test]
    async fn an_unfinished_refresh_never_blocks_the_beat_round() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut dm = manager(tmp.path());
        let id = dm.disks()[0].id.clone();

        // 一发"永不返回"的刷新(模拟卡在大 D 状态的 `statvfs`);用 channel 放行以保证测试能收尾。
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let slow_id = id.clone();
        let mut slots: RefreshSlots = HashMap::new();
        slots.insert(
            id.clone(),
            tokio::task::spawn_blocking(move || {
                let _ = blocked.recv();
                (slow_id, None)
            }),
        );

        let capacity_before = dm.disks()[0].capacity_bytes;
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            collect_refresh(&mut slots, &mut dm),
        )
        .await
        .expect("未完成的刷新不许把这一拍堵住");

        assert_eq!(slots.len(), 1, "没回来的刷新要留在在飞表里,下拍接着等");
        assert_eq!(
            dm.disks()[0].capacity_bytes,
            capacity_before,
            "没回来就沿用上一拍的容量(陈旧一拍无害,心跳必须发出去)"
        );

        // 同盘再发一次:单飞守卫必须挡住(盘挂死时阻塞线程收不回来,重复发会吃光池)。
        spawn_refresh(&mut slots, dm.disks());
        assert_eq!(slots.len(), 1, "同盘至多一发在飞");

        // 放行后应能在有限次让出内收掉,且**不写回**(模拟的这次没有 statfs 结果)。
        release.send(()).expect("放行阻塞的刷新");
        assert!(drain(&mut slots, &mut dm).await, "放行后应能收干净");
        assert_eq!(
            dm.disks()[0].capacity_bytes,
            capacity_before,
            "没有结果就不改容量(不猜测、不清零)"
        );
    }
}

//! `ReplicaManager`:按 partition 管理本地 `BlobStore`(每个 partition-replica 一个)。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use antfs_blob::{BlobStore, StoreConfig, StoreMetrics};

use crate::disk::DiskManager;
use crate::error::{Error, Result};
use crate::PartitionId;

/// 删本地副本目录前等在飞持有者退出的**最长宽限**(见 [`ReplicaManager::drop_replica`])。
/// 取值依据:在飞数据面请求与 worker 的单次 flush 都是**毫秒级**,2s 有 ~百倍余量;
/// 再长的持有者(如整库 compaction)不在此列 —— 不为它无限等,以免拖住心跳 loop。
const DROP_DRAIN_MAX_WAIT: Duration = Duration::from_secs(2);
/// 上面那把等待的轮询间隔(只是等 `Arc` 计数归 1,不碰磁盘)。
const DROP_DRAIN_POLL: Duration = Duration::from_millis(10);

/// 每副本的拉复制滞后估算(字节)。复制侧每轮写入、心跳侧只读采样;
/// **纯诊断量,不参与复制/一致性决策**(故 `Relaxed` 原子足够、无需锁)。
#[derive(Debug, Default)]
pub struct LagEstimate(AtomicU64);

impl LagEstimate {
    pub fn new() -> Self {
        LagEstimate(AtomicU64::new(0))
    }

    /// 当前估算值(字节);0 = 已追平或尚无基线。
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn set(&self, bytes: u64) {
        self.0.store(bytes, Ordering::Relaxed);
    }
}

/// 副本实然态:**已追平**,可作数据源/可依赖(心跳上报给 master)。
pub const STATE_ACTIVE: &str = "ACTIVE";
/// 副本实然态:**尚未追平**(新建 / 重开 / 物理 bootstrap 之后),还在等拉复制补数据。
/// 不是错误、也不是"可依赖的另一份数据"—— master 侧"先补后删"的删源闸门就靠它(CLAUDE.md §3.1):
/// 搬迁/evacuation 的"后删"与 `DROPPED` 下发都要求继任者已 `ACTIVE`(master
/// `reconcile::plan_move` / `drop_confirmed`)。
///
/// **不会因它触发再复制风暴**:master 算"要不要补副本"用的是 `up && !errored`
/// (`reconcile::plan_rereplication`),不看 `BOOTSTRAP`;`BOOTSTRAP` 只进"欠副本主因"的展示
/// (`classify_partition` → `bootstrapping`,前端显示"同步中")。故重启后短时报 `BOOTSTRAP` 是安全的。
pub const STATE_BOOTSTRAP: &str = "BOOTSTRAP";
/// 副本实然态:**IO 故障**(引擎自关 hook 被动置位,S2)。是 master 再复制判据的输入,
/// 故**只能由故障检测置位**、不被追平覆盖(见 [`ReplicaHandle::mark_active`])。
pub const STATE_ERROR: &str = "ERROR";

/// 一个本地 partition-replica 句柄(`BlobStore` 用 `Arc` 共享给并发 put/get,方法均 `&self`)。
#[derive(Clone)]
pub struct ReplicaHandle {
    pub disk_id: String,
    pub store: Arc<BlobStore>,
    /// 该 replica 的实然态(心跳上报):初值 [`STATE_BOOTSTRAP`],追平后经
    /// [`mark_active`](Self::mark_active) 置 [`STATE_ACTIVE`];IO 故障 hook 置 [`STATE_ERROR`]。
    pub current_state: Arc<RwLock<String>>,
    /// 该 replica 的拉复制滞后估算(心跳上报;口径与误差见 [`crate::replication::lag_bytes`])。
    pub lag: Arc<LagEstimate>,
    /// **分区可写态**(master 权威:`RW` / `RO` / `SEALED`),随心跳指令下发;`""` = 未知
    /// (未接 master 的单机模式,或指令还没到)。
    ///
    /// 与 `current_state`(副本实然态:追平了吗/坏了吗)是两码事,别混用:本地维护(定时 compaction)
    /// 按它挑候选集,见 [`crate::worker`]。
    pub partition_state: Arc<RwLock<String>>,
    /// **墓碑回收闸门**(master 权威,随心跳指令每拍全量下发):`true` = 该分区**所有副本都已追平**,
    /// compaction 才允许回收墓碑;`false` = 墓碑一律照搬。
    ///
    /// 为什么需要它(CLAUDE.md §3.1):墓碑一收,还有没追平的副本就永远学不到那条删除,已删对象会
    /// 以"活数据"永久留在它上面(僵尸数据)。初值 `false` —— **没得到 master 放行前不回收**;
    /// 单机模式(未接 master)没有别的副本可滞后,由消费侧按放行处理(见 [`crate::worker`])。
    pub tombstones_reclaim_allowed: Arc<RwLock<bool>>,
}

/// 分区状态未知(未接 master / 指令未到):本地维护按"可压"处理,保持单机模式的既有行为。
pub const STATE_UNKNOWN: &str = "";

impl ReplicaHandle {
    /// 追平后置 [`STATE_ACTIVE`];返回是否**真的发生了**状态变化(幂等:已是 `ACTIVE` 返回 `false`)。
    ///
    /// 三条不变量:
    /// - **只由拉复制路径调用**([`crate::replication::sync_round`] 判定追平后),创建路径只给初值;
    /// - **不覆盖 [`STATE_ERROR`]**:IO 故障是被动检测出来的事实(master 用它算再复制),一轮拉复制
    ///   成功不得把它抹掉;
    /// - **单向**:追平是持久事实,不因后续某一轮暂时拉不到 peer 而回退(避免状态抖动/误报)。
    pub fn mark_active(&self) -> bool {
        let mut state = self
            .current_state
            .write()
            .expect("current_state lock poisoned");
        if *state == STATE_ACTIVE || *state == STATE_ERROR {
            return false;
        }
        *state = STATE_ACTIVE.to_string();
        true
    }

    /// 当前分区可写态(`RW` / `RO` / `SEALED`;`""` = 未知)。本地维护据此挑 compaction 候选集。
    pub fn partition_state_now(&self) -> String {
        self.partition_state
            .read()
            .expect("partition_state lock poisoned")
            .clone()
    }

    /// 当前墓碑回收闸门(master 权威;消费口径见 [`crate::worker::tombstones_allowed`])。
    pub fn tombstones_reclaim_allowed(&self) -> bool {
        *self
            .tombstones_reclaim_allowed
            .read()
            .expect("tombstones_reclaim_allowed lock poisoned")
    }
}

/// 一个本地副本的心跳实况(心跳 beat 的填充源)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaState {
    pub partition_id: PartitionId,
    pub disk_id: String,
    pub current_state: String,
    /// 该分区在本副本的**实际占用**:`BlobStore::valid_data_size()`(未删除赢家的记录字节,
    /// 墓碑不计;含记录头/crc,故略大于对象负载)。
    pub used_bytes: u64,
    /// 拉复制滞后估算(字节);0 = 已追平或尚无基线(见 [`crate::replication::lag_bytes`])。
    pub lag_bytes: u64,
}

/// 本节点所有 partition-replica 的管理器。
pub struct ReplicaManager {
    disks: DiskManager,
    store_config: StoreConfig,
    store_metrics: StoreMetrics,
    partition_capacity: u64,
    replicas: RwLock<HashMap<PartitionId, ReplicaHandle>>,
}

impl ReplicaManager {
    pub fn new(
        disks: DiskManager,
        store_config: StoreConfig,
        store_metrics: StoreMetrics,
        partition_capacity: u64,
    ) -> Self {
        ReplicaManager {
            disks,
            store_config,
            store_metrics,
            partition_capacity,
            replicas: RwLock::new(HashMap::new()),
        }
    }

    /// 幂等地确保某分区在本地有 replica:已存在则返回,否则选盘 create/open 一个 `BlobStore`。
    ///
    /// 建/开出来的副本实然态一律从 [`STATE_BOOTSTRAP`] 起(见 [`Self::make_handle`]):
    /// **重开**(`dir.exists()` → `open_with`)也走保守路径 —— 盘上数据可能是旧的,先报 `BOOTSTRAP`,
    /// 等一轮拉复制确认追平再转 `ACTIVE`(重启期间短时报"同步中"是诚实状态,不是故障)。
    pub fn ensure_replica(&self, pid: PartitionId) -> Result<ReplicaHandle> {
        self.ensure_replica_with_origin(pid).map(|(h, _)| h)
    }

    /// 同 [`ensure_replica`](Self::ensure_replica),但额外告知"这次是不是**从零**建出来的":
    /// `true` = 盘上原本没有这份副本(本次新建了副本目录)。
    ///
    /// 控制面用它决定要不要向 master 申领副本身份(见 `heartbeat::claim_incarnation`):
    /// **只有从零建副本才换身份**。OFFLINE 摘除后挂回、进程重启重开、幂等心跳重复下发 —— 盘上数据
    /// 都还在(身份文件也在),身份必须**保持**;否则对端手里绑着这份副本的游标会被无故作废、
    /// 每挂回一次就全量重拉一轮。
    pub fn ensure_replica_with_origin(&self, pid: PartitionId) -> Result<(ReplicaHandle, bool)> {
        if let Some(h) = self
            .replicas
            .read()
            .expect("replicas lock poisoned")
            .get(&pid)
        {
            return Ok((h.clone(), false));
        }
        let mut guard = self.replicas.write().expect("replicas lock poisoned");
        if let Some(h) = guard.get(&pid) {
            return Ok((h.clone(), false));
        }
        let disk = self
            .disks
            .pick(self.partition_capacity)
            .ok_or(Error::NoDiskSpace)?;
        let dir = disk.dir.join(format!("partition_{pid}"));
        let store_id = format!("p{pid}");
        // 判据取**盘上是否已有副本目录**(而不是"内存里有没有句柄"):OFFLINE 只摘句柄、数据留着,
        // 挂回时不能被当成"从零建"。
        let fresh = !dir.exists();
        // `*_partition(..)` 必须带上 `pid`:它同时是段头的 key 前缀与写入校验的准绳 ——
        // 用默认前缀 0 建库,则该分区上**任何真实 blobId 的写都会被拒**(前缀不符 → 400),
        // 读也永远 miss。打开时顺带校验盘上副本的分区正是这个 pid。
        let store = if fresh {
            BlobStore::create_with_partition(
                &dir,
                self.partition_capacity,
                &self.store_config,
                &self.store_metrics,
                &store_id,
                pid,
            )?
        } else {
            BlobStore::open_with_partition(
                &dir,
                self.partition_capacity,
                &self.store_config,
                &self.store_metrics,
                &store_id,
                pid,
            )?
        };
        let handle = Self::make_handle(disk.id.clone(), store);
        guard.insert(pid, handle.clone());
        Ok((handle, fresh))
    }

    /// 记下某分区的可写态(来自 master 心跳指令)。本地没有该副本 → 返回 `false`(忽略即可:
    /// 指令与本地副本建立存在竞态,下一拍还会重发)。
    pub fn set_partition_state(&self, pid: PartitionId, state: &str) -> bool {
        let handle = self.get(pid);
        match handle {
            Some(h) => {
                let mut g = h
                    .partition_state
                    .write()
                    .expect("partition_state lock poisoned");
                if *g == state {
                    return false;
                }
                *g = state.to_string();
                true
            }
            None => false,
        }
    }

    /// 记下某分区的**墓碑回收闸门**(来自 master 心跳指令)。本地没有该副本 → 返回 `false`(忽略即可:
    /// 指令与本地副本建立存在竞态,下一拍还会重发)。返回值 = 是否**真的发生了变化**(幂等)。
    pub fn set_tombstones_reclaim_allowed(&self, pid: PartitionId, allowed: bool) -> bool {
        match self.get(pid) {
            Some(h) => {
                let mut g = h
                    .tombstones_reclaim_allowed
                    .write()
                    .expect("tombstones_reclaim_allowed lock poisoned");
                if *g == allowed {
                    return false;
                }
                *g = allowed;
                true
            }
            None => false,
        }
    }

    /// 取某分区的本地 replica(数据面路由用);未建则 `None`。
    pub fn get(&self, pid: PartitionId) -> Option<ReplicaHandle> {
        self.replicas
            .read()
            .expect("replicas lock poisoned")
            .get(&pid)
            .cloned()
    }

    /// 用 `BlobStore` 建句柄并接被动 IO-error hook(置 ERROR)。
    ///
    /// 实然态初值是 [`STATE_BOOTSTRAP`] 而**不是** `ACTIVE`:本函数覆盖副本的诞生/重开/重装三条路径,
    /// 三条都还没有"已追平"的证据(追平只由拉复制判定后置 `ACTIVE`,见
    /// [`crate::replication::sync_round`]):
    /// - **新建空分区**:分区刚建时所有节点上都是空的,没有数据可同步 —— 但"空"要**问过 peer** 才知道
    ///   (peer 无该副本 / 双方都空),由拉复制那轮判定;
    /// - **物理 bootstrap**(`install_bootstrap`):只拷了 peer 的封存段,尾部还要靠增量拉,拷完不算追平;
    /// - **重启重开**(`ensure_replica` 的 `open_with` 分支):盘上数据可能是**旧的**,先报 `BOOTSTRAP`
    ///   是诚实状态(重启期间短暂显示"同步中"),等一轮拉复制确认追平再转正。
    ///
    /// 若这里谎报 `ACTIVE`,master 侧"搬迁要等继任者 `ACTIVE` 才算转移成功"的删源闸门会瞬间成立
    /// (见 `crates/master/src/reconcile.rs` 的 `plan_move`/`drop_confirmed`),源副本被摘除+
    /// `DROPPED` 删本地数据 —— 源若是当时**唯一有数据**的副本,就是丢数据(CLAUDE.md §3.1)。
    fn make_handle(disk_id: String, store: BlobStore) -> ReplicaHandle {
        let handle = ReplicaHandle {
            disk_id,
            store: Arc::new(store),
            current_state: Arc::new(RwLock::new(STATE_BOOTSTRAP.to_string())),
            lag: Arc::new(LagEstimate::new()),
            // 分区状态由 master 心跳指令填;新副本先按"未知"起步(未知 = 可压,单机模式行为不变)。
            partition_state: Arc::new(RwLock::new(STATE_UNKNOWN.to_string())),
            // 墓碑回收闸门同样由 master 心跳指令填;新副本先按"不放行"起步 —— 没得到权威放行前
            // **宁可不回收**(见字段文档),单机模式由消费侧放行。
            tombstones_reclaim_allowed: Arc::new(RwLock::new(false)),
        };
        let cs = handle.current_state.clone();
        handle.store.set_io_error_hook(Arc::new(move || {
            *cs.write().expect("current_state lock poisoned") = STATE_ERROR.to_string();
        }));
        handle
    }

    /// S3b:把来自 peer 的封存段字节装载为本地某分区副本(物理 bootstrap)。
    ///
    /// 崩溃安全:段文件 `tmp→fsync→rename` 逐个落地,全部就位后 `open_rebuild_index_with` 重建索引打开。
    /// 覆盖式(先清 partition 目录):仅用于**空/新** replica 的 bootstrap,幂等(失败重试重来)。
    ///
    /// 装载出来的副本实然态仍是 [`STATE_BOOTSTRAP`]:拷来的只是**封存段**,活跃尾还要靠随后的增量
    /// 拉复制补齐(见 [`crate::replication::sync_from_peer`] 的 first+empty 路径),追平判定通过后才转 `ACTIVE`。
    pub fn install_bootstrap(
        &self,
        pid: PartitionId,
        segments: &[(antfs_blob::LogSegmentName, Vec<u8>)],
    ) -> Result<ReplicaHandle> {
        use std::io::Write;
        self.remove_replica(pid);
        let disk = self
            .disks
            .pick(self.partition_capacity)
            .ok_or(Error::NoDiskSpace)?;
        let dir = disk.dir.join(format!("partition_{pid}"));
        let _ = std::fs::remove_dir_all(&dir);
        let log_dir = dir.join("log");
        if let Err(e) = std::fs::create_dir_all(&log_dir) {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e.into());
        }
        // 逐段按**段名**(位置 + 代次)落盘:段身份必须原样保留,否则该副本的段表与 peer 对不上。
        // 任一步失败(含最终打开时发现段头与文件名代次不符 —— 那说明对端在拷贝期间重写过 log)
        // 都**清掉半成品目录**再报错:让调用方干净地回落纯逻辑拉取。
        let write_all = || -> Result<()> {
            for (name, bytes) in segments {
                let final_path = log_dir.join(name.filename());
                let tmp_path = log_dir.join(format!("{}.tmp", name.filename()));
                let mut f = std::fs::File::create(&tmp_path)?;
                f.write_all(bytes)?;
                f.sync_all()?;
                drop(f);
                std::fs::rename(&tmp_path, &final_path)?;
            }
            Ok(())
        };
        if let Err(e) = write_all() {
            let _ = std::fs::remove_dir_all(&dir);
            return Err(e);
        }
        let store_id = format!("p{pid}");
        // 拷来的段头自述其分区与代次 —— 都必须与本次 bootstrap 对得上(拷错分区 / 拷到别的 incarnation
        // 的段,当场拒掉)。
        let opened = antfs_blob::BlobStore::open_rebuild_index_with_partition(
            &dir,
            self.partition_capacity,
            &self.store_config,
            &self.store_metrics,
            &store_id,
            pid,
        );
        let store = match opened {
            Ok(s) => s,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&dir);
                return Err(e.into());
            }
        };
        let handle = Self::make_handle(disk.id.clone(), store);
        self.replicas
            .write()
            .expect("replicas lock poisoned")
            .insert(pid, handle.clone());
        Ok(handle)
    }

    /// 摘除某分区本地 replica 的**内存句柄**(OFFLINE):盘上数据保留 —— 副本可能再回来。
    /// 要连盘上数据一起删,用 [`drop_replica`](Self::drop_replica)。
    pub fn remove_replica(&self, pid: PartitionId) {
        self.replicas
            .write()
            .expect("replicas lock poisoned")
            .remove(&pid);
    }

    /// **删除**某分区本地 replica 的盘上数据(整目录 `partition_{pid}`),并摘除内存句柄。
    ///
    /// `bool` = 是否真的删了(**幂等**:本地本来就没有 → `false`,不报错 —— 重复的 `DROPPED`
    /// 指令/重放必须安全)。
    ///
    /// 只在 master 下发 `DROPPED`(语义 = 该副本已从集群 placement 正式移除,且继任者已 `ACTIVE`)
    /// 后才可调用:此后不再有写入路由到本副本,删数据是安全的;`OFFLINE` 是**临时摘除**,
    /// 明确不删数据(见 [`remove_replica`](Self::remove_replica))。
    ///
    /// 删除顺序:**先摘句柄**再删目录 —— 句柄持有 `BlobStore` 的段文件 fd/mmap,反过来会让
    /// 已删除的文件仍被句柄钉住(Unix 下 inode 直到最后引用关闭才释放);摘出 map 后还会**短暂
    /// 等**在飞持有者退出(见 [`wait_for_writers_to_drain`](Self::wait_for_writers_to_drain))。
    /// 目录定位:句柄在时按它的 `disk_id` 找该盘;句柄已被 OFFLINE 摘掉(盘上数据还在)时
    /// 按盘列表找残留目录 —— 两条路径都覆盖,不留下孤儿副本目录。
    ///
    /// **阻塞**(含删目录的同步 IO 与有界等待):调用方须在阻塞池([`tokio::task::spawn_blocking`])
    /// 里跑,别在 reactor 线程上直调。
    pub fn drop_replica(&self, pid: PartitionId) -> Result<bool> {
        let removed = self
            .replicas
            .write()
            .expect("replicas lock poisoned")
            .remove(&pid);
        let disk_id = removed.as_ref().map(|h| h.disk_id.clone());
        // 句柄在这里 drop:释放 BlobStore(及其 fd/mmap)后,库里才没有对外声明在服务的副本。
        if let Some(h) = &removed {
            self.wait_for_writers_to_drain(&h.store);
        }
        drop(removed);
        let mut dropped = false;
        for disk in self.disks.disks() {
            if disk_id.as_deref().is_some_and(|id| id != disk.id) {
                continue;
            }
            let dir = disk.dir.join(format!("partition_{pid}"));
            if !dir.exists() {
                continue;
            }
            tracing::info!(partition = pid, dir = %dir.display(), "dropping local replica data");
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => dropped = true,
                // 竞态:`exists()` 与删除之间目录已被删掉 → 目的已达到,视为成功。
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => dropped = true,
                Err(e) => return Err(Error::Io(e)),
            }
            tracing::info!(partition = pid, dir = %dir.display(), "local replica data dropped");
        }
        Ok(dropped)
    }

    /// 删目录前**等在飞持有者退出**(有界):`drop_replica` 摘出 map 后不会再有**新**持有者,
    /// 但**老**持有者(在飞请求、worker 的 flush/compaction 快照)仍握着 `Arc<BlobStore>`,
    /// 它们写盘时的 `create_dir_all`(索引水位、段封存)会把刚删掉的目录复活成一个只有
    /// `index/` 的空壳 —— 那个空壳会让该分区日后在本节点**再也建不起来**
    /// (`ensure_replica` 走 `BlobStore::open`,而空壳里没有 log 段 → Corrupt)。
    /// 故先摘下句柄、等这些持有者自行 drop(通常几毫秒),再删目录。
    ///
    /// 超时(如正在跑的长 compaction)就照删不误:**不无限等** —— 本函数在阻塞池里跑,等太久
    /// 会拖住心跳 loop;残留空壳只是小概率的可运维问题,不像"卡住心跳"那样影响面大。
    fn wait_for_writers_to_drain(&self, store: &Arc<BlobStore>) {
        let deadline = std::time::Instant::now() + DROP_DRAIN_MAX_WAIT;
        while Arc::strong_count(store) > 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(DROP_DRAIN_POLL);
        }
        if Arc::strong_count(store) > 1 {
            tracing::warn!(
                holders = Arc::strong_count(store) - 1,
                "replica still in use after drain grace; deleting anyway"
            );
        }
    }

    /// 各 replica 的心跳实况(心跳 beat 的填充源;`used_bytes`/`lag_bytes` 见 [`ReplicaState`])。
    pub fn replica_states(&self) -> Vec<ReplicaState> {
        self.replicas
            .read()
            .expect("replicas lock poisoned")
            .iter()
            .map(|(pid, h)| {
                let st = h
                    .current_state
                    .read()
                    .expect("current_state lock poisoned")
                    .clone();
                ReplicaState {
                    partition_id: *pid,
                    disk_id: h.disk_id.clone(),
                    current_state: st,
                    used_bytes: h.store.valid_data_size(),
                    lag_bytes: h.lag.get(),
                }
            })
            .collect()
    }

    /// store 引擎的段容量(字节):复制层按段线性化估算滞后用(口径见 [`crate::replication::lag_bytes`])。
    pub fn segment_size_bytes(&self) -> u64 {
        self.store_config.segment_size_bytes
    }

    /// 当前本地 replica 数(指标用)。
    pub fn count(&self) -> usize {
        self.replicas.read().expect("replicas lock poisoned").len()
    }

    /// 快照全部 `(partition_id, ReplicaHandle)`(本地 worker 遍历维护用)。
    pub fn handles(&self) -> Vec<(PartitionId, ReplicaHandle)> {
        self.replicas
            .read()
            .expect("replicas lock poisoned")
            .iter()
            .map(|(pid, h)| (*pid, h.clone()))
            .collect()
    }

    /// 某分区副本的**暂存目录**(流式 put 的中转文件落点;按需创建)。
    ///
    /// 默认落在该副本的数据盘上而不是系统临时目录:大文件暂存不该吃 tmpfs(RAM);崩溃遗留由
    /// [`crate::disk::DiskManager::mount`] 的 `clean_tmp_dir` 清理。配了 `store_server.staging_dir`
    /// 则所有分区都用那一个目录(此时它已在 mount 期建好,这里的 `create_dir_all` 只是幂等兜底)。
    pub fn tmp_dir_for(&self, pid: PartitionId) -> Result<std::path::PathBuf> {
        let disk_id = self.get(pid).map(|h| h.disk_id).ok_or(Error::NoReplica)?;
        let dir = self
            .disks
            .tmp_dir_for_disk(&disk_id)
            .ok_or(Error::NoReplica)?;
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(dir: &std::path::Path) -> ReplicaManager {
        let dm = DiskManager::mount(&[dir.to_string_lossy().into_owned().into()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        ReplicaManager::new(dm, sc, sm, 4 << 20)
    }

    /// 副本诞生即 `BOOTSTRAP`(不是 `ACTIVE`);`mark_active` 单向、幂等,且**不覆盖 `ERROR`**
    /// (IO 故障是 master 再复制判据的输入,不能被一轮拉复制成功抹掉)。
    #[test]
    fn replica_starts_bootstrapping_and_mark_active_is_one_way() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let h = rm.ensure_replica(3).unwrap();
        assert_eq!(
            *h.current_state.read().unwrap(),
            STATE_BOOTSTRAP,
            "新建副本不得自称 ACTIVE"
        );
        assert!(h.mark_active(), "首次追平必须真的转正");
        assert_eq!(*h.current_state.read().unwrap(), STATE_ACTIVE);
        assert!(!h.mark_active(), "重复置位幂等(返回是否真的变了)");
        assert_eq!(*h.current_state.read().unwrap(), STATE_ACTIVE);

        // IO 故障标记不得被追平覆盖。
        *h.current_state.write().unwrap() = STATE_ERROR.to_string();
        assert!(!h.mark_active(), "ERROR 不得被 ACTIVE 覆盖");
        assert_eq!(*h.current_state.read().unwrap(), STATE_ERROR);
    }

    /// 副本的 key 前缀必须等于它的 pid,且**重开后依旧** —— 记录只存 blobId 低 32bit,
    /// 前缀错了则该分区上任何真实 blobId 都写不进去(实测过的 wiring bug:曾用默认前缀 0 建库)。
    #[test]
    fn replica_accepts_keys_of_its_own_partition_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned().into()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        let rm = ReplicaManager::new(dm, sc, sm, 4 << 20);

        let mine = antfs_blob::StoreKey::from_parts(9, 1);
        let foreign = antfs_blob::StoreKey::from_parts(0, 1);
        let h = rm.ensure_replica(9).unwrap();
        assert_eq!(h.store.key_prefix(), 9, "建库必须声明本分区前缀");
        h.store.put(mine, b"mine").unwrap();
        assert_eq!(h.store.get(&mine).unwrap().as_slice(), b"mine");
        assert!(
            h.store.put(foreign, b"foreign").is_err(),
            "别的 partition 的 key 必须被拒"
        );
        h.store.flush().unwrap();

        // 重开:前缀从段头读回,且仍拒绝外来 key(否则重启后就写不进去了)。
        rm.remove_replica(9);
        let h2 = rm.ensure_replica(9).unwrap();
        assert_eq!(h2.store.key_prefix(), 9, "前缀必须从段头读回");
        assert_eq!(h2.store.get(&mine).unwrap().as_slice(), b"mine");
        assert!(h2.store.put(foreign, b"foreign").is_err());
    }

    #[test]
    fn ensure_replica_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned().into()]).unwrap();
        let registry = antfs_metric::Registry::new();
        let sc = StoreConfig {
            segment_size_bytes: 1 << 20,
            index_max_inmem_elements: 1000,
            io_error_count_to_trigger_shutdown: 0,
        };
        let sm = StoreMetrics::new(&registry).unwrap();
        let rm = ReplicaManager::new(dm, sc, sm, 4 << 20);

        let a = rm.ensure_replica(7).unwrap();
        let b = rm.ensure_replica(7).unwrap();
        assert_eq!(
            rm.count(),
            1,
            "same partition must not create a second replica"
        );
        assert_eq!(a.disk_id, b.disk_id);
        assert!(rm.get(7).is_some());
        assert!(rm.get(8).is_none());
    }

    /// `ensure_replica_with_origin` 的 `true` = "**盘上原本没有**这份副本"(本次从零建出)——
    /// 控制面只在这时向 master 申领副本身份(见 `heartbeat::claim_incarnation`)。判据必须落在**盘上**:
    /// OFFLINE 摘除后挂回、进程重启重开都不算从零建,否则每挂回一次就换一次身份,对端手里的游标被
    /// 无故作废、白跑一轮全量重拉。
    #[test]
    fn ensure_replica_origin_reports_only_from_scratch_creations() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());

        let (h, fresh) = rm.ensure_replica_with_origin(4).unwrap();
        assert!(fresh, "盘上本来没有 → 从零建出");
        assert!(h.store.is_empty());

        let (_, fresh) = rm.ensure_replica_with_origin(4).unwrap();
        assert!(!fresh, "内存里已有句柄 → 幂等空转,不算从零建");

        // OFFLINE = 只摘句柄,盘上数据留着 → 挂回时是"打开已有副本",不是从零建。
        rm.remove_replica(4);
        let (_, fresh) = rm.ensure_replica_with_origin(4).unwrap();
        assert!(!fresh, "盘上还有副本目录 → 不算从零建(挂回不换身份)");

        // DROPPED = 真删目录 → 再建就是新的一代副本(该换身份)。
        assert!(rm.drop_replica(4).unwrap());
        let (_, fresh) = rm.ensure_replica_with_origin(4).unwrap();
        assert!(fresh, "目录已被删 → 重建属于从零建(该申领新身份)");
    }

    /// A6:删本地副本 = 目录消失 + 句柄摘除 + 幂等(重复调用返回 false 且仍成功)。
    #[test]
    fn drop_replica_deletes_dir_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        let h = rm.ensure_replica(5).unwrap();
        h.store
            .put(antfs_blob::StoreKey::from_parts(5, 5), b"to be dropped")
            .unwrap();
        h.store.flush().unwrap();
        let dir = tmp.path().join("partition_5");
        assert!(dir.is_dir(), "建副本必须落盘出目录");

        assert!(rm.drop_replica(5).unwrap(), "首次删除必须报告真的删了");
        assert!(!dir.exists(), "副本目录必须消失");
        assert!(rm.get(5).is_none(), "句柄必须被摘除");
        assert_eq!(rm.count(), 0);
        assert!(rm.replica_states().is_empty(), "心跳不再上报该副本");

        assert!(
            !rm.drop_replica(5).unwrap(),
            "重复调用幂等:本地没有 → false,且不报错"
        );
    }

    /// OFFLINE 只摘句柄不删盘;其后到达的 DROPPED 必须仍能删掉残留目录(不留孤儿副本)。
    #[test]
    fn drop_replica_removes_dir_left_by_offline() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = manager(tmp.path());
        rm.ensure_replica(9).unwrap();
        rm.remove_replica(9);
        let dir = tmp.path().join("partition_9");
        assert!(dir.is_dir(), "OFFLINE 不得删盘上数据");

        assert!(rm.drop_replica(9).unwrap(), "句柄已摘但目录仍在 → 仍须删掉");
        assert!(!dir.exists());
        assert!(!rm.drop_replica(9).unwrap(), "再来一次幂等");
    }

    /// 回归:还有别的持有者在用这个引擎(worker flush / 在飞请求都 clone 过句柄)时,必须先等
    /// 它们退出再删目录 —— 否则它们写盘的 `create_dir_all` 会把目录复活成只剩 `index/` 的空壳,
    /// 那个空壳会让该分区日后在本节点再也建不起来。
    #[test]
    fn drop_replica_waits_for_in_flight_holders_before_deleting() {
        let tmp = tempfile::tempdir().unwrap();
        let rm = Arc::new(manager(tmp.path()));
        let h = rm.ensure_replica(4).unwrap();
        let dir = tmp.path().join("partition_4");

        // 模拟"在飞持有者":另持一个引擎 Arc(在飞请求/维护快照就是这么干的)。
        let held = h.store.clone();
        let rm2 = rm.clone();
        let dropper = std::thread::spawn(move || rm2.drop_replica(4));
        // 持有者还在 → 目录不得被删(给它足够时间跑到等待点)。
        std::thread::sleep(Duration::from_millis(100));
        assert!(dir.is_dir(), "还有持有者时不得删目录");

        drop(held);
        assert!(dropper.join().expect("drop thread").expect("drop ok"));
        assert!(!dir.exists(), "持有者退出后必须真的删掉(不留空壳)");
        assert!(rm.get(4).is_none(), "句柄任何时候都已被摘除");
    }
}

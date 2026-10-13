//! `MasterConfig`:master 节点的类型化配置。
//!
//! 范式见 [`docs/design/common.md` §1];清单见 [`docs/design/cluster.md`](../../../docs/design/cluster.md)「配置项清单」。
//! 字段名即 TOML key(在 `[master]` 表下)。默认值 / 范围必须与本文件 `impl Default` + `Validate` 对齐
//! (CLAUDE.md §7)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// master 节点配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct MasterConfig {
    /// 本节点在 Raft 集群里的稳定 id(必须跨重启不变)。
    pub node_id: u64,
    /// Raft 内部 RPC(`RaftInternal` gRPC)监听地址(host:port)。
    pub raft_addr: String,
    /// 控制面 gRPC(`Master` 服务)监听地址(host:port)。
    pub grpc_addr: String,
    /// redb 数据目录(存 Raft log + 状态机快照)。
    pub data_dir: String,
    /// 初始成员集(node_id 列表);启动引导用。空 = 由外部 `initialize` 引导。
    pub initial_members: Vec<u64>,
    /// 准入共享密钥(Q1 安全点):注册 / 心跳校验;空串表示 M1 关闭校验(仅本地测试)。
    pub cluster_secret: String,
    /// 心跳超时(秒):超过则在软状态里标节点 down。
    pub heartbeat_timeout_secs: u64,
    /// 分区容量上界(字节):选盘时盘可用容量须 ≥ 它(§7.3)。
    pub partition_capacity_bytes: u64,
    /// 盘将满阈值(0,1]:用量超此比例的盘不再放新分区(本项目默认 0.8,较 Ambry ≈95% 更保守)。
    pub disk_near_full_ratio: f64,
    /// 是否开启自动扩容(Q5)。**默认关**(安全):开启才后台按水位建分区。
    pub auto_grow_enabled: bool,
    /// 自动扩容低水位:可写(RW)分区数低于它就补建。
    pub auto_grow_min_writable: u64,
    /// 自动扩容总量上限:分区总数达到它就停止自动建(护栏)。
    pub auto_grow_max_partitions: u64,
    /// 自动扩容检查周期(秒),兼作限速节拍(护栏:每周期最多建 `auto_grow_batch` 个)。
    pub auto_grow_interval_secs: u64,
    /// 自动扩容单周期建分区上限(限速护栏)。
    pub auto_grow_batch: u64,
    /// 调谐 / 失联检测周期(秒,M3)。
    pub reconcile_interval_secs: u64,
    /// 成为 leader 后的调谐稳定期(秒):期内不做失败驱动的再复制,等 store 心跳重建软状态,
    /// 避免重启/切主时误判全体失联触发再复制风暴(0 = 关闭宽限,立即调谐)。
    pub reconcile_startup_grace_secs: u64,
    /// 号段大小 N:client 单次向 master 申请的 objectId 个数,也是 client 一次缓存的号数
    /// (`GetBlobIds` 的 `count=0` 时取此值)。号段越大越省 RPC、client 崩溃留下的空洞越大
    /// (空洞无害,见 blobid-master-assign D3/D9)。
    pub id_alloc_segment_size: u32,
    /// 水位批次倍数:master 一次向 Raft 预留 `N × 该值` 个 objectId,再由 leader 切成 N 大小的号段发出。
    /// 越大 Raft 提交越省、切主留下的空洞越大(默认 2,见 blobid-master-assign D9)。
    pub id_alloc_raft_batch_multiple: u32,
    /// 计划式均衡:执行期同时在途的搬迁数上限(写入计划快照,`StartBalance` 后按它节流)。
    pub balance_max_in_flight: u32,
    /// 计划式均衡:一份计划最多包含的搬迁条数(`EvalBalanceRequest.max_moves=0` 时取此值)。
    pub balance_max_moves: u32,
    /// 计划式均衡:倾斜阈值——每盘副本数/用量比超过均值 × 该值才认为该盘倾斜
    /// (1.0 = 严格高于均值;另有"副本数差 ≥ 2 或用量比显著更好"的改善护栏防抖,见 balance.rs)。
    pub balance_skew_threshold: f64,
    /// 计划式均衡:单条在途搬迁的超时(秒);超时未摘源 → 判败(`FailBalanceMove`),释放并发预算,
    /// 不再重试该条(重试同样的落点大概率再卡,留给运维重新 eval)。
    pub balance_move_timeout_secs: u64,
    /// **判活迟滞**(D5):节点被判 down 需要**连续 K 个 reconcile tick** 都观察到心跳过期。
    ///
    /// 有效判死窗口 ≈ `heartbeat_timeout_secs + K × reconcile_interval_secs`(默认 15 + 3×5 = 30s)。
    /// 为什么需要它:判活原先只看单窗口(`elapsed() <= heartbeat_timeout_secs`,15s),而 store 每 3s
    /// 一拍 —— 一次 GC/调度停顿、一次 `statvfs` 卡顿就能跨过阈值,于是**一次抖动 = 判死 = 全节点再复制**。
    /// 3s 心跳在这段窗口里本应有 10 拍,少一两拍不该等于死亡。
    ///
    /// **0 = 保持改造前行为**(单窗口无迟滞),便于回退对比。
    pub node_down_debounce_ticks: u64,
    /// **再复制每 tick 预算**(D6):每 tick 最多**新发起**这么多个欠副本分区的补副本。
    ///
    /// 按 `PartitionId` 排序取前 K(确定性,便于复现),其余下 tick 继续;被压下的数量暴露成
    /// `master_rereplication_deferred`。为什么必须有它:原先 `add_targets` 一 tick 全量发起,
    /// 一个节点判死 ⇒ 它承载的**所有**分区同时开始搬数据(可能是几万个),把网络与盘写瞬间打满。
    ///
    /// 注意:它限的是"每 tick 的**数量**",不是 `balance_max_in_flight` 那种并发信号量 ——
    /// 两者名字相近但意义不同,别照抄。
    pub rereplication_max_per_tick: u64,
    /// **坏盘撤离总开关**(D4)。**默认 false**:这是新写的**自动搬迁动作**,与该类动作的保守惯例一致
    /// (同 `auto_grow_enabled`)。关闭时坏盘只被"不再接新副本"避开(既有行为),**不会有任何自动搬迁**。
    pub disk_evac_enabled: bool,
    /// **持续证据门槛**(秒,仅 `disk_evac_enabled` 时生效):某盘为 `UNAVAILABLE` 需**持续**这么久才允许
    /// 撤离它上面的副本。
    ///
    /// 为什么单轮故障枚举不够:一次抖动就搬 GB 级数据,代价不对称 —— 多留一份副本几乎免费,一次不必要的
    /// 迁移是真金白银(还可能让新副本与源副本一起进入追平)。故门槛取得比"判坏"宽松得多
    /// (判坏:3 轮/30s;撤离:默认 300s)。
    pub disk_evac_min_bad_secs: u64,
    /// **在途撤离预算**(个,仅 `disk_evac_enabled` 时生效):同时在途的撤离分区数上限。
    ///
    /// "在途" = 该分区在硬放置里**还带着坏盘上的那个副本**(搬迁未完成)。撤离是真搬 GB 数据,
    /// 风格对齐 `balance_max_in_flight = 1`:宁可慢,也别把网络与盘写一次打满。
    pub disk_evac_max_in_flight: u64,
    /// 磁盘预留空间比例(`0.0..1.0`):可用容量上限 `usable = disk_capacity × (1 − ratio)`。
    ///
    /// 预留覆盖 index 文件(Binary Fuse Filter、`.idx`)、`COMPACT_POLICY_STATE`、暂存 `.tmp`、
    /// 文件系统元数据以及 compaction 临时双份空间。`select_disks` 的逻辑承诺检查和物理空间检查
    /// 均用 `usable` 而非 `disk_capacity`。
    pub disk_reserved_ratio: f64,
    /// 引擎单段容量(字节):用于分区级写满感知(`used >= capacity − segment_cap` → 转 RO)。
    ///
    /// **须与 store 侧 `store.segment_size_bytes` 一致**;错配时 master 侧偏保守(提前一点转 RO)。
    pub segment_capacity_bytes: u64,
    /// `adjusted_free` 单调递减守卫超时(秒):心跳上报 `free_bytes > adjusted_free` 时,
    /// 若距上次心跳不超过此值则不覆盖(防滞后心跳回滚扣减);超时后接受心跳值。
    pub adjusted_free_timeout_secs: u64,
}

impl Default for MasterConfig {
    fn default() -> Self {
        MasterConfig {
            node_id: 1,
            raft_addr: "127.0.0.1:7100".to_string(),
            grpc_addr: "127.0.0.1:7101".to_string(),
            data_dir: "master-data".to_string(),
            initial_members: Vec::new(),
            cluster_secret: String::new(),
            heartbeat_timeout_secs: 15,
            partition_capacity_bytes: 100 * 1024 * 1024 * 1024,
            disk_near_full_ratio: 0.8,
            auto_grow_enabled: false,
            auto_grow_min_writable: 4,
            auto_grow_max_partitions: 100_000,
            auto_grow_interval_secs: 30,
            auto_grow_batch: 4,
            reconcile_interval_secs: 5,
            reconcile_startup_grace_secs: 15,
            // blobid-master-assign D9 的默认值:号段 1024、水位批次倍数 2(即一次 Raft 预留 2048 个号)。
            id_alloc_segment_size: 1024,
            id_alloc_raft_batch_multiple: 2,
            balance_max_in_flight: 1,
            balance_max_moves: 100,
            balance_skew_threshold: 1.0,
            balance_move_timeout_secs: 300,
            // 经验初值(见字段注释;标定前先按"宁可迟,不可早"取值)。
            node_down_debounce_ticks: 3,
            rereplication_max_per_tick: 4,
            // 自动搬迁动作:保守默认关(同 `auto_grow_enabled`)。
            disk_evac_enabled: false,
            disk_evac_min_bad_secs: 300,
            disk_evac_max_in_flight: 1,
            disk_reserved_ratio: 0.05,
            segment_capacity_bytes: 4 * 1024 * 1024 * 1024,
            adjusted_free_timeout_secs: 120,
        }
    }
}

impl Validate for MasterConfig {
    fn validate(&self) -> Result<()> {
        at_least("master.node_id", self.node_id, 1)?;
        at_least(
            "master.heartbeat_timeout_secs",
            self.heartbeat_timeout_secs,
            1,
        )?;
        at_least(
            "master.partition_capacity_bytes",
            self.partition_capacity_bytes,
            1,
        )?;
        if !(self.disk_near_full_ratio > 0.0 && self.disk_near_full_ratio <= 1.0) {
            return Err(antfs_config::ConfigError::invalid(
                "master.disk_near_full_ratio",
                "must be in (0, 1]",
            ));
        }
        at_least(
            "master.auto_grow_interval_secs",
            self.auto_grow_interval_secs,
            1,
        )?;
        at_least("master.auto_grow_batch", self.auto_grow_batch, 1)?;
        at_least(
            "master.reconcile_interval_secs",
            self.reconcile_interval_secs,
            1,
        )?;
        at_least(
            "master.id_alloc_segment_size",
            self.id_alloc_segment_size,
            1,
        )?;
        at_least(
            "master.id_alloc_raft_batch_multiple",
            self.id_alloc_raft_batch_multiple,
            1,
        )?;
        at_least(
            "master.balance_max_in_flight",
            self.balance_max_in_flight,
            1,
        )?;
        at_least("master.balance_max_moves", self.balance_max_moves, 1)?;
        if self.balance_skew_threshold <= 0.0 {
            return Err(antfs_config::ConfigError::invalid(
                "master.balance_skew_threshold",
                "must be > 0",
            ));
        }
        at_least(
            "master.balance_move_timeout_secs",
            self.balance_move_timeout_secs,
            1,
        )?;
        // D5/D6:迟滞轮数允许 0(= 关迟滞,保持改造前行为);再复制预算是**必须 ≥1** 的护栏
        // (0 会让欠副本永远补不上,那是静默停摆,不是"保守")。
        at_least(
            "master.rereplication_max_per_tick",
            self.rereplication_max_per_tick,
            1,
        )?;
        // D4:持续证据门槛必须 ≥1(0 等于"一看到 UNAVAILABLE 就搬",那正是门槛要防的事);
        // 在途预算 ≥1(0 会让撤离永远不动,那是静默停摆,不是"保守")。
        at_least(
            "master.disk_evac_min_bad_secs",
            self.disk_evac_min_bad_secs,
            1,
        )?;
        at_least(
            "master.disk_evac_max_in_flight",
            self.disk_evac_max_in_flight,
            1,
        )?;
        if !(self.disk_reserved_ratio >= 0.0 && self.disk_reserved_ratio < 1.0) {
            return Err(antfs_config::ConfigError::invalid(
                "master.disk_reserved_ratio",
                "must be in [0.0, 1.0)",
            ));
        }
        at_least(
            "master.segment_capacity_bytes",
            self.segment_capacity_bytes,
            1,
        )?;
        at_least(
            "master.adjusted_free_timeout_secs",
            self.adjusted_free_timeout_secs,
            1,
        )?;
        if self.raft_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "master.raft_addr",
                "must not be empty",
            ));
        }
        if self.grpc_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "master.grpc_addr",
                "must not be empty",
            ));
        }
        if self.data_dir.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "master.data_dir",
                "must not be empty",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 本批(盘故障处置 D4/D5/D6)新增配置项的**默认值闸门**:改任何一个都必须显式改本测试。
    ///
    /// 为什么逐个断言而不是只测 `Default`:`Default` 变了但没人注意,是"经验初值被误当成标定过的值"
    /// 最常见的路径。这些数字在**压测标定**(见 `docs/design/cluster.md` §9.3)之前都是经验值。
    #[test]
    fn disk_fault_defaults_are_pinned_pending_calibration() {
        let c = MasterConfig::default();
        assert_eq!(
            c.node_down_debounce_ticks, 3,
            "判活迟滞拍数(有效判死窗口 ≈ heartbeat_timeout + K × reconcile_interval)"
        );
        assert_eq!(
            c.rereplication_max_per_tick, 4,
            "每 tick 新发起的再复制分区上限"
        );
        assert!(
            !c.disk_evac_enabled,
            "自动搬迁动作默认关(与 auto_grow_enabled 同风格)"
        );
        assert_eq!(
            c.disk_evac_min_bad_secs, 300,
            "坏盘撤离的持续证据门槛(应远大于判坏门槛)"
        );
        assert_eq!(
            c.disk_evac_max_in_flight, 1,
            "在途撤离分区数上限(风格对齐 balance_max_in_flight)"
        );
    }

    /// 本批配置的校验:危险的取值必须**当场起不来**,而不是悄悄退化成"静默停摆"或"每拍刷屏"。
    ///
    /// - `rereplication_max_per_tick = 0` ⇒ 欠副本永远补不上(静默停摆),不是"保守";
    /// - `disk_evac_min_bad_secs = 0` ⇒ 一看到 `UNAVAILABLE` 就搬数据(正是门槛要防的事);
    /// - `disk_evac_max_in_flight = 0` ⇒ 撤离永远不动,同样是静默停摆;
    /// - `node_down_debounce_ticks = 0` ⇒ **合法**(保持改造前行为,便于回退对比)。
    #[test]
    fn disk_fault_validation_rejects_silent_stalls() {
        let base = MasterConfig::default();
        assert!(base.validate().is_ok(), "默认配置应合法");

        for (field, cfg) in [
            (
                "master.rereplication_max_per_tick",
                MasterConfig {
                    rereplication_max_per_tick: 0,
                    ..base.clone()
                },
            ),
            (
                "master.disk_evac_min_bad_secs",
                MasterConfig {
                    disk_evac_min_bad_secs: 0,
                    ..base.clone()
                },
            ),
            (
                "master.disk_evac_max_in_flight",
                MasterConfig {
                    disk_evac_max_in_flight: 0,
                    ..base.clone()
                },
            ),
        ] {
            assert!(cfg.validate().is_err(), "{field} 的危险取值必须被拒");
        }

        let no_debounce = MasterConfig {
            node_down_debounce_ticks: 0,
            ..base
        };
        assert!(
            no_debounce.validate().is_ok(),
            "0 = 关迟滞(保持改造前行为),必须合法以便回退对比"
        );
    }

    #[test]
    fn capacity_guard_defaults_are_pinned() {
        let c = MasterConfig::default();
        assert!(
            (c.disk_reserved_ratio - 0.05).abs() < f64::EPSILON,
            "默认预留 5%"
        );
        assert_eq!(
            c.segment_capacity_bytes,
            4 * 1024 * 1024 * 1024,
            "默认 4 GiB 对齐 store 侧"
        );
        assert_eq!(c.adjusted_free_timeout_secs, 120, "默认 120 秒超时");
    }

    #[test]
    fn capacity_guard_validation() {
        let base = MasterConfig::default();

        for (field, cfg) in [
            (
                "disk_reserved_ratio negative",
                MasterConfig {
                    disk_reserved_ratio: -0.1,
                    ..base.clone()
                },
            ),
            (
                "disk_reserved_ratio >= 1.0",
                MasterConfig {
                    disk_reserved_ratio: 1.0,
                    ..base.clone()
                },
            ),
            (
                "segment_capacity_bytes = 0",
                MasterConfig {
                    segment_capacity_bytes: 0,
                    ..base.clone()
                },
            ),
            (
                "adjusted_free_timeout_secs = 0",
                MasterConfig {
                    adjusted_free_timeout_secs: 0,
                    ..base.clone()
                },
            ),
        ] {
            assert!(cfg.validate().is_err(), "{field} 应被拒绝");
        }

        let edge = MasterConfig {
            disk_reserved_ratio: 0.0,
            ..base
        };
        assert!(
            edge.validate().is_ok(),
            "disk_reserved_ratio = 0.0 合法(不预留)"
        );
    }
}

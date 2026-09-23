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

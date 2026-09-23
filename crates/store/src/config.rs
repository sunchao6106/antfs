//! `StoreServerConfig`:数据节点配置(对齐 [`docs/design/cluster.md`](../../../docs/design/cluster.md) §9)。
//!
//! 字段名即 TOML key(在 `[store_server]` 表下)。默认值/范围须与本文件 `impl Default` + `Validate`
//! 对齐(CLAUDE.md §7)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// 数据节点配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct StoreServerConfig {
    /// 本数据节点 id(与 master 侧一致)。
    pub node_id: u64,
    /// 挂载的盘目录列表(每个目录当一块盘,`statfs` 探真实容量)。
    pub data_dirs: Vec<String>,
    /// 数据面 HTTP 监听地址(host:port)。
    pub http_addr: String,
    /// 每个分区副本 `BlobStore` 的容量上界(字节),对齐 master `partition_capacity_bytes`。
    pub partition_capacity_bytes: u64,
    /// master 控制面 gRPC 地址;**空 = 不接 master**(S1 独立模式)。
    pub master_addr: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致);空 = 关校验。
    pub cluster_secret: String,
    /// 心跳周期(秒)。
    pub heartbeat_interval_secs: u64,
    /// 是否开启主动磁盘探测(默认关,对齐 Ambry)。
    pub disk_health_enabled: bool,
    /// 主动磁盘探测周期(秒)。
    pub disk_health_interval_secs: u64,
    /// 主动磁盘探测单次读写的超时(秒,抓盘 hang)。
    pub disk_health_timeout_secs: u64,
    /// 是否开启拉复制(S3a);默认关(安全)。需 `master_addr` 才能发现 peer。
    pub replication_enabled: bool,
    /// 拉复制周期(秒)。
    pub replication_interval_secs: u64,
    /// 单次向某 peer 拉取元数据的批字节上限(限流)。
    pub replication_max_batch_bytes: u64,
    /// 是否用封存段物理拷贝加速 bootstrap(S3b);默认 true,随 `replication_enabled` 生效。
    pub bootstrap_copy_enabled: bool,
    /// 是否开启本地维护 worker(S4:周期 flush / compaction);默认 true。
    pub local_worker_enabled: bool,
    /// 本地 worker flush 周期(秒):推进持久化边界,缩小 W=2 page-cache 掉电窗口。
    pub flush_interval_secs: u64,
    /// 本地 worker compaction 检查周期(秒):按死数据占比阈值增量压缩。
    pub compaction_interval_secs: u64,
    /// compaction 触发阈值 (0,1]:单个封存段死数据占比 ≥ 此值才压缩(对齐 Ambry StatsBased)。
    pub compaction_dead_ratio: f64,
    /// 数据面单请求 body 上限(字节),经 axum `DefaultBodyLimit` 生效。
    /// **默认 2 MiB = axum `DefaultBodyLimit` 的默认值**(CLAUDE.md §5.1 不擅自改默认):要观测/接收
    /// 真正的大 blob 需显式调大;调小则超限请求在 extractor 前即被 413 拒掉。
    pub max_body_bytes: u64,
    /// 请求静默(`/admin/request-control`)未指定 `ttl_secs` 时的默认禁用时长(秒)。
    /// **0 = 不设默认时长**(即未指定 `ttl_secs` 时永久禁用,须显式开回)。
    pub request_control_default_ttl_secs: u64,
    /// 本地完整性校验(`GET /partitions/{pid}/verify`)单次扫描的**读取字节上限**(默认 64 MiB):
    /// 深扫会读全量数据体,限流以免打爆盘;到点返回部分结果并在响应里标 `truncated`。
    pub scrub_max_batch_bytes: u64,
}

impl Default for StoreServerConfig {
    fn default() -> Self {
        StoreServerConfig {
            node_id: 1,
            data_dirs: Vec::new(),
            http_addr: "127.0.0.1:7200".to_string(),
            partition_capacity_bytes: 100 * 1024 * 1024 * 1024,
            master_addr: String::new(),
            cluster_secret: String::new(),
            heartbeat_interval_secs: 3,
            disk_health_enabled: false,
            disk_health_interval_secs: 60,
            disk_health_timeout_secs: 10,
            replication_enabled: false,
            replication_interval_secs: 5,
            replication_max_batch_bytes: 4 * 1024 * 1024,
            bootstrap_copy_enabled: true,
            local_worker_enabled: true,
            flush_interval_secs: 30,
            compaction_interval_secs: 3600,
            compaction_dead_ratio: 0.5,
            // axum `DefaultBodyLimit` 的默认值(2 MiB);不擅自改默认,调大才收得下真正的大 blob。
            max_body_bytes: 2 * 1024 * 1024,
            request_control_default_ttl_secs: 1800,
            scrub_max_batch_bytes: 64 << 20,
        }
    }
}

impl Validate for StoreServerConfig {
    fn validate(&self) -> Result<()> {
        at_least("store_server.node_id", self.node_id, 1)?;
        at_least(
            "store_server.partition_capacity_bytes",
            self.partition_capacity_bytes,
            1,
        )?;
        if self.data_dirs.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.data_dirs",
                "must declare at least one disk directory",
            ));
        }
        if self.http_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.http_addr",
                "must not be empty",
            ));
        }
        at_least(
            "store_server.heartbeat_interval_secs",
            self.heartbeat_interval_secs,
            1,
        )?;
        at_least(
            "store_server.disk_health_interval_secs",
            self.disk_health_interval_secs,
            1,
        )?;
        at_least(
            "store_server.disk_health_timeout_secs",
            self.disk_health_timeout_secs,
            1,
        )?;
        at_least(
            "store_server.replication_interval_secs",
            self.replication_interval_secs,
            1,
        )?;
        at_least(
            "store_server.replication_max_batch_bytes",
            self.replication_max_batch_bytes,
            1,
        )?;
        at_least(
            "store_server.flush_interval_secs",
            self.flush_interval_secs,
            1,
        )?;
        at_least(
            "store_server.compaction_interval_secs",
            self.compaction_interval_secs,
            1,
        )?;
        at_least("store_server.max_body_bytes", self.max_body_bytes, 1)?;
        // 0 合法 = 不设默认时长(永久禁用),故下界为 0。
        at_least(
            "store_server.request_control_default_ttl_secs",
            self.request_control_default_ttl_secs,
            0,
        )?;
        at_least(
            "store_server.scrub_max_batch_bytes",
            self.scrub_max_batch_bytes,
            1,
        )?;
        if !(self.compaction_dead_ratio > 0.0 && self.compaction_dead_ratio <= 1.0) {
            return Err(antfs_config::ConfigError::invalid(
                "store_server.compaction_dead_ratio",
                "must be in (0, 1]",
            ));
        }
        Ok(())
    }
}

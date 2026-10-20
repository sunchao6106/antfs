//! `RouterConfig`:router(客户端 SDK)配置(TOML `[router]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。
//! **没有 W/R 配置项**:写成功副本数由该分区**实际副本数**推导(多数派),见
//! [`crate::Router::put`];读恒为「第一个成功即返回」——这两条是正确性约束,不做成可配。
//!
//! frontend 内嵌本配置时放在 `[frontend.router]` 子表下(结构完全相同)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// router(客户端 SDK)配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct RouterConfig {
    /// master 控制面 gRPC 地址(host:port)。
    ///
    /// **须指向 leader**(或前面挂一层转发):`GetBlobIds` 是写路径,非 leader 会回 `ok=false`
    /// 并给出 leader 地址(当前回填的是 raft 地址,不能直接当 gRPC 端用,详见 `docs/design/client.md`)。
    pub master_addr: String,
    /// 集群准入密钥(与 master `cluster_secret` 一致);空 = 关校验(仅本地测试)。
    pub cluster_secret: String,
    /// 单次数据面 HTTP 请求超时(秒)。
    pub request_timeout_secs: u64,
    /// **每个可写分区均分时**一次向 master 申请的 blobId 个数(号段大小 N)。router 稳态下同时持有
    /// 「可写分区数 × N」个号(**总预留口径与写权重无关**;每个分区实际拿到多少按 master 下发的写权重
    /// 配比,见 `docs/design/router.md` §2):越大越省 RPC、router 崩溃留下的空洞越大
    /// (空洞无害,见 `blobid-master-assign` D3/D9)。
    pub id_segment_size: u32,
    /// **读路由是否按观测延迟排序副本**(默认 `true`)。
    ///
    /// 读恒为 R=1(第一个成功即返回),故**试副本的顺序直接决定读延迟**。开着时,router 会记录每个副本
    /// 最近若干次请求的耗时(指数移动平均),读时**先试快的**;没有测量数据的副本按"已知值的均值"参与排序,
    /// 全都无数据时保持 master 给的顺序(即与关闭时完全一致)。
    ///
    /// **为什么默认开着**(与"不擅改默认值"不冲突):它**只改试副本的顺序,不改副本集合** ——
    /// 读的候选集、写的 W 多数派语义、冲突解决全都不变,故不可能引入新的正确性问题;
    /// 而无数据时它是**恒等变换**。慢副本本该被绕开 —— 这正是客户端侧最便宜的一块收益。
    ///
    /// 关掉它的场景:排查"为什么读打到了某个副本"(定位问题时想要确定性顺序),或对照压测。
    pub latency_aware_routing: bool,
    /// 后台刷新**可写分区列表**的周期(秒)。router 起一个后台任务,按此周期向 master 拉一次可写(RW)分区列表:
    /// 已不可写的分区,其本地号段被丢掉;新出现的可写分区,补一段加进缓存。
    /// **0 = 关闭**刷新(退回"只在号段耗尽时拉一次列表")。
    pub partition_refresh_secs: u64,
    /// SDK 层透明数据压缩(`router-blob-compression`,对齐 Ambry `router.compression.enabled`):
    /// 开启后 Bytes 形态写入过三闸(enabled / min_size / ratio)以**自描述帧**落盘 —— 帧头带
    /// 算法号,集群可混存不同时期不同算法写的对象,读侧按帧自动解压。**默认 false = 行为
    /// 与无该特性完全一致**;开启是运维决策(读 CPU 换盘/网带宽)。
    pub compression_enabled: bool,
    /// 压缩算法:`gzip`(默认)| `zstd` | `lz4` | `snappy`。改配置只影响**新写入**;
    /// 旧对象按帧头算法号照常读回(算法演进不破坏可读性)。
    pub compression_algorithm: String,
    /// 压缩最小体量(字节):小于它的对象不试压(海量小文件为主体的仓,压无益)。
    pub compression_min_size_bytes: usize,
    /// 最小压缩比率(orig/compressed):试压不足此值即回落原文(熵数据自动出局)。
    pub compression_min_ratio: f64,
    /// 压缩级别:zstd 1..=19 / gzip 0..=9 / lz4、snappy 定档(忽略级别)。默认 1 = 速度面
    /// (网关 CPU 是共享资源)。
    pub compression_level: i32,
}

impl Default for RouterConfig {
    fn default() -> Self {
        RouterConfig {
            master_addr: "127.0.0.1:7101".to_string(),
            cluster_secret: String::new(),
            request_timeout_secs: 10,
            id_segment_size: 1024,
            latency_aware_routing: true,
            partition_refresh_secs: 30,
            compression_enabled: false,
            compression_algorithm: "gzip".to_string(),
            compression_min_size_bytes: 4096,
            compression_min_ratio: 1.05,
            compression_level: 1,
        }
    }
}

impl Validate for RouterConfig {
    fn validate(&self) -> Result<()> {
        if self.master_addr.is_empty() {
            return Err(antfs_config::ConfigError::invalid(
                "client.master_addr",
                "must not be empty",
            ));
        }
        at_least("router.request_timeout_secs", self.request_timeout_secs, 1)?;
        at_least("router.id_segment_size", self.id_segment_size, 1)?;
        if !crate::compression::SUPPORTED_ALGOS.contains(&self.compression_algorithm.as_str()) {
            return Err(antfs_config::ConfigError::invalid(
                "router.compression_algorithm",
                "must be one of: zstd, gzip, lz4, snappy",
            ));
        }
        match self.compression_algorithm.as_str() {
            "zstd" if !(1..=19).contains(&self.compression_level) => {
                return Err(antfs_config::ConfigError::invalid(
                    "router.compression_level",
                    "zstd level must be in [1, 19]",
                ));
            }
            "gzip" if !(0..=9).contains(&self.compression_level) => {
                return Err(antfs_config::ConfigError::invalid(
                    "router.compression_level",
                    "gzip level must be in [0, 9]",
                ));
            }
            _ => {}
        }
        if !(self.compression_min_ratio.is_finite()
            && (1.0..=100.0).contains(&self.compression_min_ratio))
        {
            return Err(antfs_config::ConfigError::invalid(
                "router.compression_min_ratio",
                "must be in [1.0, 100.0]",
            ));
        }
        // min_size 不设下限(0 = 全试压是合法运维选择);零长 body 恒走 too_small 闸。
        Ok(())
    }
}

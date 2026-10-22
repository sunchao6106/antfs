//! `RouterConfig`:router(客户端 SDK)配置(TOML `[router]` 表下)。
//!
//! 字段名即 TOML key;默认值/范围须与 `impl Default` + `Validate` 对齐(CLAUDE.md §7)。
//! **没有 W/R 配置项**:写成功副本数由该分区**实际副本数**推导(多数派),见
//! [`crate::Router::put`];读恒为「第一个成功即返回」——这两条是正确性约束,不做成可配。
//!
//! frontend 内嵌本配置时放在 `[frontend.router]` 子表下(结构完全相同)。

use antfs_config::{at_least, Result, Validate};
use serde::Deserialize;

/// `put_bytes_chunk_bytes` 默认值:4 MiB —— 与网关恒切片宽同档,Ambry
/// `router.max.put.chunk.size.bytes` 同型默认(RouterConfig.java,恒切、无开关)。
pub const DEFAULT_PUT_BYTES_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
/// `put_bytes_chunk_bytes` 下限:切无收益、描述符开销反超的界。
pub const PUT_BYTES_CHUNK_MIN_BYTES: u64 = 64 * 1024;
/// `put_bytes_chunk_bytes` 上限:片是"中等单元",过 1 GiB 失切义(与内存帧上界同档)。
pub const PUT_BYTES_CHUNK_MAX_BYTES: u64 = 1 << 30;

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
    // 压缩五旋钮 2026-10-21 整体移除(所有者裁决13:读接口定型后基于流式读口重开;
    // 存量压缩帧将按原字节读回,重开须带帧头兼容,见 router.md 待办)。
    /// SDK 直连大 Bytes 写(`put` / `put_auto` 族)的**恒切片宽**(router 下沉,2026-10-21):
    /// body 超过它时,router 内部切成多片**普通 blob** + 尾笔写**拼装描述符**,
    /// 片经发号轮转散向不同可写分区(热点分摊);对齐 Ambry `router.max.put.chunk.size.bytes`
    /// (恒切、无开关)。**禁 0**(2026-10-22 裁决:恒切不可关,validate 拒 0)。读侧成对口 = `get_object`
    /// (透明拼装);`get` / `get_file` 保持裸字节语义。**内嵌本表做网关的仓**须交叉校验
    /// "网关自身片宽 ≤ 本值"(frontend / s3 的 `validate` 已带),否则网关片会被二次切。
    pub put_bytes_chunk_bytes: u64,
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
            put_bytes_chunk_bytes: DEFAULT_PUT_BYTES_CHUNK_BYTES,
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
        // 零长 body 恒走 too_small 闸。
        // 2026-10-22 裁决:恒切不可关(0 必须拒绝)——直连 GB 级单笔失去兜底防线。
        if !(PUT_BYTES_CHUNK_MIN_BYTES..=PUT_BYTES_CHUNK_MAX_BYTES)
            .contains(&self.put_bytes_chunk_bytes)
        {
            return Err(antfs_config::ConfigError::invalid(
                "router.put_bytes_chunk_bytes",
                "must be in [64 KiB, 1 GiB] and nonzero: chunking cannot be disabled",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_bytes_chunk_bytes_bounds() {
        let mut cfg = RouterConfig::default();
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.put_bytes_chunk_bytes, DEFAULT_PUT_BYTES_CHUNK_BYTES);

        for bad in [0u64, 1, 63 * 1024, (1 << 30) + 1] {
            cfg.put_bytes_chunk_bytes = bad;
            assert!(cfg.validate().is_err(), "rejected width {bad}");
        }
        for ok in [64 * 1024, 1 << 30] {
            cfg.put_bytes_chunk_bytes = ok;
            assert!(cfg.validate().is_ok(), "accepted width {ok}");
        }
    }
}

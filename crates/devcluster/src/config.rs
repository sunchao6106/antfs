//! devcluster 的启动参数:内置默认 → TOML 配置文件 → 环境变量(`AMBRY_*`)→ 命令行,后覆盖前。
//!
//! 配置文件来源:显式 `--config PATH`(不存在即报错),或**当前目录下的** [`AUTO_FILE`](存在才读)。
//! 字段名即 key(与仓库其它模块一致),单层 flat、不留分组;样例见仓库根的 `devcluster.toml.example`。
//! 只写想改的键,**没写的键回落内置默认** —— 于是本地会话可以把"宿主盘偏满"这类环境差异钉在文件里,
//! 不必每次敲 flag。

use std::path::{Path, PathBuf};

use antfs_config::{at_least, in_range, ConfigError, Loader, Result, Validate};

/// 当前目录下自动读取的配置文件名(存在才读;`--config` 显式指定的路径优先级更高)。
pub const AUTO_FILE: &str = "devcluster.toml";

/// 本地集群的形状、端口与可选压测参数。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DevClusterConfig {
    /// 数据/日志根目录(退出不删,便于复检)。
    pub dir: PathBuf,
    /// 启动后创建的分区数。默认 3:本地联调够用(3 分区 × RF3 = 9 个副本),
    /// 再多只是让每个 store 多开几份引擎占内存。
    pub partitions: u32,
    /// 每分区副本数(必须 ≤ [`store_count`](Self::store_count))。
    pub replication_factor: u32,
    /// master 控制面 gRPC 端口。
    pub master_grpc_port: u16,
    /// master Raft 内部 RPC 端口。
    pub master_raft_port: u16,
    /// store 节点 HTTP 起始端口,依次 +0/+1/+2。
    pub store_base_port: u16,
    /// admin web 端口。
    pub admin_port: u16,
    /// store 节点数(默认 3;≥4 才能在 RF=3 时真正执行 move/均衡——devcluster 用于验证搬迁与删源)。
    pub store_count: u32,
    /// master 的 `disk_near_full_ratio`(默认 0.8;调小可在本地复现"盘将满 → 分区转 RO")。
    ///
    /// 口径是**宿主机整盘**:`statfs` 的已用比例一旦高于它,该盘就不再参与放置 ——
    /// 于是"一个分区都建不出来"、且每个分区会被陆续转只读。本地盘偏满时把它提到 0.95 即可。
    pub disk_near_full_ratio: f64,
    /// store 是否开启主动盘探测(`disk_health_enabled`;开启后心跳带 HEALTHY/... 健康值)。
    pub disk_health: bool,
    /// frontend REST 端口。
    pub frontend_port: u16,
    /// 对外服务的监听 host(容器里传 `0.0.0.0` 才能被 `-p` 发布的端口访问)。
    /// **只影响监听地址**:集群内部互连恒为 `127.0.0.1`(master/store/admin/frontend 同进程)。
    pub host: String,
    /// `router` 或 `frontend`:起完集群(等就绪)后在同一进程内跑一轮压测并退出。
    /// 客户端与被测服务共享同一份 CPU/盘 IO,数字只作冒烟参考 —— 正式压测请另起进程跑 bench 二进制。
    pub run_bench: Option<String>,
    /// 压测写操作数(仅 `run_bench` 时用)。
    pub bench_ops: u64,
    /// 压测并发数。
    pub bench_concurrency: u64,
    /// 压测对象字节数。
    pub bench_size: usize,
    /// 压测读阶段占比 %。
    pub bench_read_ratio: u64,
}

impl Default for DevClusterConfig {
    fn default() -> Self {
        DevClusterConfig {
            dir: PathBuf::from("./.localcluster"),
            partitions: 3,
            replication_factor: 3,
            master_grpc_port: 7101,
            master_raft_port: 7100,
            store_base_port: 7200,
            admin_port: 7300,
            store_count: 3,
            disk_near_full_ratio: 0.8,
            disk_health: false,
            frontend_port: 7400,
            host: "127.0.0.1".to_string(),
            run_bench: None,
            bench_ops: 20_000,
            bench_concurrency: 32,
            bench_size: 4096,
            bench_read_ratio: 20,
        }
    }
}

impl Validate for DevClusterConfig {
    fn validate(&self) -> Result<()> {
        at_least("partitions", self.partitions, 1)?;
        at_least("replication_factor", self.replication_factor, 1)?;
        at_least("store_count", self.store_count, 1)?;
        at_least("bench_ops", self.bench_ops, 1)?;
        at_least("bench_concurrency", self.bench_concurrency, 1)?;
        at_least("bench_size", self.bench_size, 1)?;
        in_range("bench_read_ratio", self.bench_read_ratio, 0, 100)?;
        // 副本数 > 节点数必然放不下。选盘那里会耗满 20s 才失败(只留一句"insufficient disks"),
        // 不如在解析期当场说清:这类错配一眼就能改对。
        if self.replication_factor > self.store_count {
            return Err(ConfigError::invalid(
                "replication_factor",
                format!(
                    "{} 个副本 > {} 个 store 节点,放不下",
                    self.replication_factor, self.store_count
                ),
            ));
        }
        // 与 master 的 `MasterConfig::validate` 同口径:`0` 会让每块盘都判"将满"。
        if !(self.disk_near_full_ratio > 0.0 && self.disk_near_full_ratio <= 1.0) {
            return Err(ConfigError::invalid(
                "disk_near_full_ratio",
                format!("{} 不在 (0,1] 内", self.disk_near_full_ratio),
            ));
        }
        if self.host.trim().is_empty() {
            return Err(ConfigError::invalid("host", "监听 host 不能为空"));
        }
        if let Some(mode) = self.run_bench.as_deref() {
            antfs_config::one_of("run_bench", mode, &["router", "frontend"])?;
        }
        Ok(())
    }
}

/// 读配置(不校验:命令行还没叠加,见 [`Validate`] 的调用点在 `parse_args` 末尾)。
///
/// 返回 `(配置, 实际用的文件)`:文件路径要打日志 —— 隐式读取必须看得见,否则"我明明改了键"
/// 与"其实读到的是另一个文件的键"会耗掉一整轮排查。
pub fn load(explicit: Option<&Path>) -> Result<(DevClusterConfig, Option<PathBuf>)> {
    let file = match explicit {
        Some(p) => Some(p.to_path_buf()),
        None => {
            let auto = PathBuf::from(AUTO_FILE);
            auto.is_file().then_some(auto)
        }
    };
    let mut loader = Loader::new().with_env_prefix("AMBRY");
    if let Some(p) = &file {
        // 显式指定的路径必须存在(打错文件名要当场报错);自动发现的那个已经确认过存在。
        loader = loader.with_file(p.to_string_lossy(), explicit.is_some());
    }
    let cfg: DevClusterConfig = loader.load()?;
    Ok((cfg, file))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_self_consistent_and_match_the_help_text() {
        let d = DevClusterConfig::default();
        assert_eq!(d.partitions, 3);
        assert_eq!(d.replication_factor, 3);
        assert_eq!(d.store_count, 3);
        assert_eq!(d.disk_near_full_ratio, 0.8);
        assert_eq!(d.dir, PathBuf::from("./.localcluster"));
        d.validate()
            .expect("内置默认值必须自洽(RF=3 ≤ store_count=3)");
    }

    #[test]
    fn a_config_file_overrides_only_the_keys_it_mentions() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(AUTO_FILE);
        std::fs::write(&path, "partitions = 2\ndisk_near_full_ratio = 0.95\n").unwrap();

        let (cfg, file) = load(Some(&path)).unwrap();
        assert_eq!(
            file.as_deref(),
            Some(path.as_path()),
            "要能回答'读的是哪个文件'"
        );
        assert_eq!(cfg.partitions, 2);
        assert_eq!(cfg.disk_near_full_ratio, 0.95);
        assert_eq!(cfg.replication_factor, 3, "没写的键回落内置默认");
        assert_eq!(cfg.dir, PathBuf::from("./.localcluster"));
        cfg.validate().unwrap();
    }

    #[test]
    fn a_missing_explicit_config_file_is_an_error_not_a_silent_default() {
        let tmp = tempfile::tempdir().unwrap();
        let err = load(Some(&tmp.path().join("nope.toml"))).unwrap_err();
        assert!(
            err.to_string().contains("nope.toml"),
            "报错要带上文件名:{err}"
        );
    }

    #[test]
    fn a_typo_in_the_config_file_is_rejected_instead_of_silently_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join(AUTO_FILE);
        std::fs::write(&path, "partitons = 2\n").unwrap();
        let err = load(Some(&path)).unwrap_err();
        assert!(
            err.to_string().contains("partitons"),
            "报错要点名那个键:{err}"
        );
    }

    #[test]
    fn validation_rejects_misconfiguration_that_could_never_be_placed() {
        let rf_too_big = DevClusterConfig {
            replication_factor: 4,
            ..Default::default()
        };
        assert!(rf_too_big.validate().is_err(), "RF > 节点数必须解析期就拒");

        let zero_partitions = DevClusterConfig {
            partitions: 0,
            ..Default::default()
        };
        assert!(zero_partitions.validate().is_err());

        let bad_ratio = DevClusterConfig {
            disk_near_full_ratio: 0.0,
            ..Default::default()
        };
        assert!(bad_ratio.validate().is_err());

        let bad_bench = DevClusterConfig {
            run_bench: Some("routerr".to_string()),
            ..Default::default()
        };
        assert!(bad_bench.validate().is_err());
    }
}

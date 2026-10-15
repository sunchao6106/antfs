//! `devcluster`:一条命令拉起完整的本地集群 —— 1 个内建 Raft master + 3 个 store 数据节点 + admin web。
//!
//! 全部**进程内**跑在同一个 tokio runtime(单日志流、单 Ctrl-C 收尾),用于本地端到端联调 / 演示 /
//! 给 `bench-cluster` 提供压测目标。数据落 `--dir`(默认 `./.localcluster`,退出不自动删,便于复检)。
//!
//! 参数来源(`crates/devcluster/src/config.rs`):**内置默认 < `devcluster.toml` < 环境变量 < 命令行**。
//! 当前目录下的 `devcluster.toml` 存在就自动读(`--config PATH` 可指定别处),于是本地环境差异
//! (例如"宿主盘偏满,得把 `disk_near_full_ratio` 提到 0.95")可以钉在文件里,不必每次敲 flag。
//!
//! 流程:起 master → 单节点自举等 leader → 起 N 个 store 节点(开启拉复制)→ 等节点注册 →
//! master 建 N 个 RF 分区(经心跳散布到各节点)→ 起 admin → 打印各服务地址 → 常驻等 Ctrl-C。

mod config;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use antfs_config::Validate;
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{CreatePartitionsRequest, ListPartitionsRequest};
use antfs_master::{MasterConfig, MasterNode};
use antfs_store::{StoreServer, StoreServerConfig};
use config::DevClusterConfig;
use openraft::BasicNode;

const SECRET: &str = "local-dev-secret";
const OPERATOR_TOKEN: &str = "operator";
const VIEWER_TOKEN: &str = "viewer";
/// store 节点 id 序列:`101, 102, ...`(数据目录/端口按序推导)。
fn store_node_ids(count: u32) -> Vec<u64> {
    (0..count as u64).map(|i| 101 + i).collect()
}

/// 解析命令行:`--config`(若有)先铺底,其余 flag 在其上覆盖,最后统一校验。
fn parse_args(argv: &[String]) -> std::result::Result<(DevClusterConfig, Option<PathBuf>), String> {
    // 先单独扫一遍 `--config`:它决定"铺底"读哪个文件,必须早于其它 flag 生效。
    let mut explicit = None;
    let mut scan = argv.iter();
    while let Some(a) = scan.next() {
        if a == "--config" {
            explicit = Some(PathBuf::from(
                scan.next()
                    .ok_or_else(|| "--config requires a value".to_string())?,
            ));
        }
    }
    let (mut a, file) =
        config::load(explicit.as_deref()).map_err(|e| format!("failed to load config: {e}"))?;

    let mut it = argv.iter();
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{flag} requires a value"));
        match flag.as_str() {
            // 已在上面读过并铺底,这里只吃掉它的值。
            "--config" => {
                val()?;
            }
            "--dir" => a.dir = PathBuf::from(val()?),
            "--partitions" => a.partitions = num(flag, val()?)?,
            "--rf" => a.replication_factor = num(flag, val()?)?,
            "--master-grpc-port" => a.master_grpc_port = num(flag, val()?)?,
            "--master-raft-port" => a.master_raft_port = num(flag, val()?)?,
            "--store-base-port" => a.store_base_port = num(flag, val()?)?,
            "--admin-port" => a.admin_port = num(flag, val()?)?,
            "--store-count" => a.store_count = num(flag, val()?)?,
            "--disk-near-full-ratio" => a.disk_near_full_ratio = num(flag, val()?)?,
            "--partition-capacity-bytes" => a.partition_capacity_bytes = num(flag, val()?)?,
            "--disk-health" => a.disk_health = true,
            "--frontend-port" => a.frontend_port = num(flag, val()?)?,
            "--s3-port" => a.s3_port = num(flag, val()?)?,
            "--s3-metastore-url" => a.s3_metastore_url = Some(val()?.clone()),
            "--host" => a.host = val()?.clone(),
            "--run-bench" => a.run_bench = Some(val()?.clone()),
            "--bench-ops" => a.bench_ops = num(flag, val()?)?,
            "--bench-concurrency" => a.bench_concurrency = num(flag, val()?)?,
            "--bench-size" => a.bench_size = num(flag, val()?)?,
            "--bench-read-ratio" => a.bench_read_ratio = num(flag, val()?)?,
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                print_help();
                return Err(format!("unknown flag: {other}"));
            }
        }
    }
    a.validate().map_err(|e| e.to_string())?;
    Ok((a, file))
}

/// 数值解析:报错要指出哪个 flag、给的是什么值(配置文件与命令行走同一条报错口径)。
fn num<T: std::str::FromStr>(flag: &str, raw: &str) -> std::result::Result<T, String> {
    raw.parse()
        .map_err(|_| format!("invalid value `{raw}` for {flag}"))
}

fn print_help() {
    eprintln!(
        "devcluster — one-command local cluster: master + 3x store + admin web\n\
         \n\
         Config sources (later overrides earlier): built-in defaults < config file < AMBRY_* env vars < command line.\n\
         See `devcluster.toml.example` at the repo root for a sample.\n\
         \n\
         --config PATH          config file path (default: ./devcluster.toml in the current dir, read if present)\n\
         --dir PATH             data/log root directory (default ./.localcluster, kept after exit)\n\
         --partitions N         target partition count (default 3; idempotent — no new partitions once at least that many exist)\n\
         --rf N                 replicas per partition (default 3; must not exceed the store node count)\n\
         --master-grpc-port P   master control-plane gRPC port (default 7101)\n\
         --master-raft-port P   master Raft internal RPC port (default 7100)\n\
         --store-base-port P    first store node HTTP port, then +0/+1/+2 (default 7200)\n\
         --admin-port P         admin web port (default 7300)\n\
         --store-count N        store node count (default 3; >=4 to exercise move/balance and source removal under RF=3)\n\
         --disk-near-full-ratio F  master near-full threshold (default 0.8; measured on the host's whole disk; lower it\n\
                                to locally reproduce partition-turns-RO; raise to 0.95 if the local disk is too full\n\
                                to place new partitions)\n\
         --partition-capacity-bytes B  partition capacity in bytes (default 64MiB, deliberately small so it fills up\n\
                                locally, handy for verifying 507 / RO transition; raise it for large-object throughput\n\
                                or long-lived data)\n\
         --disk-health          enable store active disk probing (health values in heartbeats, to verify backfill and avoidance)\n\
         --frontend-port P      frontend REST port (default 7400)\n\
         --host ADDR            listen host for the public endpoints (master gRPC / admin / frontend)\n\
                                (default 127.0.0.1; pass 0.0.0.0 inside a container for the -p published ports to reach it)\n\
         --s3-port P            S3 gateway port (default 7500; only when --s3-metastore-url is set)\n\
         --s3-metastore-url U   MetaStore URL (e.g. postgres://user:pass@localhost/antfs_s3)\n\
                                When set, starts the S3 gateway; otherwise S3 is not started\n\
         --run-bench MODE       once the cluster is ready, run one bench round and exit: MODE = router | frontend\n\
                                (in-process bench; numbers are smoke-level only — for real benchmarks run the\n\
                                antfs-bench-* binaries)\n\
         --bench-ops N          bench write ops (default 20000; only with --run-bench)\n\
         --bench-concurrency C  bench concurrency (default 32)\n\
         --bench-size B         bench object size in bytes (default 4096)\n\
         --bench-read-ratio P   bench read-phase ratio in % (default 20)"
    );
}

/// 小段大小引擎配置:本地演示用小 segment,快速滚段、看得到 compaction。
fn dev_store_engine() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 8 << 20,
        index_max_inmem_elements: 100_000,
        io_error_count_to_trigger_shutdown: 0,
        defer_seal_flush: true,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().init();
    // 参数:配置文件(默认 ./devcluster.toml)铺底 + 命令行覆盖 —— 解析失败要看得见原因,不 panic。
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let (args, config_file) = match parse_args(&argv) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("devcluster: {e}");
            std::process::exit(2);
        }
    };
    if let Some(f) = &config_file {
        tracing::info!(file = %f.display(), "loaded devcluster config file");
    }
    std::fs::create_dir_all(&args.dir)?;

    // `grpc_addr` = 集群内部互连地址(恒回环);`*_bind` = 实际监听地址(`--host`,容器里 0.0.0.0)。
    let grpc_addr = format!("127.0.0.1:{}", args.master_grpc_port);
    let grpc_bind = format!("{}:{}", args.host, args.master_grpc_port);
    let raft_addr = format!("127.0.0.1:{}", args.master_raft_port);

    // ---- master(单节点自举,开可写分区所需的健康副本 quorum 由 3 store 提供)----
    let mcfg = MasterConfig {
        node_id: 1,
        raft_addr: raft_addr.clone(),
        grpc_addr: grpc_bind.clone(),
        data_dir: args.dir.join("master").to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 15,
        partition_capacity_bytes: args.partition_capacity_bytes,
        segment_capacity_bytes: 8 << 20,
        disk_near_full_ratio: args.disk_near_full_ratio,
        auto_grow_enabled: false,
        reconcile_interval_secs: 5,
        ..Default::default()
    };
    let mreg = antfs_metric::Registry::new();
    let master = MasterNode::start(mcfg.clone(), &mreg).await?;
    master.serve()?;
    // 仅首次(未初始化)才 bootstrap;重启时集群已在 redb 里初始化,再调 initialize 会被 openraft
    // 拒绝并打一条 ERROR。先查 is_initialized 跳过,启动日志就干净了。
    if !master.raft.is_initialized().await.unwrap_or(false) {
        let members = BTreeMap::from([(1u64, BasicNode::new(raft_addr.clone()))]);
        if let Err(e) = master.initialize(members).await {
            tracing::warn!(error = %e, "master bootstrap initialize failed");
        }
    }
    master
        .raft
        .wait(Some(Duration::from_secs(15)))
        .metrics(|m| m.current_leader == Some(1), "master leader")
        .await?;
    tracing::info!("master leader elected");

    // ---- N 个 store 数据节点(开启拉复制,W/R 靠拉复制收敛)----
    let store_ids = store_node_ids(args.store_count);
    let mut store_addrs = Vec::new();
    let mut store_replicas = Vec::new();
    for (i, node_id) in store_ids.iter().enumerate() {
        let http_addr = format!("127.0.0.1:{}", args.store_base_port + i as u16);
        let data_dir = args.dir.join(format!("store-{node_id}"));
        std::fs::create_dir_all(&data_dir)?;
        let scfg = StoreServerConfig {
            node_id: *node_id,
            data_dirs: vec![data_dir.to_string_lossy().into_owned().into()],
            http_addr: http_addr.clone(),
            partition_capacity_bytes: args.partition_capacity_bytes,
            master_addr: grpc_addr.clone(),
            cluster_secret: SECRET.to_string(),
            heartbeat_interval_secs: 1,
            replication_enabled: true,
            replication_interval_secs: 2,
            flush_interval_secs: 5,
            // dev 环境显式放宽单请求上限(默认 2 MiB 只够小 blob;CLAUDE.md §5.1 不改默认值,靠这里注入)。
            max_body_bytes: 16 << 20,
            disk_health_enabled: args.disk_health,
            ..Default::default()
        };
        let sreg = antfs_metric::Registry::new();
        let server = StoreServer::start_with(scfg, dev_store_engine(), &sreg)?;
        store_replicas.push(server.replicas());
        server.serve().await?;
        // 泄漏 server 句柄使其在进程存活期间常驻(单命令 dev 工具,退出即回收)。
        Box::leak(Box::new(server));
        store_addrs.push(http_addr);
    }
    tracing::info!(
        count = store_ids.len(),
        "store nodes started, waiting for cluster registration"
    );

    // 等三节点全部注册(经心跳上报盘余量,选盘才有候选)。
    let sm = master.sm_data.clone();
    wait_until(Duration::from_secs(20), || {
        let g = sm.read().expect("sm poisoned");
        store_ids.iter().all(|id| g.state.nodes.contains_key(id))
    })
    .await;

    // ---- 建分区:**补到** `partitions` 个(幂等)----
    // 数据目录是保留的,而 `--partitions` 的语义是"最终至少有这么多分区":直接按 count 建会在**复跑**时
    // 每次再叠 N 个(本地 3 → 6 → 9 …,与"把本地分区数调低"正好相反)。故先问已有多少,只补差额;
    // 补完再问一次,横幅与"应有副本数"都按**总数**算(复跑时本次没建、但总数并不为 0)。
    let mut mc = MasterClient::connect(format!("http://{grpc_addr}")).await?;
    let existing = list_partition_ids(&mut mc).await?;
    let missing = args.partitions.saturating_sub(existing.len() as u32);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut created = Vec::new();
    let mut last_err = String::new();
    while missing > 0 && Instant::now() < deadline {
        let resp = mc
            .create_partitions(CreatePartitionsRequest {
                version: 1,
                cluster_secret: SECRET.into(),
                count: missing,
                replication_factor: args.replication_factor,
            })
            .await?
            .into_inner();
        if resp.ok {
            created = resp.partition_ids;
            break;
        }
        last_err = resp.error;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // 建不出来不能只留一句 `count=0`:master 拒的原因(选盘没有候选 / 非 leader / 盘都判不可用)
    // 必须看得见 —— 否则集群照样打印"已就绪",直到第一个 PUT 才暴露"一个可写分区都没有"。
    if created.len() < missing as usize && !last_err.is_empty() {
        tracing::error!(
            missing,
            error = %last_err,
            "create partitions failed"
        );
    }
    let all = list_partition_ids(&mut mc).await?;
    tracing::info!(
        existing = existing.len(),
        created = created.len(),
        total = all.len(),
        "partitions ensured: {all:?}"
    );

    // ---- 等副本真的建出来再报"就绪" ----
    // master 经心跳下发 transition、store 才 `ensure_replica`;不等就会出现"客户端首个 PUT 404"的假故障。
    // 判据:各 store 的本地副本总数 ≥ 分区总数 × RF(放置已确定的应有总数)。
    let expected: usize = all.len() * args.replication_factor as usize;
    let ready = wait_until(Duration::from_secs(30), || {
        store_replicas.iter().map(|r| r.count()).sum::<usize>() >= expected
    })
    .await;
    let got: usize = store_replicas.iter().map(|r| r.count()).sum();
    if ready {
        tracing::info!(replicas = got, expected, "all replicas created");
    } else {
        tracing::warn!(
            replicas = got,
            expected,
            "replicas not fully created within timeout"
        );
    }

    // ---- admin web ----
    let areg = antfs_metric::Registry::new();
    let ametrics = Arc::new(antfs_admin::AdminMetrics::new(&areg)?);
    let amaster = Arc::new(
        antfs_admin::master_api::GrpcMasterApi::connect(&grpc_addr, SECRET.to_string()).await?,
    );
    let astate = antfs_admin::server::AppState {
        master: amaster,
        metrics: ametrics,
        registry: areg,
        grafana_url: String::new(),
        // 审计落 `--dir/admin-audit.jsonl`(每次跑 devcluster 追加;`AuditLog::open` 会打印路径)。
        audit: Arc::new(antfs_admin::audit::AuditLog::open(
            args.dir.join("admin-audit.jsonl"),
        )),
        scrub_runs: std::sync::Arc::new(antfs_admin::scrub::ScrubRuns::default()),
    };
    let aauth = antfs_admin::server::AuthConfig {
        operator_token: OPERATOR_TOKEN.to_string(),
        viewer_token: VIEWER_TOKEN.to_string(),
        ..Default::default()
    };
    let arouter = antfs_admin::server::build_router(astate, aauth);
    let admin_addr = format!("127.0.0.1:{}", args.admin_port);
    let admin_bind = format!("{}:{}", args.host, args.admin_port);
    let listener = tokio::net::TcpListener::bind(&admin_bind).await?;
    tokio::spawn(async move {
        // 带 connect info 以记录审计的 remote_addr。
        let _ = axum::serve(
            listener,
            arouter.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    // ---- frontend(HTTP REST 入口,内嵌同一个 router SDK)----
    // 装配见 `start_frontend`:与 admin 一样在进程内起一个 axum 服务,把 REST 请求翻译成 SDK 调用。
    let frontend_addr = start_frontend(&args, &grpc_addr).await;

    // ---- S3 gateway(可选;仅当 --s3-metastore-url 给出时)----
    let s3_addr = start_s3(&args, &grpc_addr).await;

    print_summary(
        &args,
        &Banner {
            grpc_addr: &grpc_addr,
            store_ids: &store_ids,
            store_addrs: &store_addrs,
            admin_addr: &admin_addr,
            frontend_addr: frontend_addr.as_deref(),
            s3_addr: s3_addr.as_deref(),
            partitions: &all,
            config_file: config_file.as_deref(),
        },
    );

    // ---- 可选:起完就压一轮(--run-bench)----
    if let Some(mode) = args.run_bench.as_deref() {
        println!("--------------------------------------------------------");
        println!(
            "  --run-bench {mode}:在**本进程内**跑一轮压测(客户端与被测服务共享同一份 CPU/盘 IO,"
        );
        println!(
            "  数字只作冒烟参考;正式压测请另起进程跑 antfs-bench-router / antfs-bench-frontend)"
        );
        let result = match mode {
            "router" => {
                antfs_bench_router::run(antfs_bench_router::Cfg {
                    master_addr: grpc_addr.clone(),
                    cluster_secret: SECRET.to_string(),
                    ops: args.bench_ops,
                    concurrency: args.bench_concurrency,
                    size: args.bench_size,
                    read_ratio: args.bench_read_ratio,
                    verify: true, // 冒烟必带写读对拍:证明链路无静默损坏
                    ..Default::default()
                })
                .await
            }
            _ => match frontend_addr.as_deref() {
                Some(fa) => {
                    antfs_bench_frontend::run(antfs_bench_frontend::Cfg {
                        base: format!("http://{fa}"),
                        ops: args.bench_ops,
                        concurrency: args.bench_concurrency,
                        size: args.bench_size,
                        read_ratio: args.bench_read_ratio,
                        verify: true,
                        ..Default::default()
                    })
                    .await
                }
                None => Err(
                    "frontend not started (port busy?); cannot run the frontend bench".to_string(),
                ),
            },
        };
        match result {
            Ok(()) => {
                println!("  --run-bench {mode} finished, exiting (cluster ends with this process)");
                return Ok(());
            }
            Err(e) => {
                eprintln!("  --run-bench {mode} failed: {e}");
                std::process::exit(1);
            }
        }
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down devcluster");
    Ok(())
}

/// 起 frontend(HTTP REST 入口):内嵌同一个 router SDK,把 REST 请求翻译成 SDK 调用。
///
/// 失败只记 warn 并返回 `None` —— devcluster 的主职责是 master+store 起得来,REST 入口缺了不影响直连模式联调。
async fn start_frontend(args: &DevClusterConfig, grpc_addr: &str) -> Option<String> {
    let cfg = antfs_frontend::config::FrontendConfig {
        listen_addr: format!("{}:{}", args.host, args.frontend_port),
        // dev 环境放宽(默认 4 MiB);与 store 侧同因、同值。自动分片走默认(开启,4 MiB 片)。
        max_body_bytes: 16 << 20,
        router: antfs_router::RouterConfig {
            master_addr: grpc_addr.to_string(),
            cluster_secret: SECRET.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let state = match antfs_frontend::server::AppState::from_config(&cfg, &registry).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "frontend start failed");
            return None;
        }
    };
    let router = antfs_frontend::server::build_router(state);
    let listener = match tokio::net::TcpListener::bind(&cfg.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, addr = %cfg.listen_addr, "frontend bind failed");
            return None;
        }
    };
    let addr = cfg.listen_addr.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    tracing::info!(addr = %addr, "frontend listening");
    Some(addr)
}

/// 起 S3 gateway(可选;仅当 `--s3-metastore-url` 给出时)。
///
/// 与 [`start_frontend`] 同型:连 MetaStore → 连 router SDK → 组 `AppState` → 起 axum 服务。
/// 失败只记 warn 并返回 `None` —— S3 是 devcluster 的附加入口,缺了不影响 master+store 主职责。
async fn start_s3(args: &DevClusterConfig, grpc_addr: &str) -> Option<String> {
    let url = args.s3_metastore_url.as_deref()?;

    let cfg = antfs_s3::config::S3GatewayConfig {
        listen_addr: format!("{}:{}", args.host, args.s3_port),
        region: "us-east-1".to_string(),
        metastore: antfs_s3::config::MetaStoreConfig {
            driver: if url.starts_with("mysql") {
                "mysql".to_string()
            } else {
                "postgres".to_string()
            },
            url: url.to_string(),
            max_connections: 10,
        },
        router: antfs_router::RouterConfig {
            master_addr: grpc_addr.to_string(),
            cluster_secret: SECRET.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    let registry = antfs_metric::Registry::new();
    let metastore = match antfs_s3::metastore::connect(&cfg.metastore).await {
        Ok(ms) => ms,
        Err(e) => {
            tracing::warn!(error = %e, "S3 gateway: metastore connect failed");
            return None;
        }
    };
    let router = match antfs_router::Router::connect(cfg.router.clone(), &registry).await {
        Ok(r) => Arc::new(r),
        Err(e) => {
            tracing::warn!(error = %e, "S3 gateway: router connect failed");
            return None;
        }
    };
    let metrics = match antfs_s3::metrics::S3Metrics::new(&registry) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            tracing::warn!(error = %e, "S3 gateway: metrics registration failed");
            return None;
        }
    };
    let state = antfs_s3::server::AppState {
        router,
        metastore,
        metrics,
        registry,
        config: Arc::new(cfg),
    };
    // 后台 GC(软删行保留期清理 + 孤儿 multipart TTL;默认 config 即启用)。
    antfs_s3::gc::spawn(state.clone(), antfs_s3::config::GcConfig::default());
    let app = antfs_s3::server::build_router(state);
    let addr = format!("{}:{}", args.host, args.s3_port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, addr = %addr, "S3 gateway bind failed");
            return None;
        }
    };
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tracing::info!(addr = %addr, "S3 gateway listening");
    Some(addr)
}

/// 问 master 要**全量**分区 id(按 pid 升序;`limit = 0` = 不分页)。
///
/// 建分区前用它算差额、建完用它出"总数" —— 这样复跑不叠加(理由见调用处注释)。
async fn list_partition_ids(
    mc: &mut MasterClient<tonic::transport::Channel>,
) -> Result<Vec<u32>, Box<dyn std::error::Error + Send + Sync>> {
    let resp = mc
        .list_partitions(ListPartitionsRequest {
            version: 1,
            offset: 0,
            limit: 0,
            state: String::new(),
        })
        .await?
        .into_inner();
    Ok(resp
        .partitions
        .into_iter()
        .map(|p| p.partition_id)
        .collect())
}

async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cond()
}

/// 启动横幅要打印的东西:起完集群后的**实际布局**(各服务地址 + 真建成的分区)与参数来源。
///
/// 收成一个结构体只为避开 clippy 的"参数过多"上限,同时让调用点读起来是"一份布局"而非一串位置参数。
#[derive(Clone, Copy)]
struct Banner<'a> {
    grpc_addr: &'a str,
    store_ids: &'a [u64],
    store_addrs: &'a [String],
    admin_addr: &'a str,
    frontend_addr: Option<&'a str>,
    s3_addr: Option<&'a str>,
    partitions: &'a [u32],
    config_file: Option<&'a std::path::Path>,
}

fn print_summary(args: &DevClusterConfig, banner: &Banner) {
    let Banner {
        grpc_addr,
        store_ids,
        store_addrs,
        admin_addr,
        frontend_addr,
        s3_addr,
        partitions,
        config_file,
    } = *banner;
    println!("\n========================================================");
    println!(
        "  antfs local cluster ready (data dir {})",
        args.dir.display()
    );
    println!(
        "  config source  : {}",
        match config_file {
            Some(p) => format!("{} (plus command-line overrides)", p.display()),
            None => "built-in defaults (no devcluster.toml)".to_string(),
        }
    );
    println!("--------------------------------------------------------");
    println!("  admin web     : http://{admin_addr}");
    println!("                  operator token = {OPERATOR_TOKEN} (read-write)");
    println!("                  viewer   token = {VIEWER_TOKEN} (read-only)");
    println!("  master gRPC   : {grpc_addr}");
    if let Some(fa) = frontend_addr {
        println!("  frontend REST : http://{fa}");
    }
    if let Some(sa) = s3_addr {
        println!("  S3 gateway    : http://{sa}");
    }
    for (i, addr) in store_addrs.iter().enumerate() {
        println!(
            "  store node {}  : http://{addr}  (node_id={})",
            i + 1,
            store_ids[i]
        );
    }
    println!(
        "  partitions    : {} (RF={}) {partitions:?}",
        partitions.len(),
        args.replication_factor
    );
    println!("--------------------------------------------------------");
    println!("  end-to-end test (direct router SDK: lease id ranges from master, then talk to stores directly)");
    println!("    cargo run -p antfs-bench-router --release -- --master {grpc_addr} --secret {SECRET} \\");
    println!("      --ops 20000 --concurrency 32 --size 4096 --read-ratio 20 --verify");
    println!("    # single-object smoke: --ops 1 --size 1048576");
    if let Some(fa) = frontend_addr {
        println!("  REST path (via frontend; any HTTP client works)");
        println!(
            "    # note: `curl -T` sends PUT; uploads must use POST (see the contract table in docs/design/frontend.md)"
        );
        println!("    curl -X POST --data-binary @/tmp/a.bin http://{fa}/blobs -i   # 201 + Location: /blobs/<blobId>");
        println!("    curl http://{fa}/blobs/<blobId> -o /tmp/b.bin && cmp /tmp/a.bin /tmp/b.bin");
        println!(
            "    curl -I http://{fa}/blobs/<blobId>             # metadata headers (no body read)"
        );
        println!("    curl -X DELETE http://{fa}/blobs/<blobId> -i   # 204");
        println!("    curl -X POST --data-binary @/tmp/a.bin -H 'x-antfs-ttl-ms: 1780000000000' http://{fa}/blobs -i   # 201; TTL is write-time only");
        println!("    cargo run -p antfs-bench-frontend --release -- --frontend http://{fa} \\");
        println!("      --ops 20000 --concurrency 32 --size 4096 --read-ratio 20");
    }
    if let Some(pid) = partitions.first() {
        let key = "0f".repeat(8); // key 为 8 字节 → 16 hex
        println!("  raw data-plane example (bypasses the router, hits a single store; W/R is the client's job — **not for production use**)");
        println!(
            "    curl -X PUT --data-binary hello http://{}/partitions/{pid}/blobs/{key}",
            store_addrs[0]
        );
        println!(
            "    curl http://{}/partitions/{pid}/blobs/{key}",
            store_addrs[0]
        );
    }
    println!("  engine-level bench (standalone; does not stress this cluster):");
    println!("    cargo run -p antfs-bench-blob --release -- --ops 100000 --value-size 4096");
    println!("========================================================");
    println!("  Ctrl-C to stop.\n");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn cli_flags_win_over_the_config_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("devcluster.toml");
        std::fs::write(&path, "partitions = 2\nadmin_port = 7500\n").unwrap();

        let (a, file) = parse_args(&argv(&[
            "--config",
            path.to_str().unwrap(),
            "--partitions",
            "5",
        ]))
        .unwrap();
        assert_eq!(
            file.as_deref(),
            Some(path.as_path()),
            "要能回答用的是哪个文件"
        );
        assert_eq!(a.partitions, 5, "命令行覆盖文件");
        assert_eq!(a.admin_port, 7500, "文件里没被命令行覆盖的键照样生效");
    }

    #[test]
    fn a_bad_cli_value_names_the_flag_instead_of_panicking() {
        let err = parse_args(&argv(&["--partitions", "many"])).unwrap_err();
        assert!(
            err.contains("--partitions") && err.contains("many"),
            "{err}"
        );

        let err = parse_args(&argv(&["--config"])).unwrap_err();
        assert!(err.contains("--config"), "{err}");

        let err = parse_args(&argv(&["--nope"])).unwrap_err();
        assert!(err.contains("--nope"), "{err}");
    }

    #[test]
    fn validation_runs_after_the_cli_overrides() {
        // 文件本身合法、命令行把它改成放不下 → 仍要在解析期拒掉,而不是等 20s 选盘失败。
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("devcluster.toml");
        std::fs::write(&path, "store_count = 3\n").unwrap();
        let err =
            parse_args(&argv(&["--config", path.to_str().unwrap(), "--rf", "5"])).unwrap_err();
        assert!(err.contains("replication_factor"), "{err}");
    }
}

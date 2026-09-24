//! `devcluster`:一条命令拉起完整的本地集群 —— 1 个内建 Raft master + 3 个 store 数据节点 + admin web。
//!
//! 全部**进程内**跑在同一个 tokio runtime(单日志流、单 Ctrl-C 收尾),用于本地端到端联调 / 演示 /
//! 给 `bench-cluster` 提供压测目标。数据落 `--dir`(默认 `./.localcluster`,退出不自动删,便于复检)。
//!
//! 流程:起 master → 单节点自举等 leader → 起 3 个 store 节点(开启拉复制)→ 等三节点注册 →
//! master 建 N 个 RF 分区(经心跳散布到各节点)→ 起 admin → 打印各服务地址 → 常驻等 Ctrl-C。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::CreatePartitionsRequest;
use antfs_master::{MasterConfig, MasterNode};
use antfs_store::{StoreServer, StoreServerConfig};
use openraft::BasicNode;

const SECRET: &str = "local-dev-secret";
const OPERATOR_TOKEN: &str = "operator";
const VIEWER_TOKEN: &str = "viewer";
/// store 节点 id 序列:`101, 102, ...`(数据目录/端口按序推导)。
fn store_node_ids(count: u32) -> Vec<u64> {
    (0..count as u64).map(|i| 101 + i).collect()
}

struct Args {
    dir: std::path::PathBuf,
    partitions: u32,
    replication_factor: u32,
    master_grpc_port: u16,
    master_raft_port: u16,
    store_base_port: u16,
    admin_port: u16,
    /// store 节点数(默认 3;≥4 才能在 RF=3 时真正执行 move/均衡——devcluster 用于验证搬迁与删源)。
    store_count: u32,
    /// master 的 `disk_near_full_ratio`(默认 0.8;调小可在本地复现"盘将满 → 分区转 RO")。
    disk_near_full_ratio: f64,
    /// store 是否开启主动盘探测(`disk_health_enabled`,默认 false;开启后心跳会带 HEALTHY/... 健康值)。
    disk_health: bool,
    frontend_port: u16,
    /// 对外服务的监听 host(`--host`,默认回环)。容器里传 `0.0.0.0` 才能被 `-p` 发布的端口访问。
    /// 只影响**监听**地址:集群内部互连地址恒为 `127.0.0.1`(master/store/admin/frontend 同进程)。
    host: String,
    /// `--run-bench router|frontend`:起完集群(等就绪)后在**同一进程**内跑一轮压测并退出。
    /// 客户端与被测服务共享同一份 CPU/盘 IO,数字只作冒烟参考 —— 正式压测请另起进程跑 bench 二进制。
    run_bench: Option<String>,
    bench_ops: u64,
    bench_concurrency: u64,
    bench_size: usize,
    bench_read_ratio: u64,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            dir: std::path::PathBuf::from("./.localcluster"),
            partitions: 6,
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

fn parse_args() -> Args {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().expect("missing value for flag");
        match flag.as_str() {
            "--dir" => a.dir = std::path::PathBuf::from(val()),
            "--partitions" => a.partitions = val().parse().expect("partitions"),
            "--rf" => a.replication_factor = val().parse().expect("rf"),
            "--master-grpc-port" => a.master_grpc_port = val().parse().expect("port"),
            "--master-raft-port" => a.master_raft_port = val().parse().expect("port"),
            "--store-base-port" => a.store_base_port = val().parse().expect("port"),
            "--admin-port" => a.admin_port = val().parse().expect("port"),
            "--store-count" => a.store_count = val().parse::<u32>().expect("store-count").max(1),
            "--disk-near-full-ratio" => {
                a.disk_near_full_ratio = val().parse().expect("disk-near-full-ratio")
            }
            "--disk-health" => a.disk_health = true,
            "--frontend-port" => a.frontend_port = val().parse().expect("port"),
            "--host" => a.host = val(),
            "--run-bench" => {
                let mode = val();
                if mode != "router" && mode != "frontend" {
                    eprintln!("--run-bench expects `router` or `frontend`, got `{mode}`");
                    std::process::exit(2);
                }
                a.run_bench = Some(mode);
            }
            "--bench-ops" => a.bench_ops = val().parse().expect("bench-ops"),
            "--bench-concurrency" => {
                a.bench_concurrency = val().parse::<u64>().expect("bench-concurrency").max(1)
            }
            "--bench-size" => a.bench_size = val().parse().expect("bench-size"),
            "--bench-read-ratio" => a.bench_read_ratio = val().parse().expect("bench-read-ratio"),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                print_help();
                std::process::exit(2);
            }
        }
    }
    a
}

fn print_help() {
    eprintln!(
        "devcluster — 本地 master + 3×store + admin 一键起集群\n\
         \n\
         --dir PATH             数据/日志根目录 (default ./.localcluster,退出不删)\n\
         --partitions N         启动后创建的分区数 (default 6)\n\
         --rf N                 每分区副本数 (default 3)\n\
         --master-grpc-port P   master 控制面 gRPC 端口 (default 7101)\n\
         --master-raft-port P   master Raft 内部 RPC 端口 (default 7100)\n\
         --store-base-port P    store 节点 HTTP 起始端口,依次 +0/+1/+2 (default 7200)\n\
         --admin-port P         admin web 端口 (default 7300)\n\
         --store-count N        store 节点数 (default 3;≥4 才能验证 RF=3 下的 move/均衡与删源)\n\
         --disk-near-full-ratio F  master 盘将满阈值 (default 0.95;调小可在本地复现「分区转 RO」)\n\
         --disk-health          开启 store 主动盘探测(心跳带健康值,用于验证健康回填与避让)\n\
         --frontend-port P      frontend REST 端口 (default 7400)\n\
         --host ADDR            对外服务(master gRPC / admin / frontend)的监听 host\n\
                                (default 127.0.0.1;容器里传 0.0.0.0 才能被 -p 发布的端口访问)\n\
         --run-bench MODE       起完集群(等就绪)后跑一轮压测并退出:MODE = router | frontend\n\
                                (同进程压测,数字只作冒烟参考;正式压测请跑 antfs-bench-* 二进制)\n\
         --bench-ops N          压测写操作数 (default 20000;仅 --run-bench 时用)\n\
         --bench-concurrency C  压测并发数 (default 32)\n\
         --bench-size B         压测对象字节数 (default 4096)\n\
         --bench-read-ratio P   压测读阶段占比 % (default 20)"
    );
}

/// 小段大小引擎配置:本地演示用小 segment,快速滚段、看得到 compaction。
fn dev_store_engine() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 8 << 20,
        index_max_inmem_elements: 100_000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().init();
    let args = parse_args();
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
        partition_capacity_bytes: 64 << 20,
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
            partition_capacity_bytes: 64 << 20,
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

    // ---- 建分区(重试到选盘成功:心跳需先把盘余量报上来)----
    let mut mc = MasterClient::connect(format!("http://{grpc_addr}")).await?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut created = Vec::new();
    while Instant::now() < deadline {
        let resp = mc
            .create_partitions(CreatePartitionsRequest {
                version: 1,
                cluster_secret: SECRET.into(),
                count: args.partitions,
                replication_factor: args.replication_factor,
            })
            .await?
            .into_inner();
        if resp.ok {
            created = resp.partition_ids;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tracing::info!(count = created.len(), "partitions created: {created:?}");

    // ---- 等副本真的建出来再报"就绪" ----
    // master 经心跳下发 transition、store 才 `ensure_replica`;不等就会出现"客户端首个 PUT 404"的假故障。
    // 判据:各 store 的本地副本总数 ≥ 分区数 × RF(放置已确定的应有总数)。
    let expected: usize = created.len() * args.replication_factor as usize;
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

    print_summary(
        &args,
        &grpc_addr,
        &store_ids,
        &store_addrs,
        &admin_addr,
        frontend_addr.as_deref(),
        &created,
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
                None => Err("frontend 未启动(端口被占?),无法跑 frontend 压测".to_string()),
            },
        };
        match result {
            Ok(()) => {
                println!("  --run-bench {mode} 完成,退出(集群随本进程结束)");
                return Ok(());
            }
            Err(e) => {
                eprintln!("  --run-bench {mode} 失败: {e}");
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
async fn start_frontend(args: &Args, grpc_addr: &str) -> Option<String> {
    let cfg = antfs_frontend::config::FrontendConfig {
        listen_addr: format!("{}:{}", args.host, args.frontend_port),
        // dev 环境放宽(默认 2 MiB);与 store 侧同因、同值。
        max_body_bytes: 16 << 20,
        cors_allowed_origins: Vec::new(),
        router: antfs_router::RouterConfig {
            master_addr: grpc_addr.to_string(),
            cluster_secret: SECRET.to_string(),
            ..Default::default()
        },
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

fn print_summary(
    args: &Args,
    grpc_addr: &str,
    store_ids: &[u64],
    store_addrs: &[String],
    admin_addr: &str,
    frontend_addr: Option<&str>,
    partitions: &[u32],
) {
    println!("\n========================================================");
    println!("  antfs 本地集群已就绪 (数据目录 {})", args.dir.display());
    println!("--------------------------------------------------------");
    println!("  admin web     : http://{admin_addr}");
    println!("                  operator token = {OPERATOR_TOKEN} (读写)");
    println!("                  viewer   token = {VIEWER_TOKEN} (只读)");
    println!("  master gRPC   : {grpc_addr}");
    if let Some(fa) = frontend_addr {
        println!("  frontend REST : http://{fa}");
    }
    for (i, addr) in store_addrs.iter().enumerate() {
        println!(
            "  store node {}  : http://{addr}  (node_id={})",
            i + 1,
            store_ids[i]
        );
    }
    println!(
        "  partitions    : {} 个 (RF={}) {partitions:?}",
        partitions.len(),
        args.replication_factor
    );
    println!("--------------------------------------------------------");
    println!("  端到端联调(直连 router SDK:自己向 master 取号段再直连 store)");
    println!("    cargo run -p antfs-bench-router --release -- --master {grpc_addr} --secret {SECRET} \\");
    println!("      --ops 20000 --concurrency 32 --size 4096 --read-ratio 20 --verify");
    println!("    # 单对象冒烟:--ops 1 --size 1048576");
    if let Some(fa) = frontend_addr {
        println!("  REST 路径(经 frontend;任何 HTTP 客户端都能用)");
        println!(
            "    # 注意:`curl -T` 发的是 PUT,上传要用 POST(见 docs/design/frontend.md 的契约表)"
        );
        println!("    curl -X POST --data-binary @/tmp/a.bin http://{fa}/blobs -i   # 201 + Location: /blobs/<blobId>");
        println!("    curl http://{fa}/blobs/<blobId> -o /tmp/b.bin && cmp /tmp/a.bin /tmp/b.bin");
        println!("    curl -I http://{fa}/blobs/<blobId>             # 元数据头(不读数据体)");
        println!("    curl -X DELETE http://{fa}/blobs/<blobId> -i   # 204");
        println!("    curl -X POST --data-binary @/tmp/a.bin -H 'x-ambry-ttl-ms: 1780000000000' http://{fa}/blobs -i   # 201;TTL 只能在写入时定");
        println!("    cargo run -p antfs-bench-frontend --release -- --frontend http://{fa} \\");
        println!("      --ops 20000 --concurrency 32 --size 4096 --read-ratio 20");
    }
    if let Some(pid) = partitions.first() {
        let key = "0f".repeat(8); // key 为 8 字节 → 16 hex
        println!("  裸数据面示例(绕过 router 只打单个 store;W/R 由 client 负责,**别当生产用法**)");
        println!(
            "    curl -X PUT --data-binary hello http://{}/partitions/{pid}/blobs/{key}",
            store_addrs[0]
        );
        println!(
            "    curl http://{}/partitions/{pid}/blobs/{key}",
            store_addrs[0]
        );
    }
    println!("  引擎层压测(独立跑,不压本集群):");
    println!("    cargo run -p antfs-bench-blob --release -- --ops 100000 --value-size 4096");
    println!("========================================================");
    println!("  Ctrl-C 停止。\n");
}

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
use openraft::BasicNode;
use antfs_store::{StoreServer, StoreServerConfig};

const SECRET: &str = "local-dev-secret";
const OPERATOR_TOKEN: &str = "operator";
const VIEWER_TOKEN: &str = "viewer";
const STORE_NODE_IDS: [u64; 3] = [101, 102, 103];

struct Args {
    dir: std::path::PathBuf,
    partitions: u32,
    replication_factor: u32,
    master_grpc_port: u16,
    master_raft_port: u16,
    store_base_port: u16,
    admin_port: u16,
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
         --admin-port P         admin web 端口 (default 7300)"
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

    let grpc_addr = format!("127.0.0.1:{}", args.master_grpc_port);
    let raft_addr = format!("127.0.0.1:{}", args.master_raft_port);

    // ---- master(单节点自举,开可写分区所需的健康副本 quorum 由 3 store 提供)----
    let mcfg = MasterConfig {
        node_id: 1,
        raft_addr: raft_addr.clone(),
        grpc_addr: grpc_addr.clone(),
        data_dir: args.dir.join("master").to_string_lossy().into_owned(),
        initial_members: vec![1],
        cluster_secret: SECRET.to_string(),
        heartbeat_timeout_secs: 15,
        partition_capacity_bytes: 64 << 20,
        disk_near_full_ratio: 0.95,
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

    // ---- 3 个 store 数据节点(开启拉复制,W=2/R=1 靠拉复制收敛)----
    let mut store_addrs = Vec::new();
    for (i, node_id) in STORE_NODE_IDS.iter().enumerate() {
        let http_addr = format!("127.0.0.1:{}", args.store_base_port + i as u16);
        let data_dir = args.dir.join(format!("store-{node_id}"));
        std::fs::create_dir_all(&data_dir)?;
        let scfg = StoreServerConfig {
            node_id: *node_id,
            data_dirs: vec![data_dir.to_string_lossy().into_owned()],
            http_addr: http_addr.clone(),
            partition_capacity_bytes: 64 << 20,
            master_addr: grpc_addr.clone(),
            cluster_secret: SECRET.to_string(),
            heartbeat_interval_secs: 1,
            replication_enabled: true,
            replication_interval_secs: 2,
            flush_interval_secs: 5,
            ..Default::default()
        };
        let sreg = antfs_metric::Registry::new();
        let server = StoreServer::start_with(scfg, dev_store_engine(), &sreg)?;
        server.serve().await?;
        // 泄漏 server 句柄使其在进程存活期间常驻(单命令 dev 工具,退出即回收)。
        Box::leak(Box::new(server));
        store_addrs.push(http_addr);
    }
    tracing::info!("3 store nodes started, waiting for cluster registration");

    // 等三节点全部注册(经心跳上报盘余量,选盘才有候选)。
    let sm = master.sm_data.clone();
    wait_until(Duration::from_secs(20), || {
        let g = sm.read().expect("sm poisoned");
        STORE_NODE_IDS.iter().all(|id| g.state.nodes.contains_key(id))
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

    // ---- admin web ----
    let areg = antfs_metric::Registry::new();
    let ametrics = Arc::new(antfs_admin::AdminMetrics::new(&areg)?);
    let amaster = Arc::new(
        antfs_admin::master_api::GrpcMasterApi::connect(&grpc_addr, SECRET.to_string()).await?,
    );
    let astate = antfs_admin::server::AppState {
        master: amaster,
        metrics: ametrics,
        grafana_url: String::new(),
    };
    let aauth = antfs_admin::server::AuthConfig {
        operator_token: OPERATOR_TOKEN.to_string(),
        viewer_token: VIEWER_TOKEN.to_string(),
    };
    let arouter = antfs_admin::server::build_router(astate, aauth);
    let admin_addr = format!("127.0.0.1:{}", args.admin_port);
    let listener = tokio::net::TcpListener::bind(&admin_addr).await?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, arouter).await;
    });

    print_summary(&args, &grpc_addr, &store_addrs, &admin_addr, &created);
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down devcluster");
    Ok(())
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
    store_addrs: &[String],
    admin_addr: &str,
    partitions: &[u32],
) {
    println!("\n========================================================");
    println!("  antfs 本地集群已就绪 (数据目录 {})", args.dir.display());
    println!("--------------------------------------------------------");
    println!("  admin web     : http://{admin_addr}");
    println!("                  operator token = {OPERATOR_TOKEN} (读写)");
    println!("                  viewer   token = {VIEWER_TOKEN} (只读)");
    println!("  master gRPC   : {grpc_addr}");
    for (i, addr) in store_addrs.iter().enumerate() {
        println!("  store node {}  : http://{addr}  (node_id={})", i + 1, STORE_NODE_IDS[i]);
    }
    println!("  partitions    : {} 个 (RF={}) {partitions:?}", partitions.len(), args.replication_factor);
    if let Some(pid) = partitions.first() {
        let key = "0f".repeat(32);
        println!("--------------------------------------------------------");
        println!("  数据面读写示例(任一 store 节点,RF 副本靠拉复制收敛):");
        println!("    curl -X PUT --data-binary hello \\");
        println!("      http://{}/partitions/{pid}/blobs/{key}", store_addrs[0]);
        println!("    curl http://{}/partitions/{pid}/blobs/{key}", store_addrs[0]);
        println!("  引擎层压测(独立跑,不压本集群):");
        println!("    cargo run -p antfs-bench-blob --release -- --ops 100000 --value-size 4096");
    }
    println!("========================================================");
    println!("  Ctrl-C 停止。\n");
}

//! **直连模式**压测逻辑(供 `antfs-bench-router` 二进制与 devcluster `--run-bench router` 复用)。
//!
//! 进程内嵌 [`antfs_router::Router`]:自己向 master 取 blobId 号段、直连 store 写读。
//! 被测项 =「Router + master 发号 + 数据面」这条链。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use antfs_bench_common::{pattern_bytes, spread, Phase};
use antfs_router::{Router, RouterConfig, StoreKey};
use bytes::Bytes;

/// 一轮压测的参数。
#[derive(Debug, Clone)]
pub struct Cfg {
    pub master_addr: String,
    pub cluster_secret: String,
    pub request_timeout_secs: u64,
    pub id_segment_size: u32,
    /// 写操作数。
    pub ops: u64,
    /// 并发任务数。
    pub concurrency: u64,
    /// 每个对象字节数。
    pub size: usize,
    /// 读阶段规模 = 写成功数 × 该百分比(0 = 不读)。
    pub read_ratio: u64,
    /// 抽前 64 个对象回读对拍。
    pub verify: bool,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg {
            master_addr: "127.0.0.1:7101".to_string(),
            cluster_secret: String::new(),
            request_timeout_secs: 10,
            id_segment_size: 1024,
            ops: 10_000,
            concurrency: 32,
            size: 4096,
            read_ratio: 0,
            verify: false,
        }
    }
}

/// 错误标签:普通错误用 [`antfs_router::Error::kind`];**`Quorum` 要把逐副本失败原因带出来**
/// (只报 "quorum" 会让排障丢掉真因,如 `store_full` / `silenced`)。
fn err_label(e: &antfs_router::Error) -> String {
    match e {
        antfs_router::Error::Quorum { detail, .. } => format!("quorum[{detail}]"),
        other => other.kind().to_string(),
    }
}

/// 跑一轮:PUT(必须)+ GET(可选)+ verify(可选)。返回 Err = 连不上 master 等致命问题
/// (单次操作失败只计入报表的 `err`,不中断)。
pub async fn run(cfg: Cfg) -> Result<(), String> {
    println!("target: master {} (直连 router SDK)", cfg.master_addr);
    let registry = antfs_metric::Registry::new();
    let router = Router::connect(
        RouterConfig {
            master_addr: cfg.master_addr.clone(),
            cluster_secret: cfg.cluster_secret.clone(),
            request_timeout_secs: cfg.request_timeout_secs,
            id_segment_size: cfg.id_segment_size,
            partition_refresh_secs: 30,
        },
        &registry,
    )
    .await
    .map_err(|e| format!("connect master failed: {e}"))?;
    let router = Arc::new(router);

    let payload = Bytes::from(pattern_bytes(cfg.size));
    let written: Arc<Mutex<Vec<StoreKey>>> =
        Arc::new(Mutex::new(Vec::with_capacity(cfg.ops as usize)));
    let next = AtomicU64::new(0);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..cfg.concurrency {
        let router = router.clone();
        let payload = payload.clone();
        let written = written.clone();
        let (ops, size) = (cfg.ops, cfg.size);
        let next = &next;
        tasks.push(async move {
            let mut phase = Phase::new("PUT", 0);
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= ops {
                    break;
                }
                let t0 = Instant::now();
                let key = match router.allocate_one().await {
                    Ok(k) => k,
                    Err(e) => {
                        phase.record_err(&err_label(&e));
                        continue;
                    }
                };
                match router.put(key, payload.clone()).await {
                    Ok(_) => {
                        phase.record_ok(t0.elapsed().as_nanos() as u64);
                        phase.bytes += size as u64;
                        written.lock().expect("written poisoned").push(key);
                    }
                    Err(e) => phase.record_err(&err_label(&e)),
                }
            }
            phase
        });
    }
    let mut put_phase = Phase::new("PUT", cfg.ops);
    for phase in futures::future::join_all(tasks).await {
        put_phase.merge(phase);
    }
    put_phase.elapsed = started.elapsed();
    put_phase.report();

    let written = Arc::new(written.lock().expect("written poisoned").clone());
    if !written.is_empty() && cfg.read_ratio > 0 {
        let reads = (written.len() as u64 * cfg.read_ratio / 100).max(1);
        let next = AtomicU64::new(0);
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..cfg.concurrency {
            let router = router.clone();
            let written = written.clone();
            let next = &next;
            tasks.push(async move {
                let mut phase = Phase::new("GET", 0);
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= reads {
                        break;
                    }
                    let key = written[spread(i, written.len())];
                    let t0 = Instant::now();
                    match router.get(&key).await {
                        Ok(b) => {
                            phase.record_ok(t0.elapsed().as_nanos() as u64);
                            phase.bytes += b.len() as u64;
                        }
                        Err(e) => phase.record_err(e.kind()),
                    }
                }
                phase
            });
        }
        let mut get_phase = Phase::new("GET", reads);
        for phase in futures::future::join_all(tasks).await {
            get_phase.merge(phase);
        }
        get_phase.elapsed = started.elapsed();
        get_phase.report();
    }

    if cfg.verify {
        // 抽前若干个写回读,逐字节对拍(证明写读端到端没有静默损坏)。
        let mut checked = 0usize;
        for key in written.iter().take(64) {
            match router.get(key).await {
                Ok(b) if b == payload => checked += 1,
                Ok(b) => {
                    return Err(format!(
                        "verify {key:?}: size {} != {}",
                        b.len(),
                        payload.len()
                    ))
                }
                Err(e) => return Err(format!("verify {key:?}: {e}")),
            }
        }
        println!("verify: {checked} blobs byte-identical");
    }
    Ok(())
}

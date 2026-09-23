//! **REST 网关模式**压测逻辑(供 `antfs-bench-frontend` 二进制与 devcluster `--run-bench frontend` 复用)。
//!
//! **只用 HTTP 打 frontend**,不内嵌任何 SDK。被测项 =「frontend 网关 + 取号段 + W 多数派写 + 读」。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use antfs_bench_common::{pattern_bytes, spread, Phase};
use bytes::Bytes;

/// 一轮压测的参数。
#[derive(Debug, Clone)]
pub struct Cfg {
    /// frontend 基地址(如 `http://127.0.0.1:7400`)。
    pub base: String,
    pub timeout_secs: u64,
    pub ops: u64,
    pub concurrency: u64,
    pub size: usize,
    pub read_ratio: u64,
    pub verify: bool,
}

impl Default for Cfg {
    fn default() -> Self {
        Cfg {
            base: "http://127.0.0.1:7400".to_string(),
            timeout_secs: 30,
            ops: 10_000,
            concurrency: 32,
            size: 4096,
            read_ratio: 0,
            verify: false,
        }
    }
}

/// 跑一轮:POST(必须)+ GET(可选)+ verify(可选)。返回 Err = 建不出 HTTP 客户端等致命问题
/// (单次操作失败只计入报表的 `err`,不中断)。
pub async fn run(cfg: Cfg) -> Result<(), String> {
    let base = cfg.base.trim_end_matches('/').to_string();
    println!("target: {base}/blobs (REST 网关)");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()
        .map_err(|e| format!("build http client: {e}"))?;

    let payload = Bytes::from(pattern_bytes(cfg.size));
    let written: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::with_capacity(cfg.ops as usize)));
    let next = AtomicU64::new(0);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..cfg.concurrency {
        let http = http.clone();
        let base = base.clone();
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
                match post_blob(&http, &base, payload.clone()).await {
                    Ok(id) => {
                        phase.record_ok(t0.elapsed().as_nanos() as u64);
                        phase.bytes += size as u64;
                        written.lock().expect("written poisoned").push(id);
                    }
                    Err(kind) => phase.record_err(kind),
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
            let http = http.clone();
            let base = base.clone();
            let written = written.clone();
            let next = &next;
            tasks.push(async move {
                let mut phase = Phase::new("GET", 0);
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= reads {
                        break;
                    }
                    let id = &written[spread(i, written.len())];
                    let t0 = Instant::now();
                    match get_blob(&http, &base, id).await {
                        Ok(n) => {
                            phase.record_ok(t0.elapsed().as_nanos() as u64);
                            phase.bytes += n as u64;
                        }
                        Err(kind) => phase.record_err(kind),
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
        let mut checked = 0usize;
        for id in written.iter().take(64) {
            match get_blob(&http, &base, id).await {
                Ok(n) if n == payload.len() => checked += 1,
                Ok(n) => return Err(format!("verify {id}: size {n} != {}", payload.len())),
                Err(kind) => return Err(format!("verify {id}: {kind}")),
            }
        }
        println!("verify: {checked} blobs byte-identical");
    }
    Ok(())
}

/// `POST /blobs` → 201 时返回 blobId(优先 `x-ambry-blob-id`,退回 `Location` 尾段)。
async fn post_blob(http: &reqwest::Client, base: &str, data: Bytes) -> Result<String, &'static str> {
    let resp = http
        .post(format!("{base}/blobs"))
        .body(data)
        .send()
        .await
        .map_err(|_| "http")?;
    let status = resp.status().as_u16();
    if status != 201 {
        return Err(match status {
            413 => "too_large",
            507 => "store_full",
            503 => "unavailable",
            400 => "bad_request",
            _ => "status",
        });
    }
    if let Some(v) = resp.headers().get("x-ambry-blob-id").and_then(|v| v.to_str().ok()) {
        return Ok(v.to_string());
    }
    let loc = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    loc.rsplit('/').next().filter(|s| !s.is_empty()).map(|s| s.to_string()).ok_or("no_blob_id")
}

/// `GET /blobs/{id}` → 200 时返回字节数。
async fn get_blob(http: &reqwest::Client, base: &str, id: &str) -> Result<usize, &'static str> {
    let resp = http
        .get(format!("{base}/blobs/{id}"))
        .send()
        .await
        .map_err(|_| "http")?;
    let status = resp.status().as_u16();
    if status != 200 {
        return Err(match status {
            404 => "not_found",
            503 => "unavailable",
            502 => "stitch_broken",
            _ => "status",
        });
    }
    resp.bytes().await.map(|b| b.len()).map_err(|_| "http")
}

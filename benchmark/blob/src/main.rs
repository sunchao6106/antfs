//! `bench-blob`:BlobStore 层面的吞吐/延迟压测二进制。
//!
//! 直接压 `blob::BlobStore`(单机引擎),多线程共享 `Arc<BlobStore>`(D4:写方法 `&self`),
//! 分 PUT / GET 两阶段测吞吐(ops/s、MB/s)与延迟分位(p50/p99/max)。
//!
//! 用法:`bench-blob [--ops N] [--value-size B] [--threads T] [--read-ops N]
//!        [--read-threads T] [--index-max N] [--segment-mib M] [--dir PATH]`
//!
//! 后续 `benchmark/` 下会新增数据节点(HTTP 数据面)与 S3 frontend 的压测二进制;本程序只覆盖 blob 引擎层。

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use blob::{BlobStore, StoreKey};

struct Args {
    ops: u64,
    value_size: usize,
    threads: u64,
    read_ops: u64,
    read_threads: u64,
    index_max: usize,
    segment_bytes: u64,
    dir: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        let par = thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(4);
        Args {
            ops: 100_000,
            value_size: 4096,
            threads: par,
            read_ops: 100_000,
            read_threads: par,
            index_max: 100_000,
            segment_bytes: 64 * 1024 * 1024,
            dir: None,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().expect("missing value for flag");
        match flag.as_str() {
            "--ops" => a.ops = val().parse().expect("ops"),
            "--value-size" => a.value_size = val().parse().expect("value-size"),
            "--threads" => a.threads = val().parse::<u64>().expect("threads").max(1),
            "--read-ops" => a.read_ops = val().parse().expect("read-ops"),
            "--read-threads" => a.read_threads = val().parse::<u64>().expect("read-threads").max(1),
            "--index-max" => a.index_max = val().parse::<usize>().expect("index-max").max(1),
            "--segment-mib" => {
                a.segment_bytes = val().parse::<u64>().expect("segment-mib") * 1024 * 1024
            }
            "--dir" => a.dir = Some(PathBuf::from(val())),
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
        "bench-blob — BlobStore 层压测\n\
         \n\
         --ops N           PUT 次数 (default 100000)\n\
         --value-size B    每个 blob 字节数 (default 4096)\n\
         --threads T       PUT 并发线程 (default = CPU 数)\n\
         --read-ops N      GET 次数 (default 100000)\n\
         --read-threads T  GET 并发线程 (default = CPU 数)\n\
         --index-max N     活跃索引封存阈值 (default 100000)\n\
         --segment-mib M   单段大小 MiB (default 64)\n\
         --dir PATH        数据目录 (default 临时目录,跑完删除)"
    );
}

/// 由序号生成 8 字节 key(黄金比奇常数乘法散列;u64 上的双射,保唯一且分布均匀,避免键分布退化)。
fn make_key(n: u64) -> StoreKey {
    StoreKey::new(n.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes())
}

/// xorshift64*,无依赖的确定性伪随机(GET 阶段选 key)。
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

struct Summary {
    label: &'static str,
    ops: u64,
    errors: u64,
    bytes: u64,
    elapsed_s: f64,
    lat: Vec<u64>, // nanos, sorted
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

fn report(s: &Summary) {
    let ops_per_s = s.ops as f64 / s.elapsed_s;
    let mb_per_s = s.bytes as f64 / s.elapsed_s / (1024.0 * 1024.0);
    let avg_us = if s.ops > 0 {
        s.lat.iter().sum::<u64>() as f64 / s.ops as f64 / 1000.0
    } else {
        0.0
    };
    println!(
        "{:>5} | {:>10} ops | {:>8.0} ops/s | {:>8.1} MB/s | avg {:>7.1}us  p50 {:>7.1}us  p99 {:>8.1}us  max {:>8.1}us  err {}",
        s.label,
        s.ops,
        ops_per_s,
        mb_per_s,
        avg_us,
        percentile(&s.lat, 50.0) as f64 / 1000.0,
        percentile(&s.lat, 99.0) as f64 / 1000.0,
        percentile(&s.lat, 100.0) as f64 / 1000.0,
        s.errors,
    );
}

/// 把区间 `[0, total)` 按线程数近似均分,返回每线程的 `[start, end)`。
fn partition(total: u64, threads: u64) -> Vec<(u64, u64)> {
    let chunk = total.div_ceil(threads);
    (0..threads)
        .map(|t| (t * chunk, ((t + 1) * chunk).min(total)))
        .filter(|(s, e)| s < e)
        .collect()
}

fn run_put(store: &Arc<BlobStore>, args: &Args) -> Summary {
    let value = vec![0xABu8; args.value_size];
    let ranges = partition(args.ops, args.threads);
    let start = Instant::now();
    let (mut lat, errors) = thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|&(lo, hi)| {
                let store = Arc::clone(store);
                let value = &value;
                scope.spawn(move || {
                    let mut lats = Vec::with_capacity((hi - lo) as usize);
                    let mut errors = 0u64;
                    for n in lo..hi {
                        let t = Instant::now();
                        let r = store.put(make_key(n), value);
                        lats.push(t.elapsed().as_nanos() as u64);
                        if r.is_err() {
                            errors += 1;
                        }
                    }
                    (lats, errors)
                })
            })
            .collect();
        let mut all = Vec::new();
        let mut errs = 0u64;
        for h in handles {
            let (lats, e) = h.join().expect("put thread panicked");
            errs += e;
            all.extend(lats);
        }
        (all, errs)
    });
    let elapsed = start.elapsed().as_secs_f64();
    lat.sort_unstable();
    let ok = args.ops - errors;
    Summary {
        label: "PUT",
        ops: args.ops,
        errors,
        bytes: ok * args.value_size as u64,
        elapsed_s: elapsed,
        lat,
    }
}

fn run_get(store: &Arc<BlobStore>, args: &Args) -> Summary {
    let ranges = partition(args.read_ops, args.read_threads);
    let start = Instant::now();
    let (mut lat, errors, bytes) = thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .enumerate()
            .map(|(i, &(lo, hi))| {
                let store = Arc::clone(store);
                let total_keys = args.ops;
                scope.spawn(move || {
                    let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (i as u64).wrapping_add(1);
                    let mut lats = Vec::with_capacity((hi - lo) as usize);
                    let mut errors = 0u64;
                    let mut bytes = 0u64;
                    for _ in lo..hi {
                        let n = xorshift(&mut rng) % total_keys;
                        let t = Instant::now();
                        match store.get(&make_key(n)) {
                            Ok(v) => bytes += v.len() as u64,
                            Err(_) => errors += 1,
                        }
                        lats.push(t.elapsed().as_nanos() as u64);
                    }
                    (lats, errors, bytes)
                })
            })
            .collect();
        let mut all = Vec::new();
        let mut errs = 0u64;
        let mut total_bytes = 0u64;
        for h in handles {
            let (lats, e, b) = h.join().expect("get thread panicked");
            errs += e;
            total_bytes += b;
            all.extend(lats);
        }
        (all, errs, total_bytes)
    });
    let elapsed = start.elapsed().as_secs_f64();
    lat.sort_unstable();
    Summary {
        label: "GET",
        ops: args.read_ops,
        errors,
        bytes,
        elapsed_s: elapsed,
        lat,
    }
}

fn main() {
    let args = parse_args();

    // 容量:按记录估算 + 25% 余量,向上取段容量整数倍。段容量须容得下单条记录。
    let record = args.value_size as u64 + 64;
    let segment = args.segment_bytes.max(record + 1024 * 1024);
    let needed = (args.ops.saturating_mul(record)).saturating_mul(5) / 4;
    let total = needed.div_ceil(segment).max(1) * segment;

    let (dir, temp) = match &args.dir {
        Some(p) => (p.clone(), false),
        None => {
            let ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/var/tmp".into());
            (
                PathBuf::from(format!("{base}/ambry_bench_blob_{ns}")),
                true,
            )
        }
    };

    println!(
        "bench-blob: ops={} value={}B put-threads={} read-ops={} read-threads={} index-max={} segment={}MiB total-capacity={}MiB dir={}",
        args.ops,
        args.value_size,
        args.threads,
        args.read_ops,
        args.read_threads,
        args.index_max,
        segment / (1024 * 1024),
        total / (1024 * 1024),
        dir.display(),
    );

    let store = Arc::new(
        BlobStore::create(&dir, total, segment, args.index_max)
            .expect("create BlobStore (increase --segment-mib if capacity too small)"),
    );

    let put = run_put(&store, &args);
    store.flush().expect("flush");
    report(&put);

    let get = run_get(&store, &args);
    report(&get);

    println!(
        "store: used={}MiB valid={}MiB full={}",
        store.size_in_bytes() / (1024 * 1024),
        store.valid_data_size() / (1024 * 1024),
        store.is_full(),
    );

    Arc::into_inner(store)
        .expect("no dangling refs")
        .close()
        .expect("close");
    if temp {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

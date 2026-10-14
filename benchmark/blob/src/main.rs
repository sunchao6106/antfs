//! `antfs-bench-blob`:BlobStore 层面的吞吐/延迟压测二进制。
//!
//! 直接压 `antfs_blob::BlobStore`(单机引擎),多线程共享 `Arc<BlobStore>`(D4:写方法 `&self`),
//! 分 PUT / GET 两阶段测吞吐(ops/s、MB/s)与延迟分位(p50/p99/max)。
//!
//! 用法:`antfs-bench-blob [--ops N] [--value-size B] [--threads T] [--read-ops N]
//!        [--read-threads T] [--index-max N] [--segment-mib M] [--dir PATH]`
//!
//! 后续 `benchmark/` 下会新增数据节点(HTTP 数据面)与 S3 frontend 的压测二进制;本程序只覆盖 blob 引擎层。
//! **集群/数据面端到端压测已落地**:`antfs-client bench`(见 `crates/client` 与 `docs/design/client.md §8`)。

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use antfs_blob::{BlobStore, StoreKey};

struct Args {
    ops: u64,
    value_size: usize,
    threads: u64,
    read_ops: u64,
    read_threads: u64,
    index_max: usize,
    segment_bytes: u64,
    dir: Option<PathBuf>,
    flush_interval_ms: u64,
    sync_seal_flush: bool,
    evict_before_read: bool,
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
            flush_interval_ms: 0,
            sync_seal_flush: false,
            evict_before_read: false,
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
            "--flush-interval-ms" => {
                a.flush_interval_ms = val().parse::<u64>().expect("flush-interval-ms")
            }
            "--sync-seal-flush" => a.sync_seal_flush = true,
            "--evict-before-read" => a.evict_before_read = true,
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
        "antfs-bench-blob — BlobStore 层压测\n\
         \n\
         --ops N           PUT 次数 (default 100000)\n\
         --value-size B    每个 blob 字节数 (default 4096)\n\
         --threads T       PUT 并发线程 (default = CPU 数)\n\
         --read-ops N      GET 次数 (default 100000)\n\
         --read-threads T  GET 并发线程 (default = CPU 数)\n\
         --index-max N     活跃索引封存阈值 (default 100000)\n\
         --segment-mib M   单段大小 MiB (default 64)\n\
         --dir PATH        数据目录 (default 临时目录,跑完删除)\n\
         --flush-interval-ms N  周期 fsync 线程间隔 (default 0 = 关)。store 生产路径有 flush worker\n\
                            周期清脏页;裸引擎压不带上它会把段的 fsync 尾部整个甩到滚转点\n\
                            (对齐生产形态请给 1000~5000)\n\
         --sync-seal-flush        关闭 defer_seal_flush(滚转点同步 fsync 旧段=旧行为,作 A/B 对照)\n\
         --evict-before-read      GET 阶段前对数据段 fadvise DONTNEED 驱逐 page cache:\n\
                            冷读口径(「冷数据·暖索引」——索引 mmap 页未驱逐)\n"
    );
}

/// 由序号生成 8 字节 key:**partition 0**(与 `BlobStore::create` 的默认 key 前缀一致 ——
/// 记录只存低 32bit,前缀不符的 key 会被写路径拒掉)+ 黄金比奇数乘法散列取低 32bit
/// (u32 上的双射,保唯一且分布均匀,避免键分布退化)。
fn make_key(n: u64) -> StoreKey {
    StoreKey::from_parts(0, n.wrapping_mul(0x9E37_79B9_7F4A_7C15) as u32)
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

/// 对 log 目录(`dir/log`,引擎固定布局)里每个数据段 `posix_fadvise(DONTNEED)`,
/// 把写阶段热起来的数据页从 page cache 驱逐 —— 供冷读口径。失败(文件消失等)静默忽略:
/// 这只是基准口径装置,不构成正确性面。仅 Linux 有效(macOS 无 posix_fadvise,no-op)。
#[cfg(target_os = "linux")]
fn evict_data_pages(store_dir: &PathBuf) {
    let log_dir = store_dir.join("log");
    let Ok(entries) = std::fs::read_dir(&log_dir) else {
        return;
    };
    use std::os::unix::io::AsRawFd;
    for ent in entries.flatten() {
        if ent.file_name().to_string_lossy().ends_with("_log.seg") {
            if let Ok(f) = std::fs::File::open(ent.path()) {
                unsafe {
                    libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn evict_data_pages(_store_dir: &PathBuf) {}

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
            (PathBuf::from(format!("{base}/antfs_bench_blob_{ns}")), true)
        }
    };

    println!(
        "antfs-bench-blob: ops={} value={}B put-threads={} read-ops={} read-threads={} index-max={} segment={}MiB total-capacity={}MiB dir={}",
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

    let store = Arc::new(if args.sync_seal_flush {
        let registry = antfs_metric::Registry::new();
        let metrics = antfs_blob::StoreMetrics::new(&registry).expect("register store metrics");
        let config = antfs_blob::StoreConfig {
            segment_size_bytes: segment,
            index_max_inmem_elements: args.index_max,
            io_error_count_to_trigger_shutdown: 0,
            defer_seal_flush: false,
        };
        BlobStore::create_with(&dir, total, &config, &metrics, "bench-sync-seal")
            .expect("create BlobStore (increase --segment-mib if capacity too small)")
    } else {
        BlobStore::create(&dir, total, segment, args.index_max)
            .expect("create BlobStore (increase --segment-mib if capacity too small)")
    });

    let mut flush_thread = None;
    if args.flush_interval_ms > 0 {
        let store = store.clone();
        let interval = std::time::Duration::from_millis(args.flush_interval_ms);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_flag = stop.clone();
        flush_thread = Some((
            std::thread::spawn(move || {
                while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(interval);
                    if let Err(e) = store.flush() {
                        eprintln!("flush loop error: {e}");
                    }
                }
            }),
            stop,
        ));
    }

    let put = run_put(&store, &args);
    store.flush().expect("flush");
    report(&put);

    if args.evict_before_read {
        // 冷读口径:只驱逐数据段(「冷数据·暖索引」—— 热 GET 走 mmap .idx 常驻,如实记录)。
        evict_data_pages(&dir);
    }
    let get = run_get(&store, &args);
    report(&get);

    println!(
        "store: used={}MiB valid={}MiB full={}",
        store.size_in_bytes() / (1024 * 1024),
        store.valid_data_size() / (1024 * 1024),
        store.is_full(),
    );

    if let Some((handle, stop)) = flush_thread {
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        handle
            .join()
            .expect("join flush thread (最长再等一个 interval)");
    }
    Arc::into_inner(store)
        .expect("no dangling refs")
        .close()
        .expect("close");
    if temp {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

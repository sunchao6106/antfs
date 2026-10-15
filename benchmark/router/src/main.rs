//! `antfs-bench-router`:**直连模式**的端到端压测二进制。
//!
//! 压测逻辑在 [`antfs_bench_router`](`src/lib.rs`)里 —— devcluster 的 `--run-bench router` 复用同一份,
//! 避免"两处各写一遍压测"。
//!
//! 经 frontend 的 REST 路径见 `antfs-bench-frontend`;单机引擎层见 `antfs-bench-blob`。
//!
//! ```text
//! antfs-bench-router --master 127.0.0.1:7101 --secret s --ops 20000 --concurrency 32 --size 4096
//! antfs-bench-router --master 127.0.0.1:7101 --secret s --ops 1 --size 1048576    # 单对象冒烟
//! ```

use antfs_bench_common::Args;
use antfs_bench_router::Cfg;

const USAGE: &str = "antfs-bench-router — end-to-end benchmark via the direct router SDK\n\
\n\
--master ADDR       master control-plane gRPC address (default 127.0.0.1:7101)\n\
--secret S          cluster admission secret (empty = no check)\n\
--timeout-secs N    per-request timeout in seconds (default 10)\n\
--segment-size N    blobId count leased from master per request (default 1024)\n\
--ops N             write ops (default 10000)\n\
--concurrency C     concurrent tasks (default 32)\n\
--size B            object size in bytes (default 4096)\n\
--read-ratio P      read phase = successful writes x P% (default 0 = no reads)\n\
--verify            read back the first 64 objects and compare byte-for-byte\n";

#[tokio::main]
async fn main() {
    let args = Args::parse(&["verify", "help"]);
    args.help(USAGE);
    let cfg = Cfg {
        master_addr: args.flag("master").unwrap_or("127.0.0.1:7101").to_string(),
        cluster_secret: args.flag("secret").unwrap_or("").to_string(),
        request_timeout_secs: args.num("timeout-secs", 10),
        id_segment_size: args.num("segment-size", 1024),
        ops: args.num("ops", 10_000),
        concurrency: args.num::<u64>("concurrency", 32).max(1),
        size: args.num("size", 4096),
        read_ratio: args.num("read-ratio", 0),
        verify: args.has("verify"),
    };
    if let Err(e) = antfs_bench_router::run(cfg).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

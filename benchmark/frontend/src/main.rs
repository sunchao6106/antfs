//! `antfs-bench-frontend`:**REST 网关模式**的端到端压测二进制 —— 只用 HTTP 打 `frontend`。
//!
//! 压测逻辑在 [`antfs_bench_frontend`](`src/lib.rs`)里 —— devcluster 的 `--run-bench frontend` 复用同一份。
//!
//! ```text
//! antfs-bench-frontend --frontend http://127.0.0.1:7400 --ops 20000 --concurrency 32 --size 4096
//! ```

use antfs_bench_common::Args;
use antfs_bench_frontend::Cfg;

const USAGE: &str = "antfs-bench-frontend — 经 frontend(HTTP REST)的端到端压测\n\
\n\
--frontend URL      frontend 基地址 (default http://127.0.0.1:7400)\n\
--timeout-secs N    单请求超时秒数 (default 30)\n\
--ops N             写操作数 (default 10000)\n\
--concurrency C     并发任务数 (default 32)\n\
--size B            每个对象字节数 (default 4096)\n\
--read-ratio P      读阶段规模 = 写成功数 × P% (default 0 = 不读)\n\
--verify            抽前 64 个对象回读对拍\n";

#[tokio::main]
async fn main() {
    let args = Args::parse(&["verify", "help"]);
    args.help(USAGE);
    let cfg = Cfg {
        base: args
            .flag("frontend")
            .unwrap_or("http://127.0.0.1:7400")
            .to_string(),
        timeout_secs: args.num("timeout-secs", 30),
        ops: args.num("ops", 10_000),
        concurrency: args.num::<u64>("concurrency", 32).max(1),
        size: args.num("size", 4096),
        read_ratio: args.num("read-ratio", 0),
        verify: args.has("verify"),
    };
    if let Err(e) = antfs_bench_frontend::run(cfg).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

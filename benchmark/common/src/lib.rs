//! `benchmark/` 下各压测二进制的公用件:**负载生成 + 逐操作延迟收集 + 报表 + 极简 flag 解析**。
//!
//! 只服务压测程序,故刻意保持"最小够用":不引 clap、不做泛型抽象。
//! (先有的 `benchmark/blob`(`antfs-bench-blob`)自带同款实现,可后续迁移到本 crate。)

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

/// 确定性伪随机负载(xorshift64*):避免全零数据掩盖真实链路问题,且每次跑的内容一致。
pub fn pattern_bytes(len: usize) -> Vec<u8> {
    let mut s = 0x2545_F491_4F6C_DD1Du64;
    (0..len)
        .map(|_| {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            (s.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) as u8
        })
        .collect()
}

/// 打散用的黄金比乘数 + xorshift,给出 `[0, n)` 的下标(读阶段随机选已写的对象)。
pub fn spread(i: u64, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let x = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    ((x >> 17) as usize) % n
}

/// 一个阶段的统计。
#[derive(Debug, Default)]
pub struct Phase {
    pub label: String,
    /// 计划操作数。
    pub ops: u64,
    /// 成功操作数。
    pub ok: u64,
    /// 成功操作的字节数。
    pub bytes: u64,
    /// 阶段墙钟耗时。
    pub elapsed: Duration,
    /// 逐操作延迟(纳秒,未排序)。
    pub lat_ns: Vec<u64>,
    /// 错误分类(`kind → 次数`)。
    pub err_kinds: BTreeMap<String, u64>,
}

impl Phase {
    pub fn new(label: impl Into<String>, ops: u64) -> Self {
        Phase {
            label: label.into(),
            ops,
            ..Default::default()
        }
    }

    pub fn record_ok(&mut self, nanos: u64) {
        self.ok += 1;
        self.lat_ns.push(nanos);
    }

    pub fn record_err(&mut self, kind: &str) {
        *self.err_kinds.entry(kind.to_string()).or_default() += 1;
    }

    /// 合并一个并发任务的局部统计。
    pub fn merge(&mut self, other: Phase) {
        self.ok += other.ok;
        self.bytes += other.bytes;
        self.lat_ns.extend(other.lat_ns);
        for (k, n) in other.err_kinds {
            *self.err_kinds.entry(k).or_default() += n;
        }
    }

    pub fn errors(&self) -> u64 {
        self.err_kinds.values().sum()
    }

    /// 打印一行汇总(ops/s、MB/s、avg/p50/p99/max、错误数)+ 错误分类明细。
    pub fn report(&self) {
        let mut lat = self.lat_ns.clone();
        lat.sort_unstable();
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        let avg_us = if lat.is_empty() {
            0.0
        } else {
            lat.iter().sum::<u64>() as f64 / lat.len() as f64 / 1000.0
        };
        println!(
            "{:>5} | {:>8} ops | {:>9.0} ops/s | {:>8.1} MB/s | avg {avg_us:>7.1}us  p50 {:>7.1}us  p99 {:>8.1}us  max {:>8.1}us  err {}",
            self.label,
            self.ops,
            self.ok as f64 / secs,
            self.bytes as f64 / secs / (1024.0 * 1024.0),
            percentile(&lat, 50.0),
            percentile(&lat, 99.0),
            lat.last().map(|v| *v as f64 / 1000.0).unwrap_or(0.0),
            self.errors(),
        );
        if !self.err_kinds.is_empty() {
            let detail: Vec<String> = self
                .err_kinds
                .iter()
                .map(|(k, n)| format!("{k}={n}"))
                .collect();
            println!("        errors: {}", detail.join(" "));
        }
    }
}

/// 已排序样本的百分位(单位:微秒;输入为纳秒)。
pub fn percentile(sorted_ns: &[u64], p: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx =
        (((p / 100.0) * (sorted_ns.len() - 1) as f64).round() as usize).min(sorted_ns.len() - 1);
    sorted_ns[idx] as f64 / 1000.0
}

/// 极简 flag 解析:`--k v` / `--k=v` / 开关型 `--bool`。与仓库其它 CLI(devcluster)同风格,不引 clap。
pub struct Args {
    flags: HashMap<String, String>,
    bools: HashSet<String>,
}

impl Args {
    /// `bool_flags` 里的 flag 不带值。`--help` 由调用方自行处理(见 [`Self::help`])。
    pub fn parse(bool_flags: &[&str]) -> Self {
        let mut flags = HashMap::new();
        let mut bools = HashSet::new();
        let mut it = std::env::args().skip(1);
        while let Some(tok) = it.next() {
            let Some(name) = tok.strip_prefix("--") else {
                eprintln!("unexpected argument: {tok}");
                std::process::exit(2);
            };
            let (name, inline) = match name.split_once('=') {
                Some((k, v)) => (k.to_string(), Some(v.to_string())),
                None => (name.to_string(), None),
            };
            if inline.is_none() && bool_flags.contains(&name.as_str()) {
                bools.insert(name);
                continue;
            }
            let value = inline.unwrap_or_else(|| {
                it.next().unwrap_or_else(|| {
                    eprintln!("--{name} needs a value");
                    std::process::exit(2);
                })
            });
            flags.insert(name, value);
        }
        Args { flags, bools }
    }

    /// 有 `--help` 就打印用法并退出(0)。
    pub fn help(&self, usage: &str) {
        if self.bools.contains("help") {
            eprintln!("{usage}");
            std::process::exit(0);
        }
    }

    pub fn flag(&self, name: &str) -> Option<&str> {
        self.flags.get(name).map(|s| s.as_str())
    }

    pub fn has(&self, name: &str) -> bool {
        self.bools.contains(name)
    }

    pub fn num<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        match self.flag(name) {
            Some(v) => v.parse().unwrap_or_else(|_| {
                eprintln!("--{name} expects a number, got {v}");
                std::process::exit(2);
            }),
            None => default,
        }
    }

    pub fn require(&self, name: &str) -> String {
        self.flag(name)
            .unwrap_or_else(|| {
                eprintln!("--{name} is required");
                std::process::exit(2);
            })
            .to_string()
    }
}

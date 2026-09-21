# bench-store

`store::BlobStore` 层的吞吐/延迟压测二进制。多线程共享 `Arc<BlobStore>`(D4:写方法 `&self`),
分 PUT / GET 两阶段测吞吐(ops/s、MB/s)与延迟分位(p50/p99/max)。std-only,无第三方依赖。

> `benchmark/` 目录后续会新增 blob-server 层与 S3 frontend 的压测二进制;本 crate 只覆盖 store 层。

## 运行

```bash
# 命令行(生产目标建议 --release)
cargo run -p bench-store --release -- --ops 100000 --value-size 4096 --threads 8
cargo run -p bench-store --release -- --help
```

RustRover / IDE:重新加载 workspace 后会识别 `bench-store` 二进制目标,点 `src/main.rs` 中
`fn main` 左侧运行图标,或新建 Cargo 运行配置(command `run -p bench-store --release`,
program args 填 `-- --ops ...`)。工具链配置见 [`docs/RUSTROVER.md`](../../docs/RUSTROVER.md)。

## 参数

| 参数 | 默认 | 含义 |
|---|---|---|
| `--ops N` | 100000 | PUT 次数 |
| `--value-size B` | 4096 | 每个 blob 字节数 |
| `--threads T` | CPU 数 | PUT 并发线程 |
| `--read-ops N` | 100000 | GET 次数(随机读已写入的 key) |
| `--read-threads T` | CPU 数 | GET 并发线程 |
| `--index-max N` | 100000 | 活跃索引封存阈值 |
| `--segment-mib M` | 64 | 单段大小(MiB) |
| `--dir PATH` | 临时目录 | 数据目录;缺省用 `$TMPDIR`/`var/tmp` 下临时目录,跑完删除 |

总容量按 `ops × 记录大小 × 1.25` 向上取段容量整数倍自动推算(写满会报 `StoreFull`,可调大 `--segment-mib`)。

## 输出

```
  PUT |  100000 ops |  46652 ops/s |  182.2 MB/s | avg 74.1us  p50 6.7us  p99 374us  max 52ms  err 0
  GET |  100000 ops | 560751 ops/s | 2190.4 MB/s | avg  7.1us  p50 7.0us  p99  13us  max 87us  err 0
store: used=79MiB valid=79MiB full=true
```

- `ops/s` / `MB/s`:该阶段整体吞吐;`avg/p50/p99/max`:单次操作延迟;`err`:失败次数。
- `used`:物理占盘(含死记录/墓碑);`valid`:未删除赢家字节;`full`:是否已达定容上界。

## 落到真实磁盘 / Linux 保真

默认临时目录多在 tmpfs/本机盘。要在生产目标 Linux ext4 上压测,用 `--dir` 指向 colima VM 挂载路径,
或经 `./vm-cargo.sh run -p bench-store --release -- ...` 转进 VM(见仓库根 README「开发环境」)。

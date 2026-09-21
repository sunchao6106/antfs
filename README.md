# ambry-rs

用 Rust 重写 **整个 Ambry**(不止存储引擎)。分阶段推进:

- **现阶段:忠实的 Rust 翻译** —— 对齐 Ambry 的架构与语义,先把功能 1:1 搬到 Rust(仅在等价前提下用现代实现,如 Binary Fuse 过滤器替 Bloom)。
- **后续:在 Rust 版本之上做优化** —— 待翻译完成、正确性锁定后,再针对性能 / 并发 / 存储密度做改进。

## 模块

| 模块 | 说明 | 状态 |
|---|---|---|
| store | 单机存储引擎:`Log` / `LogSegment` / `PersistentIndex` / `BlobStore`(put/get/delete/undelete/updateTtl/compaction/hard_delete/恢复);对标 Ambry `ambry-store` | 进行中 |
| frontend / S3 网关、复制、集群等 | 其余 Ambry 模块 | 规划中 |

store 当前要点:
- IO:**同步阻塞 `std::fs` + buffered(OS page cache)**,对齐 Ambry 原版
- 索引:并发活跃段 + **mmap 封存段 + 持久化 Binary Fuse 过滤器**

详细设计见 [`docs/design/`](docs/design/)(总览 + 各模块);决策档案见 `openspec/changes/`。

## 开发环境

工程**跨平台**(Linux / macOS,均 Unix)。本机 macOS 可直接开发:

```bash
cargo build
cargo test
cargo run -p smoke
```
> 需 rustup 工具链;新开的 shell 先 `. "$HOME/.cargo/env"`。

### 可选:Linux 目标保真(colima VM)

生产目标是 Linux ext4。需在真实目标 OS 上做保真 / 大数据量 / 性能测试时,用 colima Linux VM:

```bash
./vm-cargo.sh build   # 自动转进 VM 跑 cargo(源码 virtiofs 挂载,target/数据落 VM 原生 ext4)
./vm-cargo.sh test
./vm-cargo.sh run -p smoke
```

### RustRover

IDE 配置(本机工具链 / 可选远程 VM 工具链)见 `docs/RUSTROVER.md`。

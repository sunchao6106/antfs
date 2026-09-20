# ambry-rs

用 Rust 重写 Ambry 存储引擎(整体对齐 Ambry 架构,局部替换为业界最新实现):
- IO:**同步阻塞 `std::fs` + buffered(OS page cache)**,对齐 Ambry 原版
- 索引:并发活跃段 + **mmap 封存段 + 持久化 Binary Fuse 过滤器**
- 目标组件:`Log` / `LogSegment` / `PersistentIndex` / `BlobStore`

详细设计见 `docs/DESIGN.md`;当前规划见 `openspec/changes/io-index-design/`。

## 开发环境(macOS 宿主 + colima Linux VM)

本机是 macOS,引擎需在 Linux 跑(数据量大、colima 可挂盘),因此用 colima 起了一个 Linux VM:
- Ubuntu 24.04 / kernel 6.8
- 源码在宿主机(编辑器可见),通过 virtiofs 挂载进 VM
- 在 VM 内编译/测试;**数据文件与 cargo target 放 VM 原生 ext4**(virtiofs 对性能/IO 不友好)

### 用法
```bash
# 在宿主机(macOS)执行,自动进 VM 跑 cargo:
./vm-cargo.sh build
./vm-cargo.sh test
./vm-cargo.sh run -p smoke
```

### RustRover
IDE 配置(运行配置 + 远程工具链)见 `docs/RUSTROVER.md`。
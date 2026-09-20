# ambry-rs

用 Rust 重写 Ambry 存储引擎(局部替换为业界最新实现):
- IO:**monoio + io_uring + O_DIRECT**(仅 Linux)
- 目标组件:`Log` / `LogSegment` / `PersistentIndex`

## 开发环境(macOS 宿主 + colima Linux VM)

本机是 macOS,monoio/io_uring/O_DIRECT 只能在 Linux 跑,因此用 colima 起了一个 Linux VM:
- Ubuntu 24.04 / kernel 6.8 / io_uring 启用
- 源码在宿主机(编辑器可见),通过 virtiofs 挂载进 VM
- 在 VM 内编译/测试;**数据文件与 cargo target 放 VM 原生 ext4**(virtiofs 对 O_DIRECT/性能不友好)

### 用法
```bash
# 在宿主机(macOS)执行,自动进 VM 跑 cargo:
./vm-cargo.sh build
./vm-cargo.sh test
./vm-cargo.sh run -p smoke
```

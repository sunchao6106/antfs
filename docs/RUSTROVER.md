# RustRover 环境配置

工程已跨平台(Linux / macOS)。**推荐:本机(macOS)本地工具链开发**,RustRover 开箱即用,无需 VM。
仅在需要 Linux 目标 OS 保真 / 大数据量 / 性能测试时,才用可选的 colima VM 路径(见文末)。

## 1. 本地开发(推荐,默认)

前置:装 rustup 工具链(一次性):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

RustRover 会自动识别 `~/.cargo/bin` 下的 rustc/cargo 与 `Cargo.toml`,补全 / 跳转 / 实时分析 / 运行配置全部本地生效。若未自动识别:`Settings → Languages & Frameworks → Rust`,工具链指向 `~/.cargo/bin`。

常用命令(内置终端直接跑):

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt
cargo run -p smoke
```

数据/测试文件默认写 `/var/tmp/ambry_*`,macOS 本地即可。

## 2. 可选:Linux 目标保真(colima VM)

生产目标是 Linux ext4。要在真实目标 OS 上验证(`posix_fallocate` 真实占盘、fsync、mmap、性能等)时用 VM:

### 2.1 命令行
`.run/` 下提供了通过 [`vm-cargo.sh`](../vm-cargo.sh) 转进 VM 的 Shell 运行配置(build / test / clippy / smoke),不写死 SSH 端口、可提交共享。也可直接命令行:
```bash
./vm-cargo.sh build
./vm-cargo.sh test -p store
```

### 2.2 IDE 代码洞察走 VM(JetBrains Gateway)
若想让 IDE 后端用 VM 的工具链:**Gateway → New Connection → SSH**,连接信息取自 `colima ssh-config`(注意 `Port` 每次重启 VM 会变);远端选 RustRover,项目路径填 VM 内同一路径(virtiofs 同路径挂载)`/Users/admin/code/opensource/ambry-rs`;若提示选工具链指向 `/home/lima.guest/.cargo/bin`。

### 2.3 VM 注意事项
- cargo target 放 VM 原生 ext4,勿写进 virtiofs 挂载点:`CARGO_TARGET_DIR=/var/tmp/ambry-rs-target`(`vm-cargo.sh` 已自动导出)。
- 数据文件同样落 VM 原生盘(`/var/tmp/ambry_*`),不要放 virtiofs 工程目录下。

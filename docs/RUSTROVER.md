# RustRover 环境配置

本工程只能在 Linux 下编译(monoio + io_uring + O_DIRECT),macOS 宿主**没有** Rust 工具链,
全部编译/测试都在 colima Linux VM 里跑(见 [`vm-cargo.sh`](../vm-cargo.sh))。因此 RustRover 需按下面两块配置。

## 1. 编译/测试按钮(已配好,开箱即用)

`.run/` 下已提供 4 个 Shell 运行配置(依赖 RustRover 内置的 *Shell Script* 插件),
它们通过 `vm-cargo.sh` 把命令转进 VM,不写死 SSH 端口,可直接提交共享:

| 配置名 | 作用 |
|---|---|
| **VM: cargo build** | `./vm-cargo.sh build` |
| **VM: cargo test (ambry-store)** | `./vm-cargo.sh test -p ambry-store` |
| **VM: cargo clippy (-D warnings)** | clippy 全告警拦截 |
| **VM: run smoke** | 跑 `smoke` crate(真 io_uring 读写闭环) |

打开工程后,右上角运行配置下拉里即可看到,点绿色三角运行,输出走内置终端。

## 2. 代码洞察(补全/跳转/实时分析)

宿主无 rustc,想要完整的补全和分析,**必须让 IDE 后端用到 VM 里的工具链**。推荐用
JetBrains **Remote Development(Gateway)**:IDE 后端跑在 VM 内,直接使用 VM 的 rustc 1.98。

### 步骤

1. 取 SSH 连接信息:
   ```bash
   colima ssh-config
   ```
   记下 `Hostname`(127.0.0.1)、`Port`(每次重启 VM 会变)、`User`(lima)、`IdentityFile`。

2. 打开 **JetBrains Gateway → New Connection → SSH**,填入上面 host/port/user/key。

3. 远端选 **RustRover** 作为 IDE,项目路径填 VM 内的同一路径(virtiofs 同路径挂载):
   ```
   /Users/admin/code/opensource/ambry-rs
   ```

4. 后端首次打开会自动识别 `Cargo.toml`;若提示选工具链,指向
   `/home/lima.guest/.cargo/bin`。

> 备选(不推荐):RustRover 内 `Settings → Languages & Frameworks → Rust` 配 SSH 远程工具链;
> 因 colima 端口重启即变,维护成本高,建议直接用 Gateway。

## 3. 环境变量

VM 内编译时把 cargo target 放原生 ext4(virtiofs 对 O_DIRECT/性能不友好):

```
CARGO_TARGET_DIR=/var/tmp/ambry-rs-target
```

`vm-cargo.sh` 已自动导出该变量;若在 Gateway 远端手动跑 cargo,请在 VM shell 里同样导出,
避免把 target 写进 virtiofs 挂载点。

## 4. 数据文件目录

测试/数据文件同样要落 VM 原生盘(代码里默认写 `/var/tmp/ambry_*`),不要放到 virtiofs
挂载的工程目录下,否则 O_DIRECT 可能失败或极慢。

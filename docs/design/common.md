# common 设计:公共库(config / metric)

`crates/common/` 下的公共能力。本轮落地两个 crate:

| crate | 目录 | 职责 | Rust 方案 | 对标 Ambry |
|---|---|---|---|---|
| `config` | `crates/common/config` | 类型化分层配置加载 + 语义校验 | `serde` + [`config`](https://docs.rs/config)(TOML) | `com.github.ambry.config`(`VerifiableProperties` + `*Config`) |
| `metric` | `crates/common/metric` | 统计打点(注册表 + typed handle) | [`prometheus`](https://docs.rs/prometheus) | Dropwizard `MetricRegistry` + `*Metrics` |
| `router` | `crates/common/router` | 客户端侧 **router**(通用组件):向 master 取 blobId 号段、按 blobId 定位副本、W 多数派写 / R=1 读、流式文件读写 | `tonic`(控制面)+ `reqwest`(数据面);纯库、无二进制 | `NonBlockingRouter`(封装进 SDK) |

> `router` 的详细设计(写读路径、错误契约、配置/监控清单)**不只属于本文件**:见 [`router.md`](./router.md);
> `frontend`(`crates/frontend`)与 `benchmark/router` 都依赖它,W/R 逻辑只有一份实现。

原则:**照搬 Ambry 的形状(每模块一个 `*Config` / `*Metrics`,handle 存成字段),实现换成 Rust 生态最佳库**,不手搬
`VerifiableProperties`(字符串 getter + 手写区间校验)与 Dropwizard(客户端 EWMA/reservoir)这些 Java 包袱。

> 注:`config` crate 与 crates.io 的 `config` 同名。命令行需消歧:`cargo test -p config@0.0.0`;
> 其对底层库的依赖已在 `Cargo.toml` 里重命名为 `config_rs` 以避免冲突。

## 1. config:配置

### 1.1 每模块配置的声明范式(= Ambry `@Config`/`@Default`)

```rust
use serde::Deserialize;
use config::{Loader, Validate, Result, in_range, at_least};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]                       // 缺失字段回落到 Default —— 等价 @Default
pub struct StoreConfig {
    pub data_flush_interval_seconds: u64,
    pub index_max_memory_size_bytes: u64,
}
impl Default for StoreConfig {          // 默认值集中在此
    fn default() -> Self {
        Self { data_flush_interval_seconds: 60, index_max_memory_size_bytes: 20_971_520 }
    }
}
impl Validate for StoreConfig {         // 区间/枚举校验 —— 等价 getIntInRange 等
    fn validate(&self) -> Result<()> {
        in_range("data_flush_interval_seconds", self.data_flush_interval_seconds, 1, 86_400)?;
        at_least("index_max_memory_size_bytes", self.index_max_memory_size_bytes, 1)?;
        Ok(())
    }
}
```

- 字段名即配置 key;TOML 用嵌套表分组(`[store]` 下 `data_flush_interval_seconds = 30`)。
- 校验辅助:`in_range` / `at_least` / `one_of`(失败返回 `ConfigError::Invalid { key, message }`)。

### 1.2 加载(分层合并,后覆盖前)

`Loader` 来源优先级:**struct 默认 < 内联/文件 TOML < 环境变量 < 显式 override**。

```rust
let cfg: StoreConfig = Loader::new()
    .with_file("config/store.toml", /*required=*/ false)
    .with_env_prefix("AMBRY")          // AMBRY_STORE__FLUSH=1 → store.flush(前缀分隔 `_`,嵌套分隔 `__`)
    .set_override("data_flush_interval_seconds", "30")
    .load_validated()?;               // load() + Validate::validate()
```

环境变量:前缀分隔符固定 `_`,嵌套分隔符默认 `__`(可 `with_env_separator` 改),`try_parsing` 已开启使字符串正确转成
数值/布尔并覆盖同名 TOML 值。

**表名与 flat key(踩过的坑)**:设计文档写的是"字段名即 TOML key(在 `[admin]` / `[store_server]` / `[master]` /
`[frontend]` 表下)",但 `load()` 只按**根级**字段反序列化 —— 表里的值会被 serde 默认**静默掩盖**(配置写了不生效,
不报错)。凡配置写在表里(或环境变量用嵌套形式 `AMBRY_ADMIN__MASTER_ADDR`)的进程,都要用:

```rust
let cfg: AdminConfig = Loader::new()
    .with_file("admin.toml", false)
    .with_env_prefix("AMBRY")
    .load_table("admin")?;            // 表存在 → 只取该表;不存在 → 回落 flat(兼容旧写法)
```

`load_table` 的语义:**表存在时只解析该表,表内字段写错直接报错**(不从表静默回落到 flat,否则又会变成"配了没生效");
表不存在时回落顶层 flat。同一份配置里**不要混用**两种写法。frontend 用的是等价的 `FrontendToml` 包装结构(先例)。

### 1.3 配置项清单

`config` crate **自身不定义任何业务配置 key**(只提供加载/校验机制)。它对外的行为常量:

| 项 | 值 | 说明 |
|---|---|---|
| 环境变量前缀分隔符 | `_` | 固定,分隔 `with_env_prefix` 的前缀与键 |
| 环境变量嵌套分隔符 | `__` | `with_env_separator` 可改;映射到嵌套配置路径 |
| 配置文件格式 | TOML | `with_file` / `with_inline_toml` |

> 各业务模块(store 等)的**配置项清单**写在各自的 `docs/design/<module>.md`,列:
> key(TOML 路径)· 类型 · 默认值(= `impl Default`)· 取值范围/校验(= `Validate`)· 含义/影响 · 对齐的 Ambry 原配置。

## 2. metric:监控

### 2.1 每模块指标的声明范式(= Ambry `*Metrics`)

```rust
use metric::{counter, gauge, latency_seconds, Registry, IntCounter, IntGauge, Histogram};

pub struct StoreMetrics {
    pub get_response: Histogram,        // 延迟直方图(秒)
    pub store_start_failure: IntCounter,
    pub used_capacity_bytes: IntGauge,
}
impl StoreMetrics {
    pub fn new(registry: &Registry) -> metric::Result<Self> {
        Ok(Self {
            get_response: latency_seconds(registry, "store_get_response_seconds", "get latency")?,
            store_start_failure: counter(registry, "store_start_failure_total", "start failures")?,
            used_capacity_bytes: gauge(registry, "store_used_capacity_bytes", "used capacity")?,
        })
    }
}
```

- 一个共享 `Registry`(对齐 Ambry 传来传去的 `MetricRegistry`),handle 存成 `*Metrics` 字段。
- 辅助构造:`counter`(单调计数)· `gauge`(可增减瞬时量)· `histogram(buckets)`(自定义桶)· `latency_seconds`(默认延迟桶,配合 `Histogram::start_timer` 的 drop guard 记录耗时)。
- 导出:`metric::gather(&registry)` → Prometheus 文本格式(供 `/metrics`)。
- 命名:Prometheus 名不允许 `.`,用 `metric_name(subsystem, name)` 以 `_` 连接;counter 惯例后缀 `_total`,时间单位用秒(`_seconds`)。

### 2.2 多实例共享 Registry 的区分 + DI 所有权(上层必须感知的策略,固化)

**所有权链(依赖注入,对齐 Ambry `AmbryServer`→`StorageManager`→`BlobStore`→`Log`/`PersistentIndex`)**:
一个进程 **一个 `Registry`** + 各模块 **一份 config**,由**顶层创建一次**,构造参数逐层**透传**,下层组件**只读不建**(不建自己的 registry、不读全局单例)。
当前 antfs 无 server 层 → `Registry`/`StoreConfig` 由 **`BlobStore` 的调用方**持有并注入;将来抽象出 `StorageManager` 角色后,所有权上移到它,`BlobStore` 仍只接收。

**多实例(多 store / replica / partition)共享同一 `Registry` 的区分:用 label,不用 name 前缀。**

| 方案 | Ambry(Dropwizard) | 本项目(Prometheus) |
|---|---|---|
| 区分手段 | metric **name 前缀**(`storeId + SEPARATOR + ...`) | **label** `store="<id>"`(id = partition/replica 标识) |
| 聚合/过滤 | 做不到(name 是死字符串) | `sum without(store)(...)` / `{store="p42"}` 天然支持 |

**实现落法(便于后续接入时照做)**:
- Registry 上注册的是 **`*Vec`(带 `store` label)**,**每个 registry 只注册一次**;第二个 store 复用已注册的 Vec,不重复 register(否则 `AlreadyReg` 报错)。
- 每个 store 实例在 `StoreMetrics::new(registry, store_id)` 里用 `vec.with_label_values(&[store_id])` 取**自己的子 handle** 存进字段;打点时直接用该子 handle(无需每次带 label)。
- → `metric` crate 需补 **`*_vec` 系列 helper**(`counter_vec` / `gauge_vec` / `histogram_vec`);现有单指标 helper 用于**无实例维度的全局/per-host 指标**(如 host 级 `IndexDirectMemoryUsage`,不带 `store` label)。
- **label 基数纪律**:只把**有界、低基数**维度放 label(`store` / `op` / `result` / `kind`);**绝不**把 blob id、key 等无界值放 label(会指标爆炸)。

### 2.3 与 Dropwizard 的差异(现代做法)

| 维度 | Ambry / Dropwizard | 本项目 / Prometheus |
|---|---|---|
| 速率(Meter) | 客户端算 1/5/15min EWMA | 客户端只累加 counter,服务端 `rate()` 算 |
| 分位(Histogram/Timer) | 客户端 reservoir 采样算分位 | 客户端只累加桶,服务端 `histogram_quantile()` 算 |
| Timer | Histogram + Meter 组合 | 延迟 Histogram(秒)+ `start_timer()` guard |

### 2.4 监控项清单

`metric` crate **自身不注册任何业务指标**(只提供注册/导出机制)。对外行为常量:

| 项 | 值 | 说明 |
|---|---|---|
| 延迟直方图默认桶 | Prometheus `DEFAULT_BUCKETS`(0.005…10s) | `latency_seconds` 使用 |
| 导出格式 | Prometheus 文本 | `gather()`,`prometheus` 默认特性已裁剪(无 protobuf/process) |
| 名称分隔 | `_`(`metric_name`) | Prometheus 命名规范 |

> 各业务模块的**监控项清单**写在各自的 `docs/design/<module>.md`,列:
> 指标名 · 类型(counter/gauge/histogram)· 标签(labels)· 单位 · 含义 · 对齐的 Ambry 原指标。

### 2.5 测试约定

- **对被测对象实际接入的那个 `Registry` 实例断言**:测试里把 `Registry` 建好、传进被测组件的 `*Metrics::new(&registry)`,
  再用同一个 `registry` 的 handle(或 `gather(&registry)` 文本)校验增量。**不要在测试里另建一个 `Registry`/`*Metrics`**
  —— 它和被测代码用的不是同一份,观测不到真实增量(对齐 Ambry 的评审教训)。
- handle 是 `Arc` 语义(`clone` 共享同一底层计数),可直接持有被测组件的字段断言,如 `assert_eq!(m.store_start_failure.get(), 1)`。


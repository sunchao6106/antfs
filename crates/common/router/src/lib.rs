//! `Router`:antfs 的客户端侧**通用组件** —— **向 master 申请 blobId 号段 → 直连数据面写读**。
//!
//! 定位见 [`docs/design/router.md`](../../../../docs/design/router.md) 与 `cluster.md §10`:Ambry 里这层叫
//! router(封装进 SDK),antfs 把它做成纯库(`crates/common/router`)—— **frontend 内嵌同一个 `Router`**,
//! `benchmark/router` 也用它,故 W/R 逻辑只有一份实现。本 crate **不提供二进制**。
//!
//! ```text
//! ListPartitions/GetBlobIds(gRPC) ─▶ master  回 (可写 partitionId 列表;每分区一段 objectId [start,start+N) + 副本位置)
//! PUT ×N (HTTP)    ─▶ store   收 W 个 ack 即返回(其余副本靠拉复制补齐)
//! GET   (HTTP)     ─▶ store   第一个成功即返回(R=1)
//! ```
//!
//! **发号 = 多分区按写权重分配**:补段时先拉 master 的当前可写(RW)分区列表(**带写权重**),把号段总预算
//! 按权重切给各分区(显式点名 `partition_id` 各申请一批),之后写请求在这些分区之间**按权重**轮转取号
//! (平滑加权轮转)—— 越空的分区拿到越多号,相邻写落在不同分区、不同盘上,写 IOPS 摊开,
//! 而不是把一段号连续耗尽在一个分区上。
//!
//! 权重由 **master 下发**(`PartitionEntry.write_weight`,千分比 = 剩余容量占比;口径与算法只在 master 一处):
//! 分区分派仍由 master 定,router 不自选分区、**不自算容量**(D10)。未下发(旧 master / 容量未配)时行为
//! 与**均权轮转逐位一致**;权重生效粒度 = 补齐/刷新的那一拍。
//!
//! **可写分区列表由后台任务保鲜**:`connect` 起一个后台任务,按 `partition_refresh_secs` 周期向 master 拉一次
//! 可写分区列表,**按新权重收敛各分区已有号段的段尾**、丢掉已不可写分区的号段、给新出现的可写分区补一段
//! (号段耗尽的写路径自己也会同步一次;周期配 0 = 不起后台任务)。任务持号段池的 `Weak` 引用,
//! 最后一个 `Router` 析构后自行退出。
//!
//! **两条正确性约束(不做成可配)**:
//! - 写:W = 该分区**实际副本数 / 2 + 1**(多数派)。RF=3 → 2(对齐 Ambry `routerPutSuccessTarget`)、
//!   RF=1 → 1。低于多数派的写会破坏"已确认 ⇒ 多数副本有"的前提,故不给配置项。
//! - 读:R = 1(blob 不可变,读任一有的副本即可)。
//!
//! blobId = 8 字节 `partitionId << 32 | objectId`([`antfs_blob::StoreKey`],大端),URL 里编成 16 位 hex。

pub mod compression;
pub mod config;
pub mod error;
pub mod metrics;

pub use antfs_blob::{StoreKey, NO_EXPIRATION};
pub use config::RouterConfig;
pub use error::{Error, Result};
pub use metrics::RouterMetrics;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{
    GetBlobIdsRequest, GetBlobIdsResponse, ListPartitionsRequest, LookupRequest, ReplicaLoc,
};
use antfs_master::PartitionId;

const PROTO_VERSION: u32 = 1;

/// `put_file_with_ttl` 因分区写满(全副本 507)而**换分区重试**的最大次数。
///
/// 不做成配置项:重试是"分区满 → 换一个分区"的确定性补救,不是退避策略;次数写死 2 次
/// (共 3 次尝试)足够覆盖"恰好撞上一个刚写满的分区",又不会在集群整体写满时反复空转。
const MAX_STORE_FULL_RETRIES: usize = 2;

/// 一次写的实际结果:收满 [`required`](Self::required) 个 ack 才返回成功。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutReport {
    /// 实际收到的 ack 数。
    pub acks: usize,
    /// 该分区的副本总数(placement)。
    pub total: usize,
    /// 成功门槛 = `total / 2 + 1`(多数派)。
    pub required: usize,
}

/// blob 元数据(不读数据体),对齐 store 的 `.../metadata` 响应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct BlobStat {
    pub size: u64,
    pub expiration_ms: i64,
    pub deleted: bool,
    pub expired: bool,
    /// 拼装描述符类型位;旧 store 的响应无此字段 → false(滚动升级读接缝,serde default)。
    #[serde(default)]
    pub composed: bool,
}

/// 一个分区上已从 master 取回、尚未发完的 blobId 号段。
struct Segment {
    pid: PartitionId,
    /// 下一个待发号(含);`next >= end` 即已取完。
    next: u32,
    end: u32,
}

/// 号段池:**每个可写分区各持一段**(段大小按写权重配比),取号时在它们之间**按写权重**轮转。
#[derive(Default)]
struct IdCache {
    /// 未发完的段(`pid → 段`);取完即移除。
    segments: HashMap<PartitionId, Segment>,
    /// 可写分区 + **写权重**(补段时刷新,master 给的顺序 = pid 升序)。
    order: Vec<PartitionWeight>,
    /// 平滑加权轮转(SWRR)的**当前信用**(`pid → credit`)。只为**手上还有号段**的分区维护:
    /// 段取完即清、补段从 0 起 —— 否则空转期攒下的信用会在补段后变成一串突发。
    ///
    /// 它同时顶替了早期实现里"保持轮转相位"的 `cursor`:一次什么都没补的空刷新之后,
    /// "下一个该轮到谁"由信用高低决定,不会被清空重来。
    credit: HashMap<PartitionId, i64>,
}

/// 一个可写分区的**写权重**(来自 master `PartitionEntry.write_weight`,千分比 `1..=1000`)。
///
/// **已归一**:`0`(未下发 / 未配容量上限)在 [`list_writable`] 里就变成 `1`,故这里的值恒 `≥1`。
/// "0 = 均权"这条映射是兼容性的落点:master 还不下发权重(或容量未配)时,行为与纯轮转**完全一致**。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionWeight {
    pid: PartitionId,
    weight: u32,
}

impl IdCache {
    /// 按写权重取一个号(**平滑加权轮转**,SWRR):每拍给在册(手上有号段)的分区各加 `weight` 信用,
    /// 取信用最大的那个,再把它的信用减去在册权重总和。
    ///
    /// 于是**任意窗口**内的取号比例都趋近权重之比 —— 这是"只按权重切段大小、取号仍顺序轮转"做不到的:
    /// 顺序轮转每轮给每个分区各 1 个号,任何中途窗口内各分区计数都**相等**,权重只能决定最满的那个
    /// 何时停止发号(取舍论证见 `openspec/changes/router-weighted-partition-alloc`)。
    ///
    /// - 手上没号段的分区**不参与、也不攒信用**(它此刻不该被发号);
    /// - 同分取 `order` 里靠前的(pid 小的)⇒ 结果**确定**,可被单测钉住;
    /// - **均权时逐位退化为顺序轮转**:相邻写仍落在不同分区(进而是不同副本、不同盘),写 IOPS 摊开。
    fn take_weighted(&mut self) -> Option<StoreKey> {
        let mut best: Option<(PartitionId, i64)> = None;
        let mut total: i64 = 0;
        for slot in &self.order {
            if !self.segments.contains_key(&slot.pid) {
                continue;
            }
            // `.max(1)`:0 = "未下发"(见 [`PartitionWeight`]),此处再兜一道,防将来有人直接构造 0 权重。
            let w = i64::from(slot.weight.max(1));
            total = total.saturating_add(w);
            let credit = self.credit.entry(slot.pid).or_insert(0);
            *credit = credit.saturating_add(w);
            if best.is_none_or(|(_, best_credit)| *credit > best_credit) {
                best = Some((slot.pid, *credit));
            }
        }
        let (pid, _) = best?;
        if let Some(credit) = self.credit.get_mut(&pid) {
            *credit = credit.saturating_sub(total);
        }
        self.take_from(pid)
    }

    /// 从该分区取一个号;段取完即丢掉段**与信用**(下次补段从 0 起,不留突发)。
    fn take_from(&mut self, pid: PartitionId) -> Option<StoreKey> {
        let seg = self.segments.get_mut(&pid)?;
        let key = StoreKey::from_parts(pid, seg.next);
        seg.next += 1;
        if seg.next >= seg.end {
            self.segments.remove(&pid);
            self.credit.remove(&pid);
        }
        Some(key)
    }

    /// 分区写满(全副本 507)后把它从本地号段池里摘掉:段、权重、信用一并清除。
    /// 之后 `take_weighted` 不会再挑它;下次 `sync_segments` 若 master 仍列它为 RW 会重新补段。
    fn evict_partition(&mut self, pid: PartitionId) {
        self.segments.remove(&pid);
        self.order.retain(|w| w.pid != pid);
        self.credit.remove(&pid);
    }
}

/// 按写权重把号段总预算切成各分区的**份额**(个数,每个 `≥1`)。
///
/// - `pool = id_segment_size × 分区数`:总预留口径与均权时**不变**(空洞上界、换段频率都不变)。
/// - `share_i = max(1, pool × w_i / Σw)`:`≥1` 保证一个分区(哪怕上报已满、权重落到下限)也不会被
///   完全饿死,同时保证**不存在 0 长度的段**(0 长度的段等于"这个分区不发号",那该由 master 把它转 RO 表达)。
/// - 余数按 **(小数部分降序, pid 升序)** 分配 ⇒ 结果确定、可单测;但下界 `1` 可能让总量略超 `pool`
///   (最多超 `分区数` 个号),此时不再分配余数 —— 总预留只多出可忽略的一小截。
/// - **均权时 `share_i == id_segment_size`**,与改动前逐位一致。
fn segment_shares(pool: u64, weights: &[PartitionWeight]) -> Vec<u64> {
    let total: u64 = weights.iter().map(|w| u64::from(w.weight)).sum();
    if weights.is_empty() || total == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(weights.len());
    let mut rem = Vec::with_capacity(weights.len());
    let mut assigned = 0u64;
    for w in weights {
        let numer = pool.saturating_mul(u64::from(w.weight));
        let share = (numer / total).max(1);
        out.push(share);
        rem.push((numer % total, out.len() - 1));
        assigned = assigned.saturating_add(share);
    }
    rem.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut extra = pool.saturating_sub(assigned);
    for (_, i) in rem {
        if extra == 0 {
            break;
        }
        out[i] += 1;
        extra -= 1;
    }
    out
}

/// 按本次份额**收敛**已有号段的段尾(只减不增 —— 增大段尾等于替 master 多要号,而号只能由 master 发);
/// 返回 `false` 表示收敛后已无号可发(调用方丢掉该段)。
///
/// 收敛掉的余号成为**空洞**(D3 无害)。这一步是"权重变化何时生效"的关键:有了它,分区快满而降权
/// 不必等整池号段自然轮完,在一个刷新周期内就收紧。
fn trim_segment(seg: &mut Segment, share: u32) -> bool {
    seg.end = seg.end.min(seg.next.saturating_add(share));
    seg.next < seg.end
}

/// antfs router(客户端 SDK)。可 `Arc` 共享给多任务并发使用。
pub struct Router {
    cfg: RouterConfig,
    /// master 控制面客户端(tonic 方法需 `&mut self`,故加锁;调用频率 = 每号段一次,不构成瓶颈)。
    master: Mutex<MasterClient<Channel>>,
    http: reqwest::Client,
    /// 号段池。**锁跨取段的 `await` 持有**:号段边界单飞,避免并发调用各取一段。
    ///
    /// `Arc` 是为了让后台刷新任务持 `Weak` 引用 —— 最后一个 `Router` 析构后任务自己退出,不留常驻任务。
    ids: Arc<Mutex<IdCache>>,
    /// `partitionId → 副本位置` 读路由缓存(读路径用;写路径优先用发号响应带回的副本)。
    ///
    /// 缓存里存的是 **master 顺序 + `up` 优先**;延迟排序在**每次读**时另做(见
    /// [`Router::ordered_for_read`])—— 否则测量值变了、缓存里的顺序却一直是旧的。
    topology: Arc<RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>>,
    /// 每副本的观测延迟(读路由按它"先试快的")。
    latency: Arc<LatencyTable>,
    metrics: Arc<RouterMetrics>,
}

/// 一次多数派扇出的结果。
struct Fanout<T> {
    ok: Vec<T>,
    failures: Vec<(String, Error)>,
    total: usize,
    required: usize,
}

impl<T> Fanout<T> {
    fn reached(&self) -> bool {
        self.ok.len() >= self.required
    }

    /// 失败摘要(填进 [`Error::Quorum`] 的 `detail`,压测/排障一眼看出主因)。
    fn detail(&self) -> String {
        if self.failures.is_empty() {
            return "no failures reported".to_string();
        }
        let mut by_kind: HashMap<&'static str, usize> = HashMap::new();
        for (_, e) in &self.failures {
            *by_kind.entry(e.kind()).or_default() += 1;
        }
        let mut pairs: Vec<_> = by_kind.into_iter().collect();
        pairs.sort_unstable();
        pairs
            .into_iter()
            .map(|(k, n)| format!("{k}×{n}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Router {
    /// 连 master(懒连接:首个 RPC 才真正建连)、注册指标,并起**后台刷新任务**(见 [`spawn_refresher`])。
    pub async fn connect(cfg: RouterConfig, registry: &antfs_metric::Registry) -> Result<Self> {
        antfs_config::Validate::validate(&cfg)?;
        let channel = Endpoint::from_shared(format!("http://{}", cfg.master_addr))
            .map_err(Error::Connect)?
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .connect_lazy();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .map_err(Error::HttpClient)?;
        let metrics = Arc::new(RouterMetrics::new(registry)?);
        let ids = Arc::new(Mutex::new(IdCache::default()));
        let topology = Arc::new(RwLock::new(HashMap::new()));
        spawn_refresher(
            &cfg,
            channel.clone(),
            Arc::downgrade(&ids),
            Arc::clone(&topology),
            Arc::clone(&metrics),
        );
        Ok(Router {
            cfg,
            master: Mutex::new(MasterClient::new(channel)),
            http,
            ids,
            topology,
            latency: Arc::new(LatencyTable::new()),
            metrics,
        })
    }

    /// 记一次数据面请求的耗时(毫秒)。**成功与失败都记** —— 超时才是最该被记住的那种"慢"。
    fn note_latency(&self, addr: &str, elapsed: Duration) {
        if !self.cfg.latency_aware_routing {
            return; // 关掉时连表都不维护(零开销)
        }
        self.latency.note(addr, elapsed.as_secs_f64() * 1000.0);
    }

    /// 读路径的副本顺序:**`up` 优先 → 观测延迟升序 → master 原序**。
    ///
    /// - `up == false` 的副本仍排在最后(它们大概率不可达,先试活的);
    /// - 延迟未知的副本按 **已测副本的均值**参与排序(先验:不奖励也不惩罚新人;全都没测时所有键相等,
    ///   排序退化成 master 顺序 —— 即与关闭本特性时**完全一致**);
    /// - 排序**稳定**,故延迟相同(或全无数据)时保持 master 给的顺序,行为可预期。
    fn ordered_for_read(&self, replicas: &[ReplicaLoc]) -> Vec<ReplicaLoc> {
        order_by_latency(replicas, &self.latency, self.cfg.latency_aware_routing)
    }

    /// 本客户端的配置(供 CLI 打印/转发)。
    pub fn config(&self) -> &RouterConfig {
        &self.cfg
    }

    /// 指标(测试对**真 Registry** 断言用)。
    pub fn metrics(&self) -> &RouterMetrics {
        &self.metrics
    }

    // ── 发号 ────────────────────────────────────────────────────────────────────────────────

    /// 申请一个 blobId:本地号段里**按写权重**取(零 RPC);所有分区的号段都取完才对齐一次可写分区列表。
    pub async fn allocate_one(&self) -> Result<StoreKey> {
        let mut cache = self.ids.lock().await;
        if let Some(key) = cache.take_weighted() {
            return Ok(key);
        }
        sync_segments(
            &mut cache,
            &self.master,
            &self.cfg,
            &self.metrics,
            &self.topology,
        )
        .await?;
        cache.take_weighted().ok_or(Error::NoWritablePartition)
    }

    /// 申请 `count` 个 blobId(按分区**写权重**取,可能跨多个分区)。
    pub async fn allocate_batch(&self, count: u32) -> Result<Vec<StoreKey>> {
        let mut out = Vec::with_capacity(count as usize);
        while (out.len() as u32) < count {
            out.push(self.allocate_one().await?);
        }
        Ok(out)
    }

    /// 当前生效的**写权重**(pid 升序;master 的 `write_weight` 千分比,未下发已归一成 `1`)。
    ///
    /// 只读快照,供压测/排障对照"**期望比例 vs 实际分布**" —— 权重是否真的生效,一眼可核。
    pub async fn write_weights(&self) -> Vec<(PartitionId, u32)> {
        self.ids
            .lock()
            .await
            .order
            .iter()
            .map(|w| (w.pid, w.weight))
            .collect()
    }

    /// 分区写满(全副本 507)后把它从本地号段池里摘掉(段/权重/信用一并清除),
    /// 让后续 `allocate_one` 不再挑它。下次 `sync_segments` 若 master 仍列它为 RW 会重新补段。
    pub async fn evict_partition(&self, pid: PartitionId) {
        self.ids.lock().await.evict_partition(pid);
    }

    // ── 读路由 ──────────────────────────────────────────────────────────────────────────────

    /// 该分区的副本位置:优先用缓存(发号响应里带回过),否则向 master `LookupPartition` 拉一次。
    ///
    /// 排序保证 `up` 的副本在前 —— 读路径(R=1)按顺序试,先试活着的。
    async fn replicas_for(&self, pid: PartitionId) -> Result<Vec<ReplicaLoc>> {
        if let Some(rs) = self
            .topology
            .read()
            .expect("topology lock poisoned")
            .get(&pid)
        {
            if !rs.is_empty() {
                return Ok(rs.clone());
            }
        }
        let resp = self
            .master
            .lock()
            .await
            .lookup_partition(LookupRequest {
                version: PROTO_VERSION,
                partition_id: pid,
            })
            .await;
        let resp = match resp {
            Ok(r) => r.into_inner(),
            // master 对未知分区回 gRPC NotFound:归一成"对象/分区不存在",让上层(REST)能映射成 404,
            // 而不是把"查不到"报成服务端错误。
            Err(s) if s.code() == tonic::Code::NotFound => return Err(Error::NotFound { pid }),
            Err(s) => return Err(Error::Master(s)),
        };
        let rs = sorted_replicas(resp.replicas);
        if rs.is_empty() {
            return Err(Error::Rejected(format!("partition {pid} has no replica")));
        }
        self.topology
            .write()
            .expect("topology lock poisoned")
            .insert(pid, rs.clone());
        Ok(rs)
    }

    // ── 写 ─────────────────────────────────────────────────────────────────────────────────

    /// 写一个 blob(不过期):向该 partition 的**全部副本并行 PUT**,收满**多数派** ack 即返回。
    ///
    /// 收满即返回意味着发往其余副本的请求被取消 —— 第 3 份由拉复制补齐(`cluster.md §10`)。
    pub async fn put(&self, key: StoreKey, data: Bytes) -> Result<PutReport> {
        self.put_with_ttl(key, data, NO_EXPIRATION).await
    }

    /// 写一个 blob 并在**写入时定下过期时刻**(`expiration_ms`,`NO_EXPIRATION` = 不过期)。
    ///
    /// 对象不可变,TTL 只有这一次机会确定 —— 没有事后"改期"的路径(要不同 TTL 就换一个 blobId)。
    pub async fn put_with_ttl(
        &self,
        key: StoreKey,
        data: Bytes,
        expiration_ms: i64,
    ) -> Result<PutReport> {
        self.put_keyed(key, data, expiration_ms, false).await
    }

    /// SDK 压缩压缩口:三闸 + 帧封 + 指标(监控 outcome 标签即设计契约,见 router.md)。
    fn compress_for_put(&self, data: Bytes) -> Bytes {
        let cfg = &self.cfg;
        // `connect()` 已过 Validate(算法名白名单):此处不变量成立,expect 属确证不变量例。
        let algo = compression::algo_id_by_name(&cfg.compression_algorithm)
            .expect("compression_algorithm validated in connect");
        match compression::maybe_compress(
            cfg.compression_enabled,
            algo,
            cfg.compression_level,
            cfg.compression_min_size_bytes,
            cfg.compression_min_ratio,
            &data,
        ) {
            compression::Gate::Kept(frame, saved) => {
                self.metrics
                    .compression_attempts
                    .with_label_values(&["kept"])
                    .inc();
                self.metrics.compression_saved_bytes.inc_by(saved);
                frame
            }
            compression::Gate::Skipped(outcome) => {
                self.metrics
                    .compression_attempts
                    .with_label_values(&[outcome])
                    .inc();
                data
            }
        }
    }

    /// [`put_with_ttl`](Self::put_with_ttl) 的组合写形态:数据面置 `composed=true`,落库
    /// 记录带拼装描述符类型位(`POST /stitch` 描述符写口;仲裁语义与 plain 逐字一致)。
    pub async fn put_composed_with_ttl(
        &self,
        key: StoreKey,
        data: Bytes,
        expiration_ms: i64,
    ) -> Result<PutReport> {
        self.put_keyed(key, data, expiration_ms, true).await
    }

    /// 组合写的既定 key 形([`put`](Self::put) 的描述符形态,不过期)。
    pub async fn put_composed(&self, key: StoreKey, data: Bytes) -> Result<PutReport> {
        self.put_composed_with_ttl(key, data, NO_EXPIRATION).await
    }

    /// 两个 key 形的共用仲裁环(环体原样,唯 `put_body` 携带 composed 位)。
    async fn put_keyed(
        &self,
        key: StoreKey,
        data: Bytes,
        expiration_ms: i64,
        composed: bool,
    ) -> Result<PutReport> {
        // SDK 压缩put 三闸,一次判定供全部副本:扇出前的单点 = 复制带宽共收益。
        let data = self.compress_for_put(data);
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let started = Instant::now();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            let data = data.clone();
            futs.push(async move {
                let res = self
                    .put_body(pid, &key, data, expiration_ms, composed, &addr)
                    .await;
                (addr, res)
            });
        }
        let f = collect_majority(futs, total).await;
        let outcome = if f.reached() { "ok" } else { "quorum" };
        self.metrics.put_total.with_label_values(&[outcome]).inc();
        self.metrics
            .put_seconds
            .with_label_values(&[outcome])
            .observe(started.elapsed().as_secs_f64());
        for (_, e) in &f.failures {
            self.metrics
                .put_replica_errors
                .with_label_values(&[e.kind()])
                .inc();
        }
        if f.reached() {
            self.metrics.put_bytes.inc_by(data.len() as u64);
            return Ok(PutReport {
                acks: f.ok.len(),
                total: f.total,
                required: f.required,
            });
        }
        // 全部副本都 507 = 分区写满:key 已绑定该分区,无法换分区重试 —— 摘掉该分区号段,返回 StoreFull。
        if let Some(addr) = all_store_full(&f) {
            self.evict_partition(pid).await;
            return Err(Error::StoreFull { addr });
        }
        // 全部(除 ok 外)副本都 409 = key 已被占/已删:确定性失败,透传冲突而非装可重试。
        if let Some(addr) = all_already_exists(&f) {
            return Err(Error::AlreadyExists { addr });
        }
        Err(Error::Quorum {
            acks: f.ok.len(),
            required: f.required,
            total: f.total,
            detail: f.detail(),
        })
    }

    /// 分配 blobId + 写入(不过期),**分区满时自动换分区重试**(最多 [`MAX_STORE_FULL_RETRIES`] 次)。
    ///
    /// 与 [`put`] 的区别:key 由 router **内部分配**,因此 507 时可以摘掉该分区、重新发号换另一个
    /// 分区重写 —— `put(key, data)` 做不到这一点,因为 key(从而 partition)是调用方决定的。
    ///
    /// 适用于 frontend / S3 网关等"上层不关心落在哪个分区,只要写成功"的场景。
    pub async fn put_auto(&self, data: Bytes) -> Result<(StoreKey, PutReport)> {
        self.put_auto_with_ttl(data, NO_EXPIRATION).await
    }

    /// [`put_auto`](Self::put_auto) + 写入时定 TTL,语义同 [`put_with_ttl`](Self::put_with_ttl)。
    pub async fn put_auto_with_ttl(
        &self,
        data: Bytes,
        expiration_ms: i64,
    ) -> Result<(StoreKey, PutReport)> {
        self.put_auto_keyed(data, expiration_ms, false).await
    }

    /// [`put_auto_with_ttl`](Self::put_auto_with_ttl) 的组合写形态(自动发号 + 类型位;
    /// 自动分片管线的描述符写口)。
    pub async fn put_auto_composed_with_ttl(
        &self,
        data: Bytes,
        expiration_ms: i64,
    ) -> Result<(StoreKey, PutReport)> {
        self.put_auto_keyed(data, expiration_ms, true).await
    }

    /// 两个 auto 形的共用环:发号 → 多数派;全 507 摘分区换号重试。
    async fn put_auto_keyed(
        &self,
        data: Bytes,
        expiration_ms: i64,
        composed: bool,
    ) -> Result<(StoreKey, PutReport)> {
        // SDK 压缩·put 三闸:auto 发号是网关写的主路径(与 put_keyed 对称,一次判定供全部副本)。
        let data = self.compress_for_put(data);
        let mut key = self.allocate_one().await?;
        let mut first_err: Option<Error> = None;
        for attempt in 0..=MAX_STORE_FULL_RETRIES {
            let pid = key.partition_id();
            let replicas = self.replicas_for(pid).await?;
            let total = replicas.len();
            let started = Instant::now();
            let futs = FuturesUnordered::new();
            for r in &replicas {
                let addr = r.addr.clone();
                let data = data.clone();
                futs.push(async move {
                    let res = self
                        .put_body(pid, &key, data, expiration_ms, composed, &addr)
                        .await;
                    (addr, res)
                });
            }
            let f = collect_majority(futs, total).await;
            let outcome = if f.reached() { "ok" } else { "quorum" };
            self.metrics.put_total.with_label_values(&[outcome]).inc();
            self.metrics
                .put_seconds
                .with_label_values(&[outcome])
                .observe(started.elapsed().as_secs_f64());
            for (_, e) in &f.failures {
                self.metrics
                    .put_replica_errors
                    .with_label_values(&[e.kind()])
                    .inc();
            }
            if f.reached() {
                self.metrics.put_bytes.inc_by(data.len() as u64);
                return Ok((
                    key,
                    PutReport {
                        acks: f.ok.len(),
                        total: f.total,
                        required: f.required,
                    },
                ));
            }
            if first_err.is_none() {
                first_err = Some(Error::Quorum {
                    acks: f.ok.len(),
                    required: f.required,
                    total: f.total,
                    detail: f.detail(),
                });
            }
            // 全部(除 ok 外)副本都 409 = key 冲突:确定性失败直接透传,且**不换号重试** ——
            // auto 发的新号撞旧号意味着号被复用(严重问题,见 `Error::AlreadyExists` 注释)。
            if let Some(addr) = all_already_exists(&f) {
                return Err(Error::AlreadyExists { addr });
            }
            // 全部副本都 507 = 分区写满:摘掉该分区、换一个分区重试(最多 MAX_STORE_FULL_RETRIES 次)。
            if all_store_full(&f).is_some() && attempt < MAX_STORE_FULL_RETRIES {
                self.metrics.router_put_store_full_retries.inc();
                self.evict_partition(pid).await;
                key = self.allocate_one().await?;
                continue;
            }
            // 重试耗尽(或非全 507):返回第一次尝试的原始错误。
            return Err(first_err.expect("first_err is set on the first failing attempt"));
        }
        unreachable!("loop always returns")
    }

    /// 上传本地文件:分配 blobId → 并行 PUT 全部副本,**每个副本各自从文件流式读**
    /// (`reqwest::Body::from(File)` 自带 `Content-Length`),内存不随文件大小涨。
    pub async fn put_file(&self, path: impl AsRef<Path>) -> Result<(StoreKey, PutReport)> {
        self.put_file_with_ttl(path, NO_EXPIRATION).await
    }

    /// [`put_file`](Self::put_file) + 写入时定 TTL,语义同 [`put_with_ttl`](Self::put_with_ttl)。
    pub async fn put_file_with_ttl(
        &self,
        path: impl AsRef<Path>,
        expiration_ms: i64,
    ) -> Result<(StoreKey, PutReport)> {
        let path = path.as_ref().to_path_buf();
        let len = tokio::fs::metadata(&path).await?.len();
        let mut key = self.allocate_one().await?;
        let mut first_err: Option<Error> = None;
        for attempt in 0..=MAX_STORE_FULL_RETRIES {
            let pid = key.partition_id();
            let replicas = self.replicas_for(pid).await?;
            let total = replicas.len();
            let started = Instant::now();
            let futs = FuturesUnordered::new();
            for r in &replicas {
                let addr = r.addr.clone();
                let path = path.clone();
                futs.push(async move {
                    let res = self
                        .put_file_to(pid, &key, &path, expiration_ms, &addr)
                        .await;
                    (addr, res)
                });
            }
            let f = collect_majority(futs, total).await;
            let outcome = if f.reached() { "ok" } else { "quorum" };
            self.metrics.put_total.with_label_values(&[outcome]).inc();
            self.metrics
                .put_seconds
                .with_label_values(&[outcome])
                .observe(started.elapsed().as_secs_f64());
            for (_, e) in &f.failures {
                self.metrics
                    .put_replica_errors
                    .with_label_values(&[e.kind()])
                    .inc();
            }
            if f.reached() {
                self.metrics.put_bytes.inc_by(len);
                return Ok((
                    key,
                    PutReport {
                        acks: f.ok.len(),
                        total: f.total,
                        required: f.required,
                    },
                ));
            }
            if first_err.is_none() {
                first_err = Some(Error::Quorum {
                    acks: f.ok.len(),
                    required: f.required,
                    total: f.total,
                    detail: f.detail(),
                });
            }
            // 全部(除 ok 外)副本都 409 = key 冲突:确定性失败直接透传,且**不换号重试** ——
            // auto 发的新号撞旧号意味着号被复用(严重问题,见 `Error::AlreadyExists` 注释)。
            if let Some(addr) = all_already_exists(&f) {
                return Err(Error::AlreadyExists { addr });
            }
            // 全部副本都 507 = 分区写满:摘掉该分区、换一个分区重试(最多 MAX_STORE_FULL_RETRIES 次)。
            if all_store_full(&f).is_some() && attempt < MAX_STORE_FULL_RETRIES {
                self.metrics.router_put_store_full_retries.inc();
                self.evict_partition(pid).await;
                key = self.allocate_one().await?;
                continue;
            }
            // 重试耗尽(或非全 507):返回第一次尝试的原始错误。
            return Err(first_err.expect("first_err is set on the first failing attempt"));
        }
        unreachable!("loop always returns")
    }

    async fn put_body(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        data: Bytes,
        expiration_ms: i64,
        composed: bool,
        addr: &str,
    ) -> Result<()> {
        let t0 = Instant::now();
        let resp = self
            .http
            .put(put_url(addr, pid, key, expiration_ms, composed))
            .body(data)
            .send()
            .await
            .map_err(|source| {
                self.note_latency(addr, t0.elapsed());
                Error::Http {
                    addr: addr.to_string(),
                    source,
                }
            })?;
        self.note_latency(addr, t0.elapsed());
        classify_put(addr, pid, resp).await
    }

    async fn put_file_to(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        path: &Path,
        expiration_ms: i64,
        addr: &str,
    ) -> Result<()> {
        let file = tokio::fs::File::open(path).await?;
        let resp = self
            .http
            .put(put_url(addr, pid, key, expiration_ms, false))
            .body(reqwest::Body::from(file))
            .send()
            .await
            .map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            })?;
        classify_put(addr, pid, resp).await
    }

    // ── 读 ─────────────────────────────────────────────────────────────────────────────────

    /// 读一个 blob:**第一个成功即返回**(R=1)。
    ///
    /// 全部副本都报"不存在"才判 [`Error::NotFound`];若还有副本不可达,则报最后一个真实故障
    /// (不能把"读不到"当成"不存在")。
    pub async fn get(&self, key: &StoreKey) -> Result<Bytes> {
        let pid = key.partition_id();
        let replicas = self.ordered_for_read(&self.replicas_for(pid).await?);
        let mut last: Option<Error> = None;
        let mut missing = 0usize;
        for r in &replicas {
            match self.get_from(pid, key, &r.addr).await {
                Ok(b) => {
                    self.metrics.get_total.with_label_values(&["ok"]).inc();
                    self.metrics.get_bytes.inc_by(b.len() as u64);
                    return Ok(b);
                }
                Err(e @ Error::NotFound { .. }) => {
                    missing += 1;
                    last = Some(e);
                }
                Err(e) => last = Some(e),
            }
        }
        let err = if missing == replicas.len() {
            Error::NotFound { pid }
        } else {
            last.unwrap_or(Error::NotFound { pid })
        };
        self.metrics
            .get_total
            .with_label_values(&[err.kind()])
            .inc();
        Err(err)
    }

    /// 下载到本地文件(流式,内存 = 单块)。
    pub async fn get_file(&self, key: &StoreKey, path: impl AsRef<Path>) -> Result<u64> {
        let pid = key.partition_id();
        let replicas = self.ordered_for_read(&self.replicas_for(pid).await?);
        let mut last: Option<Error> = None;
        let mut missing = 0usize;
        for r in &replicas {
            let addr = r.addr.clone();
            match self.get_stream(pid, key, &addr).await {
                Ok(resp) => return drain_to_file(resp, &addr, path.as_ref(), &self.metrics).await,
                Err(e @ Error::NotFound { .. }) => {
                    missing += 1;
                    last = Some(e);
                }
                Err(e) => last = Some(e),
            }
        }
        let err = if missing == replicas.len() {
            Error::NotFound { pid }
        } else {
            last.unwrap_or(Error::NotFound { pid })
        };
        self.metrics
            .get_total
            .with_label_values(&[err.kind()])
            .inc();
        Err(err)
    }

    async fn get_stream(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        addr: &str,
    ) -> Result<reqwest::Response> {
        let t0 = Instant::now();
        let resp = self
            .http
            .get(blob_url(addr, pid, key))
            .send()
            .await
            .map_err(|source| {
                self.note_latency(addr, t0.elapsed());
                Error::Http {
                    addr: addr.to_string(),
                    source,
                }
            })?;
        self.note_latency(addr, t0.elapsed());
        match resp.status().as_u16() {
            200 => Ok(resp),
            _ => Err(classify_err_status(addr, pid, resp).await),
        }
    }

    async fn get_from(&self, pid: PartitionId, key: &StoreKey, addr: &str) -> Result<Bytes> {
        let resp = self.get_stream(pid, key, addr).await?;
        let raw = resp.bytes().await.map_err(|source| Error::Http {
            addr: addr.to_string(),
            source,
        })?;
        // 压缩帧透明解压(bulk 口):非帧原样透传;帧坏(伪 magic/未知算法/orig_len 不符)→
        // 该副本按损坏处理,上层 get 的 R=1 循环会换下一副本重试。
        compression::decode_frame(&raw).map_err(|detail| Error::ReplicaStatus {
            addr: addr.to_string(),
            status: 500,
            detail,
        })
    }

    /// 读元数据(`HEAD` 语义:不读数据体):第一个成功即返回(R=1)。
    pub async fn stat(&self, key: &StoreKey) -> Result<BlobStat> {
        let pid = key.partition_id();
        let replicas = self.ordered_for_read(&self.replicas_for(pid).await?);
        let mut last: Option<Error> = None;
        let mut missing = 0usize;
        for r in &replicas {
            let addr = r.addr.clone();
            match self.stat_from(pid, key, &addr).await {
                Ok(m) => {
                    self.metrics
                        .mutation_total
                        .with_label_values(&["stat", "ok"])
                        .inc();
                    return Ok(m);
                }
                Err(e @ Error::NotFound { .. }) => {
                    missing += 1;
                    last = Some(e);
                }
                Err(e) => last = Some(e),
            }
        }
        let err = if missing == replicas.len() {
            Error::NotFound { pid }
        } else {
            last.unwrap_or(Error::NotFound { pid })
        };
        self.metrics
            .mutation_total
            .with_label_values(&["stat", err.kind()])
            .inc();
        Err(err)
    }

    async fn stat_from(&self, pid: PartitionId, key: &StoreKey, addr: &str) -> Result<BlobStat> {
        let url = format!(
            "http://{addr}/partitions/{pid}/blobs/{}/metadata",
            hex_key(key)
        );
        let t0 = Instant::now();
        let resp = self.http.get(url).send().await.map_err(|source| {
            self.note_latency(addr, t0.elapsed());
            Error::Http {
                addr: addr.to_string(),
                source,
            }
        })?;
        self.note_latency(addr, t0.elapsed());
        if resp.status().as_u16() == 200 {
            return resp.json::<BlobStat>().await.map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            });
        }
        Err(classify_err_status(addr, pid, resp).await)
    }

    // ── 变更(delete)──────────────────────────────────────────────────────────────────────

    /// 删除(写墓碑):多数派 ack。墓碑 key 重复删幂等成功(204);未存在的 key 报 `NotFound`。
    pub async fn delete(&self, key: &StoreKey) -> Result<PutReport> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            futs.push(async move {
                let res = self.mutate_one(pid, key, &addr).await;
                (addr, res)
            });
        }
        let f = collect_majority(futs, total).await;
        self.record_mutation("delete", &f);
        if f.reached() {
            Ok(PutReport {
                acks: f.ok.len(),
                total: f.total,
                required: f.required,
            })
        } else {
            Err(Error::Quorum {
                acks: f.ok.len(),
                required: f.required,
                total: f.total,
                detail: f.detail(),
            })
        }
    }

    fn record_mutation(&self, op: &'static str, f: &Fanout<Option<u16>>) {
        let outcome = if f.reached() { "ok" } else { "quorum" };
        self.metrics
            .mutation_total
            .with_label_values(&[op, outcome])
            .inc();
        for (_, e) in &f.failures {
            self.metrics
                .put_replica_errors
                .with_label_values(&[e.kind()])
                .inc();
        }
    }

    /// 单副本删除请求(写墓碑)。
    async fn mutate_one(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        addr: &str,
    ) -> Result<Option<u16>> {
        let resp = self
            .http
            .delete(blob_url(addr, pid, key))
            .send()
            .await
            .map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            })?;
        match resp.status().as_u16() {
            204 => Ok(None),
            _ => Err(classify_err_status(addr, pid, resp).await),
        }
    }
}

// ── 号段 ↔ 可写分区列表对齐(热路径补段 + 后台刷新任务共用)────────────────────────────────────

/// 起**后台刷新任务**:按 `cfg.partition_refresh_secs` 周期向 master 拉可写分区列表并调账本地号段
/// (已不可写的分区丢掉号段,新出现的可写分区补一段)。`partition_refresh_secs = 0` 不起任务。
///
/// 任务持号段池的 `Weak`:最后一个 `Router` 析构后引用计数归零,任务下一拍自己退出。
/// 刷新失败只计 `router_partition_refresh_total{outcome="error"}` —— 后台刷新是**增强**(让缓存跟上
/// 分区增减),不是正确性依赖:号段耗尽的写路径自己会 [`sync_segments`] 一次。
fn spawn_refresher(
    cfg: &RouterConfig,
    channel: Channel,
    ids: Weak<Mutex<IdCache>>,
    topology: Arc<RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>>,
    metrics: Arc<RouterMetrics>,
) {
    if cfg.partition_refresh_secs == 0 {
        return;
    }
    let period = Duration::from_secs(cfg.partition_refresh_secs);
    let cfg = cfg.clone();
    tokio::spawn(async move {
        // 自带的控制面 client(与写路径各自独立,不争同一把锁)。
        let master = Mutex::new(MasterClient::new(channel));
        loop {
            let Some(cache) = ids.upgrade() else {
                return;
            };
            let res = {
                let mut cache = cache.lock().await;
                sync_segments(&mut cache, &master, &cfg, &metrics, &topology).await
            };
            let outcome = if res.is_ok() { "ok" } else { "error" };
            metrics
                .partition_refresh_total
                .with_label_values(&[outcome])
                .inc();
            tokio::time::sleep(period).await;
        }
    });
}

/// 一次「可写分区列表 ↔ 本地号段」对齐:
/// 拉列表(+ **写权重**)→ **丢掉**已不在列表里的分区的号段(余号成空洞,无害,D3)→ 按权重把号段总预算
/// 切成各分区的份额 → 已有段按份额**收敛段尾**、缺段的分区**各申请一批**(显式点名 `partition_id`,
/// `count` = 该分区份额)。
///
/// - 分区集合与合法性由 master 给:router 只从权威 RW 列表里取分区、**不自造 id**、**不自算容量权重**
///   (权重照抄 master 的 `PartitionEntry.write_weight`,只当相对值用),且每个分区在发号时被 master
///   复核(RW + 至少一个副本)。这补回了 D10"按剩余容量加权"的容量感知,粒度 = 补齐/刷新的这一拍。
/// - **已有段也要收敛**:权重下降(分区快满)要在一个刷新周期内生效,而不是等整池号轮完;收敛掉的
///   余号成空洞(D3 无害)。
/// - 单个分区取号失败(期间被改成 RO/封存、号耗尽、已下线)只跳过它,**不影响其余分区**。
/// - 取段并发(`master` client 克隆后各自调用);整轮在号段锁里跑 —— 热路径借它单飞,后台任务借它
///   与取号互斥。缓存补齐后仍一个号段都没有才算失败。
async fn sync_segments(
    cache: &mut IdCache,
    master: &Mutex<MasterClient<Channel>>,
    cfg: &RouterConfig,
    metrics: &RouterMetrics,
    topology: &RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>,
) -> Result<()> {
    let order = list_writable(master).await?;
    let writable: HashSet<PartitionId> = order.iter().map(|w| w.pid).collect();
    cache.segments.retain(|pid, _| writable.contains(pid));

    // 总预算(分区数 × id_segment_size)按写权重切给各分区 —— 逐个分区"本轮该发多少个号"。
    let n = u64::try_from(order.len()).unwrap_or(u64::MAX);
    let shares = segment_shares(u64::from(cfg.id_segment_size).saturating_mul(n), &order);

    let mut want: Vec<(PartitionId, u32)> = Vec::new();
    let mut exhausted: Vec<PartitionId> = Vec::new();
    for (slot, share) in order.iter().zip(&shares) {
        let share = u32::try_from(*share).unwrap_or(u32::MAX);
        match cache.segments.get_mut(&slot.pid) {
            // 已有段:按本次份额收敛段尾(见 [`trim_segment`])。
            Some(seg) => {
                if !trim_segment(seg, share) {
                    exhausted.push(slot.pid);
                }
            }
            None => want.push((slot.pid, share)),
        }
    }
    // 收敛到没号的段直接丢掉(取号本就会跳过它;留着只会让 `writable_partitions` 虚高)。
    for pid in exhausted {
        cache.segments.remove(&pid);
    }
    cache.order = order;
    // 信用只留给"手上有号段"的分区:补段/收敛后的分区从 0 起,不留空转期攒下的信用变成突发。
    cache
        .credit
        .retain(|pid, _| cache.segments.contains_key(pid));

    let mut pending = FuturesUnordered::new();
    for (pid, size) in want {
        let mut client = master.lock().await.clone();
        let req = GetBlobIdsRequest {
            version: PROTO_VERSION,
            cluster_secret: cfg.cluster_secret.clone(),
            count: size,
            partition_id: Some(pid),
        };
        pending.push(async move { client.get_blob_ids(req).await });
    }

    let mut last_err: Option<Error> = None;
    while let Some(resp) = pending.next().await {
        match resp {
            Ok(resp) => match segment_from_response(resp.into_inner(), metrics, topology) {
                Ok(seg) => {
                    cache.segments.insert(seg.pid, seg);
                }
                Err(e) => last_err = Some(e),
            },
            Err(status) => last_err = Some(Error::Master(status)),
        }
    }
    metrics.writable_partitions.set(cache.segments.len() as i64);
    if cache.segments.is_empty() {
        return Err(last_err.unwrap_or(Error::NoWritablePartition));
    }
    Ok(())
}

/// master 当前可写(RW)分区列表 + **写权重**(pid 升序)。跳过没有副本的分区 —— 挑了它只会拿到一个
/// 必然写不进去的号段。
///
/// 权重**照抄** master 的 `PartitionEntry.write_weight`(千分比 `1..=1000`);`0` = 未下发 / 容量未配
/// → 归一成 `1`(均权),于是"master 还不下发权重"时行为与纯轮转**逐位一致**(§3.4 兼容)。
/// router **不解释容量语义**:权重怎么算(master 按剩余占比现算,将来 admin 手动设置)与客户端无关。
async fn list_writable(master: &Mutex<MasterClient<Channel>>) -> Result<Vec<PartitionWeight>> {
    let resp = master
        .lock()
        .await
        .list_partitions(ListPartitionsRequest {
            version: PROTO_VERSION,
            offset: 0,
            limit: 0, // 0 = 不限,一次取全
            state: "RW".to_string(),
        })
        .await
        .map_err(Error::Master)?
        .into_inner();
    Ok(resp
        .partitions
        .into_iter()
        .filter(|p| !p.replicas.is_empty())
        .map(|p| PartitionWeight {
            pid: p.partition_id,
            weight: p.write_weight.max(1),
        })
        .collect())
}

/// 发号响应 → 一段号段(两层批处理的第一层;水位抬升由 master 侧完成)。顺带把响应里的副本位置写进读路由缓存。
fn segment_from_response(
    resp: GetBlobIdsResponse,
    metrics: &RouterMetrics,
    topology: &RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>,
) -> Result<Segment> {
    if !resp.ok {
        return Err(map_master_error(&resp.error, resp.partition_id));
    }
    if resp.count == 0 {
        return Err(Error::Rejected(
            "master returned an empty id segment".into(),
        ));
    }
    let start = u32::try_from(resp.start_object_id).map_err(|_| {
        Error::Rejected(format!(
            "start_object_id {} overflows u32",
            resp.start_object_id
        ))
    })?;
    let pid = resp.partition_id;
    metrics.id_segments.inc();
    metrics.id_segment_ids.inc_by(resp.count as u64);
    topology
        .write()
        .expect("topology lock poisoned")
        .insert(pid, sorted_replicas(resp.replicas));
    Ok(Segment {
        pid,
        next: start,
        end: start.saturating_add(resp.count),
    })
}

/// 多数派扇形收集:并行结果里收满 `total / 2 + 1` 个成功即返回(其余未完成的不再等)。
///
/// 提前返回意味着"凑不够了"(剩余未完成的即使全成功也不够),避免无谓等待。
async fn collect_majority<T>(
    mut futs: FuturesUnordered<impl std::future::Future<Output = (String, Result<T>)>>,
    total: usize,
) -> Fanout<T> {
    let required = total / 2 + 1;
    let mut ok = Vec::new();
    let mut failures = Vec::new();
    while let Some((addr, res)) = futs.next().await {
        match res {
            Ok(v) => ok.push(v),
            Err(e) => failures.push((addr, e)),
        }
        if ok.len() >= required || ok.len() + futs.len() < required {
            break;
        }
    }
    Fanout {
        ok,
        failures,
        total,
        required,
    }
}

/// 全部副本都报 507(StoreFull)= 分区写满;返回第一个报满的副本地址。
///
/// 部分 507(其余成功或其它错误)≠ 分区满 —— 多数派可能仍可达,不触发换分区重试。
/// `collect_majority` 可能提前返回(剩余未完成的 future 被丢弃),故只看**已收集到的失败**。
fn all_store_full<T>(f: &Fanout<T>) -> Option<String> {
    if f.failures.is_empty() {
        return None;
    }
    f.failures
        .iter()
        .all(|(_, e)| matches!(e, Error::StoreFull { .. }))
        .then(|| f.failures[0].0.clone())
}

/// 全部已收集失败均为 409(AlreadyExists)= 确定性 key 冲写;返回首个报冲突的副本地址。
///
/// 口径与 [`all_store_full`] 一致(只看已收集失败;多数派未达时 `collect_majority` 必然
/// 等全部 future settle,故 all = 除 ok 外全部副本报 409)。判定只在 `reached()==false`
/// 之后调用:有 1 副本成功、其余全 409(如 W=2 得 1 ok + 2 409)仍判成立 —— 该场景
/// 数据已在多数副本位,重试必然再撞,透传冲突比装可重试诚实。
/// 部分失败为 409、部分为其它错误时维持 Quorum(判定不了就按暂时性失败处理)。
fn all_already_exists<T>(f: &Fanout<T>) -> Option<String> {
    if f.failures.is_empty() {
        return None;
    }
    f.failures
        .iter()
        .all(|(_, e)| matches!(e, Error::AlreadyExists { .. }))
        .then(|| f.failures[0].0.clone())
}

/// 副本位置排序:`up` 的在前(读路径按序试,先试活着的),同组保持 master 给的顺序。
fn sorted_replicas(mut replicas: Vec<ReplicaLoc>) -> Vec<ReplicaLoc> {
    replicas.sort_by_key(|r| !r.up);
    replicas
}

/// 读路径的副本排序(抽成自由函数,便于单测 —— [`Router::ordered_for_read`] 只是它的薄封装):
/// **`up` 优先 → 观测延迟升序 → 原序**。
///
/// - `enabled == false` ⇒ 原样返回(恒等变换);
/// - `up == false` 的副本仍排最后(它们大概率不可达,先试活的);
/// - 延迟未知的副本按**已测副本的均值**参与排序(先验:不奖励也不惩罚新人;全都没测时所有键相等,
///   排序退化成原序 —— 即与关闭本特性时**完全一致**);
/// - 排序**稳定**,故延迟相同(或全无数据)时保持 master 给的顺序,行为可预期。
fn order_by_latency(
    replicas: &[ReplicaLoc],
    latency: &LatencyTable,
    enabled: bool,
) -> Vec<ReplicaLoc> {
    if !enabled {
        return replicas.to_vec();
    }
    let prior = latency.mean_ms();
    let key = |r: &ReplicaLoc| -> (bool, f64) {
        let ms = latency.get(&r.addr).or(prior).unwrap_or(0.0);
        (!r.up, ms)
    };
    let mut out = replicas.to_vec();
    out.sort_by(|a, b| {
        let (ua, ma) = key(a);
        let (ub, mb) = key(b);
        ua.cmp(&ub).then(ma.total_cmp(&mb))
    });
    out
}

/// **每副本的观测延迟**(毫秒,指数移动平均)—— 读路径据此"先试快的"。
///
/// ## 为什么只做这一件事
///
/// 读恒为 R=1(第一个成功即返回),故**试副本的顺序直接决定读延迟**;而慢副本恰恰是最该被绕开的东西。
/// 这里**只改顺序、不改候选集** ⇒ 读的候选、写的 W 多数派、冲突解决全都不变,**不可能引入新的正确性问题**
/// (慢副本本来就可能是任一被试到的副本;一致性模型早已允许"试到任意副本")。
///
/// 诚实边界:顺序变化会改变"你**先**看到哪个副本的状态"。而对象不可变 + 删除是墓碑,滞后副本可能
/// 暂时读不到(→ 404 → 试下一个)或暂时看到已删对象仍活着 —— **这个风险在任意顺序下都存在**,
/// 本节只改变"先撞上哪一个",不引入新的语义。
///
/// ## 平滑系数的取舍
///
/// `ALPHA = 0.2`(写死,不做配置):≈ 最近 5 次请求的等效窗口。太小则跟不上盘的变化,太大则一次抖动
/// 就把好副本排到后面。**它不需要按部署标定** —— 这个排序是"尽力而为的启发",错了的代价只是
/// "这次读多花一点"(而不是像 `disk_watchdog_ms` 那样错了就摘盘),所以不值得为它加旋钮。
struct LatencyTable {
    /// `addr -> EWMA(毫秒)`。用 `addr` 而不是 `node_id`:它直接对应 HTTP 目标,与 master 的 id 无关。
    ///
    /// **标准库 `Mutex`**(不是 `tokio::sync::Mutex`):临界区里没有任何 `await`(只做几次浮点运算),
    /// 用异步锁反而要求调用方 `await`,还会把"排序"这件事变成一次跨 yield 的操作。
    ewma_ms: std::sync::Mutex<HashMap<String, f64>>,
}

/// EWMA 平滑系数(见 [`LatencyTable`] 的说明:刻意不做配置项)。
const LATENCY_ALPHA: f64 = 0.2;

impl LatencyTable {
    fn new() -> Self {
        LatencyTable {
            ewma_ms: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// 记一次观测(成功或失败都记 —— **超时才是最该被记住的那种"慢"**)。
    fn note(&self, addr: &str, ms: f64) {
        let mut g = self.ewma_ms.lock().unwrap_or_else(|e| e.into_inner());
        match g.get_mut(addr) {
            Some(v) => *v = LATENCY_ALPHA * ms + (1.0 - LATENCY_ALPHA) * *v,
            None => {
                g.insert(addr.to_string(), ms);
            }
        }
    }

    /// 已测副本的均值(没有任何测量 → `None`)。**未测副本用它当先验**:
    /// 既不奖励也不惩罚一个刚出现的副本,而"全都没测"时所有键相等 ⇒ 排序退化为 master 顺序。
    fn mean_ms(&self) -> Option<f64> {
        let g = self.ewma_ms.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_empty() {
            return None;
        }
        Some(g.values().sum::<f64>() / g.len() as f64)
    }

    fn get(&self, addr: &str) -> Option<f64> {
        self.ewma_ms
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(addr)
            .copied()
    }
}

fn hex_key(key: &StoreKey) -> String {
    hex::encode(key.as_bytes())
}

fn blob_url(addr: &str, pid: PartitionId, key: &StoreKey) -> String {
    format!("http://{addr}/partitions/{pid}/blobs/{}", hex_key(key))
}

/// PUT 的 URL:`expiration_ms` 非 [`NO_EXPIRATION`] 时带上写入时定下的 TTL;组合写
/// 追加 `composed=true`(数据面 `PUT ?expiration_ms=…&composed=true`)。
fn put_url(
    addr: &str,
    pid: PartitionId,
    key: &StoreKey,
    expiration_ms: i64,
    composed: bool,
) -> String {
    let base = blob_url(addr, pid, key);
    let mut query: Vec<String> = Vec::new();
    if expiration_ms != NO_EXPIRATION {
        query.push(format!("expiration_ms={expiration_ms}"));
    }
    if composed {
        query.push("composed=true".into());
    }
    if query.is_empty() {
        base
    } else {
        format!("{base}?{}", query.join("&"))
    }
}

/// 非 2xx 的状态码 → 归一错误(写/读/变更共用同一套语义)。
///
/// 消费掉响应体(状态码 + 文案一起判定),故调用方在 2xx 分支之外才能调用它。
async fn classify_err_status(addr: &str, pid: PartitionId, resp: reqwest::Response) -> Error {
    let status = resp.status().as_u16();
    let retry = retry_after_secs(&resp);
    let body = resp.text().await.unwrap_or_default();
    match status {
        // store 对"无此副本"与"对象不存在"都回 404;按响应体区分(文案是 store 的稳定契约)。
        404 if body.contains("no replica for partition") => Error::NoReplica {
            addr: addr.to_string(),
            pid,
        },
        404 => Error::NotFound { pid },
        409 => Error::AlreadyExists {
            addr: addr.to_string(),
        },
        507 => Error::StoreFull {
            addr: addr.to_string(),
        },
        503 => Error::Silenced {
            addr: addr.to_string(),
            retry_after_secs: retry,
        },
        s => Error::ReplicaStatus {
            addr: addr.to_string(),
            status: s,
            detail: body,
        },
    }
}

/// 写响应归一:201 = ack;其余按 [`classify_err_status`]。
async fn classify_put(addr: &str, pid: PartitionId, resp: reqwest::Response) -> Result<()> {
    if resp.status().as_u16() == 201 {
        return Ok(());
    }
    Err(classify_err_status(addr, pid, resp).await)
}

/// `Retry-After`(秒)。store 的请求静默会带上;缺失(永久静默)= `None`。
fn retry_after_secs(resp: &reqwest::Response) -> Option<u64> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// 把响应体流式写进本地文件(内存 = 单块)。
async fn drain_to_file(
    resp: reqwest::Response,
    addr: &str,
    path: &Path,
    metrics: &RouterMetrics,
) -> Result<u64> {
    use tokio::io::AsyncWriteExt;

    let mut stream = resp.bytes_stream();
    use bytes::BytesMut;
    // 窥满 17B(不足帧头长的短 body 也按非帧处理);peek 期间多收的部分存 `carry`,
    // 判定**之后**才决定去向 —— 不得在判帧前就把流引去解码管道。
    let mut head = BytesMut::with_capacity(compression::FRAME_HEADER);
    let mut carry: Vec<Bytes> = Vec::new();
    while head.len() < compression::FRAME_HEADER {
        match stream.next().await {
            Some(chunk) => {
                let chunk = chunk.map_err(|source| Error::Http {
                    addr: addr.to_string(),
                    source,
                })?;
                let want = compression::FRAME_HEADER - head.len();
                if chunk.len() > want {
                    head.extend_from_slice(&chunk[..want]);
                    carry.push(chunk.slice(want..));
                } else {
                    head.extend_from_slice(&chunk);
                }
            }
            None => break,
        }
    }
    if compression::is_frame(&head) {
        // 帧:多收部分依序首喂解码源(pipe 前置一段 spill ula)。
        return drain_compressed(
            &mut stream,
            carry.first().cloned().unwrap_or_default(),
            head.freeze(),
            addr,
            path,
            metrics,
        )
        .await;
    }
    // 溢收的独立块(理论上单 chunk 溢出即走 carry[0] 单块)—— 防御:全部走非帧写出。
    let spilled: Vec<Bytes> = carry;
    // 非帧:普通直落盘(peek 出的 head 与溢收块补写)。
    let mut file = tokio::fs::File::create(path).await?;
    let mut n = 0u64;
    if !head.is_empty() {
        n += head.len() as u64;
        file.write_all(&head).await?;
    }
    for b in &spilled {
        n += b.len() as u64;
        file.write_all(b).await?;
    }
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| Error::Http {
            addr: addr.to_string(),
            source,
        })?;
        n += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    file.sync_all().await?;
    metrics.get_total.with_label_values(&["ok"]).inc();
    metrics.get_bytes.inc_by(n);
    Ok(n)
}

/// 压缩帧 → 文件:解码线程(spawn_blocking)经**一条**有界通道接网络,内存 = 块级。
/// `head`:已窥出的 17B 帧头;`first`:peek 期溢收的首块(无则空)。
async fn drain_compressed(
    stream: &mut (impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin),
    first: bytes::Bytes,
    head: bytes::Bytes,
    addr: &str,
    path: &Path,
    _metrics: &RouterMetrics,
) -> Result<u64> {
    // 网络块 → 有界通道(容量 4);解码线程(下)阻塞拉取喂同步 Decoder,天然背压。
    let (net_tx, net_rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(4);
    let dec_path = path.to_path_buf();
    let decode = tokio::task::spawn_blocking(move || -> std::result::Result<u64, String> {
        /// 网络通道 → `std::io::Read` 桥(帧头已被消费,源从 peek 余部续起)。
        struct NetSource {
            rx: tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
            front: std::io::Cursor<bytes::Bytes>,
        }
        impl std::io::Read for NetSource {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                loop {
                    if self.front.position() < self.front.get_ref().len() as u64 {
                        return std::io::Read::read(&mut self.front, buf);
                    }
                    match self.rx.blocking_recv() {
                        Some(Ok(b)) => self.front = std::io::Cursor::new(b),
                        Some(Err(e)) => return Err(e),
                        None => return Ok(0),
                    }
                }
            }
        }
        let (mut decoder, orig_len, algo) = compression::streaming_decoder(
            &head,
            Box::new(NetSource {
                rx: net_rx,
                front: std::io::Cursor::new(first),
            }),
        )
        .ok_or_else(|| "not a frame".to_string())?;
        let mut file = std::fs::File::create(&dec_path).map_err(|e| format!("create: {e}"))?;
        let mut produced = 0u64;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            use std::io::Read as _;
            let n = decoder
                .read(&mut buf)
                .map_err(|e| format!("{algo} decode: {e}"))?;
            if n == 0 {
                break;
            }
            produced += n as u64;
            if produced > orig_len {
                return Err(format!(
                    "{algo} decode produced {produced} > declared orig_len {orig_len}"
                ));
            }
            std::io::Write::write_all(&mut file, &buf[..n]).map_err(|e| format!("write: {e}"))?;
        }
        if produced != orig_len {
            return Err(format!(
                "{algo} decode produced {produced} < declared orig_len {orig_len}"
            ));
        }
        file.sync_all().map_err(|e| format!("fsync: {e}"))?;
        Ok(produced)
    });

    // async 侧:喂通道 + 收集解码结果(stream 错误优先上报)。
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| Error::Http {
            addr: addr.to_string(),
            source,
        })?;
        net_tx
            .send(Ok(chunk))
            .await
            .map_err(|_| Error::ReplicaStatus {
                addr: addr.to_string(),
                status: 500,
                detail: "decode worker exited early".to_string(),
            })?;
    }
    drop(net_tx);
    let produced = decode
        .await
        .map_err(|e| Error::ReplicaStatus {
            addr: addr.to_string(),
            status: 500,
            detail: format!("decode join: {e}"),
        })?
        .map_err(|detail| Error::ReplicaStatus {
            addr: addr.to_string(),
            status: 500,
            detail,
        })?;
    Ok(produced)
}

/// master 的业务性拒绝(`ok=false`)→ 归一错误。
fn map_master_error(error: &str, pid: PartitionId) -> Error {
    if error.contains("not leader") {
        Error::Rejected(format!(
            "master is not the leader ({error}); point client.master_addr at the leader"
        ))
    } else if error.contains("no writable partition") {
        Error::NoWritablePartition
    } else if error.contains("exhausted") {
        Error::IdExhausted(pid)
    } else {
        Error::Rejected(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 滚动升级读接缝:旧 store 的 metadata JSON 无 composed 字段 → false 不炸;
    /// 新 store 回 true。
    #[test]
    fn blob_stat_tolerates_missing_composed_field() {
        let old = serde_json::json!({
            "size": 10,
            "expiration_ms": -1,
            "deleted": false,
            "expired": false
        });
        let stat: BlobStat = serde_json::from_value(old).expect("old store JSON");
        assert!(!stat.composed);

        let fresh = serde_json::json!({
            "size": 10,
            "expiration_ms": -1,
            "deleted": false,
            "expired": false,
            "composed": true
        });
        let stat: BlobStat = serde_json::from_value(fresh).expect("new store JSON");
        assert!(stat.composed);
    }

    fn loc(node_id: u64, addr: &str, up: bool) -> ReplicaLoc {
        ReplicaLoc {
            node_id,
            addr: addr.to_string(),
            disk_id: "d0".to_string(),
            target_state: "ACTIVE".to_string(),
            up,
            incarnation: 1,
        }
    }

    /// **延迟感知排序的直接收益**:快的副本被先试。
    ///
    /// 读恒为 R=1(第一个成功即返回),故"先试谁"直接决定读延迟 —— 这是这一整块改动存在的理由。
    #[test]
    fn latency_order_prefers_the_faster_replica() {
        let t = LatencyTable::new();
        t.note("slow:1", 200.0);
        t.note("fast:1", 5.0);
        let rs = vec![
            loc(1, "slow:1", true), // master 把它排第一
            loc(2, "fast:1", true),
        ];
        let out = order_by_latency(&rs, &t, true);
        assert_eq!(out[0].addr, "fast:1", "实测快的那份要被先试");
        assert_eq!(out[1].addr, "slow:1");
        assert_eq!(out.len(), 2, "候选集不变(只改顺序)");
    }

    /// **没有任何测量 ⇒ 恒等变换**:新增特性在"还没数据"时不能改变任何行为。
    #[test]
    fn latency_order_is_identity_without_measurements() {
        let t = LatencyTable::new();
        let rs = vec![
            loc(3, "c:1", true),
            loc(1, "a:1", true),
            loc(2, "b:1", true),
        ];
        let out = order_by_latency(&rs, &t, true);
        let addrs: Vec<&str> = out.iter().map(|r| r.addr.as_str()).collect();
        assert_eq!(addrs, vec!["c:1", "a:1", "b:1"], "保持 master 原序");
    }

    /// **关掉时也是恒等变换**(留出对照/排查用的确定性顺序)。
    #[test]
    fn latency_order_disabled_is_identity() {
        let t = LatencyTable::new();
        t.note("a:1", 500.0);
        t.note("b:1", 1.0);
        let rs = vec![loc(1, "a:1", true), loc(2, "b:1", true)];
        let out = order_by_latency(&rs, &t, false);
        let addrs: Vec<&str> = out.iter().map(|r| r.addr.as_str()).collect();
        assert_eq!(addrs, vec!["a:1", "b:1"], "关掉后顺序完全由 master 决定");
    }

    /// **`up = false` 的副本永远排最后** —— 哪怕它历史延迟最低。
    ///
    /// master 已经告诉我们它不健康;把一次读先拿去撞它,只会白白等一次超时。
    #[test]
    fn latency_order_keeps_down_replicas_last() {
        let t = LatencyTable::new();
        t.note("down:1", 1.0); // 历史很快,但自己是 down
        t.note("up:1", 300.0);
        let rs = vec![loc(1, "down:1", false), loc(2, "up:1", true)];
        let out = order_by_latency(&rs, &t, true);
        assert_eq!(out[0].addr, "up:1", "活的优先,与延迟无关");
        assert_eq!(out[1].addr, "down:1");
    }

    /// **未测副本用"已测均值"当先验**:既不奖励也不惩罚新副本 ——
    /// 比已测**慢**的那份靠前,比已测**快**的那份靠后。
    #[test]
    fn unmeasured_replica_uses_the_mean_as_prior() {
        let t = LatencyTable::new();
        t.note("fast:1", 5.0);
        t.note("slow:1", 100.0);
        // 均值 = 52.5 ⇒ 期望顺序:fast(5) < new(52.5) < slow(100)
        let rs = vec![
            loc(1, "slow:1", true),
            loc(2, "fast:1", true),
            loc(3, "new:1", true),
        ];
        let out = order_by_latency(&rs, &t, true);
        let addrs: Vec<&str> = out.iter().map(|r| r.addr.as_str()).collect();
        assert_eq!(addrs, vec!["fast:1", "new:1", "slow:1"]);
    }

    /// EWMA 朝最近观测移动,且量级正确(第一次观测即初始值)。
    #[test]
    fn latency_ewma_moves_toward_recent_observations() {
        let t = LatencyTable::new();
        t.note("a:1", 100.0);
        assert_eq!(t.get("a:1"), Some(100.0), "第一次观测即初值");
        t.note("a:1", 200.0);
        // 0.2*200 + 0.8*100 = 120
        let v = t.get("a:1").expect("有值");
        assert!(
            (v - 120.0).abs() < 1e-9,
            "EWMA 应按 alpha=0.2 平滑,实际 {v}"
        );
        assert_eq!(t.get("never:1"), None, "没测过的副本没有值");
        assert_eq!(LatencyTable::new().mean_ms(), None, "空表没有均值");
    }

    /// 段 [start, start+count) —— 与发号响应同构(`next` 含、`end` 不含)。
    fn seg(pid: PartitionId, start: u32, count: u32) -> Segment {
        Segment {
            pid,
            next: start,
            end: start + count,
        }
    }

    /// 缓存里装上各分区的一段号,轮转顺序 = `pids`。
    /// 缓存里装上各分区的一段号 + 写权重,取号顺序 = `weights` 的顺序。
    fn cache_with(weights: &[(PartitionId, u32)], count: u32) -> IdCache {
        let mut c = IdCache {
            order: weights
                .iter()
                .map(|(pid, weight)| PartitionWeight {
                    pid: *pid,
                    weight: *weight,
                })
                .collect(),
            ..Default::default()
        };
        for (i, (pid, _)) in weights.iter().enumerate() {
            c.segments
                .insert(*pid, seg(*pid, 100 * (i as u32 + 1), count));
        }
        c
    }

    /// 均权时:取号在持有号段的分区之间**交替** —— 与加权之前的顺序轮转逐位一致,是兼容性基线。
    #[test]
    fn uniform_weights_rotate_across_partitions() {
        let mut c = cache_with(&[(1, 1), (2, 1), (3, 1)], 2);
        let pids: Vec<PartitionId> = (0..6)
            .map(|_| c.take_weighted().expect("有号可取").partition_id())
            .collect();
        assert_eq!(pids, vec![1, 2, 3, 1, 2, 3], "均权必须按分区交替");
        assert!(c.take_weighted().is_none(), "号取完 → 交给调用方补段");
        assert!(c.segments.is_empty(), "取完的段必须被丢掉");
    }

    /// **核心行为**:权重决定取号比例,且**在短窗口内就生效** —— 这正是"只按权重切段大小、取号仍顺序
    /// 轮转"做不到的(那种做法在整池轮完之前,各分区的计数一直相等)。
    #[test]
    fn weights_steer_allocation_within_a_short_window() {
        let mut c = cache_with(&[(1, 1000), (2, 500), (3, 500)], 100);
        let mut by_pid: HashMap<PartitionId, u32> = HashMap::new();
        for _ in 0..20 {
            *by_pid
                .entry(c.take_weighted().expect("有号可取").partition_id())
                .or_default() += 1;
        }
        assert_eq!(
            (
                by_pid.get(&1).copied().unwrap_or(0),
                by_pid.get(&2).copied().unwrap_or(0),
                by_pid.get(&3).copied().unwrap_or(0)
            ),
            (10, 5, 5),
            "20 个号必须按 1000:500:500 分配(不是轮完一整池才均等)"
        );
    }

    /// 权重 `0`(旧 master 未下发 / 容量未配)按**均权**处理 —— 不能解释成"零分配"。
    #[test]
    fn zero_weight_means_uniform_not_zero() {
        let mut c = cache_with(&[(1, 0), (2, 1)], 4);
        let pids: Vec<PartitionId> = (0..4)
            .map(|_| c.take_weighted().expect("有号").partition_id())
            .collect();
        assert_eq!(pids, vec![1, 2, 1, 2], "0 权重 = 均权,不能被饿死");
    }

    /// 一段只有一个号(count=1)也必须发得出去 —— `next` 含 / `end` 不含 的边界。
    #[test]
    fn take_weighted_handles_single_id_segment() {
        let mut c = cache_with(&[(7, 1)], 1);
        let key = c.take_weighted().expect("单号段仍可发一个号");
        assert_eq!((key.partition_id(), key.object_id()), (7, 100));
        assert!(c.take_weighted().is_none());
    }

    /// 某分区的段取完后跳过它,其余分区继续取;空缓存返回 None。
    #[test]
    fn take_weighted_skips_exhausted_and_empty_cache_returns_none() {
        assert!(IdCache::default().take_weighted().is_none());
        let mut c = cache_with(&[(1, 1), (2, 1)], 1);
        let pids: Vec<PartitionId> = (0..2)
            .map(|_| c.take_weighted().expect("有号可取").partition_id())
            .collect();
        assert_eq!(pids, vec![1, 2]);
        assert!(c.take_weighted().is_none());
    }

    /// 分区不在了(转 RO/封存后从 `order` 里消失)不 panic、不卡死:取号只看 `order` 里的分区
    /// (它剩下的段由 `sync_segments` 连同段一起丢掉)。
    #[test]
    fn take_weighted_wraps_when_order_shrinks() {
        let mut c = cache_with(&[(1, 1), (2, 1)], 4);
        assert_eq!(c.take_weighted().expect("号").partition_id(), 1);
        c.order.retain(|w| w.pid == 2); // 分区 1 转 RO:从取号顺序里摘掉
        assert_eq!(c.take_weighted().expect("号").partition_id(), 2);
        assert_eq!(c.take_weighted().expect("号").partition_id(), 2);
    }

    /// 份额:按权重配比、每段 `≥1`、结果确定(余数给小数部分最大的,同分 pid 小的优先)。
    #[test]
    fn segment_shares_are_proportional_with_a_floor_of_one() {
        let w = |pid: PartitionId, weight: u32| PartitionWeight { pid, weight };
        // 均权:每段恰好 = 均分时的大小(总预留口径不变)。
        assert_eq!(
            segment_shares(12, &[w(1, 1), w(2, 1), w(3, 1)]),
            vec![4, 4, 4]
        );
        // 加权且整除:12 个号按 1000:500:500 → 6:3:3。
        assert_eq!(
            segment_shares(12, &[w(1, 1000), w(2, 500), w(3, 500)]),
            vec![6, 3, 3]
        );
        // 有余数:7 个号按 1000:500:500 → 取整 3:1:1,余 2 个给小数部分最大的两个。
        assert_eq!(
            segment_shares(7, &[w(1, 1000), w(2, 500), w(3, 500)]),
            vec![3, 2, 2]
        );
        // e2e 里那组:池 128(2 分区 × 段 64)按 1000:500 → 85:43(余数给小数部分大的那一个)。
        assert_eq!(segment_shares(128, &[w(1, 1000), w(2, 500)]), vec![85, 43]);
        // 极端偏斜也不产生 0 长度的段(0 长度 = 该分区永远发不出号)。
        assert_eq!(segment_shares(2048, &[w(1, 1000), w(2, 1)]), vec![2046, 2]);
        assert_eq!(segment_shares(2, &[w(1, 1000), w(2, 1)]), vec![1, 1]);
        // 空输入 / 全 0 权重(防御):不 panic、不分配。
        assert!(segment_shares(1024, &[]).is_empty());
        assert!(segment_shares(1024, &[w(1, 0)]).is_empty());
    }

    /// 分区写满后摘掉它:段、权重、信用一并清除,`take_weighted` 不再挑它。
    #[test]
    fn evict_partition_removes_segment_order_and_credit() {
        let mut c = cache_with(&[(1, 1), (2, 1)], 4);
        assert_eq!(
            c.take_weighted().expect("号").partition_id(),
            1,
            "先取一个号让分区 1 攒下信用"
        );
        c.evict_partition(1);
        assert!(!c.segments.contains_key(&1), "段必须被清除");
        assert!(!c.order.iter().any(|w| w.pid == 1), "权重必须被清除");
        assert!(!c.credit.contains_key(&1), "信用必须被清除");
        for _ in 0..4 {
            assert_eq!(
                c.take_weighted().expect("号").partition_id(),
                2,
                "摘掉后只从剩余分区取号"
            );
        }
        assert!(c.take_weighted().is_none(), "分区 2 的段取完");
    }

    /// 全部失败为 409 = key 冲突;混合错误 / 无失败 ≠ 冲突(语义镜像 507 用例)。
    #[test]
    fn all_already_exists_detects_conflict() {
        let conflict = |addr: &str| {
            (
                addr.to_string(),
                Error::AlreadyExists {
                    addr: addr.to_string(),
                },
            )
        };
        let silenced = |addr: &str| {
            (
                addr.to_string(),
                Error::Silenced {
                    addr: addr.to_string(),
                    retry_after_secs: None,
                },
            )
        };
        let fanout = |ok: Vec<()>, failures: Vec<(String, Error)>, total: usize| Fanout {
            ok,
            failures,
            total,
            required: total / 2 + 1,
        };

        // 全部 409 → 冲突(返回首个报冲突的副本地址)。
        assert_eq!(
            all_already_exists(&fanout(vec![], vec![conflict("a:1"), conflict("b:1")], 2)),
            Some("a:1".to_string())
        );
        // 409 + 其它错误混合 → 判不了,按 Quorum 处理。
        assert!(
            all_already_exists(&fanout(vec![], vec![conflict("a:1"), silenced("b:1")], 2))
                .is_none()
        );
        // 无失败 → 不是冲突。
        assert!(all_already_exists(&fanout(vec![(), ()], vec![], 2)).is_none());
        // 有成功但多数派未达(1/2,如 W=2 首写后重发:两旧副本 409、一新鲜副本得 ok)
        // → 仍是确定性冲突,透传。
        assert_eq!(
            all_already_exists(&fanout(vec![()], vec![conflict("b:1")], 2)),
            Some("b:1".to_string())
        );
    }

    /// 全部副本都 507 = 分区写满;部分 507 / 混合错误 / 无失败 ≠ 分区满。
    #[test]
    fn all_store_full_detects_partition_full() {
        let full = |addr: &str| {
            (
                addr.to_string(),
                Error::StoreFull {
                    addr: addr.to_string(),
                },
            )
        };
        let silenced = |addr: &str| {
            (
                addr.to_string(),
                Error::Silenced {
                    addr: addr.to_string(),
                    retry_after_secs: None,
                },
            )
        };
        let fanout = |ok: Vec<()>, failures: Vec<(String, Error)>, total: usize| Fanout {
            ok,
            failures,
            total,
            required: total / 2 + 1,
        };

        // 全部 507 → 分区满(返回第一个报满的副本地址)。
        assert_eq!(
            all_store_full(&fanout(vec![], vec![full("a:1"), full("b:1")], 2)),
            Some("a:1".to_string())
        );
        // 部分 507 + 其它错误 → 不是分区满。
        assert!(all_store_full(&fanout(vec![], vec![full("a:1"), silenced("b:1")], 2)).is_none());
        // 无失败 → 不是分区满。
        assert!(all_store_full(&fanout(vec![(), ()], vec![], 2)).is_none());
        // 有成功但多数派未达(1/2)+ 其余全 507 → 仍判分区满(换分区重试是正确补救;
        // "部分 507 不重试"只指多数派已达成的情形,那由调用方先判 `reached` 短路)。
        assert_eq!(
            all_store_full(&fanout(vec![()], vec![full("b:1")], 2)),
            Some("b:1".to_string())
        );
    }
}

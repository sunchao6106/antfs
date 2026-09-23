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
//! **发号 = 多分区轮转**:补段时先拉 master 的当前可写(RW)分区列表,对**每个**分区各申请一小批号
//! (显式点名 `partition_id`),之后写请求在这些分区之间轮转取号 —— 相邻写落在不同分区、不同盘上,
//! 写 IOPS 摊开,而不是把一段号连续耗尽在一个分区上。**分区分派仍由 master 定**,router 不自选(D10)。
//!
//! **可写分区列表由后台任务保鲜**:`connect` 起一个后台任务,按 `partition_refresh_secs` 周期向 master 拉一次
//! 可写分区列表,把已不可写分区的号段丢掉、给新出现的可写分区补一段(号段耗尽的写路径自己也会同步一次;
//! 周期配 0 = 不起后台任务)。任务持号段池的 `Weak` 引用,最后一个 `Router` 析构后自行退出。
//!
//! **两条正确性约束(不做成可配)**:
//! - 写:W = 该分区**实际副本数 / 2 + 1**(多数派)。RF=3 → 2(对齐 Ambry `routerPutSuccessTarget`)、
//!   RF=1 → 1。低于多数派的写会破坏"已确认 ⇒ 多数副本有"的前提,故不给配置项。
//! - 读:R = 1(blob 不可变,读任一有的副本即可)。
//!
//! blobId = 8 字节 `partitionId << 32 | objectId`([`antfs_blob::StoreKey`],大端),URL 里编成 16 位 hex。

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
}

/// 一个分区上已从 master 取回、尚未发完的 blobId 号段。
struct Segment {
    pid: PartitionId,
    /// 下一个待发号(含);`next >= end` 即已取完。
    next: u32,
    end: u32,
}

/// 号段池:**每个可写分区各持一段**,取号时在它们之间轮转。
#[derive(Default)]
struct IdCache {
    /// 未发完的段(`pid → 段`);取完即移除。
    segments: HashMap<PartitionId, Segment>,
    /// 轮转顺序(补段时刷新的可写分区列表,pid 升序)。
    order: Vec<PartitionId>,
    /// 轮转游标:`order` 里下一个待取的分区位置。
    cursor: usize,
}

impl IdCache {
    /// 轮转取一个号:游标所指分区的段没取完就用它,取完就丢掉并转到下一个分区。
    ///
    /// 走完一轮都没号 → `None`(调用方去补段)。**顺序轮转**保证相邻写落在不同分区
    /// (进而是不同副本、不同盘),写 IOPS 摊开而不是把一段号连续耗尽在一个分区上。
    fn take_rr(&mut self) -> Option<StoreKey> {
        for _ in 0..self.order.len() {
            if self.cursor >= self.order.len() {
                self.cursor = 0;
            }
            let pid = self.order[self.cursor];
            self.cursor += 1;
            let Some(seg) = self.segments.get_mut(&pid) else {
                continue;
            };
            if seg.next >= seg.end {
                self.segments.remove(&pid);
                continue;
            }
            let key = StoreKey::from_parts(pid, seg.next);
            seg.next += 1;
            let exhausted = seg.next >= seg.end;
            if exhausted {
                self.segments.remove(&pid);
            }
            return Some(key);
        }
        None
    }
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
    topology: Arc<RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>>,
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
            metrics,
        })
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

    /// 申请一个 blobId:本地号段里轮转取(零 RPC);所有分区的号段都取完才对齐一次可写分区列表。
    pub async fn allocate_one(&self) -> Result<StoreKey> {
        let mut cache = self.ids.lock().await;
        if let Some(key) = cache.take_rr() {
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
        cache.take_rr().ok_or(Error::NoWritablePartition)
    }

    /// 申请 `count` 个 blobId(按分区轮转取,可能跨多个分区)。
    pub async fn allocate_batch(&self, count: u32) -> Result<Vec<StoreKey>> {
        let mut out = Vec::with_capacity(count as usize);
        while (out.len() as u32) < count {
            out.push(self.allocate_one().await?);
        }
        Ok(out)
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
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let started = Instant::now();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            let data = data.clone();
            futs.push(async move {
                let res = self.put_body(pid, &key, data, expiration_ms, &addr).await;
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
        let key = self.allocate_one().await?;
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
            Ok((
                key,
                PutReport {
                    acks: f.ok.len(),
                    total: f.total,
                    required: f.required,
                },
            ))
        } else {
            Err(Error::Quorum {
                acks: f.ok.len(),
                required: f.required,
                total: f.total,
                detail: f.detail(),
            })
        }
    }

    async fn put_body(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        data: Bytes,
        expiration_ms: i64,
        addr: &str,
    ) -> Result<()> {
        let resp = self
            .http
            .put(put_url(addr, pid, key, expiration_ms))
            .body(data)
            .send()
            .await
            .map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            })?;
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
            .put(put_url(addr, pid, key, expiration_ms))
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
        let replicas = self.replicas_for(pid).await?;
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
        let replicas = self.replicas_for(pid).await?;
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
        let resp = self
            .http
            .get(blob_url(addr, pid, key))
            .send()
            .await
            .map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            })?;
        match resp.status().as_u16() {
            200 => Ok(resp),
            _ => Err(classify_err_status(addr, pid, resp).await),
        }
    }

    async fn get_from(&self, pid: PartitionId, key: &StoreKey, addr: &str) -> Result<Bytes> {
        let resp = self.get_stream(pid, key, addr).await?;
        resp.bytes().await.map_err(|source| Error::Http {
            addr: addr.to_string(),
            source,
        })
    }

    /// 读元数据(`HEAD` 语义:不读数据体):第一个成功即返回(R=1)。
    pub async fn stat(&self, key: &StoreKey) -> Result<BlobStat> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
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
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| Error::Http {
                addr: addr.to_string(),
                source,
            })?;
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
/// 拉列表 → **丢掉**已不在列表里的分区的号段(余号成空洞,无害,D3)→ 给缺号段的分区**各申请一小批**
/// (显式点名 `partition_id`,每批 `id_segment_size` 个)→ 按 pid 升序重建轮转顺序。
///
/// - 分区集合与合法性由 master 给:router 只从权威 RW 列表里取分区、**不自造 id**、不缓存容量判断,
///   且每个分区在发号时被 master 复核(RW + 至少一个副本)。代价:负载由 D10 的"按剩余容量加权随机"
///   变为"可写分区上的**均匀轮转**"(取舍见 `openspec/changes/router-partition-round-robin`)。
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
    // 重建轮转顺序时**保持相位**:记下"下一个本该轮到谁",在新顺序里找回它的位置。
    // 否则一次什么都没补的空刷新(后台任务常走到这条)会把游标打回起点,让同一分区连出两个号。
    let anchor = if cache.order.is_empty() {
        None
    } else {
        Some(cache.order[cache.cursor % cache.order.len()])
    };
    let writable: HashSet<PartitionId> = order.iter().copied().collect();
    cache.segments.retain(|pid, _| writable.contains(pid));
    let want: Vec<PartitionId> = order
        .iter()
        .copied()
        .filter(|pid| !cache.segments.contains_key(pid))
        .collect();
    cache.order = order;
    cache.cursor = anchor
        .and_then(|pid| cache.order.iter().position(|p| *p == pid))
        .unwrap_or(0);

    let mut pending = FuturesUnordered::new();
    for pid in want {
        let mut client = master.lock().await.clone();
        let req = GetBlobIdsRequest {
            version: PROTO_VERSION,
            cluster_secret: cfg.cluster_secret.clone(),
            count: cfg.id_segment_size,
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

/// master 当前可写(RW)分区列表(pid 升序)。跳过没有副本的分区 —— 挑了它只会拿到一个必然写不进去的号段。
async fn list_writable(master: &Mutex<MasterClient<Channel>>) -> Result<Vec<PartitionId>> {
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
        .map(|p| p.partition_id)
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

/// 副本位置排序:`up` 的在前(读路径按序试,先试活着的),同组保持 master 给的顺序。
fn sorted_replicas(mut replicas: Vec<ReplicaLoc>) -> Vec<ReplicaLoc> {
    replicas.sort_by_key(|r| !r.up);
    replicas
}

fn hex_key(key: &StoreKey) -> String {
    hex::encode(key.as_bytes())
}

fn blob_url(addr: &str, pid: PartitionId, key: &StoreKey) -> String {
    format!("http://{addr}/partitions/{pid}/blobs/{}", hex_key(key))
}

/// PUT 的 URL:`expiration_ms` 非 [`NO_EXPIRATION`] 时带上写入时定下的 TTL(数据面 `PUT ?expiration_ms=`)。
fn put_url(addr: &str, pid: PartitionId, key: &StoreKey, expiration_ms: i64) -> String {
    let base = blob_url(addr, pid, key);
    if expiration_ms == NO_EXPIRATION {
        base
    } else {
        format!("{base}?expiration_ms={expiration_ms}")
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
    let mut file = tokio::fs::File::create(path).await?;
    let mut stream = resp.bytes_stream();
    let mut n = 0u64;
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

    /// 段 [start, start+count) —— 与发号响应同构(`next` 含、`end` 不含)。
    fn seg(pid: PartitionId, start: u32, count: u32) -> Segment {
        Segment {
            pid,
            next: start,
            end: start + count,
        }
    }

    /// 缓存里装上各分区的一段号,轮转顺序 = `pids`。
    fn cache_with(pids: &[PartitionId], count: u32) -> IdCache {
        let mut c = IdCache {
            order: pids.to_vec(),
            ..Default::default()
        };
        for (i, pid) in pids.iter().enumerate() {
            c.segments
                .insert(*pid, seg(*pid, 100 * (i as u32 + 1), count));
        }
        c
    }

    /// 核心行为:取号在持有号段的分区之间**交替**,而不是把一个分区的一段连续耗尽。
    #[test]
    fn take_rr_rotates_across_partitions() {
        let mut c = cache_with(&[1, 2, 3], 2);
        let pids: Vec<PartitionId> = (0..6)
            .map(|_| c.take_rr().expect("有号可取").partition_id())
            .collect();
        assert_eq!(pids, vec![1, 2, 3, 1, 2, 3], "必须按分区轮转");
        assert!(c.take_rr().is_none(), "号取完 → 交给调用方补段");
        assert!(c.segments.is_empty(), "取完的段必须被丢掉");
    }

    /// 一段只有一个号(count=1)也必须发得出去 —— `next` 含 / `end` 不含 的边界。
    #[test]
    fn take_rr_handles_single_id_segment() {
        let mut c = cache_with(&[7], 1);
        let key = c.take_rr().expect("单号段仍可发一个号");
        assert_eq!((key.partition_id(), key.object_id()), (7, 100));
        assert!(c.take_rr().is_none());
    }

    /// 某分区的段取完后跳过它,其余分区继续轮转;空缓存返回 None。
    #[test]
    fn take_rr_skips_exhausted_and_empty_cache_returns_none() {
        assert!(IdCache::default().take_rr().is_none());
        let mut c = cache_with(&[1, 2], 1);
        let pids: Vec<PartitionId> = (0..2)
            .map(|_| c.take_rr().expect("有号可取").partition_id())
            .collect();
        assert_eq!(pids, vec![1, 2]);
        assert!(c.take_rr().is_none());
    }

    /// 分区不在了(转 RO/封存后从 `order` 里消失)不 panic、不卡死:游标越过 `order` 长度要回绕。
    #[test]
    fn take_rr_wraps_cursor_when_order_shrinks() {
        let mut c = cache_with(&[1, 2], 4);
        assert_eq!(c.take_rr().expect("号").partition_id(), 1);
        c.order = vec![2]; // 分区 1 转 RO:从轮转顺序里摘掉
        assert_eq!(c.take_rr().expect("号").partition_id(), 2);
        assert_eq!(c.take_rr().expect("号").partition_id(), 2);
    }
}

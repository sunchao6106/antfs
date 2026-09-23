//! `Router`:antfs 的客户端侧**通用组件** —— **向 master 申请 blobId 号段 → 直连数据面写读**。
//!
//! 定位见 [`docs/design/router.md`](../../../../docs/design/router.md) 与 `cluster.md §10`:Ambry 里这层叫
//! router(封装进 SDK),antfs 把它做成纯库(`crates/common/router`)—— **frontend 内嵌同一个 `Router`**,
//! `benchmark/router` 也用它,故 W/R 逻辑只有一份实现。本 crate **不提供二进制**。
//!
//! ```text
//! GetBlobIds(gRPC) ─▶ master  回 (可写 partitionId, objectId 号段 [start,start+N), 副本位置)
//! PUT ×N (HTTP)    ─▶ store   收 W 个 ack 即返回(其余副本靠拉复制补齐)
//! GET   (HTTP)     ─▶ store   第一个成功即返回(R=1)
//! ```
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

pub use antfs_blob::StoreKey;
pub use config::RouterConfig;
pub use error::{Error, Result};
pub use metrics::RouterMetrics;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::{GetBlobIdsRequest, LookupRequest, ReplicaLoc};
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
    pub undeleted: bool,
    pub ttl_updated: bool,
    pub expired: bool,
    pub life_version: u16,
}

/// 一池已从 master 取回、尚未发完的 blobId 号段。
struct Segment {
    pid: PartitionId,
    next: u32,
    end: u32,
}

#[derive(Default)]
struct IdCache {
    segments: Vec<Segment>,
}

/// antfs router(客户端 SDK)。可 `Arc` 共享给多任务并发使用。
pub struct Router {
    cfg: RouterConfig,
    /// master 控制面客户端(tonic 方法需 `&mut self`,故加锁;调用频率 = 每号段一次,不构成瓶颈)。
    master: Mutex<MasterClient<Channel>>,
    http: reqwest::Client,
    /// 号段池。**锁跨取段的 `await` 持有**:号段边界单飞,避免并发调用各取一段。
    ids: Mutex<IdCache>,
    /// `partitionId → 副本位置` 读路由缓存(读路径用;写路径优先用发号响应带回的副本)。
    topology: RwLock<HashMap<PartitionId, Vec<ReplicaLoc>>>,
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
    /// 连 master(懒连接:首个 RPC 才真正建连)并注册指标。
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
        Ok(Router {
            cfg,
            master: Mutex::new(MasterClient::new(channel)),
            http,
            ids: Mutex::new(IdCache::default()),
            topology: RwLock::new(HashMap::new()),
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

    /// 申请一个 blobId:号段内直接取(零 RPC),号段耗尽时向 master 取下一段。
    pub async fn allocate_one(&self) -> Result<StoreKey> {
        let mut cache = self.ids.lock().await;
        if let Some(seg) = cache.segments.iter_mut().find(|s| s.next < s.end) {
            let key = StoreKey::from_parts(seg.pid, seg.next);
            seg.next += 1;
            return Ok(key);
        }
        let (seg, first) = self.fetch_segment().await?;
        cache.segments.retain(|s| s.next < s.end);
        cache.segments.push(seg);
        Ok(first)
    }

    /// 申请 `count` 个 blobId(顺序取,可能跨多个号段/分区)。
    pub async fn allocate_batch(&self, count: u32) -> Result<Vec<StoreKey>> {
        let mut out = Vec::with_capacity(count as usize);
        while (out.len() as u32) < count {
            out.push(self.allocate_one().await?);
        }
        Ok(out)
    }

    /// 向 master 取一段 blobId(两层批处理的第一层;水位抬升由 master 侧完成)。
    ///
    /// 返回 `(段, 首个 blobId)`。顺带把响应里的副本位置写进读路由缓存。
    async fn fetch_segment(&self) -> Result<(Segment, StoreKey)> {
        let want = self.cfg.id_segment_size;
        let req = GetBlobIdsRequest {
            version: PROTO_VERSION,
            cluster_secret: self.cfg.cluster_secret.clone(),
            count: want,
            // 不指定 partition = 由 master 按剩余容量加权挑一个可写分区(blobid-master-assign D10)
            partition_id: None,
        };
        let resp = self
            .master
            .lock()
            .await
            .get_blob_ids(req)
            .await
            .map_err(Error::Master)?
            .into_inner();
        if !resp.ok {
            return Err(map_master_error(&resp.error, resp.partition_id));
        }
        if resp.count == 0 {
            return Err(Error::Rejected("master returned an empty id segment".into()));
        }
        let start = u32::try_from(resp.start_object_id)
            .map_err(|_| Error::Rejected(format!("start_object_id {} overflows u32", resp.start_object_id)))?;
        let pid = resp.partition_id;
        self.metrics.id_segments.inc();
        self.metrics.id_segment_ids.inc_by(resp.count as u64);
        self.topology
            .write()
            .expect("topology lock poisoned")
            .insert(pid, sorted_replicas(resp.replicas));
        Ok((
            Segment { pid, next: start + 1, end: start.saturating_add(resp.count) },
            StoreKey::from_parts(pid, start),
        ))
    }

    // ── 读路由 ──────────────────────────────────────────────────────────────────────────────

    /// 该分区的副本位置:优先用缓存(发号响应里带回过),否则向 master `LookupPartition` 拉一次。
    ///
    /// 排序保证 `up` 的副本在前 —— 读路径(R=1)按顺序试,先试活着的。
    async fn replicas_for(&self, pid: PartitionId) -> Result<Vec<ReplicaLoc>> {
        if let Some(rs) = self.topology.read().expect("topology lock poisoned").get(&pid) {
            if !rs.is_empty() {
                return Ok(rs.clone());
            }
        }
        let resp = self
            .master
            .lock()
            .await
            .lookup_partition(LookupRequest { version: PROTO_VERSION, partition_id: pid })
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

    /// 写一个 blob:向该 partition 的**全部副本并行 PUT**,收满**多数派** ack 即返回。
    ///
    /// 收满即返回意味着发往其余副本的请求被取消 —— 第 3 份由拉复制补齐(`cluster.md §10`)。
    pub async fn put(&self, key: StoreKey, data: Bytes) -> Result<PutReport> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let started = Instant::now();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            let data = data.clone();
            futs.push(async move {
                let res = self.put_body(pid, &key, data, &addr).await;
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
            self.metrics.put_replica_errors.with_label_values(&[e.kind()]).inc();
        }
        if f.reached() {
            self.metrics.put_bytes.inc_by(data.len() as u64);
            Ok(PutReport { acks: f.ok.len(), total: f.total, required: f.required })
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
                let res = self.put_file_to(pid, &key, &path, &addr).await;
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
            self.metrics.put_replica_errors.with_label_values(&[e.kind()]).inc();
        }
        if f.reached() {
            self.metrics.put_bytes.inc_by(len);
            Ok((key, PutReport { acks: f.ok.len(), total: f.total, required: f.required }))
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
        addr: &str,
    ) -> Result<()> {
        let resp = self
            .http
            .put(blob_url(addr, pid, key))
            .body(data)
            .send()
            .await
            .map_err(|source| Error::Http { addr: addr.to_string(), source })?;
        classify_put(addr, pid, resp).await
    }

    async fn put_file_to(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        path: &Path,
        addr: &str,
    ) -> Result<()> {
        let file = tokio::fs::File::open(path).await?;
        let resp = self
            .http
            .put(blob_url(addr, pid, key))
            .body(reqwest::Body::from(file))
            .send()
            .await
            .map_err(|source| Error::Http { addr: addr.to_string(), source })?;
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
        self.metrics.get_total.with_label_values(&[err.kind()]).inc();
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
                Ok(resp) => {
                    return drain_to_file(resp, &addr, path.as_ref(), &self.metrics).await
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
        self.metrics.get_total.with_label_values(&[err.kind()]).inc();
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
            .map_err(|source| Error::Http { addr: addr.to_string(), source })?;
        match resp.status().as_u16() {
            200 => Ok(resp),
            _ => Err(classify_err_status(addr, pid, resp).await),
        }
    }

    async fn get_from(&self, pid: PartitionId, key: &StoreKey, addr: &str) -> Result<Bytes> {
        let resp = self.get_stream(pid, key, addr).await?;
        resp.bytes()
            .await
            .map_err(|source| Error::Http { addr: addr.to_string(), source })
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
                    self.metrics.mutation_total.with_label_values(&["stat", "ok"]).inc();
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
        self.metrics.mutation_total.with_label_values(&["stat", err.kind()]).inc();
        Err(err)
    }

    async fn stat_from(&self, pid: PartitionId, key: &StoreKey, addr: &str) -> Result<BlobStat> {
        let url = format!("http://{addr}/partitions/{pid}/blobs/{}/metadata", hex_key(key));
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| Error::Http { addr: addr.to_string(), source })?;
        if resp.status().as_u16() == 200 {
            return resp
                .json::<BlobStat>()
                .await
                .map_err(|source| Error::Http { addr: addr.to_string(), source });
        }
        Err(classify_err_status(addr, pid, resp).await)
    }

    // ── 变更(delete / undelete / ttl)──────────────────────────────────────────────────────

    /// 删除(写墓碑):多数派 ack。
    pub async fn delete(&self, key: &StoreKey) -> Result<PutReport> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            futs.push(async move {
                let res = self.mutate_one(pid, key, &addr, "delete", None).await;
                (addr, res)
            });
        }
        let f = collect_majority(futs, total).await;
        self.record_mutation("delete", &f);
        if f.reached() {
            Ok(PutReport { acks: f.ok.len(), total: f.total, required: f.required })
        } else {
            Err(Error::Quorum {
                acks: f.ok.len(),
                required: f.required,
                total: f.total,
                detail: f.detail(),
            })
        }
    }

    /// 复活已删除对象:多数派 ack,返回首个成功副本上的新生命版本。
    pub async fn undelete(&self, key: &StoreKey) -> Result<(PutReport, u16)> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            futs.push(async move {
                let res = self.mutate_one(pid, key, &addr, "undelete", None).await;
                (addr, res)
            });
        }
        let f = collect_majority(futs, total).await;
        self.record_mutation("undelete", &f);
        if f.reached() {
            let life = f.ok.first().and_then(|v| *v).unwrap_or(0);
            Ok((PutReport { acks: f.ok.len(), total: f.total, required: f.required }, life))
        } else {
            Err(Error::Quorum {
                acks: f.ok.len(),
                required: f.required,
                total: f.total,
                detail: f.detail(),
            })
        }
    }

    /// 更新过期时间:多数派 ack。
    pub async fn update_ttl(&self, key: &StoreKey, expiration_ms: i64) -> Result<PutReport> {
        let pid = key.partition_id();
        let replicas = self.replicas_for(pid).await?;
        let total = replicas.len();
        let futs = FuturesUnordered::new();
        for r in &replicas {
            let addr = r.addr.clone();
            futs.push(async move {
                let res = self.mutate_one(pid, key, &addr, "ttl", Some(expiration_ms)).await;
                (addr, res)
            });
        }
        let f = collect_majority(futs, total).await;
        self.record_mutation("ttl", &f);
        if f.reached() {
            Ok(PutReport { acks: f.ok.len(), total: f.total, required: f.required })
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
        self.metrics.mutation_total.with_label_values(&[op, outcome]).inc();
        for (_, e) in &f.failures {
            self.metrics.put_replica_errors.with_label_values(&[e.kind()]).inc();
        }
    }

    /// 单副本变更请求。`expiration_ms` 仅 `ttl` 用。
    async fn mutate_one(
        &self,
        pid: PartitionId,
        key: &StoreKey,
        addr: &str,
        op: &'static str,
        expiration_ms: Option<i64>,
    ) -> Result<Option<u16>> {
        let url = match op {
            "delete" => blob_url(addr, pid, key),
            "undelete" => format!("http://{addr}/partitions/{pid}/blobs/{}/undelete", hex_key(key)),
            "ttl" => format!(
                "http://{addr}/partitions/{pid}/blobs/{}/ttl?expiration_ms={}",
                hex_key(key),
                expiration_ms.unwrap_or(-1)
            ),
            other => return Err(Error::Rejected(format!("unknown mutation {other}"))),
        };
        let req = match op {
            "delete" => self.http.delete(url),
            _ => self.http.post(url),
        };
        let resp = req
            .send()
            .await
            .map_err(|source| Error::Http { addr: addr.to_string(), source })?;
        let status = resp.status().as_u16();
        match status {
            204 => Ok(None),
            200 => {
                let text = resp.text().await.unwrap_or_default();
                Ok(Some(text.trim().parse::<u16>().unwrap_or(0)))
            }
            _ => Err(classify_err_status(addr, pid, resp).await),
        }
    }
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
    Fanout { ok, failures, total, required }
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

/// 非 2xx 的状态码 → 归一错误(写/读/变更共用同一套语义)。
///
/// 消费掉响应体(状态码 + 文案一起判定),故调用方在 2xx 分支之外才能调用它。
async fn classify_err_status(addr: &str, pid: PartitionId, resp: reqwest::Response) -> Error {
    let status = resp.status().as_u16();
    let retry = retry_after_secs(&resp);
    let body = resp.text().await.unwrap_or_default();
    match status {
        // store 对"无此副本"与"对象不存在"都回 404;按响应体区分(文案是 store 的稳定契约)。
        404 if body.contains("no replica for partition") => {
            Error::NoReplica { addr: addr.to_string(), pid }
        }
        404 => Error::NotFound { pid },
        409 => Error::AlreadyExists { addr: addr.to_string() },
        507 => Error::StoreFull { addr: addr.to_string() },
        503 => Error::Silenced { addr: addr.to_string(), retry_after_secs: retry },
        s => Error::ReplicaStatus { addr: addr.to_string(), status: s, detail: body },
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
        let chunk = chunk.map_err(|source| Error::Http { addr: addr.to_string(), source })?;
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

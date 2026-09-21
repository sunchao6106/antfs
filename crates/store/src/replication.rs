//! S3a 拉复制:`PeerSource` 抽象 + 收敛循环 + HTTP adapter + 后台复制器。
//!
//! 承 cluster.md D4/§10:step-1 元数据(`find_entries_since`)走 HTTP(见 design.md D-B),
//! step-2 缺失数据体走 HTTP blob 读(`?replication=1`)。忠实写入由 store `apply_replicated` 完成
//! (保留 peer 的 flags/lifeVersion,防副本发散)。收敛逻辑同步阻塞,由调用方经 `spawn_blocking` offload。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use master::pb::master_client::MasterClient;
use master::pb::LookupRequest;
use serde::{Deserialize, Serialize};
use blob::{
    ApplyOutcome, BlobStore, FindInfo, FindToken, LogOffset, MessageInfo, ReplicatedRecord, StoreKey,
};

use crate::config::StoreServerConfig;
use crate::error::{Error, Result};
use crate::metrics::StoreServerMetrics;
use crate::replica::ReplicaManager;
use crate::PartitionId;

/// 一条复制元数据(store `MessageInfo` 的可序列化镜像;key 用 hex)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEntry {
    pub key: String,
    pub size: u64,
    pub expiration_ms: i64,
    pub deleted: bool,
    pub ttl_updated: bool,
    pub undeleted: bool,
    pub life_version: u16,
    pub operation_time_ms: u64,
}

/// `replica-metadata` 端点响应:一批条目 + 新游标(seg/off,均 None = Uninitialized)+ 已读字节。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub entries: Vec<MetadataEntry>,
    pub token_seg: Option<u64>,
    pub token_off: Option<u64>,
    pub bytes_read: u64,
}

impl MetadataResponse {
    pub fn from_find(info: FindInfo) -> Self {
        let (token_seg, token_off) = match info.token {
            FindToken::Offset(o) => (Some(o.segment), Some(o.offset)),
            FindToken::Uninitialized => (None, None),
        };
        MetadataResponse {
            entries: info.entries.iter().map(entry_from).collect(),
            token_seg,
            token_off,
            bytes_read: info.bytes_read,
        }
    }

    fn next_token(&self) -> FindToken {
        match (self.token_seg, self.token_off) {
            (Some(s), Some(o)) => FindToken::Offset(LogOffset::new(s, o)),
            _ => FindToken::Uninitialized,
        }
    }
}

/// 把请求里的 `seg`/`off` 查询参数解成 store 游标。
pub fn token_from(seg: Option<u64>, off: Option<u64>) -> FindToken {
    match (seg, off) {
        (Some(s), Some(o)) => FindToken::Offset(LogOffset::new(s, o)),
        _ => FindToken::Uninitialized,
    }
}

fn entry_from(m: &MessageInfo) -> MetadataEntry {
    MetadataEntry {
        key: hex::encode(m.key.as_bytes()),
        size: m.size,
        expiration_ms: m.expiration_ms,
        deleted: m.deleted,
        ttl_updated: m.ttl_updated,
        undeleted: m.undeleted,
        life_version: m.life_version,
        operation_time_ms: m.operation_time_ms,
    }
}

/// store 记录三态标志位(与 `blob::index::value` 的 FLAG_* 对齐)。
const FLAG_DELETED: u8 = 1 << 0;
const FLAG_TTL_UPDATED: u8 = 1 << 1;
const FLAG_UNDELETED: u8 = 1 << 2;

fn flags_of(e: &MetadataEntry) -> u8 {
    (if e.deleted { FLAG_DELETED } else { 0 })
        | (if e.ttl_updated { FLAG_TTL_UPDATED } else { 0 })
        | (if e.undeleted { FLAG_UNDELETED } else { 0 })
}

/// peer 数据源抽象:生产 = [`HttpPeer`];测试 = 进程内 `BlobStore` 包装(见单测)。
pub trait PeerSource {
    /// 拉 peer 自 `token` 起的复制元数据(step-1)。
    fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<MetadataResponse>;
    /// 拉 peer 上某 key 的存活数据体(step-2);`None` = peer 也已无该数据(被回收)。
    fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>>;
    /// S3b:列 peer 的封存段 `(position, data_end)`;默认空(=无物理拷贝,回落纯逻辑)。
    fn sealed_segments(&self) -> Result<Vec<SegmentInfo>> {
        Ok(Vec::new())
    }
    /// S3b:拉 peer 某封存段的原始字节。默认不支持。
    fn fetch_segment(&self, _position: u64) -> Result<Vec<u8>> {
        Err(Error::Replication("fetch_segment unsupported".to_string()))
    }
}

/// S3b:一个封存段的定位(物理拷贝清单项)。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentInfo {
    pub position: u64,
    pub data_end: u64,
}

/// 一轮拉复制的统计。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateStats {
    pub applied: u64,
    pub bytes: u64,
}

/// 对某 peer 从 `token` 起拉复制直到追平,忠实 apply 到 `local`;返回新游标 + 统计。
///
/// 幂等:`apply_replicated` 只增不删,本地已 ≥ peer 状态时 `Skipped`。仅"本地缺失 + peer 存活"才拉数据体。
pub fn replicate_from_peer(
    local: &BlobStore,
    peer: &dyn PeerSource,
    mut token: FindToken,
    max_bytes: u64,
) -> Result<(FindToken, ReplicateStats)> {
    let mut stats = ReplicateStats::default();
    loop {
        let resp = peer.metadata(token, max_bytes)?;
        if resp.entries.is_empty() {
            token = resp.next_token();
            break;
        }
        for e in &resp.entries {
            let bytes = hex::decode(&e.key)
                .ok()
                .and_then(|b| StoreKey::from_slice(&b));
            let Some(key) = bytes else {
                return Err(Error::Replication(format!("bad key hex: {}", e.key)));
            };
            // 仅本地缺失且 peer 存活时才拉数据体;变更(delete/ttl/undelete on existing)数据在本地。
            let need_data = !e.deleted && local.stat(&key).is_err();
            let data = if need_data {
                match peer.fetch_data(&key)? {
                    Some(d) => Some(d),
                    None => continue, // peer 已无数据(被回收),跳过
                }
            } else {
                None
            };
            let outcome = local.apply_replicated(ReplicatedRecord {
                key,
                flags: flags_of(e),
                expiration_ms: e.expiration_ms,
                operation_time_ms: e.operation_time_ms,
                life_version: e.life_version,
                data,
            })?;
            if outcome == ApplyOutcome::Applied {
                stats.applied += 1;
                stats.bytes += e.size;
            }
        }
        token = resp.next_token();
    }
    Ok((token, stats))
}

/// 用阻塞 HTTP 客户端实现 `PeerSource`(在 `spawn_blocking` 线程里用,不碰 tokio reactor)。
pub struct HttpPeer {
    base: String,
    pid: PartitionId,
    client: reqwest::blocking::Client,
}

impl HttpPeer {
    pub fn new(base: impl Into<String>, pid: PartitionId, timeout: Duration) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::Replication(e.to_string()))?;
        Ok(HttpPeer { base: base.into(), pid, client })
    }
}

impl PeerSource for HttpPeer {
    fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<MetadataResponse> {
        let mut url = format!(
            "{}/partitions/{}/replica-metadata?max_bytes={}",
            self.base, self.pid, max_bytes
        );
        if let FindToken::Offset(o) = token {
            url.push_str(&format!("&seg={}&off={}", o.segment, o.offset));
        }
        let resp = self.client.get(&url).send().map_err(|e| Error::Replication(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Replication(format!("metadata status {}", resp.status())));
        }
        resp.json::<MetadataResponse>().map_err(|e| Error::Replication(e.to_string()))
    }

    fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>> {
        let url = format!(
            "{}/partitions/{}/blobs/{}?replication=1",
            self.base,
            self.pid,
            hex::encode(key.as_bytes())
        );
        let resp = self.client.get(&url).send().map_err(|e| Error::Replication(e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::Replication(format!("fetch status {}", resp.status())));
        }
        let bytes = resp.bytes().map_err(|e| Error::Replication(e.to_string()))?;
        Ok(Some(bytes.to_vec()))
    }

    fn sealed_segments(&self) -> Result<Vec<SegmentInfo>> {
        let url = format!("{}/partitions/{}/segments", self.base, self.pid);
        let resp = self.client.get(&url).send().map_err(|e| Error::Replication(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Replication(format!("segments status {}", resp.status())));
        }
        resp.json::<Vec<SegmentInfo>>().map_err(|e| Error::Replication(e.to_string()))
    }

    fn fetch_segment(&self, position: u64) -> Result<Vec<u8>> {
        let url = format!("{}/partitions/{}/segments/{}", self.base, self.pid, position);
        let resp = self.client.get(&url).send().map_err(|e| Error::Replication(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Replication(format!("segment status {}", resp.status())));
        }
        let bytes = resp.bytes().map_err(|e| Error::Replication(e.to_string()))?;
        Ok(bytes.to_vec())
    }
}

/// S3b bootstrap 统计。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapStats {
    pub segments: u64,
    pub bytes: u64,
}

/// 物理 bootstrap:拉 peer 全部封存段字节 → `install_bootstrap` 装载重建。
///
/// 返回续拉游标 `Offset(max_sealed_pos, data_end)`(供其后逻辑补活跃尾);peer 无封存段返回 `None`
/// (调用方回落纯逻辑 `Uninitialized`)。
pub fn bootstrap_from_peer(
    replicas: &ReplicaManager,
    pid: PartitionId,
    peer: &dyn PeerSource,
) -> Result<(Option<FindToken>, BootstrapStats)> {
    let sealed = peer.sealed_segments()?;
    if sealed.is_empty() {
        return Ok((None, BootstrapStats::default()));
    }
    let mut segs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(sealed.len());
    let mut stats = BootstrapStats::default();
    for s in &sealed {
        let bytes = peer.fetch_segment(s.position)?;
        stats.bytes += bytes.len() as u64;
        segs.push((s.position, bytes));
    }
    stats.segments = segs.len() as u64;
    replicas.install_bootstrap(pid, &segs)?;
    // 续拉游标 = 最高封存段末尾(其后即活跃段,逻辑补)。
    let last = sealed.iter().max_by_key(|s| s.position).expect("non-empty");
    Ok((Some(FindToken::Offset(LogOffset::new(last.position, last.data_end))), stats))
}

/// 一轮对某 peer 的完整同步:空副本首次先物理 bootstrap(若启用),再从续拉游标逻辑补。
///
/// `prior_token=None` 表示"首次"(可尝试 bootstrap);`Some(t)` 表示续拉(仅逻辑)。返回新游标 + 统计。
#[allow(clippy::too_many_arguments)]
pub fn sync_from_peer(
    replicas: &ReplicaManager,
    pid: PartitionId,
    peer: &dyn PeerSource,
    prior_token: Option<FindToken>,
    max_bytes: u64,
    bootstrap_enabled: bool,
) -> Result<(FindToken, ReplicateStats, BootstrapStats)> {
    let mut start = prior_token.unwrap_or_default();
    let mut boot = BootstrapStats::default();
    let is_first = prior_token.is_none();
    let empty = replicas.get(pid).map(|h| h.store.is_empty()).unwrap_or(false);
    if is_first && bootstrap_enabled && empty {
        if let Ok((Some(tok), s)) = bootstrap_from_peer(replicas, pid, peer) {
            start = tok;
            boot = s;
        }
    }
    let handle = replicas
        .get(pid)
        .ok_or_else(|| Error::Replication(format!("no local replica {pid}")))?;
    let store = handle.store.clone();
    let (token, stats) = replicate_from_peer(&store, peer, start, max_bytes)?;
    Ok((token, stats, boot))
}

/// 后台拉复制器:每周期对每个本地 replica 的每个存活 peer(经 master `LookupPartition` 发现)拉一轮。
///
/// 仅在配置了 `master_addr`(需发现 peer)且 `replication_enabled` 时启动。
pub fn spawn_replicator(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
) {
    tokio::spawn(async move {
        if let Err(e) = run(config, replicas, metrics).await {
            tracing::warn!(error = %e, "store-server replicator stopped");
        }
    });
}

async fn run(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
) -> Result<()> {
    let mut client = MasterClient::connect(format!("http://{}", config.master_addr))
        .await
        .map_err(|e| Error::Replication(e.to_string()))?;
    let timeout = Duration::from_secs(config.disk_health_timeout_secs.max(1));
    let mut tokens: HashMap<(PartitionId, u64), FindToken> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(config.replication_interval_secs));
    loop {
        tick.tick().await;
        let pids: Vec<PartitionId> =
            replicas.replica_states().into_iter().map(|(pid, _, _)| pid).collect();
        for pid in pids {
            let locs = match client.lookup_partition(LookupRequest { version: 1, partition_id: pid }).await {
                Ok(r) => r.into_inner().replicas,
                Err(_) => continue,
            };
            for loc in locs {
                if loc.node_id == config.node_id || !loc.up || loc.addr.is_empty() {
                    continue;
                }
                let prior = tokens.get(&(pid, loc.node_id)).copied();
                let store = replicas.clone();
                let base = format!("http://{}", loc.addr);
                let max_bytes = config.replication_max_batch_bytes;
                let bootstrap = config.bootstrap_copy_enabled;
                let res = tokio::task::spawn_blocking(move || {
                    let peer = HttpPeer::new(base, pid, timeout)?;
                    sync_from_peer(&store, pid, &peer, prior, max_bytes, bootstrap)
                })
                .await;
                match res {
                    Ok(Ok((new_token, stats, boot))) => {
                        tokens.insert((pid, loc.node_id), new_token);
                        metrics.replication_rounds.inc();
                        metrics.replication_applied.inc_by(stats.applied);
                        metrics.replication_bytes.inc_by(stats.bytes);
                        metrics.bootstrap_segments.inc_by(boot.segments);
                        metrics.bootstrap_bytes.inc_by(boot.bytes);
                    }
                    Ok(Err(_)) | Err(_) => metrics.replication_errors.inc(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blob::StoreConfig;

    fn store(dir: &std::path::Path) -> BlobStore {
        BlobStore::create(dir, 64 << 20, 1 << 20, 1000).unwrap()
    }

    fn hex_key(n: u8) -> String {
        let mut b = [0u8; 8];
        b[0] = n;
        hex::encode(b)
    }

    /// 进程内 PeerSource:直接包裹一个 `BlobStore`,免 socket 的确定性收敛测试。
    struct LocalPeer<'a> {
        store: &'a BlobStore,
    }
    impl PeerSource for LocalPeer<'_> {
        fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<MetadataResponse> {
            Ok(MetadataResponse::from_find(self.store.find_entries_since(&token, max_bytes)?))
        }
        fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>> {
            match self.store.get_with_options(key, true, true) {
                Ok(d) => Ok(Some(d)),
                Err(blob::Error::NotFound) => Ok(None),
                Err(e) => Err(Error::Store(e)),
            }
        }
    }

    fn key(n: u8) -> StoreKey {
        StoreKey::from_slice(&hex::decode(hex_key(n)).unwrap()).unwrap()
    }

    #[test]
    fn converges_put_and_delete_idempotent() {
        let _ = StoreConfig::default();
        let (dp, dl) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let peer = store(dp.path());
        let local = store(dl.path());
        peer.put(key(1), b"hello").unwrap();
        peer.put(key(2), b"world").unwrap();

        let src = LocalPeer { store: &peer };
        let (tok, stats) =
            replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(local.get(&key(1)).unwrap(), b"hello");
        assert_eq!(local.get(&key(2)).unwrap(), b"world");

        // 幂等:再拉一轮无新增。
        let (_, stats2) = replicate_from_peer(&local, &src, tok, u64::MAX).unwrap();
        assert_eq!(stats2.applied, 0);

        // delete 传播。
        peer.delete(key(1)).unwrap();
        let (_, _) = replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert!(matches!(local.get(&key(1)), Err(blob::Error::Deleted)));
    }

    #[test]
    fn bootstrap_full_replay_catches_up() {
        let (dp, dl) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let peer = store(dp.path());
        for n in 0..10u8 {
            peer.put(key(n), &[n; 16]).unwrap();
        }
        let local = store(dl.path());
        let src = LocalPeer { store: &peer };
        let (tok, _) =
            replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        for n in 0..10u8 {
            assert_eq!(local.get(&key(n)).unwrap(), vec![n; 16]);
        }
        // 追平后增量为空。
        let resp = src.metadata(tok, u64::MAX).unwrap();
        assert!(resp.entries.is_empty(), "caught up: no new entries");
    }
}

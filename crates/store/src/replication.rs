//! S3a 拉复制:`PeerSource` 抽象 + 收敛循环 + HTTP adapter + 后台复制器。
//!
//! 承 cluster.md D4/§10:step-1 元数据(`find_entries_since`)走 HTTP(见 design.md D-B),
//! step-2 缺失数据体走 HTTP blob 读(`?replication=1`)。忠实写入由 store `apply_replicated` 完成
//! (保留 peer 的 flags/lifeVersion,防副本发散)。收敛逻辑同步阻塞,由调用方经 `spawn_blocking` offload。
//!
//! # 追平判据(副本实然态 `BOOTSTRAP → ACTIVE` 的唯一入口)
//!
//! 副本诞生/重开/重装出来时一律报 `BOOTSTRAP`(见 `crate::replica::STATE_BOOTSTRAP`);本模块在每轮
//! 拉复制结束后判定"**这轮是否已追平**",通过才把该副本置 `ACTIVE`。这条判据是**数据安全**问题:
//! master 侧"搬迁要等继任者已 `ACTIVE`"的删源闸门(`reconcile::plan_move`/`drop_confirmed`)
//! 若不成立,就会立刻摘源并下发 `DROPPED` 删源数据 —— 源若是当时唯一有数据的副本就丢数据。
//!
//! 判据(见 [`caught_up_with_peer`]、[`local_has_nothing_past`])——**逐 peer、每轮**判:
//!
//! 1. **peer 无该分区副本**(`replica-metadata` 404)= peer 没有该分区数据 → 视为满足(没有数据要同步);
//! 2. **peer 正常应答**且本轮**没有待 apply 的条目**(`replicate_from_peer` 只在拿到空条目批时返回),
//!    且**本地 log 末端与 peer 报的游标一致**(从 peer 游标再问一次本地库:没有任何其后的条目;
//!    两者都空也算一致);
//! 3. 其余一律**不追平**,保守留在 `BOOTSTRAP`(拿不准宁可晚转正,不可早转正)。
//!
//! 与**某个** peer 的一轮满足判据即可置 `ACTIVE`(不等全部 peer;置位后单向、不再回退)。生产复制器
//! 每 tick 遍历该分区**所有**可达 peer 各拉一轮,故"追平到其中一个"通常就意味着最短路径已经走完。
//!
//! **已知边界**(如实记录,不装作没有):store 只看得到"联系得上的 peer"的应答,分不清"整个分区真的
//! 空"与"唯一联系得上的 peer 恰好也空、而持有数据的副本暂时不可达"。故本地也空时会对空 peer 放行
//! (否则全新空分区永远无法转正,见下)。这一层的兜底在 master 的 placement 级闸门:`up` 过滤、
//! "健康副本数 ≥ RF"、"源仍持有"(`soft.holds`)等 —— store 侧只负责**如实**报自己追平了没有。
//!
//! 必须处理对的场景:
//!
//! - **全新空分区**:分区刚建时**所有**节点上都是空的。此时既没有 peer 数据也没本地数据:
//!   peer 无该副本(404)走 1;peer 有副本但零条 → peer 报 `Uninitialized` 游标,本地也是空 →
//!   "两者都空也算一致" 走 2。两条都落在"没有数据需要同步"= 已追平 → 立即 `ACTIVE`
//!   (否则空分区永远卡在 `BOOTSTRAP`,master 侧看着像"无健康副本")。
//! - **单副本分区**(RF=1 / 单节点集群):placement 里本节点之外没有其他副本 = 没有 peer 可同步
//!   (判据的空集情形),由复制 loop 直接置 `ACTIVE`(见 [`mark_caught_up`] 的调用点)。
//! - **物理 bootstrap 的新副本**(`install_bootstrap`):只拷了封存段,活跃尾还要靠增量拉。
//!   故 bootstrap 之后**仍报 `BOOTSTRAP`**,直到增量拉那一轮按上面 2 判定追平(同一轮即可)。
//! - **重启后重开的副本**(`dir.exists()` → `open_with`):盘上数据可能是旧的,**故意**先报 `BOOTSTRAP`,
//!   等一轮拉复制追平再 `ACTIVE`。这是保守而非故障:重启期间显示"同步中"是诚实状态。它**不会**
//!   引发 master 的再复制风暴 —— master 算"要不要补副本"用的是 `up && !errored`
//!   (`reconcile::plan_rereplication`),不看 `BOOTSTRAP`;`BOOTSTRAP` 只进"欠副本主因"的展示。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::LookupRequest;
use serde::{Deserialize, Serialize};
use antfs_blob::{
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

/// store 记录三态标志位(与 `antfs_blob::index::value` 的 FLAG_* 对齐)。
const FLAG_DELETED: u8 = 1 << 0;
const FLAG_TTL_UPDATED: u8 = 1 << 1;
const FLAG_UNDELETED: u8 = 1 << 2;

fn flags_of(e: &MetadataEntry) -> u8 {
    (if e.deleted { FLAG_DELETED } else { 0 })
        | (if e.ttl_updated { FLAG_TTL_UPDATED } else { 0 })
        | (if e.undeleted { FLAG_UNDELETED } else { 0 })
}

/// peer 对 `GET /partitions/{pid}/replica-metadata` 的应答。
///
/// 两态必须**分开**,不能都折成"空一批条目":判据里"peer 无该分区数据"与"peer 有副本但没写零条"
/// 的语义不同(前者无条件满足,后者要和本地 log 末端对照,见模块头「追平判据」)。
#[derive(Debug, Clone)]
pub enum PeerMetadata {
    /// 正常应答:一批条目 + 新游标。
    Entries(MetadataResponse),
    /// peer 上**没有该分区的副本**(HTTP 404,如该 peer 还没被 master 指到建副本):
    /// 视为"peer 没有该分区数据",不是错误。
    NoReplica,
}

/// peer 数据源抽象:生产 = [`HttpPeer`];测试 = 进程内 `BlobStore` 包装(见单测)。
pub trait PeerSource {
    /// 拉 peer 自 `token` 起的复制元数据(step-1);peer 无该分区副本 → [`PeerMetadata::NoReplica`]。
    fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<PeerMetadata>;
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

/// 一轮拉复制的结果(`replicate_from_peer` / `sync_from_peer` / `sync_round` 共用同一形状)。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncOutcome {
    /// 本轮结束后的续拉游标(**peer 坐标系**);`no_replica` 时 = 入参游标(未推进)。
    pub token: FindToken,
    /// 本轮忠实 apply 的统计。
    pub stats: ReplicateStats,
    /// 本轮物理 bootstrap 的统计(只有 `sync_from_peer`/`sync_round` 会填)。
    pub bootstrap: BootstrapStats,
    /// peer 上**没有该分区的副本**(404)= 该 peer 没有该分区数据(不是错误;判据见模块头)。
    pub no_replica: bool,
}

/// 对某 peer 从 `token` 起拉复制直到追平,忠实 apply 到 `local`;返回新游标 + 统计。
///
/// 幂等:`apply_replicated` 只增不删,本地已 ≥ peer 状态时 `Skipped`。仅"本地缺失 + peer 存活"才拉数据体。
///
/// 循环**只在拿到一批空条目时结束**(= peer 在其 log 末端已无更新);peer 无该分区副本则立即返回
/// `no_replica = true`(没有数据可同步,不是错误)。
pub fn replicate_from_peer(
    local: &BlobStore,
    peer: &dyn PeerSource,
    mut token: FindToken,
    max_bytes: u64,
) -> Result<SyncOutcome> {
    let mut stats = ReplicateStats::default();
    loop {
        let resp = match peer.metadata(token, max_bytes)? {
            PeerMetadata::Entries(resp) => resp,
            PeerMetadata::NoReplica => {
                return Ok(SyncOutcome { token, stats, bootstrap: BootstrapStats::default(), no_replica: true })
            }
        };
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
    Ok(SyncOutcome { token, stats, bootstrap: BootstrapStats::default(), no_replica: false })
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
    fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<PeerMetadata> {
        let mut url = format!(
            "{}/partitions/{}/replica-metadata?max_bytes={}",
            self.base, self.pid, max_bytes
        );
        if let FindToken::Offset(o) = token {
            url.push_str(&format!("&seg={}&off={}", o.segment, o.offset));
        }
        let resp = self.client.get(&url).send().map_err(|e| Error::Replication(e.to_string()))?;
        // 404 = 该 peer 上没有这个分区的副本(不是故障):如实回 `NoReplica`,让判据把它当
        // "peer 没有该分区数据"(见模块头「追平判据」第 1 条),而不是当成一轮错误。
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(PeerMetadata::NoReplica);
        }
        if !resp.status().is_success() {
            return Err(Error::Replication(format!("metadata status {}", resp.status())));
        }
        let resp = resp.json::<MetadataResponse>().map_err(|e| Error::Replication(e.to_string()))?;
        Ok(PeerMetadata::Entries(resp))
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
/// `prior_token=None` 表示"首次"(可尝试 bootstrap);`Some(t)` 表示续拉(仅逻辑)。
/// 返回本轮结果(游标 + 统计 + `no_replica`);bootstrap **不**改判实然态(见模块头第 2 个场景)。
#[allow(clippy::too_many_arguments)]
pub fn sync_from_peer(
    replicas: &ReplicaManager,
    pid: PartitionId,
    peer: &dyn PeerSource,
    prior_token: Option<FindToken>,
    max_bytes: u64,
    bootstrap_enabled: bool,
) -> Result<SyncOutcome> {
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
    let mut out = replicate_from_peer(&store, peer, start, max_bytes)?;
    out.bootstrap = boot;
    Ok(out)
}

/// 把「peer 游标」与「本地已 apply 的游标」之差线性化为字节(**估算**)。
///
/// 口径:`(peer.segment − applied.segment) * segment_capacity + (peer.offset − applied.offset)`;
/// 仅 peer 在前时为正,追平 / 本地在前 / 游标未知(`Uninitialized`)一律记 0。
///
/// 已知误差(故意只作倾向性观测,不参与任何一致性决策):
/// - 两个游标都在 **peer 的 log 坐标系**里(本地 apply 的进度用 peer 返回的 token 记录),
///   故 offset 之差是**记录字节**(含记录头/crc),比对象负载略大;
/// - 段容量按配置 `segment_size_bytes` 线性化,真实段数据容量还要扣 header;且只在本地/peer
///   **段大小相同**时成立(同配置部署下成立);
/// - 游标语义是「最后一条已审视记录的**起点**」,故两个游标之差 = 两个起点之间的字节,**不含新游标
///   处那条记录本身** → 最多偏小一条记录:前后两条记录大小相近(稳态流)时基本准确,大小悬殊时
///   该误差不可忽略(只落后一条大记录时,报出的可能是上一条小记录的长度);
/// - 采样点是**滞后采样**:反映"上一轮以来积压了多少",不是实时值——轮内已追平的那部分要到
///   下一轮才归 0(每轮各采样一次)。
pub fn lag_bytes(peer: FindToken, applied: FindToken, segment_capacity: u64) -> u64 {
    let (FindToken::Offset(p), FindToken::Offset(l)) = (peer, applied) else {
        return 0;
    };
    let segs = i128::from(p.segment).saturating_sub(i128::from(l.segment));
    let offs = i128::from(p.offset).saturating_sub(i128::from(l.offset));
    let bytes = segs.saturating_mul(i128::from(segment_capacity)).saturating_add(offs);
    // 负值 = 本地已 apply 越过 peer 游标(对端重建/清空等),按"未滞后"报 0。
    u64::try_from(bytes).unwrap_or(0)
}

/// 追平判据里的本地探测上限:只判"peer 游标之后本地**还有没有**内容",取到第一条候选即止,
/// 故取最小非零值(1 字节)。
const CATCHUP_PROBE_MAX_BYTES: u64 = 1;

/// 本地是否**已没有** peer 游标 `token` 之后的内容(=「本地 log 末端与 peer 报的游标一致」)。
///
/// 与 peer 侧游标同语义(`BlobStore::find_entries_since`:游标指向"最后一条已审视记录的**起点**",
/// 从该游标再问一次会**跳过**该记录本身):返回空条目 + 原游标 = 本地已到 peer 游标处。
/// 两处都空(双方皆空库 → 双侧 `Uninitialized`)同样算一致。
///
/// 两道守卫:
/// - **本地一条记录都没有**时,只有 peer 游标也是"空"(`Uninitialized`)才算一致 —— 否则说明本地
///   落后于 peer(还什么都没拉到),绝不能判追平(挡住"陈旧游标 + 空/重建的本地库"这种误报);
/// - 探测返回的游标**必须没推进**:游标推进说明本地游标之后还有候选记录(只是索引里已无对应赢家),
///   那就不算"末端一致"(保守)。
///
/// 拿不准(本地读失败)一律返回 `false` → 保守留在 `BOOTSTRAP`。
fn local_has_nothing_past(local: &BlobStore, token: FindToken) -> bool {
    if local.is_empty() {
        return token == FindToken::Uninitialized;
    }
    match local.find_entries_since(&token, CATCHUP_PROBE_MAX_BYTES) {
        Ok(info) => info.entries.is_empty() && info.token == token,
        Err(_) => false,
    }
}

/// 「本轮是否已追平该 peer」—— 转 `ACTIVE` 的判据(模块头有完整场景说明)。
///
/// 逐 peer 判:peer 无该分区副本(404)→ 满足;peer 正常应答且**本轮没有待 apply 的条目**
/// (`replicate_from_peer` 只在空条目批时返回)且本地 log 末端与 peer 游标一致 → 满足。
/// 拿到某个 peer 的"满足"即可置 `ACTIVE`(置位后单向,不因后续某轮拉不到而回退)。
fn caught_up_with_peer(local: &BlobStore, out: &SyncOutcome) -> bool {
    if out.no_replica {
        // peer 上根本没有该分区副本 = 没有数据要同步。只在**本轮没从它身上拉到东西**时算满足:
        // 若拉到一半对方把副本删了,本地可能还缺数据 → 保守不追平(下轮再看)。
        return out.stats.applied == 0;
    }
    local_has_nothing_past(local, out.token)
}

/// 追平判定通过 → 把该分区副本置 `ACTIVE`(幂等;`ERROR` 不被覆盖,见
/// [`ReplicaHandle::mark_active`])。
fn mark_caught_up(replicas: &ReplicaManager, pid: PartitionId, reason: &str) {
    let Some(handle) = replicas.get(pid) else {
        return;
    };
    if handle.mark_active() {
        tracing::info!(partition = pid, reason, "replica caught up; reporting ACTIVE");
    }
}

/// 一轮同步 + 滞后记账 + **追平判定**(后台复制器与测试共用同一入口/同一口径)。
///
/// `sync_from_peer` 之后做两件事:
/// 1. 把 `lag_bytes(本轮 peer 游标, 本轮起点游标, 段容量)` 记进该分区句柄(供心跳上报;只写一个
///    诊断用的原子量,**不触碰复制/apply 语义**);
/// 2. 判"本轮是否已追平该 peer"([`caught_up_with_peer`]),通过则把副本实然态由 `BOOTSTRAP` 置
///    `ACTIVE`(见模块头「追平判据」)。**注意取句柄要在 `sync_from_peer` 之后** —— 物理 bootstrap
///    会把本地句柄整个换掉(`install_bootstrap`),必须给新句柄置位。
pub fn sync_round(
    replicas: &ReplicaManager,
    pid: PartitionId,
    peer: &dyn PeerSource,
    prior_token: Option<FindToken>,
    max_bytes: u64,
    bootstrap_enabled: bool,
) -> Result<SyncOutcome> {
    let out = sync_from_peer(replicas, pid, peer, prior_token, max_bytes, bootstrap_enabled)?;
    let lag = lag_bytes(out.token, prior_token.unwrap_or_default(), replicas.segment_size_bytes());
    let Some(handle) = replicas.get(pid) else {
        return Ok(out);
    };
    handle.lag.set(lag);
    if caught_up_with_peer(&handle.store, &out) {
        mark_caught_up(replicas, pid, "peer_synced");
    }
    Ok(out)
}

/// 后台拉复制器:每周期对每个本地 replica 的每个存活 peer(经 master `LookupPartition` 发现)拉一轮。
///
/// 仅在配置了 `master_addr`(需发现 peer)且 `replication_enabled` 时启动。
/// `paused` 是运行期复制开关(A7):每 tick 读一次,`true` 跳过本轮**主动拉取**。
pub fn spawn_replicator(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    paused: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        if let Err(e) = run(config, replicas, metrics, paused).await {
            tracing::warn!(error = %e, "store-server replicator stopped");
        }
    });
}

async fn run(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    paused: Arc<AtomicBool>,
) -> Result<()> {
    let mut client = MasterClient::connect(format!("http://{}", config.master_addr))
        .await
        .map_err(|e| Error::Replication(e.to_string()))?;
    let timeout = Duration::from_secs(config.disk_health_timeout_secs.max(1));
    let mut tokens: HashMap<(PartitionId, u64), FindToken> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(config.replication_interval_secs));
    loop {
        tick.tick().await;
        // A7 复制开关:每轮读一次。暂停只跳过**本节点主动拉**这一轮 —— 别人来拉本节点(复制读端点)
        // 不受影响,`RequestControl` 静默也与之正交;恢复后从同一游标自然续拉(游标跨暂停保留)。
        if paused.load(Ordering::Relaxed) {
            continue;
        }
        let pids: Vec<PartitionId> =
            replicas.replica_states().into_iter().map(|s| s.partition_id).collect();
        for pid in pids {
            let locs = match client.lookup_partition(LookupRequest { version: 1, partition_id: pid }).await {
                Ok(r) => r.into_inner().replicas,
                Err(_) => continue,
            };
            // 该分区在本节点之外**没有任何其他副本**(RF=1 / 单节点集群):没有 peer 可同步,也就没有
            // "尚未追平的数据" —— 这是追平判据里"所有 peer 都无数据"的**空集**情形,直接判追平转
            // `ACTIVE`。否则单副本分区会永远停在 `BOOTSTRAP`(master 侧一直报"欠副本/同步中")。
            // `pid` 取自本地副本清单,故这里一定有本地句柄可置位。
            // 对端存在但 `up=false`/地址为空 → 不在此列:那是"peer 暂时拉不到",保守留在 `BOOTSTRAP`。
            if !locs.iter().any(|loc| loc.node_id != config.node_id) {
                mark_caught_up(&replicas, pid, "no_peer");
            } else {
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
                        sync_round(&store, pid, &peer, prior, max_bytes, bootstrap)
                    })
                    .await;
                    match res {
                        Ok(Ok(out)) => {
                            tokens.insert((pid, loc.node_id), out.token);
                            metrics.replication_rounds.inc();
                            metrics.replication_applied.inc_by(out.stats.applied);
                            metrics.replication_bytes.inc_by(out.stats.bytes);
                            metrics.bootstrap_segments.inc_by(out.bootstrap.segments);
                            metrics.bootstrap_bytes.inc_by(out.bootstrap.bytes);
                        }
                        Ok(Err(_)) | Err(_) => metrics.replication_errors.inc(),
                    }
                }
            }
        }
        // 本轮可能把若干副本由 `BOOTSTRAP` 转成 `ACTIVE`(也可能没有):每 tick 同步刷新一次副本
        // 相关 gauge,别等下一拍心跳(否则"同步中"的观测要滞后一个心跳周期)。
        metrics.set_replica_gauges(&replicas.replica_states());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use antfs_blob::StoreConfig;

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
        fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<PeerMetadata> {
            Ok(PeerMetadata::Entries(MetadataResponse::from_find(
                self.store.find_entries_since(&token, max_bytes)?,
            )))
        }
        fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>> {
            match self.store.get_with_options(key, true, true) {
                Ok(d) => Ok(Some(d)),
                Err(antfs_blob::Error::NotFound) => Ok(None),
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
        let out = replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert_eq!(out.stats.applied, 2);
        assert!(!out.no_replica, "peer 有副本 → 不是 NoReplica");
        assert_eq!(local.get(&key(1)).unwrap(), b"hello");
        assert_eq!(local.get(&key(2)).unwrap(), b"world");

        // 幂等:再拉一轮无新增。
        let out2 = replicate_from_peer(&local, &src, out.token, u64::MAX).unwrap();
        assert_eq!(out2.stats.applied, 0);

        // delete 传播。
        peer.delete(key(1)).unwrap();
        let _ = replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert!(matches!(local.get(&key(1)), Err(antfs_blob::Error::Deleted)));
    }

    /// 滞后线性化:跨段按段容量折算;peer 不在前 / 游标未知 → 0。
    #[test]
    fn lag_bytes_linearizes_across_segments() {
        let cap = 1 << 20;
        let early = FindToken::Offset(LogOffset::new(1, 1000));
        let late = FindToken::Offset(LogOffset::new(3, 500));
        assert_eq!(lag_bytes(late, early, cap), 2 * cap - 500);
        assert_eq!(lag_bytes(early, late, cap), 0, "本地已在前 → 未滞后");
        assert_eq!(lag_bytes(early, early, cap), 0, "追平 → 0");
        assert_eq!(lag_bytes(FindToken::Uninitialized, early, cap), 0, "无基线 → 0");
        assert_eq!(lag_bytes(early, FindToken::Uninitialized, cap), 0, "无基线 → 0");
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
        let out = replicate_from_peer(&local, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        for n in 0..10u8 {
            assert_eq!(local.get(&key(n)).unwrap(), vec![n; 16]);
        }
        // 追平后增量为空。
        let PeerMetadata::Entries(resp) = src.metadata(out.token, u64::MAX).unwrap() else {
            panic!("peer 有副本")
        };
        assert!(resp.entries.is_empty(), "caught up: no new entries");
    }

    /// 追平判据的两半(本地是否"已没有 peer 游标之后的内容")逐个盯死:
    /// - 双方皆空的**全新空分区** → 一致(否则空分区永远转不正);
    /// - 本地有内容、peer 游标为空 → 不一致(本地还没追平,保守留在 BOOTSTRAP);
    /// - 本地一条都没有、peer 游标非空 → 不一致(本地落后于 peer,绝不判追平);
    /// - 本地已收敛到 peer 游标 → 一致。
    #[test]
    fn local_has_nothing_past_handles_empty_partition_and_lagging_local() {
        let (d_local, d_peer, d_empty) =
            (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let empty = store(d_empty.path());
        // 双方皆空(游标都是 Uninitialized)→ 一致。
        assert!(local_has_nothing_past(&empty, FindToken::Uninitialized), "空分区(两侧都空)必须算追平");

        // 本地有数据 + peer 游标未知:本地还有内容 → 不一致。
        let local = store(d_local.path());
        local.put(key(1), b"a").unwrap();
        assert!(!local_has_nothing_past(&local, FindToken::Uninitialized), "本地有数据 ≠ 追平");

        // 本地一条都没有 + peer 游标非空:本地落后 → 不一致。
        assert!(!local_has_nothing_past(&empty, FindToken::Offset(LogOffset::new(0, 0))));

        // 本地已从 peer 拉到末端:从 peer 游标再问本地库一无所获 → 一致。
        let peer = store(d_peer.path());
        peer.put(key(2), b"b").unwrap();
        let src = LocalPeer { store: &peer };
        let out = replicate_from_peer(&empty, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert_eq!(out.stats.applied, 1, "peer 的 1 条记录必须拉过来");
        assert!(local_has_nothing_past(&empty, out.token), "本地已 apply 到 peer 末端 → 一致");
    }

    /// `no_replica` 的判据半:peer 无该分区副本 → 满足(没有数据要同步);本轮从它身上拉到过东西则不算。
    #[test]
    fn no_replica_peer_counts_as_caught_up_only_when_nothing_applied() {
        let d = tempfile::tempdir().unwrap();
        let local = store(d.path());
        let empty = SyncOutcome::default();
        assert!(
            caught_up_with_peer(&local, &SyncOutcome { no_replica: true, ..empty }),
            "peer 无副本 = 无数据可同步 → 算追平"
        );
        assert!(
            !caught_up_with_peer(
                &local,
                &SyncOutcome {
                    no_replica: true,
                    stats: ReplicateStats { applied: 1, bytes: 8 },
                    ..empty
                }
            ),
            "拉到一半对方删了副本 → 保守不追平"
        );
    }
}

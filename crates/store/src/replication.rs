//! S3a 拉复制:`PeerSource` 抽象 + 收敛循环 + HTTP adapter + 后台复制器。
//!
//! 承 cluster.md D4/§10:step-1 元数据(`find_entries_since`)走 HTTP(见 design.md D-B),
//! step-2 缺失数据体走 HTTP blob 读(`?replication=1`)。忠实写入由 store `apply_replicated` 完成
//! (忠实保留 peer 的 flags,防副本发散)。收敛逻辑同步阻塞,由调用方经 `spawn_blocking` offload。
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
//! # 游标重定位(对齐 Ambry `revalidateStoreFindToken` + resetKey)
//!
//! 游标是**对端 log 坐标系的绝对位置**,而 compaction 会改写坐标系(全库 compact 段号从 0 重来)。
//! 若把"拿陈旧游标问对端、对端返回空批"当成"已追上",就会在**作废的坐标系**上判追平 → 副本带着
//! 缺失的数据转 `ACTIVE` → master 的删源闸门误成立 → 静默丢数据。故两道机制:
//!
//! 1. **对端 revalidate**(引擎 + 服务层,见 `BlobStore::find_entries_since` 与 `server::replica_metadata`):
//!    游标认不出(段没了 / 偏移越界 / **身份不符**)**或对端身份变了** → 一律**重置为从头重扫**
//!    (方向是多拉,安全),并在响应里报 `token_reset`;
//! 2. **本轮不判追平**:响应里带 `token_reset`、或本轮发现对端 log 身份变化 ⇒ 即使"末端一致"也不算追平
//!    (见 [`caught_up_with_peer`] 的前置条件),下轮从对端给的新游标继续。
//!
//! 身份是**两层**(见 `antfs_blob::BlobStore::incarnation` 与 `LogSegmentName`):段身份(位置 + 代次)
//! 识别"同一副本内 log 被 compaction 重写",由引擎在 `find_entries_since` 里校验;**副本 incarnation**
//! 识别"对面整个换了一代副本"(重建/换盘),由服务层校验 —— incarnation 由 master 发放(从零建副本时
//! 申领),落盘在副本目录的 `LOG_INCARNATION`,master 不可达时用本地兜底值。会话状态(游标 + 身份 +
//! 无进展计数)收在 [`PullSession`],生产 loop 与集成测试共用。
//!
//! 连续两轮"游标没动、也没 apply"⇒ 丢弃游标从头重拉([`PullSession::advance`]),避免因对端行为超出
//! 预期而永远卡在 `BOOTSTRAP`。
//!
//! **已知边界**(如实记录,不装作没有):store 只看得到"联系得上的 peer"的应答,分不清"整个分区真的
//! 空"与"唯一联系得上的 peer 恰好也空、而持有数据的副本暂时不可达"。故本地也空时会对空 peer 放行
//! (否则全新空分区永远无法转正,见下)。这一层的兜底在 master 的 placement 级闸门:`up` 过滤、
//! "健康副本数 ≥ RF"、"源仍持有"(`soft.holds`)等 —— store 侧只负责**如实**报自己追平了没有。
//! 没有"旧版本 peer"这一类边界:线上尚未启用,无新旧节点并存,故**不做任何兼容分支**(不兼容的
//! 旧游标一律被认作"认不出" → 从头重扫,方向是多拉)。
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

use antfs_blob::{
    ApplyOutcome, BlobStore, FindInfo, FindToken, LogSegmentName, MessageInfo, ReplicatedRecord,
    StoreKey,
};
use antfs_master::pb::master_client::MasterClient;
use antfs_master::pb::LookupRequest;
use serde::{Deserialize, Serialize};

use crate::config::StoreServerConfig;
use crate::error::{Error, Result};
use crate::heartbeat::claim_incarnation;
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
}

/// `replica-metadata` 端点响应:一批条目 + 新游标(seg/off,均 None = Uninitialized)+ 已读字节。
///
/// 另带两个**身份/重定位**字段(对齐 Ambry `StoreFindToken` 的 incarnation 语义):
/// - `log_incarnation`:本副本的 incarnation(master 发放,落盘在 `LOG_INCARNATION`);
/// - `token_reset`:本轮游标是否被**重定位**(原因短名)。`Some(_)` 时拉取方不得判"已追上"。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub entries: Vec<MetadataEntry>,
    /// 续拉游标 = 段身份(位置 + 代次)+ 段内偏移;三者全空 = `Uninitialized`。
    pub token_position: Option<u32>,
    pub token_generation: Option<u32>,
    pub token_offset: Option<u64>,
    pub bytes_read: u64,
    /// 本副本的 **incarnation**(副本级身份,见 `BlobStore::incarnation`):拉取方把新游标绑定到它,
    /// 下次请求带回来;不符即说明对面换了一代副本(重建/换盘),游标作废。
    pub log_incarnation: u64,
    /// 本轮游标是否被服务端**重定位**(原因短名);`Some(_)` 时拉取方不得判"已追上"。
    #[serde(default)]
    pub token_reset: Option<String>,
}

impl MetadataResponse {
    /// 由引擎结果构造响应(服务层把**自己的** incarnation 贴上去 —— 引擎看不到副本级身份)。
    pub fn from_find(info: FindInfo, log_incarnation: u64) -> Self {
        let (token_position, token_generation, token_offset) = match info.token.segment_name() {
            Some(name) => (
                Some(name.position),
                Some(name.generation),
                info.token.offset().map(|o| o.offset),
            ),
            None => (None, None, None),
        };
        MetadataResponse {
            entries: info.entries.iter().map(entry_from).collect(),
            token_position,
            token_generation,
            token_offset,
            bytes_read: info.bytes_read,
            log_incarnation,
            token_reset: info.token_reset.map(|r| r.as_str().to_string()),
        }
    }

    fn next_token(&self) -> FindToken {
        match (
            self.token_position,
            self.token_generation,
            self.token_offset,
        ) {
            (Some(p), Some(g), Some(o)) => {
                FindToken::at(LogSegmentName::new(u64::from(p), u64::from(g)), o)
            }
            _ => FindToken::Uninitialized,
        }
    }
}

/// 把请求里的 `pos`/`gen`/`off` 查询参数解成 store 游标;三件套不全一律 `Uninitialized`
/// (残缺游标不可信 → 从头重扫,方向是多拉)。
pub fn token_from(
    position: Option<u32>,
    generation: Option<u32>,
    offset: Option<u64>,
) -> FindToken {
    match (position, generation, offset) {
        (Some(p), Some(g), Some(o)) => {
            FindToken::at(LogSegmentName::new(u64::from(p), u64::from(g)), o)
        }
        _ => FindToken::Uninitialized,
    }
}

fn entry_from(m: &MessageInfo) -> MetadataEntry {
    MetadataEntry {
        key: hex::encode(m.key.as_bytes()),
        size: m.size,
        expiration_ms: m.expiration_ms,
        deleted: m.deleted,
    }
}

/// store 记录标志位(与 `antfs_blob::index::value` 的 FLAG_* 对齐)。
const FLAG_DELETED: u8 = 1 << 0;

fn flags_of(e: &MetadataEntry) -> u8 {
    if e.deleted {
        FLAG_DELETED
    } else {
        0
    }
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
    /// S3b:列 peer 的封存段清单 + 它的坐标系身份;默认空(=无物理拷贝,回落纯逻辑)。
    fn sealed_segments(&self) -> Result<SealedListing> {
        Ok(SealedListing::default())
    }
    /// S3b:拉 peer 某封存段的原始字节(按**段身份** = 位置 + 代次;对端认不出该段 → 报错,调用方
    /// 丢弃拷贝回落纯逻辑拉取)。默认不支持。
    fn fetch_segment(&self, _name: LogSegmentName) -> Result<Vec<u8>> {
        Err(Error::Replication("fetch_segment unsupported".to_string()))
    }
}

/// S3b:一个封存段的定位(物理拷贝清单项)。位置 + **代次** = 段身份(代号让"对端在拷贝期间重写 log"
/// 也能被认出来:同位置、不同代次)。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentInfo {
    pub position: u32,
    pub generation: u32,
    pub data_end: u64,
}

impl SegmentInfo {
    pub fn name(&self) -> LogSegmentName {
        LogSegmentName::new(u64::from(self.position), u64::from(self.generation))
    }
}

/// S3b:peer 的封存段清单 + **peer 的副本 incarnation**。
///
/// incarnation 随清单一起回来,是为了让 bootstrap 之后的**初始游标直接绑定 peer 的副本代次** —— 否则
/// "拷完段、还没问过对端身份"这段时间里对端若重写了 log,初始游标会看似合法(静默漏数据)。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SealedListing {
    /// peer 的 **incarnation**(bootstrap 后的初始游标据此绑定副本级身份)。
    pub incarnation: u64,
    #[serde(default)]
    pub segments: Vec<SegmentInfo>,
}

/// 一轮拉复制的统计。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplicateStats {
    pub applied: u64,
    pub bytes: u64,
}

/// 一轮拉复制的结果(`replicate_from_peer` / `sync_from_peer` / `sync_round` 共用同一形状)。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncOutcome {
    /// 本轮结束后的续拉游标(**peer 坐标系**);`no_replica` 时 = 入参游标(未推进)。
    pub token: FindToken,
    /// 本轮忠实 apply 的统计。
    pub stats: ReplicateStats,
    /// 本轮物理 bootstrap 的统计(只有 `sync_from_peer`/`sync_round` 会填)。
    pub bootstrap: BootstrapStats,
    /// peer 上**没有该分区的副本**(404)= 该 peer 没有该分区数据(不是错误;判据见模块头)。
    pub no_replica: bool,
    /// 本轮对端是否要求**重定位游标**(= 响应里的 `token_reset` 原因短名)。`Some(_)` 时
    /// **不得判追平** —— 那种"末端一致"是在刚作废的坐标系上算出来的。
    pub cursor_reset_reason: Option<String>,
    /// peer 本轮报告的 **incarnation**(副本级身份);调用方据此判断对端是否换了一代副本。
    pub peer_incarnation: Option<u64>,
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
    let mut cursor_reset_reason: Option<String> = None;
    let mut peer_incarnation: Option<u64> = None;
    loop {
        let resp = match peer.metadata(token, max_bytes)? {
            PeerMetadata::Entries(resp) => resp,
            PeerMetadata::NoReplica => {
                return Ok(SyncOutcome {
                    token,
                    stats,
                    bootstrap: BootstrapStats::default(),
                    no_replica: true,
                    cursor_reset_reason,
                    peer_incarnation,
                })
            }
        };
        // 对端说"你这个游标我认不出来"(revalidate 重定位)⇒ 记下来:那一轮不判追平。
        if let Some(reason) = &resp.token_reset {
            cursor_reset_reason = Some(reason.clone());
        }
        peer_incarnation = Some(resp.log_incarnation);
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
            // 只有本地**根本没有**这个 key 时才要拉数据体。删除(墓碑)/写入都在 peer 的元数据里带全了:
            // 本地已有该 key 时,写一条 local 的墓碑或覆盖就够,数据体不需要搬。
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
                data,
            })?;
            if outcome == ApplyOutcome::Applied {
                stats.applied += 1;
                stats.bytes += e.size;
            }
        }
        token = resp.next_token();
    }
    Ok(SyncOutcome {
        token,
        stats,
        bootstrap: BootstrapStats::default(),
        no_replica: false,
        cursor_reset_reason,
        peer_incarnation,
    })
}

/// 用阻塞 HTTP 客户端实现 `PeerSource`(在 `spawn_blocking` 线程里用,不碰 tokio reactor)。
pub struct HttpPeer {
    base: String,
    pid: PartitionId,
    client: reqwest::blocking::Client,
    /// 要携带的"游标所声明的 incarnation"(Cell: **每次响应后从 `log_incarnation` 自我学习** ——
    /// 同一排空过程中,首条响应把对端身份带回来后,后续请求就带着它,不会因"还没学会"而白白触发一次
    /// incarnation 重置;`None` = 全新拉取/还没学会)。
    incarnation: std::cell::Cell<Option<u64>>,
}

impl HttpPeer {
    /// 带上"游标所声明的 incarnation"(由复制会话提供;不设 = 全新拉取)。
    pub fn with_incarnation(self, incarnation: Option<u64>) -> Self {
        self.incarnation.set(incarnation);
        self
    }

    pub fn new(base: impl Into<String>, pid: PartitionId, timeout: Duration) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::Replication(e.to_string()))?;
        Ok(HttpPeer {
            base: base.into(),
            pid,
            client,
            incarnation: std::cell::Cell::new(None),
        })
    }
}

impl PeerSource for HttpPeer {
    fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<PeerMetadata> {
        let mut url = format!(
            "{}/partitions/{}/replica-metadata?max_bytes={}",
            self.base, self.pid, max_bytes
        );
        if let (Some(name), Some(o)) = (token.segment_name(), token.offset()) {
            url.push_str(&format!(
                "&pos={}&gen={}&off={}",
                name.position, name.generation, o.offset
            ));
            // 带上游标声明的副本 incarnation:对端据此判定"这份副本还是我游标所在的那一代吗"。
            if let Some(inc) = self.incarnation.get() {
                url.push_str(&format!("&incarnation={inc}"));
            }
        }
        let resp = self
            .client
            .get(&url)
            .send()
            .map_err(|e| Error::Replication(e.to_string()))?;
        // 404 = 该 peer 上没有这个分区的副本(不是故障):如实回 `NoReplica`,让判据把它当
        // "peer 没有该分区数据"(见模块头「追平判据」第 1 条),而不是当成一轮错误。
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(PeerMetadata::NoReplica);
        }
        if !resp.status().is_success() {
            return Err(Error::Replication(format!(
                "metadata status {}",
                resp.status()
            )));
        }
        let resp = resp
            .json::<MetadataResponse>()
            .map_err(|e| Error::Replication(e.to_string()))?;
        // 自我学习:下一次请求就带着对端告诉我们的身份(首次拉取的那条响应即带回)。
        self.incarnation.set(Some(resp.log_incarnation));
        Ok(PeerMetadata::Entries(resp))
    }

    fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>> {
        let url = format!(
            "{}/partitions/{}/blobs/{}?replication=1",
            self.base,
            self.pid,
            hex::encode(key.as_bytes())
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .map_err(|e| Error::Replication(e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::Replication(format!(
                "fetch status {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .map_err(|e| Error::Replication(e.to_string()))?;
        Ok(Some(bytes.to_vec()))
    }

    fn sealed_segments(&self) -> Result<SealedListing> {
        let url = format!("{}/partitions/{}/segments", self.base, self.pid);
        let resp = self
            .client
            .get(&url)
            .send()
            .map_err(|e| Error::Replication(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Replication(format!(
                "segments status {}",
                resp.status()
            )));
        }
        resp.json::<SealedListing>()
            .map_err(|e| Error::Replication(e.to_string()))
    }

    fn fetch_segment(&self, name: LogSegmentName) -> Result<Vec<u8>> {
        let url = format!("{}/partitions/{}/segments/{}", self.base, self.pid, name);
        let resp = self
            .client
            .get(&url)
            .send()
            .map_err(|e| Error::Replication(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Replication(format!(
                "segment status {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .map_err(|e| Error::Replication(e.to_string()))?;
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
///
/// **任一环节发现"拷贝横跨两个代次"就丢弃整份拷贝**(返回 `None`,调用方回落纯逻辑拉取):
/// - 取某段时对端 404 / 出错(段身份已变,或网络失败);
/// - 取回的字节自述代次与清单不符;
/// - 拷完再列一次清单,对端 incarnation 变了(整个副本被重建)。
///
/// 三者的方向一致:宁可退化为慢速逻辑拉取(幂等、必然收敛),也不装出混合快照。
pub fn bootstrap_from_peer(
    replicas: &ReplicaManager,
    pid: PartitionId,
    peer: &dyn PeerSource,
) -> Result<(Option<FindToken>, BootstrapStats)> {
    let listing = peer.sealed_segments()?;
    if listing.segments.is_empty() {
        return Ok((None, BootstrapStats::default()));
    }
    let mut segs: Vec<(LogSegmentName, Vec<u8>)> = Vec::with_capacity(listing.segments.len());
    let mut stats = BootstrapStats::default();
    for s in &listing.segments {
        // 取段按**段身份**请求:对端在"列清单"与"取这一段"之间重写过 log → 那段已经不是清单里那一段
        // → 对端 404。这类失败(以及任何取段失败)**一律丢弃这份拷贝**回落纯逻辑拉取(幂等、必然收敛),
        // 绝不把"同位置、新代次"的字节当成清单里的段装进去(那会装出混合快照 —— 见场景 8)。
        let bytes = match peer.fetch_segment(s.name()) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    partition = pid,
                    position = s.position,
                    generation = s.generation,
                    error = %e,
                    "fetching a peer segment failed; discarding the physical copy and falling back to logical pull"
                );
                return Ok((None, BootstrapStats::default()));
            }
        };
        // 第二道防线:段头自述的代次必须等于清单里的代次。取段路径已按段身份 404 化(认不出就报错),
        // 这里再核一次字节自述 —— 防"对端段文件被换过/返回值被中间层改动"这类不发 404 的路径。
        if antfs_blob::peek_generation(&bytes) != Some(s.generation) {
            tracing::warn!(
                partition = pid,
                position = s.position,
                expected = s.generation,
                "peer segment changed during physical bootstrap; discarding the copy"
            );
            return Ok((None, BootstrapStats::default()));
        }
        stats.bytes += bytes.len() as u64;
        segs.push((s.name(), bytes));
    }
    stats.segments = segs.len() as u64;

    // 拷完**再列一次**:对端若在这期间整库重写(换 incarnation),逐段取来的字节会横跨两代副本,
    // 装出来是个**混合快照**。这时**丢弃拷贝**,返回 `None` 让调用方回落纯逻辑拉取(幂等、必然收敛)。
    let after = peer.sealed_segments()?;
    if after.incarnation != listing.incarnation {
        tracing::warn!(
            partition = pid,
            before = listing.incarnation,
            after = after.incarnation,
            "peer replica re-created during physical bootstrap; discarding the copy and falling back to logical pull"
        );
        return Ok((None, BootstrapStats::default()));
    }

    replicas.install_bootstrap(pid, &segs)?;
    // 续拉游标 = 最高封存段末尾(其后即活跃段,逻辑补),并**绑定 peer 的坐标系身份** —— 否则
    // "拷完段、还没问过对端身份"这段时间里对端若重写了 log,这个游标会看似合法(静默漏数据)。
    let last = listing
        .segments
        .iter()
        .max_by_key(|s| s.position)
        .expect("non-empty");
    Ok((Some(FindToken::at(last.name(), last.data_end)), stats))
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
    let empty = replicas
        .get(pid)
        .map(|h| h.store.is_empty())
        .unwrap_or(false);
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
    let (Some(p), Some(l)) = (peer.offset(), applied.offset()) else {
        return 0;
    };
    let segs = i128::from(p.segment).saturating_sub(i128::from(l.segment));
    let offs = i128::from(p.offset).saturating_sub(i128::from(l.offset));
    let bytes = segs
        .saturating_mul(i128::from(segment_capacity))
        .saturating_add(offs);
    // 负值 = 本地已 apply 越过 peer 游标(对端重建/清空等),按"未滞后"报 0。
    u64::try_from(bytes).unwrap_or(0)
}

/// 本地是否**已没有** peer 游标 `token` 之后的内容(=「本地 log 末端与 peer 报的游标一致」)。
///
/// 与 peer 侧游标同语义(`BlobStore::find_entries_since`:游标指向"最后一条已审视记录的**起点**",
/// 从该游标再问一次会**跳过**该记录本身):返回空条目 + 游标没推进 = 本地已到 peer 游标处。
/// 两处都空(双方皆空库 → 双侧 `Uninitialized`)同样算一致。
///
/// 三道守卫:
/// - **本地一条记录都没有**时,只有 peer 游标也是"空"(`Uninitialized`)才算一致 —— 否则说明本地
///   落后于 peer(还什么都没拉到),绝不能判追平(挡住"陈旧游标 + 空/重建的本地库"这种误报);
/// - 探测返回的游标**必须没推进**:游标推进说明本地游标之后还有候选记录(只是索引里已无对应赢家),
///   那就不算"末端一致"(保守);
/// - 比较**只比位置**:游标里的坐标系身份是**对端**的身份,本地无从校验(本地调 `find_entries_since`
///   会对不认识的游标做重定位 → 位置对不上 → 保守不追平,这正是想要的方向)。
///
/// 拿不准(本地读失败)一律返回 `false` → 保守留在 `BOOTSTRAP`。
fn local_has_nothing_past(local: &BlobStore, token: FindToken) -> bool {
    if local.is_empty() {
        return token == FindToken::Uninitialized;
    }
    match token.offset() {
        // peer 也空(或尚无游标)而本地非空 ⇒ 本地有对端没有的内容,保守不判追平。
        None => false,
        // 只比**位置**:peer 游标的段身份属于对端坐标系,本地无从校验(见 `BlobStore::has_content_after`)。
        Some(o) => !local.has_content_after(o),
    }
}

/// 「本轮是否已追平该 peer」—— 转 `ACTIVE` 的判据(模块头有完整场景说明)。
///
/// 逐 peer 判:peer 无该分区副本(404)→ 满足;peer 正常应答且**本轮没有待 apply 的条目**
/// (`replicate_from_peer` 只在空条目批时返回)且本地 log 末端与 peer 游标一致 → 满足。
/// 拿到某个 peer 的"满足"即可置 `ACTIVE`(置位后单向,不因后续某轮拉不到而回退)。
///
/// **本轮发生过游标重定位 ⇒ 一律不算追平**:那种"末端一致"是在刚作废的坐标系上算出来的,
/// 据此置 `ACTIVE` 会让 master 的删源闸门误成立(见模块头与 `docs/design/store.md`)。
fn caught_up_with_peer(local: &BlobStore, out: &SyncOutcome) -> bool {
    if out.cursor_reset_reason.is_some() {
        return false;
    }
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
        tracing::info!(
            partition = pid,
            reason,
            "replica caught up; reporting ACTIVE"
        );
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
    prior_incarnation: Option<u64>,
    max_bytes: u64,
    bootstrap_enabled: bool,
) -> Result<SyncOutcome> {
    let out = sync_from_peer(
        replicas,
        pid,
        peer,
        prior_token,
        max_bytes,
        bootstrap_enabled,
    )?;
    let lag = lag_bytes(
        out.token,
        prior_token.unwrap_or_default(),
        replicas.segment_size_bytes(),
    );
    let Some(handle) = replicas.get(pid) else {
        return Ok(out);
    };
    handle.lag.set(lag);
    // 对端 incarnation 变了 = 它换了一代副本(重建/换盘):本轮"末端一致"是在作废的坐标系上算的,
    // 故**本轮不判追平**(下一轮从对端给的新游标继续,收敛后自然转 `ACTIVE`)。
    let incarnation_changed =
        matches!((prior_incarnation, out.peer_incarnation), (Some(a), Some(b)) if a != b);
    if incarnation_changed {
        tracing::info!(
            partition = pid,
            prior = ?prior_incarnation,
            current = ?out.peer_incarnation,
            "peer replica incarnation changed; not marking caught up this round"
        );
    }
    if !incarnation_changed && caught_up_with_peer(&handle.store, &out) {
        mark_caught_up(replicas, pid, "peer_synced");
    }
    Ok(out)
}

/// 一个 `(partition, peer)` 的**续拉会话**:游标 + 对端 log 身份 + 无进展计数。
///
/// 抽成独立类型,是为了让"身份变了 ⇒ 本轮不判追平"与"连续无进展 ⇒ 丢弃游标从头重拉"这套状态推进
/// **被集成测试直接驱动**(测试与生产用同一份状态机,不给测试另写一套)。
#[derive(Debug, Clone, Default)]
pub struct PullSession {
    token: FindToken,
    /// 上次见到的对端 incarnation(副本级身份);用于判"对面是不是换了一代副本"。
    incarnation: Option<u64>,
    stalled: u32,
    resets: u64,
}

impl PullSession {
    pub fn new() -> Self {
        Self::default()
    }

    /// 游标所声明的 incarnation(与 [`prior_token`](Self::prior_token) 一起发给对端)。
    pub fn prior_incarnation(&self) -> Option<u64> {
        self.incarnation
    }

    /// 本轮要发给对端的游标;`None` = 从 log 起点重拉(首次,或上一轮判定游标已不可用)。
    pub fn prior_token(&self) -> Option<FindToken> {
        if self.token == FindToken::Uninitialized {
            None
        } else {
            Some(self.token)
        }
    }

    /// 当前续拉游标。
    pub fn token(&self) -> FindToken {
        self.token
    }

    /// 累计发生的游标重定位次数(观测/断言用)。
    pub fn resets(&self) -> u64 {
        self.resets
    }

    /// 一轮结束后推进会话,返回**该记的指标标签**(每项一次 `store_server_replication_cursor_resets_total`)。
    ///
    /// 三条状态推进:
    /// 1. 对端要求重定位(它自己的 revalidate / 身份不符)→ 记 `cursor_reset_reason`;
    /// 2. 对端换了一代副本 → 记 `incarnation_mismatch`(第二道防线:即使它没报重定位,身份变化本身
    ///    就说明旧游标作废);
    /// 3. 连续两轮"游标没动且没 apply" → 丢弃游标(下轮从头重拉)并记 `no_progress` ---- 这是
    ///    "对端行为超出预期"时的收敛兜底,否则副本会永远卡在 `BOOTSTRAP`。
    pub fn advance(&mut self, out: &SyncOutcome) -> Vec<String> {
        let mut labels = Vec::new();
        if let Some(reason) = &out.cursor_reset_reason {
            labels.push(reason.clone());
            self.resets += 1;
        }
        // 对端 incarnation 变了(第二道防线:即使它没报重定位,换了一代副本本身就说明游标作废)。
        if let Some(inc) = out.peer_incarnation {
            if self.incarnation.is_some_and(|prev| prev != inc) {
                labels.push("incarnation_mismatch".to_string());
                self.resets += 1;
            }
            self.incarnation = Some(inc);
        }
        if out.stats.applied > 0 || out.token != self.token {
            self.stalled = 0;
        } else {
            self.stalled += 1;
            if self.stalled >= 2 {
                self.stalled = 0;
                self.resets += 1;
                labels.push("no_progress".to_string());
                tracing::info!(
                    token = ?out.token,
                    "replication cursor stalled; restarting full pull"
                );
                // 不落 `out.token`(它就是那个不再推进的旧游标),下轮从起点重拉。
                self.token = FindToken::Uninitialized;
                return labels;
            }
        }
        self.token = out.token;
        labels
    }
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
    pool: Arc<crate::disk_pool::DiskPools>,
    health: crate::disk_health::DiskHealthMap,
) {
    tokio::spawn(async move {
        if let Err(e) = run(config, replicas, metrics, paused, pool, health).await {
            tracing::warn!(error = %e, "store-server replicator stopped");
        }
    });
}

async fn run(
    config: StoreServerConfig,
    replicas: Arc<ReplicaManager>,
    metrics: Arc<StoreServerMetrics>,
    paused: Arc<AtomicBool>,
    pool: Arc<crate::disk_pool::DiskPools>,
    health: crate::disk_health::DiskHealthMap,
) -> Result<()> {
    let mut client = MasterClient::connect(format!("http://{}", config.master_addr))
        .await
        .map_err(|e| Error::Replication(e.to_string()))?;
    let timeout = Duration::from_secs(config.disk_health_timeout_secs.max(1));
    let mut sessions: HashMap<(PartitionId, u64), PullSession> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(config.replication_interval_secs));
    loop {
        tick.tick().await;
        // A7 复制开关:每轮读一次。暂停只跳过**本节点主动拉**这一轮 —— 别人来拉本节点(复制读端点)
        // 不受影响,`RequestControl` 静默也与之正交;恢复后从同一游标自然续拉(游标跨暂停保留)。
        if paused.load(Ordering::Relaxed) {
            continue;
        }
        let pids: Vec<PartitionId> = replicas
            .replica_states()
            .into_iter()
            .map(|s| s.partition_id)
            .collect();
        for pid in pids {
            // **盘不可服务 ⇒ 跳过该 pid**(既不做源也不做目标),但**绝不动游标** —— 恢复后从原游标
            // 续拉,与 `/admin/replication-control` 的暂停语义一致(`sessions` 里的 prior 状态原样留着)。
            // 往一块已知卡死的盘上再塞一轮拉取,除了再漏一条线程之外毫无收益。
            let disk_id = match replicas.get(pid) {
                Some(h) => h.disk_id,
                None => continue,
            };
            if !crate::disk_health::is_available(&health, &disk_id) {
                metrics
                    .disk_worker_skipped
                    .with_label_values(&[disk_id.as_str(), "replication"])
                    .inc();
                tracing::debug!(partition = pid, disk = %disk_id, "replication skipped: disk unavailable");
                continue;
            }
            let locs = match client
                .lookup_partition(LookupRequest {
                    version: 1,
                    partition_id: pid,
                })
                .await
            {
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
                    let key = (pid, loc.node_id);
                    let (prior, prior_incarnation) = {
                        let s = sessions.entry(key).or_default();
                        (s.prior_token(), s.prior_incarnation())
                    };
                    let store = replicas.clone();
                    let base = format!("http://{}", loc.addr);
                    let max_bytes = config.replication_max_batch_bytes;
                    let bootstrap = config.bootstrap_copy_enabled;
                    // 本地盘的写/读按盘归口到该盘的池(网络侧仍有自己的超时):一块盘挂死时,
                    // 占住的是**它自己**的 worker,而不是全节点共用的阻塞池。
                    // (`disk_id` 已在本 pid 的入口处取好并做过健康判定 —— 不再取第二次。)
                    let res = pool
                        .run(&disk_id, "replication", move || {
                            let peer = HttpPeer::new(base, pid, timeout)?
                                .with_incarnation(prior_incarnation);
                            sync_round(
                                &store,
                                pid,
                                &peer,
                                prior,
                                prior_incarnation,
                                max_bytes,
                                bootstrap,
                            )
                        })
                        .await;
                    match res {
                        Ok(Ok(out)) => {
                            metrics.replication_rounds.inc();
                            metrics.replication_applied.inc_by(out.stats.applied);
                            metrics.replication_bytes.inc_by(out.stats.bytes);
                            metrics.bootstrap_segments.inc_by(out.bootstrap.segments);
                            metrics.bootstrap_bytes.inc_by(out.bootstrap.bytes);
                            // 本轮真的物理重装过(`install_bootstrap` 把本地句柄整个换掉了)=
                            // 这份副本是**从零建起来的** → 向 master 申领新身份并在**新句柄**上采纳
                            // (旧身份会让拉取方手里的旧游标看似合法,见 `claim_incarnation`)。
                            // `out.bootstrap.segments > 0` 只有在 `install_bootstrap` 成功后才成立:
                            // 拷贝失败会被丢弃并回落纯逻辑拉取(统计保持 0)。
                            if out.bootstrap.segments > 0 {
                                match replicas.get(pid) {
                                    Some(h) => {
                                        claim_incarnation(&mut client, &config, pid, &h).await
                                    }
                                    None => tracing::warn!(
                                        partition = pid,
                                        "bootstrap installed a replica that is already gone"
                                    ),
                                }
                            }
                            // 会话推进(身份变化 / 无进展兜底)与打点都走 [`PullSession::advance`] ——
                            // 与集成测试**同一份状态机**。
                            let labels = sessions.entry(key).or_default().advance(&out);
                            for label in labels {
                                metrics
                                    .replication_cursor_resets
                                    .with_label_values(&[label.as_str()])
                                    .inc();
                            }
                        }
                        Ok(Err(_)) => metrics.replication_errors.inc(),
                        // 池拒(本盘被压满/未知盘):本轮跳过这个 pid,游标保持不动,下一轮再来。
                        // 不算复制的错(它没出错),只记日志 —— 池指标上已经有一次 rejected。
                        Err(reject) => tracing::debug!(
                            partition = pid,
                            disk = %disk_id,
                            reason = reject.as_str(),
                            "replication round skipped by disk pool"
                        ),
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
    use antfs_blob::{CompactPolicy, StoreConfig};

    /// 测试用的"历史口径"策略:只回收被取代的旧记录(墓碑/过期回收不在本模块的考察范围)。
    fn legacy_policy(ratio: f64) -> CompactPolicy {
        CompactPolicy::preserve_legacy(ratio)
    }

    fn store(dir: &std::path::Path) -> BlobStore {
        BlobStore::create(dir, 64 << 20, 1 << 20, 1000).unwrap()
    }

    /// 进程内 PeerSource:直接包裹一个 `BlobStore`,免 socket 的确定性收敛测试。
    ///
    /// 忠实镜像服务端 `replica_metadata` 的两步:①游标身份与**本副本身份**不符 → 重定位(等价于
    /// Ambry 段名代次不符);②引擎自带的**位置** revalidate(段没了 / 偏移越界)。
    struct LocalPeer<'a> {
        store: &'a BlobStore,
    }
    impl PeerSource for LocalPeer<'_> {
        fn metadata(&self, token: FindToken, max_bytes: u64) -> Result<PeerMetadata> {
            // 引擎侧只做**段身份**校验(位置 + 代次);副本 incarnation 由调用方(会话)比对 —— 测试里
            // 直接用 `LocalPeer` 时视为同一代副本。
            let info = self.store.find_entries_since(&token, max_bytes)?;
            Ok(PeerMetadata::Entries(MetadataResponse::from_find(
                info,
                self.store.incarnation(),
            )))
        }
        fn fetch_data(&self, key: &StoreKey) -> Result<Option<Vec<u8>>> {
            match self.store.get_with_options(key, true) {
                Ok(d) => Ok(Some(d)),
                Err(antfs_blob::Error::NotFound) => Ok(None),
                Err(e) => Err(Error::Store(e)),
            }
        }
        fn sealed_segments(&self) -> Result<SealedListing> {
            let segments = self
                .store
                .sealed_segments()
                .into_iter()
                .map(|(name, data_end)| SegmentInfo {
                    position: name.position,
                    generation: name.generation,
                    data_end,
                })
                .collect();
            Ok(SealedListing {
                incarnation: self.store.incarnation(),
                segments,
            })
        }
        fn fetch_segment(&self, name: antfs_blob::LogSegmentName) -> Result<Vec<u8>> {
            self.store.read_segment_prefix(name).map_err(Error::Store)
        }
    }

    /// 测试用的 `SyncOutcome` 构造器(默认字段够用,避免 struct-update 把 String 字段 move 掉)。
    fn outcome(no_replica: bool, applied: u64) -> SyncOutcome {
        SyncOutcome {
            token: FindToken::Uninitialized,
            stats: ReplicateStats {
                applied,
                bytes: applied * 8,
            },
            bootstrap: BootstrapStats::default(),
            no_replica,
            cursor_reset_reason: None,
            peer_incarnation: None,
        }
    }

    fn key(n: u8) -> StoreKey {
        StoreKey::from_parts(0, n as u32)
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
        assert!(matches!(
            local.get(&key(1)),
            Err(antfs_blob::Error::Deleted)
        ));
    }

    /// 滞后线性化:跨段按段容量折算;peer 不在前 / 游标未知 → 0。
    #[test]
    fn lag_bytes_linearizes_across_segments() {
        let cap = 1 << 20;
        let early = FindToken::at(LogSegmentName::new(1, 1), 1000);
        let late = FindToken::at(LogSegmentName::new(3, 1), 500);
        assert_eq!(lag_bytes(late, early, cap), 2 * cap - 500);
        assert_eq!(lag_bytes(early, late, cap), 0, "本地已在前 → 未滞后");
        assert_eq!(lag_bytes(early, early, cap), 0, "追平 → 0");
        assert_eq!(
            lag_bytes(FindToken::Uninitialized, early, cap),
            0,
            "无基线 → 0"
        );
        assert_eq!(
            lag_bytes(early, FindToken::Uninitialized, cap),
            0,
            "无基线 → 0"
        );
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
        let (d_local, d_peer, d_empty) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let empty = store(d_empty.path());
        // 双方皆空(游标都是 Uninitialized)→ 一致。
        assert!(
            local_has_nothing_past(&empty, FindToken::Uninitialized),
            "空分区(两侧都空)必须算追平"
        );

        // 本地有数据 + peer 游标未知:本地还有内容 → 不一致。
        let local = store(d_local.path());
        local.put(key(1), b"a").unwrap();
        assert!(
            !local_has_nothing_past(&local, FindToken::Uninitialized),
            "本地有数据 ≠ 追平"
        );

        // 本地一条都没有 + peer 游标非空:本地落后 → 不一致。
        assert!(!local_has_nothing_past(
            &empty,
            FindToken::at(LogSegmentName::new(0, 1), 0)
        ));

        // 本地已从 peer 拉到末端:从 peer 游标再问本地库一无所获 → 一致。
        let peer = store(d_peer.path());
        peer.put(key(2), b"b").unwrap();
        let src = LocalPeer { store: &peer };
        let out = replicate_from_peer(&empty, &src, FindToken::Uninitialized, u64::MAX).unwrap();
        assert_eq!(out.stats.applied, 1, "peer 的 1 条记录必须拉过来");
        assert!(
            local_has_nothing_past(&empty, out.token),
            "本地已 apply 到 peer 末端 → 一致"
        );
    }

    /// `no_replica` 的判据半:peer 无该分区副本 → 满足(没有数据要同步);本轮从它身上拉到过东西则不算。
    #[test]
    fn no_replica_peer_counts_as_caught_up_only_when_nothing_applied() {
        let d = tempfile::tempdir().unwrap();
        let local = store(d.path());
        assert!(
            caught_up_with_peer(&local, &outcome(true, 0)),
            "peer 无副本 = 无数据可同步 → 算追平"
        );
        assert!(
            !caught_up_with_peer(&local, &outcome(true, 1)),
            "拉到一半对方删了副本 → 保守不追平"
        );
        // 本轮发生过**游标重定位** → 一律不算追平(那种"末端一致"是在作废的坐标系上算的)。
        let mut reset = outcome(false, 0);
        reset.cursor_reset_reason = Some("incarnation_mismatch".to_string());
        assert!(!caught_up_with_peer(&local, &reset), "重定位那轮不得判追平");
    }
    /// 会话状态机(生产 `run()` 与集成测试**共用同一份**):
    /// ①对端要求重定位 → 记原因;②对端换了一代副本 → 记 `incarnation_mismatch`;③连续无进展 → 丢弃游标。
    #[test]
    fn pull_session_advances_on_reset_incarnation_change_and_stall() {
        let mut s = PullSession::new();
        assert!(s.prior_token().is_none(), "首次:没有游标,从起点拉");

        // ① 对端要求重定位。
        let mut out = outcome(false, 0);
        out.token = FindToken::at(LogSegmentName::new(0, 5), 10);
        out.cursor_reset_reason = Some("segment_missing".to_string());
        out.peer_incarnation = Some(5);
        assert_eq!(s.advance(&out), vec!["segment_missing".to_string()]);
        assert_eq!(
            s.prior_token(),
            Some(FindToken::at(LogSegmentName::new(0, 5), 10))
        );
        assert_eq!(s.resets(), 1);

        // ② 对端换了一代副本(incarnation 变了):即使它没报重定位,也必须作废游标。
        let mut out = outcome(false, 0);
        out.token = FindToken::at(LogSegmentName::new(0, 5), 10);
        out.peer_incarnation = Some(6);
        assert_eq!(s.advance(&out), vec!["incarnation_mismatch".to_string()]);
        assert_eq!(s.resets(), 2);

        // ③ 无进展(游标没动、也没 apply)连续两轮 → 丢弃游标,下轮从头重拉。
        // 这里用**新会话**:上一段的 ② 本身也是"游标没动",已经为那个会话记了一轮无进展。
        let mut s2 = PullSession::new();
        let mut seed = outcome(false, 1); // 先推进一轮(拿到游标)
        seed.token = FindToken::at(LogSegmentName::new(0, 5), 10);
        assert!(s2.advance(&seed).is_empty());
        let mut idle = outcome(false, 0);
        idle.token = FindToken::at(LogSegmentName::new(0, 5), 10);
        assert!(s2.advance(&idle).is_empty(), "第一轮无进展只记账");
        assert_eq!(s2.advance(&idle), vec!["no_progress".to_string()]);
        assert!(s2.prior_token().is_none(), "丢弃游标 → 下轮从起点重拉");
        assert_eq!(s2.resets(), 1);
    }

    /// 核心危险路径(单元级):**两次拉取之间对端做了全库 compact** —— 旧游标 + 旧身份。
    ///
    /// 断言三件事:①对端作废旧游标(重定位而非"看似已追上");②重写后的新数据必须拉到、旧数据不得丢;
    /// ③重定位那一轮**不得判追平**(下一轮才恢复判定)。对齐 `docs/design/store.md` 的"复制游标重定位"。
    #[test]
    fn peer_full_compaction_between_rounds_is_revalidated_and_converges() {
        let (dp, dl) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let peer = store(dp.path());
        for n in 0..12u8 {
            peer.put(key(n), &[n; 64]).unwrap();
        }
        let local = store(dl.path());
        let src = LocalPeer { store: &peer };
        let mut session = PullSession::new();

        // 第一轮:拉满,会话记住"对端身份 + 游标"。
        let out1 = replicate_from_peer(
            &local,
            &src,
            session.prior_token().unwrap_or_default(),
            u64::MAX,
        )
        .unwrap();
        assert_eq!(out1.stats.applied, 12);
        assert!(out1.cursor_reset_reason.is_none(), "首轮无重定位");
        assert!(session.advance(&out1).is_empty());
        assert_eq!(
            session.prior_incarnation(),
            Some(peer.incarnation()),
            "会话必须记住对端 incarnation"
        );

        // 两次 tick 之间:对端全库重写(换身份)+ 重写后又写了一条新数据。
        let max_gen_before = peer.max_segment_generation();
        peer.compact(&legacy_policy(0.3)).unwrap();
        assert!(
            peer.max_segment_generation() > max_gen_before,
            "全库 compact 后段代次必须接着旧 log 往后取(段身份永不复用)"
        );
        peer.put(key(12), b"after-compact").unwrap();

        // 第二轮:旧游标 + 旧身份 → 对端重定位,必须把重写后的新数据带来。
        let out2 = replicate_from_peer(
            &local,
            &src,
            session.prior_token().unwrap_or_default(),
            u64::MAX,
        )
        .unwrap();
        assert!(
            out2.cursor_reset_reason.is_some(),
            "对端必须作废旧游标(而不是返回空批)"
        );
        assert!(
            !caught_up_with_peer(&local, &out2),
            "重定位那一轮不得判追平"
        );
        assert_eq!(
            local.get(&key(12)).unwrap(),
            b"after-compact",
            "重写后新写的数据必须拉到"
        );
        for n in 0..12u8 {
            assert!(local.get(&key(n)).is_ok(), "旧数据不得丢(第 {n} 条)");
        }
        let labels = session.advance(&out2);
        assert!(
            labels.iter().any(|l| l == "segment_generation_mismatch"),
            "全库 compact 在同一 incarnation 内重写 log ⇒ 必须由**段身份(代次)**识破: {labels:?}"
        );

        // 第三轮:身份稳定、无重定位 → 恢复判定,且确实已追上。
        let out3 = replicate_from_peer(
            &local,
            &src,
            session.prior_token().unwrap_or_default(),
            u64::MAX,
        )
        .unwrap();
        assert!(out3.cursor_reset_reason.is_none());
        assert!(session.advance(&out3).is_empty());
        assert!(
            caught_up_with_peer(&local, &out3),
            "身份稳定后必须能正常判追平(否则副本永远转不了 ACTIVE)"
        );
    }
}

//! 请求静默(对标 Ambry `RequestControl` / `DataNodeRequestControl`):运维可在**节点级**或
//! **分区级**临时屏蔽某类数据面操作(如 put),用于止血/降级,不重启、不摘盘。
//!
//! 两条硬约束(见 [`classify`] 的豁免清单):
//! 1. **绝不屏蔽拉复制需要的端点** —— 屏蔽 `?replication=1` 的 blob 读 / `replica-metadata` /
//!    `segments*` 会让副本停止收敛甚至掉数据(触碰持久性红线,CLAUDE.md §3.1);
//! 2. **绝不屏蔽 `/admin/**` 与 `/metrics`** —— 否则做出一个「无法远程恢复」的节点。
//!
//! 另外两处刻意偏离 Ambry:状态**不持久化**(重启即恢复全开),以及**带默认 TTL 的惰性过期**
//! (判定时比较 deadline,过期即视为启用并顺手摘除,不起后台清理任务)。三处偏离见
//! [`docs/design/store.md`](../../../docs/design/store.md) §12。

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use axum::http::Method;
use serde::Serialize;

use crate::PartitionId;

/// 可被静默的数据面操作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataOp {
    Put,
    Get,
    Delete,
    Undelete,
    TtlUpdate,
}

impl DataOp {
    /// 全部可选值(声明顺序;`parse` 与测试用)。
    pub const ALL: [DataOp; 5] =
        [DataOp::Put, DataOp::Get, DataOp::Delete, DataOp::Undelete, DataOp::TtlUpdate];

    /// 对外名称(admin API 的 `op` 字段、指标标签、日志字段的**唯一真值**)。
    pub fn as_str(self) -> &'static str {
        match self {
            DataOp::Put => "put",
            DataOp::Get => "get",
            DataOp::Delete => "delete",
            DataOp::Undelete => "undelete",
            DataOp::TtlUpdate => "ttl_update",
        }
    }

    /// 解析 admin API 的 `op` 字段;未知 → `None`(端点回 400)。
    pub fn parse(s: &str) -> Option<DataOp> {
        DataOp::ALL.into_iter().find(|op| op.as_str() == s)
    }
}

/// 把 HTTP 请求判为「可被静默的数据面操作」;返回 `None` = **豁免**(永不静默)。
///
/// 只有 5 类数据面请求可禁,其余一律豁免。豁免清单及理由(新增端点必须在此显式归类,
/// **默认豁免**是刻意的失败方向:宁可禁不掉,也不可禁出数据问题或做出不可运维的节点):
/// - `GET /partitions/{pid}/blobs/{key}?replication=1`:与普通 GET **同 method + 同 path**,
///   只能按 query 区分。它是拉复制 step-2 取数据体的通道,挡掉它 = 副本永久不收敛/掉数据。
/// - `GET /partitions/{pid}/replica-metadata`:拉复制 step-1(元数据游标),挡掉它 = 复制停摆。
/// - `GET /partitions/{pid}/segments` 与 `GET /partitions/{pid}/segments/{position}`:
///   拉复制 bootstrap 的物理段拷贝,挡掉它 = 空副本无法 bootstrap(只能退化为慢速逻辑补)。
/// - `GET /metrics`:运维抓取入口(挡掉它等于自瞎双眼)。
/// - 任何 `/admin/**`(**含 `POST|GET /admin/request-control` 自身**):否则禁用后无法远程恢复。
pub fn classify(method: &Method, path: &str, query: Option<&str>) -> Option<DataOp> {
    if path == "/metrics" || path.starts_with("/admin/") {
        return None;
    }
    let tail: Vec<&str> = path.strip_prefix("/partitions/")?.split('/').skip(1).collect();
    match (method, tail.as_slice()) {
        (&Method::PUT, ["blobs", _key]) => Some(DataOp::Put),
        (&Method::DELETE, ["blobs", _key]) => Some(DataOp::Delete),
        (&Method::GET, ["blobs", _key]) => {
            if replication_query(query) {
                None
            } else {
                Some(DataOp::Get)
            }
        }
        (&Method::POST, ["blobs", _key, "undelete"]) => Some(DataOp::Undelete),
        (&Method::POST, ["blobs", _key, "ttl"]) => Some(DataOp::TtlUpdate),
        // replica-metadata / segments / segments/{position} 与任何未知路径:豁免。
        _ => None,
    }
}

/// query 里是否带 `replication=<非 0>`(与 handler 的 `GetParams` 语义一致:`unwrap_or(0) != 0`)。
/// 值无法解析成数字时按"是复制读"处理(宁可漏禁,不可误禁复制读)。
fn replication_query(query: Option<&str>) -> bool {
    query.is_some_and(|q| {
        q.split('&').any(|kv| match kv.split_once('=') {
            Some(("replication", v)) => v.parse::<u8>() != Ok(0),
            _ => false,
        })
    })
}

/// 从数据面路径取 `{pid}`(`/partitions/{pid}/...`);非数据面路径/不可解析 → `None`(只按节点级判定)。
pub fn partition_of_path(path: &str) -> Option<PartitionId> {
    path.strip_prefix("/partitions/")?.split('/').next()?.parse().ok()
}

/// 判定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// 放行。
    Open,
    /// 静默(503);`retry_after_secs` = 剩余秒数,`None` = 永久禁用(不写 `Retry-After`)。
    Disabled { retry_after_secs: Option<u64> },
}

/// 一条禁用记录(存在即禁用)。
#[derive(Debug, Clone, Copy)]
struct Disable {
    /// `None` = 永久(不设截止时刻);`Some(t)` = 到 `t` 为止。
    deadline: Option<Instant>,
}

impl Disable {
    /// `ttl: None` = 永久;`Some(d)` = d 之后到期(`Some(Duration::ZERO)` 立即到期)。
    fn new(ttl: Option<Duration>) -> Self {
        Disable { deadline: ttl.map(|d| Instant::now() + d) }
    }

    fn expired(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|d| now >= d)
    }

    /// 剩余秒数(向上取整:剩 500ms 也报 1,避免 `Retry-After: 0`)。
    fn retry_after_secs(&self, now: Instant) -> Option<u64> {
        self.deadline.map(|d| {
            let left = d.saturating_duration_since(now);
            left.as_secs() + u64::from(left.subsec_nanos() > 0)
        })
    }
}

/// 单个操作的禁用范围(节点级 + 若干分区级);两条记录都是"存在即禁用"。
#[derive(Debug, Default)]
struct Scope {
    /// 节点级禁用(命中即静默全部该 op 请求)。
    node: Option<Disable>,
    /// 分区级禁用(`partition → 记录`;在节点级未禁用时生效)。
    partitions: HashMap<PartitionId, Disable>,
}

impl Scope {
    fn is_empty(&self) -> bool {
        self.node.is_none() && self.partitions.is_empty()
    }

    /// 判定并**顺手摘除**已过期条目(惰性过期)。
    fn gate(&mut self, partition: Option<PartitionId>, now: Instant) -> Gate {
        if let Some(d) = self.node {
            if d.expired(now) {
                self.node = None;
            } else {
                return Gate::Disabled { retry_after_secs: d.retry_after_secs(now) };
            }
        }
        if let Some(pid) = partition {
            if let Some(d) = self.partitions.get(&pid).copied() {
                if d.expired(now) {
                    self.partitions.remove(&pid);
                } else {
                    return Gate::Disabled { retry_after_secs: d.retry_after_secs(now) };
                }
            }
        }
        Gate::Open
    }
}

/// `GET /admin/request-control` 的一条禁用记录(`op` 用 [`DataOp::as_str`] 的真值)。
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DisableEntry {
    pub op: String,
    /// `None` = 节点级;`Some(pid)` = 只该分区。
    pub partition_id: Option<PartitionId>,
    /// 剩余秒数(向上取整);`None` = 永久(不设截止)。
    pub remaining_secs: Option<u64>,
}

/// 节点级请求静默状态。
#[derive(Debug)]
pub struct RequestControl {
    scopes: RwLock<HashMap<DataOp, Scope>>,
    /// 请求未指定 `ttl_secs` 时用的默认时长;`None` = 永久(配置 `0` 映射到这里)。
    default_ttl: Option<Duration>,
}

impl RequestControl {
    /// `default_ttl_secs = 0` → **不设默认时长**(即不填 `ttl_secs` 时永久禁用)。
    pub fn new(default_ttl_secs: u64) -> Self {
        RequestControl {
            scopes: RwLock::new(HashMap::new()),
            default_ttl: (default_ttl_secs > 0).then(|| Duration::from_secs(default_ttl_secs)),
        }
    }

    /// 设置/解除静默。`partition = None` 为节点级。
    ///
    /// - `enable = true`:`ttl_secs = None` 用配置默认时长(`Some(0)` = 立即到期,`None` 默认值
    ///   为永久);`Some(n)` = n 秒后自动恢复。
    /// - `enable = false`:**解除**;`partition = None` 时解除该 op 的**全部**静默(节点级 + 各分区),
    ///   指定分区时只解除该分区(便于运维"一键恢复全开"而不必逐个分区解)。
    pub fn set(
        &self,
        op: DataOp,
        partition: Option<PartitionId>,
        enable: bool,
        ttl_secs: Option<u64>,
    ) {
        let mut scopes = self.scopes.write().expect("request_control lock poisoned");
        if !enable {
            if let Some(scope) = scopes.get_mut(&op) {
                match partition {
                    Some(pid) => {
                        scope.partitions.remove(&pid);
                    }
                    None => {
                        // 节点级解除 = 该 op 全恢复(节点级 + 各分区),便于"一键恢复全开"。
                        scope.node = None;
                        scope.partitions.clear();
                    }
                }
                if scope.is_empty() {
                    scopes.remove(&op);
                }
            }
            return;
        }
        let disable =
            Disable::new(ttl_secs.map(Duration::from_secs).or(self.default_ttl));
        let scope = scopes.entry(op).or_default();
        match partition {
            Some(pid) => {
                scope.partitions.insert(pid, disable);
            }
            None => scope.node = Some(disable),
        }
    }

    /// 判定:`Disabled` 表示该请求应被 503 拒掉;已过期条目在此顺手摘除(惰性,无后台清理任务)。
    ///
    /// 数据面热路径走**读锁**快路径(`peek`):禁用是少见状态,常规请求不该在所有数据面请求间
    /// 争一把写锁。
    pub fn check(&self, op: DataOp, partition: Option<PartitionId>) -> Gate {
        let now = Instant::now();
        if let Some(gate) = self.peek(op, partition, now) {
            return gate;
        }
        self.check_at(op, partition, now)
    }

    /// 读锁快路径:无相关记录 → 放行;命中**未过期**记录 → 静默;返回 `None` = 存在**已过期**记录
    /// (需摘除)→ 交给写锁慢路径。判定优先级与 [`check_at`](Self::check_at) 一致(节点级优先)。
    fn peek(&self, op: DataOp, partition: Option<PartitionId>, now: Instant) -> Option<Gate> {
        let scopes = self.scopes.read().expect("request_control lock poisoned");
        let scope = scopes.get(&op)?;
        let node = scope.node;
        let partitioned = partition.and_then(|pid| scope.partitions.get(&pid).copied());
        if let Some(d) = [node, partitioned].into_iter().flatten().find(|d| !d.expired(now)) {
            return Some(Gate::Disabled { retry_after_secs: d.retry_after_secs(now) });
        }
        // 本次不适用:两个候选都存在过(且都已过期)才值得取写锁摘除,否则直接放行。
        if node.is_some() || partitioned.is_some() {
            None
        } else {
            Some(Gate::Open)
        }
    }

    /// 当前禁用集(已过期条目按"视为启用"剔除,与判定口径一致)。
    pub fn snapshot(&self) -> Vec<DisableEntry> {
        let now = Instant::now();
        let mut scopes = self.scopes.write().expect("request_control lock poisoned");
        let mut out = Vec::new();
        scopes.retain(|op, scope| {
            if let Some(d) = scope.node {
                if d.expired(now) {
                    scope.node = None;
                } else {
                    out.push(DisableEntry {
                        op: op.as_str().to_string(),
                        partition_id: None,
                        remaining_secs: d.retry_after_secs(now),
                    });
                }
            }
            scope.partitions.retain(|pid, d| {
                if d.expired(now) {
                    return false;
                }
                out.push(DisableEntry {
                    op: op.as_str().to_string(),
                    partition_id: Some(*pid),
                    remaining_secs: d.retry_after_secs(now),
                });
                true
            });
            !scope.is_empty()
        });
        out.sort_by(|a, b| (a.op.as_str(), a.partition_id).cmp(&(b.op.as_str(), b.partition_id)));
        out
    }

    /// 带显式时刻的判定(单测注入 `now` 用,免 sleep;生产一律走 [`check`](Self::check))。
    fn check_at(&self, op: DataOp, partition: Option<PartitionId>, now: Instant) -> Gate {
        let mut scopes = self.scopes.write().expect("request_control lock poisoned");
        let Some(scope) = scopes.get_mut(&op) else {
            return Gate::Open;
        };
        let gate = scope.gate(partition, now);
        if scope.is_empty() {
            scopes.remove(&op);
        }
        gate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(s: &str) -> Method {
        Method::from_bytes(s.as_bytes()).expect("valid method")
    }

    /// `router()` 里每条路由的归类(防止后人加端点被默认挡掉、或把复制/运维端点误挡)。
    #[test]
    fn classify_covers_every_router_route() {
        // 可禁用的 5 类。
        assert_eq!(classify(&m("PUT"), "/partitions/0/blobs/0011223344556677", None), Some(DataOp::Put));
        assert_eq!(classify(&m("GET"), "/partitions/0/blobs/0011223344556677", None), Some(DataOp::Get));
        assert_eq!(classify(&m("DELETE"), "/partitions/0/blobs/0011223344556677", None), Some(DataOp::Delete));
        assert_eq!(
            classify(&m("POST"), "/partitions/0/blobs/0011223344556677/undelete", None),
            Some(DataOp::Undelete)
        );
        assert_eq!(
            classify(&m("POST"), "/partitions/0/blobs/0011223344556677/ttl", Some("expiration_ms=1")),
            Some(DataOp::TtlUpdate)
        );

        // 白名单:router() 里其余每条路由 × 每种方法都必须判 None。
        for path in [
            "/partitions/0/replica-metadata",
            "/partitions/0/segments",
            "/partitions/0/segments/3",
            "/metrics",
            "/admin/partitions/0",
            "/admin/partitions/0/compact",
            "/admin/request-control",
            "/admin/replication-control",
            "/admin/compactions",
            "/admin/compactions/7",
        ] {
            for method in ["GET", "POST", "PUT", "DELETE"] {
                assert_eq!(classify(&m(method), path, None), None, "{method} {path} 必须豁免");
            }
        }
    }

    /// 拉复制读与普通读同 method + 同 path,只能按 query 区分 —— 这是最危险的一条。
    #[test]
    fn classify_splits_replication_read_from_plain_read_by_query() {
        let path = "/partitions/0/blobs/0011223344556677";
        assert_eq!(classify(&m("GET"), path, Some("replication=1")), None, "复制读必须放行");
        assert_eq!(classify(&m("GET"), path, Some("replication=2")), None);
        assert_eq!(classify(&m("GET"), path, Some("x=1&replication=1")), None);
        assert_eq!(classify(&m("GET"), path, Some("replication=1&max_bytes=9")), None);
        assert_eq!(classify(&m("GET"), path, Some("replication=0")), Some(DataOp::Get));
        assert_eq!(classify(&m("GET"), path, Some("other=1")), Some(DataOp::Get));
        assert_eq!(classify(&m("GET"), path, None), Some(DataOp::Get));
    }

    /// 未知路径/方法默认豁免(空 path、非 partitions 前缀、已知前缀未知后缀)。
    #[test]
    fn classify_exempts_unknown_paths_by_default() {
        assert_eq!(classify(&m("PUT"), "/", None), None);
        assert_eq!(classify(&m("PUT"), "/partitions", None), None);
        assert_eq!(classify(&m("PUT"), "/partitions/0", None), None);
        assert_eq!(classify(&m("PUT"), "/partitions/0/blobs", None), None);
        assert_eq!(classify(&m("POST"), "/partitions/0/blobs/0011223344556677", None), None);
        assert_eq!(classify(&m("GET"), "/partitions/0/blobs/0011223344556677/undelete", None), None);
        assert_eq!(partition_of_path("/partitions/7/blobs/00"), Some(7));
        assert_eq!(partition_of_path("/metrics"), None);
        assert_eq!(partition_of_path("/partitions/x/blobs/00"), None);
    }

    #[test]
    fn op_name_roundtrip_is_single_source_of_truth() {
        for op in DataOp::ALL {
            assert_eq!(DataOp::parse(op.as_str()), Some(op));
        }
        assert_eq!(DataOp::parse("PUT"), None);
        assert_eq!(DataOp::parse(""), None);
    }

    /// 读锁快路径与写锁慢路径语义必须一致:过期 = 启用(快路径也放行),并随后被摘除。
    #[test]
    fn check_fast_path_treats_expired_as_open_then_prunes() {
        let rc = RequestControl::new(0);
        rc.set(DataOp::Put, None, true, Some(0)); // 立即到期
        assert_eq!(rc.check(DataOp::Put, None), Gate::Open, "过期 = 启用(快路径也不得误挡)");
        assert!(rc.scopes.read().expect("lock").is_empty(), "慢路径必须摘除过期条目");

        rc.set(DataOp::Put, None, true, None); // 永久
        assert!(
            matches!(rc.check(DataOp::Put, Some(7)), Gate::Disabled { .. }),
            "节点级禁用:快路径即可判定(全部区域)"
        );

        // 只禁了分区 3:其他分区/其他 op 由快路径直接放行。
        rc.set(DataOp::Get, Some(3), true, None);
        assert_eq!(rc.check(DataOp::Get, Some(4)), Gate::Open);
        assert!(matches!(rc.check(DataOp::Get, Some(3)), Gate::Disabled { .. }));
        assert_eq!(rc.check(DataOp::Delete, None), Gate::Open, "该 op 无任何记录");
    }

    /// 节点级禁用 → 所有分区都挡;解除后放行。
    #[test]
    fn node_level_scope_blocks_every_partition() {
        let rc = RequestControl::new(1800);
        assert_eq!(rc.check(DataOp::Put, Some(0)), Gate::Open);
        rc.set(DataOp::Put, None, true, None);
        assert!(matches!(rc.check(DataOp::Put, Some(0)), Gate::Disabled { .. }));
        assert!(matches!(rc.check(DataOp::Put, Some(99)), Gate::Disabled { .. }));
        assert!(matches!(rc.check(DataOp::Put, None), Gate::Disabled { .. }));
        // 别的 op 不受影响。
        assert_eq!(rc.check(DataOp::Get, Some(0)), Gate::Open);
        rc.set(DataOp::Put, None, false, None);
        assert_eq!(rc.check(DataOp::Put, Some(0)), Gate::Open);
    }

    /// 分区级禁用只挡该分区;解除节点级时一并全清(一键恢复)。
    #[test]
    fn partition_level_scope_is_narrow_and_fully_cleared_on_node_re_enable() {
        let rc = RequestControl::new(1800);
        rc.set(DataOp::Put, Some(1), true, None);
        assert!(matches!(rc.check(DataOp::Put, Some(1)), Gate::Disabled { .. }));
        assert_eq!(rc.check(DataOp::Put, Some(2)), Gate::Open);
        assert_eq!(rc.check(DataOp::Put, None), Gate::Open, "分区级不挡节点级判定");

        rc.set(DataOp::Put, Some(2), true, None);
        rc.set(DataOp::Put, None, false, None);
        assert_eq!(rc.check(DataOp::Put, Some(1)), Gate::Open);
        assert_eq!(rc.check(DataOp::Put, Some(2)), Gate::Open);

        // 只解一个分区时另一个仍禁。
        rc.set(DataOp::Put, Some(1), true, None);
        rc.set(DataOp::Put, Some(2), true, None);
        rc.set(DataOp::Put, Some(1), false, None);
        assert_eq!(rc.check(DataOp::Put, Some(1)), Gate::Open);
        assert!(matches!(rc.check(DataOp::Put, Some(2)), Gate::Disabled { .. }));
    }

    /// 惰性过期:判定时比较 deadline,过期即视为启用并摘除条目(注入 now,不 sleep)。
    #[test]
    fn expired_entries_are_pruned_lazily() {
        let rc = RequestControl::new(0);
        let t0 = Instant::now();
        rc.set(DataOp::Put, None, true, Some(60));
        rc.set(DataOp::Get, Some(3), true, Some(60));
        assert!(matches!(rc.check_at(DataOp::Put, None, t0), Gate::Disabled { .. }));
        // 60 秒后:过期 → 放行,且条目已被摘除(不留空壳)。
        let later = t0 + Duration::from_secs(61);
        assert_eq!(rc.check_at(DataOp::Put, None, later), Gate::Open);
        assert_eq!(rc.check_at(DataOp::Get, Some(3), later), Gate::Open);
        assert!(rc.scopes.read().expect("lock").is_empty(), "过期条目必须被摘除");
        assert!(rc.snapshot().is_empty());
    }

    /// 永久禁用(`ttl = None` 且配置默认 0)不带 `Retry-After`,且不会自行过期。
    #[test]
    fn permanent_disable_has_no_deadline() {
        let rc = RequestControl::new(0);
        rc.set(DataOp::Put, None, true, None);
        assert_eq!(rc.check(DataOp::Put, None), Gate::Disabled { retry_after_secs: None });
        assert!(matches!(rc.check(DataOp::Put, None), Gate::Disabled { retry_after_secs: None }));
    }

    /// 配置默认 TTL 生效;请求显式 `ttl_secs = 0` = 0 秒有效期(立即到期)。
    #[test]
    fn default_ttl_applies_and_zero_ttl_expires_immediately() {
        let rc = RequestControl::new(120);
        rc.set(DataOp::Delete, None, true, None);
        let entry = &rc.snapshot()[0];
        assert!(matches!(entry.remaining_secs, Some(s) if s <= 120 && s > 0));

        rc.set(DataOp::Put, None, true, Some(0));
        assert_eq!(rc.check(DataOp::Put, None), Gate::Open, "0 秒有效期立即到期");
        assert_eq!(rc.snapshot().len(), 1, "只留 Delete 那条");
    }

    /// `Retry-After` 向上取整:剩余不足 1 秒也报 1 秒。
    #[test]
    fn retry_after_rounds_up() {
        let rc = RequestControl::new(0);
        rc.set(DataOp::Put, None, true, Some(60));
        let t0 = Instant::now();
        let Gate::Disabled { retry_after_secs } = rc.check_at(DataOp::Put, None, t0) else {
            panic!("expected disabled");
        };
        assert_eq!(retry_after_secs, Some(60), "剩 60s 减微秒 → 向上取整 60");
        let Gate::Disabled { retry_after_secs } =
            rc.check_at(DataOp::Put, None, t0 + Duration::from_millis(59_500))
        else {
            panic!("expected disabled");
        };
        assert_eq!(retry_after_secs, Some(1), "剩 500ms 也必须报 1 而不是 0");
    }

    /// snapshot 的条目形状(op 名 + 范围 + 剩余秒数)。
    #[test]
    fn snapshot_reports_node_and_partition_entries() {
        let rc = RequestControl::new(0);
        rc.set(DataOp::Put, None, true, None);
        rc.set(DataOp::Get, Some(4), true, None);
        let snap = rc.snapshot();
        assert_eq!(
            snap,
            vec![
                DisableEntry { op: "get".to_string(), partition_id: Some(4), remaining_secs: None },
                DisableEntry { op: "put".to_string(), partition_id: None, remaining_secs: None },
            ]
        );
    }
}

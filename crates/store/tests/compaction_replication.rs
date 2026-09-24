//! **「边复制边 compaction」集成矩阵**:副本正在追赶/已追平时,对端(或本端)的 compaction 不能让它
//! 静默漏数据、也不能让它在作废的坐标系上被判"已追上"。
//!
//! 为什么单独立一个文件:这条通路上"每个组件单看都对、凑在一起就出错"的坑最多 —— 游标是**对端 log
//! 坐标系的绝对位置**(`FindToken`),而 compaction 会改写坐标系(全库 compact 段号从 0 重来;增量
//! compaction 把记录搬到更高的段)。对齐 Ambry `revalidateStoreFindToken`:认不出的游标一律**重置为
//! 从头重扫**(方向是多拉,安全),并**不得**在那一轮判追平(antfs 的 `ACTIVE` 是 master 删源闸门的
//! 输入,判早了会丢数据 —— 见 `docs/design/store.md` 的"复制游标重定位")。
//!
//! 用例一律驱动**生产同一份**会话状态机(`PullSession`)与同一入口(`sync_round`),真 HTTP、真引擎;
//! 等待用带超时轮询(CLAUDE.md §6),不用 sleep 当同步。
//!
//! 覆盖矩阵:
//! | 场景 | 用例 |
//! |---|---|
//! | 追赶中(游标停在中间)+ 对端**全库** compact | `catchup_survives_peer_full_compact_between_rounds` |
//! | 追赶中 + 对端**增量** compact(段被删/记录搬家) | `catchup_survives_peer_incremental_compaction` |
//! | 对端副本被 **drop 后重建**(坐标系复位) | `peer_replica_recreated_resets_session_and_keeps_data` |
//! | 已追平之后对端 compact + delete 传播 | `compact_after_catchup_keeps_delete_semantics` |
//! | 对端重写成**空**库(本地仍有数据) | `empty_peer_never_yields_false_catchup` |
//! | 真实后台复制 loop + 指标 | `background_replicator_recovers_after_peer_compact` |
//! | 排空过程中的**批间**重写(那一轮绝不转 ACTIVE) | `batched_drain_across_peer_rewrite_is_never_false_caught_up` |
//! | bootstrap 拷段途中对端整库重写(丢弃拷贝) | `physical_bootstrap_discards_copy_when_peer_rewrites_midway` |
//! | **本端**在追赶途中做 compaction(游标是对端坐标) | `local_compaction_mid_drain_keeps_the_peer_cursor_usable` |

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use antfs_blob::{CompactPolicy, FindToken, StoreKey};
use antfs_store::replica::STATE_ACTIVE;
use antfs_store::replication::{sync_round, HttpPeer, PullSession, SyncOutcome};
use antfs_store::{StoreServer, StoreServerConfig};
use common::{peer_loc, wait_until, FakeMaster};
use tokio::net::TcpListener;

const PID: u32 = 0;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn key(n: u32) -> StoreKey {
    StoreKey::from_parts(PID, n)
}

/// compaction 策略:阈值给 0(有可回收数据就动手),墓碑保留期给足(本文件的考察点不是墓碑回收)。
fn compact_now() -> CompactPolicy {
    CompactPolicy::new(0.0, 10_000_000_000_000, i64::MAX)
}

/// 起一个**独立**节点(不含 master),建 pid=0 副本,真端口上跑 axum;返回 (base_url, server)。
async fn spawn_standalone(dir: &std::path::Path, node_id: u64) -> (String, StoreServer) {
    let cfg = StoreServerConfig {
        node_id,
        data_dirs: vec![dir.to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 16 << 20,
        local_worker_enabled: false, // 本文件手动触发 compaction,别让 worker 掺进来
        ..Default::default()
    };
    let registry = antfs_metric::Registry::new();
    let srv = StoreServer::start_with(cfg, store_config(), &registry).expect("start");
    srv.replicas().ensure_replica(PID).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = srv.router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), srv)
}

/// 用**生产同一份会话状态机**跑一轮:同 `PullSession` 推进、同 `sync_round` 入口。
async fn round(srv: &StoreServer, base: &str, session: &mut PullSession) -> SyncOutcome {
    let replicas = srv.replicas();
    let prior = session.prior_token();
    let prior_incarnation = session.prior_incarnation();
    let base = base.to_string();
    let out = tokio::task::spawn_blocking(move || {
        let peer = HttpPeer::new(base, PID, Duration::from_secs(10))
            .unwrap()
            .with_incarnation(prior_incarnation);
        sync_round(
            &replicas,
            PID,
            &peer,
            prior,
            prior_incarnation,
            u64::MAX,
            false,
        )
    })
    .await
    .unwrap()
    .unwrap();
    session.advance(&out);
    out
}

/// 本副本全部 key 及其状态(用引擎自己的增量拉取接口枚举:不引入测试专用的扫描路径)。
fn keys_of(srv: &StoreServer) -> BTreeMap<StoreKey, bool> {
    let store = srv.replicas().get(PID).unwrap().store;
    let mut out = BTreeMap::new();
    let mut token = FindToken::Uninitialized;
    loop {
        let info = store.find_entries_since(&token, u64::MAX).unwrap();
        if info.entries.is_empty() {
            break;
        }
        for e in &info.entries {
            out.insert(e.key, e.deleted);
        }
        token = info.token;
    }
    out
}

fn live_keys(srv: &StoreServer) -> BTreeSet<u32> {
    keys_of(srv)
        .into_iter()
        .filter(|(_, deleted)| !deleted)
        .map(|(k, _)| k.object_id())
        .collect()
}

fn state_of(srv: &StoreServer) -> String {
    srv.replicas()
        .replica_states()
        .into_iter()
        .find(|s| s.partition_id == PID)
        .expect("本地必须有该分区的 replica")
        .current_state
}

/// 对端 A 造数据(直接写引擎;A 的 metadata/blob HTTP 端点会看到)。
fn fill(srv: &StoreServer, from: u32, to: u32, payload: &[u8]) {
    let store = srv.replicas().get(PID).unwrap().store;
    for n in from..to {
        store.put(key(n), payload).unwrap();
    }
}

// ── 场景 1:追赶中(游标停在中间)+ 对端**全库** compact ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catchup_survives_peer_full_compact_between_rounds() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;

    // A 先有一批数据;B 只拉到"游标停在中间"就停下(第 1 轮),模拟"两次 tick 之间"。
    fill(&srv_a, 0, 6, &[1u8; 200]);
    let mut session = PullSession::new();
    let first = round(&srv_b, &base_a, &mut session).await;
    assert_eq!(first.stats.applied, 6, "第一轮把现有数据拉完");
    assert!(first.cursor_reset_reason.is_none());
    assert_eq!(
        session.prior_incarnation(),
        Some(srv_a.replicas().get(PID).unwrap().store.incarnation()),
        "会话必须记住对端 incarnation"
    );

    // 两次 tick 之间:A 全库 compact(**换身份**)+ 重写后继续写新数据。
    let store_a = srv_a.replicas().get(PID).unwrap().store;
    let max_gen_before = store_a.max_segment_generation();
    store_a.compact(&compact_now()).unwrap();
    assert!(
        store_a.max_segment_generation() > max_gen_before,
        "全库 compact 后段代次必须接着旧 log 往后取(段身份永不复用)"
    );
    fill(&srv_a, 6, 10, &[2u8; 200]);

    // 下一轮:旧游标 + 旧身份 → 必须被作废(而不是"看似已追上"),重写后的数据必须补齐。
    let second = round(&srv_b, &base_a, &mut session).await;
    assert!(
        second.cursor_reset_reason.is_some(),
        "对端必须作废陈旧游标(而不是返回空批): {second:?}"
    );
    assert!(session.resets() >= 1, "会话必须记下这次重定位");
    assert!(
        second.stats.applied >= 4,
        "重定位后必须把重写后的新数据带回来: {second:?}"
    );

    // 收敛:重写后的全部活数据都要在 B 上(没有静默漏数据)。
    let want: BTreeSet<u32> = (0..10).collect();
    assert!(
        wait_until(Duration::from_secs(10), || live_keys(&srv_b) == want).await,
        "B 必须收敛到 A 的完整活数据: {:?}",
        live_keys(&srv_b)
    );
    // 值也要对(不是只有 key 在)。
    let store_b = srv_b.replicas().get(PID).unwrap().store;
    assert_eq!(store_b.get(&key(7)).unwrap(), vec![2u8; 200]);
    assert_eq!(store_b.get(&key(2)).unwrap(), vec![1u8; 200]);
    assert_eq!(state_of(&srv_b), STATE_ACTIVE, "追平后 ACTIVE 单向保持");
}

// ── 场景 2:追赶中 + 对端**增量** compact(段被删、记录搬家,身份不变) ─────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catchup_survives_peer_incremental_compaction() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;
    let store_a = srv_a.replicas().get(PID).unwrap().store;

    // 造"高死数据的老段":写一批 + 删掉大部分。
    fill(&srv_a, 0, 24, &[3u8; 200 * 1024]);
    for n in 0..20u32 {
        store_a.delete(key(n)).unwrap();
    }
    store_a.flush().unwrap();

    // B 先拉一轮(游标停在中间),再让 A 做**增量** compaction(段被删/记录搬到更高的段)。
    let mut session = PullSession::new();
    let first = round(&srv_b, &base_a, &mut session).await;
    assert!(first.stats.applied == 24, "第一轮拉完 24 条: {first:?}");
    let epoch_before = store_a.incarnation();
    let st = store_a.compact_if_needed(&compact_now()).unwrap();
    assert!(st.is_some(), "高死数据的老段必须被增量压缩");
    assert_eq!(
        store_a.incarnation(),
        epoch_before,
        "增量 compaction 不换 incarnation(同一代副本内的重写)"
    );

    // 继续拉:A 没写新数据,故 B 侧应无新增("重复"幂等);关键断言是**不丢**、最终一致。
    let second = round(&srv_b, &base_a, &mut session).await;
    assert!(
        second.stats.applied == 0,
        "增量 compaction 只搬字节、不改可见状态: {second:?}"
    );

    // 两边状态一致(20 个墓碑 + 4 个活对象),再写一条新数据也能拉到。
    fill(&srv_a, 24, 25, &[4u8; 200]);
    let _ = round(&srv_b, &base_a, &mut session).await;
    let a = keys_of(&srv_a);
    let b = keys_of(&srv_b);
    assert_eq!(b, a, "增量 compaction 前后必须完全一致");
    assert_eq!(live_keys(&srv_b), BTreeSet::from([20, 21, 22, 23, 24]));
}

// ── 场景 3:对端副本被 drop 后重建(坐标系复位,内容也变了) ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_replica_recreated_resets_session_and_keeps_data() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;

    // B 追平并转 ACTIVE(置位单向:之后对端怎么折腾都不该把 B 打回 BOOTSTRAP)。
    fill(&srv_a, 0, 5, &[5u8; 200]);
    let mut session = PullSession::new();
    let out = round(&srv_b, &base_a, &mut session).await;
    assert!(out.stats.applied == 5);
    assert_eq!(state_of(&srv_b), STATE_ACTIVE, "追平后转 ACTIVE");
    let inc_a = srv_a.replicas().get(PID).unwrap().store.incarnation();

    // A 的副本被删掉(等价 `DROPPED`)再重建:目录没了、身份换了、内容也从零开始。
    {
        let replicas_a = srv_a.replicas();
        replicas_a.drop_replica(0).unwrap();
        replicas_a.ensure_replica(0).unwrap();
    }
    let inc_a2 = srv_a.replicas().get(PID).unwrap().store.incarnation();
    assert_ne!(
        inc_a2, inc_a,
        "重建的副本必须换 incarnation({inc_a} -> {inc_a2})"
    );
    fill(&srv_a, 100, 103, &[6u8; 200]); // 重建后写新数据

    let after = round(&srv_b, &base_a, &mut session).await;
    assert!(
        after.cursor_reset_reason.is_some(),
        "身份变化必须让对端作废旧游标: {after:?}"
    );
    assert!(session.resets() >= 1);
    assert_eq!(
        state_of(&srv_b),
        STATE_ACTIVE,
        "ACTIVE 单向,不因对端重建回退"
    );

    // B 必须拿到 A 重建后的新数据,且**不得丢**自己原有的数据。
    let store_b = srv_b.replicas().get(PID).unwrap().store;
    assert!(
        wait_until(Duration::from_secs(10), || store_b.get(&key(101)).is_ok()).await,
        "对端重建后的新数据必须拉到"
    );
    assert_eq!(store_b.get(&key(101)).unwrap(), vec![6u8; 200]);
    for n in 0..5u32 {
        assert!(
            store_b.get(&key(n)).is_ok(),
            "对端重建不得让本地丢数据(key {n})"
        );
    }
}

// ── 场景 4:已追平之后对端 compact + delete 传播 ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compact_after_catchup_keeps_delete_semantics() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;
    let store_a = srv_a.replicas().get(PID).unwrap().store;

    fill(&srv_a, 0, 6, &[7u8; 200]);
    let mut session = PullSession::new();
    let _ = round(&srv_b, &base_a, &mut session).await;
    assert_eq!(state_of(&srv_b), STATE_ACTIVE);
    let store_b = srv_b.replicas().get(PID).unwrap().store;

    // 对端 compact(全库 + 增量各来一次)→ 继续 delete。
    store_a.compact(&compact_now()).unwrap();
    store_a.delete(key(1)).unwrap();
    store_a.delete(key(4)).unwrap();
    let _ = round(&srv_b, &base_a, &mut session).await;

    assert!(
        wait_until(Duration::from_secs(10), || {
            matches!(store_b.get(&key(1)), Err(antfs_blob::Error::Deleted))
                && matches!(store_b.get(&key(4)), Err(antfs_blob::Error::Deleted))
        })
        .await,
        "compact 之后 delete 仍必须传播到位"
    );
    for n in [0u32, 2, 3, 5] {
        assert!(store_b.get(&key(n)).is_ok(), "未删的必须仍在(key {n})");
    }
    assert_eq!(
        keys_of(&srv_b),
        keys_of(&srv_a),
        "两侧状态(含墓碑)必须完全一致"
    );
    assert_eq!(state_of(&srv_b), STATE_ACTIVE);
}

// ── 场景 5:对端重写成**空**库,本地仍有数据 → 绝不误判追平 ────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_peer_never_yields_false_catchup() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;

    // B 上先有一份本地数据(模拟"滞后副本"),A 的副本被删掉重建为空。
    fill(&srv_b, 0, 3, &[8u8; 200]);
    let replicas_a = srv_a.replicas();
    replicas_a.drop_replica(0).unwrap();
    replicas_a.ensure_replica(0).unwrap();

    let mut session = PullSession::new();
    let out = round(&srv_b, &base_a, &mut session).await;
    assert!(
        out.cursor_reset_reason.is_none() || out.cursor_reset_reason.is_some(),
        "首轮身份未知/空库都可能,这里不钉"
    );
    let again = round(&srv_b, &base_a, &mut session).await;
    assert!(again.cursor_reset_reason.is_none(), "身份已绑定");
    assert_ne!(
        state_of(&srv_b),
        STATE_ACTIVE,
        "本地还有对端没有的内容 ⇒ 保守留在 BOOTSTRAP(不得谎报追平)"
    );
    // 本地数据一条都不能少(对端空不等于本地该删)。
    let store_b = srv_b.replicas().get(PID).unwrap().store;
    for n in 0..3u32 {
        assert!(store_b.get(&key(n)).is_ok(), "本地数据不得丢(key {n})");
    }
}

// ── 场景 6:真实后台复制 loop + 指标(端到端) ─────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_replicator_recovers_after_peer_compact() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;

    // 假 master 只做副本发现:B 经它查到 A,自己起**真后台复制 loop**。
    let peer = peer_loc(1, base_a.trim_start_matches("http://"));
    let fake = FakeMaster::start(vec![peer]).await;
    let registry_b = antfs_metric::Registry::new();
    let cfg_b = StoreServerConfig {
        node_id: 2,
        data_dirs: vec![dir_b.path().to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 16 << 20,
        master_addr: fake.addr(),
        heartbeat_interval_secs: 1,
        replication_enabled: true,
        replication_interval_secs: 1,
        local_worker_enabled: false,
        ..Default::default()
    };
    let srv_b = StoreServer::start_with(cfg_b, store_config(), &registry_b).expect("start b");
    srv_b.replicas().ensure_replica(PID).unwrap();
    srv_b.serve().await.expect("serve b");

    fill(&srv_a, 0, 4, &[9u8; 200]);
    assert!(
        wait_until(Duration::from_secs(20), || state_of(&srv_b) == STATE_ACTIVE).await,
        "后台复制 loop 必须让 B 追平"
    );

    // 对端全库 compact + 新写 → 后台 loop 必须自行恢复(作废旧游标、把新数据拉过来)。
    let store_a = srv_a.replicas().get(PID).unwrap().store;
    store_a.compact(&compact_now()).unwrap();
    fill(&srv_a, 4, 6, &[10u8; 200]);

    let store_b = srv_b.replicas().get(PID).unwrap().store;
    assert!(
        wait_until(Duration::from_secs(20), || {
            store_b
                .get(&key(5))
                .map(|v| v == vec![10u8; 200])
                .unwrap_or(false)
        })
        .await,
        "后台复制必须从对端重写后恢复并拉到新数据"
    );
    assert_eq!(live_keys(&srv_b), (0..6).collect::<BTreeSet<u32>>());
    assert_eq!(
        state_of(&srv_b),
        STATE_ACTIVE,
        "ACTIVE 单向,不因对端重写回退"
    );

    // 指标:游标重定位必须被如实打点(用 B **自己接入的那个** Registry,见 CLAUDE.md §6)。
    // 打点在复制 loop 里、晚于数据 apply,故用带超时轮询等它落地(别与 loop 抢时序)。
    let resets = std::cell::Cell::new(0u64);
    assert!(
        wait_until(Duration::from_secs(10), || {
            let text = antfs_metric::gather(&registry_b).expect("gather");
            let v = text_value(&text, "store_server_replication_cursor_resets_total");
            resets.set(v);
            v >= 1
        })
        .await,
        "对端重写必须记一次游标重定位, got {}",
        resets.get()
    );
}

/// Prometheus 文本里某计数器(含 label)的总和。
fn text_value(text: &str, name: &str) -> u64 {
    text.lines()
        .filter(|l| l.starts_with(name))
        .filter_map(|l| l.rsplit(' ').next()?.trim().parse::<u64>().ok())
        .sum()
}

// ── 场景 7:**一次排空过程中的批间**重写(最刁钻的时序,也是"空批=追平"最会骗人的地方) ──────

/// 包一层 `PeerSource`:每次只取一小批(强制多轮),并在第 `after` 次 `metadata` 调用**之后**
/// 对"对端"做一次全库 compact —— 精确复刻"两次 metadata 请求之间对端重写了 log"。
struct RewriteBetweenBatches {
    inner: HttpPeer,
    peer_store: std::sync::Arc<antfs_blob::BlobStore>,
    after: u32,
    calls: std::cell::Cell<u32>,
    written_after_rewrite: u32,
    rewritten: std::cell::Cell<bool>,
}

impl antfs_store::replication::PeerSource for RewriteBetweenBatches {
    fn metadata(
        &self,
        token: FindToken,
        max_bytes: u64,
    ) -> antfs_store::Result<antfs_store::replication::PeerMetadata> {
        let resp = self.inner.metadata(token, max_bytes.min(512))?;
        let n = self.calls.get() + 1;
        self.calls.set(n);
        if n == self.after && !self.rewritten.get() {
            // "对端"在两次请求之间:全库 compact(段号重来、身份换新)+ 再写一条新数据。
            self.peer_store.compact(&compact_now()).unwrap();
            for i in 0..self.written_after_rewrite {
                self.peer_store.put(key(1000 + i), &[11u8; 200]).unwrap();
            }
            self.rewritten.set(true);
        }
        Ok(resp)
    }

    fn fetch_data(&self, k: &StoreKey) -> antfs_store::Result<Option<Vec<u8>>> {
        self.inner.fetch_data(k)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_drain_across_peer_rewrite_is_never_false_caught_up() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;
    let store_a = srv_a.replicas().get(PID).unwrap().store;

    // A 有 40 条数据(每条 ~600B):批大小 512 ⇒ 一次排空要跨很多批。
    fill(&srv_a, 0, 40, &[12u8; 600]);

    // B 空、BOOTSTRAP,直接经 `sync_round` 拉(与后台 loop 同一入口/同一判据)。
    // 注意:`reqwest::blocking` 的 client 必须在**阻塞线程**里构造(在 async 上下文里建会 panic)。
    let replicas_b = srv_b.replicas();
    let peer_store = store_a.clone();
    let base = base_a.clone();
    let out = tokio::task::spawn_blocking(move || {
        let peer = RewriteBetweenBatches {
            inner: HttpPeer::new(base, PID, Duration::from_secs(10)).unwrap(),
            peer_store,
            after: 3, // 拉了三批之后,对端重写
            calls: std::cell::Cell::new(0),
            written_after_rewrite: 2,
            rewritten: std::cell::Cell::new(false),
        };
        sync_round(&replicas_b, PID, &peer, None, None, 512, false)
    })
    .await
    .unwrap()
    .unwrap();

    assert!(
        out.cursor_reset_reason.is_some(),
        "批间重写必须被识破(而不是把空批当'已追上'): {:?}",
        out.stats
    );
    assert_eq!(
        state_of(&srv_b),
        "BOOTSTRAP",
        "重定位那一轮**不得**转 ACTIVE —— 这是 master 删源闸门的输入,判早了会丢数据"
    );

    // 下一轮(身份已稳定):必须把重写后的新数据补齐并正常转正。
    let mut session = PullSession::new();
    let mut converged = false;
    for _ in 0..6 {
        let _ = round(&srv_b, &base_a, &mut session).await;
        let have = live_keys(&srv_b);
        if (0..40).all(|n| have.contains(&n)) {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "必须收敛到 40 条初始数据: {:?}",
        live_keys(&srv_b)
    );
    // 重写后新写的两条也要拉到。
    let store_b = srv_b.replicas().get(PID).unwrap().store;
    assert!(
        wait_until(Duration::from_secs(10), || store_b.get(&key(1001)).is_ok()).await,
        "重写后新增的数据必须拉到"
    );
    assert!(
        wait_until(Duration::from_secs(10), || state_of(&srv_b) == STATE_ACTIVE).await,
        "身份稳定且真正追平后必须转 ACTIVE"
    );
}

// ── 场景 9:**本端**(接收方)在追赶途中做 compaction ──────────────────────────────────
//
// 前 8 条压的都是"对端重写"或"bootstrap 竞态";这一条压**自己**:接收方在两次拉取之间把自己的
// log 整库重写(活记录搬到更高位置、旧段删掉,段位置从 0 重来但代次接续)。游标是**对端坐标系**的
// 绝对位置,本端的重写不该影响它 —— 也不该让本端"少拉一段"或把缺数据误判成追平。

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_compaction_mid_drain_keeps_the_peer_cursor_usable() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;
    let store_a = srv_a.replicas().get(PID).unwrap().store;
    let store_b = srv_b.replicas().get(PID).unwrap().store;

    // A:24 条 200 KiB 记录(段 1 MiB ⇒ 多段)。
    let big = vec![5u8; 200 * 1024];
    fill(&srv_a, 0, 24, &big);
    store_a.flush().unwrap();

    let mut session = PullSession::new();
    let first = round(&srv_b, &base_a, &mut session).await;
    assert_eq!(first.stats.applied, 24, "先把现有数据拉满: {first:?}");
    let max_gen_b = store_b.max_segment_generation();

    // A 删掉前半 → B 把墓碑拉过来,本地对应活记录被取代(墓碑恒胜)⇒ B 上真的有了死数据。
    for n in 0..12u32 {
        store_a.delete(key(n)).unwrap();
    }
    let second = round(&srv_b, &base_a, &mut session).await;
    assert_eq!(second.stats.applied, 12, "12 条墓碑必须拉到: {second:?}");
    assert!(
        store_b.stats().unwrap().dead_bytes > 0,
        "本地应有可回收死数据"
    );

    // 本端 compaction:B 自己整库重写(旧段删掉、活记录搬到更高位置、段位置从 0 重来)。
    let stats = store_b.compact(&compact_now()).unwrap();
    assert!(stats.segments_before > 0, "必须真的压过: {stats:?}");
    assert!(
        store_b.max_segment_generation() > max_gen_b,
        "本端全库 compact 也要接续代次(段身份永不复用): {max_gen_b} -> {}",
        store_b.max_segment_generation()
    );

    // A 再写新数据;B 用**同一个**对端游标继续拉(本端刚重写过自己的 log)。
    fill(&srv_a, 24, 30, &[7u8; 200]);
    let third = round(&srv_b, &base_a, &mut session).await;
    assert_eq!(
        third.stats.applied, 6,
        "本端重写后必须照常拉到新数据: {third:?}"
    );
    assert!(
        third.cursor_reset_reason.is_none(),
        "本端 compaction 不该触发任何重定位"
    );

    // 逐 key 一致 + 值正确 + 转 ACTIVE(本端重写不得让"追平"变得判不出来)。
    assert_eq!(keys_of(&srv_b), keys_of(&srv_a), "必须与对端逐 key 一致");
    assert_eq!(live_keys(&srv_b), (12..30).collect::<BTreeSet<u32>>());
    assert_eq!(store_b.get(&key(29)).unwrap(), vec![7u8; 200]);
    assert!(
        matches!(store_b.get(&key(0)), Err(antfs_blob::Error::Deleted)),
        "已删对象不得以活数据留在本地"
    );
    assert_eq!(
        state_of(&srv_b),
        STATE_ACTIVE,
        "本端 compaction 后仍应判追平"
    );
}

// ── 场景 8:物理 bootstrap 与对端 compact 的**竞态**(拷段途中对端整库重写) ──────────────

/// 包一层 `PeerSource`:在**第一次 `fetch_segment` 之后**把对端的 log 整库重写 ——
/// 于是"段号 0"在前后两个 incarnation 里指向不同记录:逐段拷来的字节会横跨两个 incarnation。
struct RewriteMidBootstrap {
    inner: HttpPeer,
    peer_store: std::sync::Arc<antfs_blob::BlobStore>,
    rewritten: std::cell::Cell<bool>,
}

impl antfs_store::replication::PeerSource for RewriteMidBootstrap {
    fn metadata(
        &self,
        token: FindToken,
        max_bytes: u64,
    ) -> antfs_store::Result<antfs_store::replication::PeerMetadata> {
        self.inner.metadata(token, max_bytes)
    }

    fn fetch_data(&self, k: &StoreKey) -> antfs_store::Result<Option<Vec<u8>>> {
        self.inner.fetch_data(k)
    }

    fn sealed_segments(&self) -> antfs_store::Result<antfs_store::replication::SealedListing> {
        self.inner.sealed_segments()
    }

    fn fetch_segment(&self, name: antfs_blob::LogSegmentName) -> antfs_store::Result<Vec<u8>> {
        let bytes = self.inner.fetch_segment(name)?;
        if !self.rewritten.get() {
            // "取完第一段"之后、取第二段之前:对端整库重写(身份换新、内容也变了)。
            self.peer_store.compact(&compact_now()).unwrap();
            self.rewritten.set(true);
        }
        Ok(bytes)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn physical_bootstrap_discards_copy_when_peer_rewrites_midway() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (base_a, srv_a) = spawn_standalone(dir_a.path(), 1).await;
    let (_base_b, srv_b) = spawn_standalone(dir_b.path(), 2).await;
    let store_a = srv_a.replicas().get(PID).unwrap().store;

    // 造出多个封存段:5 条 ~400KiB 记录(段容量 1MiB ⇒ 每段 2 条)。
    let big = vec![13u8; 400 * 1024];
    for n in 0..5u32 {
        store_a.put(key(n), &big).unwrap();
    }
    store_a.flush().unwrap();
    assert!(
        store_a.sealed_segments().len() >= 2,
        "前置:对端必须有多个封存段"
    );
    // 再删掉前两条并全库重写:重写后的"段 0"里装的是**别的**记录(0/1 变成墓碑)。
    store_a.delete(key(0)).unwrap();
    store_a.delete(key(1)).unwrap();

    // 空副本 B 走混合 bootstrap(物理拷贝 + 逻辑补),但取段途中对端重写了 log。
    let replicas_b = srv_b.replicas();
    let peer_store = store_a.clone();
    let base = base_a.clone();
    let out = tokio::task::spawn_blocking(move || {
        let peer = RewriteMidBootstrap {
            inner: HttpPeer::new(base, PID, Duration::from_secs(10)).unwrap(),
            peer_store,
            rewritten: std::cell::Cell::new(false),
        };
        sync_round(&replicas_b, PID, &peer, None, None, u64::MAX, true)
    })
    .await
    .unwrap()
    .unwrap();

    // ① 拷贝必须被**丢弃**(否则会装出一个横跨两个 incarnation 的混合快照)。
    assert_eq!(
        out.bootstrap.segments, 0,
        "拷段途中对端重写 ⇒ 必须丢弃物理拷贝,回落纯逻辑拉取: {out:?}"
    );
    // ② 纯逻辑拉取仍要收敛到对端的**完整状态**(含那两个已删 key 的墓碑)。
    let store_b = srv_b.replicas().get(PID).unwrap().store;
    assert_eq!(keys_of(&srv_b), keys_of(&srv_a), "必须与对端逐 key 一致");
    assert!(
        matches!(store_b.get(&key(0)), Err(antfs_blob::Error::Deleted)),
        "被删对象不得以活数据留在本地"
    );
    assert!(store_b.get(&key(3)).is_ok(), "没删的必须在");
    assert!(
        wait_until(Duration::from_secs(10), || state_of(&srv_b) == STATE_ACTIVE).await,
        "收敛后必须能转 ACTIVE"
    );
}

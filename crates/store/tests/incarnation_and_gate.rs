//! 副本身份申领(`AssignReplicaIncarnation`)+ 墓碑回收闸门(`tombstones_reclaim_allowed`)两条通路:
//! 假 master + store 侧**真 gRPC 心跳 loop**,断言落在真副本句柄上。
//!
//! 为什么这两条要一起盯:身份申领错了(没申领 / 采纳到旧句柄 / 每拍重复申领),别的节点手里的复制
//! 游标就会"看似合法",误判追平 → master 删源 → 丢数据;墓碑闸门接错了,还有副本没追平时就把墓碑
//! 回收掉,那份滞后副本永远学不到删除,已删对象会以"活数据"永久留在它上面(僵尸数据)。两条都是
//! "接缝"型缺陷 —— 单测各测一半看不出来。
//!
//! 等待一律有界条件轮询(CLAUDE.md §6),不用 sleep 当同步。
//!
//! 墓碑闸门的两个方向各起一个 store 实例:假 master 的心跳流会在**首条 beat 之后**把排队的指令
//! 一次性下发,同一实例内两拍之间只隔微秒 —— "false → true 的中间态"不可观测(断言会 flaky)。
//! 拆成两个实例后,每个方向的断言都确定、且都能被证伪(见各用例的注释)。

mod common;

use std::path::Path;
use std::time::Duration;

use antfs_master::pb::{MasterDirective, PartitionStateInfo};
use antfs_store::{StoreServer, StoreServerConfig};
use common::{wait_until, FakeMaster};

/// 假 master(`common::FakeMaster`)首发/逐次递增给出的 incarnation;本地兜底值是**纳秒时间戳**
/// (量级 ~1e18),故二者绝不会碰巧相等 —— 断言有证明力。
const ASSIGNED_INCARNATION: u64 = 1000;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

/// 接了假 master 的节点(本地维护 worker 关掉:本文件只关心控制面通路)。
fn node(dir: &Path, fake: &FakeMaster) -> StoreServer {
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        master_addr: fake.addr(),
        heartbeat_interval_secs: 1,
        local_worker_enabled: false,
        ..Default::default()
    };
    StoreServer::start_with(cfg, store_config(), &antfs_metric::Registry::new()).expect("start")
}

/// 一条只带分区态(含墓碑闸门)的指令 —— 闸门只记在**已存在**的副本上,故副本要先建出来。
fn gate_directive(pid: u32, allowed: bool) -> MasterDirective {
    MasterDirective {
        version: 1,
        transitions: Vec::new(),
        tasks: Vec::new(),
        partition_states: vec![PartitionStateInfo {
            partition_id: pid,
            state: "RW".to_string(),
            tombstones_reclaim_allowed: allowed,
        }],
    }
}

/// `Transition` 建出来的副本必须**向 master 申领**一份新身份并采纳到本地 —— 而不是把建库时的本地
/// 兜底值(纳秒时间戳)当最终值。
///
/// 若这里只是"建副本、不申领",重建/换盘后的副本会沿用旧身份,别的节点手里那条旧游标看似合法
/// → 误判"已追平" → master 据此删源 → 丢数据(CLAUDE.md §3.1)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creating_a_replica_claims_its_incarnation_from_master() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;
    fake.push_transition(0, "ACTIVE");

    let srv = node(tmp.path(), &fake);
    srv.serve().await.expect("serve");

    let replicas = srv.replicas();
    let incarnation = || replicas.get(0).map(|h| h.store.incarnation());
    assert!(
        wait_until(Duration::from_secs(15), || incarnation()
            == Some(ASSIGNED_INCARNATION))
        .await,
        "建副本必须采纳 master 发的身份(假 master 首发 {ASSIGNED_INCARNATION}),实际 {:?}",
        incarnation()
    );
    // 身份也落盘了(重启/重开读到的是同一个号,而不是又回落到本地兜底值)。
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("partition_0/LOG_INCARNATION")).unwrap(),
        ASSIGNED_INCARNATION.to_string(),
        "采纳的身份必须随副本落盘"
    );
}

/// **幂等心跳不重复申领**:同一份 `ACTIVE` 指令被重复下发时,第二次只是幂等空转,不得再领一个号
/// (否则 master 每重发一次指令、身份就换一次,别的节点手里的游标被反复作废,甚至判不出追平)。
///
/// 断言手法:两条 `ACTIVE` 之后再压一条闸门指令当**顺序标记** —— 等它落地即说明前两条都已处理完,
/// 此时读到的身份才是终值(否则可能在第二次申领生效前抢先断言,变成 flaky)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_active_directives_do_not_re_claim_the_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;
    fake.push_transition(0, "ACTIVE");
    fake.push_transition(0, "ACTIVE");
    fake.push_directive(gate_directive(0, true));

    let srv = node(tmp.path(), &fake);
    srv.serve().await.expect("serve");

    let replicas = srv.replicas();
    assert!(
        wait_until(Duration::from_secs(15), || replicas
            .get(0)
            .is_some_and(|h| h.tombstones_reclaim_allowed()))
        .await,
        "顺序标记(闸门指令)必须落地 —— 它说明两条 ACTIVE 都已处理完"
    );
    assert_eq!(
        replicas.get(0).unwrap().store.incarnation(),
        ASSIGNED_INCARNATION,
        "重复的 ACTIVE 不得再领号(第二次领到的会是 {})",
        ASSIGNED_INCARNATION + 1
    );
}

/// **OFFLINE 摘除后挂回不换身份**:`OFFLINE` 只摘内存句柄、盘上数据(含身份文件)保留,再 `ACTIVE`
/// 是"打开已有副本"而不是"从零建副本" —— 身份必须保持原号。
///
/// 若判据取"内存里有没有句柄"(旧做法),这次挂回会被当成从零建 → 领新号并覆盖盘上身份 → 对端手里
/// 绑着这份副本的游标被无故作废、白跑一轮全量重拉。断言:挂回后身份仍是第一个号,且盘上文件也是它。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_then_active_keeps_the_incarnation() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;
    fake.push_transition(0, "ACTIVE");
    fake.push_transition(0, "OFFLINE");
    fake.push_transition(0, "ACTIVE");
    fake.push_directive(gate_directive(0, true));

    let srv = node(tmp.path(), &fake);
    srv.serve().await.expect("serve");

    let replicas = srv.replicas();
    assert!(
        wait_until(Duration::from_secs(15), || replicas
            .get(0)
            .is_some_and(|h| h.tombstones_reclaim_allowed()))
        .await,
        "顺序标记(闸门指令)必须落地 —— 它说明 ACTIVE→OFFLINE→ACTIVE 都走完了"
    );
    let handle = replicas.get(0).expect("挂回后本地应有该副本");
    assert_eq!(
        handle.store.incarnation(),
        ASSIGNED_INCARNATION,
        "OFFLINE 挂回不得换身份(旧做法会领到 {})",
        ASSIGNED_INCARNATION + 1
    );
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("partition_0/LOG_INCARNATION")).unwrap(),
        ASSIGNED_INCARNATION.to_string(),
        "盘上的身份也必须还是第一个号"
    );
}

/// 闸门**关**:master 说该分区还有副本没追平(`tombstones_reclaim_allowed: false`)时,句柄上的
/// 闸门必须是 `false`。
///
/// 先把闸门人为置 `true`(模拟"master 之前放行过"):句柄初值本就是 `false`,不这么做的话"false
/// 被采纳"根本看不出来(断言会自证)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_gate_closes_when_master_says_not_allowed() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;
    fake.push_directive(gate_directive(0, false));

    let srv = node(tmp.path(), &fake);
    // 本地先有该副本(闸门记在"当时存在的副本"上),并先放行一次。
    let handle = srv.replicas().ensure_replica(0).expect("ensure");
    assert!(
        srv.replicas().set_tombstones_reclaim_allowed(0, true),
        "先人为放行,后面的 false 才有可证伪性"
    );
    srv.serve().await.expect("serve");

    assert!(
        wait_until(Duration::from_secs(15), || !handle
            .tombstones_reclaim_allowed())
        .await,
        "master 说还有副本没追平 → 闸门必须关回 false"
    );
}

/// 闸门**开**:master 放行后(`true`)句柄上的闸门变 `true`;没得到放行前一直是 `false`
/// (初值不放行 = "没证据就不回收",宁可不回收)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tombstone_gate_opens_when_master_allows() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;
    fake.push_directive(gate_directive(0, true));

    let srv = node(tmp.path(), &fake);
    let handle = srv.replicas().ensure_replica(0).expect("ensure");
    assert!(
        !handle.tombstones_reclaim_allowed(),
        "没得到 master 放行前,新副本的闸门必须是关的"
    );
    srv.serve().await.expect("serve");

    assert!(
        wait_until(Duration::from_secs(15), || handle
            .tombstones_reclaim_allowed())
        .await,
        "master 放行 → 闸门必须开"
    );
}

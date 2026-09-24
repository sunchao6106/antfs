//! 分区可写态下发集成测试:master 的 `MasterDirective.partition_states` 要到得了 replica 句柄。
//!
//! 为什么盯这条通路:定时 compaction 的候选集完全按这个状态挑(`crates/store/src/worker.rs`)——
//! 字段名/消费点任一处对不上,`SEALED` 分区会被照压、`RO` 分区又压不到,而单测各测一半看不出接缝。
//! 这里用假 master 把指令真发出去,store 侧走**真 gRPC 心跳 loop**,断言句柄上的状态。
//!
//! 等待一律有界条件轮询(CLAUDE.md §6),不用 sleep 当同步。

mod common;

use std::time::Duration;

use antfs_master::pb::{MasterDirective, PartitionStateInfo};
use antfs_store::{StoreServer, StoreServerConfig};
use common::{wait_until, FakeMaster};

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_states_from_master_reach_the_replica_handle() {
    let tmp = tempfile::tempdir().unwrap();
    let fake = FakeMaster::start(Vec::new()).await;

    // 第一条:让本地建出分区 0 的副本(状态只记在"已存在的副本"上)。
    fake.push_transition(0, "ACTIVE");
    // 第二条:下发可写态 —— pid 0 是本地有的(应写进句柄),pid 7 是本地没有的(应被忽略而非报错)。
    fake.push_directive(MasterDirective {
        version: 1,
        transitions: Vec::new(),
        tasks: Vec::new(),
        partition_states: vec![
            PartitionStateInfo {
                partition_id: 0,
                state: "RO".to_string(),
                tombstones_reclaim_allowed: false,
            },
            PartitionStateInfo {
                partition_id: 7,
                state: "SEALED".to_string(),
                tombstones_reclaim_allowed: false,
            },
        ],
        resend_full_report: false,
        delta_report_supported: false,
    });

    let registry = antfs_metric::Registry::new();
    let cfg = StoreServerConfig {
        node_id: 1,
        data_dirs: vec![tmp.path().to_string_lossy().into_owned().into()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 8 << 20,
        master_addr: fake.addr(),
        heartbeat_interval_secs: 1,
        // 本地维护 worker 关掉:本用例只关心状态下发通路。
        local_worker_enabled: false,
        ..Default::default()
    };
    let srv = StoreServer::start_with(cfg, store_config(), &registry).expect("start");
    srv.serve().await.expect("serve");

    let replicas = srv.replicas();
    assert!(
        wait_until(Duration::from_secs(10), || replicas
            .get(0)
            .map(|h| h.partition_state_now() == "RO")
            .unwrap_or(false))
        .await,
        "master 下发的 RO 状态必须落到 replica 句柄上"
    );
    // 本地没有 pid 7 的副本 → 指令被忽略(不建副本、不报错)。
    assert!(replicas.get(7).is_none(), "状态指令不得凭空建出副本");

    // 未收到指令的分区保持"未知"(单机语义:未知按可压处理)。
    srv.replicas().ensure_replica(9).unwrap();
    assert_eq!(srv.replicas().get(9).unwrap().partition_state_now(), "");
}

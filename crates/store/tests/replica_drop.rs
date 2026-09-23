//! A6 集成测试:master 经**真心跳流**下发 `DROPPED` → 数据节点删掉本地副本目录。
//!
//! 假 master 只替换 master 那一端(真 master 目前只下发 `ACTIVE`);store 侧走真 gRPC 心跳
//! loop + 真 `ReplicaManager` + 真 HTTP 数据面。

mod common;

use std::time::Duration;

use antfs_master::pb::{MasterDirective, Transition};
use antfs_store::{StoreServer, StoreServerConfig};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{peer_loc, wait_until, FakeMaster};
use tower::ServiceExt;

fn store_config() -> antfs_blob::StoreConfig {
    antfs_blob::StoreConfig {
        segment_size_bytes: 1 << 20,
        index_max_inmem_elements: 1000,
        io_error_count_to_trigger_shutdown: 0,
    }
}

fn dropped(pid: u32) -> Transition {
    Transition {
        partition_id: pid,
        disk_id: String::new(),
        target_state: "DROPPED".to_string(),
    }
}

/// 该节点 Prometheus 文本(用**它自己接入的那个** Registry,指标断言才有证明力)。
fn metrics_text(registry: &antfs_metric::Registry) -> String {
    antfs_metric::gather(registry).expect("gather")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_directive_deletes_replica_data_and_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("disk");
    std::fs::create_dir_all(&data_dir).unwrap();

    // 一条 directive 里放三次 DROPPED:pid 0 连发两次(批内幂等)+ 一个本地没有的 pid 42。
    let fake = FakeMaster::start(vec![peer_loc(1, "127.0.0.1:1")]).await;
    fake.push_directive(MasterDirective {
        version: 1,
        transitions: vec![dropped(0), dropped(0), dropped(42)],
        tasks: Vec::new(),
        partition_states: Vec::new(),
    });

    let registry = antfs_metric::Registry::new();
    // 本地维护 worker **保持默认开启**:它的周期 flush 会拿着(删除前的)句柄快照写盘,
    // 正是"删目录 vs 在飞持有者"那条竞态的真实来源(见 `ReplicaManager::drop_replica` 的等待逻辑)。
    let cfg = StoreServerConfig {
        node_id: 101,
        data_dirs: vec![data_dir.to_string_lossy().into_owned()],
        http_addr: "127.0.0.1:0".into(),
        partition_capacity_bytes: 4 << 20,
        master_addr: fake.addr(),
        heartbeat_interval_secs: 1,
        ..Default::default()
    };
    let ss = StoreServer::start_with(cfg, store_config(), &registry).expect("start");

    // 本地先有两个副本(真场景:上一轮 ACTIVE 指令建的)。
    ss.replicas().ensure_replica(0).expect("ensure 0");
    ss.replicas().ensure_replica(1).expect("ensure 1");
    let dir0 = data_dir.join("partition_0");
    let dir1 = data_dir.join("partition_1");
    assert!(dir0.is_dir() && dir1.is_dir(), "副本必须先落盘出目录");

    ss.serve().await.expect("serve"); // 注册 + 心跳 loop(HTTP 绑 0 端口即可)

    // ---- DROPPED 送达:pid 0 的数据必须消失 ----
    assert!(
        wait_until(Duration::from_secs(15), || !dir0.exists()).await,
        "DROPPED 后 pid 0 的副本目录必须被删掉;beats={} metrics:\n{}",
        fake.beat_count(),
        metrics_text(&registry)
    );
    assert!(ss.replicas().get(0).is_none(), "句柄必须摘除");
    assert_eq!(ss.replicas().count(), 1, "只该删被指令点到的那个分区");
    assert!(dir1.is_dir(), "未被指令点到的分区数据不得被动");

    // 数据面:该分区已无副本 → 404(旧副本彻底下线)。
    let resp = ss
        .router()
        .oneshot(
            Request::get(format!("/partitions/0/blobs/{}", "01".repeat(8)))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // ---- 幂等:同一批里重复的 DROPPED 不得重复计数 ----
    // 等到节点再发一轮心跳(≈1 个心跳周期)后再断言:此时上面那条 directive 的整轮处理
    // (含两条重复 DROPPED)必已完成,断言是确定的而非"抢在第二次调用之前"。
    let beats = fake.beat_count();
    assert!(
        wait_until(Duration::from_secs(15), || fake.beat_count() > beats).await,
        "心跳必须继续"
    );
    let text = metrics_text(&registry);
    assert!(
        text.contains("store_server_replicas_dropped_total 1"),
        "只该计 1 次成功删除(重复 DROPPED 被幂等忽略), got:\n{text}"
    );
    assert!(
        text.contains("store_server_replicas 1"),
        "副本数 gauge 必须跟着降, got:\n{text}"
    );
    assert!(
        text.contains("store_server_heartbeats_total 1"),
        "心跳仍在跑(指令处理未把心跳 loop 卡死), got:\n{text}"
    );
}

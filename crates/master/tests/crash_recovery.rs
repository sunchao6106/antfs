//! master 崩溃恢复的属性测试(CLAUDE.md §3.1 / §6:"事务提交是持久化边界")。
//!
//! 断言:把一串命令经状态机 `apply`(每次 apply = 一个 redb 提交事务)后,**关闭并重开** redb,
//! 恢复出的 `applied_state` 与状态机内容必须与关闭前一致 —— 已提交命令不丢、不重复。

use std::collections::BTreeMap;

use antfs_master::store::open;
use antfs_master::types::{MasterCommand, NodeInfo, TypeConfig};
use antfs_master::MasterMetrics;
use openraft::storage::RaftStateMachine;
use openraft::testing::log_id;
use openraft::{Entry, EntryPayload};
use proptest::prelude::*;

fn normal_ent(index: u64, cmd: MasterCommand) -> Entry<TypeConfig> {
    Entry { log_id: log_id(1, 0, index), payload: EntryPayload::Normal(cmd) }
}

fn arb_cmd() -> impl Strategy<Value = MasterCommand> {
    prop_oneof![
        Just(MasterCommand::AllocatePartitionId),
        (0u32..4, 1u32..1000)
            .prop_map(|(partition_id, count)| MasterCommand::AllocateObjectId { partition_id, count }),
        (0u64..8, 1u64..1000).prop_map(|(node_id, cap)| MasterCommand::RegisterNode {
            node_id,
            info: NodeInfo {
                dc: "dc".into(),
                rack: "r".into(),
                addr: format!("127.0.0.1:{}", 7000 + node_id),
                disks: BTreeMap::from([(
                    "d0".to_string(),
                    antfs_master::types::DiskSpec { capacity_bytes: cap },
                )]),
                registered_at_ms: 42,
            },
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn apply_then_reopen_recovers_committed_state(cmds in prop::collection::vec(arb_cmd(), 0..40)) {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("master.redb");

            let (applied_before, nodes_before, next_pid_before, object_next_before) = {
                let (_ls, mut sm) = open(&path, MasterMetrics::disabled()).unwrap();
                let entries: Vec<_> = cmds
                    .iter()
                    .enumerate()
                    .map(|(i, c)| normal_ent(i as u64 + 1, c.clone()))
                    .collect();
                if !entries.is_empty() {
                    sm.apply(entries).await.unwrap();
                }
                let (applied, _mem) = sm.applied_state().await.unwrap();
                let data = sm.shared_data();
                let g = data.read().unwrap();
                (applied, g.state.nodes.clone(), g.state.next_partition_id, g.state.object_next.clone())
            };

            // 重开:模拟进程重启后从 redb 恢复。
            let (_ls2, mut sm2) = open(&path, MasterMetrics::disabled()).unwrap();
            let (applied_after, _mem2) = sm2.applied_state().await.unwrap();
            let data2 = sm2.shared_data();
            let g2 = data2.read().unwrap();

            prop_assert_eq!(applied_after, applied_before, "last_applied must survive reopen");
            prop_assert_eq!(&g2.state.nodes, &nodes_before, "node registry must survive reopen");
            prop_assert_eq!(g2.state.next_partition_id, next_pid_before, "id allocator must survive reopen");
            prop_assert_eq!(&g2.state.object_next, &object_next_before, "objectId watermark must survive reopen");
            Ok(())
        })?;
    }
}

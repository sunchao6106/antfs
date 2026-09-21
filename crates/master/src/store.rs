//! `redb` 后端的 Raft 存储(Q3):[`RaftLogStorage`] + [`RaftStateMachine`]。
//!
//! 设计见 `openspec/changes/master-node/design.md` §4。**持久性铁律**(CLAUDE.md §3.1):
//! `apply` 把状态机变更与 `last_applied` 写进**同一个 redb 写事务**并 `commit`,该 commit 即持久化
//! 边界;崩溃后从 redb 恢复到最后一次成功提交的 applied 状态,已提交命令不丢、不重复应用。
//!
//! log 与状态机共享一个 [`redb::Database`](单一 fsync 域);二者各由 openraft 的不同 worker 持有,
//! redb 支持并发事务,故用 `Arc<Database>` 共享安全。

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use redb::{Database, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::metrics::MasterMetrics;
use crate::types::{MasterResponse, MasterState, NodeId, TypeConfig};

/// log 表:log index → 序列化的 [`Entry`]。
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
/// 元数据表:`"vote"` / `"committed"` / `"last_purged"` → 序列化值。
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");
/// 状态机表:`"data"`(状态机全量)/ `"snapshot"`(当前快照)。
const SM: TableDefinition<&str, &[u8]> = TableDefinition::new("state_machine");

const META_VOTE: &str = "vote";
const META_COMMITTED: &str = "committed";
const META_LAST_PURGED: &str = "last_purged";
const SM_DATA: &str = "data";
const SM_SNAPSHOT: &str = "snapshot";

fn ser<T: Serialize>(v: &T) -> Result<Vec<u8>, AnyError> {
    serde_json::to_vec(v).map_err(|e| AnyError::new(&e))
}

fn de<T: DeserializeOwned>(b: &[u8]) -> Result<T, AnyError> {
    serde_json::from_slice(b).map_err(|e| AnyError::new(&e))
}

fn to_err<E: std::error::Error + 'static>(e: E) -> AnyError {
    AnyError::new(&e)
}

// openraft 的便捷构造器在 `StorageIOError` 上;`StorageError` 经 `#[from]` 承接。下列 helper 直接产出
// `StorageError<NodeId>`,消掉每处 `.into()`。参数取 `impl Into<AnyError>`,兼容 `to_err()` 与 `de()`。
use openraft::StorageIOError;
use openraft::storage::SnapshotSignature;

fn read_logs(src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::read_logs(src).into()
}
fn write_logs(src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::write_logs(src).into()
}
fn read_vote(src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::read_vote(src).into()
}
fn write_vote(src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::write_vote(src).into()
}
fn write_state_machine(src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::write_state_machine(src).into()
}
fn read_snapshot(sig: Option<SnapshotSignature<NodeId>>, src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::read_snapshot(sig, src).into()
}
fn write_snapshot(sig: Option<SnapshotSignature<NodeId>>, src: impl Into<AnyError>) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(sig, src).into()
}

/// 状态机的全部内存态,同时是快照/持久化的序列化单元。
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StateMachineData {
    pub last_applied: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    pub state: MasterState,
}

/// 持久化 / 传输的快照。
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    pub data: Vec<u8>,
}

/// redb 后端的 log 存储;自身即 `LogReader`(`Clone`,共享 `Arc<Database>`)。
#[derive(Clone)]
pub struct RedbLogStore {
    db: Arc<Database>,
}

/// redb 后端的状态机 + 快照。
#[derive(Clone)]
pub struct RedbStateMachine {
    db: Arc<Database>,
    /// 内存态,gRPC 只读查询(ListNodes 等)也读它 —— 故用 `Arc<RwLock>` 与 server 共享。
    data: Arc<RwLock<StateMachineData>>,
    /// 快照 id 单调计数(保证同一 last_log_id 下快照 id 仍唯一)。
    snapshot_seq: Arc<AtomicU64>,
    metrics: MasterMetrics,
}

/// 打开(或创建)master 的 redb 存储,返回 log 存储与状态机(共享同一 DB)。
///
/// 启动时预建所有表(避免只读事务遇到"表不存在"),并从 redb 恢复状态机内存态。
// result_large_err:错误类型是外部 `redb::Error`,尺寸不由我们控制;控制面启动路径 box 无实益。
#[allow(clippy::result_large_err)]
pub fn open(
    path: &std::path::Path,
    metrics: MasterMetrics,
) -> Result<(RedbLogStore, RedbStateMachine), redb::Error> {
    let db = Arc::new(Database::create(path)?);
    {
        let w = db.begin_write()?;
        w.open_table(LOGS)?;
        w.open_table(META)?;
        w.open_table(SM)?;
        w.commit()?;
    }

    let data = {
        let r = db.begin_read()?;
        let t = r.open_table(SM)?;
        match t.get(SM_DATA)? {
            Some(g) => de(g.value()).map_err(|e| redb::Error::Corrupted(e.to_string()))?,
            None => StateMachineData::default(),
        }
    };
    metrics
        .registered_nodes
        .set(data.state.nodes.len() as i64);

    let sm = RedbStateMachine {
        db: db.clone(),
        data: Arc::new(RwLock::new(data)),
        snapshot_seq: Arc::new(AtomicU64::new(0)),
        metrics,
    };
    Ok((RedbLogStore { db }, sm))
}

impl RedbStateMachine {
    /// 共享的内存状态机句柄,供 gRPC server 只读查询(ListNodes / GetClusterState)。
    pub fn shared_data(&self) -> Arc<RwLock<StateMachineData>> {
        self.data.clone()
    }

    // result_large_err:错误是 openraft 契约的 `StorageError`,尺寸由外部固定,box 会破坏 trait 签名一致性。
    #[allow(clippy::result_large_err)]
    fn persist_data(&self, data: &StateMachineData) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(data).map_err(write_state_machine)?;
        let w = self.db.begin_write().map_err(|e| write_state_machine(to_err(e)))?;
        {
            let mut t = w.open_table(SM).map_err(|e| write_state_machine(to_err(e)))?;
            t.insert(SM_DATA, bytes.as_slice())
                .map_err(|e| write_state_machine(to_err(e)))?;
        }
        w.commit().map_err(|e| write_state_machine(to_err(e)))?;
        Ok(())
    }
}

impl openraft::RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let r = self.db.begin_read().map_err(|e| read_logs(to_err(e)))?;
        let t = r.open_table(LOGS).map_err(|e| read_logs(to_err(e)))?;
        let mut out = Vec::new();
        for item in t.range(range).map_err(|e| read_logs(to_err(e)))? {
            let (_k, v) = item.map_err(|e| read_logs(to_err(e)))?;
            let entry: Entry<TypeConfig> = de(v.value()).map_err(read_logs)?;
            out.push(entry);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let r = self.db.begin_read().map_err(|e| read_logs(to_err(e)))?;
        let last_purged: Option<LogId<NodeId>> = {
            let t = r.open_table(META).map_err(|e| read_logs(to_err(e)))?;
            match t.get(META_LAST_PURGED).map_err(|e| read_logs(to_err(e)))? {
                Some(g) => Some(de(g.value()).map_err(read_logs)?),
                None => None,
            }
        };
        let last_log: Option<LogId<NodeId>> = {
            let t = r.open_table(LOGS).map_err(|e| read_logs(to_err(e)))?;
            let last = t.last().map_err(|e| read_logs(to_err(e)))?;
            match last {
                Some((_k, v)) => {
                    let entry: Entry<TypeConfig> = de(v.value()).map_err(read_logs)?;
                    Some(entry.log_id)
                }
                None => None,
            }
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(vote).map_err(write_vote)?;
        let w = self.db.begin_write().map_err(|e| write_vote(to_err(e)))?;
        {
            let mut t = w.open_table(META).map_err(|e| write_vote(to_err(e)))?;
            t.insert(META_VOTE, bytes.as_slice())
                .map_err(|e| write_vote(to_err(e)))?;
        }
        w.commit().map_err(|e| write_vote(to_err(e)))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let r = self.db.begin_read().map_err(|e| read_vote(to_err(e)))?;
        let t = r.open_table(META).map_err(|e| read_vote(to_err(e)))?;
        match t.get(META_VOTE).map_err(|e| read_vote(to_err(e)))? {
            Some(g) => Ok(Some(de(g.value()).map_err(read_vote)?)),
            None => Ok(None),
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(&committed).map_err(write_logs)?;
        let w = self.db.begin_write().map_err(|e| write_logs(to_err(e)))?;
        {
            let mut t = w.open_table(META).map_err(|e| write_logs(to_err(e)))?;
            t.insert(META_COMMITTED, bytes.as_slice())
                .map_err(|e| write_logs(to_err(e)))?;
        }
        w.commit().map_err(|e| write_logs(to_err(e)))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let r = self.db.begin_read().map_err(|e| read_logs(to_err(e)))?;
        let t = r.open_table(META).map_err(|e| read_logs(to_err(e)))?;
        match t.get(META_COMMITTED).map_err(|e| read_logs(to_err(e)))? {
            Some(g) => Ok(de(g.value()).map_err(read_logs)?),
            None => Ok(None),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let w = self.db.begin_write().map_err(|e| write_logs(to_err(e)))?;
        {
            let mut t = w.open_table(LOGS).map_err(|e| write_logs(to_err(e)))?;
            for entry in entries {
                let bytes = ser(&entry).map_err(write_logs)?;
                t.insert(entry.log_id.index, bytes.as_slice())
                    .map_err(|e| write_logs(to_err(e)))?;
            }
        }
        // commit = 持久化边界(redb 事务 fsync)。commit 成功后才回调 log_io_completed。
        w.commit().map_err(|e| write_logs(to_err(e)))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let w = self.db.begin_write().map_err(|e| write_logs(to_err(e)))?;
        {
            let mut t = w.open_table(LOGS).map_err(|e| write_logs(to_err(e)))?;
            let keys: Vec<u64> = t
                .range(log_id.index..)
                .map_err(|e| write_logs(to_err(e)))?
                .map(|it| it.map(|(k, _v)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| write_logs(to_err(e)))?;
            for k in keys {
                t.remove(k).map_err(|e| write_logs(to_err(e)))?;
            }
        }
        w.commit().map_err(|e| write_logs(to_err(e)))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let bytes = ser(&log_id).map_err(write_logs)?;
        let w = self.db.begin_write().map_err(|e| write_logs(to_err(e)))?;
        {
            let mut t = w.open_table(LOGS).map_err(|e| write_logs(to_err(e)))?;
            let keys: Vec<u64> = t
                .range(..=log_id.index)
                .map_err(|e| write_logs(to_err(e)))?
                .map(|it| it.map(|(k, _v)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| write_logs(to_err(e)))?;
            for k in keys {
                t.remove(k).map_err(|e| write_logs(to_err(e)))?;
            }
        }
        {
            let mut m = w.open_table(META).map_err(|e| write_logs(to_err(e)))?;
            m.insert(META_LAST_PURGED, bytes.as_slice())
                .map_err(|e| write_logs(to_err(e)))?;
        }
        w.commit().map_err(|e| write_logs(to_err(e)))?;
        Ok(())
    }
}

/// 快照构建器:持有构建时刻的状态机视图副本(§getting_started 两步式)。
pub struct RedbSnapshotBuilder {
    db: Arc<Database>,
    snapshot_seq: Arc<AtomicU64>,
    data: StateMachineData,
}

impl RaftSnapshotBuilder<TypeConfig> for RedbSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data_bytes = ser(&self.data).map_err(|e| write_snapshot(None, e))?;
        let seq = self.snapshot_seq.fetch_add(1, Ordering::SeqCst);
        let last = self.data.last_applied;
        let snapshot_id = match last {
            Some(l) => format!("{}-{}-{}", l.leader_id, l.index, seq),
            None => format!("empty-{seq}"),
        };
        let meta = SnapshotMeta {
            last_log_id: last,
            last_membership: self.data.last_membership.clone(),
            snapshot_id,
        };
        let stored = StoredSnapshot { meta: meta.clone(), data: data_bytes.clone() };
        let stored_bytes = ser(&stored).map_err(|e| write_snapshot(None, e))?;
        let w = self
            .db
            .begin_write()
            .map_err(|e| write_snapshot(None, to_err(e)))?;
        {
            let mut t = w
                .open_table(SM)
                .map_err(|e| write_snapshot(None, to_err(e)))?;
            t.insert(SM_SNAPSHOT, stored_bytes.as_slice())
                .map_err(|e| write_snapshot(None, to_err(e)))?;
        }
        w.commit().map_err(|e| write_snapshot(None, to_err(e)))?;
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data_bytes)) })
    }
}

impl RaftStateMachine<TypeConfig> for RedbStateMachine {
    type SnapshotBuilder = RedbSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>>
    {
        let d = self.data.read().expect("state machine lock poisoned");
        Ok((d.last_applied, d.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MasterResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let snapshot = {
            let mut d = self.data.write().expect("state machine lock poisoned");
            for entry in entries {
                d.last_applied = Some(entry.log_id);
                let resp = match entry.payload {
                    EntryPayload::Blank => MasterResponse::Empty,
                    EntryPayload::Normal(ref cmd) => {
                        let r = d.state.apply_command(cmd);
                        self.metrics.applied_commands.inc();
                        r
                    }
                    EntryPayload::Membership(ref mem) => {
                        d.last_membership =
                            StoredMembership::new(Some(entry.log_id), mem.clone());
                        MasterResponse::Empty
                    }
                };
                responses.push(resp);
            }
            self.metrics.registered_nodes.set(d.state.nodes.len() as i64);
            d.clone()
        };
        // apply 与 last_applied 在同一 redb 事务提交 = 持久化边界(§design 2)。
        self.persist_data(&snapshot)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let data = self.data.read().expect("state machine lock poisoned").clone();
        RedbSnapshotBuilder {
            db: self.db.clone(),
            snapshot_seq: self.snapshot_seq.clone(),
            data,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let data: StateMachineData = de(&bytes)
            .map_err(|e| read_snapshot(Some(meta.signature()), e))?;
        {
            let mut d = self.data.write().expect("state machine lock poisoned");
            *d = data.clone();
            self.metrics.registered_nodes.set(d.state.nodes.len() as i64);
        }
        self.persist_data(&data)?;
        let stored = StoredSnapshot { meta: meta.clone(), data: bytes };
        let stored_bytes = ser(&stored).map_err(|e| write_snapshot(Some(meta.signature()), e))?;
        let w = self
            .db
            .begin_write()
            .map_err(|e| write_snapshot(Some(meta.signature()), to_err(e)))?;
        {
            let mut t = w
                .open_table(SM)
                .map_err(|e| write_snapshot(Some(meta.signature()), to_err(e)))?;
            t.insert(SM_SNAPSHOT, stored_bytes.as_slice())
                .map_err(|e| write_snapshot(Some(meta.signature()), to_err(e)))?;
        }
        w.commit()
            .map_err(|e| write_snapshot(Some(meta.signature()), to_err(e)))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let r = self.db.begin_read().map_err(|e| read_snapshot(None, to_err(e)))?;
        let t = r.open_table(SM).map_err(|e| read_snapshot(None, to_err(e)))?;
        match t.get(SM_SNAPSHOT).map_err(|e| read_snapshot(None, to_err(e)))? {
            Some(g) => {
                let stored: StoredSnapshot =
                    de(g.value()).map_err(|e| read_snapshot(None, e))?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod conformance {
    use super::*;
    use openraft::testing::StoreBuilder;
    use tempfile::TempDir;

    struct RedbBuilder;

    impl StoreBuilder<TypeConfig, RedbLogStore, RedbStateMachine, TempDir> for RedbBuilder {
        async fn build(
            &self,
        ) -> Result<(TempDir, RedbLogStore, RedbStateMachine), StorageError<NodeId>> {
            let dir = tempfile::tempdir().expect("create tempdir");
            let path = dir.path().join("master.redb");
            let (ls, sm) = open(&path, MasterMetrics::disabled())
                .map_err(|e| StorageIOError::read(to_err(e)))?;
            Ok((dir, ls, sm))
        }
    }

    #[test]
    // result_large_err:返回 openraft `StorageError` 是 Suite 契约,测试函数无需 box。
    #[allow(clippy::result_large_err)]
    fn openraft_storage_conformance() -> Result<(), StorageError<NodeId>> {
        openraft::testing::Suite::test_all(RedbBuilder)
    }
}

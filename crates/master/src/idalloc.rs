//! master 侧 objectId 发号器:leader 内存**号段池** + 必要时向 Raft 抬水位。
//!
//! 决策见 `openspec/changes/blobid-master-assign` D1–D4/D9。两层批处理:
//!
//! 1. **master → client**:一次发一整段 `[start, start+N)`,client 本地消费,热路径零 RPC;
//! 2. **master → Raft**:leader 内存池快耗尽时才 `client_write(AllocateObjectId)` 一次预留
//!    `B = max(请求数, N) × raft_batch_multiple`,再切成 N 大小的段发给 client。
//!
//! **正确性骨架**(守 §3.1 持久性红线):
//! - 池的 `end` 恒 ≤ **已提交水位** → 崩溃/切主后新 leader 池为空、从已提交水位续发,**绝不重叠**;
//! - 水位与实际发出之间的未用号 = **空洞**;client 崩溃丢缓存也成空洞。递增语义容忍跳号,
//!   **任何逻辑不得假设"发了号 = 对象存在"**;
//! - 号段池为空时锁跨 `await` 持有:号段边界单飞,避免并发 client 各自向 Raft 要一段(放大提交量)。

use std::collections::HashMap;
use std::sync::Arc;

use openraft::Raft;
use tokio::sync::Mutex;

use crate::config::MasterConfig;
use crate::metrics::MasterMetrics;
use crate::types::{MasterCommand, MasterResponse, ObjectId, PartitionId, TypeConfig};

/// 一个 partition 的号段池:`[start, end)` 是已提交水位之内、尚未发出的号;`next` 是下一个待发号。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Pool {
    start: ObjectId,
    end: ObjectId,
    next: ObjectId,
}

impl Pool {
    /// 池内还能发出多少个号。
    fn remaining(&self) -> u32 {
        self.end.saturating_sub(self.next)
    }

    /// 从池里取 `count` 个号并返回起点。调用方须保证 `remaining() >= count`。
    fn take(&mut self, count: u32) -> ObjectId {
        let start = self.next;
        self.next = self.next.saturating_add(count);
        start
    }
}

/// 发号失败原因。调用方(`GetBlobIds`)据此填 `ok=false` 与 `error`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdAllocError {
    /// 该 partition 的 u32 objectId 空间已耗尽:应封存该分区,**绝不复用**(D3)。
    Exhausted,
    /// 经 Raft 抬水位失败(非 leader / 提交失败)。
    Raft(String),
    /// 状态机返回了预期之外的结果(调用方与状态机不一致,属 bug)。
    Unexpected(String),
}

impl std::fmt::Display for IdAllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdAllocError::Exhausted => {
                write!(f, "objectId space exhausted for this partition; seal it")
            }
            IdAllocError::Raft(e) => write!(f, "raft commit failed: {e}"),
            IdAllocError::Unexpected(e) => write!(f, "unexpected apply response: {e}"),
        }
    }
}

/// leader 侧发号器(可克隆,内部共享号段池)。仅在 leader 上有效:非 leader 调用 `raft.client_write` 会失败。
#[derive(Clone, Default)]
pub struct IdAllocator {
    pools: Arc<Mutex<HashMap<PartitionId, Pool>>>,
}

impl IdAllocator {
    /// 一次水位预留的号数:`max(请求数, 号段大小) × 倍数`(饱和运算,不回绕、不 panic)。
    ///
    /// 取 `max(请求数, N)` 保证预留量 ≥ 本次请求量,一次抬水位即可满足该次发号。
    fn batch_size(requested: u32, cfg: &MasterConfig) -> u32 {
        requested
            .max(cfg.id_alloc_segment_size)
            .saturating_mul(cfg.id_alloc_raft_batch_multiple)
    }

    /// 为 `partition_id` 发 `count` 个**连续** objectId,返回 `(起点, 个数)`。池不够时先抬水位(经 Raft)。
    pub async fn allocate(
        &self,
        raft: &Raft<TypeConfig>,
        partition_id: PartitionId,
        count: u32,
        cfg: &MasterConfig,
        metrics: &MasterMetrics,
    ) -> Result<(ObjectId, u32), IdAllocError> {
        if count == 0 {
            return Err(IdAllocError::Unexpected("count must be > 0".into()));
        }
        let mut pools = self.pools.lock().await;
        let pool = pools.entry(partition_id).or_default();
        if pool.remaining() < count {
            let (start, n) = Self::reserve(raft, partition_id, count, cfg, metrics).await?;
            // 换池:旧池未发完的号成为**空洞**(D3 无害),计数仅供观测。
            let holes = pool.remaining();
            if holes > 0 {
                metrics.id_alloc_holes.inc_by(holes as u64);
            }
            *pool = Pool {
                start,
                end: start.saturating_add(n),
                next: start,
            };
        }
        Ok((pool.take(count), count))
    }

    /// 向 Raft 提交一次水位预留(两层批处理第 2 层),返回 `(起点, 个数)`。
    ///
    /// 顶端不足一整批时(`ObjectIdExhausted`)退回**按本次请求量**再申请一次:宁可少发,也不回绕复用。
    async fn reserve(
        raft: &Raft<TypeConfig>,
        partition_id: PartitionId,
        count: u32,
        cfg: &MasterConfig,
        metrics: &MasterMetrics,
    ) -> Result<(ObjectId, u32), IdAllocError> {
        let batch = Self::batch_size(count, cfg);
        metrics.id_alloc_raft_commits.inc();
        match Self::commit(raft, partition_id, batch).await? {
            MasterResponse::ObjectIdRange {
                start, count: n, ..
            } => Ok((start, n)),
            MasterResponse::ObjectIdExhausted { .. } if batch > count => {
                metrics.id_alloc_raft_commits.inc();
                match Self::commit(raft, partition_id, count).await? {
                    MasterResponse::ObjectIdRange {
                        start, count: n, ..
                    } => Ok((start, n)),
                    MasterResponse::ObjectIdExhausted { .. } => Err(IdAllocError::Exhausted),
                    other => Err(IdAllocError::Unexpected(format!("{other:?}"))),
                }
            }
            MasterResponse::ObjectIdExhausted { .. } => Err(IdAllocError::Exhausted),
            other => Err(IdAllocError::Unexpected(format!("{other:?}"))),
        }
    }

    async fn commit(
        raft: &Raft<TypeConfig>,
        partition_id: PartitionId,
        count: u32,
    ) -> Result<MasterResponse, IdAllocError> {
        raft.client_write(MasterCommand::AllocateObjectId {
            partition_id,
            count,
        })
        .await
        .map(|r| r.data)
        .map_err(|e| IdAllocError::Raft(format!("{e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(segment_size: u32, multiple: u32) -> MasterConfig {
        MasterConfig {
            id_alloc_segment_size: segment_size,
            id_alloc_raft_batch_multiple: multiple,
            ..Default::default()
        }
    }

    #[test]
    fn pool_take_advances_and_reports_remaining() {
        let mut p = Pool {
            start: 100,
            end: 110,
            next: 100,
        };
        assert_eq!(p.remaining(), 10);
        assert_eq!(p.take(4), 100);
        assert_eq!(p.remaining(), 6);
        assert_eq!(p.take(6), 104);
        assert_eq!(p.remaining(), 0);
        assert_eq!(p.next, 110);
    }

    #[test]
    fn empty_pool_has_no_remaining() {
        let p = Pool::default();
        assert_eq!(
            p.remaining(),
            0,
            "fresh partition pool starts empty → forces a raft reservation"
        );
    }

    #[test]
    fn batch_size_is_max_of_request_and_segment_times_multiple() {
        // 请求小于号段 → 按号段算。
        assert_eq!(IdAllocator::batch_size(4, &cfg(1024, 2)), 2048);
        // 请求大于号段 → 按请求算(保证一次抬水位够本次发)。
        assert_eq!(IdAllocator::batch_size(5000, &cfg(1024, 2)), 10_000);
        // 倍数为 1 → 正好一段。
        assert_eq!(IdAllocator::batch_size(1, &cfg(64, 1)), 64);
    }

    #[test]
    fn batch_size_saturates_instead_of_wrapping() {
        // 饱和到 u32::MAX,不回绕(回绕会让水位倒退 → 复用号,踩持久性红线)。
        assert_eq!(
            IdAllocator::batch_size(u32::MAX, &cfg(u32::MAX, 2)),
            u32::MAX
        );
        assert_eq!(
            IdAllocator::batch_size(u32::MAX, &cfg(u32::MAX, u32::MAX)),
            u32::MAX
        );
    }

    #[test]
    fn alloc_error_messages_are_actionable() {
        assert!(IdAllocError::Exhausted.to_string().contains("seal"));
        assert!(IdAllocError::Raft("x".into()).to_string().contains('x'));
        assert!(IdAllocError::Unexpected("y".into())
            .to_string()
            .contains('y'));
    }
}

//! A3:手动 compaction 的**异步任务台账**(内存态)。
//!
//! `POST /admin/partitions/{pid}/compact?async=1` 立即返回 `job_id`,后台在阻塞池里跑
//! `BlobStore::compact()`,完成后把结果写回本表。表是**进程内内存态**:重启即丢,只保留最近
//! [`MAX_TRACKED_JOBS`] 条(超出淘汰最旧)—— 台账只服务"这次 compaction 跑完没/结果如何"的
//! 运维问询,不承担持久化职责(compaction 本身是幂等的重构动作,崩溃由引擎侧 `COMPACT_COMMIT`
//! 收敛,与台账无关)。

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::PartitionId;

/// 台账保留的最近任务条数(超出逐条淘汰最旧)。
pub const MAX_TRACKED_JOBS: usize = 32;

/// 任务状态(JSON 里为小写字符串:`running` / `done` / `failed`;也是指标 label 的来源)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Running,
    Done,
    Failed,
}

impl JobState {
    /// 对外名称(JSON 字段、指标 label 的**唯一真值**)。
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
        }
    }
}

/// compact 结果(与同步路径 `POST /admin/partitions/{pid}/compact` 的响应体**同字段**,
/// 两处口径一致;`antfs_blob::CompactionStats` 在 blob crate,不依赖 serde,故在此定义镜像)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CompactionResult {
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub segments_before: usize,
    pub segments_after: usize,
}

impl From<antfs_blob::CompactionStats> for CompactionResult {
    fn from(s: antfs_blob::CompactionStats) -> Self {
        CompactionResult {
            live_bytes: s.live_bytes,
            dead_bytes: s.dead_bytes,
            segments_before: s.segments_before,
            segments_after: s.segments_after,
        }
    }
}

/// 一条 compaction 任务(`GET /admin/compactions[/{job_id}]` 的响应元素)。
///
/// 未完成字段**不出现在 JSON 里**(`skip_serializing_if`),故运行中的任务没有
/// `finished_at_ms`/`result`/`error` 三个 key。
#[derive(Debug, Clone, Serialize)]
pub struct CompactionJob {
    pub job_id: u64,
    pub partition_id: PartitionId,
    pub state: JobState,
    /// 提交时刻(Unix 毫秒)。
    pub started_at_ms: u64,
    /// 结束时刻(Unix 毫秒);仅 `done`/`failed` 有。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// compact 统计;仅 `done` 有。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<CompactionResult>,
    /// 失败原因;仅 `failed` 有。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 台账内容(一个锁护住 `next_id` + 插入序 + 条目,保证三者始终一致)。
#[derive(Default)]
struct Inner {
    next_id: u64,
    /// 插入序的 `job_id`(队首 = 最旧;`job_id` 单调自增,故与提交序一致)。
    order: VecDeque<u64>,
    jobs: HashMap<u64, CompactionJob>,
}

/// 进程内 compaction 任务台账。
#[derive(Default)]
pub struct CompactionJobs {
    inner: RwLock<Inner>,
}

impl CompactionJobs {
    pub fn new() -> Self {
        CompactionJobs::default()
    }

    /// 登记一个 `running` 任务;返回其 `job_id`(单调自增,从 1 起)。
    ///
    /// 超过 [`MAX_TRACKED_JOBS`] 时淘汰最旧的一条:**只丢台账记录**——被淘汰的 `running` 任务
    /// 之后仍会照常跑完,其终态只是写不回表(且不再计 `done`/`failed` 的标签之外的任何东西)。
    pub fn submit(&self, pid: PartitionId) -> u64 {
        let mut g = self.inner.write().expect("compaction jobs lock poisoned");
        g.next_id += 1;
        let job_id = g.next_id;
        g.jobs.insert(
            job_id,
            CompactionJob {
                job_id,
                partition_id: pid,
                state: JobState::Running,
                started_at_ms: now_ms(),
                finished_at_ms: None,
                result: None,
                error: None,
            },
        );
        g.order.push_back(job_id);
        while g.order.len() > MAX_TRACKED_JOBS {
            if let Some(old) = g.order.pop_front() {
                g.jobs.remove(&old);
            }
        }
        job_id
    }

    /// 回写任务终态(`Ok` = 结果 → `done`;`Err` = 原因 → `failed`)。
    ///
    /// 任务已被淘汰(表满)→ 静默忽略(台账尽力而为,不改变 compaction 本身的结果)。
    pub fn finish(&self, job_id: u64, outcome: Result<antfs_blob::CompactionStats, String>) {
        let mut g = self.inner.write().expect("compaction jobs lock poisoned");
        let Some(job) = g.jobs.get_mut(&job_id) else {
            return;
        };
        job.finished_at_ms = Some(now_ms());
        match outcome {
            Ok(stats) => {
                job.state = JobState::Done;
                job.result = Some(CompactionResult::from(stats));
            }
            Err(e) => {
                job.state = JobState::Failed;
                job.error = Some(e);
            }
        }
    }

    /// 单条任务;`None` = 未知 `job_id`(端点回 404)或已被淘汰。
    pub fn get(&self, job_id: u64) -> Option<CompactionJob> {
        self.inner
            .read()
            .expect("compaction jobs lock poisoned")
            .jobs
            .get(&job_id)
            .cloned()
    }

    /// 全表快照,按 `job_id` **降序**(最新在前;运维看板先看最近几次)。
    pub fn snapshot(&self) -> Vec<CompactionJob> {
        let g = self.inner.read().expect("compaction jobs lock poisoned");
        let mut out: Vec<CompactionJob> = g.jobs.values().cloned().collect();
        out.sort_by_key(|j| std::cmp::Reverse(j.job_id));
        out
    }
}

/// 当前时刻(Unix 毫秒;系统钟早于 epoch 时报 0,不 panic)。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(live: u64, dead: u64) -> antfs_blob::CompactionStats {
        antfs_blob::CompactionStats {
            live_bytes: live,
            dead_bytes: dead,
            segments_before: 3,
            segments_after: 1,
        }
    }

    #[test]
    fn job_ids_are_monotonic_and_get_round_trips() {
        let jobs = CompactionJobs::new();
        let a = jobs.submit(7);
        let b = jobs.submit(8);
        assert!(b > a, "job_id 必须单调递增");
        assert_eq!(jobs.get(a).unwrap().partition_id, 7);
        assert!(jobs.get(u64::MAX).is_none(), "未知 job_id → None(端点 404)");
    }

    #[test]
    fn finish_records_result_or_error_and_keeps_running_until_then() {
        let jobs = CompactionJobs::new();
        let ok = jobs.submit(1);
        let bad = jobs.submit(2);

        // 未完成:状态 running,且三个结果字段都不在(JSON 里应缺 key)。
        let running = serde_json::to_value(jobs.get(ok).unwrap()).unwrap();
        assert_eq!(running["state"], "running");
        for absent in ["finished_at_ms", "result", "error"] {
            assert!(running.get(absent).is_none(), "running 不该有 {absent}: {running}");
        }

        jobs.finish(ok, Ok(stats(100, 20)));
        let done = serde_json::to_value(jobs.get(ok).unwrap()).unwrap();
        assert_eq!(done["state"], "done");
        assert_eq!(done["result"]["live_bytes"], 100);
        assert_eq!(done["result"]["dead_bytes"], 20);
        assert_eq!(done["result"]["segments_before"], 3);
        assert_eq!(done["result"]["segments_after"], 1);
        assert!(done["finished_at_ms"].is_u64(), "done 必须带结束时刻: {done}");

        jobs.finish(bad, Err("io: disk on fire".to_string()));
        let failed = serde_json::to_value(jobs.get(bad).unwrap()).unwrap();
        assert_eq!(failed["state"], "failed");
        assert_eq!(failed["error"], "io: disk on fire");
        assert!(failed.get("result").is_none(), "failed 不该有 result: {failed}");

        // 回写未知 job_id(已被淘汰/从未存在)不 panic、不改任何条目。
        jobs.finish(9999, Ok(stats(1, 1)));
        assert!(jobs.get(9999).is_none());
    }

    #[test]
    fn only_most_recent_jobs_are_kept_newest_first() {
        let jobs = CompactionJobs::new();
        let ids: Vec<u64> = (0..MAX_TRACKED_JOBS as u32 + 5).map(|i| jobs.submit(i)).collect();
        assert_eq!(jobs.snapshot().len(), MAX_TRACKED_JOBS, "超出上限必须淘汰最旧");
        assert!(jobs.get(ids[0]).is_none(), "最旧的 5 条应已被淘汰");
        assert!(jobs.get(ids[4]).is_none());
        assert!(jobs.get(ids[5]).is_some(), "边界:第 6 条仍在");

        let snap = jobs.snapshot();
        assert_eq!(snap[0].job_id, *ids.last().unwrap(), "快照最新在前");
        let ids_in_snap: Vec<u64> = snap.iter().map(|j| j.job_id).collect();
        assert!(ids_in_snap.windows(2).all(|w| w[0] > w[1]), "必须严格降序: {ids_in_snap:?}");
    }
}

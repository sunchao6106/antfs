//! 周期性跨副本比对(admin 级一致性巡检的调度循环,openspec `admin-scrub-schedule`)。
//!
//! 把"运维手点的 `POST /api/partitions/{id}/scrub`"加上一条**定时腿**:每拍对**一个**
//! 健康分区跑一次浅层比对(与手动端点共用 [`crate::server::run_scrub`] 与整套计数/环形表,
//! 条目带 `origin=scheduled`),**只报警不修复** —— 分歧的处置永远是运维弧。
//!
//! 节拍(蜗牛家风,别打爆):
//! - 每 tick 只比对一个分区,tick 间睡 `scrub_schedule_interval_secs`(默认 600s;
//!   全量覆盖周期 = 分区数 × 间隔;千分区 ≈ 7 天一轮);
//! - 候选 = `healthy_replicas ≥ 2` 的分区(欠副本的正在被修复,比了也是 inconclusive 噪声);
//! - **deep 恒否**:bit-rot 的周期深扫是 store 侧蜗牛爬(`store-scrub-background`)的职责,
//!   admin 层只做逻辑态比对,两层各司其职;
//! - 并发写会造成窗口末端假不一致 → `partial` 口径如实呈现;周期轮盘天然"下轮重跑",
//!   连续多轮都报同一处差异才是值得人工看的信号;
//! - 运行期闸:`/api/scrub-schedule-control`(不持久化;手动端点永不受它管)。
//!
//! 轮转游标在内存,重启从列表头重新开始(比对只读幂等,多比无害)。

use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::error::Error;
use crate::master_api::MasterApi;
use crate::scrub::ScrubOrigin;
use crate::server::{run_scrub, AppState};

/// 候选列表分页大小(与布局 join 同款:一页 500,齐 total/空页即止)。
const PAGE_LIMIT: u32 = 500;

/// 起周期比对循环(仅 `admin.scrub_schedule_enabled` 时由 main 调用)。
/// 间隔来自已过 `Validate` 的配置,这里只做防御性 `max(1)`。
pub fn spawn_scrub_schedule(state: AppState, interval_secs: u64) {
    let interval = interval_secs.max(1);
    tokio::spawn(async move {
        let mut rr: usize = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(interval)).await;
            if state.scrub_schedule_paused.load(Ordering::Relaxed) {
                continue;
            }
            let candidates = match schedulable_partitions(&state.master).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::warn!(error = %e, "scheduled scrub: listing partitions failed");
                    continue;
                }
            };
            if candidates.is_empty() {
                continue;
            }
            let pid = candidates[rr % candidates.len()];
            rr = rr.wrapping_add(1);
            match run_scrub(&state, pid, false, ScrubOrigin::Scheduled).await {
                Ok(_) => {
                    tracing::debug!(partition = pid, "scheduled scrub round done");
                }
                // 列表与详情两拍之间分区消失:轮盘继续,不算故障。
                Err(Error::PartitionNotFound(_)) => {}
                Err(e) => {
                    tracing::warn!(partition = pid, error = %e, "scheduled scrub round failed");
                }
            }
        }
    });
}

/// 候选 = 健康副本 ≥ 2 的分区 id(升序)。中层错误向上(由循环记 warn 跳过本拍)。
async fn schedulable_partitions(
    master: &std::sync::Arc<dyn MasterApi>,
) -> crate::error::Result<Vec<u32>> {
    let mut ids = Vec::new();
    let mut total = u64::MAX;
    while (ids.len() as u64) < total {
        let (page, t) = master.partitions("", ids.len() as u64, PAGE_LIMIT).await?;
        total = t;
        if page.is_empty() {
            break;
        }
        ids.extend(
            page.into_iter()
                .filter(|p| p.healthy_replicas >= 2)
                .map(|p| p.partition_id),
        );
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use crate::master_api::PartitionDto;

    /// `schedulable_partitions` 的过滤判据:healthy ≥ 2 才进候选(纯逻辑对照;
    /// 列表端到端行为见 server.rs 的循环用例,那里用 FakeMaster + 假 store 走真路径)。
    #[test]
    fn healthy_filter_is_two_or_more() {
        let mk = |pid: u32, healthy: u32| PartitionDto {
            partition_id: pid,
            state: "RW".into(),
            replication_factor: 3,
            replicas: vec![],
            used_bytes: 0,
            healthy_replicas: healthy,
            healable: true,
            under_replicated_reason: String::new(),
            moving_out_nodes: vec![],
        };
        let picked: Vec<u32> = [mk(1, 3), mk(2, 1), mk(3, 2), mk(4, 0)]
            .into_iter()
            .filter(|p| p.healthy_replicas >= 2)
            .map(|p| p.partition_id)
            .collect();
        assert_eq!(picked, vec![1, 3]);
    }
}

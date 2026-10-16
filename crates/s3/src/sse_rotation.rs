//! `s3-sse` 主钥轮换:离线重加密工具(`s3-sse-key-rotation`)。
//!
//! 语义:按 `(bucket, key)` 升序扫全部 `state=1 且 sse 非空`行,逐行「旧钥解帧 →
//! 新钥重帧 → 新 blob → `put_object` 换行(全字段原样,仅 blob_id 与描述符换新)→
//! 遮蔽墓碑 + WAL 消账」。行描述符自报 key_id:**已轮换行重跑即自然跳过**(可续跑 =
//! 构造所得,无工具侧进度状态);他代 key_id 行(过往轮换残留)原样不动;任何
//! 解不开/写不进的行**中止**并报告进度,续跑交操作员。
//!
//! 硬前提:**网关停机运行**(并发写同 key 与轮换的读-改-写竞态会丢更新;`run_tool`
//! 起手即警告,不做在线检测)。

use crate::metastore::types::{ObjectMeta, PutObjectMeta};
use crate::server::AppState;
use crate::sse::{self, MasterKey};
use antfs_router::NO_EXPIRATION;
use axum::body::Bytes;

/// CLI 模式批大小:单轮一页扫描的行数上限(背压常量,防长事务)。
const TOOL_SCAN_BATCH: u64 = 1000;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RotationReport {
    pub scanned: u64,
    pub rotated: u64,
    pub skipped_other_key: u64,
}

#[derive(Debug)]
pub struct RotationFailure {
    /// 中止时的累计进度(仅统计到失败行之前)。
    pub report: RotationReport,
    pub bucket: String,
    pub key: String,
    pub error: String,
}

enum RowOutcome {
    Rotated,
    OtherKey,
}

pub async fn rotate(
    st: &AppState,
    old_master: &MasterKey,
    new_master: &MasterKey,
    scan_batch: u64,
    bucket: Option<&str>,
) -> Result<RotationReport, RotationFailure> {
    let mut report = RotationReport::default();
    let mut after: Option<(String, String)> = None;
    loop {
        let page = st
            .metastore
            .scan_sse_objects(bucket, after.clone(), scan_batch)
            .await
            .map_err(|e| RotationFailure {
                report: report.clone(),
                bucket: after.as_ref().map(|(b, _)| b.clone()).unwrap_or_default(),
                key: after.as_ref().map(|(_, k)| k.clone()).unwrap_or_default(),
                error: format!("scan: {e}"),
            })?;
        if page.is_empty() {
            break;
        }
        for meta in &page {
            after = Some((meta.bucket.clone(), meta.key.clone()));
            report.scanned += 1;
            match rotate_one(st, old_master, new_master, meta).await {
                Ok(RowOutcome::Rotated) => report.rotated += 1,
                Ok(RowOutcome::OtherKey) => report.skipped_other_key += 1,
                Err(error) => {
                    return Err(RotationFailure {
                        report,
                        bucket: meta.bucket.clone(),
                        key: meta.key.clone(),
                        error,
                    })
                }
            }
        }
        if (page.len() as u64) < scan_batch {
            break;
        }
    }
    Ok(report)
}

async fn rotate_one(
    st: &AppState,
    old_master: &MasterKey,
    new_master: &MasterKey,
    meta: &ObjectMeta,
) -> Result<RowOutcome, String> {
    let desc = sse::parse_descriptor(meta.sse.as_deref().unwrap_or_default())
        .ok_or("unparseable sse descriptor")?;
    if desc.alg != "AES256" {
        return Err(format!("unknown sse algorithm `{}`", desc.alg));
    }
    if desc.key_id != old_master.key_id() {
        return Ok(RowOutcome::OtherKey);
    }
    if meta.parts_manifest.is_some() {
        return Err("sse row also carries a parts manifest (unreachable state)".into());
    }
    let frame = st
        .router
        .get(&meta.blob_id)
        .await
        .map_err(|e| format!("blob read: {e}"))?;
    let plain =
        sse::read_object_frame(old_master, &frame, &desc.key_id).map_err(|e| e.to_string())?;
    let new_frame = sse::frame_object(new_master, &plain).map_err(|e| e.to_string())?;
    let (new_blob, _report) = st
        .router
        .put_auto_with_ttl(Bytes::from(new_frame), NO_EXPIRATION)
        .await
        .map_err(|e| format!("new blob write: {e}"))?;
    let outcome = st
        .metastore
        .put_object(PutObjectMeta {
            bucket: meta.bucket.clone(),
            key: meta.key.clone(),
            blob_id: new_blob,
            size: meta.size,
            etag: meta.etag.clone(),
            content_type: meta.content_type.clone(),
            user_meta: meta.user_meta.clone(),
            parts_manifest: None,
            sse: Some(sse::descriptor(new_master.key_id())),
            checksum: meta.checksum.clone(),
        })
        .await;
    match outcome {
        Ok(out) => {
            for old in &out.shadowed_blobs {
                let _ = crate::handlers::tombstone_and_resolve(st, old).await;
            }
            Ok(RowOutcome::Rotated)
        }
        Err(e) => {
            // 行写失败:刚落的新 blob 同请求墓碑(配额拒弧同款,幂等删 + 尽力消账);
            // 旧行与其 blob 引用未动,数据无损。
            let _ = crate::handlers::tombstone_and_resolve(st, &new_blob).await;
            Err(format!("row write: {e}"))
        }
    }
}

/// CLI 入口:`antfs-s3 --rotate-sse-key <new-key-file>`。同 config 起状态(旧主钥
/// 来自 `s3.sse_master_key_file`),不 spawn GC、不监听,跑完即退。
pub async fn run_tool(state: AppState, new_key_file: &str) {
    tracing::warn!(
        "s3 sse key rotation: OFFLINE mode — the gateway MUST be stopped while this runs (concurrent writes during rotation would be lost)"
    );
    let Some(old_master) = state.sse.as_ref() else {
        eprintln!(
            "fatal: s3.sse_master_key_file is not configured — this gateway has no SSE rows to rotate"
        );
        std::process::exit(2);
    };
    let new_master = match MasterKey::load(new_key_file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("fatal: cannot load new master key `{new_key_file}`: {e}");
            std::process::exit(2);
        }
    };
    if new_master.key_id() == old_master.key_id() {
        eprintln!(
            "fatal: new key has the same key_id as the configured master ({}) — nothing to rotate",
            old_master.key_id()
        );
        std::process::exit(2);
    }
    match rotate(&state, old_master, &new_master, TOOL_SCAN_BATCH, None).await {
        Ok(report) => {
            println!(
                "sse key rotation complete: scanned={} rotated={} skipped-other-key={}",
                report.scanned, report.rotated, report.skipped_other_key
            );
            println!(
                "next: write the new key material into s3.sse_master_key_file (currently: {}) and start the gateway",
                state.config.sse_master_key_file
            );
        }
        Err(f) => {
            eprintln!(
                "sse key rotation ABORTED at {}/{}: {} (progress: scanned={} rotated={} skipped-other-key={})",
                f.bucket,
                f.key,
                f.error,
                f.report.scanned,
                f.report.rotated,
                f.report.skipped_other_key
            );
            eprintln!(
                "fix the cause and re-run; rows already rotated are skipped on re-run (their descriptors now carry the new key id)"
            );
            std::process::exit(1);
        }
    }
}

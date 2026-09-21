//! 主动磁盘健康探测(照搬 Ambry `DiskManager$DiskHealthCheck`):周期 write→read→verify→delete,
//! 带超时抓盘 hang。**仅上报观测**(S2 不据此自动下线;放置避让留后续)。默认关(对齐 Ambry)。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::disk::Disk;

/// 磁盘健康状态(对齐 Ambry `DiskHealthStatus`)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskHealth {
    Healthy,
    MountNotAccessible,
    WriteTimeout,
    WriteError,
    ReadTimeout,
    ReadError,
    ReadMismatch,
    DeleteError,
}

impl DiskHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            DiskHealth::Healthy => "HEALTHY",
            DiskHealth::MountNotAccessible => "MOUNT_NOT_ACCESSIBLE",
            DiskHealth::WriteTimeout => "WRITE_TIMEOUT",
            DiskHealth::WriteError => "WRITE_ERROR",
            DiskHealth::ReadTimeout => "READ_TIMEOUT",
            DiskHealth::ReadError => "READ_ERROR",
            DiskHealth::ReadMismatch => "READ_MISMATCH",
            DiskHealth::DeleteError => "DELETE_ERROR",
        }
    }
}

/// 共享的 `disk_id -> DiskHealth`(prober 写、心跳读)。
pub type HealthMap = Arc<RwLock<HashMap<String, DiskHealth>>>;

/// 对一块盘做 5 步探测(带超时)。同步 IO 经 `spawn_blocking` + `tokio::time::timeout` 抓 hang。
pub async fn probe_disk(dir: PathBuf, timeout: Duration) -> DiskHealth {
    if !dir.exists() {
        return DiskHealth::MountNotAccessible;
    }
    let file = dir.join(".ambry_healthcheck");
    let payload = format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );

    let w = {
        let (f, p) = (file.clone(), payload.clone());
        tokio::task::spawn_blocking(move || std::fs::write(&f, p.as_bytes()))
    };
    match tokio::time::timeout(timeout, w).await {
        Err(_) => return DiskHealth::WriteTimeout,
        Ok(Ok(Ok(()))) => {}
        Ok(_) => return DiskHealth::WriteError,
    }

    let r = {
        let f = file.clone();
        tokio::task::spawn_blocking(move || std::fs::read(&f))
    };
    let content = match tokio::time::timeout(timeout, r).await {
        Err(_) => return DiskHealth::ReadTimeout,
        Ok(Ok(Ok(c))) => c,
        Ok(_) => return DiskHealth::ReadError,
    };
    if content != payload.as_bytes() {
        return DiskHealth::ReadMismatch;
    }

    match std::fs::remove_file(&file) {
        Ok(()) => DiskHealth::Healthy,
        Err(_) if !file.exists() => DiskHealth::Healthy,
        Err(_) => DiskHealth::DeleteError,
    }
}

/// 后台周期探测所有盘,结果写入共享 `HealthMap`。
pub fn spawn_prober(disks: Vec<Disk>, interval: Duration, timeout: Duration, out: HealthMap) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            for d in &disks {
                let h = probe_disk(d.dir.clone(), timeout).await;
                out.write().expect("health map poisoned").insert(d.id.clone(), h);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn healthy_disk_probes_healthy() {
        let tmp = tempfile::tempdir().unwrap();
        let h = probe_disk(tmp.path().to_path_buf(), Duration::from_secs(5)).await;
        assert_eq!(h, DiskHealth::Healthy);
    }

    #[tokio::test]
    async fn missing_mount_is_not_accessible() {
        let missing = std::path::PathBuf::from("/nonexistent/ambry/disk/xyz");
        let h = probe_disk(missing, Duration::from_secs(5)).await;
        assert_eq!(h, DiskHealth::MountNotAccessible);
    }
}

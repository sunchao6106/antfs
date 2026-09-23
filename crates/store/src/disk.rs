//! `DiskManager`:声明盘目录 + 运行时 `statfs` 探真实容量/余量(Q1,取 SeaweedFS 式,不写死)。

use std::path::{Path, PathBuf};

/// 一块盘的运行时视图。
#[derive(Clone, Debug)]
pub struct Disk {
    /// 盘 id(目录 basename;为空则用 `disk{index}`)。
    pub id: String,
    /// 盘目录。
    pub dir: PathBuf,
    /// 总容量(字节,`statfs` 探得)。
    pub capacity_bytes: u64,
    /// 可用容量(字节,`statfs` 探得)。
    pub free_bytes: u64,
}

impl Disk {
    /// 暂存目录:流式 put 的落地中转文件(也是崩溃遗留 `.tmp` 的清理点)。
    ///
    /// 落在**数据盘**上而非系统临时目录 —— 大文件暂存不该吃掉 tmpfs 的内存。
    pub fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }
}

/// 清掉暂存目录里遗留的 `*.tmp`(流式 put 中途崩溃/被杀留下的半成品)。
///
/// 安全性:这些文件**从未提交进 log**(提交发生在引擎 `Reserved::commit`),丢弃不影响任何已确认的写;
/// log 里若有半截记录,由既有的 CRC 扫描恢复逻辑淘汰。目录不存在 = 无操作(没跑过流式 put 的部署)。
fn clean_tmp_dir(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().ends_with(".tmp") {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// 用 `statfs`(libc `statvfs`,macOS/Linux 通用)探目录所在文件系统的总量/可用量。
///
/// `total = f_blocks * f_frsize`,`free = f_bavail * f_frsize`(f_bavail 是非特权可用块)。
pub fn statfs(dir: &Path) -> std::io::Result<(u64, u64)> {
    let c_path = std::ffi::CString::new(dir.as_os_str().to_string_lossy().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: statvfs 只读入 c_path、写出 stat;失败返回非 0,不产生别名或未初始化读。
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let frsize = stat.f_frsize as u64;
    let total = (stat.f_blocks as u64).saturating_mul(frsize);
    let free = (stat.f_bavail as u64).saturating_mul(frsize);
    Ok((total, free))
}

/// 持有本节点所有盘的运行时状态。
#[derive(Clone, Debug, Default)]
pub struct DiskManager {
    disks: Vec<Disk>,
}

impl DiskManager {
    /// 从盘目录列表构建:创建目录(不存在则建)并 `statfs` 探容量。
    pub fn mount(dirs: &[String]) -> std::io::Result<Self> {
        let mut disks = Vec::with_capacity(dirs.len());
        for (i, d) in dirs.iter().enumerate() {
            let dir = PathBuf::from(d);
            std::fs::create_dir_all(&dir)?;
            let (capacity_bytes, free_bytes) = statfs(&dir)?;
            // 清掉上次崩溃/中断遗留的流式 put 暂存文件(未提交,见 clean_tmp_dir)。
            clean_tmp_dir(&dir.join("tmp"));
            let id = dir
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("disk{i}"));
            disks.push(Disk {
                id,
                dir,
                capacity_bytes,
                free_bytes,
            });
        }
        Ok(DiskManager { disks })
    }

    /// 重新 `statfs` 刷新各盘余量(心跳前调,S2 用;S1 也可调)。
    pub fn refresh(&mut self) {
        for d in &mut self.disks {
            if let Ok((cap, free)) = statfs(&d.dir) {
                d.capacity_bytes = cap;
                d.free_bytes = free;
            }
        }
    }

    pub fn disks(&self) -> &[Disk] {
        &self.disks
    }

    /// 选第一块可用容量 ≥ `need` 的盘(S1 简单选盘;S2 用 master 指令里的 disk_id)。
    pub fn pick(&self, need: u64) -> Option<&Disk> {
        self.disks.iter().find(|d| d.free_bytes >= need)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statfs_reports_nonzero_capacity() {
        let tmp = tempfile::tempdir().unwrap();
        let (total, free) = statfs(tmp.path()).expect("statfs");
        assert!(total > 0, "capacity must be > 0");
        assert!(free > 0, "free must be > 0");
    }

    #[test]
    fn mount_probes_each_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned()]).unwrap();
        assert_eq!(dm.disks().len(), 1);
        assert!(dm.disks()[0].capacity_bytes > 0);
    }
}

//! `DiskManager`:声明盘目录 + 运行时 `statfs` 探真实容量/余量(Q1,取 SeaweedFS 式,不写死)。

use std::path::{Path, PathBuf};

use crate::config::DataDir;

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
    /// 全节点共用的暂存目录(配置 `store_server.staging_dir`):`None` = 用各盘自己的 `tmp/` 子目录。
    ///
    /// 为什么要留一个"共用"选项:把流式 put 的中转写入从数据盘上挪开(单独一块本地 SSD / scratch 盘),
    /// 代价见 [`StoreServerConfig::staging_dir`](crate::config::StoreServerConfig::staging_dir) 与 store.md §12.11。
    staging_dir: Option<PathBuf>,
}

impl DiskManager {
    /// 从盘声明构建:创建目录(不存在则建)并 `statfs` 探容量。暂存目录走默认(各盘的 `tmp/`)。
    pub fn mount(dirs: &[DataDir]) -> std::io::Result<Self> {
        Self::mount_with(dirs, None)
    }

    /// 同 [`mount`](Self::mount),但可指定**全节点共用的暂存目录**;`Some` 时该目录会被创建并清掉
    /// 遗留的 `*.tmp`(与各盘 `tmp/` 同样的收尾口径)。
    pub fn mount_with(dirs: &[DataDir], staging_dir: Option<PathBuf>) -> std::io::Result<Self> {
        let mut disks = Vec::with_capacity(dirs.len());
        for (i, d) in dirs.iter().enumerate() {
            let dir = PathBuf::from(d.dir());
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
        if let Some(dir) = &staging_dir {
            // 起不来比"悄悄退回默认目录"好:配了专用暂存盘却不可用,属于运维必须立刻知道的错配。
            std::fs::create_dir_all(dir)?;
            clean_tmp_dir(dir);
        }
        Ok(DiskManager { disks, staging_dir })
    }

    pub fn disks(&self) -> &[Disk] {
        &self.disks
    }

    /// 配了共用暂存目录则返回它(供 mount 时打日志/自省用)。
    pub fn staging_dir(&self) -> Option<&Path> {
        self.staging_dir.as_deref()
    }

    /// 某盘的**暂存目录**:配了共用目录就所有盘都用它,否则用该盘自己的 `tmp/` 子目录。
    /// 盘 id 未知 → `None`(调用方据此报"无此副本/盘";配了共用目录也不放宽这条,免得调用方把
    /// "盘不在本节点"当成"盘在但没暂存目录")。
    pub fn tmp_dir_for_disk(&self, disk_id: &str) -> Option<PathBuf> {
        let disk = self.disks.iter().find(|d| d.id == disk_id)?;
        Some(match &self.staging_dir {
            Some(dir) => dir.clone(),
            None => disk.tmp_dir(),
        })
    }

    /// 把一次 `statvfs` 的结果写回某盘(单飞刷新路径用:探测在阻塞池里跑,结果回来再落到这里)。
    /// 盘 id 不存在 = 无操作(盘列表在运行期不变,只可能是调用方传错)。
    pub fn apply_statfs(&mut self, disk_id: &str, capacity_bytes: u64, free_bytes: u64) {
        if let Some(d) = self.disks.iter_mut().find(|d| d.id == disk_id) {
            d.capacity_bytes = capacity_bytes;
            d.free_bytes = free_bytes;
        }
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

    /// 默认(未配 `staging_dir`):每块盘各用自己的 `tmp/` 子目录 —— 这是**既有默认落点**,不许悄悄改。
    #[test]
    fn default_staging_dir_is_per_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned().into()]).unwrap();
        let id = dm.disks()[0].id.clone();
        assert!(dm.staging_dir().is_none());
        assert_eq!(dm.tmp_dir_for_disk(&id), Some(dm.disks()[0].tmp_dir()));
    }

    /// 配了共用暂存目录:所有盘都指向它、嵌套路径由 mount 自己建出来、遗留 `*.tmp` 被清掉,
    /// 且各盘不再被建 `tmp/`;未知盘 id 仍然报 `None`。
    #[test]
    fn staging_override_is_shared_swept_and_created() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir()
            .unwrap()
            .path()
            .join("nested")
            .join("staging");
        std::fs::create_dir_all(&scratch).unwrap();
        std::fs::write(scratch.join("put_1_1.tmp"), b"leftover").unwrap();

        let dm = DiskManager::mount_with(
            &[
                a.path().to_string_lossy().into_owned().into(),
                b.path().to_string_lossy().into_owned().into(),
            ],
            Some(scratch.clone()),
        )
        .unwrap();

        assert_eq!(dm.staging_dir(), Some(scratch.as_path()));
        for d in dm.disks() {
            assert_eq!(
                dm.tmp_dir_for_disk(&d.id).as_deref(),
                Some(scratch.as_path()),
                "两块盘都该用同一个暂存目录"
            );
            assert!(
                !d.dir.join("tmp").exists(),
                "配了共用暂存目录就不该再建各盘的 tmp/"
            );
        }
        assert!(
            dm.tmp_dir_for_disk("no-such-disk").is_none(),
            "未知盘仍报 None(不因为配了共用目录就放宽)"
        );
        assert!(
            std::fs::read_dir(&scratch).unwrap().next().is_none(),
            "遗留 .tmp 应被 mount 清掉"
        );
    }

    /// 暂存目录不存在时由 mount 创建(嵌套路径也要能建),失败则整个挂载失败(宁可起不来)。
    #[test]
    fn staging_override_is_created_by_mount() {
        let tmp = tempfile::tempdir().unwrap();
        let scratch = tmp.path().join("x").join("y");
        assert!(!scratch.exists());
        let dm = DiskManager::mount_with(
            &[tmp.path().to_string_lossy().into_owned().into()],
            Some(scratch.clone()),
        )
        .unwrap();
        assert!(scratch.is_dir(), "mount 应把暂存目录建出来");
        assert_eq!(dm.staging_dir(), Some(scratch.as_path()));
    }

    #[test]
    fn mount_probes_each_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let dm = DiskManager::mount(&[tmp.path().to_string_lossy().into_owned().into()]).unwrap();
        assert_eq!(dm.disks().len(), 1);
        assert!(dm.disks()[0].capacity_bytes > 0);
    }
}

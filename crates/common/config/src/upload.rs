//! 公共上传上限层(`[common]`/`[upload]` 表):**网关之间共用的单文件上限**。
//!
//! 动机(2026-10-21 所有者裁决):REST 与 S3 两个网关必须执行**同一个**单文件上传上限;
//! 各写各的旋钮会漂移(改了一边忘了另一边)。公共层的形态 = 一张小表,任一网关的
//! 配置链都先读共享文件(`common.toml`,可选),自家文件同键可覆盖。

use serde::Deserialize;

use crate::{at_least, Result, Validate};

/// 单文件上传上限公共配置。
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct UploadConfig {
    /// **单次上传的单文件大小上限**(字节):REST 面超限直接 413;S3 面 PutObject/整 multipart
    /// 完成时合计超限拒绝。网关行为差异(直传Collection拒绝 vs 分片完成对账拒绝)一致执行。
    ///
    /// **默认 16 GiB** = REST 描述符的容量天花板(4096 片 × 默认片 4 MiB)。REST 侧配置校验
    /// 会强制 `≤ 4096 × auto_chunk_bytes`(超 = 配置错,先调大片宽);S3 侧无片宽耦合,
    /// 仅受此数约束。
    pub max_object_bytes: u64,
}

impl Default for UploadConfig {
    fn default() -> Self {
        UploadConfig {
            max_object_bytes: 4096u64 * 4 * 1024 * 1024,
        }
    }
}

impl Validate for UploadConfig {
    fn validate(&self) -> Result<()> {
        at_least("common.max_object_bytes", self.max_object_bytes, 1)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_validation() {
        let cfg = UploadConfig::default();
        assert_eq!(cfg.max_object_bytes, 16 * 1024 * 1024 * 1024);
        cfg.validate().expect("默认必须自洽");
        let bad = UploadConfig {
            max_object_bytes: 0,
        };
        assert!(bad.validate().is_err(), "0 上限必须拒");
    }

    #[test]
    fn parses_the_upload_table_via_loader() {
        // 与各网关实装同一条 Loader 路径(内联 TOML 验证顶层 [upload] 表可解析)。
        let wrapper: LoaderWrap = crate::Loader::new()
            .with_inline_toml("[upload]\nmax_object_bytes = 1048576\n")
            .load()
            .expect("解析 [upload] 表");
        assert_eq!(wrapper.upload.max_object_bytes, 1048576);
        wrapper.upload.validate().expect("值合法");
    }

    /// 反序列化载体:顶层 `[upload]` 表(网关各自的 TOML 顶层结构内嵌同一张表,
    /// 共享文件与自家文件都能生效)。
    #[derive(Deserialize)]
    struct LoaderWrap {
        upload: UploadConfig,
    }
}

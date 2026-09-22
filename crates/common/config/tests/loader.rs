use antfs_config::{at_least, in_range, one_of, ConfigError, Loader, Result, Validate};
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(default)]
struct StoreConfig {
    data_flush_interval_seconds: u64,
    index_max_memory_size_bytes: u64,
    compaction_policy: String,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            data_flush_interval_seconds: 60,
            index_max_memory_size_bytes: 20_971_520,
            compaction_policy: "hybrid".to_string(),
        }
    }
}

impl Validate for StoreConfig {
    fn validate(&self) -> Result<()> {
        in_range(
            "data_flush_interval_seconds",
            self.data_flush_interval_seconds,
            1,
            86_400,
        )?;
        at_least(
            "index_max_memory_size_bytes",
            self.index_max_memory_size_bytes,
            1,
        )?;
        one_of(
            "compaction_policy",
            self.compaction_policy.as_str(),
            &["hybrid", "stats", "compact_all"],
        )?;
        Ok(())
    }
}

#[test]
fn empty_loader_yields_struct_defaults() {
    let cfg: StoreConfig = Loader::new().load().unwrap();
    assert_eq!(cfg, StoreConfig::default());
}

#[test]
fn inline_toml_overrides_single_field() {
    let cfg: StoreConfig = Loader::new()
        .with_inline_toml("data_flush_interval_seconds = 30")
        .load()
        .unwrap();
    assert_eq!(cfg.data_flush_interval_seconds, 30);
    assert_eq!(cfg.index_max_memory_size_bytes, 20_971_520);
}

#[test]
fn explicit_override_wins_over_toml() {
    let cfg: StoreConfig = Loader::new()
        .with_inline_toml("data_flush_interval_seconds = 30")
        .set_override("data_flush_interval_seconds", "45")
        .load()
        .unwrap();
    assert_eq!(cfg.data_flush_interval_seconds, 45);
}

#[test]
fn env_overrides_toml() {
    std::env::set_var("AMBRYCFGTEST_DATA_FLUSH_INTERVAL_SECONDS", "99");
    let cfg: StoreConfig = Loader::new()
        .with_inline_toml("data_flush_interval_seconds = 30")
        .with_env_prefix("AMBRYCFGTEST")
        .load()
        .unwrap();
    std::env::remove_var("AMBRYCFGTEST_DATA_FLUSH_INTERVAL_SECONDS");
    assert_eq!(cfg.data_flush_interval_seconds, 99);
}

#[test]
fn validate_rejects_out_of_range() {
    let err = Loader::new()
        .with_inline_toml("data_flush_interval_seconds = 0")
        .load_validated::<StoreConfig>()
        .unwrap_err();
    match err {
        ConfigError::Invalid { key, .. } => assert_eq!(key, "data_flush_interval_seconds"),
        other => panic!("expected Invalid, got {other:?}"),
    }
}

#[test]
fn validate_rejects_unknown_enum() {
    let err = Loader::new()
        .with_inline_toml("compaction_policy = \"bogus\"")
        .load_validated::<StoreConfig>()
        .unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { .. }));
}

#[test]
fn range_helpers_accept_valid_values() {
    assert_eq!(in_range("k", 5, 1, 10).unwrap(), 5);
    assert_eq!(at_least("k", 5, 1).unwrap(), 5);
    assert_eq!(one_of("k", "a", &["a", "b"]).unwrap(), "a");
}

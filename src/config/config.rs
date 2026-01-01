use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs;

use serde::Deserialize;
use serde::Serialize;

use super::StorageParams;
use crate::error::Error;
use crate::error::Result;

const DEFAULT_MIN_CHECKPOINT_PAGE_NUMBER: u64 = 1000;
const DEFAULT_MAX_CHECKPOINT_PAGE_NUMBER: u64 = 10000;
const DEFAULT_TRUNCATE_PAGE_NUMBER: u64 = 500000;
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 60;
const DEFAULT_WAL_RETENTION_COUNT: u64 = 10;
const DEFAULT_APPLY_CHECKPOINT_FRAME_INTERVAL: u32 = 128;
const DEFAULT_APPLY_CHECKPOINT_INTERVAL_MS: u64 = 2000;
const DEFAULT_MAX_CONCURRENT_SNAPSHOTS: usize = 5;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub log: LogConfig,

    pub database: Vec<DbConfig>,
}

impl Config {
    pub fn load(config_file: &str) -> Result<Self> {
        let toml_str = match fs::read_to_string(config_file) {
            Ok(toml_str) => toml_str,
            Err(e) => {
                return Err(Error::ReadConfigFail(format!(
                    "read config file {config_file} fail: {e:?}",
                )));
            }
        };

        let config: Config = match toml::from_str(&toml_str) {
            Ok(config) => config,
            Err(e) => {
                return Err(Error::ParseConfigFail(format!(
                    "parse config file {config_file} fail: {e:?}",
                )));
            }
        };

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.database.is_empty() {
            return Err(Error::InvalidConfig(
                "config MUST has at least one database config",
            ));
        }
        for db in &self.database {
            db.validate()?;
        }
        Ok(())
    }
}

/// Config for logging.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    pub dir: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            dir: "/var/log/replited".to_string(),
        }
    }
}

impl Display for LogConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "level={:?}, dir={}", self.level, self.dir)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbConfig {
    // db file full path
    pub db: String,

    // replicates of db file config
    pub replicate: Vec<StorageConfig>,

    // Minimum threshold of WAL size, in pages, before a passive checkpoint.
    // A passive checkpoint will attempt a checkpoint but fail if there are
    // active transactions occurring at the same time.
    #[serde(default = "default_min_checkpoint_page_number")]
    pub min_checkpoint_page_number: u64,

    // Maximum threshold of WAL size, in pages, before a forced checkpoint.
    // A forced checkpoint will block new transactions and wait for existing
    // transactions to finish before issuing a checkpoint and resetting the WAL.
    //
    // If zero, no checkpoints are forced. This can cause the WAL to grow
    // unbounded if there are always read transactions occurring.
    #[serde(default = "default_max_checkpoint_page_number")]
    pub max_checkpoint_page_number: u64,

    // Threshold of WAL size, in pages, before a forced truncation checkpoint.
    // A forced truncation checkpoint will block new transactions and wait for
    // existing transactions to finish before issuing a checkpoint and
    // truncating the WAL.
    //
    // If zero, no truncates are forced. This can cause the WAL to grow
    // unbounded if there's a sudden spike of changes between other
    // checkpoints.
    #[serde(default = "default_truncate_page_number")]
    pub truncate_page_number: u64,

    // Seconds between automatic checkpoints in the WAL. This is done to allow
    // more fine-grained WAL files so that restores can be performed with
    // better precision.
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    // Replica-side WAL apply: how many frames to buffer before forcing a checkpoint.
    // Lower values make new schema/rows visible to readers sooner at the cost of more I/O.
    #[serde(default = "default_apply_checkpoint_frame_interval")]
    pub apply_checkpoint_frame_interval: u32,

    // Replica-side WAL apply: max milliseconds between checkpoints even if
    // the frame threshold is not reached.
    #[serde(default = "default_apply_checkpoint_interval_ms")]
    pub apply_checkpoint_interval_ms: u64,

    // Number of WAL files to retain in the local filesystem after replication.
    // This allows for gap filling when a replica reconnects.
    #[serde(default = "default_wal_retention_count")]
    pub wal_retention_count: u64,

    // Maximum number of concurrent snapshot streams allowed.
    // This protects the Primary from overload when many Replicas request snapshots simultaneously.
    #[serde(default = "default_max_concurrent_snapshots")]
    pub max_concurrent_snapshots: usize,
}

fn default_min_checkpoint_page_number() -> u64 {
    DEFAULT_MIN_CHECKPOINT_PAGE_NUMBER
}

fn default_max_checkpoint_page_number() -> u64 {
    DEFAULT_MAX_CHECKPOINT_PAGE_NUMBER
}

fn default_truncate_page_number() -> u64 {
    DEFAULT_TRUNCATE_PAGE_NUMBER
}

fn default_checkpoint_interval_secs() -> u64 {
    DEFAULT_CHECKPOINT_INTERVAL_SECS
}

fn default_apply_checkpoint_frame_interval() -> u32 {
    DEFAULT_APPLY_CHECKPOINT_FRAME_INTERVAL
}

fn default_apply_checkpoint_interval_ms() -> u64 {
    DEFAULT_APPLY_CHECKPOINT_INTERVAL_MS
}

fn default_wal_retention_count() -> u64 {
    DEFAULT_WAL_RETENTION_COUNT
}

fn default_max_concurrent_snapshots() -> usize {
    DEFAULT_MAX_CONCURRENT_SNAPSHOTS
}

impl Debug for DbConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ReplicateDbConfig")
            .field("db", &self.db)
            .field("storage", &self.replicate)
            .field(
                "min_checkpoint_page_number",
                &self.min_checkpoint_page_number,
            )
            .field(
                "max_checkpoint_page_number",
                &self.max_checkpoint_page_number,
            )
            .field("truncate_page_number", &self.truncate_page_number)
            .field("checkpoint_interval_secs", &self.checkpoint_interval_secs)
            .field(
                "apply_checkpoint_frame_interval",
                &self.apply_checkpoint_frame_interval,
            )
            .field(
                "apply_checkpoint_interval_ms",
                &self.apply_checkpoint_interval_ms,
            )
            .field("wal_retention_count", &self.wal_retention_count)
            .finish()
    }
}

impl DbConfig {
    fn validate(&self) -> Result<()> {
        if self.replicate.is_empty() {
            return Err(Error::InvalidConfig(
                "database MUST has at least one replicate config",
            ));
        }

        if self.min_checkpoint_page_number == 0 {
            return Err(Error::InvalidConfig(
                "min_checkpoint_page_number cannot be zero",
            ));
        }

        if self.min_checkpoint_page_number > self.max_checkpoint_page_number {
            return Err(Error::InvalidConfig(
                "min_checkpoint_page_number cannot bigger than max_checkpoint_page_number",
            ));
        }
        if self.apply_checkpoint_frame_interval == 0 {
            return Err(Error::InvalidConfig(
                "apply_checkpoint_frame_interval must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub name: String,
    pub params: StorageParams,
}

impl Debug for StorageConfig {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("StorageS3Config")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::StorageFsConfig;

    fn create_valid_storage_config() -> StorageConfig {
        StorageConfig {
            name: "test-storage".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: "/tmp/replited".to_string(),
            })),
        }
    }

    fn create_valid_db_config() -> DbConfig {
        DbConfig {
            db: "/tmp/test.db".to_string(),
            replicate: vec![create_valid_storage_config()],
            min_checkpoint_page_number: DEFAULT_MIN_CHECKPOINT_PAGE_NUMBER,
            max_checkpoint_page_number: DEFAULT_MAX_CHECKPOINT_PAGE_NUMBER,
            truncate_page_number: DEFAULT_TRUNCATE_PAGE_NUMBER,
            checkpoint_interval_secs: DEFAULT_CHECKPOINT_INTERVAL_SECS,
            apply_checkpoint_frame_interval: DEFAULT_APPLY_CHECKPOINT_FRAME_INTERVAL,
            apply_checkpoint_interval_ms: DEFAULT_APPLY_CHECKPOINT_INTERVAL_MS,
            wal_retention_count: DEFAULT_WAL_RETENTION_COUNT,
            max_concurrent_snapshots: DEFAULT_MAX_CONCURRENT_SNAPSHOTS,
        }
    }

    // ========== LogConfig tests ==========

    #[test]
    fn test_log_config_default() {
        let log_config = LogConfig::default();
        assert_eq!(LogLevel::Info, log_config.level);
        assert_eq!("/var/log/replited", log_config.dir);
    }

    #[test]
    fn test_log_config_display() {
        let log_config = LogConfig {
            level: LogLevel::Debug,
            dir: "/custom/log".to_string(),
        };
        let display = format!("{}", log_config);
        assert!(display.contains("Debug"));
        assert!(display.contains("/custom/log"));
    }

    // ========== DbConfig validation tests ==========

    #[test]
    fn test_db_config_validate_success() {
        let config = create_valid_db_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_db_config_validate_empty_replicate() {
        let mut config = create_valid_db_config();
        config.replicate = vec![];

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_db_config_validate_min_checkpoint_zero() {
        let mut config = create_valid_db_config();
        config.min_checkpoint_page_number = 0;

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_db_config_validate_min_greater_than_max() {
        let mut config = create_valid_db_config();
        config.min_checkpoint_page_number = 10000;
        config.max_checkpoint_page_number = 1000;

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_db_config_validate_apply_checkpoint_frame_interval_zero() {
        let mut config = create_valid_db_config();
        config.apply_checkpoint_frame_interval = 0;

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_db_config_validate_min_equals_max() {
        // Edge case: min equals max should be valid
        let mut config = create_valid_db_config();
        config.min_checkpoint_page_number = 5000;
        config.max_checkpoint_page_number = 5000;

        assert!(config.validate().is_ok());
    }

    // ========== Default value function tests ==========

    #[test]
    fn test_default_values() {
        assert_eq!(1000, default_min_checkpoint_page_number());
        assert_eq!(10000, default_max_checkpoint_page_number());
        assert_eq!(500000, default_truncate_page_number());
        assert_eq!(60, default_checkpoint_interval_secs());
        assert_eq!(128, default_apply_checkpoint_frame_interval());
        assert_eq!(2000, default_apply_checkpoint_interval_ms());
        assert_eq!(10, default_wal_retention_count());
        assert_eq!(5, default_max_concurrent_snapshots());
    }

    // ========== Config validation tests ==========

    #[test]
    fn test_config_validate_empty_database() {
        let config = Config {
            log: LogConfig::default(),
            database: vec![],
        };

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_validate_with_valid_db() {
        let config = Config {
            log: LogConfig::default(),
            database: vec![create_valid_db_config()],
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_propagates_db_error() {
        let mut db_config = create_valid_db_config();
        db_config.replicate = vec![]; // Invalid

        let config = Config {
            log: LogConfig::default(),
            database: vec![db_config],
        };

        let result = config.validate();
        assert!(result.is_err());
    }

    // ========== DbConfig Debug tests ==========

    #[test]
    fn test_db_config_debug_format() {
        let config = create_valid_db_config();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("ReplicateDbConfig"));
        assert!(debug_str.contains("/tmp/test.db"));
    }

    // ========== StorageConfig Debug tests ==========

    #[test]
    fn test_storage_config_debug_format() {
        let config = create_valid_storage_config();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("StorageS3Config"));
        assert!(debug_str.contains("test-storage"));
    }
}

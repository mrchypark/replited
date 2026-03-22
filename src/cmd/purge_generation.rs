use super::command::Command;
use crate::config::Config;
use crate::config::PurgeGenerationOptions;
use crate::config::StorageParams;
use crate::error::Error;
use crate::error::Result;
use crate::log::init_log;
use crate::storage::StorageClient;

pub struct PurgeGeneration {
    config: Config,
    options: PurgeGenerationOptions,
}

impl PurgeGeneration {
    pub fn try_create(config: &str, options: PurgeGenerationOptions) -> Result<Box<Self>> {
        let config = Config::load(config)?;
        let log_config = config.log.clone();

        init_log(log_config)?;
        Ok(Box::new(Self { config, options }))
    }
}

#[async_trait::async_trait]
impl Command for PurgeGeneration {
    async fn run(&mut self) -> Result<()> {
        self.options.validate()?;

        for db in &self.config.database {
            if db.db != self.options.db {
                continue;
            }

            for replicate in &db.replicate {
                if matches!(replicate.params, StorageParams::Stream(_)) {
                    continue;
                }
                let client = StorageClient::try_create(db.db.clone(), replicate.clone())?;
                client
                    .purge_manifest_generation(&self.options.generation)
                    .await?;
            }

            return Ok(());
        }

        Err(Error::InvalidArg(format!(
            "cannot find db {} in config file",
            self.options.db
        )))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::base::Generation;
    use crate::base::compress_buffer;
    use crate::config::StorageFtpConfig;
    use crate::config::StorageStreamConfig;
    use crate::config::{
        DbConfig, LogConfig, LogLevel, StorageConfig, StorageFsConfig, StorageParams,
    };
    use crate::database::WalGenerationPos;

    fn sample_config(db: &str, roots: &[String]) -> Config {
        Config {
            log: LogConfig {
                level: LogLevel::Off,
                dir: "/tmp".to_string(),
            },
            database: vec![DbConfig {
                db: db.to_string(),
                replicate: roots
                    .iter()
                    .enumerate()
                    .map(|(idx, root)| StorageConfig {
                        name: format!("fs-{idx}"),
                        params: StorageParams::Fs(Box::new(StorageFsConfig { root: root.clone() })),
                    })
                    .collect(),
                cache_root: None,
                min_checkpoint_page_number: 1000,
                max_checkpoint_page_number: 10000,
                truncate_page_number: 500000,
                checkpoint_interval_secs: 60,
                monitor_interval_ms: 1000,
                apply_checkpoint_frame_interval: 128,
                apply_checkpoint_interval_ms: 2000,
                wal_retention_count: 10,
                max_concurrent_snapshots: 5,
            }],
        }
    }

    async fn publish_generation(root: &str, db: &str, generation: &Generation) -> StorageClient {
        let client = StorageClient::try_create(
            db.to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string(),
                })),
            },
        )
        .expect("storage client");

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        client
    }

    #[tokio::test]
    async fn purge_generation_command_purges_all_supported_targets_for_db() {
        let temp = tempdir().expect("tempdir");
        let root_a = temp.path().join("a");
        let root_b = temp.path().join("b");
        let db = "/tmp/test.db";
        let old_generation = Generation::new();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let new_generation = Generation::new();

        for root in [&root_a, &root_b] {
            let client =
                publish_generation(root.to_string_lossy().as_ref(), db, &old_generation).await;
            client
                .publish_manifest_snapshot(
                    &WalGenerationPos {
                        generation: new_generation.clone(),
                        index: 1,
                        offset: 4096,
                    },
                    compress_buffer(b"new snapshot").expect("compress new snapshot"),
                )
                .await
                .expect("publish new snapshot");
        }

        let mut cmd = PurgeGeneration {
            config: sample_config(
                db,
                &[
                    root_a.to_string_lossy().to_string(),
                    root_b.to_string_lossy().to_string(),
                ],
            ),
            options: PurgeGenerationOptions {
                db: db.to_string(),
                generation: old_generation.as_str().to_string(),
            },
        };

        cmd.run().await.expect("purge all supported targets");

        for root in [&root_a, &root_b] {
            let manifest = root.join(format!(
                "test.db/manifests/generations/{}.manifest.json",
                old_generation.as_str()
            ));
            assert!(!std::fs::exists(manifest).expect("manifest exists check"));
        }
    }

    #[tokio::test]
    async fn purge_generation_command_surfaces_supported_target_failure() {
        let temp = tempdir().expect("tempdir");
        let root_a = temp.path().join("a");
        let root_b = temp.path().join("b");
        let db = "/tmp/test.db";
        let old_generation = Generation::new();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let new_generation = Generation::new();

        let client_a =
            publish_generation(root_a.to_string_lossy().as_ref(), db, &old_generation).await;
        client_a
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: new_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"new snapshot").expect("compress new snapshot"),
            )
            .await
            .expect("publish new snapshot");
        let client_b =
            publish_generation(root_b.to_string_lossy().as_ref(), db, &new_generation).await;
        drop(client_b);

        let mut cmd = PurgeGeneration {
            config: sample_config(
                db,
                &[
                    root_a.to_string_lossy().to_string(),
                    root_b.to_string_lossy().to_string(),
                ],
            ),
            options: PurgeGenerationOptions {
                db: db.to_string(),
                generation: old_generation.as_str().to_string(),
            },
        };

        let err = cmd
            .run()
            .await
            .expect_err("second supported target should fail");
        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn purge_generation_command_rejects_unsupported_target_instead_of_skipping() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("a");
        let db = "/tmp/test.db";
        let old_generation = Generation::new();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let new_generation = Generation::new();

        let client = publish_generation(root.to_string_lossy().as_ref(), db, &old_generation).await;
        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: new_generation,
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"new snapshot").expect("compress new snapshot"),
            )
            .await
            .expect("publish new snapshot");

        let mut cmd = PurgeGeneration {
            config: Config {
                log: LogConfig {
                    level: LogLevel::Off,
                    dir: "/tmp".to_string(),
                },
                database: vec![DbConfig {
                    db: db.to_string(),
                    replicate: vec![
                        StorageConfig {
                            name: "fs".to_string(),
                            params: StorageParams::Fs(Box::new(StorageFsConfig {
                                root: root.to_string_lossy().to_string(),
                            })),
                        },
                        StorageConfig {
                            name: "ftp".to_string(),
                            params: StorageParams::Ftp(Box::new(StorageFtpConfig::default())),
                        },
                    ],
                    cache_root: None,
                    min_checkpoint_page_number: 1000,
                    max_checkpoint_page_number: 10000,
                    truncate_page_number: 500000,
                    checkpoint_interval_secs: 60,
                    monitor_interval_ms: 1000,
                    apply_checkpoint_frame_interval: 128,
                    apply_checkpoint_interval_ms: 2000,
                    wal_retention_count: 10,
                    max_concurrent_snapshots: 5,
                }],
            },
            options: PurgeGenerationOptions {
                db: db.to_string(),
                generation: old_generation.as_str().to_string(),
            },
        };

        let err = cmd
            .run()
            .await
            .expect_err("unsupported target should no longer be skipped");
        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn purge_generation_command_ignores_stream_targets_and_purges_fs_targets() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("a");
        let db = "/tmp/test.db";
        let old_generation = Generation::new();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let new_generation = Generation::new();

        let client = publish_generation(root.to_string_lossy().as_ref(), db, &old_generation).await;
        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: new_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"new snapshot").expect("compress new snapshot"),
            )
            .await
            .expect("publish new snapshot");

        let mut cmd = PurgeGeneration {
            config: Config {
                log: LogConfig {
                    level: LogLevel::Off,
                    dir: "/tmp".to_string(),
                },
                database: vec![DbConfig {
                    db: db.to_string(),
                    replicate: vec![
                        StorageConfig {
                            name: "fs".to_string(),
                            params: StorageParams::Fs(Box::new(StorageFsConfig {
                                root: root.to_string_lossy().to_string(),
                            })),
                        },
                        StorageConfig {
                            name: "stream".to_string(),
                            params: StorageParams::Stream(Box::new(StorageStreamConfig::default())),
                        },
                    ],
                    cache_root: None,
                    min_checkpoint_page_number: 1000,
                    max_checkpoint_page_number: 10000,
                    truncate_page_number: 500000,
                    checkpoint_interval_secs: 60,
                    monitor_interval_ms: 1000,
                    apply_checkpoint_frame_interval: 128,
                    apply_checkpoint_interval_ms: 2000,
                    wal_retention_count: 10,
                    max_concurrent_snapshots: 5,
                }],
            },
            options: PurgeGenerationOptions {
                db: db.to_string(),
                generation: old_generation.as_str().to_string(),
            },
        };

        cmd.run()
            .await
            .expect("stream target should be ignored for purge");

        let manifest = root.join(format!(
            "test.db/manifests/generations/{}.manifest.json",
            old_generation.as_str()
        ));
        assert!(!std::fs::exists(manifest).expect("manifest exists check"));
    }
}

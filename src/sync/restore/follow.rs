use log::info;

use crate::error::Result;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;

fn new_generation_start_index(snapshot: &SnapshotInfo) -> u64 {
    snapshot.index
}

#[derive(Debug, PartialEq, Eq)]
enum FollowManifestAction {
    None,
    ApplySameGeneration,
    SwitchGeneration,
}

fn determine_follow_manifest_action(
    current_snapshot: &SnapshotInfo,
    current_progress: (u64, u64),
    latest_plan: &super::DiscoveredRestorePlan,
) -> FollowManifestAction {
    if latest_plan.snapshot.generation > current_snapshot.generation {
        return FollowManifestAction::SwitchGeneration;
    }

    if latest_plan.snapshot.generation == current_snapshot.generation
        && !latest_plan.wal_objects.is_empty()
        && super::restore_plan_progress(latest_plan) > current_progress
    {
        return FollowManifestAction::ApplySameGeneration;
    }

    FollowManifestAction::None
}

impl super::Restore {
    async fn apply_follow_generation_switch(
        &self,
        client: &StorageClient,
        latest_plan: &super::DiscoveredRestorePlan,
    ) -> Result<(u64, u64)> {
        self.restore_snapshot_from_key(
            client,
            &latest_plan.snapshot_key,
            &latest_plan.snapshot_sha256,
            &self.options.output,
        )
        .await?;

        let (new_last_index, new_last_offset, _) = self
            .apply_wal_objects(
                client,
                &latest_plan.wal_objects,
                &self.options.output,
                new_generation_start_index(&latest_plan.snapshot),
                0,
                false,
            )
            .await?;

        Ok((new_last_index, new_last_offset))
    }

    pub(super) async fn follow_loop(
        &self,
        mut current_snapshot: SnapshotInfo,
        mut last_index: u64,
        mut last_offset: u64,
    ) -> Result<()> {
        info!("entering follow mode...");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(self.options.interval)).await;

            let latest_selected = self.decide_restore_plan(None).await?;
            let latest_plan = latest_selected.plan;
            if let Some(cache) = &self.cache {
                let pinned = std::iter::once(latest_plan.snapshot_key.clone())
                    .chain(latest_plan.wal_objects.iter().map(|wal| wal.object_key.clone()))
                    .collect::<Vec<_>>();
                cache.set_pinned_keys(pinned);
            }

            match determine_follow_manifest_action(
                &current_snapshot,
                (last_index, last_offset),
                &latest_plan,
            ) {
                FollowManifestAction::None => {}
                FollowManifestAction::ApplySameGeneration => {
                    let current_client = StorageClient::try_create_with_cache(
                        self.db.clone(),
                        latest_selected.storage_config,
                        self.cache.clone(),
                    )?;
                    let (new_last_index, new_last_offset, _) = self
                        .apply_wal_objects(
                            &current_client,
                            &latest_plan.wal_objects,
                            &self.options.output,
                            last_index,
                            last_offset,
                            false,
                        )
                        .await?;

                    last_index = new_last_index;
                    last_offset = new_last_offset;
                    info!("applied updates up to index {last_index}, offset {last_offset}");
                }
                FollowManifestAction::SwitchGeneration => {
                    info!(
                        "detected new generation: {:?}",
                        latest_plan.snapshot.generation
                    );

                    let current_client = StorageClient::try_create_with_cache(
                        self.db.clone(),
                        latest_selected.storage_config,
                        self.cache.clone(),
                    )?;

                    let (new_last_index, new_last_offset) = self
                        .apply_follow_generation_switch(&current_client, &latest_plan)
                        .await?;
                    current_snapshot = latest_plan.snapshot;
                    last_index = new_last_index;
                    last_offset = new_last_offset;
                    info!("switched to new generation and applied updates");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rusqlite::Connection;
    use sha2::Digest;
    use tempfile::tempdir;

    use crate::base::Generation;
    use crate::base::compress_buffer;
    use crate::config::{RestoreOptions, StorageConfig, StorageFsConfig, StorageParams};
    use crate::storage::SnapshotInfo;
    use crate::storage::StorageClient;

    #[test]
    fn new_generation_start_index_uses_snapshot_index() {
        let snapshot = SnapshotInfo {
            generation: Generation::new(),
            index: 17,
            offset: 8192,
            size: 0,
            created_at: chrono::Utc::now(),
        };

        assert_eq!(super::new_generation_start_index(&snapshot), 17);
    }

    fn sample_plan(
        generation: Generation,
        snapshot_index: u64,
        wal_progress: &[(u64, u64, u64)],
    ) -> super::super::DiscoveredRestorePlan {
        super::super::DiscoveredRestorePlan {
            snapshot: SnapshotInfo {
                generation,
                index: snapshot_index,
                offset: 0,
                size: 0,
                created_at: Utc::now(),
            },
            snapshot_key: "db.db/generations/g/snapshots/0000000001_0000000000.snapshot.zst"
                .to_string(),
            snapshot_sha256: "snapshot-sha".to_string(),
            wal_objects: wal_progress
                .iter()
                .map(
                    |(index, offset, end_offset)| super::super::ManifestRestoreWalObject {
                        index: *index,
                        offset: *offset,
                        end_offset: *end_offset,
                        object_key: format!(
                            "db.db/generations/g/wal/{index:010}_{offset:010}.wal.zst"
                        ),
                        sha256: format!("sha-{index}-{offset}"),
                    },
                )
                .collect(),
        }
    }

    #[test]
    fn follow_manifest_action_switches_on_new_generation() {
        let current = SnapshotInfo {
            generation: Generation::try_create("019c3e53aea47afbbddfe5ebc2272e22")
                .expect("generation"),
            index: 1,
            offset: 0,
            size: 0,
            created_at: Utc::now(),
        };
        let latest = sample_plan(
            Generation::try_create("019c3e53aea57afbbddfe5ebc2272e22").expect("generation"),
            1,
            &[(1, 0, 1024)],
        );

        assert_eq!(
            super::determine_follow_manifest_action(&current, (1, 1024), &latest),
            super::FollowManifestAction::SwitchGeneration
        );
    }

    #[test]
    fn follow_manifest_action_applies_same_generation_progress_without_listing() {
        let generation =
            Generation::try_create("019c3e53aea47afbbddfe5ebc2272e22").expect("generation");
        let current = SnapshotInfo {
            generation: generation.clone(),
            index: 1,
            offset: 0,
            size: 0,
            created_at: Utc::now(),
        };
        let latest = sample_plan(generation, 1, &[(1, 0, 1024), (2, 0, 1024)]);

        assert_eq!(
            super::determine_follow_manifest_action(&current, (1, 1024), &latest),
            super::FollowManifestAction::ApplySameGeneration
        );
    }

    #[test]
    fn follow_manifest_action_ignores_same_generation_manifest_without_wal_objects() {
        let generation =
            Generation::try_create("019c3e53aea47afbbddfe5ebc2272e22").expect("generation");
        let current = SnapshotInfo {
            generation: generation.clone(),
            index: 1,
            offset: 0,
            size: 0,
            created_at: Utc::now(),
        };
        let latest = sample_plan(generation, 1, &[]);

        assert_eq!(
            super::determine_follow_manifest_action(&current, (1, 0), &latest),
            super::FollowManifestAction::None
        );
    }

    #[tokio::test]
    async fn follow_generation_switch_restores_new_snapshot_before_applying_wal() {
        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let output_path = temp.path().join("follow.db");
        let source_snapshot_path = temp.path().join("new-generation.db");

        {
            let conn = Connection::open(&output_path).expect("open output db");
            conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)", [])
                .expect("create table");
            conn.execute("INSERT INTO items(value) VALUES ('old')", [])
                .expect("insert old row");
        }
        {
            let conn = Connection::open(&source_snapshot_path).expect("open source db");
            conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)", [])
                .expect("create table");
            conn.execute("INSERT INTO items(value) VALUES ('new')", [])
                .expect("insert new row");
        }

        let storage_config = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let client = StorageClient::try_create("db.db".to_string(), storage_config.clone())
            .expect("storage client");
        let generation = Generation::new();
        let snapshot = SnapshotInfo {
            generation: generation.clone(),
            index: 1,
            offset: 0,
            size: 0,
            created_at: Utc::now(),
        };
        let snapshot_key = format!(
            "db.db/generations/{}/snapshots/0000000001_0000000000.snapshot.zst",
            generation.as_str()
        );
        let compressed_snapshot = compress_buffer(
            &std::fs::read(&source_snapshot_path).expect("read source snapshot"),
        )
        .expect("compress snapshot");
        let snapshot_sha256 = format!("{:x}", sha2::Sha256::digest(&compressed_snapshot));
        client
            .publish_manifest_snapshot(
                &crate::database::WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 0,
                },
                compressed_snapshot,
            )
            .await
            .expect("publish manifest snapshot");

        let restore = super::super::Restore::try_create(
            "db.db".to_string(),
            vec![storage_config],
            RestoreOptions {
                db: "db.db".to_string(),
                output: output_path.to_string_lossy().to_string(),
                follow: true,
                interval: 1,
                timestamp: "".to_string(),
                truth_source: String::new(),
            },
            None,
        )
        .expect("restore");

        let plan = super::super::DiscoveredRestorePlan {
            snapshot,
            snapshot_key,
            snapshot_sha256,
            wal_objects: vec![],
        };

        let (last_index, last_offset) = restore
            .apply_follow_generation_switch(&client, &plan)
            .await
            .expect("switch generation");

        let conn = Connection::open(&output_path).expect("open restored db");
        let value: String = conn
            .query_row("SELECT value FROM items ORDER BY id LIMIT 1", [], |row| {
                row.get(0)
            })
            .expect("read row");

        assert_eq!(value, "new");
        assert_eq!(last_index, 1);
        assert_eq!(last_offset, 0);
    }
}

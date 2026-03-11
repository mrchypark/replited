use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::debug;
use log::error;
use log::info;
use log::warn;
use rusqlite::Connection;
use tempfile::NamedTempFile;

use crate::base::parent_dir;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::base::decompressed_data;
use crate::error::Error;
use crate::error::Result;
use crate::storage::RestoreRequestCostSnapshot;
use crate::storage::RestoreRequestCostStats;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALHeader};
use crate::storage::StorageClient;

mod follow;
mod manifest_plan;
use manifest_plan::{
    plan_manifest_restore, ManifestPlannerInput, ManifestPlannerWalPack, ManifestRestoreWalObject,
};

struct Restore {
    db: String,
    config: Vec<StorageConfig>,
    options: RestoreOptions,
}

#[derive(Debug, Clone)]
struct DiscoveredRestorePlan {
    snapshot: crate::storage::SnapshotInfo,
    snapshot_key: String,
    wal_objects: Vec<ManifestRestoreWalObject>,
}

#[derive(Debug, Clone)]
struct SelectedRestorePlan {
    storage_config: StorageConfig,
    plan: DiscoveredRestorePlan,
}

#[async_trait]
trait ManifestRestoreSource {
    async fn discover_manifest_restore_plan(
        &self,
        db: &str,
        config: &StorageConfig,
        request_costs: RestoreRequestCostStats,
    ) -> Result<Option<DiscoveredRestorePlan>>;
}

struct StorageManifestRestoreSource;

fn restore_plan_progress(plan: &DiscoveredRestorePlan) -> (u64, u64) {
    plan.wal_objects
        .last()
        .map(|wal| (wal.index, wal.end_offset))
        .unwrap_or((plan.snapshot.index, plan.snapshot.offset))
}

fn resume_position_from_local_wal(
    wal_objects: &[ManifestRestoreWalObject],
    current_offset: u64,
) -> Option<(u64, u64)> {
    wal_objects
        .iter()
        .filter(|wal| wal.end_offset == current_offset)
        .map(|wal| (wal.index, wal.end_offset))
        .min_by_key(|(index, _end_offset)| *index)
}

fn format_restore_request_cost_summary(
    path: &str,
    request_costs: RestoreRequestCostSnapshot,
) -> String {
    format!(
        "restore_request_cost path={path} latest_pointer_gets={} generation_manifest_gets={} object_gets={} list_calls={}",
        request_costs.latest_pointer_gets,
        request_costs.generation_manifest_gets,
        request_costs.object_gets,
        request_costs.list_calls,
    )
}

#[async_trait]
impl ManifestRestoreSource for StorageManifestRestoreSource {
    async fn discover_manifest_restore_plan(
        &self,
        db: &str,
        config: &StorageConfig,
        request_costs: RestoreRequestCostStats,
    ) -> Result<Option<DiscoveredRestorePlan>> {
        let client = StorageClient::try_create_with_restore_request_costs(
            db.to_string(),
            config.clone(),
            request_costs,
        )?;
        let metadata = match client.read_manifest_restore_inputs().await? {
            Some(metadata) => metadata,
            None => return Ok(None),
        };

        let (
            generation,
            manifest_id,
            lineage_id,
            base_snapshot_id,
            base_snapshot_sha256,
            base_snapshot,
            wal_packs,
        ) = metadata;

        let plan = plan_manifest_restore(&ManifestPlannerInput {
            generation,
            manifest_id,
            lineage_id,
            base_snapshot_id,
            base_snapshot_sha256,
            base_snapshot,
            wal_packs: wal_packs
                .into_iter()
                .map(
                    |(
                        start_lsn,
                        end_lsn,
                        object_key,
                        sha256,
                        size_bytes,
                        lineage_id,
                        base_snapshot_id,
                    )| ManifestPlannerWalPack {
                        start_lsn,
                        end_lsn,
                        object_key,
                        sha256,
                        size_bytes,
                        lineage_id,
                        base_snapshot_id,
                    },
                )
                .collect(),
        })?;

        Ok(Some(DiscoveredRestorePlan {
            snapshot: plan.snapshot,
            snapshot_key: plan.snapshot_key,
            wal_objects: plan.wal_objects,
        }))
    }
}

impl Restore {
    pub fn try_create(
        db: String,
        config: Vec<StorageConfig>,
        options: RestoreOptions,
    ) -> Result<Self> {
        Ok(Self {
            db,
            config,
            options,
        })
    }

    async fn decide_restore_plan(&self, limit: Option<DateTime<Utc>>) -> Result<SelectedRestorePlan> {
        self.decide_restore_plan_with_request_costs(limit, RestoreRequestCostStats::default())
            .await
    }

    async fn decide_restore_plan_with_request_costs(
        &self,
        limit: Option<DateTime<Utc>>,
        request_costs: RestoreRequestCostStats,
    ) -> Result<SelectedRestorePlan> {
        self.decide_restore_plan_with_sources(limit, &StorageManifestRestoreSource, request_costs)
            .await
    }

    async fn decide_restore_plan_with_sources<M: ManifestRestoreSource + Sync>(
        &self,
        limit: Option<DateTime<Utc>>,
        manifest_source: &M,
        request_costs: RestoreRequestCostStats,
    ) -> Result<SelectedRestorePlan> {
        if limit.is_some() {
            return Err(Error::InvalidArg(
                "timestamp-bounded restore is unsupported in manifest-only restore".to_string(),
            ));
        }

        let mut latest_manifest_plan: Option<SelectedRestorePlan> = None;

        for config in &self.config {
            let plan = match manifest_source
                .discover_manifest_restore_plan(&self.db, config, request_costs.clone())
                .await?
            {
                Some(plan) => plan,
                None => continue,
            };

            match &latest_manifest_plan {
                Some(current)
                    if plan.snapshot.generation < current.plan.snapshot.generation => {}
                Some(current)
                    if plan.snapshot.generation == current.plan.snapshot.generation
                        && restore_plan_progress(&plan) <= restore_plan_progress(&current.plan) => {}
                _ => {
                    latest_manifest_plan = Some(SelectedRestorePlan {
                        storage_config: config.clone(),
                        plan,
                    })
                }
            }
        }

        latest_manifest_plan.ok_or_else(|| Error::NoSnapshotError(self.db.clone()))
    }

    pub async fn run(&self) -> Result<Option<crate::database::WalGenerationPos>> {
        // Ensure output path does not already exist.
        if !self.options.follow {
            for path in [
                self.options.output.clone(),
                format!("{}-wal", self.options.output),
                format!("{}-shm", self.options.output),
            ] {
                if fs::exists(&path)? {
                    return Err(Error::OverwriteDbError("cannot overwrite exist db"));
                }
            }
        }

        let limit = parse_limit_timestamp(&self.options.timestamp)?;
        let request_costs = RestoreRequestCostStats::default();

        let selected_restore_plan = self
            .decide_restore_plan_with_request_costs(limit, request_costs.clone())
            .await?;
        let latest_restore_plan = selected_restore_plan.plan;
        let client = StorageClient::try_create_with_restore_request_costs(
            self.db.clone(),
            selected_restore_plan.storage_config,
            request_costs.clone(),
        )?;

        // Determine target path
        let (target_path, _temp_file) = if self.options.follow {
            let dir = output_parent_dir(&self.options.output)?;
            fs::create_dir_all(&dir)?;
            (self.options.output.clone(), None)
        } else {
            // NOTE: Create temp file in the output directory to avoid EXDEV when
            // the system temp directory is on a different mount (common in Docker).
            let output_dir = output_parent_dir(&self.options.output)?;
            fs::create_dir_all(&output_dir)?;
            let temp_file = NamedTempFile::new_in(output_dir)?;
            let temp_file_name = temp_file.path().to_string_lossy().to_string();
            (temp_file_name, Some(temp_file))
        };

        let mut last_index = 0;
        let mut last_offset = 0;
        let mut resume = false;

        if self.options.follow && fs::exists(&target_path)? {
            info!("db {target_path} exists, trying to resume...");
            // Try to determine last_index and last_offset from WAL file
            let wal_path = format!("{target_path}-wal");
            if let Ok(metadata) = fs::metadata(&wal_path) {
                let current_offset = metadata.len();
                let mut valid = false;

                // Validate WAL file integrity
                if current_offset > WAL_HEADER_SIZE {
                    match WALHeader::read(&wal_path) {
                        Ok(header) => {
                            let frame_size = WAL_FRAME_HEADER_SIZE + header.page_size;
                            let remainder = (current_offset - WAL_HEADER_SIZE) % frame_size;
                            if remainder == 0 {
                                valid = true;
                            } else {
                                warn!(
                                    "WAL file {wal_path} has partial frame (remainder {remainder}), deleting it"
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to read WAL header: {e:?}, deleting WAL file");
                        }
                    }
                } else if current_offset > 0 {
                    warn!("WAL file {wal_path} is too small for header, deleting it");
                } else {
                    // Empty file is valid (start from 0)
                    valid = true;
                }

                if !valid {
                    let _ = fs::remove_file(&wal_path);
                    let shm_path = format!("{target_path}-shm");
                    let _ = fs::remove_file(&shm_path);
                    // last_index and last_offset remain 0
                } else {
                    // The local WAL file only stores one active WAL index at a time.
                    // Match the file length against exact manifest object end offsets so
                    // later indexes with offset 0 are not accidentally treated as applied.
                    let (resume_index, resume_offset) = resume_position_from_local_wal(
                        &latest_restore_plan.wal_objects,
                        current_offset,
                    )
                    .unwrap_or((0, current_offset));
                    last_index = resume_index;
                    last_offset = resume_offset;

                    info!("resuming from index {last_index}, offset {current_offset}");
                    resume = true;
                }
            }
        }

        if !resume {
            self.restore_snapshot_from_key(&client, &latest_restore_plan.snapshot_key, &target_path)
                .await?;
        }

        // apply wal frames
        let (new_last_index, new_last_offset, keepalive_conn) = self
            .apply_wal_objects(
                &client,
                &latest_restore_plan.wal_objects,
                &target_path,
                last_index,
                last_offset,
                self.options.follow,
            )
            .await?;

        last_index = new_last_index;
        last_offset = new_last_offset;

        // If follow mode, ensure we have a keepalive connection (either returned or new)
        let _keepalive_connection = if self.options.follow {
            if let Some(conn) = keepalive_conn {
                Some(conn)
            } else {
                // Should not happen if apply_wal_frames respects keep_alive,
                // but if wal_segments was empty, it returns None.
                let conn = Connection::open(&target_path)?;
                Some(conn)
            }
        } else {
            None
        };

        if !self.options.follow {
            // Move DB + WAL + SHM together so the output DB can see unapplied WAL frames.
            let temp_wal_path = format!("{target_path}-wal");
            let temp_shm_path = format!("{target_path}-shm");
            let out_wal_path = format!("{}-wal", self.options.output);
            let out_shm_path = format!("{}-shm", self.options.output);

            // WAL/SHM may not exist (e.g. no WAL segments). Move what exists.
            if fs::exists(&temp_wal_path)? {
                fs::rename(&temp_wal_path, &out_wal_path)?;
            }
            if fs::exists(&temp_shm_path)? {
                fs::rename(&temp_shm_path, &out_shm_path)?;
            }

            // Rename the temp DB last to treat it as the "commit" step.
            fs::rename(&target_path, &self.options.output)?;

            // Some tooling expects the classic SQLite "DB + WAL + SHM" file set.
            // Create an empty SHM file if a WAL exists but no SHM was created during restore.
            // SQLite will initialize/resize it on first open.
            if fs::exists(&out_wal_path)? && !fs::exists(&out_shm_path)? {
                let _ = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(&out_shm_path)?;
            }
        }

        info!(
            "restore db {} to {} success",
            self.options.db, self.options.output
        );
        info!(
            "{}",
            format_restore_request_cost_summary("manifest", request_costs.snapshot())
        );

        if self.options.follow {
            drop(_keepalive_connection);
            self.follow_loop(
                client,
                latest_restore_plan.snapshot,
                last_index,
                last_offset,
            )
            .await?;

            // follow mode does not return a position (it keeps running)
            return Ok(None);
        }

        let pos = crate::database::WalGenerationPos {
            generation: latest_restore_plan.snapshot.generation.clone(),
            index: last_index,
            offset: last_offset,
        };

        Ok(Some(pos))
    }

    async fn restore_snapshot_from_key(
        &self,
        client: &StorageClient,
        snapshot_key: &str,
        path: &str,
    ) -> Result<()> {
        let compressed_data = client.read_object_by_key(snapshot_key).await?;
        let decompressed_data = decompressed_data(compressed_data)?;
        debug!(
            "restore_snapshot_from_key: decompressed data size: {}",
            decompressed_data.len()
        );

        let wal_path = format!("{path}-wal");
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(format!("{path}-shm"));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("{path}.tmp"))?;

        file.write_all(&decompressed_data)?;
        file.sync_all()?;

        let temp_path = format!("{path}.tmp");
        {
            let conn = Connection::open(&temp_path)?;
            conn.pragma_query(None, "integrity_check", |row| {
                let s: String = row.get(0)?;
                if s != "ok" {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(11),
                        Some(format!("Integrity check failed: {s}")),
                    ));
                }
                Ok(())
            })?;
        }

        fs::rename(temp_path, path)?;
        Ok(())
    }

    async fn apply_wal_objects(
        &self,
        client: &StorageClient,
        wal_objects: &[ManifestRestoreWalObject],
        db_path: &str,
        mut last_index: u64,
        mut last_offset: u64,
        keep_alive: bool,
    ) -> Result<(u64, u64, Option<Connection>)> {
        let wal_file_name = format!("{db_path}-wal");

        for wal_object in wal_objects {
            if wal_object.index < last_index
                || (wal_object.index == last_index && wal_object.end_offset <= last_offset)
            {
                continue;
            }

            let compressed_data = client.read_object_by_key(&wal_object.object_key).await?;
            let data = decompressed_data(compressed_data)?;
            let should_truncate = wal_object.index > last_index;

            if should_truncate && fs::metadata(&wal_file_name).is_ok() {
                let connection = Connection::open(db_path)?;
                if let Err(e) =
                    connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
                {
                    error!("truncate checkpoint failed before new generation: {e:?}");
                    return Err(e.into());
                }
            }

            let mut wal_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(should_truncate)
                .open(&wal_file_name)?;

            wal_file.set_len(wal_object.offset)?;
            wal_file.seek(SeekFrom::Start(wal_object.offset))?;
            wal_file.write_all(&data)?;
            wal_file.sync_all()?;

            last_index = wal_object.index;
            last_offset = wal_object.offset + data.len() as u64;
        }

        let keepalive_connection = if keep_alive {
            Some(Connection::open(db_path)?)
        } else {
            None
        };

        Ok((last_index, last_offset, keepalive_connection))
    }
}

fn parse_limit_timestamp(timestamp: &str) -> Result<Option<DateTime<Utc>>> {
    if timestamp.trim().is_empty() {
        return Ok(None);
    }

    let parsed = DateTime::parse_from_rfc3339(timestamp)
        .map_err(|err| Error::InvalidArg(format!("invalid --timestamp {timestamp:?}: {err}")))?;
    Ok(Some(parsed.with_timezone(&Utc)))
}

fn output_parent_dir(path: &str) -> Result<String> {
    let dir = parent_dir(path).ok_or_else(|| Error::InvalidPath(format!("invalid path {path}")))?;
    if dir.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(dir)
    }
}

pub async fn run_restore(
    config: &DbConfig,
    options: &RestoreOptions,
) -> Result<Option<crate::database::WalGenerationPos>> {
    let restore =
        Restore::try_create(config.db.clone(), config.replicate.clone(), options.clone())?;

    restore.run().await
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use async_trait::async_trait;
    use chrono::Utc;

    use super::{
        output_parent_dir, parse_limit_timestamp, format_restore_request_cost_summary,
        restore_plan_progress, resume_position_from_local_wal, DiscoveredRestorePlan, Restore,
        ManifestRestoreSource, SelectedRestorePlan,
    };
    use super::manifest_plan::ManifestRestoreWalObject;
    use crate::base::Generation;
    use crate::config::RestoreOptions;
    use crate::config::StorageConfig;
    use crate::config::StorageFsConfig;
    use crate::config::StorageParams;
    use crate::error::Error;
    use crate::error::Result;
    use crate::storage::RestoreRequestCostSnapshot;
    use crate::storage::RestoreRequestCostStats;
    use crate::storage::SnapshotInfo;

    #[test]
    fn parse_limit_timestamp_accepts_empty() {
        assert!(parse_limit_timestamp("").unwrap().is_none());
    }

    #[test]
    fn parse_limit_timestamp_rejects_invalid() {
        let err = parse_limit_timestamp("not-a-timestamp").unwrap_err();
        assert_eq!(err.code(), crate::error::Error::INVALID_ARG);
    }

    #[test]
    fn output_parent_dir_defaults_to_current_directory() {
        assert_eq!(output_parent_dir("data.db").unwrap(), ".");
    }

    #[derive(Clone)]
    struct RestoreInfoSpec {
        generation: String,
        index: u64,
        snapshot_offset: u64,
        wal_objects: Vec<(u64, u64, u64, String)>,
    }

    #[derive(Clone)]
    struct FakeManifestSource {
        answers: HashMap<String, Result<Option<RestoreInfoSpec>>>,
    }

    #[async_trait]
    impl ManifestRestoreSource for FakeManifestSource {
        async fn discover_manifest_restore_plan(
            &self,
            db: &str,
            config: &StorageConfig,
            _request_costs: RestoreRequestCostStats,
        ) -> Result<Option<DiscoveredRestorePlan>> {
            let key = format!("{db}:{}", storage_root(config));
            match self.answers.get(&key).cloned().unwrap_or(Ok(None))? {
                Some(spec) => Ok(Some(sample_restore_plan(
                    Generation::try_create(&spec.generation).expect("generation"),
                    spec.index,
                    spec.snapshot_offset,
                    spec.wal_objects,
                ))),
                None => Ok(None),
            }
        }
    }

    fn storage_root(config: &StorageConfig) -> String {
        match &config.params {
            StorageParams::Fs(fs) => fs.root.clone(),
            other => panic!("unexpected storage params: {other:?}"),
        }
    }

    fn sample_restore_plan(
        generation: Generation,
        index: u64,
        snapshot_offset: u64,
        wal_objects: Vec<(u64, u64, u64, String)>,
    ) -> DiscoveredRestorePlan {
        let wal_objects = wal_objects
            .into_iter()
            .map(|(index, offset, end_offset, object_key)| ManifestRestoreWalObject {
                index,
                offset,
                end_offset,
                object_key,
            })
            .collect::<Vec<_>>();
        DiscoveredRestorePlan {
            snapshot: SnapshotInfo {
                generation,
                index,
                offset: snapshot_offset,
                size: 1024,
                created_at: Utc::now(),
            },
            snapshot_key: "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/snapshots/0000000001_0000000000.snapshot.zst".to_string(),
            wal_objects,
        }
    }

    fn sample_storage_config(root: &str) -> StorageConfig {
        StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: root.to_string(),
            })),
        }
    }

    fn sample_restore() -> Restore {
        sample_restore_with_timestamp("")
    }

    fn sample_restore_with_timestamp(timestamp: &str) -> Restore {
        Restore::try_create(
            "data.db".to_string(),
            vec![
                sample_storage_config("/tmp/replited-a"),
                sample_storage_config("/tmp/replited-b"),
            ],
            RestoreOptions {
                db: "data.db".to_string(),
                output: "/tmp/out.db".to_string(),
                follow: false,
                interval: 1,
                timestamp: timestamp.to_string(),
            },
        )
        .expect("restore")
    }

    #[tokio::test]
    async fn restore_source_selects_latest_generation_across_configs() {
        let restore = sample_restore();
        let newer = Generation::try_create("019c3e53aea57afbbddfe5ebc2272e22").expect("newer");
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([
                ("data.db:/tmp/replited-a".to_string(), Ok(None)),
                (
                    "data.db:/tmp/replited-b".to_string(),
                    Ok(Some(RestoreInfoSpec {
                        generation: newer.as_str().to_string(),
                        index: 0,
                        snapshot_offset: 0,
                        wal_objects: vec![],
                    })),
                ),
            ]),
        };

        let latest = restore
            .decide_restore_plan_with_sources(
                None,
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect("latest restore plan");

        assert_eq!(latest.plan.snapshot.generation, newer);
        assert_eq!(storage_root(&latest.storage_config), "/tmp/replited-b");
    }

    #[tokio::test]
    async fn restore_source_hard_fails_when_all_configs_absent() {
        let restore = sample_restore();
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([
                ("data.db:/tmp/replited-a".to_string(), Ok(None)),
                ("data.db:/tmp/replited-b".to_string(), Ok(None)),
            ]),
        };

        let err = restore
            .decide_restore_plan_with_sources(
                None,
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect_err("manifest-only restore should fail when metadata is absent");

        assert_eq!(err.code(), Error::NO_SNAPSHOT_ERROR);
    }

    #[tokio::test]
    async fn restore_manifest_present_with_timestamp_hard_fails() {
        let restore = sample_restore_with_timestamp("2026-03-10T00:00:00Z");
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([(
                "data.db:/tmp/replited-a".to_string(),
                Ok(Some(RestoreInfoSpec {
                    generation: "019c3e53aea47afbbddfe5ebc2272e22".to_string(),
                    index: 0,
                    snapshot_offset: 0,
                    wal_objects: vec![],
                })),
            )]),
        };

        let err = restore
            .decide_restore_plan_with_sources(
                parse_limit_timestamp("2026-03-10T00:00:00Z").expect("timestamp"),
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect_err("manifest metadata with timestamp should hard fail");

        assert_eq!(err.code(), Error::INVALID_ARG);
    }

    #[tokio::test]
    async fn restore_manifest_absent_with_timestamp_hard_fails() {
        let restore = sample_restore_with_timestamp("2026-03-10T00:00:00Z");
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([
                ("data.db:/tmp/replited-a".to_string(), Ok(None)),
                ("data.db:/tmp/replited-b".to_string(), Ok(None)),
            ]),
        };

        let err = restore
            .decide_restore_plan_with_sources(
                parse_limit_timestamp("2026-03-10T00:00:00Z").expect("timestamp"),
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect_err("timestamp restore should fail in manifest-only mode");

        assert_eq!(err.code(), Error::INVALID_ARG);
    }

    #[tokio::test]
    async fn restore_manifest_malformed_metadata_hard_fails() {
        let restore = sample_restore();
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([(
                "data.db:/tmp/replited-a".to_string(),
                Err(Error::StorageError("bad manifest metadata".to_string())),
            )]),
        };

        let err = restore
            .decide_restore_plan_with_sources(
                None,
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect_err("malformed metadata should hard fail");

        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn restore_manifest_present_uses_manifest_plan() {
        let restore = sample_restore();
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([(
                "data.db:/tmp/replited-a".to_string(),
                Ok(Some(RestoreInfoSpec {
                    generation: "019c3e53aea47afbbddfe5ebc2272e22".to_string(),
                    index: 5,
                    snapshot_offset: 0,
                    wal_objects: vec![],
                })),
            )]),
        };

        let latest = restore
            .decide_restore_plan_with_sources(
                None,
                &manifest_source,
                RestoreRequestCostStats::default(),
            )
            .await
            .expect("manifest restore plan");

        assert_eq!(latest.plan.snapshot.index, 5);
        assert!(latest.plan.snapshot_key.ends_with(".snapshot.zst"));
    }

    #[test]
    fn selected_restore_plan_can_retain_exact_manifest_object_keys() {
        let selected = SelectedRestorePlan {
            storage_config: sample_storage_config("/tmp/replited-a"),
            plan: DiscoveredRestorePlan {
                snapshot: SnapshotInfo {
                generation: Generation::try_create(
                        "019c3e53aea47afbbddfe5ebc2272e22",
                    )
                    .expect("generation"),
                    index: 7,
                    offset: 0,
                    size: 0,
                    created_at: Utc::now(),
                },
                snapshot_key: "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/snapshots/0000000007_0000000000.snapshot.zst".to_string(),
                wal_objects: vec![ManifestRestoreWalObject {
                    index: 7,
                    offset: 0,
                    end_offset: 4096,
                    object_key: "db.db/generations/019c3e53aea47afbbddfe5ebc2272e22/wal/0000000007_0000000000.wal.zst".to_string(),
                }],
            },
        };

        assert!(selected.plan.snapshot_key.ends_with(".snapshot.zst"));
        assert_eq!(selected.plan.wal_objects.len(), 1);
        assert!(selected.plan.wal_objects[0].object_key.ends_with(".wal.zst"));
    }

    #[test]
    fn selected_restore_plan_separates_storage_choice_from_plan() {
        let selected = SelectedRestorePlan {
            storage_config: sample_storage_config("/tmp/replited-a"),
            plan: sample_restore_plan(
                Generation::try_create("019c3e53aea47afbbddfe5ebc2272e22").expect("generation"),
                7,
                0,
                vec![],
            ),
        };

        assert_eq!(storage_root(&selected.storage_config), "/tmp/replited-a");
        assert_eq!(selected.plan.snapshot.index, 7);
    }

    #[test]
    fn restore_request_cost_summary_line_is_stable() {
        let summary = format_restore_request_cost_summary(
            "manifest",
            RestoreRequestCostSnapshot {
                latest_pointer_gets: 1,
                generation_manifest_gets: 2,
                object_gets: 3,
                list_calls: 0,
            },
        );

        assert_eq!(
            summary,
            "restore_request_cost path=manifest latest_pointer_gets=1 generation_manifest_gets=2 object_gets=3 list_calls=0"
        );
    }

    #[tokio::test]
    async fn restore_source_prefers_more_advanced_manifest_with_same_generation() {
        let restore = sample_restore();
        let generation = "019c3e53aea57afbbddfe5ebc2272e22".to_string();
        let manifest_source = FakeManifestSource {
            answers: HashMap::from([
                (
                    "data.db:/tmp/replited-a".to_string(),
                    Ok(Some(RestoreInfoSpec {
                        generation: generation.clone(),
                        index: 1,
                        snapshot_offset: 0,
                        wal_objects: vec![(1, 0, 1024, "a-1".to_string())],
                    })),
                ),
                (
                    "data.db:/tmp/replited-b".to_string(),
                    Ok(Some(RestoreInfoSpec {
                        generation,
                        index: 1,
                        snapshot_offset: 0,
                        wal_objects: vec![(1, 0, 1024, "b-1".to_string()), (1, 1024, 2048, "b-2".to_string())],
                    })),
                ),
            ]),
        };

        let latest = restore
            .decide_restore_plan_with_sources(None, &manifest_source, RestoreRequestCostStats::default())
            .await
            .expect("latest restore plan");

        assert_eq!(storage_root(&latest.storage_config), "/tmp/replited-b");
        assert_eq!(restore_plan_progress(&latest.plan), (1, 2048));
    }

    #[test]
    fn resume_position_from_local_wal_prefers_exact_end_match_over_later_index_offset_zero() {
        let wal_objects = vec![
            ManifestRestoreWalObject {
                index: 1,
                offset: 0,
                end_offset: 1024,
                object_key: "one-a".to_string(),
            },
            ManifestRestoreWalObject {
                index: 1,
                offset: 1024,
                end_offset: 2048,
                object_key: "one-b".to_string(),
            },
            ManifestRestoreWalObject {
                index: 2,
                offset: 0,
                end_offset: 1024,
                object_key: "two-a".to_string(),
            },
        ];

        assert_eq!(resume_position_from_local_wal(&wal_objects, 2048), Some((1, 2048)));
    }

    #[test]
    fn resume_position_from_local_wal_prefers_less_advanced_index_when_end_offset_is_ambiguous() {
        let wal_objects = vec![
            ManifestRestoreWalObject {
                index: 1,
                offset: 0,
                end_offset: 1024,
                object_key: "one-a".to_string(),
            },
            ManifestRestoreWalObject {
                index: 2,
                offset: 0,
                end_offset: 1024,
                object_key: "two-a".to_string(),
            },
        ];

        assert_eq!(resume_position_from_local_wal(&wal_objects, 1024), Some((1, 1024)));
    }
}

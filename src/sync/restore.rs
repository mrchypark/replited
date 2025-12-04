use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use log::debug;
use log::error;
use log::info;
use log::warn;
use rusqlite::Connection;
use tempfile::NamedTempFile;

use crate::base::decompressed_data;
use crate::base::parent_dir;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALHeader};
use crate::storage::RestoreInfo;
use crate::storage::RestoreWalSegments;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;
use std::io::{Seek, SeekFrom};

static WAL_CHECKPOINT_PASSIVE: &str = "PRAGMA wal_checkpoint(PASSIVE);";

struct Restore {
    db: String,
    config: Vec<StorageConfig>,
    options: RestoreOptions,
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

    pub async fn decide_restore_info(
        &self,
        limit: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Option<(RestoreInfo, StorageClient)>> {
        let mut latest_restore_info: Option<(RestoreInfo, StorageClient)> = None;

        for config in &self.config {
            let client = StorageClient::try_create(self.db.clone(), config.clone())?;
            let restore_info = match client.restore_info(limit).await? {
                Some(snapshot_into) => snapshot_into,
                None => continue,
            };
            match &latest_restore_info {
                Some(ls) => {
                    if restore_info.snapshot.generation > ls.0.snapshot.generation {
                        latest_restore_info = Some((restore_info, client));
                    }
                }
                None => {
                    latest_restore_info = Some((restore_info, client));
                }
            }
        }

        Ok(latest_restore_info)
    }

    async fn restore_snapshot(
        &self,
        client: &StorageClient,
        snapshot: &SnapshotInfo,
        path: &str,
    ) -> Result<()> {
        let compressed_data = client.read_snapshot(snapshot).await?;
        let decompressed_data = decompressed_data(compressed_data)?;
        println!(
            "restore_snapshot: decompressed data size: {}",
            decompressed_data.len()
        );

        // Clean up existing WAL and SHM files to prevent corruption
        let wal_path = format!("{}-wal", path);
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(format!("{}-shm", path));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("{}.tmp", path))?;

        file.write_all(&decompressed_data)?;
        file.sync_all()?;

        // Verify integrity of the downloaded snapshot
        let temp_path = format!("{}.tmp", path);
        {
            let conn = Connection::open(&temp_path)?;
            conn.pragma_query(None, "integrity_check", |row| {
                let s: String = row.get(0)?;
                if s != "ok" {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(11), // SQLITE_CORRUPT
                        Some(format!("Integrity check failed: {}", s)),
                    ));
                }
                Ok(())
            })?;
        }

        fs::rename(temp_path, path)?;

        Ok(())
    }

    async fn apply_wal_frames(
        &self,
        client: &StorageClient,
        snapshot: &SnapshotInfo,
        wal_segments: &RestoreWalSegments,
        db_path: &str,
        mut last_index: u64,
        keep_alive: bool,
    ) -> Result<(u64, u64, Option<Connection>)> {
        debug!(
            "restore db {} apply wal segments: {:?}",
            self.db, wal_segments
        );
        let wal_file_name = format!("{}-wal", db_path);
        let mut last_offset = 0;

        for (index, offsets) in wal_segments {
            let mut wal_decompressed_data = Vec::new();
            let mut start_offset: Option<u64> = None;

            for offset in offsets {
                let wal_segment = WalSegmentInfo {
                    generation: snapshot.generation.clone(),
                    index: *index,
                    offset: *offset,
                    size: 0,
                    created_at: chrono::Utc::now(),
                };

                // Filtering logic based on last_index and last_offset
                if *index < last_index {
                    continue;
                } else if *index == last_index && *offset < last_offset {
                    continue;
                }

                if start_offset.is_none() {
                    start_offset = Some(*offset);
                }

                let compressed_data = client.read_wal_segment(&wal_segment).await?;
                let data = decompressed_data(compressed_data)?;
                wal_decompressed_data.extend_from_slice(&data);
                last_offset = offset + data.len() as u64;
            }

            // prepare db wal before open db connection
            let should_truncate = *index > last_index;

            if should_truncate {
                if fs::metadata(&wal_file_name).is_ok() {
                    // If we are about to truncate the WAL (new generation), we must ensure
                    // the existing WAL is fully checkpointed into the DB.
                    // Otherwise we lose data.
                    let connection = Connection::open(db_path)?;
                    if let Err(e) =
                        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
                    {
                        error!("truncate checkpoint failed before new generation: {:?}", e);
                        // We continue, hoping for the best? Or fail?
                        // If checkpoint fails, we probably lose data.
                        return Err(e.into());
                    }
                }
            }

            if let Ok(metadata) = fs::metadata(&wal_file_name) {
                println!(
                    "WAL file {} size: {}, should_truncate: {}",
                    wal_file_name,
                    metadata.len(),
                    should_truncate
                );
            } else {
                println!(
                    "WAL file {} does not exist, should_truncate: {}",
                    wal_file_name, should_truncate
                );
            }

            let mut wal_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(should_truncate)
                .open(&wal_file_name)?;

            if let Some(start) = start_offset {
                wal_file.seek(SeekFrom::Start(start))?;
            } else {
                // If no segments applied, maybe we shouldn't write?
                // But we might have truncated?
                // If should_truncate is true, we truncated to 0.
                // So seek(0) is fine.
                // If should_truncate is false, and no segments.
                // We do nothing.
                if !wal_decompressed_data.is_empty() {
                    wal_file.seek(SeekFrom::End(0))?; // Fallback? Or Error?
                }
            }

            if !wal_decompressed_data.is_empty() {
                wal_file.write_all(&wal_decompressed_data)?;
                wal_file.sync_all()?;
            }

            last_index = *index;

            // Force SHM deletion to trigger recovery and update mxFrame
            // let shm_path = format!("{}-shm", db_path);
            // let _ = fs::remove_file(&shm_path);

            if keep_alive {
                let connection = Connection::open(db_path)?;
                return Ok((last_index, last_offset, Some(connection)));
            }
            // connection is dropped here if not returned
        }

        Ok((last_index, last_offset, None))
    }

    async fn follow_loop(
        &self,
        mut current_client: StorageClient,
        mut current_snapshot: SnapshotInfo,
        mut last_index: u64,
        mut last_offset: u64,
    ) -> Result<()> {
        println!("entering follow mode...");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(self.options.interval)).await;

            // 1. Check for new WAL segments in current generation
            let wal_segments = current_client
                .wal_segments(current_snapshot.generation.as_str())
                .await?;

            let mut new_segments = Vec::new();
            for segment in wal_segments {
                if segment.index > last_index {
                    new_segments.push(segment);
                } else if segment.index == last_index && segment.offset >= last_offset {
                    new_segments.push(segment);
                }
            }

            if !new_segments.is_empty() {
                // Sort and group segments
                new_segments.sort_by(|a, b| {
                    let ordering = a.index.partial_cmp(&b.index).unwrap();
                    if ordering.is_eq() {
                        a.offset.partial_cmp(&b.offset).unwrap()
                    } else {
                        ordering
                    }
                });

                let mut restore_wal_segments: std::collections::BTreeMap<u64, Vec<u64>> =
                    std::collections::BTreeMap::new();
                for segment in new_segments {
                    restore_wal_segments
                        .entry(segment.index)
                        .or_default()
                        .push(segment.offset);
                }
                let restore_wal_segments: RestoreWalSegments =
                    restore_wal_segments.into_iter().collect();

                let (new_last_index, new_last_offset, _) = self
                    .apply_wal_frames(
                        &current_client,
                        &current_snapshot,
                        &restore_wal_segments,
                        &self.options.output,
                        last_index,
                        false,
                    )
                    .await?;

                last_index = new_last_index;
                last_offset = new_last_offset;
                println!(
                    "applied updates up to index {}, offset {}",
                    last_index, last_offset
                );
            }

            // 2. Check for new generations (restarts)
            // This is a simplified check. In a real scenario, we might need to handle generation switch more robustly.
            // For now, we check if a newer snapshot exists.
            if let Some((latest_info, client)) = self.decide_restore_info(None).await? {
                if latest_info.snapshot.generation > current_snapshot.generation {
                    println!(
                        "detected new generation: {:?}",
                        latest_info.snapshot.generation
                    );
                    // Switch to new generation
                    // Note: This might require re-applying the snapshot if it's a full snapshot.
                    // For simplicity in this iteration, we assume we can just switch and continue applying WALs
                    // IF the new generation is compatible. However, usually a new generation means a new snapshot.
                    // So we should probably re-restore the snapshot?
                    // But that would overwrite the DB.
                    // Correct approach for "follow" across generations:
                    // If new generation starts with a snapshot, we might need to apply it?
                    // But applying a full snapshot on an active DB is dangerous/impossible if it's open.
                    // "Follow" mode usually implies applying WALs.
                    // If the new generation has a base snapshot, we might be able to skip it if we have the data?
                    // Let's assume for now we just switch context and look for WALs.

                    current_client = client;
                    current_snapshot = latest_info.snapshot;

                    // If there are WAL segments in the new generation, apply them.
                    // If there are WAL segments in the new generation, apply them.
                    let (new_last_index, new_last_offset, _) = self
                        .apply_wal_frames(
                            &current_client,
                            &current_snapshot,
                            &latest_info.wal_segments,
                            &self.options.output,
                            current_snapshot.offset, // Start fresh for new generation
                            false,
                        )
                        .await?;
                    last_index = new_last_index;
                    last_offset = new_last_offset;
                    println!("switched to new generation and applied updates");
                }
            }
        }
    }

    pub async fn run(&self) -> Result<Option<crate::database::WalGenerationPos>> {
        // Ensure output path does not already exist.
        if fs::exists(&self.options.output)? {
            if !self.options.follow {
                println!("db {} already exists but cannot overwrite", self.db);
                return Err(Error::OverwriteDbError("cannot overwrite exist db"));
            }
        }

        let limit = if self.options.timestamp.is_empty() {
            None
        } else {
            Some(
                chrono::DateTime::parse_from_rfc3339(&self.options.timestamp)
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            )
        };

        let (latest_restore_info, client) = match self.decide_restore_info(limit).await? {
            Some(latest_restore_info) => latest_restore_info,
            None => {
                debug!("cannot find snapshot");
                return Err(Error::NoSnapshotError(self.db.clone()));
            }
        };

        // Determine target path
        let (target_path, _temp_file) = if self.options.follow {
            let dir = parent_dir(&self.options.output).unwrap();
            fs::create_dir_all(&dir)?;
            (self.options.output.clone(), None)
        } else {
            let temp_file = NamedTempFile::new()?;
            let temp_file_name = temp_file.path().to_str().unwrap().to_string();
            let dir = parent_dir(&temp_file_name).unwrap();
            fs::create_dir_all(&dir)?;
            (temp_file_name, Some(temp_file))
        };

        let mut last_index = 0;
        let mut resume = false;

        if self.options.follow && fs::exists(&target_path)? {
            info!("db {} exists, trying to resume...", target_path);
            // Try to determine last_index and last_offset from WAL file
            let wal_path = format!("{}-wal", target_path);
            if let Ok(metadata) = fs::metadata(&wal_path) {
                let current_offset = metadata.len();
                let mut valid = false;

                // Validate WAL file integrity
                if current_offset > WAL_HEADER_SIZE as u64 {
                    match WALHeader::read(&wal_path) {
                        Ok(header) => {
                            let frame_size = WAL_FRAME_HEADER_SIZE as u64 + header.page_size;
                            let remainder = (current_offset - WAL_HEADER_SIZE as u64) % frame_size;
                            if remainder == 0 {
                                valid = true;
                            } else {
                                warn!(
                                    "WAL file {} has partial frame (remainder {}), deleting it",
                                    wal_path, remainder
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to read WAL header: {:?}, deleting WAL file", e);
                        }
                    }
                } else if current_offset > 0 {
                    warn!("WAL file {} is too small for header, deleting it", wal_path);
                } else {
                    // Empty file is valid (start from 0)
                    valid = true;
                }

                if !valid {
                    let _ = fs::remove_file(&wal_path);
                    let shm_path = format!("{}-shm", target_path);
                    let _ = fs::remove_file(&shm_path);
                    // last_index and last_offset remain 0
                } else {
                    // Find last_index based on offset
                    last_index = latest_restore_info
                        .wal_segments
                        .iter()
                        .filter(|(_index, offsets)| offsets.iter().any(|&o| o < current_offset))
                        .map(|(index, _)| *index)
                        .max()
                        .unwrap_or(0);

                    info!(
                        "resuming from index {}, offset {}",
                        last_index, current_offset
                    );
                    resume = true;
                }
            }
        }

        if !resume {
            // restore snapshot
            self.restore_snapshot(&client, &latest_restore_info.snapshot, &target_path)
                .await?;
        }

        // apply wal frames
        let (new_last_index, new_last_offset, keepalive_conn) = self
            .apply_wal_frames(
                &client,
                &latest_restore_info.snapshot,
                &latest_restore_info.wal_segments,
                &target_path,
                last_index,
                self.options.follow,
            )
            .await?;

        last_index = new_last_index;
        let last_offset = new_last_offset;

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
            // rename the temp file to output file
            fs::rename(&target_path, &self.options.output)?;
            // We don't need to move WAL/SHM because PASSIVE checkpoint moves data to DB,
            // and we don't care about WAL history for non-follow mode.
        }

        println!(
            "restore db {} to {} success",
            self.options.db, self.options.output
        );

        if self.options.follow {
            drop(_keepalive_connection);
            self.follow_loop(
                client,
                latest_restore_info.snapshot,
                last_index,
                last_offset,
            )
            .await?;

            // follow mode does not return a position (it keeps running)
            return Ok(None);
        }

        let pos = crate::database::WalGenerationPos {
            generation: latest_restore_info.snapshot.generation.clone(),
            index: last_index,
            offset: last_offset,
        };

        Ok(Some(pos))
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

use std::fs;

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
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALHeader};
use crate::storage::RestoreInfo;
use crate::storage::StorageClient;

mod apply_wal;
mod follow;
mod snapshot;

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

    pub async fn run(&self) -> Result<Option<crate::database::WalGenerationPos>> {
        // Ensure output path does not already exist.
        if fs::exists(&self.options.output)? && !self.options.follow {
            return Err(Error::OverwriteDbError("cannot overwrite exist db"));
        }

        let limit = parse_limit_timestamp(&self.options.timestamp)?;

        let (latest_restore_info, client) = match self.decide_restore_info(limit).await? {
            Some(latest_restore_info) => latest_restore_info,
            None => {
                debug!("cannot find snapshot");
                return Err(Error::NoSnapshotError(self.db.clone()));
            }
        };

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
                    // Find last_index based on offset
                    last_index = latest_restore_info
                        .wal_segments
                        .iter()
                        .filter(|(_index, offsets)| offsets.iter().any(|&o| o < current_offset))
                        .map(|(index, _)| *index)
                        .max()
                        .unwrap_or(0);

                    info!("resuming from index {last_index}, offset {current_offset}");
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

        info!(
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
    use super::{output_parent_dir, parse_limit_timestamp};

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
}

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{Seek, SeekFrom};

use log::debug;
use log::error;
use rusqlite::Connection;

use crate::base::decompressed_data;
use crate::error::Result;
use crate::storage::RestoreWalSegments;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;

impl super::Restore {
    pub(super) async fn apply_wal_frames(
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
        let wal_file_name = format!("{db_path}-wal");
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
                if *index < last_index || (*index == last_index && *offset < last_offset) {
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

            if should_truncate && fs::metadata(&wal_file_name).is_ok() {
                // If we are about to truncate the WAL (new generation), we must ensure
                // the existing WAL is fully checkpointed into the DB.
                // Otherwise we lose data.
                let connection = Connection::open(db_path)?;
                if let Err(e) =
                    connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
                {
                    error!("truncate checkpoint failed before new generation: {e:?}");
                    return Err(e.into());
                }
            }

            if let Ok(metadata) = fs::metadata(&wal_file_name) {
                debug!(
                    "WAL file {} size: {}, should_truncate: {}",
                    wal_file_name,
                    metadata.len(),
                    should_truncate
                );
            } else {
                debug!(
                    "WAL file {wal_file_name} does not exist, should_truncate: {should_truncate}"
                );
            }

            let mut wal_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(should_truncate)
                .open(&wal_file_name)?;

            if let Some(start) = start_offset {
                wal_file.seek(SeekFrom::Start(start))?;
            } else if !wal_decompressed_data.is_empty() {
                // If we didn't find any segments to apply but we have data, append.
                wal_file.seek(SeekFrom::End(0))?;
            }

            if !wal_decompressed_data.is_empty() {
                wal_file.write_all(&wal_decompressed_data)?;
                wal_file.sync_all()?;
            }

            last_index = *index;
        }

        let keepalive_connection = if keep_alive {
            Some(Connection::open(db_path)?)
        } else {
            None
        };

        Ok((last_index, last_offset, keepalive_connection))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::base::Generation;
    use crate::base::compress_buffer;
    use crate::config::RestoreOptions;
    use crate::config::StorageConfig;
    use crate::config::StorageFsConfig;
    use crate::config::StorageParams;
    use crate::storage::SnapshotInfo;
    use crate::storage::StorageClient;

    #[tokio::test]
    async fn apply_wal_frames_with_keepalive_processes_all_indexes() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("restore_target.db");
        let db_path_str = db_path.to_string_lossy().to_string();

        let storage_root = temp.path().join("storage");
        let client = StorageClient::try_create(
            "db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: storage_root.to_string_lossy().to_string(),
                })),
            },
        )
        .expect("storage client");

        let generation = Generation::new();
        let snapshot = SnapshotInfo {
            generation: generation.clone(),
            index: 0,
            offset: 0,
            size: 0,
            created_at: chrono::Utc::now(),
        };

        client
            .write_wal_segment(
                &crate::database::WalGenerationPos {
                    generation: generation.clone(),
                    index: 0,
                    offset: 0,
                },
                compress_buffer(b"first-segment").expect("compress first"),
            )
            .await
            .expect("write first segment");
        client
            .write_wal_segment(
                &crate::database::WalGenerationPos {
                    generation,
                    index: 1,
                    offset: 0,
                },
                compress_buffer(b"second-segment").expect("compress second"),
            )
            .await
            .expect("write second segment");

        let restore = super::super::Restore {
            db: "db.db".to_string(),
            config: Vec::new(),
            options: RestoreOptions {
                db: "db.db".to_string(),
                output: db_path_str.clone(),
                follow: false,
                interval: 1,
                timestamp: String::new(),
            },
        };

        let (last_index, _last_offset, keepalive_conn) = restore
            .apply_wal_frames(
                &client,
                &snapshot,
                &vec![(0, vec![0]), (1, vec![0])],
                &db_path_str,
                0,
                true,
            )
            .await
            .expect("apply wal frames");

        assert_eq!(last_index, 1);
        assert!(keepalive_conn.is_some());
    }
}

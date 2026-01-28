use std::fs;
use std::fs::OpenOptions;
use std::io::Write;

use log::debug;
use rusqlite::Connection;

use crate::base::decompressed_data;
use crate::error::Result;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;

impl super::Restore {
    pub(super) async fn restore_snapshot(
        &self,
        client: &StorageClient,
        snapshot: &SnapshotInfo,
        path: &str,
    ) -> Result<()> {
        let compressed_data = client.read_snapshot(snapshot).await?;
        let decompressed_data = decompressed_data(compressed_data)?;
        debug!(
            "restore_snapshot: decompressed data size: {}",
            decompressed_data.len()
        );

        // Clean up existing WAL and SHM files to prevent corruption
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

        // Verify integrity of the downloaded snapshot
        let temp_path = format!("{path}.tmp");
        {
            let conn = Connection::open(&temp_path)?;
            conn.pragma_query(None, "integrity_check", |row| {
                let s: String = row.get(0)?;
                if s != "ok" {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(11), // SQLITE_CORRUPT
                        Some(format!("Integrity check failed: {s}")),
                    ));
                }
                Ok(())
            })?;
        }

        fs::rename(temp_path, path)?;

        Ok(())
    }
}

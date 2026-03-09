use super::*;
use tempfile::{NamedTempFile, TempPath};

pub struct SnapshotStreamData {
    pub compressed_path: TempPath,
    pub compressed_size_bytes: u64,
    pub compressed_sha256: Vec<u8>,
    pub position: WalGenerationPos,
    pub page_size: u64,
}

pub async fn snapshot_for_stream(config: DbConfig) -> Result<SnapshotStreamData> {
    let connection = Connection::open(&config.db)?;
    Database::init_params(&config.db, &connection)?;
    Database::create_internal_tables(&connection)?;

    let page_size = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
    let wal_file = format!("{}-wal", config.db);
    let meta_dir = Database::init_directory(&config)?;

    let mut database = Database {
        config,
        meta_dir,
        wal_file,
        page_size,
        connection,
        tx_connection: None,
        sync_notifiers: Vec::new(),
        syncs: Vec::new(),
        last_retention_log: None,
        last_retention_floor: None,
        last_retention_tail: None,
    };

    let (compressed_path, compressed_size_bytes, compressed_sha256, position) =
        database.snapshot_to_temp_path().await?;
    Ok(SnapshotStreamData {
        compressed_path,
        compressed_size_bytes,
        compressed_sha256,
        position,
        page_size,
    })
}

impl Database {
    pub(super) async fn snapshot(&mut self) -> Result<(Vec<u8>, WalGenerationPos)> {
        // Always release the read lock on return, even on intermediate failures, so we don't
        // accidentally block future checkpoints.
        let result: Result<(Vec<u8>, WalGenerationPos)> = async {
            // Ensure WAL exists before trying to derive a generation position.
            // This is required for brand new databases that have not yet produced a WAL header.
            self.ensure_wal_exists().await?;

            // Snapshotting requires a generation + an initialized shadow WAL file because
            // checkpointing and position derivation are anchored to the shadow WAL inventory.
            if self.current_generation()?.is_empty() {
                self.create_generation().await?;
            }

            // Issue a PASSIVE checkpoint to flush pages to disk.
            // We use PASSIVE because TRUNCATE would delete the WAL header, causing create_generation to fail.
            // With PASSIVE, the new generation will start with the existing WAL header and frames.
            // The snapshot will contain the DB state including those frames.
            // Restoring will re-apply those frames, which is safe.
            self.checkpoint(CheckpointMode::Passive)?;

            // Acquire a read lock on the database during snapshot to prevent external checkpoints
            // and ensure the DB file is stable while we compress it.
            self.acquire_read_lock()?;

            // Obtain current position.
            let pos = self.wal_generation_position()?;
            if pos.is_empty() {
                return Err(Error::NoGenerationError("no generation"));
            }

            info!("db {} snapshot created, pos: {:?}", self.config.db, pos);

            // compress db file
            let compressed_data = compress_file(&self.config.db)?;

            Ok((compressed_data.to_owned(), pos))
        }
        .await;

        let release_result = self.release_read_lock();
        match (result, release_result) {
            (Ok(v), Ok(())) => Ok(v),
            (Ok(_), Err(e)) => Err(e),
            (Err(e), Ok(())) => Err(e),
            (Err(e), Err(_)) => Err(e),
        }
    }

    pub(super) async fn snapshot_to_temp_path(
        &mut self,
    ) -> Result<(TempPath, u64, Vec<u8>, WalGenerationPos)> {
        let (stable_snapshot_path, pos) = self.create_stable_snapshot_source().await?;

        info!(
            "db {} stream snapshot created from stable source, pos: {:?}",
            self.config.db, pos
        );

        let compressed_file = NamedTempFile::new_in(&self.meta_dir)?;
        let compressed_path = compressed_file.into_temp_path();
        let compressed_artifact = crate::base::compress_path_to_path_and_hash(
            stable_snapshot_path.as_ref(),
            compressed_path.as_ref(),
        )?;

        Ok((
            compressed_path,
            compressed_artifact.compressed_size_bytes,
            compressed_artifact.compressed_sha256,
            pos,
        ))
    }

    pub(super) async fn create_stable_snapshot_source(
        &mut self,
    ) -> Result<(TempPath, WalGenerationPos)> {
        let result: Result<(TempPath, WalGenerationPos)> = async {
            self.ensure_wal_exists().await?;

            if self.current_generation()?.is_empty() {
                self.create_generation().await?;
            }

            self.checkpoint(CheckpointMode::Passive)?;
            self.acquire_read_lock()?;

            let pos = self.wal_generation_position()?;
            if pos.is_empty() {
                return Err(Error::NoGenerationError("no generation"));
            }

            let mut source = std::fs::OpenOptions::new()
                .read(true)
                .open(&self.config.db)?;
            let mut stable_snapshot = NamedTempFile::new_in(&self.meta_dir)?;
            std::io::copy(&mut source, stable_snapshot.as_file_mut())?;
            stable_snapshot.as_file_mut().sync_all()?;

            Ok((stable_snapshot.into_temp_path(), pos))
        }
        .await;

        let release_result = self.release_read_lock();
        match (result, release_result) {
            (Ok(v), Ok(())) => Ok(v),
            (Ok(_), Err(e)) => Err(e),
            (Err(e), Ok(())) => Err(e),
            (Err(e), Err(_)) => Err(e),
        }
    }

    pub(super) async fn handle_db_snapshot_command(&mut self, index: usize) -> Result<()> {
        let (compressed_data, generation_pos) = self.snapshot().await?;
        debug!(
            "db {} snapshot {} data of pos {:?}",
            self.config.db,
            compressed_data.len(),
            generation_pos
        );
        if let Some(notifier) = &self.sync_notifiers[index] {
            notifier
                .send(ReplicateCommand::Snapshot((
                    generation_pos,
                    compressed_data,
                )))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;

    fn sha256_file(path: &Path) -> Result<Vec<u8>> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hasher.finalize().to_vec())
    }

    fn stream_only_db_config(db_path: &str) -> DbConfig {
        let toml_str = format!(
            r#"
db = "{db_path}"
min_checkpoint_page_number = 1000
max_checkpoint_page_number = 10000
truncate_page_number = 500000
checkpoint_interval_secs = 60
monitor_interval_ms = 50
apply_checkpoint_frame_interval = 128
apply_checkpoint_interval_ms = 2000
wal_retention_count = 10
max_concurrent_snapshots = 5

[[replicate]]
name = "stream"
[replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
"#,
        );

        toml::from_str(&toml_str).expect("parse db config")
    }

    #[tokio::test]
    async fn snapshot_releases_read_lock_on_compress_error() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");
        assert!(
            db.tx_connection.is_some(),
            "try_create should hold read lock"
        );

        // Make the DB file unreadable for new file descriptors so `compress_file()` fails.
        let mut perms = std::fs::metadata(&db.config.db)
            .expect("metadata")
            .permissions();
        perms.set_mode(0o0);
        std::fs::set_permissions(&db.config.db, perms).expect("chmod 0");

        let err = db.snapshot().await.expect_err("snapshot should fail");
        assert_ne!(err.code(), Error::OK);

        // Even on error, snapshot should release its read lock to avoid stalling checkpoints.
        assert!(
            db.tx_connection.is_none(),
            "snapshot leaked read lock (tx_connection still set); err={err:?}"
        );
    }

    #[tokio::test]
    async fn snapshot_for_stream_returns_file_backed_snapshot() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let snapshot = snapshot_for_stream(config).await.expect("snapshot");

        assert!(
            snapshot.compressed_path.exists(),
            "snapshot should be written to a temp compressed file"
        );
        assert!(
            snapshot.compressed_size_bytes > 0,
            "compressed snapshot file should not be empty"
        );
        assert_eq!(32, snapshot.compressed_sha256.len());
        assert_eq!(
            snapshot.compressed_sha256,
            sha256_file(snapshot.compressed_path.as_ref()).expect("hash snapshot"),
        );
    }

    #[tokio::test]
    async fn create_stable_snapshot_source_releases_read_lock() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");
        assert!(
            db.tx_connection.is_some(),
            "try_create should hold read lock"
        );

        let (stable_snapshot, pos) = db
            .create_stable_snapshot_source()
            .await
            .expect("stable snapshot");

        assert!(
            stable_snapshot.exists(),
            "stable snapshot source should exist"
        );
        assert!(
            !pos.is_empty(),
            "stable snapshot should return a boundary lsn"
        );
        assert!(
            db.tx_connection.is_none(),
            "stable snapshot source creation should release the read lock before compression"
        );
    }
}

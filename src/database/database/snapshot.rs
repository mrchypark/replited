use super::*;

pub struct SnapshotStreamData {
    pub compressed_data: Vec<u8>,
    pub position: WalGenerationPos,
    pub page_size: u64,
}

pub async fn snapshot_for_stream(config: DbConfig) -> Result<SnapshotStreamData> {
    let connection = Connection::open(&config.db)?;
    Database::init_params(&config.db, &connection)?;
    Database::create_internal_tables(&connection)?;

    let page_size_i64: i64 = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
    let page_size = u64::try_from(page_size_i64)
        .map_err(|_| Error::InvalidArg(format!("invalid sqlite page_size: {page_size_i64}")))?;
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
        last_checkpointed_snapshot_mod_time: None,
    };

    let (compressed_data, position) = database.snapshot().await?;
    Ok(SnapshotStreamData {
        compressed_data,
        position,
        page_size,
    })
}

impl Database {
    fn acquire_snapshot_write_lock(&self) -> Result<Connection> {
        let conn = Connection::open(&self.config.db)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        Ok(conn)
    }

    pub(super) async fn snapshot(&mut self) -> Result<(Vec<u8>, WalGenerationPos)> {
        let result: Result<(Vec<u8>, WalGenerationPos)> = async {
            // Ensure WAL exists before trying to derive a generation position.
            // This is required for brand new databases that have not yet produced a WAL header.
            if !self.ensure_wal_exists().await? {
                let pos = self.wal_generation_position()?;
                if pos.offset == 0 {
                    return Err(Error::SqliteWalError(format!(
                        "wal {} header not found",
                        self.wal_file
                    )));
                }
                let compressed_data = compress_file(&self.config.db)?;
                info!(
                    "db {} checkpointed snapshot created without live WAL, pos: {:?}",
                    self.config.db, pos
                );
                return Ok((compressed_data.to_owned(), pos));
            }

            // Snapshotting requires a generation + an initialized shadow WAL file because
            // checkpointing and position derivation are anchored to the shadow WAL inventory.
            if self.current_generation()?.is_empty() {
                self.create_generation().await?;
            }

            // Block concurrent writers so the checkpointed DB file and the published shadow-WAL
            // position describe the same exact state.
            let snapshot_write_lock = self.acquire_snapshot_write_lock()?;

            // Copy every committed WAL frame into the current shadow WAL before checkpointing it
            // into the database file. This keeps the shadow position aligned with the snapshot DB.
            let generation = self.current_generation()?;
            let shadow_wal_file = self.current_shadow_wal_file(&generation)?;
            self.copy_to_shadow_wal(&shadow_wal_file)?;

            let pos = self.wal_generation_position()?;
            if pos.is_empty() {
                return Err(Error::NoGenerationError("no generation"));
            }

            let wal_header_before_checkpoint = WALHeader::read(&self.wal_file)?;

            // Flush those exact frames into the DB file while writers are still blocked.
            self.exec_checkpoint(CheckpointMode::Passive.as_str())?;

            info!("db {} snapshot created, pos: {:?}", self.config.db, pos);

            // compress db file
            let compressed_data = compress_file(&self.config.db)?;

            // Mirror do_checkpoint(): force a post-snapshot WAL write so SQLite either keeps
            // using the current header or visibly restarts the WAL right away. That lets us
            // advance the shadow WAL immediately instead of deferring a spurious generation
            // rollover to the next background sync.
            snapshot_write_lock.execute(
                "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
                (),
            )?;
            snapshot_write_lock.execute_batch("COMMIT;")?;

            let wal_header_after_snapshot = WALHeader::read(&self.wal_file)?;
            let current_index = parse_wal_path(&shadow_wal_file)?;
            let next_shadow_wal_file = self.shadow_wal_file(&generation, current_index + 1);
            if wal_header_before_checkpoint != wal_header_after_snapshot {
                debug!(
                    "db {} snapshot rotated WAL header, seeding next shadow WAL index {}",
                    self.config.db,
                    current_index + 1
                );
            } else {
                debug!(
                    "db {} snapshot kept WAL header, still seeding next shadow WAL index {} for restorable archival",
                    self.config.db,
                    current_index + 1
                );
            }
            self.init_shadow_wal_file(&next_shadow_wal_file)?;

            Ok((compressed_data.to_owned(), pos))
        }
        .await;

        match result {
            Ok(v) => {
                if let Err(err) = self.acquire_read_lock() {
                    warn!(
                        "db {} snapshot completed but read lock reacquire failed: {:?}",
                        self.config.db, err
                    );
                }
                Ok(v)
            }
            Err(e) => {
                let _ = self.release_read_lock();
                Err(e)
            }
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

    pub(super) async fn handle_db_snapshot_new_generation_command(
        &mut self,
        index: usize,
    ) -> Result<()> {
        if !self.ensure_wal_exists().await? {
            return Err(Error::SqliteWalError(format!(
                "wal {} header not found",
                self.wal_file
            )));
        }
        self.create_generation().await?;
        self.handle_db_snapshot_command(index).await
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use tempfile::tempdir;
    use tokio::sync::mpsc;

    use super::*;
    use crate::base::decompressed_data;
    use crate::sync::ReplicateCommand;

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
    async fn snapshot_keeps_read_lock_after_success() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.snapshot().await.expect("snapshot");

        assert!(
            db.tx_connection.is_some(),
            "successful snapshot should restore the steady-state read lock"
        );
    }

    #[tokio::test]
    async fn forced_new_generation_snapshot_command_emits_newer_generation() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");
        let (sync_tx, mut sync_rx) = mpsc::channel(2);
        db.sync_notifiers = vec![Some(sync_tx)];

        db.handle_db_snapshot_command(0)
            .await
            .expect("first snapshot command");
        let first_generation = match sync_rx.recv().await.expect("first snapshot notification") {
            ReplicateCommand::Snapshot((pos, _compressed)) => pos.generation,
            other => panic!("expected snapshot notification, got {other:?}"),
        };

        tokio::time::sleep(Duration::from_millis(2)).await;
        db.handle_db_snapshot_new_generation_command(0)
            .await
            .expect("forced new-generation snapshot command");
        let second_generation = match sync_rx.recv().await.expect("second snapshot notification") {
            ReplicateCommand::Snapshot((pos, _compressed)) => pos.generation,
            other => panic!("expected snapshot notification, got {other:?}"),
        };

        assert!(second_generation > first_generation);
    }

    #[tokio::test]
    async fn snapshot_position_captures_unsynced_wal_before_checkpoint() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.connection
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshot_consistency (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                (),
            )
            .expect("create user table");
        if db
            .current_generation()
            .expect("current generation")
            .is_empty()
        {
            db.create_generation().await.expect("create generation");
        }
        let generation = db.current_generation().expect("current generation");
        let shadow_wal_file = db
            .current_shadow_wal_file(&generation)
            .expect("current shadow wal");
        let shadow_size_before = std::fs::metadata(&shadow_wal_file)
            .expect("shadow wal metadata")
            .len();

        db.connection
            .execute(
                "INSERT INTO snapshot_consistency (value) VALUES ('before')",
                (),
            )
            .expect("seed unsynced row");

        let (compressed_data, snapshot_pos) = db.snapshot().await.expect("snapshot");

        let restored_path = dir.path().join("restored.db");
        std::fs::write(
            &restored_path,
            decompressed_data(compressed_data).expect("decompress snapshot"),
        )
        .expect("write restored db");
        let restored_conn = Connection::open(&restored_path).expect("open restored db");
        let restored_count: i64 = restored_conn
            .query_row("SELECT COUNT(*) FROM snapshot_consistency", [], |row| {
                row.get(0)
            })
            .expect("count restored rows");

        assert_eq!(
            restored_count, 1,
            "snapshot DB should contain the pre-snapshot row"
        );
        assert!(
            snapshot_pos.offset > shadow_size_before,
            "snapshot position must advance to cover WAL frames copied into the snapshot DB; shadow_before={shadow_size_before} snapshot_pos={snapshot_pos:?}"
        );
    }

    #[tokio::test]
    async fn snapshot_does_not_leave_shadow_wal_header_stale_for_next_sync() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.connection
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshot_followup (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                (),
            )
            .expect("create followup table");

        db.snapshot().await.expect("initial snapshot");

        let writer = Connection::open(&db.config.db).expect("open writer");
        writer
            .execute(
                "INSERT INTO snapshot_followup (value) VALUES ('after-snapshot')",
                (),
            )
            .expect("write after snapshot");

        let verify = db.verify().expect("verify after snapshot");
        assert_ne!(
            verify.reason.as_deref(),
            Some("wal header changed"),
            "snapshot should leave shadow WAL ready for the next sync; verify={verify:?}"
        );
    }

    #[tokio::test]
    async fn snapshot_always_seeds_next_shadow_wal_index_for_restoreable_followup() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.connection
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshot_rollover (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                (),
            )
            .expect("create rollover table");
        db.connection
            .execute(
                "INSERT INTO snapshot_rollover (value) VALUES ('before-snapshot')",
                (),
            )
            .expect("seed row");

        let (_compressed_data, snapshot_pos) = db.snapshot().await.expect("snapshot");
        let generation = db.current_generation().expect("current generation");
        let (tail_index, _tail_size) = db
            .current_shadow_index(&generation)
            .expect("current shadow index");
        let next_shadow_wal = db.shadow_wal_file(&generation, tail_index);

        assert_eq!(
            tail_index,
            snapshot_pos.index + 1,
            "snapshot should advance shadow WAL index so post-snapshot archival starts from offset 0"
        );
        assert!(
            std::fs::metadata(&next_shadow_wal)
                .expect("next shadow wal metadata")
                .len()
                > WAL_HEADER_SIZE,
            "next shadow WAL should be seeded with a full header-bearing WAL image"
        );
    }
}

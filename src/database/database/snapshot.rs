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

    fn compress_materialized_snapshot(&self) -> Result<Vec<u8>> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let snapshot_path = Path::new(&self.meta_dir).join(format!("snapshot_{timestamp}.db"));
        match std::fs::remove_file(&snapshot_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        let result = (|| -> Result<Vec<u8>> {
            self.connection
                .backup(rusqlite::MAIN_DB, &snapshot_path, None)?;
            let snapshot_path_str = snapshot_path.to_string_lossy();
            compress_file(&snapshot_path_str)
        })();

        let _ = std::fs::remove_file(&snapshot_path);
        result
    }

    fn restart_wal_after_materialized_snapshot(
        &mut self,
        generation: &str,
        current_index: u64,
    ) -> Result<bool> {
        let wal_header_before_restart = WALHeader::read(&self.wal_file)?;

        self.release_read_lock()?;
        let checkpoint_result = self
            .connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
        let marker_result = if checkpoint_result.is_ok() {
            self.connection
                .execute(
                    "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
                    (),
                )
                .map(|_| ())
        } else {
            Ok(())
        };
        let reacquire_result = self.acquire_read_lock();
        checkpoint_result?;
        marker_result?;
        reacquire_result?;

        let wal_header_after_restart = WALHeader::read(&self.wal_file)?;
        if wal_header_before_restart == wal_header_after_restart {
            return Ok(false);
        }

        let next_shadow_wal_file = self.shadow_wal_file(generation, current_index + 1);
        debug!(
            "db {} snapshot restarted WAL, seeding shadow WAL index {}",
            self.config.db,
            current_index + 1
        );
        self.init_shadow_wal_file(&next_shadow_wal_file)?;
        Ok(true)
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
                let compressed_data = self.compress_materialized_snapshot()?;
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

            // Flush those exact frames into the DB file while writers are still blocked.
            self.exec_checkpoint(CheckpointMode::Passive.as_str())?;

            info!("db {} snapshot created, pos: {:?}", self.config.db, pos);

            let compressed_data = self.compress_materialized_snapshot()?;

            snapshot_write_lock.execute_batch("COMMIT;")?;

            let current_index = parse_wal_path(&shadow_wal_file)?;
            let restarted =
                self.restart_wal_after_materialized_snapshot(&generation, current_index)?;

            let (stream_index, _stream_shadow_size) = self.current_shadow_index(&generation)?;
            if !restarted {
                debug!(
                    "db {} snapshot did not restart WAL during truncate checkpoint; publishing current shadow WAL index {} from offset zero",
                    self.config.db, current_index
                );
            }

            let stream_start_pos = WalGenerationPos {
                generation: pos.generation.clone(),
                index: stream_index,
                offset: 0,
            };

            Ok((compressed_data.to_owned(), stream_start_pos))
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
        assert_eq!(
            snapshot_pos.offset, 0,
            "materialized snapshot should publish a WAL-free resume boundary; shadow_before={shadow_size_before} snapshot_pos={snapshot_pos:?}"
        );
    }

    #[tokio::test]
    async fn snapshot_uses_fresh_database_view_after_external_bootstrap() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        let external = Connection::open(&db.config.db).expect("open external app connection");
        external
            .pragma_update(None, "journal_mode", "WAL")
            .expect("put external app connection in WAL mode");
        external
            .execute_batch(
                "CREATE TABLE external_bootstrap (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO external_bootstrap (value) VALUES ('created-after-replited-opened');",
            )
            .expect("simulate app bootstrap after replited opened its connection");

        let (compressed_data, _snapshot_pos) = db.snapshot().await.expect("snapshot");

        let restored_path = dir.path().join("restored-external-bootstrap.db");
        std::fs::write(
            &restored_path,
            decompressed_data(compressed_data).expect("decompress snapshot"),
        )
        .expect("write restored db");
        let restored_conn = Connection::open(&restored_path).expect("open restored db");
        let restored_value: String = restored_conn
            .query_row("SELECT value FROM external_bootstrap", [], |row| row.get(0))
            .expect("read externally bootstrapped table from snapshot");

        assert_eq!(restored_value, "created-after-replited-opened");
    }

    #[tokio::test]
    async fn snapshot_covers_wal_frames_even_when_checkpoint_is_blocked_by_reader() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.connection
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshot_blocked_checkpoint (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                (),
            )
            .expect("create user table");
        db.connection
            .execute(
                "INSERT INTO snapshot_blocked_checkpoint (value) VALUES ('reader-endmark')",
                (),
            )
            .expect("seed reader-visible row");

        let reader = Connection::open(&db.config.db).expect("open external reader");
        reader
            .execute_batch("BEGIN; SELECT COUNT(*) FROM snapshot_blocked_checkpoint;")
            .expect("hold external read transaction");

        db.connection
            .execute(
                "INSERT INTO snapshot_blocked_checkpoint (value) VALUES ('after-reader')",
                (),
            )
            .expect("write row that checkpoint cannot flush while reader is active");

        let (compressed_data, snapshot_pos) = db
            .snapshot()
            .await
            .expect("snapshot should produce a DB plus WAL boundary");
        reader
            .execute_batch("ROLLBACK;")
            .expect("release external reader");

        let restored_path = dir.path().join("restored-blocked-checkpoint.db");
        std::fs::write(
            &restored_path,
            decompressed_data(compressed_data).expect("decompress snapshot"),
        )
        .expect("write restored db");
        assert_eq!(
            snapshot_pos.offset, 0,
            "blocked-reader snapshots should be self-contained and not require WAL prefix replay"
        );
        let restored_conn = Connection::open(&restored_path).expect("open restored db");
        let quick_check: String = restored_conn
            .pragma_query_value(None, "quick_check", |row| row.get(0))
            .expect("quick_check");
        assert_eq!(quick_check, "ok");

        let restored_values: Vec<String> = {
            let mut stmt = restored_conn
                .prepare("SELECT value FROM snapshot_blocked_checkpoint ORDER BY id")
                .expect("prepare restored values query");
            stmt.query_map([], |row| row.get(0))
                .expect("query restored values")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect restored values")
        };
        assert_eq!(
            restored_values,
            vec!["reader-endmark".to_string(), "after-reader".to_string()],
            "snapshot DB should include WAL frames newer than an external reader endmark"
        );
    }

    #[tokio::test]
    async fn snapshot_payload_is_self_contained_when_checkpoint_is_blocked_by_reader() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("primary.db");

        let config = stream_only_db_config(db_path.to_string_lossy().as_ref());
        let (mut db, _rx) = Database::try_create(config).await.expect("create db");

        db.connection
            .execute(
                "CREATE TABLE IF NOT EXISTS snapshot_self_contained (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                (),
            )
            .expect("create user table");
        db.connection
            .execute(
                "INSERT INTO snapshot_self_contained (value) VALUES ('reader-endmark')",
                (),
            )
            .expect("seed reader-visible row");

        let reader = Connection::open(&db.config.db).expect("open external reader");
        reader
            .execute_batch("BEGIN; SELECT COUNT(*) FROM snapshot_self_contained;")
            .expect("hold external read transaction");

        db.connection
            .execute(
                "INSERT INTO snapshot_self_contained (value) VALUES ('after-reader')",
                (),
            )
            .expect("write row that checkpoint cannot flush while reader is active");

        let (compressed_data, _snapshot_pos) = db
            .snapshot()
            .await
            .expect("snapshot should produce a self-contained DB payload");
        reader
            .execute_batch("ROLLBACK;")
            .expect("release external reader");

        let restored_path = dir.path().join("restored-self-contained.db");
        std::fs::write(
            &restored_path,
            decompressed_data(compressed_data).expect("decompress snapshot"),
        )
        .expect("write restored db");
        let restored_conn = Connection::open(&restored_path).expect("open restored db");

        let restored_values: Vec<String> = {
            let mut stmt = restored_conn
                .prepare("SELECT value FROM snapshot_self_contained ORDER BY id")
                .expect("prepare restored values query");
            stmt.query_map([], |row| row.get(0))
                .expect("query restored values")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect restored values")
        };
        assert_eq!(
            restored_values,
            vec!["reader-endmark".to_string(), "after-reader".to_string()],
            "stream snapshot payload must be restorable without an out-of-band WAL prefix"
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
    async fn snapshot_starts_next_shadow_wal_index_after_checkpoint_restart() {
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
        if db
            .current_generation()
            .expect("current generation")
            .is_empty()
        {
            db.create_generation().await.expect("create generation");
        }
        let generation = db.current_generation().expect("current generation");
        let (index_before_snapshot, _size_before_snapshot) = db
            .current_shadow_index(&generation)
            .expect("current shadow index before snapshot");

        let (_compressed_data, snapshot_pos) = db.snapshot().await.expect("snapshot");
        let (tail_index, _tail_size) = db
            .current_shadow_index(&generation)
            .expect("current shadow index");

        assert_eq!(
            tail_index, snapshot_pos.index,
            "snapshot should publish the post-checkpoint shadow WAL as the replication resume index"
        );
        assert_eq!(
            tail_index,
            index_before_snapshot + 1,
            "materialized snapshots must move future WAL packs to a fresh aligned index when checkpoint restart succeeds"
        );
        assert_eq!(
            snapshot_pos.offset, 0,
            "future WAL packs should start from offset zero in the new shadow WAL"
        );
    }
}

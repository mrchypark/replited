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

    let (compressed_data, position) = database.snapshot().await?;
    Ok(SnapshotStreamData {
        compressed_data,
        position,
        page_size,
    })
}

impl Database {
    pub(super) async fn snapshot(&mut self) -> Result<(Vec<u8>, WalGenerationPos)> {
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

        // Release lock.
        self.release_read_lock()?;

        Ok((compressed_data.to_owned(), pos))
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

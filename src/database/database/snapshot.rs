use super::*;

impl Database {
    pub(super) async fn snapshot(&mut self) -> Result<(Vec<u8>, WalGenerationPos)> {
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

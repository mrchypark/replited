use super::*;

impl Database {
    pub(super) fn calc_wal_size(&self, n: u64) -> u64 {
        WAL_HEADER_SIZE + (WAL_FRAME_HEADER_SIZE + self.page_size) * n
    }

    pub(super) fn decide_checkpoint_mode(
        &self,
        orig_wal_size: u64,
        new_wal_size: u64,
        info: &SyncInfo,
    ) -> Option<CheckpointMode> {
        // If WAL size is great than max threshold, force checkpoint.
        // If WAL size is greater than min threshold, attempt checkpoint.
        if self.config.truncate_page_number > 0
            && orig_wal_size >= self.calc_wal_size(self.config.truncate_page_number)
        {
            debug!(
                "checkpoint by orig_wal_size({}) > truncate_page_number({})",
                orig_wal_size, self.config.truncate_page_number
            );
            return Some(CheckpointMode::Truncate);
        } else if self.config.max_checkpoint_page_number > 0
            && new_wal_size >= self.calc_wal_size(self.config.max_checkpoint_page_number)
        {
            debug!(
                "checkpoint by new_wal_size({}) > max_checkpoint_page_number({})",
                new_wal_size, self.config.max_checkpoint_page_number
            );
            return Some(CheckpointMode::Restart);
        } else if new_wal_size >= self.calc_wal_size(self.config.min_checkpoint_page_number) {
            debug!(
                "checkpoint by new_wal_size({}) > min_checkpoint_page_number({})",
                new_wal_size, self.config.min_checkpoint_page_number
            );
            return Some(CheckpointMode::Passive);
        } else if self.config.checkpoint_interval_secs > 0
            && let Some(db_mod_time) = &info.db_mod_time
        {
            let now = SystemTime::now();

            if let Ok(duration) = now.duration_since(*db_mod_time)
                && duration.as_secs() > self.config.checkpoint_interval_secs
                && new_wal_size > self.calc_wal_size(1)
            {
                debug!(
                    "checkpoint by db_mod_time > checkpoint_interval_secs({})",
                    self.config.checkpoint_interval_secs
                );
                return Some(CheckpointMode::Passive);
            }
        }

        None
    }

    pub(super) fn checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        let generation = self.current_generation()?;
        self.do_checkpoint(&generation, mode.as_str())
    }

    // checkpoint performs a checkpoint on the WAL file and initializes a
    // new shadow WAL file.
    pub(super) fn do_checkpoint(&mut self, generation: &str, mode: &str) -> Result<()> {
        // Try getting a checkpoint lock, will fail during snapshots.
        let shadow_wal_file = self.current_shadow_wal_file(generation)?;

        // Read WAL header before checkpoint to check if it has been restarted.
        let wal_header1 = WALHeader::read(&self.wal_file)?;

        // Copy shadow WAL before checkpoint to copy as much as possible.
        let (orig_size, new_size) = self.copy_to_shadow_wal(&shadow_wal_file)?;
        debug!(
            "db {} do_checkpoint copy_to_shadow_wal: {}, {}",
            self.config.db, orig_size, new_size
        );

        // Execute checkpoint and immediately issue a write to the WAL to ensure
        // a new page is written.
        self.exec_checkpoint(mode)?;

        self.connection.execute(
            "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        // If WAL hasn't been restarted, exit.
        let wal_header2 = WALHeader::read(&self.wal_file)?;
        if wal_header1 == wal_header2 {
            return Ok(());
        }

        // Start a transaction. This will be promoted immediately after.
        let mut connection = Connection::open(&self.config.db)?;
        let mut tx = connection.transaction()?;
        tx.set_drop_behavior(DropBehavior::Rollback);

        // Insert into the lock table to promote to a write tx.
        tx.execute("INSERT INTO _replited_lock (id) VALUES (1);", ())?;

        // Copy the end of the previous WAL before starting a new shadow WAL.
        let (orig_size, new_size) = self.copy_to_shadow_wal(&shadow_wal_file)?;
        debug!(
            "db {} do_checkpoint after checkpoint copy_to_shadow_wal: {}, {}",
            self.config.db, orig_size, new_size
        );

        // Parse index of current shadow WAL file.
        let index = parse_wal_path(&shadow_wal_file)?;

        // Start a new shadow WAL file with next index.
        let new_shadow_wal_file = self.shadow_wal_file(generation, index + 1);
        self.init_shadow_wal_file(&new_shadow_wal_file)?;

        Ok(())
    }

    pub(super) fn exec_checkpoint(&mut self, mode: &str) -> Result<()> {
        // Ensure the read lock has been removed before issuing a checkpoint.
        // We defer the re-acquire to ensure it occurs even on an early return.
        self.release_read_lock()?;

        // A non-forced checkpoint is issued as "PASSIVE".
        let sql = format!("PRAGMA wal_checkpoint({mode})");
        let ret = self.connection.execute_batch(&sql);

        // Reacquire the read lock immediately after the checkpoint.
        self.acquire_read_lock()?;

        ret?;
        Ok(())
    }
}

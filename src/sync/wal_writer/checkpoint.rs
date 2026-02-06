use std::io::Write;
use std::path::PathBuf;

use crate::error::Result;

impl super::WalWriter {
    pub fn checkpoint(&mut self) -> Result<()> {
        // Sync file first
        self.file.sync_all()?;

        // Use the pinned connection if available, otherwise open a temporary one
        if let Some(ref conn) = self.conn {
            // First, attempt PASSIVE checkpoint normally
            let mut res_passive: (i32, i32, i32) =
                conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                })?;

            // Conditional Recovery: If SQLite sees fewer frames than we wrote, Invalidate SHM
            // res_passive.1 is the number of valid frames in the WAL as per the SHM.
            // If we wrote more frames than SQLite sees, the SHM is stale.
            if res_passive.0 == 0
                && (res_passive.1 as u32) < self.frame_count
                && self.frame_count > 0
            {
                log::warn!(
                    "WalWriter: Stale SHM detected (SQLite saw {} frames, we have {}). Forcing Recovery...",
                    res_passive.1,
                    self.frame_count
                );

                // Invalidate SHM Header
                let shm_path = PathBuf::from(format!("{}-shm", self.db_path.display()));
                if let Ok(mut shm_file) = std::fs::OpenOptions::new().write(true).open(&shm_path) {
                    let zeros = [0u8; 32];
                    let _ = shm_file.write_all(&zeros);
                    let _ = shm_file.sync_all();
                }

                // Retry PASSIVE to rebuild Index
                match conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                }) {
                    Ok(res) => {
                        res_passive = res;
                        log::debug!("WalWriter: Recovery PASSIVE Result: {res_passive:?}");

                        // CRITICAL CHECK: Did recovery actually work?
                        // If SQLite still sees fewer frames than we wrote, it means the frames are invalid/rejected.
                        // We cannot proceed because we are drifting further apart.
                        if (res_passive.1 as u32) < self.frame_count {
                            log::error!(
                                "WalWriter: Recovery FAILED. SQLite refused to accept frames. WAL is STUCK."
                            );
                            return Err(crate::error::Error::WalStuck(
                                "Recovery failed, WAL stuck".to_string(),
                            ));
                        }
                    }
                    Err(e) => {
                        log::error!("WalWriter: Recovery PASSIVE Failed: {e}");
                    }
                }
            } else {
                log::debug!("WalWriter: Checkpoint healthy or no frames: {res_passive:?}");
            }

            // SAFETY: Only TRUNCATE if we are sure data was checkpointed or if WAL is truly empty.
            log::debug!(
                "DEBUG SAFETY: FrameCount={}, ResPassive={:?}",
                self.frame_count,
                res_passive
            );

            // EXPERIMENT: Disable TRUNCATE entirely. Just use PASSIVE.
            // This ensures chain continuity at the cost of disk space.
            /*
            if self.frame_count > 0 && res_passive.1 == 0 {
                log::error!(
                    "WalWriter: CRITICAL - SQLite ignoring frames (Count={}, Checkpoint=(0,0,0)). SKIPPING TRUNCATE to preserve data.",
                    self.frame_count
                );
                return Ok(());
            }

            // Then TRUNCATE to move data and clean up
            let res_truncate: (i32, i32, i32) =
                conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                })?;
            log::debug!("WalWriter: TRUNCATE Checkpoint result: {:?}", res_truncate);
            log::info!(
                "Checkpoint result: PASSIVE={:?}, TRUNCATE={:?}",
                res_passive,
                res_truncate
            );
            */
        } else {
            // Fallback - also use PASSIVE for now
            let conn = rusqlite::Connection::open(&self.db_path)?;
            conn.pragma_update(None, "journal_mode", "WAL")?;
            let res: (i32, i32, i32) =
                conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, i32>(1)?,
                        row.get::<_, i32>(2)?,
                    ))
                })?;
            log::debug!("WalWriter: Checkpoint result: {res:?}");
        }

        Ok(())
    }
}

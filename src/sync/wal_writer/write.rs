use std::io::{Seek, SeekFrom, Write};
use std::time::Instant;

use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};

impl super::WalWriter {
    pub fn write_frame(&mut self, data: Vec<u8>) -> Result<()> {
        // Check if this is a WAL header (32 bytes)
        if data.len() == WAL_HEADER_SIZE as usize {
            log::info!("WalWriter: Received WAL header from Primary, creating local WAL...");
            // CRITICAL: Checkpoint existing WAL data before preventing data loss upon reset/truncation
            if self.initialized {
                log::info!("WalWriter: Performing safety checkpoint before WAL reset...");
                if let Err(e) = self.checkpoint() {
                    log::error!("WalWriter: Safety checkpoint failed: {e}");
                    // We proceed anyway because we must accept the new header.
                }
            }
            return self.initialize_wal(&data);
        }

        if !self.initialized {
            return Err(crate::error::Error::from_string(
                "WAL not initialized".into(),
            ));
        }

        // Validate frame length before writing it verbatim
        let expected_len = (WAL_FRAME_HEADER_SIZE + self.page_size) as usize;
        if data.len() != expected_len {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid frame size",
            ));
        }

        self.frame_count += 1;

        // Determine the offset where the new frame will be written.
        let mut offset = self.file.metadata()?.len();

        // CRITICAL: If WAL was truncated (offset=0), we MUST restore the WAL Header (32 bytes)
        // because SQLite expects it, but TRUNCATE deleted it.
        if offset == 0 {
            if let Some(header) = self.cached_header {
                log::warn!("WalWriter: WAL Header missing (truncated). Restoring Cached Header...");
                self.file.seek(SeekFrom::Start(0))?;
                self.file.write_all(&header)?;
                self.file.sync_all()?;

                // IMPORTANT: Reset last_checksum to Header Checksum.
                // CRITICAL: Checksums are ALWAYS stored in BIG-ENDIAN per SQLite spec
                let h_c1 = u32::from_be_bytes(header[24..28].try_into().unwrap());
                let h_c2 = u32::from_be_bytes(header[28..32].try_into().unwrap());
                self.last_checksum = (h_c1, h_c2);
                log::debug!("WalWriter: Reset last_checksum to Header Checksum: ({h_c1}, {h_c2})");

                offset = 32; // After restoring header, the next write starts at 32.
            } else {
                log::error!("WalWriter: WAL Truncated and NO Cached Header! Corruption imminent.");
            }
        }

        // PASS-THROUGH MODE: Write frames VERBATIM from Primary.
        if data.len() < WAL_FRAME_HEADER_SIZE as usize {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid frame data size",
            ));
        }

        let frame_data = data;

        // Extract checksum from Primary frame for logging
        let c1 = u32::from_be_bytes(frame_data[16..20].try_into().unwrap_or([0; 4]));
        let c2 = u32::from_be_bytes(frame_data[20..24].try_into().unwrap_or([0; 4]));

        log::debug!(
            "WalWriter: Writing VERBATIM Frame #{} (len={}) Primary Checksum=({:#x}, {:#x})",
            self.frame_count,
            frame_data.len(),
            c1,
            c2
        );

        // Write to file
        log::debug!(
            "WalWriter: Writing frame #{} (len={}) at offset {}",
            self.frame_count,
            frame_data.len(),
            offset
        );

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&frame_data)?;
        self.file.sync_all()?;

        log::debug!(
            "WalWriter: Frame written, file len now={}",
            self.file.metadata()?.len()
        );

        self.frames_since_checkpoint += 1;

        // Track schema changes (page 1) and commits (db_size > 0).
        let page_num = u32::from_be_bytes(frame_data[0..4].try_into().unwrap_or([0; 4]));
        let db_size = u32::from_be_bytes(frame_data[4..8].try_into().unwrap_or([0; 4]));
        let is_commit_frame = db_size > 0;

        if page_num == 1 {
            self.schema_dirty = true;
        }

        let should_checkpoint_now = self.schema_dirty && is_commit_frame;

        // Heuristic: checkpoint periodically so readers can see schema/data without restarting.
        let periodic_checkpoint = self.frames_since_checkpoint >= self.checkpoint_frame_interval
            || self.last_checkpoint.elapsed() >= self.checkpoint_time_interval;

        if should_checkpoint_now || periodic_checkpoint {
            self.checkpoint()?;
            self.frames_since_checkpoint = 0;
            self.last_checkpoint = Instant::now();
            if should_checkpoint_now {
                self.schema_dirty = false;
            }
        }

        Ok(())
    }
}

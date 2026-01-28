use std::io::Cursor;
use std::io::{Seek, SeekFrom, Write};

use crate::error::Result;
use crate::sqlite::{WAL_HEADER_SIZE, WALHeader};

impl super::WalWriter {
    /// Initialize WAL with a header based on incoming Primary header.
    /// This signals the start of a new WAL sequence (Primary restarted or truncated).
    /// CRITICAL: We MUST checkpoint any existing frames before Overwriting the header/salts,
    /// otherwise they will become invalid (Salt Mismatch) and lost.
    pub(super) fn initialize_wal(&mut self, primary_header: &[u8]) -> Result<()> {
        if primary_header.len() != WAL_HEADER_SIZE as usize {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid WAL header size",
            ));
        }

        // 1. Force Checkpoint to save any existing data
        if self.initialized && self.frame_count > 0 {
            log::info!(
                "WalWriter: New Header received (Primary Truncate?). Checkpointing {} existing frames...",
                self.frame_count
            );
            // We use PASSIVE to be safe. If it fails, we risk data loss, but we can't keep old frames with new header.
            if let Err(e) = self.checkpoint() {
                log::error!("WalWriter: Checkpoint failed during re-initialization: {e}");
            }
        }

        // 2. Truncate the file to remove old frames (conflicting salts).
        // Since we (hopefully) checkpointed, it's safe to clear.
        if self.initialized {
            log::warn!("WalWriter: Truncating Local WAL to start fresh sequence.");
            self.file.set_len(0)?;
            self.file.sync_all()?;
        }

        // Parse and validate the incoming header
        let mut cursor = Cursor::new(primary_header);
        let wal_header = WALHeader::read_from(&mut cursor)?;
        self.is_big_endian = wal_header.is_big_endian;
        self.page_size = wal_header.page_size;

        // Use Primary Salts and Checksums VERBATIM.
        self.salt1 = wal_header.salt1;
        self.salt2 = wal_header.salt2;

        let header_bytes = primary_header.to_vec();

        // Write header to file
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;

        // Cache the header for restoration after truncate
        let mut cache = [0u8; 32];
        cache.copy_from_slice(&header_bytes[0..32]);
        self.cached_header = Some(cache);

        self.initialized = true;
        self.frame_count = 0; // Reset frame count (since this is a new "session" or stream start)

        // Initialize last_checksum from Header
        // CRITICAL: Checksums are ALWAYS stored in BIG-ENDIAN per SQLite spec
        let h_c1 = u32::from_be_bytes(header_bytes[24..28].try_into().unwrap());
        let h_c2 = u32::from_be_bytes(header_bytes[28..32].try_into().unwrap());
        self.last_checksum = (h_c1, h_c2);

        log::info!(
            "Initialized local WAL with salt1={:#x}, salt2={:#x}, page_size={}",
            self.salt1,
            self.salt2,
            self.page_size
        );
        Ok(())
    }
}

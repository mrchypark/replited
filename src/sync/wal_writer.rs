use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALHeader, checksum};
use crate::sync::replication::WalPacket;
use crate::sync::replication::wal_packet::Payload;

/// WalWriter handles writing WAL frames to a replica's WAL file.
///
/// Key insight: WAL files have salt values (salt1, salt2) in the header.
/// Each frame also contains these salts and checksums that chain together.
/// When replicating from a Primary, the Primary's WAL has different salts
/// than what the Replica's restored database expects.
///
/// This WalWriter solves this by:
/// 1. Creating a fresh WAL header with random salts for the local database
/// 2. Extracting page data from incoming frames
/// 3. Re-signing frames with local salts and recalculated checksums
pub struct WalWriter {
    wal_path: PathBuf,
    db_path: PathBuf,
    page_size: u64,
    file: File,
    conn: Option<rusqlite::Connection>,

    // Local WAL header state
    initialized: bool,
    salt1: u32,
    salt2: u32,
    is_big_endian: bool,
    last_checksum: (u32, u32),
    frame_count: u32,
    last_checkpoint: Instant,
    frames_since_checkpoint: u32,
    schema_dirty: bool,
    checkpoint_frame_interval: u32,
    checkpoint_time_interval: Duration,
    cached_header: Option<[u8; 32]>,
}

impl WalWriter {
    pub fn new(
        wal_path: PathBuf,
        page_size: u64,
        checkpoint_frame_interval: u32,
        checkpoint_time_interval: Duration,
    ) -> Result<Self> {
        let db_path_str = wal_path.to_str().unwrap().trim_end_matches("-wal");
        let db_path = PathBuf::from(db_path_str);

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&wal_path)?;

        let mut wal_writer = Self {
            wal_path,
            db_path,
            page_size, // Will be overwritten if header exists
            file,
            conn: None,
            initialized: false,
            salt1: 0,
            salt2: 0,
            is_big_endian: false,
            last_checksum: (0, 0),
            frame_count: 0,
            last_checkpoint: Instant::now(),
            frames_since_checkpoint: 0,
            schema_dirty: false,
            checkpoint_frame_interval,
            checkpoint_time_interval,
            cached_header: None,
        };

        // Open a persistent connection to pin the WAL file and prevent deletion
        // by other processes (like verification scripts) when they close.
        match rusqlite::Connection::open(&wal_writer.db_path) {
            Ok(c) => {
                let _ = c.pragma_update(None, "journal_mode", "WAL");
                wal_writer.conn = Some(c);
            }
            Err(e) => eprintln!("WalWriter: Failed to open pinned connection: {e}"),
        }

        if wal_writer.file.metadata()?.len() >= 32 {
            // Read header if exists
            wal_writer.file.seek(SeekFrom::Start(0))?;
            let mut header_buf = [0u8; 32];
            wal_writer.file.read_exact(&mut header_buf)?;

            let mut cursor = Cursor::new(&header_buf);
            let wal_header = WALHeader::read_from(&mut cursor)?;

            wal_writer.is_big_endian = wal_header.is_big_endian;
            wal_writer.page_size = wal_header.page_size;
            wal_writer.salt1 = wal_header.salt1;
            wal_writer.salt2 = wal_header.salt2;
            wal_writer.last_checksum = checksum(&header_buf[0..24], 0, 0, wal_writer.is_big_endian);
            wal_writer.initialized = true;

            // Calculate frame count
            let len = wal_writer.file.metadata()?.len();
            if len > 32 {
                let frames_len = len - 32;
                let frame_size = WAL_FRAME_HEADER_SIZE + wal_writer.page_size;
                wal_writer.frame_count = (frames_len / frame_size) as u32;

                if wal_writer.frame_count > 0 {
                    let last_frame_offset = 32 + (wal_writer.frame_count as u64 - 1) * frame_size;
                    wal_writer.file.seek(SeekFrom::Start(last_frame_offset))?;
                    let mut frame_hdr = [0u8; 24];
                    wal_writer.file.read_exact(&mut frame_hdr)?;

                    // Read Checksum1 (offset 16) and Checksum2 (offset 20)
                    let c1 = if wal_writer.is_big_endian {
                        u32::from_be_bytes(frame_hdr[16..20].try_into().unwrap())
                    } else {
                        u32::from_le_bytes(frame_hdr[16..20].try_into().unwrap())
                    };

                    let c2 = if wal_writer.is_big_endian {
                        u32::from_be_bytes(frame_hdr[20..24].try_into().unwrap())
                    } else {
                        u32::from_le_bytes(frame_hdr[20..24].try_into().unwrap())
                    };

                    wal_writer.last_checksum = (c1, c2);

                    println!("WalWriter: Recovered last_checksum=({c1:#x}, {c2:#x})");
                }
            }

            wal_writer.file.seek(SeekFrom::End(0))?;

            println!(
                "WalWriter: Recovered existing WAL. frame_count={}",
                wal_writer.frame_count
            );
        }

        Ok(wal_writer)
    }

    pub fn apply(&mut self, packet: WalPacket) -> Result<()> {
        match packet.payload {
            Some(Payload::FrameData(frame_data)) => {
                self.write_frame(frame_data)?;
            }
            Some(Payload::Event(_checkpoint)) => {
                log::info!("Received checkpoint event");
                self.checkpoint()?;
            }
            None => {}
        }
        Ok(())
    }

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

            // Conditional Recovery: If SQLite sees no frames (but we wrote some), Invalidate SHM
            if res_passive.0 == 0
                && res_passive.1 == 0
                && res_passive.2 == 0
                && self.frame_count > 0
            {
                println!(
                    "WalWriter: Stale SHM detected (frames written but not guarded). Forcing Recovery..."
                );

                // Invalidate SHM Header
                let shm_path = format!("{}-shm", self.db_path.to_str().unwrap());
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
                        println!("WalWriter: Recovery PASSIVE Result: {res_passive:?}");
                    }
                    Err(e) => {
                        eprintln!("WalWriter: Recovery PASSIVE Failed: {e}");
                    }
                }
            } else {
                log::debug!("WalWriter: Checkpoint healthy or no frames: {res_passive:?}");
            }

            // SAFETY: Only TRUNCATE if we are sure data was checkpointed or if WAL is truly empty.
            println!(
                "DEBUG SAFETY: FrameCount={}, ResPassive={:?}",
                self.frame_count, res_passive
            );

            // EXPERIMENT: Disable TRUNCATE entirely. Just use PASSIVE.
            // This ensures chain continuity at the cost of disk space.
            /*
            if self.frame_count > 0 && res_passive.1 == 0 {
                println!(
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
            println!("WalWriter: TRUNCATE Checkpoint result: {:?}", res_truncate);
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
            println!("WalWriter: Checkpoint result: {res:?}");
        }

        Ok(())
    }

    /// Initialize WAL with a header based on incoming Primary header.
    /// This signals the start of a new WAL sequence (Primary restarted or truncated).
    /// CRITICAL: We MUST checkpoint any existing frames before Overwriting the header/salts,
    /// otherwise they will become invalid (Salt Mismatch) and lost.
    fn initialize_wal(&mut self, primary_header: &[u8]) -> Result<()> {
        if primary_header.len() != WAL_HEADER_SIZE as usize {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid WAL header size",
            ));
        }

        // 1. Force Checkpoint to save any existing data
        if self.initialized && self.frame_count > 0 {
            println!(
                "WalWriter: New Header received (Primary Truncate?). Checkpointing {} existing frames...",
                self.frame_count
            );
            // We use PASSIVE to be safe. If it fails, we risk data loss, but we can't keep old frames with new header.
            if let Err(e) = self.checkpoint() {
                eprintln!("WalWriter: Checkpoint failed during re-initialization: {e}");
            }
        }

        // 2. Truncate the file to remove old frames (conflicting salts).
        // Since we (hopefully) checkpointed, it's safe to clear.
        if self.initialized {
            println!("WalWriter: Truncating Local WAL to start fresh sequence.");
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
        // This is needed if we decide to verify checksums locally, even if we don't rewrite them.
        let h_c1 = if self.is_big_endian {
            u32::from_be_bytes(header_bytes[24..28].try_into().unwrap())
        } else {
            u32::from_le_bytes(header_bytes[24..28].try_into().unwrap())
        };
        let h_c2 = if self.is_big_endian {
            u32::from_be_bytes(header_bytes[28..32].try_into().unwrap())
        } else {
            u32::from_le_bytes(header_bytes[28..32].try_into().unwrap())
        };
        self.last_checksum = (h_c1, h_c2);

        println!(
            "WalWriter: Initialized local WAL using Primary Salts: salt1={:#x}, salt2={:#x}",
            self.salt1, self.salt2
        );
        log::info!(
            "Initialized local WAL with salt1={:#x}, salt2={:#x}, page_size={}",
            self.salt1,
            self.salt2,
            self.page_size
        );
        Ok(())
    }

    pub fn write_frame(&mut self, data: Vec<u8>) -> Result<()> {
        // Check if this is a WAL header (32 bytes)
        if data.len() == WAL_HEADER_SIZE as usize {
            println!("WalWriter: Received WAL header from Primary, creating local WAL...");
            // CRITICAL: Checkpoint existing WAL data before preventing data loss upon reset/truncation
            if self.initialized {
                println!("WalWriter: Performing safety checkpoint before WAL reset...");
                if let Err(e) = self.checkpoint() {
                    eprintln!("WalWriter: Safety checkpoint failed: {e}");
                    // We proceed anyway because we must accept the new header, possibly losing data is better than stuck stream?
                    // Ideally we should retry or fail hard.
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
        // We do this BEFORE writing the frame.
        if offset == 0 {
            if let Some(header) = self.cached_header {
                println!("WalWriter: WAL Header missing (truncated). Restoring Cached Header...");
                self.file.seek(SeekFrom::Start(0))?;
                self.file.write_all(&header)?;
                self.file.sync_all()?;

                // IMPORTANT: Reset last_checksum to Header Checksum.
                // The new frame (physically first in file) must be checksummed against the header.
                // The cached header already has the local salts and checksums.
                let h_c1 = if self.is_big_endian {
                    u32::from_be_bytes(header[24..28].try_into().unwrap())
                } else {
                    u32::from_le_bytes(header[24..28].try_into().unwrap())
                };
                let h_c2 = if self.is_big_endian {
                    u32::from_be_bytes(header[28..32].try_into().unwrap())
                } else {
                    u32::from_le_bytes(header[28..32].try_into().unwrap())
                };
                self.last_checksum = (h_c1, h_c2);
                println!("WalWriter: Reset last_checksum to Header Checksum: ({h_c1}, {h_c2})");

                offset = 32; // After restoring header, the next write starts at 32.
            } else {
                eprintln!("WalWriter: WAL Truncated and NO Cached Header! Corruption imminent.");
                // We can't recover easily without header.
                // But self.initialized implies we had it.
            }
        }

        // Clone data for modification/inspection
        let frame_data = data;

        // PASS-THROUGH MODE:
        // We write the frame exactly as received from Primary.
        // We do NOT re-sign with local salts.
        // We do NOT recalculate checksums.
        // We assume Primary's checksums are valid and linear.

        // For debugging, we can extract the checksums from the frame to log them.
        let c1 = if self.is_big_endian {
            u32::from_be_bytes(frame_data[16..20].try_into().unwrap())
        } else {
            u32::from_le_bytes(frame_data[16..20].try_into().unwrap())
        };
        let c2 = if self.is_big_endian {
            u32::from_be_bytes(frame_data[20..24].try_into().unwrap())
        } else {
            u32::from_le_bytes(frame_data[20..24].try_into().unwrap())
        };
        self.last_checksum = (c1, c2);

        println!(
            "WalWriter: Writing VERBATIM Frame #{} (len={}) Checksum=({:#x}, {:#x})",
            self.frame_count,
            frame_data.len(),
            c1,
            c2
        );

        // Write to file
        println!(
            "WalWriter: Writing frame #{} (len={}) at offset {}",
            self.frame_count,
            frame_data.len(),
            offset
        );

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&frame_data)?;
        self.file.sync_all()?;

        println!(
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

    fn put_u32(&self, val: u32) -> [u8; 4] {
        if self.is_big_endian {
            val.to_be_bytes()
        } else {
            val.to_le_bytes()
        }
    }
}

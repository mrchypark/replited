use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::{Seek, SeekFrom, Write};
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
}

impl WalWriter {
    pub fn new(
        wal_path: PathBuf,
        page_size: u64,
        checkpoint_frame_interval: u32,
        checkpoint_time_interval: Duration,
    ) -> Result<Self> {
        // Clear any stale SHM to force WAL-index recovery with the new WAL stream.
        let db_path_str = wal_path.to_str().unwrap().trim_end_matches("-wal");
        let shm_path = format!("{}-shm", db_path_str);
        let _ = std::fs::remove_file(&shm_path);

        // Truncate the WAL file to start fresh
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)?;

        // Derive db_path from wal_path (remove -wal suffix)
        let db_path = PathBuf::from(db_path_str);

        Ok(Self {
            wal_path,
            db_path,
            page_size,
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
        })
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

        // Drop SHM so SQLite rebuilds WAL index, ensuring new schema visibility.
        let shm_path = format!("{}-shm", self.db_path.to_string_lossy());
        let _ = std::fs::remove_file(&shm_path);

        if self.conn.is_none() {
            // Open connection if not exists
            let conn = rusqlite::Connection::open(&self.db_path)?;
            // Ensure WAL mode
            conn.pragma_update(None, "journal_mode", "WAL")?;
            self.conn = Some(conn);
        }

        if let Some(conn) = &self.conn {
            let result: (i32, i32, i32) =
                conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })?;
            println!("WalWriter: Checkpoint result: {:?}", result);
            log::info!("Checkpoint result: {:?}", result);
        }
        Ok(())
    }

    /// Initialize WAL with a header based on incoming Primary header,
    /// but keep the Primary header verbatim so checksums stay valid.
    fn initialize_wal(&mut self, primary_header: &[u8]) -> Result<()> {
        if primary_header.len() != WAL_HEADER_SIZE as usize {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid WAL header size",
            ));
        }

        // Parse and validate the incoming header
        let mut cursor = Cursor::new(primary_header);
        let wal_header = WALHeader::read_from(&mut cursor)?;
        self.is_big_endian = wal_header.is_big_endian;
        self.page_size = wal_header.page_size;
        self.salt1 = wal_header.salt1;
        self.salt2 = wal_header.salt2;
        self.last_checksum = checksum(&primary_header[0..24], 0, 0, self.is_big_endian);

        // Write header to file
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(primary_header)?;
        self.file.sync_all()?;

        self.initialized = true;
        self.frame_count = 0;

        println!(
            "WalWriter: Initialized local WAL with salt1={:#x}, salt2={:#x}, page_size={}, file len={}",
            self.salt1,
            self.salt2,
            self.page_size,
            self.file.metadata()?.len()
        );
        log::info!(
            "Initialized local WAL with salt1={:#x}, salt2={:#x}, page_size={}",
            self.salt1,
            self.salt2,
            self.page_size
        );

        Ok(())
    }

    fn write_frame(&mut self, data: Vec<u8>) -> Result<()> {
        // Check if this is a WAL header (32 bytes)
        if data.len() == WAL_HEADER_SIZE as usize {
            println!("WalWriter: Received WAL header from Primary, creating local WAL...");
            return self.initialize_wal(&data);
        }

        // Ensure WAL is initialized
        if !self.initialized {
            println!("WalWriter: ERROR: Received frame before WAL header!");
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Frame received before WAL header",
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

        // Write to file
        let offset = self.file.metadata()?.len();
        println!(
            "WalWriter: Writing resigned frame #{} (len={}) at offset {}",
            self.frame_count,
            data.len(),
            offset
        );

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&data)?;
        self.file.sync_all()?;

        println!(
            "WalWriter: Frame written, file len now={}",
            self.file.metadata()?.len()
        );

        self.frames_since_checkpoint += 1;

        // Track schema changes (page 1) and commits (db_size > 0).
        let page_num = u32::from_be_bytes(data[0..4].try_into().unwrap_or([0; 4]));
        let db_size = u32::from_be_bytes(data[4..8].try_into().unwrap_or([0; 4]));
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

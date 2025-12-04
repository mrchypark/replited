use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, checksum};
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
}

impl WalWriter {
    pub fn new(wal_path: PathBuf, page_size: u64) -> Result<Self> {
        // Truncate the WAL file to start fresh
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)?;

        // Derive db_path from wal_path (remove -wal suffix)
        let db_path_str = wal_path.to_str().unwrap().trim_end_matches("-wal");
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

    fn checkpoint(&mut self) -> Result<()> {
        // Sync file first
        self.file.sync_all()?;

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
    /// but with fresh random salts for compatibility with local database.
    fn initialize_wal(&mut self, primary_header: &[u8]) -> Result<()> {
        if primary_header.len() != WAL_HEADER_SIZE as usize {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid WAL header size",
            ));
        }

        // Parse endianness from primary header
        let magic = &primary_header[0..4];
        self.is_big_endian = magic == [0x37, 0x7f, 0x06, 0x83];

        // Parse page size from primary header
        let page_size = u32::from_be_bytes(primary_header[8..12].try_into().unwrap());
        self.page_size = page_size as u64;

        // Generate random salts for local WAL
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u32;
        self.salt1 = now ^ 0xDEADBEEF;
        self.salt2 = now.wrapping_mul(0x12345678);

        // Build our own WAL header
        let mut header = vec![0u8; WAL_HEADER_SIZE as usize];

        // Magic (use same endianness as primary)
        header[0..4].copy_from_slice(magic);

        // File format version (3007000)
        header[4..8].copy_from_slice(&0x002DE218_u32.to_be_bytes());

        // Page size
        header[8..12].copy_from_slice(&(self.page_size as u32).to_be_bytes());

        // Checkpoint sequence number (start at 1)
        header[12..16].copy_from_slice(&1_u32.to_be_bytes());

        // Salt values
        header[16..20].copy_from_slice(&self.salt1.to_be_bytes());
        header[20..24].copy_from_slice(&self.salt2.to_be_bytes());

        // Calculate checksum for header (first 24 bytes)
        let (s1, s2) = checksum(&header[0..24], 0, 0, self.is_big_endian);
        header[24..28].copy_from_slice(&s1.to_be_bytes());
        header[28..32].copy_from_slice(&s2.to_be_bytes());

        // Initialize last_checksum with header checksum
        self.last_checksum = (s1, s2);

        // Write header to file
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
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

    /// Re-sign a frame with local salts and proper checksums
    fn resign_frame(&mut self, primary_frame: &[u8]) -> Result<Vec<u8>> {
        let frame_size = (WAL_FRAME_HEADER_SIZE + self.page_size) as usize;
        if primary_frame.len() != frame_size {
            return Err(crate::error::Error::SqliteInvalidWalHeaderError(
                "Invalid frame size",
            ));
        }

        // Parse frame header from primary
        let page_num = u32::from_be_bytes(primary_frame[0..4].try_into().unwrap());
        let db_size = u32::from_be_bytes(primary_frame[4..8].try_into().unwrap());

        // Extract page data (after 24-byte frame header)
        let page_data = &primary_frame[WAL_FRAME_HEADER_SIZE as usize..];

        // Build new frame header with local salts
        let mut frame_header = vec![0u8; WAL_FRAME_HEADER_SIZE as usize];
        frame_header[0..4].copy_from_slice(&page_num.to_be_bytes());
        frame_header[4..8].copy_from_slice(&db_size.to_be_bytes());
        frame_header[8..12].copy_from_slice(&self.salt1.to_be_bytes());
        frame_header[12..16].copy_from_slice(&self.salt2.to_be_bytes());
        // Checksums will be filled after calculation

        // Calculate checksum: chain from last checksum
        // Checksum covers: frame header (first 8 bytes) + page data
        let mut checksum_data = Vec::with_capacity(8 + page_data.len());
        checksum_data.extend_from_slice(&frame_header[0..8]);
        checksum_data.extend_from_slice(page_data);

        let (s1, s2) = checksum(
            &checksum_data,
            self.last_checksum.0,
            self.last_checksum.1,
            self.is_big_endian,
        );

        frame_header[16..20].copy_from_slice(&s1.to_be_bytes());
        frame_header[20..24].copy_from_slice(&s2.to_be_bytes());

        // Update last checksum for next frame
        self.last_checksum = (s1, s2);

        // Combine header and page data
        let mut resigned_frame = frame_header;
        resigned_frame.extend_from_slice(page_data);

        Ok(resigned_frame)
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

        // Re-sign the frame with local salts and checksums
        let resigned_frame = self.resign_frame(&data)?;

        self.frame_count += 1;

        // Write to file
        let offset = self.file.metadata()?.len();
        println!(
            "WalWriter: Writing resigned frame #{} (len={}) at offset {}",
            self.frame_count,
            resigned_frame.len(),
            offset
        );

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&resigned_frame)?;
        self.file.sync_all()?;

        println!(
            "WalWriter: Frame written, file len now={}",
            self.file.metadata()?.len()
        );

        Ok(())
    }
}

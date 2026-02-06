use std::fs::{File, OpenOptions};
use std::io::Cursor;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WALHeader, checksum, from_be_bytes_at};
use crate::sync::replication::WalPacket;
use crate::sync::replication::wal_packet::Payload;

mod checkpoint;
mod init;
mod write;

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
        let db_path = wal_path
            .to_str()
            .and_then(|path| path.strip_suffix("-wal"))
            .map(PathBuf::from)
            .ok_or_else(|| {
                crate::error::Error::InvalidPath(format!(
                    "wal path {} is not valid UTF-8 or missing -wal suffix",
                    wal_path.display(),
                ))
            })?;

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&wal_path)?;

        let mut wal_writer = Self {
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
            Err(e) => log::error!("WalWriter: Failed to open pinned connection: {e}"),
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
                    // CRITICAL: Checksums are ALWAYS stored in BIG-ENDIAN per SQLite spec
                    let c1 = from_be_bytes_at(&frame_hdr, 16)?;
                    let c2 = from_be_bytes_at(&frame_hdr, 20)?;

                    wal_writer.last_checksum = (c1, c2);

                    log::debug!("WalWriter: Recovered last_checksum=({c1:#x}, {c2:#x})");
                }
            }

            wal_writer.file.seek(SeekFrom::End(0))?;

            log::debug!(
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
}

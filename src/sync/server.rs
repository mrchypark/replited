use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::config::{Config, DbConfig};
use crate::pb::replication::replication_server::Replication;
use crate::pb::replication::snapshot_error::Code;
use crate::pb::replication::snapshot_response::Payload;
use crate::pb::replication::{
    Handshake, RestoreConfig, RestoreRequest, SnapshotError, SnapshotRequest, SnapshotResponse,
    WalPacket,
};
use crate::sqlite::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALFrame, WALHeader, align_frame, checksum,
};

use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
    snapshot_semaphores: HashMap<String, Arc<Semaphore>>,
    _connections: Arc<Mutex<Vec<rusqlite::Connection>>>, // Keep connections open to pin WAL files
}

impl ReplicationServer {
    pub fn new(config: Config) -> Self {
        let mut db_paths = HashMap::new();
        let mut db_configs = HashMap::new();
        let mut snapshot_semaphores = HashMap::new();
        let mut _connections = Vec::new();

        for db in config.database {
            let path = PathBuf::from(&db.db);
            db_paths.insert(db.db.clone(), path.clone());
            snapshot_semaphores.insert(
                db.db.clone(),
                Arc::new(Semaphore::new(db.max_concurrent_snapshots)),
            );
            db_configs.insert(db.db.clone(), db);

            // Open a persistent connection to pin the WAL file
            if let Ok(conn) = rusqlite::Connection::open(&path) {
                // Set WAL mode to ensure it's active
                if let Err(e) = conn.pragma_update(None, "journal_mode", "WAL") {
                    eprintln!(
                        "Primary: Failed to set WAL mode for {}: {}",
                        path.display(),
                        e
                    );
                }
                _connections.push(conn);
                eprintln!(
                    "Primary: Opened persistent connection for {}",
                    path.display()
                );
            } else {
                eprintln!(
                    "Primary: Failed to open persistent connection for {}",
                    path.display()
                );
            }
        }
        Self {
            db_paths,
            db_configs,
            snapshot_semaphores,
            _connections: Arc::new(Mutex::new(_connections)),
        }
    }
}

#[tonic::async_trait]
impl Replication for ReplicationServer {
    type StreamWalStream = ReceiverStream<Result<WalPacket, Status>>;

    async fn get_restore_config(
        &self,
        request: Request<RestoreRequest>,
    ) -> Result<Response<RestoreConfig>, Status> {
        let req = request.into_inner();
        eprintln!(
            "Primary: Received get_restore_config request for db: {}",
            req.db_name
        );
        let db_config = self.db_configs.get(&req.db_name).ok_or_else(|| {
            eprintln!("Primary: Database {} not found", req.db_name);
            Status::not_found(format!("Database {} not found", req.db_name))
        })?;

        let config_json = serde_json::to_string(db_config).map_err(|e| {
            eprintln!("Primary: Failed to serialize config: {e}");
            Status::internal(format!("Failed to serialize config: {e}"))
        })?;

        eprintln!("Primary: Sending restore config");
        Ok(Response::new(RestoreConfig { config_json }))
    }

    // Stream WAL by directly tailing the WAL file. Shadow WAL is bypassed.
    async fn stream_wal(
        &self,
        request: Request<Handshake>,
    ) -> Result<Response<Self::StreamWalStream>, Status> {
        let handshake = request.into_inner();
        eprintln!("Received handshake: {:?}", handshake);

        let db_path = self
            .db_paths
            .get(&handshake.db_name)
            .ok_or_else(|| Status::not_found(format!("Database {} not found", handshake.db_name)))?
            .clone();

        let mut offset = if handshake.offset == 0 {
            crate::sqlite::WAL_HEADER_SIZE
        } else {
            handshake.offset
        };

        let (tx, rx) = mpsc::channel(1);
        eprintln!("Primary: Created channel, spawning reader task (live WAL tail)...");

        tokio::spawn(async move {
            eprintln!("Primary: Reader task started");
            let wal_path = format!("{}-wal", db_path.display());
            let mut sent_header = false;
            let mut last_checksum: Option<(u32, u32)> = None;

            loop {
                // Read WAL header to get page size (handle WAL reset)
                let mut wal_header = match crate::sqlite::WALHeader::read(&wal_path) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("Primary: Failed to read WAL header: {e}.");
                        // If we can't read header yet (e.g. empty file at start), we can't stream.
                        // Retry loop or continue?
                        // For simplicity, sleep and continue
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };
                let page_size = wal_header.page_size;
                let header_seed = checksum(
                    &wal_header.to_bytes()[0..24],
                    0,
                    0,
                    wal_header.is_big_endian,
                );

                // Send WAL header first if not sent or if we are at the beginning.
                if !sent_header || offset == crate::sqlite::WAL_HEADER_SIZE {
                    let header_bytes = wal_header.to_bytes();
                    eprintln!(
                        "Primary: Sending WAL header packet ({} bytes)",
                        header_bytes.len()
                    );
                    let header_packet = WalPacket {
                        payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                            header_bytes,
                        )),
                    };
                    if let Err(e) = tx.send(Ok(header_packet)).await {
                        eprintln!("Primary: Failed to send header: {e}");
                        return;
                    }
                    sent_header = true;
                    last_checksum = Some(header_seed);
                    // Always realign offset to start of frames after a header resend.
                    offset = WAL_HEADER_SIZE;
                }

                // Check for WAL Truncation / Restart by Header Salt mismatch.
                // Reading header is cheap (fs cache).
                // If salt changes, the WAL has been reset even if size is similar.
                // If we can't read the header (EOF), it's also a Reset (Empty file).
                let mut header_buf = [0u8; 32];
                let file_result = File::open(&wal_path);

                if let Ok(mut file_handle) = file_result {
                    match file_handle.read_exact(&mut header_buf) {
                        Ok(_) => {
                            // WALHeader::read always reads salts as Big Endian (swapped on LE machines).
                            // We must match that behavior here to avoid false mismatches.
                            let new_salt1 =
                                u32::from_be_bytes(header_buf[16..20].try_into().unwrap());

                            if new_salt1 != wal_header.salt1 {
                                eprintln!(
                                    "Primary: WAL Salt Changed! Old={:#x}, New={:#x}. Resetting...",
                                    wal_header.salt1, new_salt1
                                );
                                // Reload header to update "current" salts
                                let mut cursor = std::io::Cursor::new(&header_buf);
                                if let Ok(new_header) =
                                    crate::sqlite::WALHeader::read_from(&mut cursor)
                                {
                                    // Update wal_header with the new header from disk
                                    let _ = new_header; // Acknowledge read but header will be refreshed in next loop iteration
                                }
                                // Update salt to prevent infinite loop on salt mismatch
                                wal_header.salt1 = new_salt1;

                                offset = WAL_HEADER_SIZE;
                                sent_header = false;
                                last_checksum = None;
                                continue;
                            }
                        }
                        Err(e) => {
                            // If we are expecting a valid WAL (offset > header), and can't read header -> Truncated/Deleted.
                            if offset > WAL_HEADER_SIZE {
                                eprintln!(
                                    "Primary: Failed to read WAL Header ({e}). Assuming Truncate/Reset."
                                );
                                offset = WAL_HEADER_SIZE;
                                sent_header = false;
                                last_checksum = None;

                                // Header will be re-read at the start of the next loop iteration

                                continue;
                            }
                        }
                    }
                } else if offset > WAL_HEADER_SIZE {
                    // Start from scratch check
                    eprintln!("Primary: Failed to open WAL file. Assuming Truncate/Reset.");
                    offset = WAL_HEADER_SIZE;
                    sent_header = false;
                    last_checksum = None;
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                let wal_len = match std::fs::metadata(&wal_path) {
                    Ok(meta) => align_frame(page_size, meta.len()),
                    Err(e) => {
                        eprintln!("Primary: wal metadata error: {e}");
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                if wal_len < offset {
                    // WAL was truncated (Checkpoint ran) - Size Check.
                    // This implies we missed data (gap in replication) unless we handle it.
                    // Ideally we should trigger a Full Resync (Snapshot).
                    // For now, we reset offset and hope the new data covers it, or warn loudly.
                    eprintln!(
                        "Primary: WAL Truncated! (len {wal_len} < offset {offset}). Resetting to header..."
                    );

                    // Reset to header
                    offset = WAL_HEADER_SIZE;
                    sent_header = false;
                    last_checksum = None;

                    // Also update our cached header so we don't loop on salt check
                    let mut head = [0u8; 32];
                    // Header will be re-read at the start of the next loop iteration
                    let _ = File::open(&wal_path).and_then(|mut f| f.read_exact(&mut head));

                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                if wal_len == offset {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                if let Ok(meta) = std::fs::metadata(&wal_path) {
                    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    eprintln!(
                        "Primary: streaming wal tail offset {offset} -> {wal_len} (len {wal_len}), mtime={mtime:?}"
                    );
                }

                if let Ok(mut f) = std::fs::File::open(&wal_path) {
                    use std::io::{Seek, SeekFrom};
                    if f.seek(SeekFrom::Start(offset)).is_err() {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    while offset < wal_len {
                        let frame = match WALFrame::read(&mut f, page_size) {
                            Ok(frame) => frame,
                            Err(e) => {
                                eprintln!("Primary: WAL read error at offset {offset}: {e}");
                                break;
                            }
                        };

                        // Validate salt and checksum to avoid streaming partially written frames.
                        if frame.salt1 != wal_header.salt1 || frame.salt2 != wal_header.salt2 {
                            // Check if the header has actually changed on disk
                            let current_header =
                                WALHeader::read(&wal_path).unwrap_or(wal_header.clone());
                            if current_header.salt1 != wal_header.salt1
                                || current_header.salt2 != wal_header.salt2
                            {
                                eprintln!(
                                    "Primary: WAL header changed (new salt), resetting stream"
                                );
                                sent_header = false;
                                last_checksum = None;
                                offset = WAL_HEADER_SIZE;
                                break;
                            } else {
                                // Header is same, so this frame is just garbage/stale data from previous usage.
                                // Treat as EOF (waiting for overwrite).
                                eprintln!(
                                    "Primary: Encountered stale frame (salt mismatch) at offset {offset}, waiting for overwrite..."
                                );
                                break;
                            }
                        }

                        let seeds = last_checksum.unwrap_or(header_seed);
                        let frame_bytes = frame.to_bytes();
                        let mut computed = checksum(
                            &frame_bytes[0..8],
                            seeds.0,
                            seeds.1,
                            wal_header.is_big_endian,
                        );
                        computed = checksum(
                            &frame_bytes[WAL_FRAME_HEADER_SIZE as usize..],
                            computed.0,
                            computed.1,
                            wal_header.is_big_endian,
                        );
                        if computed.0 != frame.checksum1 || computed.1 != frame.checksum2 {
                            // Similar check for checksum mismatch
                            let current_header =
                                WALHeader::read(&wal_path).unwrap_or(wal_header.clone());
                            if current_header.salt1 != wal_header.salt1
                                || current_header.salt2 != wal_header.salt2
                            {
                                eprintln!(
                                    "Primary: WAL header changed (during checksum check), resetting stream"
                                );
                                sent_header = false;
                                last_checksum = None;
                                offset = WAL_HEADER_SIZE;
                                break;
                            } else {
                                eprintln!(
                                    "Primary: WAL checksum mismatch at offset {offset} (stale data?), waiting..."
                                );
                                break;
                            }
                        }

                        last_checksum = Some(computed);
                        offset += WAL_FRAME_HEADER_SIZE + page_size;
                        let packet = WalPacket {
                            payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                                frame_bytes,
                            )),
                        };
                        if let Err(e) = tx.send(Ok(packet)).await {
                            eprintln!("Primary: Failed to send packet: {e}");
                            return;
                        }
                    }
                }
            }
        });

        eprintln!("Primary: Returning stream response");
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StreamSnapshotStream = ReceiverStream<Result<SnapshotResponse, Status>>;

    async fn stream_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.clone();

        let semaphore = self
            .snapshot_semaphores
            .get(&db_name)
            .ok_or_else(|| Status::not_found(format!("Database {db_name} not found")))?
            .clone();

        let db_path = self
            .db_paths
            .get(&db_name)
            .ok_or_else(|| Status::not_found(format!("Database {db_name} not found")))?
            .clone();

        // Try acquire permit
        let permit = match semaphore.try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let (tx, rx) = mpsc::channel(1);
                let _ = tx
                    .send(Ok(SnapshotResponse {
                        payload: Some(Payload::Error(SnapshotError {
                            code: Code::Busy as i32,
                            message: "Too many concurrent snapshots".to_string(),
                            retry_after_ms: 1000,
                        })),
                    }))
                    .await;
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        };

        let (tx, rx) = mpsc::channel(10);

        tokio::spawn(async move {
            let _permit = permit; // Hold permit until task finishes

            // 1. Checkpoint (TRUNCATE) to flush WAL to DB
            {
                match rusqlite::Connection::open(&db_path) {
                    Ok(conn) => {
                        if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                            eprintln!("Primary: Checkpoint failed: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("Primary: Failed to open DB for checkpoint: {e}");
                    }
                }
            }

            // 2. Create temp snapshot file (lz4)
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let snapshot_path = std::env::temp_dir().join(format!("snapshot_{timestamp}.zst"));

            let compression_result = (|| -> std::io::Result<()> {
                let mut source = std::fs::File::open(&db_path)?;
                let mut target = std::fs::File::create(&snapshot_path)?;
                zstd::stream::copy_encode(&mut source, &mut target, 3)?;
                Ok(())
            })();

            match compression_result {
                Ok(_) => {
                    // 3. Stream file
                    match std::fs::File::open(&snapshot_path) {
                        Ok(mut f) => {
                            let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunk
                            loop {
                                use std::io::Read;
                                match f.read(&mut buffer) {
                                    Ok(0) => break, // EOF
                                    Ok(n) => {
                                        let chunk = buffer[0..n].to_vec();
                                        if tx
                                            .send(Ok(SnapshotResponse {
                                                payload: Some(Payload::Chunk(chunk)),
                                            }))
                                            .await
                                            .is_err()
                                        {
                                            break; // Receiver dropped
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Primary: Failed to read snapshot file: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Primary: Failed to open snapshot file: {e}");
                        }
                    }
                    // Cleanup
                    let _ = std::fs::remove_file(snapshot_path);
                }
                Err(e) => {
                    eprintln!("Primary: zstd compression failed: {e}");
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

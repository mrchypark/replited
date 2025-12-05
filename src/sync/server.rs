use std::collections::HashMap;
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

use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
    snapshot_semaphores: HashMap<String, Arc<Semaphore>>,
}

impl ReplicationServer {
    pub fn new(config: Config) -> Self {
        let mut db_paths = HashMap::new();
        let mut db_configs = HashMap::new();
        let mut snapshot_semaphores = HashMap::new();

        for db in config.database {
            db_paths.insert(db.db.clone(), PathBuf::from(&db.db));
            snapshot_semaphores.insert(
                db.db.clone(),
                Arc::new(Semaphore::new(db.max_concurrent_snapshots)),
            );
            db_configs.insert(db.db.clone(), db);
        }
        Self {
            db_paths,
            db_configs,
            snapshot_semaphores,
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
            eprintln!("Primary: Failed to serialize config: {}", e);
            Status::internal(format!("Failed to serialize config: {}", e))
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
                let wal_header = match WALHeader::read(&wal_path) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("Primary: Failed to read WAL header: {}", e);
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
                        eprintln!("Primary: Failed to send header: {}", e);
                        return;
                    }
                    sent_header = true;
                    last_checksum = Some(header_seed);
                    // Always realign offset to start of frames after a header resend.
                    offset = WAL_HEADER_SIZE;
                }

                let wal_len = match std::fs::metadata(&wal_path) {
                    Ok(meta) => align_frame(page_size, meta.len()),
                    Err(e) => {
                        eprintln!("Primary: wal metadata error: {}", e);
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                if wal_len <= offset {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                if let Ok(meta) = std::fs::metadata(&wal_path) {
                    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    eprintln!(
                        "Primary: streaming wal tail offset {} -> {} (len {}), mtime={:?}",
                        offset, wal_len, wal_len, mtime
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
                                eprintln!("Primary: WAL read error at offset {}: {}", offset, e);
                                break;
                            }
                        };

                        // Validate salt and checksum to avoid streaming partially written frames.
                        if frame.salt1 != wal_header.salt1 || frame.salt2 != wal_header.salt2 {
                            eprintln!("Primary: WAL salt mismatch detected, resetting stream");
                            sent_header = false;
                            last_checksum = None;
                            offset = WAL_HEADER_SIZE;
                            break;
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
                            eprintln!(
                                "Primary: WAL checksum mismatch at offset {}, resetting stream",
                                offset
                            );
                            sent_header = false;
                            last_checksum = None;
                            offset = WAL_HEADER_SIZE;
                            break;
                        }

                        last_checksum = Some(computed);
                        offset += WAL_FRAME_HEADER_SIZE + page_size;
                        let packet = WalPacket {
                            payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                                frame_bytes,
                            )),
                        };
                        if let Err(e) = tx.send(Ok(packet)).await {
                            eprintln!("Primary: Failed to send packet: {}", e);
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
            .ok_or_else(|| Status::not_found(format!("Database {} not found", db_name)))?
            .clone();

        let db_path = self
            .db_paths
            .get(&db_name)
            .ok_or_else(|| Status::not_found(format!("Database {} not found", db_name)))?
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
                            eprintln!("Primary: Checkpoint failed: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Primary: Failed to open DB for checkpoint: {}", e);
                    }
                }
            }

            // 2. Create temp snapshot file (lz4)
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let snapshot_path = std::env::temp_dir().join(format!("snapshot_{}.zst", timestamp));

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
                                        if let Err(_) = tx
                                            .send(Ok(SnapshotResponse {
                                                payload: Some(Payload::Chunk(chunk)),
                                            }))
                                            .await
                                        {
                                            break; // Receiver dropped
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Primary: Failed to read snapshot file: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Primary: Failed to open snapshot file: {}", e);
                        }
                    }
                    // Cleanup
                    let _ = std::fs::remove_file(snapshot_path);
                }
                Err(e) => {
                    eprintln!("Primary: zstd compression failed: {}", e);
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

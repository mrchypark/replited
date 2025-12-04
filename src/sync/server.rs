use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::config::{Config, DbConfig};
use crate::pb::replication::replication_server::Replication;
use crate::pb::replication::{Handshake, RestoreConfig, RestoreRequest, WalPacket};
use crate::sqlite::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALFrame, WALHeader, align_frame, checksum,
};

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
}

impl ReplicationServer {
    pub fn new(config: Config) -> Self {
        let mut db_paths = HashMap::new();
        let mut db_configs = HashMap::new();
        for db in config.database {
            db_paths.insert(db.db.clone(), PathBuf::from(&db.db));
            db_configs.insert(db.db.clone(), db);
        }
        Self {
            db_paths,
            db_configs,
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
}

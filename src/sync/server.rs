use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use log::error;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::base::Generation;
use crate::config::Config;
use crate::config::DbConfig;
use crate::database::DatabaseInfo;
use crate::database::WalGenerationPos;
use crate::pb::replication::Handshake;
use crate::pb::replication::RestoreConfig;
use crate::pb::replication::RestoreRequest;
use crate::pb::replication::WalPacket;
use crate::pb::replication::replication_server::Replication;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sync::ShadowWalReader;

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

        // Serialize DbConfig to JSON
        let config_json = serde_json::to_string(db_config).map_err(|e| {
            eprintln!("Primary: Failed to serialize config: {}", e);
            Status::internal(format!("Failed to serialize config: {}", e))
        })?;

        eprintln!("Primary: Sending restore config");
        Ok(Response::new(RestoreConfig { config_json }))
    }

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

        let generation = if handshake.generation.is_empty() {
            let file_path = std::path::Path::new(&db_path);
            let db_name = file_path.file_name().unwrap().to_str().unwrap();
            let dir_path = match file_path.parent() {
                Some(p) if p == std::path::Path::new("") => std::path::Path::new("."),
                Some(p) => p,
                None => std::path::Path::new("."),
            };
            let meta_dir = format!("{}/.{}-replited/", dir_path.to_str().unwrap(), db_name);
            let generation_file = std::path::Path::new(&meta_dir).join("generation");

            if !generation_file.exists() {
                return Err(Status::unavailable("Generation file not found"));
            }

            let generation_str = std::fs::read_to_string(generation_file)
                .map_err(|e| Status::internal(format!("Failed to read generation file: {}", e)))?;
            Generation::try_create(&generation_str)
                .map_err(|e| Status::internal(format!("Invalid generation in file: {}", e)))?
        } else {
            Generation::try_create(&handshake.generation).map_err(|e| {
                Status::invalid_argument(format!(
                    "Invalid generation format: '{}', error: {}",
                    handshake.generation, e
                ))
            })?
        };
        let start_pos = WalGenerationPos {
            generation,
            index: handshake.index,
            // When offset=0, skip the 32-byte WAL header since we send it separately
            offset: if handshake.offset == 0 {
                crate::sqlite::WAL_HEADER_SIZE
            } else {
                handshake.offset
            },
        };

        let (tx, rx) = mpsc::channel(1);
        eprintln!("Primary: Created channel, spawning reader task...");

        tokio::spawn(async move {
            eprintln!("Primary: Reader task started");

            // Read WAL header to get page size
            let wal_path = format!("{}-wal", db_path.display());
            let wal_header = match WALHeader::read(&wal_path) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("Primary: Failed to read WAL header: {}", e);
                    let _ = tx
                        .send(Err(Status::internal(format!(
                            "Failed to read WAL header: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            };

            // Send WAL header first
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

            // Prepare DatabaseInfo for ShadowWalReader
            let file_path = std::path::Path::new(&db_path);
            let db_name = file_path.file_name().unwrap().to_str().unwrap();
            let dir_path = match file_path.parent() {
                Some(p) if p == std::path::Path::new("") => std::path::Path::new("."),
                Some(p) => p,
                None => std::path::Path::new("."),
            };
            let meta_dir = format!("{}/.{}-replited/", dir_path.to_str().unwrap(), db_name);

            let info = DatabaseInfo {
                meta_dir,
                page_size: wal_header.page_size,
            };

            // Wait for generation file
            let generation_dir = db_path.parent().unwrap().join(".data.db-replited");
            let generation_file_path = generation_dir.join("generation");

            if !generation_file_path.exists() {
                let _ = tx
                    .send(Err(Status::unavailable("Generation file not found")))
                    .await;
                return;
            }

            let mut reader = match ShadowWalReader::new(start_pos, &info) {
                Ok(reader) => {
                    eprintln!("Primary: ShadowWalReader created, left={}", reader.left());
                    reader
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                    return;
                }
            };

            loop {
                if reader.left() == 0 {
                    // Simple polling for now to wait for more data.
                    sleep(Duration::from_millis(100)).await;

                    // Try to re-create reader from current pos to refresh metadata (check for new data).
                    let pos = reader.position();
                    match ShadowWalReader::new(pos, &info) {
                        Ok(r) => reader = r,
                        Err(_) => continue, // Wait more
                    }
                    if reader.left() == 0 {
                        continue;
                    }
                }

                let wal_frame = match WALFrame::read(&mut reader, info.page_size) {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Error reading WAL frame: {}", e);
                        break;
                    }
                };

                eprintln!("Primary: Sending packet...");
                let packet = WalPacket {
                    payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                        wal_frame.to_bytes(),
                    )),
                };
                if let Err(e) = tx.send(Ok(packet)).await {
                    eprintln!("Primary: Failed to send packet: {}", e);
                    break;
                }
                eprintln!("Primary: Packet sent.");
            }
            eprintln!("Primary: Reader task ending");
        });

        eprintln!("Primary: Returning stream response");
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

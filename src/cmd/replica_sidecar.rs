use std::path::Path;
use std::time::Duration;

use log::info;
use log::warn;
use tokio::time::sleep;

use crate::config::Config;
use crate::config::RestoreOptions;
use crate::config::StorageParams;
use crate::error::Result;
use crate::sync::stream_client::StreamClient;

use crate::sync::replication::Handshake;

#[derive(Debug)]
enum ReplicaState {
    Empty,
    Restoring,
    Streaming,
}

pub struct ReplicaSidecar {
    config: Config,
}

impl ReplicaSidecar {
    pub fn try_create(config_path: &str) -> Result<Self> {
        let config = Config::load(config_path)?;
        crate::log::init_log(config.log.clone())?;
        Ok(Self { config })
    }

    fn get_local_wal_state(db_path: &str) -> Result<(String, u64, u64, u64)> {
        let wal_path = format!("{}-wal", db_path);
        if !Path::new(&wal_path).exists() {
            return Ok(("".to_string(), 0, 0, 4096)); // Default if no WAL
        }

        let file_len = std::fs::metadata(&wal_path)?.len();
        if file_len == 0 {
            return Ok(("".to_string(), 0, 0, 4096));
        }

        let wal_header = crate::sqlite::WALHeader::read(&wal_path)?;
        let page_size = wal_header.page_size;

        Ok(("".to_string(), 0, file_len, page_size))
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("ReplicaSidecar::run start");
        let mut state = ReplicaState::Empty;
        println!("ReplicaSidecar::run accessing db_config");
        let db_config = &self.config.database[0]; // FIXME: Support multiple DBs?
        let db_path = &db_config.db;
        println!("ReplicaSidecar::run db_path: {}", db_path);

        // Find stream config
        println!("ReplicaSidecar::run finding stream_config");
        let stream_config = db_config
            .replicate
            .iter()
            .find_map(|r| {
                if let StorageParams::Stream(s) = &r.params {
                    Some(s)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                crate::error::Error::InvalidConfig("No stream config found".to_string())
            })?;
        println!("ReplicaSidecar::run stream_config found");

        loop {
            println!("ReplicaSidecar::run loop state: {:?}", state);
            match state {
                ReplicaState::Empty => {
                    let path = std::path::Path::new(db_path);
                    let should_restore = !path.exists();
                    println!(
                        "ReplicaSidecar::run Empty state, exists: {}, should_restore: {}",
                        path.exists(),
                        should_restore
                    );
                    info!(
                        "db_path: {:?}, exists: {}, should_restore: {}",
                        path,
                        path.exists(),
                        should_restore
                    );

                    if !should_restore {
                        println!("ReplicaSidecar::run Local DB found. Switching to Streaming.");
                        info!(
                            "Local DB found at {}. Switching to Streaming mode.",
                            db_path
                        );
                        state = ReplicaState::Streaming;
                    } else {
                        println!(
                            "ReplicaSidecar::run Local DB not found. Connecting to Primary..."
                        );
                        info!(
                            "Local DB not found at {}. Connecting to Primary to get restore config...",
                            db_path
                        );

                        match StreamClient::connect(stream_config.addr.clone()).await {
                            Ok(client) => {
                                println!(
                                    "ReplicaSidecar::run Connected to Primary. Requesting config..."
                                );
                                match client.get_restore_config(db_config.db.clone()).await {
                                    Ok(restore_db_config) => {
                                        println!(
                                            "ReplicaSidecar::run Received restore config. Starting restore..."
                                        );
                                        info!("Received restore config. Starting restore...");
                                        let restore_options = RestoreOptions {
                                            db: db_config.db.clone(),
                                            output: db_config.db.clone(),
                                            follow: false,
                                            interval: 1,
                                            timestamp: "".to_string(),
                                        };

                                        if let Err(e) = crate::sync::run_restore(
                                            &restore_db_config,
                                            &restore_options,
                                        )
                                        .await
                                        {
                                            warn!("Restore failed: {}. Retrying in 5s...", e);
                                            sleep(Duration::from_secs(5)).await;
                                            continue;
                                        }
                                        info!("Bootstrap finished. Switching to Stream mode.");
                                        state = ReplicaState::Streaming;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to get restore config: {}. Retrying in 5s...",
                                            e
                                        );
                                        sleep(Duration::from_secs(5)).await;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to connect to Primary: {}. Retrying in 5s...", e);
                                sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                }
                ReplicaState::Restoring => {
                    // Merged into Empty
                    state = ReplicaState::Empty;
                }
                ReplicaState::Streaming => {
                    // 1. Get current position
                    let (generation, index, offset, page_size) =
                        Self::get_local_wal_state(db_path)?;
                    info!("Resuming replication from offset: {}", offset);

                    info!("Connecting to Primary at {}", stream_config.addr);
                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            let handshake = Handshake {
                                db_name: db_config.db.clone(),
                                generation,
                                offset,
                                index,
                            };

                            match client.stream_wal(handshake).await {
                                Ok(mut stream) => {
                                    eprintln!("Replica: Stream connected. Creating WalWriter...");
                                    let wal_path = format!("{}-wal", db_path);
                                    let mut writer = crate::sync::WalWriter::new(
                                        std::path::PathBuf::from(wal_path),
                                        page_size,
                                    )?;
                                    eprintln!("Replica: WalWriter created.");

                                    loop {
                                        eprintln!("Replica: Waiting for message...");
                                        let packet = match tokio::time::timeout(
                                            Duration::from_secs(2),
                                            stream.message(),
                                        )
                                        .await
                                        {
                                            Ok(Ok(Some(p))) => {
                                                eprintln!("Replica: Received WAL packet");
                                                info!("Received WAL packet");
                                                p
                                            }
                                            Ok(Ok(None)) => {
                                                eprintln!("Replica: Stream ended");
                                                break;
                                            }
                                            Ok(Err(e)) => {
                                                eprintln!("Replica: Stream error: {}", e);
                                                warn!("Stream error: {}", e);
                                                break;
                                            }
                                            Err(_) => {
                                                eprintln!("Replica: Stream timeout");
                                                warn!("Stream timeout");
                                                continue;
                                            }
                                        };

                                        if let Err(e) = writer.apply(packet) {
                                            warn!("Failed to write WAL packet: {}", e);
                                            break;
                                        }
                                    }
                                    warn!("Stream disconnected. Retrying...");
                                }
                                Err(e) => {
                                    warn!("Failed to stream: {}. Retrying...", e);
                                    sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to connect: {}. Retrying...", e);
                            sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl super::command::Command for ReplicaSidecar {
    async fn run(&mut self) -> Result<()> {
        self.run().await
    }
}

use std::path::Path;
use std::time::Duration;

use log::info;
use log::warn;
use tokio::time::sleep;

use crate::base::Generation;
use crate::config::Config;
use crate::config::RestoreOptions;
use crate::config::StorageParams;
use crate::database::WalGenerationPos;
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
    force_restore: bool,
}

impl ReplicaSidecar {
    pub fn try_create(config_path: &str, force_restore: bool) -> Result<Self> {
        let config = Config::load(config_path)?;
        crate::log::init_log(config.log.clone())?;
        Ok(Self {
            config,
            force_restore,
        })
    }

    fn read_generation(db_path: &str) -> Generation {
        let file_path = std::path::Path::new(db_path);
        let db_name = file_path.file_name().and_then(|p| p.to_str()).unwrap_or("");
        let dir_path = match file_path.parent() {
            Some(p) if p == std::path::Path::new("") => std::path::Path::new("."),
            Some(p) => p,
            None => std::path::Path::new("."),
        };
        let meta_dir = format!(
            "{}/.{}-replited/",
            dir_path.to_str().unwrap_or("."),
            db_name
        );
        let generation_file = std::path::Path::new(&meta_dir).join("generation");

        if let Ok(content) = std::fs::read_to_string(generation_file) {
            if let Ok(parsed) = Generation::try_create(content.trim()) {
                return parsed;
            }
        }

        Generation::default()
    }

    fn get_local_wal_state(db_path: &str) -> Result<(WalGenerationPos, u64)> {
        let wal_path = format!("{}-wal", db_path);
        if !Path::new(&wal_path).exists() {
            return Ok((
                WalGenerationPos {
                    generation: Self::read_generation(db_path),
                    index: 0,
                    offset: 0,
                },
                4096,
            ));
        }

        let file_len = std::fs::metadata(&wal_path)?.len();
        if file_len == 0 {
            return Ok((
                WalGenerationPos {
                    generation: Self::read_generation(db_path),
                    index: 0,
                    offset: 0,
                },
                4096,
            ));
        }

        let wal_header = crate::sqlite::WALHeader::read(&wal_path)?;
        let page_size = wal_header.page_size;

        Ok((
            WalGenerationPos {
                generation: Self::read_generation(db_path),
                index: 0,
                offset: file_len,
            },
            page_size,
        ))
    }

    pub async fn run(&mut self) -> Result<()> {
        println!("ReplicaSidecar::run start");
        let mut state = ReplicaState::Empty;
        let mut resume_pos: Option<WalGenerationPos> = None;
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
                    let should_restore = self.force_restore || !path.exists();
                    println!(
                        "ReplicaSidecar::run Empty state, exists: {}, should_restore: {}",
                        path.exists(),
                        should_restore
                    );
                    info!(
                        "db_path: {:?}, exists: {}, should_restore: {}, force_restore: {}",
                        path,
                        path.exists(),
                        should_restore,
                        self.force_restore
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

                        if self.force_restore && path.exists() {
                            let _ = std::fs::remove_file(db_path);
                            let _ = std::fs::remove_file(format!("{}-wal", db_path));
                            let _ = std::fs::remove_file(format!("{}-shm", db_path));
                        }

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

                                        match crate::sync::run_restore(
                                            &restore_db_config,
                                            &restore_options,
                                        )
                                        .await
                                        {
                                            Ok(pos) => {
                                                resume_pos = pos;
                                                info!(
                                                    "Bootstrap finished. Switching to Stream mode. resume_pos={:?}",
                                                    resume_pos
                                                );
                                                state = ReplicaState::Streaming;
                                            }
                                            Err(e) => {
                                                warn!("Restore failed: {}. Retrying in 5s...", e);
                                                sleep(Duration::from_secs(5)).await;
                                                continue;
                                            }
                                        }
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
                    let (pos, page_size) = if let Some(p) = resume_pos.take() {
                        let wal_page_size =
                            crate::sqlite::WALHeader::read(&format!("{}-wal", db_path))
                                .map(|h| h.page_size)
                                .unwrap_or(4096);
                        (p, wal_page_size)
                    } else {
                        Self::get_local_wal_state(db_path)?
                    };

                    info!(
                        "Resuming replication from generation={}, index={}, offset={}, force_restore={}",
                        pos.generation.as_str(),
                        pos.index,
                        pos.offset,
                        self.force_restore
                    );

                    info!("Connecting to Primary at {}", stream_config.addr);
                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            let handshake = Handshake {
                                db_name: db_config.db.clone(),
                                generation: pos.generation.as_str().to_string(),
                                offset: pos.offset,
                                index: pos.index,
                            };

                            match client.stream_wal(handshake).await {
                                Ok(mut stream) => {
                                    eprintln!("Replica: Stream connected. Creating WalWriter...");
                                    let wal_path = format!("{}-wal", db_path);
                                    let mut writer = crate::sync::WalWriter::new(
                                        std::path::PathBuf::from(wal_path),
                                        page_size,
                                        db_config.apply_checkpoint_frame_interval,
                                        std::time::Duration::from_millis(
                                            db_config.apply_checkpoint_interval_ms,
                                        ),
                                    )?;
                                    eprintln!("Replica: WalWriter created.");
                                    let mut timeout_streak = 0usize;

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
                                                timeout_streak = 0;
                                                p
                                            }
                                            Ok(Ok(None)) => {
                                                eprintln!("Replica: Stream ended");
                                                if let Err(e) = writer.checkpoint() {
                                                    warn!(
                                                        "Checkpoint after stream end failed: {}",
                                                        e
                                                    );
                                                }
                                                break;
                                            }
                                            Ok(Err(e)) => {
                                                eprintln!("Replica: Stream error: {}", e);
                                                warn!("Stream error: {}", e);
                                                if let Err(e) = writer.checkpoint() {
                                                    warn!(
                                                        "Checkpoint after stream error failed: {}",
                                                        e
                                                    );
                                                }
                                                break;
                                            }
                                            Err(_) => {
                                                eprintln!("Replica: Stream timeout");
                                                warn!("Stream timeout");
                                                timeout_streak += 1;
                                                if timeout_streak >= 5 {
                                                    if let Err(e) = writer.checkpoint() {
                                                        warn!(
                                                            "Checkpoint after timeouts failed: {}",
                                                            e
                                                        );
                                                    }
                                                    timeout_streak = 0;
                                                }
                                                if timeout_streak >= 10 {
                                                    // Force reconnect to catch up if Primary stopped sending.
                                                    break;
                                                }
                                                continue;
                                            }
                                        };

                                        if let Err(e) = writer.apply(packet) {
                                            warn!("Failed to write WAL packet: {}", e);
                                            if let Err(e) = writer.checkpoint() {
                                                warn!(
                                                    "Checkpoint after write failure failed: {}",
                                                    e
                                                );
                                            }
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

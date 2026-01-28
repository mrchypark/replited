use std::time::Duration;

use log::info;
use log::warn;
use tokio::time::sleep;

use crate::config::Config;
use crate::config::DbConfig;
use crate::config::RestoreOptions;
use crate::config::StorageParams;
use crate::database::WalGenerationPos;
use crate::error::Result;
use crate::sync::stream_client::StreamClient;

use crate::sync::replication::Handshake;

mod local_state;
mod process_manager;

use local_state::get_local_wal_state;
use process_manager::ProcessManager;

#[derive(Debug)]
enum ReplicaState {
    Empty,
    Streaming,
}

pub struct ReplicaSidecar {
    config: Config,
    force_restore: bool,
    exec: Option<String>,
}

impl ReplicaSidecar {
    pub fn try_create(
        config_path: &str,
        force_restore: bool,
        exec: Option<String>,
    ) -> Result<Self> {
        let config = Config::load(config_path)?;
        crate::log::init_log(config.log.clone())?;
        Ok(Self {
            config,
            force_restore,
            exec,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut handles = vec![];

        // Initialize ProcessManager if exec command is provided
        let process_manager = self.exec.clone().map(ProcessManager::new);

        // If we have a process manager, start it initially (unless blocked immediately logic applies, but tasks start async)
        // Actually, we want it running by default.
        if let Some(pm) = &process_manager {
            pm.start().await;
        }

        for db_config in &self.config.database {
            let db_config = db_config.clone();
            let force_restore = self.force_restore;
            let pm = process_manager.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = Self::run_single_db(db_config.clone(), force_restore, pm).await {
                    log::error!("ReplicaSidecar error for db {}: {}", db_config.db, e);
                }
            });
            handles.push(handle);
        }

        for h in handles {
            let _ = h.await;
        }

        Ok(())
    }

    async fn run_single_db(
        db_config: DbConfig,
        mut force_restore: bool,
        process_manager: Option<ProcessManager>,
    ) -> Result<()> {
        log::info!("ReplicaSidecar::run_single_db start for {}", db_config.db);
        let mut state = ReplicaState::Empty;
        let mut resume_pos: Option<WalGenerationPos> = None;
        let db_path = &db_config.db;
        log::debug!("ReplicaSidecar::run_single_db db_path: {db_path}");

        // Auto-Restore Safeguards
        let mut consecutive_restore_count = 0;
        let mut last_restore_time = std::time::Instant::now();
        const MAX_AUTO_RESTORES: u32 = 5;
        const RESTORE_RESET_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(60);

        // Schema change detection
        let mut last_schema_hash: Option<u64> = None;

        // Find stream config
        log::debug!("ReplicaSidecar::run_single_db finding stream_config");
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
        log::debug!("ReplicaSidecar::run_single_db stream_config found");

        let remote_db_name = stream_config
            .remote_db_name
            .clone()
            .unwrap_or_else(|| db_config.db.clone());

        loop {
            log::debug!("ReplicaSidecar::run_single_db loop state: {state:?}");
            match state {
                ReplicaState::Empty => {
                    let path = std::path::Path::new(db_path);
                    let should_restore = force_restore || !path.exists();
                    log::debug!(
                        "ReplicaSidecar::run_single_db Empty state, exists: {}, should_restore: {}",
                        path.exists(),
                        should_restore
                    );
                    info!(
                        "db_path: {:?}, exists: {}, should_restore: {}, force_restore: {}",
                        path,
                        path.exists(),
                        should_restore,
                        force_restore
                    );

                    if !should_restore {
                        log::info!(
                            "ReplicaSidecar::run_single_db Local DB found. Switching to Streaming."
                        );
                        info!("Local DB found at {db_path}. Switching to Streaming mode.");
                        state = ReplicaState::Streaming;
                    } else {
                        log::info!(
                            "ReplicaSidecar::run_single_db Local DB not found. Connecting to Primary..."
                        );
                        info!(
                            "Local DB not found at {db_path}. Connecting to Primary to get restore config..."
                        );

                        // BLOCKING START: Entire restore process should be atomic
                        if let Some(pm) = &process_manager {
                            pm.add_blocker().await;
                        }

                        // Cleanup existing files if forcing restore
                        if force_restore && path.exists() {
                            let _ = std::fs::remove_file(db_path);
                            let _ = std::fs::remove_file(format!("{db_path}-wal"));
                            let _ = std::fs::remove_file(format!("{db_path}-shm"));
                        }

                        match StreamClient::connect(stream_config.addr.clone()).await {
                            Ok(client) => {
                                log::info!("ReplicaSidecar::run_single_db Connected to Primary.");

                                // 1. Try Direct Snapshot Streaming
                                log::info!(
                                    "ReplicaSidecar::run_single_db Attempting Direct Snapshot Streaming..."
                                );
                                let snapshot_path =
                                    std::path::Path::new(db_path).with_extension("snapshot.zst");
                                let direct_restore_success = match client
                                    .download_snapshot(remote_db_name.clone(), &snapshot_path)
                                    .await
                                {
                                    Ok(_) => {
                                        log::info!(
                                            "ReplicaSidecar::run_single_db Snapshot downloaded. Decompressing..."
                                        );
                                        // Decompress zstd
                                        let decompression_result = (|| -> std::io::Result<()> {
                                            let mut source = std::fs::File::open(&snapshot_path)?;
                                            let mut target = std::fs::File::create(db_path)?;
                                            zstd::stream::copy_decode(&mut source, &mut target)?;
                                            Ok(())
                                        })(
                                        );

                                        if decompression_result.is_ok() {
                                            let _ = std::fs::remove_file(snapshot_path);
                                            log::info!(
                                                "ReplicaSidecar::run_single_db Restore success. Switching to Streaming."
                                            );
                                            state = ReplicaState::Streaming;
                                            // Reset force_restore flag after successful restore
                                            force_restore = false;
                                            true
                                        } else {
                                            warn!(
                                                "Decompression failed: {:?}. Falling back to legacy restore...",
                                                decompression_result.err()
                                            );
                                            false
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Direct snapshot failed: {e}. Falling back to legacy restore..."
                                        );
                                        false
                                    }
                                };

                                if direct_restore_success {
                                    // UNBLOCK: Success path
                                    if let Some(pm) = &process_manager {
                                        pm.remove_blocker().await;
                                    }
                                    continue;
                                }

                                log::info!(
                                    "ReplicaSidecar::run_single_db Requesting legacy restore config..."
                                );
                                match client.get_restore_config(remote_db_name.clone()).await {
                                    Ok(restore_db_config) => {
                                        log::info!(
                                            "ReplicaSidecar::run_single_db Received restore config. Starting restore..."
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
                                                    "Bootstrap finished. Switching to Stream mode. resume_pos={resume_pos:?}"
                                                );
                                                state = ReplicaState::Streaming;
                                                // Reset force_restore flag
                                                force_restore = false;
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "ReplicaSidecar::run_single_db Restore failed: {e}"
                                                );
                                                warn!("Restore failed: {e}. Retrying in 5s...");
                                                // UNBLOCK: Failure path
                                                if let Some(pm) = &process_manager {
                                                    pm.remove_blocker().await;
                                                }
                                                sleep(Duration::from_secs(5)).await;
                                                continue;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "ReplicaSidecar::run_single_db Failed to get restore config: {e}"
                                        );
                                        warn!(
                                            "Failed to get restore config: {e}. Retrying in 5s..."
                                        );
                                        // UNBLOCK: Failure path
                                        if let Some(pm) = &process_manager {
                                            pm.remove_blocker().await;
                                        }
                                        sleep(Duration::from_secs(5)).await;
                                        // Continue loop to retry
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "ReplicaSidecar::run_single_db Failed to connect to Primary: {e}"
                                );
                                warn!("Failed to connect to Primary: {e}. Retrying in 5s...");
                                // UNBLOCK: Failure path
                                if let Some(pm) = &process_manager {
                                    pm.remove_blocker().await;
                                }
                                sleep(Duration::from_secs(5)).await;
                                continue;
                            }
                        }

                        // UNBLOCK: End of restore block (fallthrough or success without continue above)
                        if let Some(pm) = &process_manager {
                            pm.remove_blocker().await;
                        }
                    }
                }
                ReplicaState::Streaming => {
                    // 1. Get current position
                    let (pos, page_size) = if let Some(p) = resume_pos.take() {
                        let wal_page_size =
                            crate::sqlite::WALHeader::read(&format!("{db_path}-wal"))
                                .map(|h| h.page_size)
                                .unwrap_or(4096);
                        (p, wal_page_size)
                    } else {
                        get_local_wal_state(db_path)?
                    };

                    info!(
                        "Resuming replication from generation={}, index={}, offset={}, force_restore={}",
                        pos.generation.as_str(),
                        pos.index,
                        pos.offset,
                        force_restore
                    );

                    info!("Connecting to Primary at {}", stream_config.addr);
                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            let handshake = Handshake {
                                db_name: remote_db_name.clone(),
                                generation: pos.generation.as_str().to_string(),
                                offset: pos.offset,
                                index: pos.index,
                            };

                            match client.stream_wal(handshake).await {
                                Ok(mut stream) => {
                                    log::info!("Replica: Stream connected. Creating WalWriter...");
                                    let wal_path = format!("{db_path}-wal");
                                    let mut writer = crate::sync::WalWriter::new(
                                        std::path::PathBuf::from(wal_path),
                                        page_size,
                                        db_config.apply_checkpoint_frame_interval,
                                        std::time::Duration::from_millis(
                                            db_config.apply_checkpoint_interval_ms,
                                        ),
                                    )?;
                                    log::debug!("Replica: WalWriter created.");
                                    let mut timeout_streak = 0usize;

                                    loop {
                                        // Logging can be noisy since this loop runs for every packet.
                                        let packet = match tokio::time::timeout(
                                            Duration::from_secs(2),
                                            stream.message(),
                                        )
                                        .await
                                        {
                                            Ok(Ok(Some(p))) => {
                                                log::debug!("Replica: Received WAL packet");
                                                info!("Received WAL packet");
                                                timeout_streak = 0;
                                                p
                                            }
                                            Ok(Ok(None)) => {
                                                log::warn!("Replica: Stream ended");
                                                if let Err(e) = writer.checkpoint() {
                                                    warn!(
                                                        "Checkpoint after stream end failed: {e}"
                                                    );
                                                }
                                                break;
                                            }
                                            Ok(Err(e)) => {
                                                log::error!("Replica: Stream error: {e}");
                                                warn!("Stream error: {e}");

                                                if let Err(e) = writer.checkpoint() {
                                                    warn!(
                                                        "Checkpoint after stream error failed: {e}"
                                                    );
                                                }
                                                break;
                                            }
                                            Err(_) => {
                                                log::warn!("Replica: Stream timeout");
                                                warn!("Stream timeout");
                                                timeout_streak += 1;
                                                if timeout_streak >= 5 {
                                                    if let Err(e) = writer.checkpoint() {
                                                        warn!(
                                                            "Checkpoint after timeouts failed: {e}"
                                                        );

                                                        // CHECK WAL STUCK HERE (Timeout Block)
                                                        if e.code()
                                                            == crate::error::Error::WAL_STUCK
                                                        {
                                                            log::error!(
                                                                "CRITICAL: WAL Stuck detected during timeout checkpoint."
                                                            );

                                                            // FIRST: Try restarting child process to release locks
                                                            if let Some(pm) = &process_manager {
                                                                log::info!(
                                                                    "WAL Stuck. Restarting child process to release locks..."
                                                                );
                                                                pm.restart().await;

                                                                // Wait a moment for locks to be released
                                                                sleep(Duration::from_millis(500))
                                                                    .await;

                                                                // Retry checkpoint after restart
                                                                if let Err(retry_err) =
                                                                    writer.checkpoint()
                                                                {
                                                                    log::warn!(
                                                                        "Checkpoint still failed after child restart: {retry_err}"
                                                                    );
                                                                    // Continue to full restore below
                                                                } else {
                                                                    // Checkpoint succeeded after restart!
                                                                    log::info!(
                                                                        "Checkpoint succeeded after child process restart."
                                                                    );
                                                                    timeout_streak = 0;
                                                                    continue;
                                                                }
                                                            }

                                                            // SECOND: If restart didn't help, do full restore
                                                            // Update safeguards
                                                            if last_restore_time.elapsed()
                                                                > RESTORE_RESET_THRESHOLD
                                                            {
                                                                consecutive_restore_count = 0;
                                                            }
                                                            consecutive_restore_count += 1;
                                                            last_restore_time =
                                                                std::time::Instant::now();

                                                            if consecutive_restore_count
                                                                > MAX_AUTO_RESTORES
                                                            {
                                                                log::error!(
                                                                    "CRITICAL: Auto-restore limit reached ({MAX_AUTO_RESTORES}). Stopping to prevent loop."
                                                                );
                                                                panic!(
                                                                    "Auto-restore limit reached."
                                                                ); // Let restart policy handle it or just stop
                                                            }

                                                            info!(
                                                                "WAL Stuck. Deleting DB to force restore..."
                                                            );

                                                            // Simplify: Mark state as empty and force restore.
                                                            // The Blocking/Deletion will be handled in the next loop iteration's Restore block.
                                                            state = ReplicaState::Empty;
                                                            force_restore = true;

                                                            // Next loop will see missing file and trigger restore (download)
                                                            continue;
                                                        }
                                                    } else {
                                                        // Checkpoint succeeded - check for schema changes
                                                        if let Some(pm) = &process_manager {
                                                            match crate::sqlite::compute_schema_hash(
                                                                db_path,
                                                            ) {
                                                                Ok(new_hash) => {
                                                                    if let Some(prev_hash) =
                                                                        last_schema_hash
                                                                    {
                                                                        if prev_hash != new_hash {
                                                                            log::info!(
                                                                                "Schema changed (hash: {prev_hash:x} -> {new_hash:x}). Restarting child process..."
                                                                            );
                                                                            pm.restart().await;
                                                                        }
                                                                    } else {
                                                                        log::info!(
                                                                            "Replica: Initial schema hash: {new_hash:x}"
                                                                        );
                                                                    }
                                                                    last_schema_hash =
                                                                        Some(new_hash);
                                                                }
                                                                Err(e) => {
                                                                    log::warn!(
                                                                        "Failed to compute schema hash: {e}"
                                                                    );
                                                                }
                                                            }
                                                        }
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
                                            warn!("Failed to write WAL packet: {e}");

                                            // CHECK WAL STUCK HERE
                                            if e.code() == crate::error::Error::WAL_STUCK {
                                                log::error!("CRITICAL: WAL Stuck detected.");

                                                // Update safeguards
                                                if last_restore_time.elapsed()
                                                    > RESTORE_RESET_THRESHOLD
                                                {
                                                    consecutive_restore_count = 0;
                                                }
                                                consecutive_restore_count += 1;
                                                last_restore_time = std::time::Instant::now();

                                                if consecutive_restore_count > MAX_AUTO_RESTORES {
                                                    log::error!(
                                                        "FATAL: Auto-Restore limit exceeded ({MAX_AUTO_RESTORES}). Aborting to prevent infinite loop."
                                                    );
                                                    panic!("Auto-Restore limit exceeded");
                                                }

                                                log::warn!(
                                                    "Initiating Emergency Auto-Restore (Attempt {consecutive_restore_count}/{MAX_AUTO_RESTORES})..."
                                                );
                                                log::warn!(
                                                    "Replica: WAL Stuck. Deleting DB to force restore..."
                                                );
                                                let _ = std::fs::remove_file(db_path);
                                                let _ =
                                                    std::fs::remove_file(format!("{db_path}-wal"));
                                                let _ =
                                                    std::fs::remove_file(format!("{db_path}-shm"));
                                                state = ReplicaState::Empty;
                                                break; // Break inner loop, outer loop will restart in Empty state
                                            }

                                            if let Err(e) = writer.checkpoint() {
                                                warn!("Checkpoint after write failure failed: {e}");
                                            }
                                            break;
                                        }
                                    }
                                    warn!("Stream disconnected. Retrying...");
                                }
                                Err(e) => {
                                    warn!("Failed to stream: {e}. Retrying...");
                                    sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to connect: {e}. Retrying...");
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

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use log::{info, warn};
use rusqlite::Connection;
use tokio::time::sleep;

use crate::base::{Generation, shadow_wal_dir, shadow_wal_file};
use crate::config::{Config, DbConfig, StorageParams};
use crate::database::WalGenerationPos;
use crate::error::{Error, Result};
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, align_frame};
use crate::sync::replication::stream_error::Code as StreamErrorCode;
use crate::sync::replication::stream_snapshot_v2_response::Payload as StreamSnapshotV2Payload;
use crate::sync::replication::stream_wal_v2_response::Payload as StreamWalV2Payload;
use crate::sync::replication::{
    AckLsnV2Request, LsnToken, StreamError, StreamSnapshotV2Request, StreamWalV2Request,
};
use crate::sync::stream_client::StreamClient;
use crate::sync::{ReplicaRecoveryAction, StreamReplicationErrorCode};

mod local_state;
mod process_manager;

use local_state::{
    ensure_meta_dir, load_or_create_replica_id, persist_last_applied_lsn, read_last_applied_lsn,
};
use process_manager::ProcessManager;

#[derive(Debug)]
enum ReplicaState {
    Bootstrapping,
    CatchingUp,
    Streaming,
    NeedsRestore,
}

#[derive(Debug)]
enum ReplicaStreamError {
    Stream(StreamReplicationErrorCode, String),
    Transport(String),
    InvalidResponse(String),
    Io(String),
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

        let process_manager = self.exec.clone().map(ProcessManager::new);
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
        let db_path = &db_config.db;
        let path = Path::new(db_path);

        let stream_config = db_config
            .replicate
            .iter()
            .find_map(|r| match &r.params {
                StorageParams::Stream(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| Error::InvalidConfig("No stream config found".to_string()))?;

        let remote_db_name = stream_config
            .remote_db_name
            .clone()
            .unwrap_or_else(|| db_config.db.clone());

        let replica_id = load_or_create_replica_id(db_path)?;
        let session_id = format!("replica-sidecar:{}", db_config.db);

        let mut last_applied_lsn = read_last_applied_lsn(db_path)?;
        let mut state = if force_restore || !path.exists() || last_applied_lsn.is_none() {
            ReplicaState::Bootstrapping
        } else {
            ReplicaState::CatchingUp
        };

        let mut resume_pos: Option<WalGenerationPos> = last_applied_lsn.clone();

        let mut consecutive_restore_count = 0;
        let mut last_restore_time = std::time::Instant::now();
        const MAX_AUTO_RESTORES: u32 = 5;
        const RESTORE_RESET_THRESHOLD: Duration = Duration::from_secs(60);
        let mut invalid_lsn_retries = 0u8;

        loop {
            log::debug!("ReplicaSidecar::run_single_db loop state: {state:?}");
            match state {
                ReplicaState::Bootstrapping => {
                    if let Some(pm) = &process_manager {
                        pm.add_blocker().await;
                    }

                    if force_restore && path.exists() {
                        let _ = fs::remove_file(db_path);
                        let _ = fs::remove_file(format!("{db_path}-wal"));
                        let _ = fs::remove_file(format!("{db_path}-shm"));
                    }

                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            match stream_snapshot_and_restore(
                                &client,
                                db_path,
                                &remote_db_name,
                                &replica_id,
                                &session_id,
                            )
                            .await
                            {
                                Ok(boundary_lsn) => {
                                    persist_last_applied_lsn(db_path, &boundary_lsn)?;
                                    if let Err(err) = ack_lsn_or_warn(
                                        &client,
                                        &remote_db_name,
                                        &replica_id,
                                        &session_id,
                                        &boundary_lsn,
                                    )
                                    .await
                                    {
                                        match err {
                                            ReplicaStreamError::Stream(code, message) => {
                                                warn!(
                                                    "ACK stream error for {}: {:?} {}",
                                                    db_path, code, message
                                                );
                                                match code.recovery_action() {
                                                    ReplicaRecoveryAction::NeedsRestore => {
                                                        state = ReplicaState::NeedsRestore;
                                                        continue;
                                                    }
                                                    ReplicaRecoveryAction::Retry => {
                                                        // Retry via outer loop.
                                                    }
                                                }
                                            }
                                            _ => {
                                                warn!("ACK failed for {}: {:?}", db_path, err);
                                            }
                                        }
                                    }
                                    last_applied_lsn = Some(boundary_lsn.clone());
                                    resume_pos = Some(boundary_lsn);
                                    state = ReplicaState::CatchingUp;
                                    force_restore = false;
                                    invalid_lsn_retries = 0;
                                }
                                Err(err) => match err {
                                    ReplicaStreamError::Stream(code, message) => {
                                        warn!(
                                            "Snapshot stream error for {}: {:?} {}",
                                            db_path, code, message
                                        );
                                        match code.recovery_action() {
                                            ReplicaRecoveryAction::NeedsRestore => {
                                                state = ReplicaState::NeedsRestore;
                                            }
                                            ReplicaRecoveryAction::Retry => {
                                                if invalid_lsn_retries == 0 {
                                                    invalid_lsn_retries += 1;
                                                } else {
                                                    state = ReplicaState::NeedsRestore;
                                                }
                                            }
                                        }
                                    }
                                    ReplicaStreamError::Transport(message)
                                    | ReplicaStreamError::InvalidResponse(message)
                                    | ReplicaStreamError::Io(message) => {
                                        warn!(
                                            "Snapshot transport error for {}: {}",
                                            db_path, message
                                        );
                                        sleep(Duration::from_secs(5)).await;
                                    }
                                },
                            }
                        }
                        Err(err) => {
                            warn!("Failed to connect to Primary: {err}. Retrying in 5s...");
                            sleep(Duration::from_secs(5)).await;
                        }
                    }

                    if let Some(pm) = &process_manager {
                        pm.remove_blocker().await;
                    }
                }
                ReplicaState::CatchingUp => {
                    let start_pos = match resume_pos.clone().or(last_applied_lsn.clone()) {
                        Some(pos) => pos,
                        None => {
                            state = ReplicaState::Bootstrapping;
                            continue;
                        }
                    };

                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            match stream_wal_and_apply(
                                &client,
                                db_path,
                                &remote_db_name,
                                &replica_id,
                                &session_id,
                                start_pos,
                                db_config.apply_checkpoint_frame_interval,
                                db_config.apply_checkpoint_interval_ms,
                                process_manager.clone(),
                            )
                            .await
                            {
                                Ok(applied_lsn) => {
                                    persist_last_applied_lsn(db_path, &applied_lsn)?;
                                    last_applied_lsn = Some(applied_lsn.clone());
                                    resume_pos = Some(applied_lsn);
                                    state = ReplicaState::Streaming;
                                    invalid_lsn_retries = 0;
                                }
                                Err(err) => match err {
                                    ReplicaStreamError::Stream(code, message) => {
                                        warn!(
                                            "WAL stream error for {}: {:?} {}",
                                            db_path, code, message
                                        );
                                        match code.recovery_action() {
                                            ReplicaRecoveryAction::NeedsRestore => {
                                                state = ReplicaState::NeedsRestore;
                                            }
                                            ReplicaRecoveryAction::Retry => {
                                                if invalid_lsn_retries == 0 {
                                                    invalid_lsn_retries += 1;
                                                    resume_pos = read_last_applied_lsn(db_path)?;
                                                } else {
                                                    state = ReplicaState::NeedsRestore;
                                                }
                                            }
                                        }
                                    }
                                    ReplicaStreamError::Transport(message)
                                    | ReplicaStreamError::InvalidResponse(message)
                                    | ReplicaStreamError::Io(message) => {
                                        warn!("WAL stream error for {}: {}", db_path, message);
                                        sleep(Duration::from_secs(5)).await;
                                    }
                                },
                            }
                        }
                        Err(err) => {
                            warn!("Failed to connect to Primary: {err}. Retrying in 5s...");
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                ReplicaState::Streaming => {
                    let start_pos = match read_last_applied_lsn(db_path)? {
                        Some(pos) => pos,
                        None => {
                            state = ReplicaState::Bootstrapping;
                            continue;
                        }
                    };

                    match StreamClient::connect(stream_config.addr.clone()).await {
                        Ok(client) => {
                            match stream_wal_and_apply(
                                &client,
                                db_path,
                                &remote_db_name,
                                &replica_id,
                                &session_id,
                                start_pos,
                                db_config.apply_checkpoint_frame_interval,
                                db_config.apply_checkpoint_interval_ms,
                                process_manager.clone(),
                            )
                            .await
                            {
                                Ok(applied_lsn) => {
                                    persist_last_applied_lsn(db_path, &applied_lsn)?;
                                    last_applied_lsn = Some(applied_lsn.clone());
                                    resume_pos = Some(applied_lsn);
                                    invalid_lsn_retries = 0;
                                    sleep(Duration::from_secs(1)).await;
                                }
                                Err(err) => match err {
                                    ReplicaStreamError::Stream(code, message) => {
                                        warn!(
                                            "WAL stream error for {}: {:?} {}",
                                            db_path, code, message
                                        );
                                        match code.recovery_action() {
                                            ReplicaRecoveryAction::NeedsRestore => {
                                                state = ReplicaState::NeedsRestore;
                                            }
                                            ReplicaRecoveryAction::Retry => {
                                                if invalid_lsn_retries == 0 {
                                                    invalid_lsn_retries += 1;
                                                } else {
                                                    state = ReplicaState::NeedsRestore;
                                                }
                                            }
                                        }
                                    }
                                    ReplicaStreamError::Transport(message)
                                    | ReplicaStreamError::InvalidResponse(message)
                                    | ReplicaStreamError::Io(message) => {
                                        warn!("WAL stream error for {}: {}", db_path, message);
                                        sleep(Duration::from_secs(5)).await;
                                    }
                                },
                            }
                        }
                        Err(err) => {
                            warn!("Failed to connect to Primary: {err}. Retrying in 5s...");
                            sleep(Duration::from_secs(5)).await;
                        }
                    }
                }
                ReplicaState::NeedsRestore => {
                    let now = std::time::Instant::now();
                    if now.duration_since(last_restore_time) > RESTORE_RESET_THRESHOLD {
                        consecutive_restore_count = 0;
                    }
                    if consecutive_restore_count >= MAX_AUTO_RESTORES {
                        warn!(
                            "Too many automatic restores for {}; backing off",
                            db_config.db
                        );
                        sleep(Duration::from_secs(10)).await;
                        continue;
                    }
                    consecutive_restore_count += 1;
                    last_restore_time = now;
                    force_restore = true;
                    state = ReplicaState::Bootstrapping;
                }
            }
        }
    }
}

async fn stream_snapshot_and_restore(
    client: &StreamClient,
    db_path: &str,
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
) -> Result<WalGenerationPos, ReplicaStreamError> {
    let meta_dir = ensure_meta_dir(db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?
        .as_nanos();
    let compressed_path = meta_dir.join(format!("snapshot_{timestamp}.zst"));
    let mut compressed_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&compressed_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    let request = StreamSnapshotV2Request {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        start_lsn: None,
    };

    let mut stream = client
        .stream_snapshot_v2(request)
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;

    let mut meta = None;
    let mut hasher = Sha256::new();
    let mut total_bytes = 0u64;

    loop {
        let response = stream
            .message()
            .await
            .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;
        let response = match response {
            Some(response) => response,
            None => break,
        };

        match response.payload {
            Some(StreamSnapshotV2Payload::Meta(snapshot_meta)) => {
                if meta.is_some() {
                    return Err(ReplicaStreamError::InvalidResponse(
                        "snapshot meta received twice".to_string(),
                    ));
                }
                meta = Some(snapshot_meta);
            }
            Some(StreamSnapshotV2Payload::Chunk(chunk)) => {
                if meta.is_none() {
                    return Err(ReplicaStreamError::InvalidResponse(
                        "snapshot chunk before meta".to_string(),
                    ));
                }
                compressed_file
                    .write_all(&chunk)
                    .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
                hasher.update(&chunk);
                total_bytes += chunk.len() as u64;
            }
            Some(StreamSnapshotV2Payload::Error(err)) => {
                return Err(map_stream_error(err));
            }
            None => {
                return Err(ReplicaStreamError::InvalidResponse(
                    "snapshot response missing payload".to_string(),
                ));
            }
        }
    }

    compressed_file
        .sync_all()
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    let meta = meta
        .ok_or_else(|| ReplicaStreamError::InvalidResponse("snapshot meta missing".to_string()))?;

    if total_bytes != meta.snapshot_size_bytes {
        return Err(ReplicaStreamError::InvalidResponse(format!(
            "snapshot size mismatch: expected {}, received {}",
            meta.snapshot_size_bytes, total_bytes
        )));
    }

    let digest = hasher.finalize();
    if digest.as_slice() != meta.snapshot_sha256.as_slice() {
        return Err(ReplicaStreamError::InvalidResponse(
            "snapshot sha256 mismatch".to_string(),
        ));
    }

    let boundary_token = meta.boundary_lsn.ok_or_else(|| {
        ReplicaStreamError::InvalidResponse("snapshot boundary missing".to_string())
    })?;
    let boundary_lsn = lsn_from_token(&boundary_token)
        .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;

    restore_snapshot_from_compressed(db_path, &compressed_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    let _ = fs::remove_file(&compressed_path);
    Ok(boundary_lsn)
}

async fn stream_wal_and_apply(
    client: &StreamClient,
    db_path: &str,
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
    start_pos: WalGenerationPos,
    checkpoint_frame_interval: u32,
    checkpoint_interval_ms: u64,
    process_manager: Option<ProcessManager>,
) -> Result<WalGenerationPos, ReplicaStreamError> {
    let wal_path = format!("{db_path}-wal");
    let mut effective_start_pos = start_pos.clone();
    let mut ack_floor: Option<WalGenerationPos> = None;
    if effective_start_pos.offset != 0 {
        let wal_needs_header = match fs::metadata(&wal_path) {
            Ok(meta) => meta.len() < WAL_HEADER_SIZE,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
            Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
        };
        if wal_needs_header {
            if seed_wal_header_from_shadow(db_path, &effective_start_pos)? {
                info!(
                    "ReplicaSidecar: seeded WAL header for {} from shadow wal at {}:{}:{}",
                    db_path,
                    effective_start_pos.generation.as_str(),
                    effective_start_pos.index,
                    effective_start_pos.offset
                );
            } else {
                warn!(
                    "ReplicaSidecar: WAL header missing for {} at offset {}; rewinding stream to 0",
                    db_path, effective_start_pos.offset
                );
            }
            effective_start_pos.offset = 0;
            ack_floor = Some(start_pos);
        }
    }
    let request = StreamWalV2Request {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        start_lsn: Some(lsn_token_from_pos(&effective_start_pos)),
    };

    let mut stream = client
        .stream_wal_v2(request)
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;

    let mut current_pos = effective_start_pos;
    let mut current_wal_index = current_pos.index;
    let page_size = load_db_page_size(db_path)?;
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    let mut frames_since_refresh: u32 = 0;
    let mut last_refresh = Instant::now();
    let mut refresh_state = WalRefreshState::new();
    let mut suppress_ack = ack_floor.is_some();
    loop {
        let response = stream
            .message()
            .await
            .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;
        let response = match response {
            Some(response) => response,
            None => break,
        };

        match response.payload {
            Some(StreamWalV2Payload::Chunk(chunk)) => {
                let chunk_start_token = chunk.start_lsn.as_ref().ok_or_else(|| {
                    ReplicaStreamError::InvalidResponse("chunk missing start lsn".to_string())
                })?;
                let chunk_next_token = chunk.next_lsn.as_ref().ok_or_else(|| {
                    ReplicaStreamError::InvalidResponse("chunk missing next lsn".to_string())
                })?;
                let chunk_start = lsn_from_token(chunk_start_token)
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;
                let chunk_next = lsn_from_token(chunk_next_token)
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;

                if let Some(floor) = &ack_floor {
                    if chunk_start.generation == floor.generation && chunk_start.index > floor.index
                    {
                        return Err(ReplicaStreamError::Stream(
                            StreamReplicationErrorCode::SnapshotBoundaryMismatch,
                            "stream advanced past rewind floor".to_string(),
                        ));
                    }
                }

                if !lsn_matches(&chunk_start, &current_pos)
                    && !allow_shadow_wal_boundary_advance(db_path, &current_pos, &chunk_start)
                {
                    return Err(ReplicaStreamError::InvalidResponse(format!(
                        "chunk start mismatch: expected {:?}, got {:?}",
                        current_pos, chunk_start
                    )));
                }

                if chunk_start.index != current_wal_index {
                    info!(
                        "ReplicaSidecar: WAL index advanced for {} ({} -> {}). Resetting local WAL.",
                        db_path, current_wal_index, chunk_start.index
                    );
                    reset_local_wal(db_path, process_manager.as_ref()).await?;
                    current_wal_index = chunk_start.index;
                }

                apply_wal_bytes(
                    db_path,
                    &chunk_start,
                    &chunk.wal_bytes,
                    process_manager.as_ref(),
                )
                .await?;
                store_shadow_wal_bytes(db_path, &chunk_start, &chunk.wal_bytes)
                    .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
                persist_last_applied_lsn(db_path, &chunk_next)
                    .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

                let mut bytes_delta = chunk_next.offset.saturating_sub(chunk_start.offset);
                if chunk_start.offset == 0 {
                    bytes_delta = bytes_delta.saturating_sub(WAL_HEADER_SIZE);
                }
                let frames_in_chunk = if frame_size > 0 {
                    (bytes_delta / frame_size) as u32
                } else {
                    0
                };
                frames_since_refresh = frames_since_refresh.saturating_add(frames_in_chunk);

                let should_refresh = frames_since_refresh >= checkpoint_frame_interval
                    || last_refresh.elapsed() >= Duration::from_millis(checkpoint_interval_ms);
                if should_refresh {
                    let expected_frames = expected_frames_from_wal_file(db_path, page_size)?;
                    refresh_wal_index(
                        db_path,
                        expected_frames,
                        &mut refresh_state,
                        process_manager.as_ref(),
                    )
                    .await?;
                    last_refresh = Instant::now();
                    frames_since_refresh = 0;
                }

                // Best-effort ACK. Transport errors should not kill the replica.
                // Explicit protocol errors must still propagate to trigger NeedsRestore.
                if suppress_ack {
                    if let Some(floor) = &ack_floor {
                        if lsn_reached(&chunk_next, floor) {
                            suppress_ack = false;
                        }
                    }
                }
                if !suppress_ack {
                    ack_lsn_or_warn(client, db_identity, replica_id, session_id, &chunk_next)
                        .await?;
                }

                current_pos = chunk_next;
            }
            Some(StreamWalV2Payload::Error(err)) => {
                return Err(map_stream_error(err));
            }
            None => {
                return Err(ReplicaStreamError::InvalidResponse(
                    "wal response missing payload".to_string(),
                ));
            }
        }
    }

    if frames_since_refresh > 0 {
        let expected_frames = expected_frames_from_wal_file(db_path, page_size)?;
        refresh_wal_index(
            db_path,
            expected_frames,
            &mut refresh_state,
            process_manager.as_ref(),
        )
        .await?;
    }

    Ok(current_pos)
}

fn load_db_page_size(db_path: &str) -> Result<u64, ReplicaStreamError> {
    let conn = Connection::open(db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    conn.pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))
}

fn expected_frames_from_wal_file(db_path: &str, page_size: u64) -> Result<u32, ReplicaStreamError> {
    let wal_path = format!("{db_path}-wal");
    let meta = match fs::metadata(&wal_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
    };
    let wal_size = align_frame(page_size, meta.len());
    if wal_size <= WAL_HEADER_SIZE {
        return Ok(0);
    }
    let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
    if frame_size == 0 {
        return Ok(0);
    }
    Ok(((wal_size - WAL_HEADER_SIZE) / frame_size) as u32)
}

struct WalRefreshState {
    stale_failures: u8,
    stale_window_start: Option<Instant>,
    last_refresh_log: Instant,
    last_stale_log: Instant,
    last_recovery_log: Instant,
}

impl WalRefreshState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            stale_failures: 0,
            stale_window_start: None,
            last_refresh_log: now - REFRESH_LOG_INTERVAL,
            last_stale_log: now - STALE_LOG_INTERVAL,
            last_recovery_log: now - RECOVERY_LOG_INTERVAL,
        }
    }
}

const REFRESH_LOG_INTERVAL: Duration = Duration::from_secs(15);
const STALE_LOG_INTERVAL: Duration = Duration::from_secs(15);
const RECOVERY_LOG_INTERVAL: Duration = Duration::from_secs(30);
const STALE_WINDOW: Duration = Duration::from_secs(30);
const STALE_THRESHOLD: u8 = 3;

fn should_log(last: &mut Instant, interval: Duration) -> bool {
    if last.elapsed() >= interval {
        *last = Instant::now();
        true
    } else {
        false
    }
}

fn update_stale_window(state: &mut WalRefreshState) {
    let now = Instant::now();
    match state.stale_window_start {
        Some(start) if now.duration_since(start) <= STALE_WINDOW => {}
        _ => {
            state.stale_window_start = Some(now);
            state.stale_failures = 0;
        }
    }
}

async fn refresh_wal_index(
    db_path: &str,
    expected_frames: u32,
    refresh_state: &mut WalRefreshState,
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    let res_passive = run_wal_checkpoint_passive(db_path)?;
    if should_log(&mut refresh_state.last_refresh_log, REFRESH_LOG_INTERVAL) {
        info!(
            "ReplicaSidecar: WAL checkpoint PASSIVE for {}: {:?}",
            db_path, res_passive
        );
    }
    if expected_frames == 0 || res_passive.0 != 0 {
        return Ok(());
    }

    if (res_passive.1 as u32) >= expected_frames {
        refresh_state.stale_failures = 0;
        refresh_state.stale_window_start = None;
        return Ok(());
    }

    update_stale_window(refresh_state);
    if should_log(&mut refresh_state.last_stale_log, STALE_LOG_INTERVAL) {
        warn!(
            "ReplicaSidecar: Stale WAL-index for {} (SQLite saw {}, expected {}).",
            db_path, res_passive.1, expected_frames
        );
    }

    if invalidate_shm_header(db_path).is_ok() {
        let res_retry = run_wal_checkpoint_passive(db_path)?;
        if should_log(&mut refresh_state.last_recovery_log, RECOVERY_LOG_INTERVAL) {
            info!(
                "ReplicaSidecar: WAL checkpoint PASSIVE after SHM refresh for {}: {:?}",
                db_path, res_retry
            );
        }
        if (res_retry.1 as u32) >= expected_frames {
            refresh_state.stale_failures = 0;
            refresh_state.stale_window_start = None;
            return Ok(());
        }
    }

    if process_manager.is_some() {
        if should_log(&mut refresh_state.last_recovery_log, RECOVERY_LOG_INTERVAL) {
            warn!(
                "ReplicaSidecar: WAL-index still stale for {}. Triggering SHM recovery.",
                db_path
            );
        }
        recover_stale_shm(db_path, process_manager).await?;
        let res_retry = run_wal_checkpoint_passive(db_path)?;
        info!(
            "ReplicaSidecar: WAL checkpoint PASSIVE retry for {}: {:?}",
            db_path, res_retry
        );
        if (res_retry.1 as u32) < expected_frames {
            return Err(ReplicaStreamError::Stream(
                StreamReplicationErrorCode::SnapshotBoundaryMismatch,
                "WAL-index refresh failed after recovery; replica requires restore".to_string(),
            ));
        }
        refresh_state.stale_failures = 0;
        refresh_state.stale_window_start = None;
        return Ok(());
    }

    refresh_state.stale_failures = refresh_state.stale_failures.saturating_add(1);
    if refresh_state.stale_failures < STALE_THRESHOLD {
        return Ok(());
    }

    refresh_state.stale_failures = 0;
    refresh_state.stale_window_start = Some(Instant::now());
    Err(ReplicaStreamError::Stream(
        StreamReplicationErrorCode::SnapshotBoundaryMismatch,
        "WAL-index refresh failed after SHM refresh; replica requires restore".to_string(),
    ))
}

fn run_wal_checkpoint_passive(db_path: &str) -> Result<(i32, i32, i32), ReplicaStreamError> {
    let conn = Connection::open(db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, i32>(1)?,
            row.get::<_, i32>(2)?,
        ))
    })
    .map_err(|e| ReplicaStreamError::Io(e.to_string()))
}

fn invalidate_shm_header(db_path: &str) -> std::io::Result<()> {
    let shm_path = format!("{db_path}-shm");
    let mut shm_file = match std::fs::OpenOptions::new().write(true).open(&shm_path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    let zeros = [0u8; 32];
    shm_file.write_all(&zeros)?;
    shm_file.sync_all()?;
    Ok(())
}

async fn recover_stale_shm(
    db_path: &str,
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    if let Some(pm) = process_manager {
        pm.add_blocker().await;
    } else {
        return Err(ReplicaStreamError::Stream(
            StreamReplicationErrorCode::SnapshotBoundaryMismatch,
            "WAL-index recovery needs managed reader shutdown; run with --exec".to_string(),
        ));
    }

    let shm_path = format!("{db_path}-shm");
    let remove_result = match fs::remove_file(&shm_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
    };

    if let Some(pm) = process_manager {
        pm.remove_blocker().await;
    }

    remove_result
}

fn restore_snapshot_from_compressed(db_path: &str, compressed_path: &Path) -> Result<()> {
    let temp_db_path = format!("{db_path}.tmp");
    {
        let compressed_file = fs::File::open(compressed_path)?;
        // Snapshot payloads are compressed with Zstd (see `crate::base::compress_file`).
        let mut decoder = zstd::stream::Decoder::new(compressed_file)?;
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_db_path)?;
        std::io::copy(&mut decoder, &mut output)?;
        output.sync_all()?;
    }

    fs::rename(&temp_db_path, db_path)?;
    let _ = fs::remove_file(format!("{db_path}-wal"));
    let _ = fs::remove_file(format!("{db_path}-shm"));
    Ok(())
}

async fn reset_local_wal(
    db_path: &str,
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    if let Some(pm) = process_manager {
        pm.add_blocker().await;
    }

    let wal_path = format!("{db_path}-wal");
    let _ = match fs::remove_file(&wal_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
    }?;

    if process_manager.is_some() {
        let shm_path = format!("{db_path}-shm");
        let _ = match fs::remove_file(&shm_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
        }?;
    } else if let Err(err) = invalidate_shm_header(db_path) {
        warn!(
            "ReplicaSidecar: Failed to invalidate SHM header for {}: {}",
            db_path, err
        );
    }

    if let Some(pm) = process_manager {
        pm.remove_blocker().await;
    }

    Ok(())
}

async fn apply_wal_bytes(
    db_path: &str,
    start_pos: &WalGenerationPos,
    wal_bytes: &[u8],
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    let wal_path = format!("{db_path}-wal");

    if start_pos.offset == 0 {
        reset_local_wal(db_path, process_manager).await?;
    }

    let current_len = match fs::metadata(&wal_path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if start_pos.offset > 0 {
                return Err(ReplicaStreamError::Io(format!(
                    "WAL offset {} beyond EOF for {}",
                    start_pos.offset, wal_path
                )));
            }
            0
        }
        Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
    };

    if start_pos.offset > 0 && current_len < start_pos.offset {
        return Err(ReplicaStreamError::Io(format!(
            "WAL offset {} beyond EOF {} for {}",
            start_pos.offset, current_len, wal_path
        )));
    }

    let mut wal_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&wal_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    if current_len > start_pos.offset {
        wal_file
            .set_len(start_pos.offset)
            .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    }

    wal_file
        .seek(SeekFrom::Start(start_pos.offset))
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    wal_file
        .write_all(wal_bytes)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    wal_file
        .sync_all()
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    Ok(())
}

fn store_shadow_wal_bytes(
    db_path: &str,
    start_pos: &WalGenerationPos,
    wal_bytes: &[u8],
) -> Result<()> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let wal_dir = shadow_wal_dir(
        meta_dir.to_str().unwrap_or("."),
        start_pos.generation.as_str(),
    );
    fs::create_dir_all(&wal_dir)?;
    let wal_path = shadow_wal_file(
        meta_dir.to_str().unwrap_or("."),
        start_pos.generation.as_str(),
        start_pos.index,
    );
    let truncate = start_pos.offset == 0;
    let mut wal_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(&wal_path)?;
    wal_file.seek(SeekFrom::Start(start_pos.offset))?;
    wal_file.write_all(wal_bytes)?;
    wal_file.sync_all()?;
    Ok(())
}

fn map_stream_error(err: StreamError) -> ReplicaStreamError {
    match stream_error_code(&err) {
        Some(code) => ReplicaStreamError::Stream(code, err.message),
        None => {
            ReplicaStreamError::InvalidResponse(format!("unknown stream error: {}", err.message))
        }
    }
}

fn stream_error_code(err: &StreamError) -> Option<StreamReplicationErrorCode> {
    let code = StreamErrorCode::try_from(err.code).ok()?;
    match code {
        StreamErrorCode::LineageMismatch => Some(StreamReplicationErrorCode::LineageMismatch),
        StreamErrorCode::WalNotRetained => Some(StreamReplicationErrorCode::WalNotRetained),
        StreamErrorCode::SnapshotBoundaryMismatch => {
            Some(StreamReplicationErrorCode::SnapshotBoundaryMismatch)
        }
        StreamErrorCode::InvalidLsn => Some(StreamReplicationErrorCode::InvalidLsn),
        StreamErrorCode::Unknown => None,
    }
}

fn lsn_from_token(token: &LsnToken) -> Result<WalGenerationPos> {
    let generation = Generation::try_create(token.generation.trim())?;
    Ok(WalGenerationPos {
        generation,
        index: token.index,
        offset: token.offset,
    })
}

fn lsn_matches(a: &WalGenerationPos, b: &WalGenerationPos) -> bool {
    a.generation == b.generation && a.index == b.index && a.offset == b.offset
}

fn lsn_reached(current: &WalGenerationPos, floor: &WalGenerationPos) -> bool {
    if current.generation != floor.generation {
        return false;
    }
    if current.index > floor.index {
        return true;
    }
    current.index == floor.index && current.offset >= floor.offset
}

fn seed_wal_header_from_shadow(
    db_path: &str,
    pos: &WalGenerationPos,
) -> Result<bool, ReplicaStreamError> {
    let meta_dir = ensure_meta_dir(db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let shadow_path = shadow_wal_file(
        meta_dir.to_str().unwrap_or("."),
        pos.generation.as_str(),
        pos.index,
    );
    let shadow_meta = match fs::metadata(&shadow_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
    };
    if shadow_meta.len() < WAL_HEADER_SIZE {
        return Ok(false);
    }

    let mut shadow_file =
        fs::File::open(&shadow_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let mut header = vec![0u8; WAL_HEADER_SIZE as usize];
    shadow_file
        .read_exact(&mut header)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    let wal_path = format!("{db_path}-wal");
    let mut wal_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&wal_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    wal_file
        .write_all(&header)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    wal_file
        .sync_all()
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    Ok(true)
}

fn allow_shadow_wal_boundary_advance(
    db_path: &str,
    expected: &WalGenerationPos,
    actual: &WalGenerationPos,
) -> bool {
    // Allow advancing to the next shadow WAL file when the requested position is
    // exactly at EOF of the current file.
    if actual.generation != expected.generation {
        return false;
    }
    if actual.index != expected.index + 1 || actual.offset != 0 {
        return false;
    }

    let meta_dir = match ensure_meta_dir(db_path) {
        Ok(dir) => dir,
        Err(_) => return false,
    };
    let expected_path = shadow_wal_file(
        meta_dir.to_str().unwrap_or("."),
        expected.generation.as_str(),
        expected.index,
    );
    let metadata = match fs::metadata(&expected_path) {
        Ok(m) => m,
        Err(_) => return false,
    };
    metadata.len() == expected.offset
}

fn lsn_token_from_pos(pos: &WalGenerationPos) -> LsnToken {
    LsnToken {
        generation: pos.generation.as_str().to_string(),
        index: pos.index,
        offset: pos.offset,
    }
}

async fn ack_lsn_or_warn(
    client: &StreamClient,
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
    pos: &WalGenerationPos,
) -> Result<(), ReplicaStreamError> {
    let ack = AckLsnV2Request {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        last_applied_lsn: Some(lsn_token_from_pos(pos)),
    };

    let response = match client.ack_lsn_v2(ack).await {
        Ok(resp) => resp,
        Err(err) => {
            warn!("ack_lsn_v2 transport failed: {err}");
            return Ok(());
        }
    };

    if response.accepted {
        return Ok(());
    }

    let err = response.error.ok_or_else(|| {
        ReplicaStreamError::InvalidResponse("AckLsnV2Response rejected without error".to_string())
    })?;
    Err(map_stream_error(err))
}

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

struct Sha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffer_len: usize,
    total_len: u64,
}

impl Sha256 {
    fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buffer: [0u8; 64],
            buffer_len: 0,
            total_len: 0,
        }
    }

    fn update(&mut self, mut data: &[u8]) {
        self.total_len += data.len() as u64;

        if self.buffer_len > 0 {
            let needed = 64 - self.buffer_len;
            if data.len() >= needed {
                self.buffer[self.buffer_len..64].copy_from_slice(&data[..needed]);
                sha256_compress(&mut self.state, &self.buffer);
                self.buffer_len = 0;
                data = &data[needed..];
            } else {
                self.buffer[self.buffer_len..self.buffer_len + data.len()].copy_from_slice(data);
                self.buffer_len += data.len();
                return;
            }
        }

        while data.len() >= 64 {
            let mut block = [0u8; 64];
            block.copy_from_slice(&data[..64]);
            sha256_compress(&mut self.state, &block);
            data = &data[64..];
        }

        if !data.is_empty() {
            self.buffer[..data.len()].copy_from_slice(data);
            self.buffer_len = data.len();
        }
    }

    fn finalize(mut self) -> Vec<u8> {
        let bit_len = self.total_len * 8;
        self.buffer[self.buffer_len] = 0x80;
        self.buffer_len += 1;

        if self.buffer_len > 56 {
            for i in self.buffer_len..64 {
                self.buffer[i] = 0;
            }
            sha256_compress(&mut self.state, &self.buffer);
            self.buffer_len = 0;
        }

        for i in self.buffer_len..56 {
            self.buffer[i] = 0;
        }
        self.buffer[56..64].copy_from_slice(&bit_len.to_be_bytes());
        sha256_compress(&mut self.state, &self.buffer);

        let mut out = vec![0u8; 32];
        for (i, word) in self.state.iter().enumerate() {
            out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
        }
        out
    }
}

fn sha256_compress(state: &mut [u32; 8], block: &[u8; 64]) {
    let mut w = [0u32; 64];
    for i in 0..16 {
        let start = i * 4;
        w[i] = u32::from_be_bytes([
            block[start],
            block[start + 1],
            block[start + 2],
            block[start + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = sha256_rotr(w[i - 15], 7) ^ sha256_rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
        let s1 = sha256_rotr(w[i - 2], 17) ^ sha256_rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }

    let mut a = state[0];
    let mut b = state[1];
    let mut c = state[2];
    let mut d = state[3];
    let mut e = state[4];
    let mut f = state[5];
    let mut g = state[6];
    let mut h = state[7];

    for i in 0..64 {
        let s1 = sha256_rotr(e, 6) ^ sha256_rotr(e, 11) ^ sha256_rotr(e, 25);
        let ch = (e & f) ^ ((!e) & g);
        let temp1 = h
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(SHA256_K[i])
            .wrapping_add(w[i]);
        let s0 = sha256_rotr(a, 2) ^ sha256_rotr(a, 13) ^ sha256_rotr(a, 22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let temp2 = s0.wrapping_add(maj);

        h = g;
        g = f;
        f = e;
        e = d.wrapping_add(temp1);
        d = c;
        c = b;
        b = a;
        a = temp1.wrapping_add(temp2);
    }

    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
    state[5] = state[5].wrapping_add(f);
    state[6] = state[6].wrapping_add(g);
    state[7] = state[7].wrapping_add(h);
}

fn sha256_rotr(value: u32, bits: u32) -> u32 {
    (value >> bits) | (value << (32 - bits))
}

#[async_trait::async_trait]
impl super::command::Command for ReplicaSidecar {
    async fn run(&mut self) -> Result<()> {
        self.run().await
    }
}

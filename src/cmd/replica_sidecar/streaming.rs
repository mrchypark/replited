use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use log::{info, warn};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

use crate::base::{shadow_wal_dir, shadow_wal_file};
use crate::database::WalGenerationPos;
use crate::error::Result;
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, align_frame};
use crate::sync::StreamReplicationErrorCode;
use crate::sync::replication::stream_snapshot_response::Payload as StreamSnapshotPayload;
use crate::sync::replication::stream_wal_response::Payload as StreamWalPayload;
use crate::sync::replication::{
    AckLsnRequest, SnapshotMeta, StreamError, StreamSnapshotRequest, StreamWalRequest,
};
use crate::sync::stream_client::StreamClient;
use crate::sync::stream_protocol::{
    lsn_token_from_pos, lsn_token_to_pos, replication_error_code_from_stream_error,
};

use super::local_state::{ensure_meta_dir, persist_last_applied_lsn};
use super::{ProcessManager, ReplicaStreamError};

#[cfg(not(test))]
fn managed_reader_startup_grace() -> Duration {
    Duration::from_secs(5)
}

#[cfg(test)]
fn managed_reader_startup_grace() -> Duration {
    Duration::ZERO
}

fn managed_reader_restart_jitter_for(seed: &str, max_ms: u64) -> Duration {
    if max_ms == 0 || seed.is_empty() {
        return Duration::ZERO;
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    Duration::from_millis(hasher.finish() % (max_ms + 1))
}

#[cfg(not(test))]
fn managed_reader_restart_jitter() -> Duration {
    let max_ms = std::env::var("REPLITED_MANAGED_READER_RESTART_JITTER_MAX_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let seed = std::env::var("HOSTNAME").unwrap_or_default();
    managed_reader_restart_jitter_for(&seed, max_ms)
}

#[cfg(test)]
fn managed_reader_restart_jitter() -> Duration {
    Duration::ZERO
}

pub(super) async fn stream_snapshot_and_restore(
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

    let request = StreamSnapshotRequest {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        start_lsn: None,
    };

    let mut stream = client
        .stream_snapshot(request)
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
            Some(StreamSnapshotPayload::Meta(snapshot_meta)) => {
                if meta.is_some() {
                    return Err(ReplicaStreamError::InvalidResponse(
                        "snapshot meta received twice".to_string(),
                    ));
                }
                meta = Some(snapshot_meta);
            }
            Some(StreamSnapshotPayload::Chunk(chunk)) => {
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
            Some(StreamSnapshotPayload::Error(err)) => {
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
    info!(
        "ReplicaSidecar: received snapshot for {} compressed_bytes={}",
        db_path, total_bytes
    );

    let meta = meta
        .ok_or_else(|| ReplicaStreamError::InvalidResponse("snapshot meta missing".to_string()))?;

    let digest = hasher.finalize();
    validate_snapshot_meta(&meta, total_bytes, digest.as_slice())?;

    let boundary_token = meta.boundary_lsn.ok_or_else(|| {
        ReplicaStreamError::InvalidResponse("snapshot boundary missing".to_string())
    })?;
    let boundary_lsn = lsn_token_to_pos(Some(boundary_token))
        .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;

    let db_path_owned = db_path.to_string();
    let compressed_path_owned = compressed_path.clone();
    tokio::task::spawn_blocking(move || {
        restore_snapshot_from_compressed(&db_path_owned, &compressed_path_owned)
    })
    .await
    .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?
    .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let restored_size = fs::metadata(db_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?
        .len();
    info!(
        "ReplicaSidecar: restored snapshot for {} db_size={} boundary={:?}",
        db_path, restored_size, boundary_lsn
    );

    let _ = fs::remove_file(&compressed_path);
    Ok(boundary_lsn)
}

fn validate_snapshot_meta(
    meta: &SnapshotMeta,
    total_bytes: u64,
    digest: &[u8],
) -> Result<(), ReplicaStreamError> {
    if total_bytes != meta.snapshot_size_bytes {
        return Err(ReplicaStreamError::InvalidResponse(format!(
            "snapshot size mismatch: expected {}, received {}",
            meta.snapshot_size_bytes, total_bytes
        )));
    }

    if digest != meta.snapshot_sha256.as_slice() {
        return Err(ReplicaStreamError::InvalidResponse(
            "snapshot sha256 mismatch".to_string(),
        ));
    }

    Ok(())
}

pub(super) async fn stream_wal_and_apply(
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
        let wal_len = match fs::metadata(&wal_path) {
            Ok(meta) => Some(meta.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
        };
        let needs_rewind = local_wal_needs_rewind(wal_len, effective_start_pos.offset);
        if needs_rewind {
            if seed_wal_prefix_from_shadow(db_path, &effective_start_pos)? {
                info!(
                    "ReplicaSidecar: seeded WAL prefix for {} from shadow wal at {}:{}:{}",
                    db_path,
                    effective_start_pos.generation.as_str(),
                    effective_start_pos.index,
                    effective_start_pos.offset
                );
            } else {
                info!(
                    "ReplicaSidecar: local WAL missing or shorter than resume offset for {} at offset {}; rewinding stream to 0 (expected on cold start or after WAL cleanup)",
                    db_path, effective_start_pos.offset
                );
                effective_start_pos.offset = 0;
                ack_floor = Some(start_pos);
            }
        }
    }
    let mut catchup_reader_blocker = if ack_floor.is_some() {
        match process_manager.as_ref() {
            Some(pm) => Some(pm.block_reader().await),
            None => None,
        }
    } else {
        None
    };
    let request = StreamWalRequest {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        start_lsn: Some(lsn_token_from_pos(&effective_start_pos)),
    };

    let mut stream = client
        .stream_wal(request)
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;

    let mut current_pos = effective_start_pos;
    let mut current_wal_index = current_pos.index;
    let page_size = load_db_page_size(db_path).await?;
    let mut frames_since_refresh: u32 = 0;
    let mut last_checkpoint_refresh = Instant::now();
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
            Some(StreamWalPayload::Chunk(chunk)) => {
                let chunk_start_token = chunk.start_lsn.as_ref().ok_or_else(|| {
                    ReplicaStreamError::InvalidResponse("chunk missing start lsn".to_string())
                })?;
                let chunk_next_token = chunk.next_lsn.as_ref().ok_or_else(|| {
                    ReplicaStreamError::InvalidResponse("chunk missing next lsn".to_string())
                })?;
                let chunk_start = lsn_token_to_pos(Some(chunk_start_token.clone()))
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;
                let chunk_next = lsn_token_to_pos(Some(chunk_next_token.clone()))
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;

                if suppress_ack {
                    if let Some(floor) = &ack_floor {
                        if chunk_start.generation == floor.generation
                            && chunk_start.index > floor.index
                        {
                            if can_accept_stream_advance_past_floor(db_path, floor, &chunk_start) {
                                // The rewind floor is exactly at EOF and stream moved to the next
                                // index. Treat floor as reached and continue.
                                current_pos = floor.clone();
                                suppress_ack = false;
                            } else {
                                return Err(ReplicaStreamError::Stream(
                                    StreamReplicationErrorCode::SnapshotBoundaryMismatch,
                                    "stream advanced past rewind floor".to_string(),
                                ));
                            }
                        }
                    }
                }

                if !lsn_matches(&chunk_start, &current_pos)
                    && !allow_shadow_wal_boundary_advance(db_path, &current_pos, &chunk_start)
                {
                    return Err(ReplicaStreamError::InvalidResponse(format!(
                        "chunk start mismatch: expected {current_pos:?}, got {chunk_start:?}",
                    )));
                }

                if chunk_start.index != current_wal_index {
                    if frames_since_refresh > 0 && !suppress_ack {
                        let expected_frames = expected_frames_from_wal_file(db_path, page_size)?;
                        refresh_wal_index(
                            db_path,
                            expected_frames,
                            &mut refresh_state,
                            process_manager.as_ref(),
                        )
                        .await?;
                        frames_since_refresh = 0;
                        last_checkpoint_refresh = Instant::now();
                    }

                    info!(
                        "ReplicaSidecar: WAL index advanced for {} ({} -> {}). Resetting local WAL.",
                        db_path, current_wal_index, chunk_start.index
                    );
                    current_wal_index = chunk_start.index;
                }

                let chunk_process_manager = if catchup_reader_blocker.is_some() {
                    None
                } else {
                    process_manager.as_ref()
                };
                apply_wal_chunk_and_refresh(
                    db_path,
                    &chunk_start,
                    &chunk_next,
                    &chunk.wal_bytes,
                    page_size,
                    &mut frames_since_refresh,
                    checkpoint_frame_interval,
                    checkpoint_interval_ms,
                    &mut last_checkpoint_refresh,
                    &mut refresh_state,
                    ack_floor.as_ref(),
                    &mut suppress_ack,
                    chunk_process_manager,
                )
                .await?;

                if catchup_reader_blocker.is_some()
                    && catchup_reader_release_ready(
                        ack_floor.as_ref(),
                        &chunk_start,
                        &chunk_next,
                        suppress_ack,
                    )
                {
                    if let Some(blocker) = catchup_reader_blocker.take() {
                        blocker.release().await;
                        current_pos = chunk_next;
                        tokio::time::sleep(managed_reader_startup_grace()).await;
                        return Ok(current_pos);
                    }
                }

                // Best-effort ACK. Transport errors should not kill the replica.
                // Explicit protocol errors must still propagate to trigger NeedsRestore.
                if !suppress_ack {
                    ack_lsn_or_warn(client, db_identity, replica_id, session_id, &chunk_next)
                        .await?;
                }

                current_pos = chunk_next;
            }
            Some(StreamWalPayload::Error(err)) => {
                return Err(map_stream_error(err));
            }
            None => {
                return Err(ReplicaStreamError::InvalidResponse(
                    "wal response missing payload".to_string(),
                ));
            }
        }
    }

    if frames_since_refresh > 0 && !suppress_ack {
        let expected_frames = expected_frames_from_wal_file(db_path, page_size)?;
        if let Some(pm) = process_manager.as_ref() {
            pm.add_blocker().await;
            let refresh_result = async {
                refresh_wal_index(db_path, expected_frames, &mut refresh_state, None).await?;
                materialize_standalone_db(db_path).await
            }
            .await;
            pm.remove_blocker().await;
            refresh_result?;
        } else {
            refresh_wal_index(db_path, expected_frames, &mut refresh_state, None).await?;
        }
    }
    if let Some(blocker) = catchup_reader_blocker.take() {
        blocker.release().await;
        tokio::time::sleep(managed_reader_startup_grace()).await;
    }

    Ok(current_pos)
}

pub(super) async fn ack_lsn_or_warn(
    client: &StreamClient,
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
    pos: &WalGenerationPos,
) -> Result<(), ReplicaStreamError> {
    let ack = AckLsnRequest {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
        session_id: session_id.to_string(),
        last_applied_lsn: Some(lsn_token_from_pos(pos)),
    };

    let response = match client.ack_lsn(ack).await {
        Ok(resp) => resp,
        Err(err) => {
            warn!("ack_lsn transport failed: {err}");
            return Ok(());
        }
    };

    if response.accepted {
        return Ok(());
    }

    let err = response.error.ok_or_else(|| {
        ReplicaStreamError::InvalidResponse("AckLsnResponse rejected without error".to_string())
    })?;
    Err(map_stream_error(err))
}

async fn load_db_page_size(db_path: &str) -> Result<u64, ReplicaStreamError> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || load_db_page_size_sync(&db_path))
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?
}

fn load_db_page_size_sync(db_path: &str) -> Result<u64, ReplicaStreamError> {
    let conn = Connection::open(db_path)?;
    let page_size_i64: i64 = conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
    u64::try_from(page_size_i64)
        .map_err(|_| ReplicaStreamError::Io(format!("invalid sqlite page_size: {page_size_i64}")))
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

fn local_wal_needs_rewind(wal_len: Option<u64>, resume_offset: u64) -> bool {
    if resume_offset == 0 {
        return false;
    }
    match wal_len {
        Some(len) => len < resume_offset,
        None => true,
    }
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

fn checkpoint_covers_visible_frames(result: (i32, i32, i32)) -> bool {
    result.0 == 0 && result.1 > 0 && result.1 == result.2
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
    let res_passive = run_wal_checkpoint_passive(db_path).await?;
    if should_log(&mut refresh_state.last_refresh_log, REFRESH_LOG_INTERVAL) {
        info!("ReplicaSidecar: WAL checkpoint PASSIVE for {db_path}: {res_passive:?}");
    }
    if expected_frames == 0 {
        return Ok(());
    }

    if checkpoint_covers_expected_frames(res_passive, expected_frames) {
        refresh_state.stale_failures = 0;
        refresh_state.stale_window_start = None;
        return Ok(());
    }
    if checkpoint_covers_visible_frames(res_passive) {
        refresh_state.stale_failures = 0;
        refresh_state.stale_window_start = None;
        return Ok(());
    }

    if let Some(pm) = process_manager {
        if should_log(&mut refresh_state.last_recovery_log, RECOVERY_LOG_INTERVAL) {
            warn!(
                "ReplicaSidecar: WAL checkpoint incomplete for {db_path}: {res_passive:?}. Pausing managed reader."
            );
        }
        let recovery =
            checkpoint_with_managed_reader_recovery(db_path, pm, expected_frames).await?;
        let res_retry = recovery.paused;
        info!(
            "ReplicaSidecar: WAL checkpoint PASSIVE after reader pause for {db_path}: {res_retry:?}"
        );
        if checkpoint_covers_expected_frames(res_retry, expected_frames)
            || checkpoint_covers_visible_frames(res_retry)
        {
            refresh_state.stale_failures = 0;
            refresh_state.stale_window_start = None;
            return Ok(());
        }

        let res_retry = recovery.recovered;
        info!("ReplicaSidecar: WAL checkpoint PASSIVE retry for {db_path}: {res_retry:?}");
        if !checkpoint_covers_expected_frames(res_retry, expected_frames)
            && !checkpoint_covers_visible_frames(res_retry)
        {
            return Err(ReplicaStreamError::Stream(
                StreamReplicationErrorCode::SnapshotBoundaryMismatch,
                "WAL-index refresh failed after recovery; replica requires restore".to_string(),
            ));
        }
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
        let res_retry = run_wal_checkpoint_passive(db_path).await?;
        if should_log(&mut refresh_state.last_recovery_log, RECOVERY_LOG_INTERVAL) {
            info!(
                "ReplicaSidecar: WAL checkpoint PASSIVE after SHM refresh for {db_path}: {res_retry:?}"
            );
        }
        if checkpoint_covers_expected_frames(res_retry, expected_frames) {
            refresh_state.stale_failures = 0;
            refresh_state.stale_window_start = None;
            return Ok(());
        }
        if checkpoint_covers_visible_frames(res_retry) {
            refresh_state.stale_failures = 0;
            refresh_state.stale_window_start = None;
            return Ok(());
        }
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

fn checkpoint_covers_expected_frames(result: (i32, i32, i32), expected_frames: u32) -> bool {
    result.0 == 0 && (result.1 as u32) >= expected_frames && (result.2 as u32) >= expected_frames
}

#[cfg(test)]
fn checkpoint_needs_reader_quiesce(result: (i32, i32, i32), expected_frames: u32) -> bool {
    result.0 != 0
        || ((result.1 as u32) >= expected_frames && (result.2 as u32) < expected_frames)
        || (result.1 > 0 && result.2 < result.1)
}

async fn run_wal_checkpoint_passive(db_path: &str) -> Result<(i32, i32, i32), ReplicaStreamError> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || run_wal_checkpoint_passive_sync(&db_path))
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?
}

fn run_wal_checkpoint_passive_sync(db_path: &str) -> Result<(i32, i32, i32), ReplicaStreamError> {
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

struct ManagedCheckpointRecovery {
    paused: (i32, i32, i32),
    recovered: (i32, i32, i32),
}

async fn checkpoint_with_managed_reader_recovery(
    db_path: &str,
    process_manager: &ProcessManager,
    expected_frames: u32,
) -> Result<ManagedCheckpointRecovery, ReplicaStreamError> {
    process_manager.add_blocker().await;

    let result = async {
        let paused = run_wal_checkpoint_passive(db_path).await?;
        if checkpoint_covers_expected_frames(paused, expected_frames) {
            return Ok(ManagedCheckpointRecovery {
                paused,
                recovered: paused,
            });
        }

        let shm_path = format!("{db_path}-shm");
        match fs::remove_file(&shm_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
        }?;
        let recovered = run_wal_checkpoint_passive(db_path).await?;
        Ok(ManagedCheckpointRecovery { paused, recovered })
    };

    let checkpoint_result = result.await;
    process_manager.remove_blocker().await;

    checkpoint_result
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

pub(super) async fn materialize_standalone_db(db_path: &str) -> Result<(), ReplicaStreamError> {
    let db_path_owned = db_path.to_string();
    tokio::task::spawn_blocking(move || materialize_standalone_db_sync(&db_path_owned))
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?
}

fn materialize_standalone_db_sync(db_path: &str) -> Result<(), ReplicaStreamError> {
    let temp_db_path = format!("{db_path}.standalone.tmp");
    match fs::remove_file(&temp_db_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
    }?;

    let conn = Connection::open(db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let escaped_path = temp_db_path.replace('\'', "''");
    conn.execute_batch(&format!("VACUUM main INTO '{escaped_path}'"))
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    drop(conn);

    fs::rename(&temp_db_path, db_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let _ = fs::remove_file(format!("{db_path}-wal"));
    let _ = fs::remove_file(format!("{db_path}-shm"));
    info!("ReplicaSidecar: materialized standalone DB for {db_path}");
    Ok(())
}

async fn reset_local_wal(db_path: &str, managed_reader: bool) -> Result<(), ReplicaStreamError> {
    let db_path_owned = db_path.to_string();
    let invalidate_warn_result =
        tokio::task::spawn_blocking(move || reset_local_wal_sync(&db_path_owned, managed_reader))
            .await;

    let invalidate_warn =
        invalidate_warn_result.map_err(|e| ReplicaStreamError::Transport(e.to_string()))??;

    if let Some(warn_message) = invalidate_warn {
        warn!("{warn_message}");
    }

    Ok(())
}

#[cfg(test)]
async fn apply_wal_bytes(
    db_path: &str,
    start_pos: &WalGenerationPos,
    wal_bytes: &[u8],
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    if let Some(pm) = process_manager {
        pm.add_blocker().await;
    }

    let result = apply_wal_bytes_with_reader_blocked(
        db_path,
        start_pos,
        wal_bytes,
        process_manager.is_some(),
    )
    .await;

    if let Some(pm) = process_manager {
        pm.remove_blocker().await;
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn apply_wal_chunk_and_refresh(
    db_path: &str,
    chunk_start: &WalGenerationPos,
    chunk_next: &WalGenerationPos,
    wal_bytes: &[u8],
    page_size: u64,
    frames_since_refresh: &mut u32,
    checkpoint_frame_interval: u32,
    checkpoint_interval_ms: u64,
    last_checkpoint_refresh: &mut Instant,
    refresh_state: &mut WalRefreshState,
    ack_floor: Option<&WalGenerationPos>,
    suppress_ack: &mut bool,
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    let managed_reader = process_manager.is_some();
    if let Some(pm) = process_manager {
        let jitter = managed_reader_restart_jitter();
        if !jitter.is_zero() {
            info!(
                "ReplicaSidecar: delaying managed reader restart by {}ms to avoid synchronized replica restarts",
                jitter.as_millis()
            );
            tokio::time::sleep(jitter).await;
        }
        pm.add_blocker().await;
    }

    let mut checkpoint_due = false;
    let result = async {
        apply_wal_bytes_with_reader_blocked(db_path, chunk_start, wal_bytes, managed_reader)
            .await?;
        store_shadow_wal_bytes(db_path, chunk_start, wal_bytes)
            .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
        persist_last_applied_lsn(db_path, chunk_next)
            .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

        let mut bytes_delta = chunk_next.offset.saturating_sub(chunk_start.offset);
        if chunk_start.offset == 0 {
            bytes_delta = bytes_delta.saturating_sub(WAL_HEADER_SIZE);
        }
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let frames_in_chunk = if frame_size > 0 {
            (bytes_delta / frame_size) as u32
        } else {
            0
        };
        *frames_since_refresh = frames_since_refresh.saturating_add(frames_in_chunk);

        if *suppress_ack {
            if let Some(floor) = ack_floor {
                if lsn_reached(chunk_next, floor) {
                    *suppress_ack = false;
                }
            }
        }

        let checkpoint_frame_due =
            checkpoint_frame_interval > 0 && *frames_since_refresh >= checkpoint_frame_interval;
        let checkpoint_time_due = checkpoint_interval_ms > 0
            && *frames_since_refresh > 0
            && last_checkpoint_refresh.elapsed() >= Duration::from_millis(checkpoint_interval_ms);
        checkpoint_due = !*suppress_ack && (checkpoint_frame_due || checkpoint_time_due);

        Ok(())
    }
    .await;

    if let Some(pm) = process_manager {
        if result.is_ok() {
            let refresh_result = async {
                if checkpoint_due {
                    let expected_frames = expected_frames_from_wal_file(db_path, page_size)?;
                    refresh_wal_index(db_path, expected_frames, refresh_state, None).await?;
                }
                *frames_since_refresh = 0;
                *last_checkpoint_refresh = Instant::now();
                Ok(())
            }
            .await;
            pm.remove_blocker().await;
            if let Err(err) = refresh_result {
                return Err(err);
            }
        } else {
            pm.remove_blocker().await;
        }
    }

    result
}

async fn apply_wal_bytes_with_reader_blocked(
    db_path: &str,
    start_pos: &WalGenerationPos,
    wal_bytes: &[u8],
    managed_reader: bool,
) -> Result<(), ReplicaStreamError> {
    if start_pos.offset == 0 {
        reset_local_wal(db_path, managed_reader).await?;
    } else {
        let wal_path = format!("{db_path}-wal");
        let wal_len = match fs::metadata(&wal_path) {
            Ok(meta) => Some(meta.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
        };
        if local_wal_needs_rewind(wal_len, start_pos.offset)
            && seed_wal_prefix_from_shadow(db_path, start_pos)?
        {
            info!(
                "ReplicaSidecar: seeded WAL prefix for {} from shadow wal at {}:{}:{}",
                db_path,
                start_pos.generation.as_str(),
                start_pos.index,
                start_pos.offset
            );
        }
    }

    let wal_path = format!("{db_path}-wal");
    let offset = start_pos.offset;
    let wal_bytes = wal_bytes.to_vec();
    let result =
        tokio::task::spawn_blocking(move || apply_wal_bytes_sync(&wal_path, offset, &wal_bytes))
            .await
            .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?;

    match result {
        Ok(()) => Ok(()),
        Err(ReplicaStreamError::Stream(StreamReplicationErrorCode::InvalidLsn, message)) => {
            // Local WAL state got truncated/reset underneath us (common when rebuilding SHM/WAL
            // state). Reset the local WAL so the next retry can safely rewind and re-stream.
            reset_local_wal(db_path, managed_reader).await?;
            Err(ReplicaStreamError::Stream(
                StreamReplicationErrorCode::InvalidLsn,
                message,
            ))
        }
        Err(other) => Err(other),
    }
}

fn reset_local_wal_sync(
    db_path: &str,
    managed_reader: bool,
) -> Result<Option<String>, ReplicaStreamError> {
    let wal_path = format!("{db_path}-wal");
    match fs::remove_file(&wal_path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
    }?;

    if managed_reader {
        let shm_path = format!("{db_path}-shm");
        match fs::remove_file(&shm_path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(ReplicaStreamError::Io(err.to_string())),
        }?;
        return Ok(None);
    }

    let warn_message = match invalidate_shm_header(db_path) {
        Ok(()) => None,
        Err(err) => Some(format!(
            "ReplicaSidecar: Failed to invalidate SHM header for {db_path}: {err}"
        )),
    };
    Ok(warn_message)
}

fn apply_wal_bytes_sync(
    wal_path: &str,
    offset: u64,
    wal_bytes: &[u8],
) -> Result<(), ReplicaStreamError> {
    let current_len = match fs::metadata(wal_path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if offset > 0 {
                return Err(ReplicaStreamError::Stream(
                    StreamReplicationErrorCode::InvalidLsn,
                    format!("WAL offset {offset} beyond EOF for {wal_path}"),
                ));
            }
            0
        }
        Err(err) => return Err(ReplicaStreamError::Io(err.to_string())),
    };

    if offset > 0 && current_len < offset {
        return Err(ReplicaStreamError::Stream(
            StreamReplicationErrorCode::InvalidLsn,
            format!("WAL offset {offset} beyond EOF {current_len} for {wal_path}"),
        ));
    }

    let mut wal_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(wal_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    if current_len > offset {
        wal_file
            .set_len(offset)
            .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    }

    wal_file
        .seek(SeekFrom::Start(offset))
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
    match replication_error_code_from_stream_error(&err) {
        Some(code) => ReplicaStreamError::Stream(code, err.message),
        None => {
            ReplicaStreamError::InvalidResponse(format!("unknown stream error: {}", err.message))
        }
    }
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

fn catchup_reader_release_ready(
    floor: Option<&WalGenerationPos>,
    chunk_start: &WalGenerationPos,
    chunk_next: &WalGenerationPos,
    suppress_ack: bool,
) -> bool {
    if suppress_ack {
        return false;
    }

    let Some(floor) = floor else {
        return true;
    };

    if lsn_matches(chunk_next, floor) {
        return false;
    }

    lsn_reached(chunk_next, floor) && chunk_start.generation == floor.generation
}

fn can_accept_stream_advance_past_floor(
    db_path: &str,
    floor: &WalGenerationPos,
    actual_start: &WalGenerationPos,
) -> bool {
    if actual_start.generation != floor.generation {
        return false;
    }
    if actual_start.index != floor.index + 1 || actual_start.offset != 0 {
        return false;
    }

    floor_is_shadow_wal_eof(db_path, floor)
}

fn floor_is_shadow_wal_eof(db_path: &str, floor: &WalGenerationPos) -> bool {
    let meta_dir = match ensure_meta_dir(db_path) {
        Ok(dir) => dir,
        Err(_) => return false,
    };
    let shadow_path = shadow_wal_file(
        meta_dir.to_str().unwrap_or("."),
        floor.generation.as_str(),
        floor.index,
    );
    let metadata = match fs::metadata(&shadow_path) {
        Ok(meta) => meta,
        Err(_) => return false,
    };
    metadata.len() == floor.offset
}

fn seed_wal_prefix_from_shadow(
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
    if shadow_meta.len() < pos.offset || pos.offset < WAL_HEADER_SIZE {
        return Ok(false);
    }

    let mut shadow_file =
        fs::File::open(&shadow_path).map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    let mut prefix = vec![0u8; pos.offset as usize];
    shadow_file
        .read_exact(&mut prefix)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;

    let wal_path = format!("{db_path}-wal");
    let mut wal_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&wal_path)
        .map_err(|e| ReplicaStreamError::Io(e.to_string()))?;
    wal_file
        .write_all(&prefix)
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

#[cfg(test)]
mod tests {
    use super::{
        ReplicaStreamError, WalRefreshState, apply_wal_bytes, apply_wal_chunk_and_refresh,
        can_accept_stream_advance_past_floor, materialize_standalone_db, refresh_wal_index,
        reset_local_wal, store_shadow_wal_bytes, validate_snapshot_meta,
    };
    use crate::base::Generation;
    use crate::cmd::replica_sidecar::process_manager::ProcessManager;
    use crate::database::WalGenerationPos;
    use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
    use crate::sync::StreamReplicationErrorCode;
    use crate::sync::replication::SnapshotMeta;
    use rusqlite::Connection;
    use std::fs;
    use std::io::Write;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn base_meta() -> SnapshotMeta {
        SnapshotMeta {
            db_identity: "db".to_string(),
            generation: "gen".to_string(),
            boundary_lsn: None,
            page_size: 4096,
            snapshot_size_bytes: 3,
            snapshot_sha256: vec![1, 2, 3, 4],
        }
    }

    #[test]
    fn rejects_snapshot_metadata_when_declared_size_differs_from_received_bytes() {
        let meta = base_meta();
        let err = validate_snapshot_meta(&meta, 4, &[1, 2, 3, 4]).unwrap_err();
        assert!(matches!(err, super::ReplicaStreamError::InvalidResponse(_)));
    }

    #[test]
    fn rejects_snapshot_metadata_when_declared_hash_differs_from_received_bytes() {
        let meta = base_meta();
        let err = validate_snapshot_meta(&meta, 3, &[9, 9, 9, 9]).unwrap_err();
        assert!(matches!(err, super::ReplicaStreamError::InvalidResponse(_)));
    }

    fn create_test_wal_db() -> (TempDir, String) {
        let temp_dir = tempfile::tempdir().expect("failed to create tempdir");
        let db_path = temp_dir.path().join("replica.db");
        let conn = Connection::open(&db_path).expect("failed to open sqlite db");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("failed to set WAL mode");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)",
            [],
        )
        .expect("failed to create table");
        conn.execute("INSERT INTO t(v) VALUES ('seed')", [])
            .expect("failed to insert seed row");
        drop(conn);
        (temp_dir, db_path.to_string_lossy().to_string())
    }

    fn create_replica_with_valid_wal_chunk(row_value: &str) -> (TempDir, String, Vec<u8>, u64) {
        let temp_dir = tempfile::tempdir().expect("failed to create tempdir");
        let primary_path = temp_dir.path().join("primary.db");
        let replica_path = temp_dir.path().join("replica.db");

        let conn = Connection::open(&primary_path).expect("failed to open primary sqlite db");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("failed to set WAL mode");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)",
            [],
        )
        .expect("failed to create table");
        conn.execute("INSERT INTO t(v) VALUES ('seed')", [])
            .expect("failed to insert seed row");
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint seed row into db file");
        let page_size_i64: i64 = conn
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .expect("read page size");
        drop(conn);

        fs::copy(&primary_path, &replica_path).expect("copy base db to replica");

        let writer = Connection::open(&primary_path).expect("reopen primary writer");
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .expect("set WAL mode on writer");
        writer
            .execute("INSERT INTO t(v) VALUES (?)", [row_value])
            .expect("write WAL row");

        let wal_bytes = fs::read(format!("{}-wal", primary_path.to_string_lossy()))
            .expect("read valid WAL bytes");
        assert!(
            wal_bytes.len() > WAL_HEADER_SIZE as usize,
            "test setup should produce WAL frames"
        );

        (
            temp_dir,
            replica_path.to_string_lossy().to_string(),
            wal_bytes,
            u64::try_from(page_size_i64).expect("valid page size"),
        )
    }

    #[tokio::test]
    async fn materialize_standalone_db_carries_visible_wal_frames_into_db_file() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let conn = Connection::open(&db_path).expect("open writer");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("set WAL mode");
        conn.execute("INSERT INTO t(v) VALUES ('wal-visible')", [])
            .expect("write WAL row");

        materialize_standalone_db(&db_path)
            .await
            .expect("materialize standalone db");
        drop(conn);

        assert!(
            !std::path::Path::new(&format!("{db_path}-wal")).exists(),
            "materialized DB should not require a WAL sidecar"
        );
        let restored = Connection::open(&db_path).expect("open materialized db");
        let values: Vec<String> = {
            let mut stmt = restored
                .prepare("SELECT v FROM t ORDER BY id")
                .expect("prepare values query");
            stmt.query_map([], |row| row.get(0))
                .expect("query values")
                .collect::<std::result::Result<Vec<_>, _>>()
                .expect("collect values")
        };
        assert_eq!(values, vec!["seed".to_string(), "wal-visible".to_string()]);
    }

    #[test]
    fn allows_next_wal_index_once_replay_reaches_floor_eof() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let db_path = temp_dir.path().join("replica.db");
        fs::write(&db_path, []).expect("create db");

        let generation = Generation::new();
        let meta_dir = temp_dir.path().join(format!(
            ".{}-replited",
            db_path.file_name().unwrap().to_string_lossy()
        ));
        let wal_dir = meta_dir
            .join("generations")
            .join(generation.as_str())
            .join("wal");
        fs::create_dir_all(&wal_dir).expect("create wal dir");
        let wal_file = wal_dir.join(format!("{:010}.wal", 7));
        fs::write(&wal_file, vec![0u8; 8192]).expect("write wal");

        let floor = WalGenerationPos {
            generation: generation.clone(),
            index: 7,
            offset: 8192,
        };
        let next = WalGenerationPos {
            generation,
            index: 8,
            offset: 0,
        };

        assert!(can_accept_stream_advance_past_floor(
            db_path.to_str().unwrap(),
            &floor,
            &next
        ));
    }

    #[test]
    fn rejects_next_wal_index_before_replay_reaches_floor_eof() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let db_path = temp_dir.path().join("replica.db");
        fs::write(&db_path, []).expect("create db");

        let generation = Generation::new();
        let meta_dir = temp_dir.path().join(format!(
            ".{}-replited",
            db_path.file_name().unwrap().to_string_lossy()
        ));
        let wal_dir = meta_dir
            .join("generations")
            .join(generation.as_str())
            .join("wal");
        fs::create_dir_all(&wal_dir).expect("create wal dir");
        let wal_file = wal_dir.join(format!("{:010}.wal", 7));
        fs::write(&wal_file, vec![0u8; 8192]).expect("write wal");

        let floor = WalGenerationPos {
            generation: generation.clone(),
            index: 7,
            offset: 4096,
        };
        let next = WalGenerationPos {
            generation,
            index: 8,
            offset: 0,
        };

        assert!(!can_accept_stream_advance_past_floor(
            db_path.to_str().unwrap(),
            &floor,
            &next
        ));
    }

    #[test]
    fn allows_checkpoint_refresh_once_replay_reaches_floor() {
        let mut suppress_ack = true;
        let floor = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 325_512,
        };
        let chunk_next = WalGenerationPos {
            generation: floor.generation.clone(),
            index: 1,
            offset: 325_512,
        };

        if suppress_ack && super::lsn_reached(&chunk_next, &floor) {
            suppress_ack = false;
        }

        assert!(!suppress_ack);
    }

    #[test]
    fn catchup_reader_stays_blocked_when_replay_lands_exactly_on_floor() {
        let floor = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 325_512,
        };
        let chunk_start = WalGenerationPos {
            generation: floor.generation.clone(),
            index: 1,
            offset: 259_592,
        };
        let chunk_next = floor.clone();

        assert!(!super::catchup_reader_release_ready(
            Some(&floor),
            &chunk_start,
            &chunk_next,
            false
        ));
    }

    #[test]
    fn catchup_reader_releases_after_replay_applies_data_past_floor() {
        let floor = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 325_512,
        };
        let chunk_start = WalGenerationPos {
            generation: floor.generation.clone(),
            index: 1,
            offset: 325_512,
        };
        let chunk_next = WalGenerationPos {
            generation: floor.generation.clone(),
            index: 1,
            offset: 469_712,
        };

        assert!(super::catchup_reader_release_ready(
            Some(&floor),
            &chunk_start,
            &chunk_next,
            false
        ));
    }

    #[test]
    fn keeps_checkpoint_refresh_suppressed_while_replay_is_still_below_floor() {
        let generation = Generation::new();
        let floor = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 325_512,
        };
        let replay_chunk = WalGenerationPos {
            generation,
            index: 1,
            offset: 259_592,
        };

        assert!(!super::lsn_reached(&replay_chunk, &floor));
    }

    #[test]
    fn checkpoint_result_is_not_safe_when_reader_blocks_visible_frames() {
        let expected_frames = 250;
        let reader_blocked_result = (0, 250, 4);

        assert!(!super::checkpoint_covers_expected_frames(
            reader_blocked_result,
            expected_frames
        ));
        assert!(super::checkpoint_needs_reader_quiesce(
            reader_blocked_result,
            expected_frames
        ));
    }

    #[test]
    fn checkpoint_result_is_not_safe_when_file_has_uncheckpointed_tail_frames() {
        let expected_frames_from_file_len = 250;
        let valid_prefix_result = (0, 4, 4);

        assert!(!super::checkpoint_covers_expected_frames(
            valid_prefix_result,
            expected_frames_from_file_len
        ));
        assert!(super::checkpoint_covers_visible_frames(valid_prefix_result));
        assert!(!super::checkpoint_needs_reader_quiesce(
            valid_prefix_result,
            expected_frames_from_file_len
        ));
    }

    #[tokio::test]
    async fn apply_wal_bytes_resets_local_wal_and_returns_invalid_lsn_when_offset_beyond_eof() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let wal_path = format!("{db_path}-wal");

        // Seed a local WAL smaller than the requested offset to simulate truncation/reset.
        fs::write(&wal_path, b"short").expect("seed wal");

        let start = WalGenerationPos {
            generation: Generation::new(),
            index: 0,
            offset: 100,
        };

        let err = apply_wal_bytes(&db_path, &start, b"data", None)
            .await
            .expect_err("apply should fail");
        match err {
            ReplicaStreamError::Stream(code, message) => {
                assert_eq!(code, StreamReplicationErrorCode::InvalidLsn);
                assert!(message.contains("beyond EOF"));
            }
            other => panic!("expected stream error, got {other:?}"),
        }

        assert!(
            !fs::exists(&wal_path).expect("stat wal"),
            "expected local WAL to be reset after invalid lsn"
        );
    }

    #[tokio::test]
    async fn apply_wal_bytes_seeds_shadow_prefix_when_resume_offset_is_beyond_local_wal() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let wal_path = format!("{db_path}-wal");
        let generation = Generation::new();
        let prefix = vec![7u8; WAL_HEADER_SIZE as usize];
        let start = WalGenerationPos {
            generation,
            index: 3,
            offset: WAL_HEADER_SIZE,
        };

        store_shadow_wal_bytes(
            &db_path,
            &WalGenerationPos {
                offset: 0,
                ..start.clone()
            },
            &prefix,
        )
        .expect("seed shadow wal");
        fs::write(&wal_path, b"short").expect("seed truncated local wal");

        apply_wal_bytes(&db_path, &start, b"data", None)
            .await
            .expect("apply should seed shadow prefix before appending");

        let wal = fs::read(&wal_path).expect("read local wal");
        assert_eq!(&wal[..WAL_HEADER_SIZE as usize], prefix.as_slice());
        assert_eq!(&wal[WAL_HEADER_SIZE as usize..], b"data");
    }

    #[tokio::test]
    async fn apply_wal_bytes_starts_managed_reader_after_offset_zero_wal_is_written() {
        let (temp_dir, db_path) = create_test_wal_db();
        let wal_path = format!("{db_path}-wal");
        fs::write(&wal_path, b"stale").expect("seed stale wal");

        let marker_path = temp_dir.path().join("reader-started-with-wal");
        let cmd = format!(
            "if [ -s '{}' ]; then printf wal-present > '{}'; else printf wal-missing > '{}'; fi; sleep 60",
            wal_path,
            marker_path.display(),
            marker_path.display()
        );
        let process_manager = ProcessManager::new(cmd);
        let start = WalGenerationPos {
            generation: Generation::new(),
            index: 4,
            offset: 0,
        };
        let wal_bytes = vec![9u8; 8 * 1024 * 1024];

        apply_wal_bytes(&db_path, &start, &wal_bytes, Some(&process_manager))
            .await
            .expect("apply should succeed");

        let deadline = Instant::now() + Duration::from_secs(2);
        while fs::read_to_string(&marker_path)
            .map(|marker| marker.is_empty())
            .unwrap_or(true)
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let marker = fs::read_to_string(&marker_path).expect("reader start marker");
        assert_eq!(
            marker, "wal-present",
            "managed reader must not start between WAL reset and WAL append"
        );
        process_manager.stop().await;
    }

    #[tokio::test]
    async fn reset_local_wal_keeps_managed_reader_blocked_for_caller_to_apply_wal() {
        let (temp_dir, db_path) = create_test_wal_db();
        let marker_path = temp_dir.path().join("reader-started");
        let cmd = format!("printf started > '{}'; sleep 60", marker_path.display());
        let process_manager = ProcessManager::new(cmd);

        process_manager.add_blocker().await;
        reset_local_wal(&db_path, true)
            .await
            .expect("reset local wal");
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            process_manager.blocker_count(),
            1,
            "caller must retain the reader blocker until WAL append/checkpoint is complete"
        );
        assert!(
            !marker_path.exists(),
            "managed reader must not restart immediately after WAL reset"
        );

        process_manager.remove_blocker().await;
        process_manager.stop().await;
    }

    #[tokio::test]
    async fn apply_wal_chunk_keeps_managed_reader_blocked_until_shadow_and_lsn_are_durable() {
        let (temp_dir, db_path, wal_bytes, page_size) =
            create_replica_with_valid_wal_chunk("chunk-visible");
        let wal_path = format!("{db_path}-wal");
        let generation = Generation::new();
        let chunk_start = WalGenerationPos {
            generation: generation.clone(),
            index: 5,
            offset: 0,
        };
        let chunk_next = WalGenerationPos {
            generation,
            index: 5,
            offset: wal_bytes.len() as u64,
        };

        let meta_dir = temp_dir.path().join(format!(
            ".{}-replited",
            std::path::Path::new(&db_path)
                .file_name()
                .unwrap()
                .to_string_lossy()
        ));
        let shadow_path = meta_dir
            .join("generations")
            .join(chunk_start.generation.as_str())
            .join("wal")
            .join(format!("{:010}.wal", chunk_start.index));
        let last_applied_lsn_path = meta_dir.join("last_applied_lsn");
        let marker_path = temp_dir.path().join("reader-started-after-durable");
        let cmd = format!(
            "if [ -s '{}' ] && [ -s '{}' ] && [ -s '{}' ]; then printf durable > '{}'; else printf early > '{}'; fi; sleep 60",
            wal_path,
            shadow_path.display(),
            last_applied_lsn_path.display(),
            marker_path.display(),
            marker_path.display()
        );
        let process_manager = ProcessManager::new(cmd);
        let mut frames_since_refresh = 0;
        let mut last_checkpoint_refresh = Instant::now();
        let mut refresh_state = WalRefreshState::new();
        let mut suppress_ack = false;

        apply_wal_chunk_and_refresh(
            &db_path,
            &chunk_start,
            &chunk_next,
            &wal_bytes,
            page_size,
            &mut frames_since_refresh,
            0,
            0,
            &mut last_checkpoint_refresh,
            &mut refresh_state,
            None,
            &mut suppress_ack,
            Some(&process_manager),
        )
        .await
        .expect("chunk should apply");

        let deadline = Instant::now() + Duration::from_secs(2);
        while !marker_path.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let marker = fs::read_to_string(&marker_path).expect("reader start marker");
        assert_eq!(
            marker, "durable",
            "managed reader must start only after live WAL, shadow WAL, and last_applied_lsn are durable"
        );
        let replica = Connection::open(&db_path).expect("open materialized replica");
        let visible: i64 = replica
            .query_row(
                "SELECT COUNT(*) FROM t WHERE v = 'chunk-visible'",
                [],
                |row| row.get(0),
            )
            .expect("query materialized row");
        assert_eq!(visible, 1, "materialized DB should include applied WAL row");
        process_manager.stop().await;
    }

    #[tokio::test]
    async fn apply_wal_chunk_restarts_managed_reader_after_non_checkpoint_tail_chunk() {
        let (temp_dir, db_path) = create_test_wal_db();
        let wal_path = format!("{db_path}-wal");
        fs::write(&wal_path, vec![0u8; WAL_HEADER_SIZE as usize]).expect("seed WAL prefix");
        let generation = Generation::new();
        let chunk_start = WalGenerationPos {
            generation: generation.clone(),
            index: 5,
            offset: WAL_HEADER_SIZE,
        };
        let wal_bytes = b"tail";
        let chunk_next = WalGenerationPos {
            generation,
            index: 5,
            offset: WAL_HEADER_SIZE + wal_bytes.len() as u64,
        };
        let marker_path = temp_dir.path().join("reader-start-count");
        let cmd = format!("printf started >> '{}'; sleep 60", marker_path.display());
        let process_manager = ProcessManager::new(cmd);
        process_manager.start().await;
        let deadline = Instant::now() + Duration::from_secs(2);
        while !marker_path.exists() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            fs::read_to_string(&marker_path).expect("initial reader start marker"),
            "started"
        );

        let mut frames_since_refresh = 0;
        let mut last_checkpoint_refresh = Instant::now();
        let mut refresh_state = WalRefreshState::new();
        let mut suppress_ack = false;

        apply_wal_chunk_and_refresh(
            &db_path,
            &chunk_start,
            &chunk_next,
            wal_bytes,
            4096,
            &mut frames_since_refresh,
            10000,
            60000,
            &mut last_checkpoint_refresh,
            &mut refresh_state,
            None,
            &mut suppress_ack,
            Some(&process_manager),
        )
        .await
        .expect("tail chunk should apply and restart reader");

        tokio::time::sleep(Duration::from_millis(100)).await;
        let marker = fs::read_to_string(&marker_path).expect("reader start marker");
        assert_eq!(
            marker, "startedstarted",
            "managed reader must restart after ordinary tail chunks so a live child reopens the updated DB/WAL set"
        );
        assert_eq!(process_manager.blocker_count(), 0);
        assert!(
            std::path::Path::new(&wal_path).exists(),
            "managed reader restart should preserve the WAL file needed for same-generation tail chunks"
        );
        process_manager.stop().await;
    }

    #[test]
    fn managed_reader_restart_jitter_is_stable_and_bounded_for_replica_seed() {
        let jitter =
            super::managed_reader_restart_jitter_for("latest-state-replited-read-a", 15_000);

        assert_eq!(
            jitter,
            super::managed_reader_restart_jitter_for("latest-state-replited-read-a", 15_000)
        );
        assert!(jitter <= Duration::from_millis(15_000));
        assert_eq!(
            super::managed_reader_restart_jitter_for("latest-state-replited-read-a", 0),
            Duration::ZERO
        );
    }

    #[test]
    fn local_wal_needs_rewind_when_resume_offset_is_beyond_local_file_length() {
        assert!(super::local_wal_needs_rewind(None, 259_592));
        assert!(super::local_wal_needs_rewind(
            Some(WAL_HEADER_SIZE),
            259_592
        ));
        assert!(!super::local_wal_needs_rewind(Some(259_592), 259_592));
        assert!(!super::local_wal_needs_rewind(Some(0), 0));
    }

    #[tokio::test]
    async fn tolerates_initial_stale_refresh_without_managed_recovery() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let mut state = WalRefreshState::new();

        refresh_wal_index(&db_path, 1, &mut state, None)
            .await
            .expect("first stale refresh should not reset the replica immediately");
        assert_eq!(state.stale_failures, 1);
    }

    #[tokio::test]
    async fn refresh_accepts_checkpointed_visible_frames_with_unindexed_tail() {
        let (temp_dir, db_path) = create_test_wal_db();
        let conn = Connection::open(&db_path).expect("open sqlite db");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("set wal mode");
        conn.execute("INSERT INTO t(v) VALUES ('committed')", [])
            .expect("insert committed row");

        let page_size_i64: i64 = conn
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .expect("read page_size");
        let page_size = u64::try_from(page_size_i64).expect("valid page size");
        let frame_size = WAL_FRAME_HEADER_SIZE + page_size;
        let (_, observed_frames, _) = super::run_wal_checkpoint_passive(&db_path)
            .await
            .expect("checkpoint passive");
        assert!(
            observed_frames > 0,
            "test setup should create visible WAL frames"
        );

        let wal_path = format!("{db_path}-wal");
        let mut wal = fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .expect("open wal");
        wal.write_all(&vec![0; frame_size as usize])
            .expect("append tail frame");
        wal.sync_all().expect("sync wal");

        let expected_frames = super::expected_frames_from_wal_file(&db_path, page_size)
            .expect("expected frame count");
        assert!(
            expected_frames > observed_frames as u32,
            "test setup should make file-derived frames exceed SQLite-visible frames"
        );

        let mut state = WalRefreshState::new();
        let process_manager = ProcessManager::new(String::new());
        refresh_wal_index(
            &db_path,
            expected_frames,
            &mut state,
            Some(&process_manager),
        )
        .await
        .expect("checkpointed visible frames should be safe even with an unindexed tail");

        drop(conn);
        drop(temp_dir);
    }

    #[tokio::test]
    async fn requests_restore_after_repeated_stale_refreshes_without_managed_recovery() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let mut state = WalRefreshState::new();

        for _ in 0..(super::STALE_THRESHOLD - 1) {
            refresh_wal_index(&db_path, 1, &mut state, None)
                .await
                .expect("stale refresh should be tolerated below the threshold");
        }

        let err = refresh_wal_index(&db_path, 1, &mut state, None)
            .await
            .unwrap_err();
        match err {
            ReplicaStreamError::Stream(code, message) => {
                assert_eq!(code, StreamReplicationErrorCode::SnapshotBoundaryMismatch);
                assert!(message.contains("requires restore"));
            }
            other => panic!("expected stream error, got {other:?}"),
        }
        assert_eq!(state.stale_failures, 0);
    }

    #[tokio::test]
    async fn requests_managed_recovery_when_refresh_stays_stale() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let mut state = WalRefreshState::new();
        let process_manager = ProcessManager::new(String::new());

        let err = refresh_wal_index(&db_path, 1, &mut state, Some(&process_manager))
            .await
            .unwrap_err();
        match err {
            ReplicaStreamError::Stream(code, message) => {
                assert_eq!(code, StreamReplicationErrorCode::SnapshotBoundaryMismatch);
                assert!(message.contains("after recovery"));
            }
            other => panic!("expected stream error, got {other:?}"),
        }
    }
}

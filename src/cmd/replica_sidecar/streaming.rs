use std::fs;
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
use crate::sync::replication::stream_snapshot_v2_response::Payload as StreamSnapshotV2Payload;
use crate::sync::replication::stream_wal_v2_response::Payload as StreamWalV2Payload;
use crate::sync::replication::{
    AckLsnV2Request, SnapshotMeta, StreamError, StreamSnapshotV2Request, StreamWalV2Request,
};
use crate::sync::stream_client::StreamClient;
use crate::sync::stream_protocol::{
    lsn_token_from_pos, lsn_token_to_pos, replication_error_code_from_stream_error,
};

use super::local_state::{ensure_meta_dir, persist_last_applied_lsn};
use super::{ProcessManager, ReplicaStreamError};

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
                info!(
                    "ReplicaSidecar: WAL header missing for {} at offset {}; rewinding stream to 0 (expected on cold start or after WAL cleanup)",
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
    let page_size = load_db_page_size(db_path).await?;
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
                let chunk_start = lsn_token_to_pos(Some(chunk_start_token.clone()))
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;
                let chunk_next = lsn_token_to_pos(Some(chunk_next_token.clone()))
                    .map_err(|e| ReplicaStreamError::InvalidResponse(e.to_string()))?;

                if let Some(floor) = &ack_floor {
                    if chunk_start.generation == floor.generation && chunk_start.index > floor.index
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

                if !lsn_matches(&chunk_start, &current_pos)
                    && !allow_shadow_wal_boundary_advance(db_path, &current_pos, &chunk_start)
                {
                    return Err(ReplicaStreamError::InvalidResponse(format!(
                        "chunk start mismatch: expected {current_pos:?}, got {chunk_start:?}",
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

pub(super) async fn ack_lsn_or_warn(
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

async fn load_db_page_size(db_path: &str) -> Result<u64, ReplicaStreamError> {
    let db_path = db_path.to_string();
    tokio::task::spawn_blocking(move || load_db_page_size_sync(&db_path))
        .await
        .map_err(|e| ReplicaStreamError::Transport(e.to_string()))?
}

fn load_db_page_size_sync(db_path: &str) -> Result<u64, ReplicaStreamError> {
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
    let res_passive = run_wal_checkpoint_passive(db_path).await?;
    if should_log(&mut refresh_state.last_refresh_log, REFRESH_LOG_INTERVAL) {
        info!("ReplicaSidecar: WAL checkpoint PASSIVE for {db_path}: {res_passive:?}");
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
        let res_retry = run_wal_checkpoint_passive(db_path).await?;
        if should_log(&mut refresh_state.last_recovery_log, RECOVERY_LOG_INTERVAL) {
            info!(
                "ReplicaSidecar: WAL checkpoint PASSIVE after SHM refresh for {db_path}: {res_retry:?}"
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
            warn!("ReplicaSidecar: WAL-index still stale for {db_path}. Triggering SHM recovery.");
        }
        recover_stale_shm(db_path, process_manager).await?;
        let res_retry = run_wal_checkpoint_passive(db_path).await?;
        info!("ReplicaSidecar: WAL checkpoint PASSIVE retry for {db_path}: {res_retry:?}");
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

    let db_path_owned = db_path.to_string();
    let managed_reader = process_manager.is_some();
    let invalidate_warn_result =
        tokio::task::spawn_blocking(move || reset_local_wal_sync(&db_path_owned, managed_reader))
            .await;

    if let Some(pm) = process_manager {
        pm.remove_blocker().await;
    }

    let invalidate_warn =
        invalidate_warn_result.map_err(|e| ReplicaStreamError::Transport(e.to_string()))??;

    if let Some(warn_message) = invalidate_warn {
        warn!("{warn_message}");
    }

    Ok(())
}

async fn apply_wal_bytes(
    db_path: &str,
    start_pos: &WalGenerationPos,
    wal_bytes: &[u8],
    process_manager: Option<&ProcessManager>,
) -> Result<(), ReplicaStreamError> {
    if start_pos.offset == 0 {
        reset_local_wal(db_path, process_manager).await?;
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
            reset_local_wal(db_path, process_manager).await?;
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

#[cfg(test)]
mod tests {
    use super::{
        ReplicaStreamError, WalRefreshState, apply_wal_bytes, can_accept_stream_advance_past_floor,
        refresh_wal_index, validate_snapshot_meta,
    };
    use crate::base::Generation;
    use crate::cmd::replica_sidecar::process_manager::ProcessManager;
    use crate::database::WalGenerationPos;
    use crate::sync::StreamReplicationErrorCode;
    use crate::sync::replication::SnapshotMeta;
    use rusqlite::Connection;
    use std::fs;
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
    fn validate_snapshot_meta_rejects_size_mismatch() {
        let meta = base_meta();
        let err = validate_snapshot_meta(&meta, 4, &[1, 2, 3, 4]).unwrap_err();
        assert!(matches!(err, super::ReplicaStreamError::InvalidResponse(_)));
    }

    #[test]
    fn validate_snapshot_meta_rejects_hash_mismatch() {
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

    #[test]
    fn accepts_floor_eof_transition_to_next_index() {
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
    fn rejects_floor_transition_when_floor_not_eof() {
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
    async fn refresh_wal_index_retries_before_restore_without_process_manager() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let mut state = WalRefreshState::new();

        assert!(
            refresh_wal_index(&db_path, 1, &mut state, None)
                .await
                .is_ok()
        );
        assert_eq!(state.stale_failures, 1);
        assert!(
            refresh_wal_index(&db_path, 1, &mut state, None)
                .await
                .is_ok()
        );
        assert_eq!(state.stale_failures, 2);
    }

    #[tokio::test]
    async fn refresh_wal_index_escalates_to_restore_after_threshold_without_process_manager() {
        let (_temp_dir, db_path) = create_test_wal_db();
        let mut state = WalRefreshState::new();

        for _ in 0..(super::STALE_THRESHOLD - 1) {
            assert!(
                refresh_wal_index(&db_path, 1, &mut state, None)
                    .await
                    .is_ok()
            );
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
    async fn refresh_wal_index_uses_recovery_path_with_process_manager() {
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

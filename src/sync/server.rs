use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::base::{Generation, generation_file_path, shadow_wal_dir, shadow_wal_file};
use crate::config::{Config, DbConfig};
use crate::database::{DatabaseInfo, SnapshotStreamData, WalGenerationPos, snapshot_for_stream};
use crate::pb::replication::replication_server::Replication;
use crate::pb::replication::stream_error::Code as StreamErrorCode;
use crate::pb::replication::stream_snapshot_v2_response::Payload as StreamSnapshotV2Payload;
use crate::pb::replication::stream_wal_v2_response::Payload as StreamWalV2Payload;
use crate::pb::replication::{
    AckLsnV2Request, AckLsnV2Response, LsnToken, SnapshotMeta, StreamError,
    StreamSnapshotV2Request, StreamSnapshotV2Response, StreamWalV2Request, StreamWalV2Response,
    WalChunkV2,
};
use crate::sqlite::{WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALFrame, WALHeader};
use crate::sync::replica_progress;
use crate::sync::stream_protocol::{
    lsn_token_from_pos, lsn_token_to_pos, meta_dir_for_db_path, optional_lsn_token_to_pos,
    stream_error_from_replication_error,
};
use crate::sync::{Lsn, ReplicaRecoveryAction, ShadowWalReader, StreamReplicationError};

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
    snapshot_semaphores: HashMap<String, Arc<Semaphore>>,
    _connections: Arc<Mutex<Vec<rusqlite::Connection>>>, // Keep connections open to pin WAL files
}

const SNAPSHOT_CHUNK_SIZE: usize = 1024 * 1024;
const SNAPSHOT_RETRY_COUNT: usize = 5;
const SNAPSHOT_RETRY_DELAY_MS: u64 = 200;

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
                    log::warn!(
                        "Primary: Failed to set WAL mode for {}: {}",
                        path.display(),
                        e
                    );
                }
                _connections.push(conn);
                log::debug!(
                    "Primary: Opened persistent connection for {}",
                    path.display()
                );
            } else {
                log::error!(
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
    type StreamWalV2Stream = ReceiverStream<Result<StreamWalV2Response, Status>>;

    async fn stream_wal_v2(
        &self,
        request: Request<StreamWalV2Request>,
    ) -> Result<Response<Self::StreamWalV2Stream>, Status> {
        let req = request.into_inner();
        let db_identity = req.db_identity.clone();
        let replica_id = req.replica_id.clone();
        let db_path = self
            .db_paths
            .get(&db_identity)
            .ok_or_else(|| Status::not_found(format!("Database {db_identity} not found")))?
            .clone();

        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            stream_wal_v2_from_shadow(tx, db_identity, db_path, replica_id, req.start_lsn).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type StreamSnapshotV2Stream = ReceiverStream<Result<StreamSnapshotV2Response, Status>>;

    async fn stream_snapshot_v2(
        &self,
        request: Request<StreamSnapshotV2Request>,
    ) -> Result<Response<Self::StreamSnapshotV2Stream>, Status> {
        let req = request.into_inner();
        let db_identity = req.db_identity.clone();

        let semaphore = self
            .snapshot_semaphores
            .get(&db_identity)
            .ok_or_else(|| Status::not_found(format!("Database {db_identity} not found")))?
            .clone();

        let db_config = self
            .db_configs
            .get(&db_identity)
            .ok_or_else(|| Status::not_found(format!("Database {db_identity} not found")))?
            .clone();

        let (tx, rx) = mpsc::channel(8);

        let permit = match semaphore.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                send_snapshot_stream_error(
                    &tx,
                    StreamErrorCode::Unknown,
                    "Too many concurrent snapshots".to_string(),
                    1000,
                )
                .await;
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        };

        tokio::spawn(async move {
            let _permit = permit;

            let requested_pos = match optional_lsn_token_to_pos(req.start_lsn) {
                Ok(pos) => pos,
                Err(err) => {
                    send_snapshot_replication_error(&tx, err).await;
                    return;
                }
            };

            let snapshot = match snapshot_for_stream_with_retry(&db_identity, &db_config).await {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    send_snapshot_replication_error(&tx, err).await;
                    return;
                }
            };

            if let Err(err) = validate_snapshot_request(requested_pos.as_ref(), &snapshot) {
                send_snapshot_replication_error(&tx, err).await;
                return;
            }

            send_snapshot_stream(&tx, &db_identity, &snapshot).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_lsn_v2(
        &self,
        request: Request<AckLsnV2Request>,
    ) -> Result<Response<AckLsnV2Response>, Status> {
        let req = request.into_inner();
        let db_identity = req.db_identity.clone();
        let replica_id = req.replica_id.clone();
        if replica_id.trim().is_empty() {
            return Ok(Response::new(AckLsnV2Response {
                accepted: false,
                error: Some(stream_error_from_replication_error(
                    StreamReplicationError::InvalidLsn("replica_id is required".to_string()),
                    0,
                )),
            }));
        }

        let db_path = self
            .db_paths
            .get(&db_identity)
            .ok_or_else(|| Status::not_found(format!("Database {db_identity} not found")))?
            .clone();

        let pos = match lsn_token_to_pos(req.last_applied_lsn) {
            Ok(pos) => pos,
            Err(err) => {
                return Ok(Response::new(AckLsnV2Response {
                    accepted: false,
                    error: Some(stream_error_from_replication_error(err, 0)),
                }));
            }
        };

        let meta_dir = match meta_dir_for_db_path(&db_path) {
            Some(dir) => dir,
            None => {
                return Ok(Response::new(AckLsnV2Response {
                    accepted: false,
                    error: Some(stream_error_from_replication_error(
                        StreamReplicationError::LineageMismatch(format!(
                            "invalid db path for ack: {}",
                            db_path.display()
                        )),
                        0,
                    )),
                }));
            }
        };

        match read_current_generation(&meta_dir) {
            Some(current_generation) if current_generation == pos.generation => {}
            Some(current_generation) => {
                return Ok(Response::new(AckLsnV2Response {
                    accepted: false,
                    error: Some(stream_error_from_replication_error(
                        StreamReplicationError::LineageMismatch(format!(
                            "ack generation mismatch for replica {replica_id}: requested {}, current {}",
                            pos.generation.as_str(),
                            current_generation.as_str()
                        )),
                        0,
                    )),
                }));
            }
            None => {
                return Ok(Response::new(AckLsnV2Response {
                    accepted: false,
                    error: Some(stream_error_from_replication_error(
                        StreamReplicationError::LineageMismatch(format!(
                            "ack generation unavailable for replica {replica_id}",
                        )),
                        0,
                    )),
                }));
            }
        }

        if let Err(err) =
            replica_progress::update_ack(&db_identity, &replica_id, &req.session_id, pos)
        {
            log::warn!(
                "Primary: ack regression for {db_identity} replica {replica_id}: previous {}:{}:{}, attempted {}:{}:{}",
                err.previous.generation.as_str(),
                err.previous.index,
                err.previous.offset,
                err.attempted.generation.as_str(),
                err.attempted.index,
                err.attempted.offset,
            );
            return Ok(Response::new(AckLsnV2Response {
                accepted: false,
                error: Some(stream_error_from_replication_error(
                    StreamReplicationError::InvalidLsn(format!(
                        "ack regression for replica {replica_id}: previous {}:{}:{}, attempted {}:{}:{}",
                        err.previous.generation.as_str(),
                        err.previous.index,
                        err.previous.offset,
                        err.attempted.generation.as_str(),
                        err.attempted.index,
                        err.attempted.offset,
                    )),
                    0,
                )),
            }));
        }

        Ok(Response::new(AckLsnV2Response {
            accepted: true,
            error: None,
        }))
    }
}

async fn stream_wal_v2_from_shadow(
    tx: mpsc::Sender<Result<StreamWalV2Response, Status>>,
    db_identity: String,
    db_path: PathBuf,
    replica_id: String,
    start_lsn: Option<LsnToken>,
) {
    let start_pos = match lsn_token_to_pos(start_lsn) {
        Ok(pos) => pos,
        Err(err) => {
            send_replication_error(&tx, err).await;
            return;
        }
    };

    let meta_dir = match meta_dir_for_db_path(&db_path) {
        Some(dir) => dir,
        None => {
            send_stream_error(
                &tx,
                StreamErrorCode::Unknown,
                format!("Invalid db path for {db_identity}: {}", db_path.display()),
            )
            .await;
            return;
        }
    };

    if let Err(err) = validate_start_generation(&meta_dir, &start_pos) {
        send_replication_error(&tx, err).await;
        return;
    }

    let _lease_guard =
        replica_progress::register_lease(&db_identity, &replica_id, start_pos.clone());

    if let Some(err) = retention_error(&db_identity, &start_pos) {
        send_replication_error(&tx, err).await;
        return;
    }

    let (info, wal_path, page_size) = match build_stream_info(&meta_dir, &start_pos) {
        Ok(value) => value,
        Err(err) => {
            send_replication_error(&tx, err).await;
            return;
        }
    };

    let (reader, chunk_start_override) = match open_stream_reader(&start_pos, &info, &wal_path) {
        Ok(value) => value,
        Err(err) => {
            send_replication_error(&tx, err).await;
            return;
        }
    };

    if let Err(err) = stream_reader_chunks(
        &tx,
        &info,
        &wal_path,
        page_size,
        reader,
        chunk_start_override,
    )
    .await
    {
        send_replication_error(&tx, err).await;
    }
}

fn validate_start_generation(
    meta_dir: &str,
    start_pos: &WalGenerationPos,
) -> Result<(), StreamReplicationError> {
    if let Some(current_generation) = read_current_generation(meta_dir)
        && current_generation != start_pos.generation
    {
        return Err(StreamReplicationError::LineageMismatch(format!(
            "generation mismatch: requested {}, current {}",
            start_pos.generation.as_str(),
            current_generation.as_str()
        )));
    }
    Ok(())
}

fn retention_error(
    db_identity: &str,
    start_pos: &WalGenerationPos,
) -> Option<StreamReplicationError> {
    let retention = replica_progress::retention_state(db_identity)?;
    if retention.generation != start_pos.generation || start_pos.index >= retention.floor_index {
        return None;
    }
    Some(StreamReplicationError::WalNotRetained(format!(
        "requested lsn {}:{}:{} behind retention floor {} (tail {})",
        start_pos.generation.as_str(),
        start_pos.index,
        start_pos.offset,
        retention.floor_index,
        retention.tail_index,
    )))
}

fn build_stream_info(
    meta_dir: &str,
    start_pos: &WalGenerationPos,
) -> Result<(DatabaseInfo, String, u64), StreamReplicationError> {
    let wal_dir = shadow_wal_dir(meta_dir, start_pos.generation.as_str());
    let wal_path = shadow_wal_file(meta_dir, start_pos.generation.as_str(), start_pos.index);
    let wal_metadata = fs::metadata(&wal_path).map_err(|_| {
        StreamReplicationError::WalNotRetained(format!(
            "shadow wal not retained at {wal_path} (dir {wal_dir})",
        ))
    })?;

    if wal_metadata.len() < WAL_HEADER_SIZE {
        return Err(StreamReplicationError::WalNotRetained(format!(
            "shadow wal too short at {wal_path}",
        )));
    }

    let wal_header = WALHeader::read(&wal_path).map_err(|err| {
        StreamReplicationError::WalNotRetained(format!(
            "failed to read shadow wal header at {wal_path}: {err}",
        ))
    })?;

    let page_size = wal_header.page_size;
    Lsn::from(start_pos.clone()).validate(page_size)?;

    Ok((
        DatabaseInfo {
            meta_dir: meta_dir.to_string(),
            page_size,
        },
        wal_path,
        page_size,
    ))
}

fn open_stream_reader(
    start_pos: &WalGenerationPos,
    info: &DatabaseInfo,
    wal_path: &str,
) -> Result<(ShadowWalReader, Option<WalGenerationPos>), StreamReplicationError> {
    let reader = ShadowWalReader::new(start_pos.clone(), info).map_err(|err| {
        StreamReplicationError::WalNotRetained(format!(
            "shadow wal unavailable at {wal_path}: {err}",
        ))
    })?;

    if reader.left == 0 {
        return match try_open_next_reader(start_pos, info) {
            Ok(Some(next_reader)) => {
                let next_start = next_reader.position();
                Ok((next_reader, Some(next_start)))
            }
            Ok(None) => Ok((reader, None)),
            Err(err) => Err(StreamReplicationError::WalNotRetained(format!(
                "shadow wal unavailable after {wal_path}: {err}",
            ))),
        };
    }

    Ok((reader, None))
}

async fn stream_reader_chunks(
    tx: &mpsc::Sender<Result<StreamWalV2Response, Status>>,
    info: &DatabaseInfo,
    wal_path: &str,
    page_size: u64,
    mut reader: ShadowWalReader,
    mut chunk_start_override: Option<WalGenerationPos>,
) -> Result<(), StreamReplicationError> {
    if reader.left == 0 && chunk_start_override.is_none() {
        return Ok(());
    }

    let frame_size = (WAL_FRAME_HEADER_SIZE + page_size) as usize;
    let max_chunk_bytes = 256 * 1024usize;

    loop {
        let chunk_start = match chunk_start_override.take() {
            Some(start) => start,
            None => reader.position(),
        };

        let mut chunk = Vec::with_capacity(max_chunk_bytes);
        if reader.position().offset == 0 {
            let header = WALHeader::read_from(&mut reader).map_err(|err| {
                StreamReplicationError::WalNotRetained(format!(
                    "failed to read wal header at {wal_path}: {err}",
                ))
            })?;
            chunk.extend_from_slice(&header.data);
        }

        while reader.left > 0 {
            if !chunk.is_empty() && chunk.len() + frame_size > max_chunk_bytes {
                break;
            }
            let frame = WALFrame::read(&mut reader, page_size).map_err(|err| {
                StreamReplicationError::WalNotRetained(format!(
                    "failed to read wal frame at {wal_path}: {err}",
                ))
            })?;
            chunk.extend_from_slice(&frame.data);
        }

        if chunk.is_empty() {
            break;
        }

        let next_pos = reader.position();
        let response = StreamWalV2Response {
            payload: Some(StreamWalV2Payload::Chunk(WalChunkV2 {
                start_lsn: Some(lsn_token_from_pos(&chunk_start)),
                next_lsn: Some(lsn_token_from_pos(&next_pos)),
                wal_bytes: chunk,
            })),
        };
        if tx.send(Ok(response)).await.is_err() {
            return Ok(());
        }

        if reader.left == 0 {
            match try_open_next_reader(&next_pos, info) {
                Ok(Some(next_reader)) => reader = next_reader,
                Ok(None) => break,
                Err(err) => {
                    return Err(StreamReplicationError::WalNotRetained(format!(
                        "failed to open next shadow wal after {wal_path}: {err}",
                    )));
                }
            }
        }
    }

    Ok(())
}

fn read_current_generation(meta_dir: &str) -> Option<Generation> {
    let generation_file = generation_file_path(meta_dir);
    if !Path::new(&generation_file).exists() {
        return None;
    }
    let content = fs::read_to_string(generation_file).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return None;
    }
    Generation::try_create(trimmed).ok()
}

fn try_open_next_reader(
    pos: &WalGenerationPos,
    info: &DatabaseInfo,
) -> Result<Option<ShadowWalReader>, StreamReplicationError> {
    let next_pos = WalGenerationPos {
        generation: pos.generation.clone(),
        index: pos.index + 1,
        offset: 0,
    };

    match ShadowWalReader::new(next_pos, info) {
        Ok(reader) => {
            if reader.left == 0 {
                Ok(None)
            } else {
                Ok(Some(reader))
            }
        }
        Err(err) => {
            if err.code() == crate::error::Error::STORAGE_NOT_FOUND {
                Ok(None)
            } else {
                Err(StreamReplicationError::WalNotRetained(format!(
                    "failed to open next shadow wal: {err}",
                )))
            }
        }
    }
}

async fn snapshot_for_stream_with_retry(
    db_identity: &str,
    db_config: &DbConfig,
) -> Result<SnapshotStreamData, StreamReplicationError> {
    snapshot_with_retry(
        db_identity,
        SNAPSHOT_RETRY_COUNT,
        Duration::from_millis(SNAPSHOT_RETRY_DELAY_MS),
        || snapshot_for_stream(db_config.clone()),
    )
    .await
}

async fn snapshot_with_retry<F, Fut>(
    db_identity: &str,
    retry_count: usize,
    retry_delay: Duration,
    mut fetch_snapshot: F,
) -> Result<SnapshotStreamData, StreamReplicationError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = crate::error::Result<SnapshotStreamData>>,
{
    match fetch_snapshot().await {
        Ok(snapshot) => Ok(snapshot),
        Err(err) => {
            if err.code() != crate::error::Error::STORAGE_NOT_FOUND {
                return Err(snapshot_replication_error(db_identity, err));
            }

            let mut last_err = err;
            for _ in 0..retry_count {
                sleep(retry_delay).await;
                match fetch_snapshot().await {
                    Ok(snapshot) => return Ok(snapshot),
                    Err(err) => {
                        if err.code() != crate::error::Error::STORAGE_NOT_FOUND {
                            return Err(snapshot_replication_error(db_identity, err));
                        }
                        last_err = err;
                    }
                }
            }

            Err(snapshot_replication_error(db_identity, last_err))
        }
    }
}

fn validate_snapshot_request(
    requested_pos: Option<&WalGenerationPos>,
    snapshot: &SnapshotStreamData,
) -> Result<(), StreamReplicationError> {
    let Some(pos) = requested_pos else {
        return Ok(());
    };

    Lsn::from(pos.clone()).validate(snapshot.page_size)?;

    if pos.generation != snapshot.position.generation {
        return Err(StreamReplicationError::LineageMismatch(format!(
            "generation mismatch: requested {}, current {}",
            pos.generation.as_str(),
            snapshot.position.generation.as_str()
        )));
    }

    if pos.index != snapshot.position.index || pos.offset != snapshot.position.offset {
        return Err(StreamReplicationError::SnapshotBoundaryMismatch(format!(
            "snapshot boundary mismatch: requested {}:{}:{}, current {}:{}:{}",
            pos.generation.as_str(),
            pos.index,
            pos.offset,
            snapshot.position.generation.as_str(),
            snapshot.position.index,
            snapshot.position.offset,
        )));
    }

    Ok(())
}

async fn send_snapshot_stream(
    tx: &mpsc::Sender<Result<StreamSnapshotV2Response, Status>>,
    db_identity: &str,
    snapshot: &SnapshotStreamData,
) {
    let snapshot_size_bytes = snapshot.compressed_data.len() as u64;
    let snapshot_sha256 = Sha256::digest(&snapshot.compressed_data).to_vec();
    let meta = SnapshotMeta {
        db_identity: db_identity.to_string(),
        generation: snapshot.position.generation.as_str().to_string(),
        boundary_lsn: Some(lsn_token_from_pos(&snapshot.position)),
        page_size: snapshot.page_size as u32,
        snapshot_size_bytes,
        snapshot_sha256,
    };

    let response = StreamSnapshotV2Response {
        payload: Some(StreamSnapshotV2Payload::Meta(meta)),
    };
    if tx.send(Ok(response)).await.is_err() {
        return;
    }

    for chunk in snapshot.compressed_data.chunks(SNAPSHOT_CHUNK_SIZE) {
        let response = StreamSnapshotV2Response {
            payload: Some(StreamSnapshotV2Payload::Chunk(chunk.to_vec())),
        };
        if tx.send(Ok(response)).await.is_err() {
            return;
        }
    }
}

async fn send_replication_error(
    tx: &mpsc::Sender<Result<StreamWalV2Response, Status>>,
    error: StreamReplicationError,
) {
    if error.recovery_action() == ReplicaRecoveryAction::NeedsRestore {
        log::warn!("Primary: NeedsRestore {}: {}", error.code().as_str(), error);
    } else {
        log::debug!("Primary: stream error {}: {}", error.code().as_str(), error);
    }
    let response = StreamWalV2Response {
        payload: Some(StreamWalV2Payload::Error(
            stream_error_from_replication_error(error, 0),
        )),
    };
    let _ = tx.send(Ok(response)).await;
}

async fn send_stream_error(
    tx: &mpsc::Sender<Result<StreamWalV2Response, Status>>,
    code: StreamErrorCode,
    message: String,
) {
    let response = StreamWalV2Response {
        payload: Some(StreamWalV2Payload::Error(StreamError {
            code: code as i32,
            message,
            retry_after_ms: 0,
        })),
    };
    let _ = tx.send(Ok(response)).await;
}

fn snapshot_replication_error(
    db_identity: &str,
    err: crate::error::Error,
) -> StreamReplicationError {
    match err.code() {
        crate::error::Error::NO_GENERATION_ERROR
        | crate::error::Error::MISMATCH_WAL_HEADER_ERROR => {
            StreamReplicationError::LineageMismatch(format!(
                "generation unavailable for {db_identity}: {err}"
            ))
        }
        crate::error::Error::NO_WALSEGMENT_ERROR
        | crate::error::Error::INVALID_WAL_SEGMENT_ERROR
        | crate::error::Error::BAD_SHADOW_WAL_ERROR
        | crate::error::Error::SQLITE_WAL_ERROR
        | crate::error::Error::SQLITE_INVALID_WAL_HEADER_ERROR => {
            StreamReplicationError::WalNotRetained(format!(
                "wal not retained for {db_identity}: {err}"
            ))
        }
        _ => StreamReplicationError::SnapshotBoundaryMismatch(format!(
            "failed to create snapshot for {db_identity}: {err}"
        )),
    }
}

async fn send_snapshot_replication_error(
    tx: &mpsc::Sender<Result<StreamSnapshotV2Response, Status>>,
    error: StreamReplicationError,
) {
    if error.recovery_action() == ReplicaRecoveryAction::NeedsRestore {
        log::warn!("Primary: NeedsRestore {}: {}", error.code().as_str(), error);
    } else {
        log::debug!(
            "Primary: snapshot stream error {}: {}",
            error.code().as_str(),
            error
        );
    }
    let response = StreamSnapshotV2Response {
        payload: Some(StreamSnapshotV2Payload::Error(
            stream_error_from_replication_error(error, 0),
        )),
    };
    let _ = tx.send(Ok(response)).await;
}

async fn send_snapshot_stream_error(
    tx: &mpsc::Sender<Result<StreamSnapshotV2Response, Status>>,
    code: StreamErrorCode,
    message: String,
    retry_after_ms: u32,
) {
    let response = StreamSnapshotV2Response {
        payload: Some(StreamSnapshotV2Payload::Error(StreamError {
            code: code as i32,
            message,
            retry_after_ms,
        })),
    };
    let _ = tx.send(Ok(response)).await;
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{
        retention_error, snapshot_with_retry, validate_snapshot_request, validate_start_generation,
    };
    use crate::base::{Generation, generation_file_path};
    use crate::database::{SnapshotStreamData, WalGenerationPos};
    use crate::error::Error;
    use crate::sync::StreamReplicationError;
    use crate::sync::replica_progress;

    fn snapshot_stub() -> SnapshotStreamData {
        SnapshotStreamData {
            compressed_data: vec![],
            position: WalGenerationPos {
                generation: Generation::new(),
                index: 0,
                offset: 0,
            },
            page_size: 4096,
        }
    }

    #[test]
    fn validate_start_generation_detects_mismatch() {
        let dir = tempdir().unwrap();
        let meta_dir = dir.path().join(".data.db-replited");
        fs::create_dir_all(&meta_dir).unwrap();

        let current_generation = Generation::new();
        let generation_file = generation_file_path(meta_dir.to_str().unwrap());
        fs::write(&generation_file, current_generation.as_str()).unwrap();

        let requested = WalGenerationPos {
            generation: Generation::new(),
            index: 0,
            offset: 0,
        };

        let err = validate_start_generation(meta_dir.to_str().unwrap(), &requested).unwrap_err();
        assert!(matches!(err, StreamReplicationError::LineageMismatch(_)));
    }

    #[test]
    fn retention_error_when_position_is_behind_floor() {
        let db_identity = format!("test-db-{}", Generation::new().as_str());
        let generation = Generation::new();
        replica_progress::update_retention(&db_identity, &generation, 5, 9);

        let pos = WalGenerationPos {
            generation,
            index: 4,
            offset: 0,
        };

        let err = retention_error(&db_identity, &pos).unwrap();
        assert!(matches!(err, StreamReplicationError::WalNotRetained(_)));
    }

    #[test]
    fn validate_snapshot_request_accepts_matching_lsn() {
        let generation = Generation::new();
        let pos = WalGenerationPos {
            generation: generation.clone(),
            index: 2,
            offset: 32,
        };
        let snapshot = SnapshotStreamData {
            compressed_data: vec![],
            position: pos.clone(),
            page_size: 4096,
        };

        assert!(validate_snapshot_request(Some(&pos), &snapshot).is_ok());
    }

    #[test]
    fn validate_snapshot_request_rejects_boundary_mismatch() {
        let generation = Generation::new();
        let requested = WalGenerationPos {
            generation: generation.clone(),
            index: 2,
            offset: 32,
        };
        let snapshot = SnapshotStreamData {
            compressed_data: vec![],
            position: WalGenerationPos {
                generation,
                index: 3,
                offset: 32,
            },
            page_size: 4096,
        };

        let err = validate_snapshot_request(Some(&requested), &snapshot).unwrap_err();
        assert!(matches!(
            err,
            StreamReplicationError::SnapshotBoundaryMismatch(_)
        ));
    }

    #[tokio::test]
    async fn snapshot_with_retry_succeeds_after_retryable_errors() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();

        let result = snapshot_with_retry("db", 2, Duration::from_millis(0), move || {
            let attempts = attempts_for_closure.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err(Error::StorageNotFound("not found".to_string()))
                } else {
                    Ok(snapshot_stub())
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn snapshot_with_retry_stops_on_non_retryable_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_closure = attempts.clone();

        let result = snapshot_with_retry("db", 5, Duration::from_millis(0), move || {
            let attempts = attempts_for_closure.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(Error::SqliteError("fatal".to_string()))
            }
        })
        .await;

        let err = match result {
            Ok(_) => panic!("expected non-retryable snapshot fetch to fail"),
            Err(err) => err,
        };

        assert!(matches!(
            err,
            StreamReplicationError::SnapshotBoundaryMismatch(_)
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}

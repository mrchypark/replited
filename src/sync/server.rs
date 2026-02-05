use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::base::{Generation, generation_file_path, shadow_wal_dir, shadow_wal_file};
use crate::config::{Config, DbConfig};
use crate::database::{DatabaseInfo, WalGenerationPos, snapshot_for_stream};
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
use crate::sync::{
    Lsn, ReplicaRecoveryAction, ShadowWalReader, StreamReplicationError, StreamReplicationErrorCode,
};

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
    snapshot_semaphores: HashMap<String, Arc<Semaphore>>,
    _connections: Arc<Mutex<Vec<rusqlite::Connection>>>, // Keep connections open to pin WAL files
}

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

            let snapshot = match snapshot_for_stream(db_config.clone()).await {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    if err.code() == crate::error::Error::STORAGE_NOT_FOUND {
                        let mut last_err = err;
                        let mut snapshot = None;
                        for _ in 0..5 {
                            sleep(Duration::from_millis(200)).await;
                            match snapshot_for_stream(db_config.clone()).await {
                                Ok(value) => {
                                    snapshot = Some(value);
                                    break;
                                }
                                Err(err) => {
                                    last_err = err;
                                    if last_err.code() != crate::error::Error::STORAGE_NOT_FOUND {
                                        let error =
                                            snapshot_replication_error(&db_identity, last_err);
                                        send_snapshot_replication_error(&tx, error).await;
                                        return;
                                    }
                                }
                            }
                        }
                        if let Some(snapshot) = snapshot {
                            snapshot
                        } else {
                            let error = snapshot_replication_error(&db_identity, last_err);
                            send_snapshot_replication_error(&tx, error).await;
                            return;
                        }
                    } else {
                        let error = snapshot_replication_error(&db_identity, err);
                        send_snapshot_replication_error(&tx, error).await;
                        return;
                    }
                }
            };

            if let Some(pos) = requested_pos {
                if let Err(err) = Lsn::from(pos.clone()).validate(snapshot.page_size) {
                    send_snapshot_replication_error(&tx, err).await;
                    return;
                }

                if pos.generation != snapshot.position.generation {
                    send_snapshot_replication_error(
                        &tx,
                        StreamReplicationError::LineageMismatch(format!(
                            "generation mismatch: requested {}, current {}",
                            pos.generation.as_str(),
                            snapshot.position.generation.as_str()
                        )),
                    )
                    .await;
                    return;
                }

                if pos.index != snapshot.position.index || pos.offset != snapshot.position.offset {
                    send_snapshot_replication_error(
                        &tx,
                        StreamReplicationError::SnapshotBoundaryMismatch(format!(
                            "snapshot boundary mismatch: requested {}:{}:{}, current {}:{}:{}",
                            pos.generation.as_str(),
                            pos.index,
                            pos.offset,
                            snapshot.position.generation.as_str(),
                            snapshot.position.index,
                            snapshot.position.offset,
                        )),
                    )
                    .await;
                    return;
                }
            }

            let snapshot_size_bytes = snapshot.compressed_data.len() as u64;
            let snapshot_sha256 = sha256_digest(&snapshot.compressed_data).to_vec();
            let meta = SnapshotMeta {
                db_identity: db_identity.clone(),
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

            let chunk_size = 1024 * 1024usize;
            for chunk in snapshot.compressed_data.chunks(chunk_size) {
                let response = StreamSnapshotV2Response {
                    payload: Some(StreamSnapshotV2Payload::Chunk(chunk.to_vec())),
                };
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
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
                    error: Some(stream_error_from_replication_error(err)),
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

    if let Some(current_generation) = read_current_generation(&meta_dir) {
        if current_generation != start_pos.generation {
            send_replication_error(
                &tx,
                StreamReplicationError::LineageMismatch(format!(
                    "generation mismatch: requested {}, current {}",
                    start_pos.generation.as_str(),
                    current_generation.as_str()
                )),
            )
            .await;
            return;
        }
    }

    let _lease_guard =
        replica_progress::register_lease(&db_identity, &replica_id, start_pos.clone());

    if let Some(retention) = replica_progress::retention_state(&db_identity) {
        if retention.generation == start_pos.generation && start_pos.index < retention.floor_index {
            send_replication_error(
                &tx,
                StreamReplicationError::WalNotRetained(format!(
                    "requested lsn {}:{}:{} behind retention floor {} (tail {})",
                    start_pos.generation.as_str(),
                    start_pos.index,
                    start_pos.offset,
                    retention.floor_index,
                    retention.tail_index,
                )),
            )
            .await;
            return;
        }
    }

    let wal_dir = shadow_wal_dir(&meta_dir, start_pos.generation.as_str());
    let wal_path = shadow_wal_file(&meta_dir, start_pos.generation.as_str(), start_pos.index);
    let wal_metadata = match fs::metadata(&wal_path) {
        Ok(metadata) => metadata,
        Err(_) => {
            send_replication_error(
                &tx,
                StreamReplicationError::WalNotRetained(format!(
                    "shadow wal not retained at {wal_path} (dir {wal_dir})",
                )),
            )
            .await;
            return;
        }
    };

    if wal_metadata.len() < WAL_HEADER_SIZE {
        send_replication_error(
            &tx,
            StreamReplicationError::WalNotRetained(format!("shadow wal too short at {wal_path}",)),
        )
        .await;
        return;
    }

    let wal_header = match WALHeader::read(&wal_path) {
        Ok(header) => header,
        Err(err) => {
            send_replication_error(
                &tx,
                StreamReplicationError::WalNotRetained(format!(
                    "failed to read shadow wal header at {wal_path}: {err}",
                )),
            )
            .await;
            return;
        }
    };

    let page_size = wal_header.page_size;
    if let Err(err) = Lsn::from(start_pos.clone()).validate(page_size) {
        send_replication_error(&tx, err).await;
        return;
    }

    let info = DatabaseInfo {
        meta_dir: meta_dir.clone(),
        page_size,
    };

    let mut reader = match ShadowWalReader::new(start_pos.clone(), &info) {
        Ok(reader) => reader,
        Err(err) => {
            send_replication_error(
                &tx,
                StreamReplicationError::WalNotRetained(format!(
                    "shadow wal unavailable at {wal_path}: {err}",
                )),
            )
            .await;
            return;
        }
    };

    let mut chunk_start_override = None;
    if reader.left == 0 {
        match try_open_next_reader(&start_pos, &info) {
            Ok(Some(next_reader)) => {
                chunk_start_override = Some(next_reader.position());
                reader = next_reader;
            }
            Ok(None) => return,
            Err(err) => {
                send_replication_error(
                    &tx,
                    StreamReplicationError::WalNotRetained(format!(
                        "shadow wal unavailable after {wal_path}: {err}",
                    )),
                )
                .await;
                return;
            }
        }
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
            match WALHeader::read_from(&mut reader) {
                Ok(header) => chunk.extend_from_slice(&header.data),
                Err(err) => {
                    send_replication_error(
                        &tx,
                        StreamReplicationError::WalNotRetained(format!(
                            "failed to read wal header at {wal_path}: {err}",
                        )),
                    )
                    .await;
                    return;
                }
            }
        }

        while reader.left > 0 {
            if !chunk.is_empty() && chunk.len() + frame_size > max_chunk_bytes {
                break;
            }
            match WALFrame::read(&mut reader, page_size) {
                Ok(frame) => chunk.extend_from_slice(&frame.data),
                Err(err) => {
                    send_replication_error(
                        &tx,
                        StreamReplicationError::WalNotRetained(format!(
                            "failed to read wal frame at {wal_path}: {err}",
                        )),
                    )
                    .await;
                    return;
                }
            }
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
            return;
        }

        if reader.left == 0 {
            match try_open_next_reader(&next_pos, &info) {
                Ok(Some(next_reader)) => reader = next_reader,
                Ok(None) => break,
                Err(err) => {
                    send_replication_error(
                        &tx,
                        StreamReplicationError::WalNotRetained(format!(
                            "failed to open next shadow wal after {wal_path}: {err}",
                        )),
                    )
                    .await;
                    return;
                }
            }
        }
    }
}

fn lsn_token_to_pos(token: Option<LsnToken>) -> Result<WalGenerationPos, StreamReplicationError> {
    let token = token
        .ok_or_else(|| StreamReplicationError::InvalidLsn("start_lsn is required".to_string()))?;
    let generation_value = token.generation.trim();
    if generation_value.is_empty() {
        return Err(StreamReplicationError::InvalidLsn(
            "start_lsn.generation is required".to_string(),
        ));
    }
    let generation = Generation::try_create(generation_value).map_err(|err| {
        StreamReplicationError::InvalidLsn(format!(
            "invalid generation {}: {err}",
            token.generation
        ))
    })?;

    Ok(WalGenerationPos {
        generation,
        index: token.index,
        offset: token.offset,
    })
}

fn optional_lsn_token_to_pos(
    token: Option<LsnToken>,
) -> Result<Option<WalGenerationPos>, StreamReplicationError> {
    let token = match token {
        Some(token) => token,
        None => return Ok(None),
    };

    if token.generation.trim().is_empty() {
        if token.index == 0 && token.offset == 0 {
            return Ok(None);
        }
        return Err(StreamReplicationError::InvalidLsn(
            "start_lsn.generation is required".to_string(),
        ));
    }

    lsn_token_to_pos(Some(token)).map(Some)
}

fn lsn_token_from_pos(pos: &WalGenerationPos) -> LsnToken {
    LsnToken {
        generation: pos.generation.as_str().to_string(),
        index: pos.index,
        offset: pos.offset,
    }
}

fn meta_dir_for_db_path(db_path: &Path) -> Option<String> {
    let db_name = db_path.file_name()?.to_str()?;
    let dir_path = match db_path.parent() {
        Some(parent) if parent == Path::new("") => Path::new("."),
        Some(parent) => parent,
        None => Path::new("."),
    };
    Some(format!("{}/.{}-replited/", dir_path.to_str()?, db_name))
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

fn stream_error_from_replication_error(error: StreamReplicationError) -> StreamError {
    let code = stream_error_code_from_replication_code(error.code());
    StreamError {
        code: code as i32,
        message: error.to_string(),
        retry_after_ms: 0,
    }
}

fn stream_error_code_from_replication_code(code: StreamReplicationErrorCode) -> StreamErrorCode {
    match code {
        StreamReplicationErrorCode::LineageMismatch => StreamErrorCode::LineageMismatch,
        StreamReplicationErrorCode::WalNotRetained => StreamErrorCode::WalNotRetained,
        StreamReplicationErrorCode::SnapshotBoundaryMismatch => {
            StreamErrorCode::SnapshotBoundaryMismatch
        }
        StreamReplicationErrorCode::InvalidLsn => StreamErrorCode::InvalidLsn,
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
            stream_error_from_replication_error(error),
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
            stream_error_from_replication_error(error),
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

fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let mut offset = 0usize;
    while offset + 64 <= data.len() {
        let mut block = [0u8; 64];
        block.copy_from_slice(&data[offset..offset + 64]);
        sha256_compress(&mut state, &block);
        offset += 64;
    }

    let bit_len = (data.len() as u64) * 8;
    let mut block = [0u8; 64];
    let remaining = &data[offset..];
    block[..remaining.len()].copy_from_slice(remaining);
    block[remaining.len()] = 0x80;

    if remaining.len() >= 56 {
        sha256_compress(&mut state, &block);
        block = [0u8; 64];
    }

    block[56..].copy_from_slice(&bit_len.to_be_bytes());
    sha256_compress(&mut state, &block);

    let mut out = [0u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
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
    value.rotate_right(bits)
}

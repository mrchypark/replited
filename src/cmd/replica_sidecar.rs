use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use log::warn;
use tokio::time::sleep;

use crate::config::{Config, DbConfig, StorageParams};
use crate::database::WalGenerationPos;
use crate::error::{Error, Result};
use crate::sync::stream_client::StreamClient;
use crate::sync::{ReplicaRecoveryAction, StreamReplicationErrorCode};

mod local_state;
mod process_manager;
mod streaming;

use local_state::{load_or_create_replica_id, persist_last_applied_lsn, read_last_applied_lsn};
use process_manager::ProcessManager;
use streaming::{
    StreamedSnapshot, ack_lsn_or_warn, restore_streamed_snapshot, stream_snapshot_to_disk,
    stream_wal_and_apply,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplicaState {
    Bootstrapping,
    Restoring(PendingSnapshotRestore),
    CatchingUp,
    Ready,
    Streaming,
    RetryBackoff(RetryBackoffState),
    NeedsRestore,
    Fatal(String),
}

#[derive(Debug)]
enum ReplicaStreamError {
    Stream(StreamReplicationErrorCode, String),
    Transport(String),
    InvalidResponse(String),
    Io(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingSnapshotRestore {
    compressed_path: PathBuf,
    boundary_lsn: WalGenerationPos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryTarget {
    Bootstrapping,
    CatchingUp,
    Streaming,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetryBackoffState {
    target: RetryTarget,
    delay: Duration,
}

enum BootstrapOutcome {
    Downloaded(PendingSnapshotRestore),
    RetryBackoff(Duration),
    NeedsRestore,
}

const MAX_AUTO_RESTORES: u32 = 5;
const RESTORE_RESET_THRESHOLD: Duration = Duration::from_secs(60);
const STREAM_IDLE_BACKOFF_MS: u64 = 50;
const STREAM_RETRY_BACKOFF: Duration = Duration::from_secs(5);

struct BootstrapContext<'a> {
    stream_addr: &'a str,
    db_path: &'a str,
    path: &'a Path,
    remote_db_name: &'a str,
    replica_id: &'a str,
    session_id: &'a str,
    process_manager: &'a Option<ProcessManager>,
}

struct WalCycleContext<'a> {
    stream_addr: &'a str,
    db_path: &'a str,
    remote_db_name: &'a str,
    replica_id: &'a str,
    session_id: &'a str,
    checkpoint_frame_interval: u32,
    checkpoint_interval_ms: u64,
    process_manager: &'a Option<ProcessManager>,
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
                let db_name = db_config.db.clone();
                let result = Self::run_single_db(db_config, force_restore, pm).await;
                if let Err(err) = &result {
                    log::error!("ReplicaSidecar error for db {db_name}: {err}");
                }
                result
            });
            handles.push(handle);
        }

        for h in handles {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => {
                    return Err(Error::TokioError(format!(
                        "replica sidecar task join error: {err}",
                    )));
                }
            }
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

        let (mut last_applied_lsn, mut state, mut resume_pos) =
            initial_replica_state(db_path, force_restore, path)?;
        let mut consecutive_restore_count = 0;
        let mut last_restore_time = std::time::Instant::now();
        let mut invalid_lsn_retries = 0u8;

        let (bootstrap_ctx, wal_ctx) = build_loop_contexts(
            &stream_config.addr,
            db_path,
            path,
            &remote_db_name,
            &replica_id,
            &session_id,
            db_config.apply_checkpoint_frame_interval,
            db_config.apply_checkpoint_interval_ms,
            &process_manager,
        );

        loop {
            log::debug!("ReplicaSidecar::run_single_db loop state: {state:?}");
            match state.clone() {
                ReplicaState::Bootstrapping => {
                    handle_bootstrapping_state(
                        &bootstrap_ctx,
                        &mut force_restore,
                        &mut invalid_lsn_retries,
                        &mut state,
                    )
                    .await?;
                }
                ReplicaState::Restoring(pending_snapshot) => {
                    handle_restoring_state(
                        &bootstrap_ctx,
                        pending_snapshot,
                        &mut force_restore,
                        &mut state,
                        &mut last_applied_lsn,
                        &mut resume_pos,
                    )
                    .await?;
                }
                ReplicaState::CatchingUp => {
                    handle_catching_up_state(
                        &wal_ctx,
                        &mut state,
                        &mut last_applied_lsn,
                        &mut resume_pos,
                        &mut invalid_lsn_retries,
                    )
                    .await?;
                }
                ReplicaState::Ready => {
                    state = ReplicaState::Streaming;
                }
                ReplicaState::Streaming => {
                    handle_streaming_state(
                        &wal_ctx,
                        &mut state,
                        &mut last_applied_lsn,
                        &mut resume_pos,
                        &mut invalid_lsn_retries,
                    )
                    .await?;
                }
                ReplicaState::RetryBackoff(backoff) => {
                    handle_retry_backoff_state(backoff, &mut state).await;
                }
                ReplicaState::NeedsRestore => {
                    handle_needs_restore_state(
                        &db_config.db,
                        &mut consecutive_restore_count,
                        &mut last_restore_time,
                        &mut force_restore,
                        &mut state,
                    )
                    .await;
                }
                ReplicaState::Fatal(message) => {
                    return Err(Error::from_string_no_backtrace(message));
                }
            }
        }
    }
}

impl RetryBackoffState {
    fn new(target: RetryTarget, delay: Duration) -> Self {
        Self { target, delay }
    }
}

fn state_for_retry_target(target: RetryTarget) -> ReplicaState {
    match target {
        RetryTarget::Bootstrapping => ReplicaState::Bootstrapping,
        RetryTarget::CatchingUp => ReplicaState::CatchingUp,
        RetryTarget::Streaming => ReplicaState::Streaming,
    }
}

fn next_streaming_state(
    start_pos: &WalGenerationPos,
    applied_lsn: &WalGenerationPos,
) -> ReplicaState {
    let delay = if applied_lsn == start_pos {
        Duration::from_millis(STREAM_IDLE_BACKOFF_MS)
    } else {
        Duration::ZERO
    };
    ReplicaState::RetryBackoff(RetryBackoffState::new(RetryTarget::Streaming, delay))
}

fn initial_replica_state(
    db_path: &str,
    force_restore: bool,
    path: &Path,
) -> Result<(
    Option<WalGenerationPos>,
    ReplicaState,
    Option<WalGenerationPos>,
)> {
    let last_applied_lsn = read_last_applied_lsn(db_path)?;
    let state = if force_restore || !path.exists() || last_applied_lsn.is_none() {
        ReplicaState::Bootstrapping
    } else {
        ReplicaState::CatchingUp
    };
    let resume_pos = last_applied_lsn.clone();
    Ok((last_applied_lsn, state, resume_pos))
}

fn build_loop_contexts<'a>(
    stream_addr: &'a str,
    db_path: &'a str,
    path: &'a Path,
    remote_db_name: &'a str,
    replica_id: &'a str,
    session_id: &'a str,
    checkpoint_frame_interval: u32,
    checkpoint_interval_ms: u64,
    process_manager: &'a Option<ProcessManager>,
) -> (BootstrapContext<'a>, WalCycleContext<'a>) {
    let bootstrap_ctx = BootstrapContext {
        stream_addr,
        db_path,
        path,
        remote_db_name,
        replica_id,
        session_id,
        process_manager,
    };
    let wal_ctx = WalCycleContext {
        stream_addr,
        db_path,
        remote_db_name,
        replica_id,
        session_id,
        checkpoint_frame_interval,
        checkpoint_interval_ms,
        process_manager,
    };
    (bootstrap_ctx, wal_ctx)
}

async fn handle_bootstrapping_state(
    ctx: &BootstrapContext<'_>,
    force_restore: &mut bool,
    invalid_lsn_retries: &mut u8,
    state: &mut ReplicaState,
) -> Result<()> {
    if let Some(pm) = ctx.process_manager {
        pm.add_blocker().await;
    }

    let outcome = run_bootstrap_cycle(
        ctx.stream_addr,
        ctx.db_path,
        ctx.path,
        *force_restore,
        ctx.remote_db_name,
        ctx.replica_id,
        ctx.session_id,
        invalid_lsn_retries,
    )
    .await?;

    match outcome {
        BootstrapOutcome::Downloaded(snapshot) => {
            *state = ReplicaState::Restoring(snapshot);
        }
        BootstrapOutcome::RetryBackoff(delay) => {
            if let Some(pm) = ctx.process_manager {
                pm.remove_blocker().await;
            }
            *state = ReplicaState::RetryBackoff(RetryBackoffState::new(
                RetryTarget::Bootstrapping,
                delay,
            ));
        }
        BootstrapOutcome::NeedsRestore => {
            if let Some(pm) = ctx.process_manager {
                pm.remove_blocker().await;
            }
            *state = ReplicaState::NeedsRestore;
        }
    }

    Ok(())
}

async fn handle_restoring_state(
    ctx: &BootstrapContext<'_>,
    pending_snapshot: PendingSnapshotRestore,
    force_restore: &mut bool,
    state: &mut ReplicaState,
    last_applied_lsn: &mut Option<WalGenerationPos>,
    resume_pos: &mut Option<WalGenerationPos>,
) -> Result<()> {
    let streamed_snapshot = StreamedSnapshot {
        compressed_path: pending_snapshot.compressed_path,
        boundary_lsn: pending_snapshot.boundary_lsn,
    };
    let boundary_lsn = match restore_streamed_snapshot(ctx.db_path, &streamed_snapshot).await {
        Ok(boundary_lsn) => boundary_lsn,
        Err(err) => {
            if let Some(pm) = ctx.process_manager {
                pm.remove_blocker().await;
            }
            *state = ReplicaState::Fatal(format!(
                "restore snapshot failed for {}: {err:?}",
                ctx.db_path
            ));
            return Ok(());
        }
    };

    if let Some(pm) = ctx.process_manager {
        pm.remove_blocker().await;
    }

    match StreamClient::connect(ctx.stream_addr.to_string()).await {
        Ok(client) => {
            if let Err(err) = ack_lsn_or_warn(
                &client,
                ctx.remote_db_name,
                ctx.replica_id,
                ctx.session_id,
                &boundary_lsn,
            )
            .await
            {
                match err {
                    ReplicaStreamError::Stream(code, message) => {
                        warn!("ACK stream error for {}: {code:?} {message}", ctx.db_path);
                        if code.recovery_action() == ReplicaRecoveryAction::NeedsRestore {
                            *state = ReplicaState::NeedsRestore;
                            return Ok(());
                        }
                    }
                    _ => warn!("ACK failed for {}: {err:?}", ctx.db_path),
                }
            }
        }
        Err(err) => warn!("ACK reconnect failed for {}: {err}", ctx.db_path),
    }

    finalize_restored_boundary(
        ctx.db_path,
        &boundary_lsn,
        force_restore,
        state,
        last_applied_lsn,
        resume_pos,
    )?;

    Ok(())
}

async fn handle_catching_up_state(
    ctx: &WalCycleContext<'_>,
    state: &mut ReplicaState,
    last_applied_lsn: &mut Option<WalGenerationPos>,
    resume_pos: &mut Option<WalGenerationPos>,
    invalid_lsn_retries: &mut u8,
) -> Result<()> {
    let start_pos = match resume_pos.clone().or(last_applied_lsn.clone()) {
        Some(pos) => pos,
        None => {
            *state = ReplicaState::Bootstrapping;
            return Ok(());
        }
    };

    match run_wal_cycle(
        ctx.stream_addr,
        ctx.db_path,
        ctx.remote_db_name,
        ctx.replica_id,
        ctx.session_id,
        start_pos.clone(),
        ctx.checkpoint_frame_interval,
        ctx.checkpoint_interval_ms,
        ctx.process_manager.clone(),
    )
    .await
    {
        Ok(applied_lsn) => {
            persist_last_applied_lsn(ctx.db_path, &applied_lsn)?;
            *last_applied_lsn = Some(applied_lsn.clone());
            *resume_pos = Some(applied_lsn);
            *state = ReplicaState::Ready;
            *invalid_lsn_retries = 0;
        }
        Err(err) => {
            handle_wal_cycle_error(
                err,
                ctx.db_path,
                state,
                invalid_lsn_retries,
                Some(resume_pos),
                RetryTarget::CatchingUp,
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_streaming_state(
    ctx: &WalCycleContext<'_>,
    state: &mut ReplicaState,
    last_applied_lsn: &mut Option<WalGenerationPos>,
    resume_pos: &mut Option<WalGenerationPos>,
    invalid_lsn_retries: &mut u8,
) -> Result<()> {
    let start_pos = match read_last_applied_lsn(ctx.db_path)? {
        Some(pos) => pos,
        None => {
            *state = ReplicaState::Bootstrapping;
            return Ok(());
        }
    };

    match run_wal_cycle(
        ctx.stream_addr,
        ctx.db_path,
        ctx.remote_db_name,
        ctx.replica_id,
        ctx.session_id,
        start_pos.clone(),
        ctx.checkpoint_frame_interval,
        ctx.checkpoint_interval_ms,
        ctx.process_manager.clone(),
    )
    .await
    {
        Ok(applied_lsn) => {
            persist_last_applied_lsn(ctx.db_path, &applied_lsn)?;
            *last_applied_lsn = Some(applied_lsn.clone());
            *resume_pos = Some(applied_lsn.clone());
            *invalid_lsn_retries = 0;
            *state = next_streaming_state(&start_pos, &applied_lsn);
        }
        Err(err) => {
            handle_wal_cycle_error(
                err,
                ctx.db_path,
                state,
                invalid_lsn_retries,
                None,
                RetryTarget::Streaming,
            )
            .await?;
        }
    }

    Ok(())
}

async fn handle_needs_restore_state(
    db_identity: &str,
    consecutive_restore_count: &mut u32,
    last_restore_time: &mut std::time::Instant,
    force_restore: &mut bool,
    state: &mut ReplicaState,
) {
    let now = std::time::Instant::now();
    if now.duration_since(*last_restore_time) > RESTORE_RESET_THRESHOLD {
        *consecutive_restore_count = 0;
    }
    if *consecutive_restore_count >= MAX_AUTO_RESTORES {
        let message = format!("too many automatic restores for {db_identity}");
        warn!("{message}");
        *state = ReplicaState::Fatal(message);
        return;
    }
    *consecutive_restore_count += 1;
    *last_restore_time = now;
    *force_restore = true;
    *state = ReplicaState::Bootstrapping;
}

async fn handle_retry_backoff_state(backoff: RetryBackoffState, state: &mut ReplicaState) {
    if !backoff.delay.is_zero() {
        sleep(backoff.delay).await;
    }
    *state = state_for_retry_target(backoff.target);
}

async fn run_wal_cycle(
    stream_addr: &str,
    db_path: &str,
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
    start_pos: WalGenerationPos,
    checkpoint_frame_interval: u32,
    checkpoint_interval_ms: u64,
    process_manager: Option<ProcessManager>,
) -> Result<WalGenerationPos, ReplicaStreamError> {
    let client = StreamClient::connect(stream_addr.to_string())
        .await
        .map_err(|err| ReplicaStreamError::Transport(err.to_string()))?;

    stream_wal_and_apply(
        &client,
        db_path,
        db_identity,
        replica_id,
        session_id,
        start_pos,
        checkpoint_frame_interval,
        checkpoint_interval_ms,
        process_manager,
    )
    .await
}

async fn run_bootstrap_cycle(
    stream_addr: &str,
    db_path: &str,
    path: &Path,
    force_restore: bool,
    remote_db_name: &str,
    replica_id: &str,
    session_id: &str,
    invalid_lsn_retries: &mut u8,
) -> Result<BootstrapOutcome> {
    if force_restore && path.exists() {
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(format!("{db_path}-wal"));
        let _ = fs::remove_file(format!("{db_path}-shm"));
    }

    let client = match StreamClient::connect(stream_addr.to_string()).await {
        Ok(client) => client,
        Err(err) => {
            warn!("Failed to connect to Primary: {err}. Retrying in 5s...");
            return Ok(BootstrapOutcome::RetryBackoff(STREAM_RETRY_BACKOFF));
        }
    };

    match stream_snapshot_to_disk(&client, db_path, remote_db_name, replica_id, session_id).await {
        Ok(snapshot) => {
            *invalid_lsn_retries = 0;
            Ok(BootstrapOutcome::Downloaded(PendingSnapshotRestore {
                compressed_path: snapshot.compressed_path,
                boundary_lsn: snapshot.boundary_lsn,
            }))
        }
        Err(err) => match err {
            ReplicaStreamError::Stream(code, message) => {
                warn!("Snapshot stream error for {db_path}: {code:?} {message}");
                match code.recovery_action() {
                    ReplicaRecoveryAction::NeedsRestore => Ok(BootstrapOutcome::NeedsRestore),
                    ReplicaRecoveryAction::Retry => {
                        if *invalid_lsn_retries == 0 {
                            *invalid_lsn_retries += 1;
                            Ok(BootstrapOutcome::RetryBackoff(STREAM_RETRY_BACKOFF))
                        } else {
                            Ok(BootstrapOutcome::NeedsRestore)
                        }
                    }
                }
            }
            ReplicaStreamError::Transport(message)
            | ReplicaStreamError::InvalidResponse(message)
            | ReplicaStreamError::Io(message) => {
                warn!("Snapshot transport error for {db_path}: {message}");
                Ok(BootstrapOutcome::RetryBackoff(STREAM_RETRY_BACKOFF))
            }
        },
    }
}

async fn handle_wal_cycle_error(
    err: ReplicaStreamError,
    db_path: &str,
    state: &mut ReplicaState,
    invalid_lsn_retries: &mut u8,
    resume_pos: Option<&mut Option<WalGenerationPos>>,
    retry_target: RetryTarget,
) -> Result<()> {
    match err {
        ReplicaStreamError::Stream(code, message) => {
            warn!("WAL stream error for {db_path}: {code:?} {message}");
            match code.recovery_action() {
                ReplicaRecoveryAction::NeedsRestore => {
                    *state = ReplicaState::NeedsRestore;
                }
                ReplicaRecoveryAction::Retry => {
                    if *invalid_lsn_retries == 0 {
                        *invalid_lsn_retries += 1;
                        if let Some(resume) = resume_pos {
                            *resume = read_last_applied_lsn(db_path)?;
                        }
                        *state = ReplicaState::RetryBackoff(RetryBackoffState::new(
                            retry_target,
                            STREAM_RETRY_BACKOFF,
                        ));
                    } else {
                        *state = ReplicaState::NeedsRestore;
                    }
                }
            }
        }
        ReplicaStreamError::Transport(message)
        | ReplicaStreamError::InvalidResponse(message)
        | ReplicaStreamError::Io(message) => {
            warn!("WAL stream error for {db_path}: {message}");
            *state = ReplicaState::RetryBackoff(RetryBackoffState::new(
                retry_target,
                STREAM_RETRY_BACKOFF,
            ));
        }
    }
    Ok(())
}

fn finalize_restored_boundary(
    db_path: &str,
    boundary_lsn: &WalGenerationPos,
    force_restore: &mut bool,
    state: &mut ReplicaState,
    last_applied_lsn: &mut Option<WalGenerationPos>,
    resume_pos: &mut Option<WalGenerationPos>,
) -> Result<()> {
    persist_last_applied_lsn(db_path, boundary_lsn)?;
    *last_applied_lsn = Some(boundary_lsn.clone());
    *resume_pos = Some(boundary_lsn.clone());
    *state = ReplicaState::CatchingUp;
    *force_restore = false;
    Ok(())
}

#[async_trait::async_trait]
impl super::command::Command for ReplicaSidecar {
    async fn run(&mut self) -> Result<()> {
        self.run().await
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;

    use super::{
        MAX_AUTO_RESTORES, PendingSnapshotRestore, ReplicaSidecar, ReplicaState, RetryBackoffState,
        RetryTarget, finalize_restored_boundary, handle_needs_restore_state, next_streaming_state,
    };
    use crate::base::Generation;
    use crate::cmd::replica_sidecar::local_state::read_last_applied_lsn;
    use crate::database::WalGenerationPos;

    #[tokio::test]
    async fn run_propagates_fatal_db_errors() {
        let dir = tempdir().expect("tempdir");
        let config_path = dir.path().join("replica.toml");
        let log_dir = dir.path().join("logs");
        fs::create_dir_all(&log_dir).expect("log dir");

        fs::write(
            &config_path,
            format!(
                r#"
[log]
level = "Info"
dir = "{}"

[[database]]
db = "{}"
min_checkpoint_page_number = 100
max_checkpoint_page_number = 1000
truncate_page_number = 50000
checkpoint_interval_secs = 30
monitor_interval_ms = 50
apply_checkpoint_frame_interval = 20
apply_checkpoint_interval_ms = 200
wal_retention_count = 5

[[database.replicate]]
name = "backup-only"
[database.replicate.params]
type = "fs"
root = "{}"
"#,
                log_dir.display(),
                dir.path().join("replica.db").display(),
                dir.path().join("backup").display()
            ),
        )
        .expect("write config");

        let mut sidecar =
            ReplicaSidecar::try_create(config_path.to_str().expect("path"), false, None)
                .expect("create sidecar");

        let err = sidecar
            .run()
            .await
            .expect_err("missing stream config should fail the sidecar");
        assert_eq!(err.code(), crate::error::Error::INVALID_CONFIG);
    }

    #[test]
    fn finalize_restored_boundary_persists_lsn_before_catch_up() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("replica.db");
        let boundary_lsn = WalGenerationPos {
            generation: Generation::new(),
            index: 7,
            offset: 256,
        };
        let mut force_restore = true;
        let mut state = ReplicaState::Restoring(PendingSnapshotRestore {
            compressed_path: dir.path().join("snapshot.zst"),
            boundary_lsn: boundary_lsn.clone(),
        });
        let mut last_applied_lsn = None;
        let mut resume_pos = None;

        finalize_restored_boundary(
            db_path.to_str().unwrap(),
            &boundary_lsn,
            &mut force_restore,
            &mut state,
            &mut last_applied_lsn,
            &mut resume_pos,
        )
        .expect("persist boundary");

        assert_eq!(state, ReplicaState::CatchingUp);
        assert_eq!(last_applied_lsn, Some(boundary_lsn.clone()));
        assert_eq!(resume_pos, Some(boundary_lsn.clone()));
        assert!(!force_restore);
        assert_eq!(
            read_last_applied_lsn(db_path.to_str().unwrap()).expect("read lsn"),
            Some(boundary_lsn),
        );
    }

    #[test]
    fn next_streaming_state_backoffs_when_eof_returns_no_progress() {
        let lsn = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 0,
        };

        assert_eq!(
            next_streaming_state(&lsn, &lsn),
            ReplicaState::RetryBackoff(RetryBackoffState::new(
                RetryTarget::Streaming,
                Duration::from_millis(super::STREAM_IDLE_BACKOFF_MS),
            )),
        );
    }

    #[test]
    fn next_streaming_state_retries_immediately_after_progressful_eof() {
        let start_lsn = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 0,
        };
        let applied_lsn = WalGenerationPos {
            generation: start_lsn.generation.clone(),
            index: 1,
            offset: 128,
        };

        assert_eq!(
            next_streaming_state(&start_lsn, &applied_lsn),
            ReplicaState::RetryBackoff(RetryBackoffState::new(
                RetryTarget::Streaming,
                Duration::ZERO,
            )),
        );
    }

    #[tokio::test]
    async fn handle_needs_restore_state_enters_fatal_after_limit() {
        let mut consecutive_restore_count = MAX_AUTO_RESTORES;
        let mut last_restore_time = Instant::now();
        let mut force_restore = false;
        let mut state = ReplicaState::NeedsRestore;

        handle_needs_restore_state(
            "db",
            &mut consecutive_restore_count,
            &mut last_restore_time,
            &mut force_restore,
            &mut state,
        )
        .await;

        assert!(matches!(state, ReplicaState::Fatal(_)));
        assert!(!force_restore);
    }
}

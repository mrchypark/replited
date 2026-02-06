use std::fs;
use std::path::Path;
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
use streaming::{ack_lsn_or_warn, stream_snapshot_and_restore, stream_wal_and_apply};

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

enum BootstrapOutcome {
    Continue,
    Bootstrapped(WalGenerationPos),
    NeedsRestore,
}

const MAX_AUTO_RESTORES: u32 = 5;
const RESTORE_RESET_THRESHOLD: Duration = Duration::from_secs(60);
const STREAM_IDLE_BACKOFF_MS: u64 = 50;

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
            match state {
                ReplicaState::Bootstrapping => {
                    handle_bootstrapping_state(
                        &bootstrap_ctx,
                        &mut force_restore,
                        &mut invalid_lsn_retries,
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
            }
        }
    }
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
    last_applied_lsn: &mut Option<WalGenerationPos>,
    resume_pos: &mut Option<WalGenerationPos>,
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

    if let Some(pm) = ctx.process_manager {
        pm.remove_blocker().await;
    }

    match outcome {
        BootstrapOutcome::Continue => {}
        BootstrapOutcome::NeedsRestore => {
            *state = ReplicaState::NeedsRestore;
        }
        BootstrapOutcome::Bootstrapped(boundary_lsn) => {
            persist_last_applied_lsn(ctx.db_path, &boundary_lsn)?;
            *last_applied_lsn = Some(boundary_lsn.clone());
            *resume_pos = Some(boundary_lsn);
            *state = ReplicaState::CatchingUp;
            *force_restore = false;
        }
    }

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
            *state = ReplicaState::Streaming;
            *invalid_lsn_retries = 0;
        }
        Err(err) => {
            handle_wal_cycle_error(
                err,
                ctx.db_path,
                state,
                invalid_lsn_retries,
                Some(resume_pos),
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
            let no_progress = applied_lsn == start_pos;
            persist_last_applied_lsn(ctx.db_path, &applied_lsn)?;
            *last_applied_lsn = Some(applied_lsn.clone());
            *resume_pos = Some(applied_lsn);
            *invalid_lsn_retries = 0;
            if no_progress {
                sleep(Duration::from_millis(STREAM_IDLE_BACKOFF_MS)).await;
            }
        }
        Err(err) => {
            handle_wal_cycle_error(err, ctx.db_path, state, invalid_lsn_retries, None).await?;
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
        warn!("Too many automatic restores for {db_identity}; backing off");
        sleep(Duration::from_secs(10)).await;
        return;
    }
    *consecutive_restore_count += 1;
    *last_restore_time = now;
    *force_restore = true;
    *state = ReplicaState::Bootstrapping;
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
            sleep(Duration::from_secs(5)).await;
            return Ok(BootstrapOutcome::Continue);
        }
    };

    match stream_snapshot_and_restore(&client, db_path, remote_db_name, replica_id, session_id)
        .await
    {
        Ok(boundary_lsn) => {
            if let Err(err) = ack_lsn_or_warn(
                &client,
                remote_db_name,
                replica_id,
                session_id,
                &boundary_lsn,
            )
            .await
            {
                match err {
                    ReplicaStreamError::Stream(code, message) => {
                        warn!("ACK stream error for {db_path}: {code:?} {message}");
                        if code.recovery_action() == ReplicaRecoveryAction::NeedsRestore {
                            return Ok(BootstrapOutcome::NeedsRestore);
                        }
                    }
                    _ => {
                        warn!("ACK failed for {db_path}: {err:?}");
                    }
                }
            }

            *invalid_lsn_retries = 0;
            Ok(BootstrapOutcome::Bootstrapped(boundary_lsn))
        }
        Err(err) => match err {
            ReplicaStreamError::Stream(code, message) => {
                warn!("Snapshot stream error for {db_path}: {code:?} {message}");
                match code.recovery_action() {
                    ReplicaRecoveryAction::NeedsRestore => Ok(BootstrapOutcome::NeedsRestore),
                    ReplicaRecoveryAction::Retry => {
                        if *invalid_lsn_retries == 0 {
                            *invalid_lsn_retries += 1;
                            Ok(BootstrapOutcome::Continue)
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
                sleep(Duration::from_secs(5)).await;
                Ok(BootstrapOutcome::Continue)
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
            sleep(Duration::from_secs(5)).await;
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl super::command::Command for ReplicaSidecar {
    async fn run(&mut self) -> Result<()> {
        self.run().await
    }
}

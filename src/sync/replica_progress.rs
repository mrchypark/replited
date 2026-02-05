use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::base::Generation;
use crate::database::WalGenerationPos;

pub const REPLICA_ACTIVE_TTL: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Default)]
pub struct ReplicaProgressSnapshot {
    pub entries: Vec<ReplicaProgressEntry>,
    pub min_acked_index: Option<u64>,
    pub min_lease_index: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ReplicaProgressEntry {
    pub replica_id: String,
    pub acked: Option<WalGenerationPos>,
    pub ack_age_secs: Option<u64>,
    pub lease: Option<WalGenerationPos>,
    pub lease_age_secs: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct RetentionState {
    pub generation: Generation,
    pub floor_index: u64,
    pub tail_index: u64,
}

#[derive(Debug, Clone, Default)]
struct ReplicaProgress {
    last_acked: WalGenerationPos,
    last_ack_seen: Option<Instant>,
    last_session_id: Option<String>,
    lease_lsn: Option<WalGenerationPos>,
    lease_seen: Option<Instant>,
}

#[derive(Debug, Default)]
struct ReplicaProgressState {
    by_db: HashMap<String, HashMap<String, ReplicaProgress>>,
    retention: HashMap<String, RetentionState>,
}

static REPLICA_PROGRESS: LazyLock<RwLock<ReplicaProgressState>> =
    LazyLock::new(|| RwLock::new(ReplicaProgressState::default()));

#[derive(Debug)]
pub struct AckUpdateError {
    pub previous: WalGenerationPos,
    pub attempted: WalGenerationPos,
}

pub struct LeaseGuard {
    db_identity: String,
    replica_id: String,
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        clear_lease(&self.db_identity, &self.replica_id);
    }
}

pub fn update_ack(
    db_identity: &str,
    replica_id: &str,
    session_id: &str,
    pos: WalGenerationPos,
) -> Result<(), AckUpdateError> {
    let mut state = REPLICA_PROGRESS.write();
    let db_entry = state.by_db.entry(db_identity.to_string()).or_default();
    let progress = db_entry
        .entry(replica_id.to_string())
        .or_insert_with(ReplicaProgress::default);

    if !progress.last_acked.is_empty() && progress.last_acked.generation == pos.generation {
        if pos.index < progress.last_acked.index
            || (pos.index == progress.last_acked.index && pos.offset < progress.last_acked.offset)
        {
            return Err(AckUpdateError {
                previous: progress.last_acked.clone(),
                attempted: pos,
            });
        }
    }

    progress.last_acked = pos;
    progress.last_ack_seen = Some(Instant::now());
    progress.last_session_id = Some(session_id.to_string());
    Ok(())
}

pub fn register_lease(
    db_identity: &str,
    replica_id: &str,
    pos: WalGenerationPos,
) -> Option<LeaseGuard> {
    if replica_id.trim().is_empty() {
        return None;
    }

    update_lease(db_identity, replica_id, pos);
    Some(LeaseGuard {
        db_identity: db_identity.to_string(),
        replica_id: replica_id.to_string(),
    })
}

fn update_lease(db_identity: &str, replica_id: &str, pos: WalGenerationPos) {
    let mut state = REPLICA_PROGRESS.write();
    let db_entry = state.by_db.entry(db_identity.to_string()).or_default();
    let progress = db_entry
        .entry(replica_id.to_string())
        .or_insert_with(ReplicaProgress::default);

    progress.lease_lsn = Some(pos);
    progress.lease_seen = Some(Instant::now());
}

pub fn clear_lease(db_identity: &str, replica_id: &str) {
    let mut state = REPLICA_PROGRESS.write();
    if let Some(db_entry) = state.by_db.get_mut(db_identity) {
        if let Some(progress) = db_entry.get_mut(replica_id) {
            progress.lease_lsn = None;
            progress.lease_seen = None;
        }
    }
}

pub fn active_snapshot(
    db_identity: &str,
    generation: &Generation,
    ttl: Duration,
) -> ReplicaProgressSnapshot {
    let now = Instant::now();
    let state = REPLICA_PROGRESS.read();
    let Some(db_entry) = state.by_db.get(db_identity) else {
        return ReplicaProgressSnapshot::default();
    };

    let mut snapshot = ReplicaProgressSnapshot::default();
    for (replica_id, progress) in db_entry.iter() {
        let acked_active = progress
            .last_ack_seen
            .map_or(false, |seen| now.duration_since(seen) <= ttl)
            && !progress.last_acked.is_empty()
            && progress.last_acked.generation == *generation;
        let lease_active = progress
            .lease_seen
            .map_or(false, |seen| now.duration_since(seen) <= ttl)
            && progress
                .lease_lsn
                .as_ref()
                .map_or(false, |lsn| lsn.generation == *generation);

        if !(acked_active || lease_active) {
            continue;
        }

        let mut entry = ReplicaProgressEntry {
            replica_id: replica_id.clone(),
            acked: None,
            ack_age_secs: None,
            lease: None,
            lease_age_secs: None,
        };

        if acked_active {
            entry.acked = Some(progress.last_acked.clone());
            entry.ack_age_secs = progress
                .last_ack_seen
                .map(|seen| now.duration_since(seen).as_secs());
            snapshot.min_acked_index = Some(match snapshot.min_acked_index {
                Some(current) => current.min(progress.last_acked.index),
                None => progress.last_acked.index,
            });
        }

        if lease_active {
            if let Some(lease) = progress.lease_lsn.clone() {
                entry.lease = Some(lease.clone());
                entry.lease_age_secs = progress
                    .lease_seen
                    .map(|seen| now.duration_since(seen).as_secs());
                snapshot.min_lease_index = Some(match snapshot.min_lease_index {
                    Some(current) => current.min(lease.index),
                    None => lease.index,
                });
            }
        }

        snapshot.entries.push(entry);
    }

    snapshot
}

pub fn update_retention(
    db_identity: &str,
    generation: &Generation,
    floor_index: u64,
    tail_index: u64,
) {
    let mut state = REPLICA_PROGRESS.write();
    state.retention.insert(
        db_identity.to_string(),
        RetentionState {
            generation: generation.clone(),
            floor_index,
            tail_index,
        },
    );
}

pub fn retention_state(db_identity: &str) -> Option<RetentionState> {
    let state = REPLICA_PROGRESS.read();
    state.retention.get(db_identity).cloned()
}

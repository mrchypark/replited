use crate::base::Generation;
use crate::database::WalGenerationPos;
use crate::sqlite::align_frame;

/// LSN is an authoritative WAL stream position.
///
/// LSN = (generation, index, offset)
/// - generation: replication generation name emitted by the primary.
/// - index: WAL segment index within the generation.
/// - offset: byte offset within the WAL segment. Offset must be frame-aligned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lsn {
    pub generation: Generation,
    pub index: u64,
    pub offset: u64,
}

impl Lsn {
    pub fn is_empty(&self) -> bool {
        self.generation.is_empty() && self.index == 0 && self.offset == 0
    }

    pub fn validate(&self, page_size: u64) -> Result<(), StreamReplicationError> {
        if self.offset != align_frame(page_size, self.offset) {
            return Err(StreamReplicationError::InvalidLsn(format!(
                "offset {} must be aligned to page size {}",
                self.offset, page_size
            )));
        }

        Ok(())
    }

    pub fn aligned_offset(page_size: u64, offset: u64) -> u64 {
        align_frame(page_size, offset)
    }
}

impl From<WalGenerationPos> for Lsn {
    fn from(pos: WalGenerationPos) -> Self {
        Self {
            generation: pos.generation,
            index: pos.index,
            offset: pos.offset,
        }
    }
}

impl From<Lsn> for WalGenerationPos {
    fn from(lsn: Lsn) -> Self {
        Self {
            generation: lsn.generation,
            index: lsn.index,
            offset: lsn.offset,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRecoveryAction {
    Retry,
    NeedsRestore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamReplicationErrorCode {
    LineageMismatch,
    WalNotRetained,
    SnapshotBoundaryMismatch,
    InvalidLsn,
}

impl StreamReplicationErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            StreamReplicationErrorCode::LineageMismatch => "LINEAGE_MISMATCH",
            StreamReplicationErrorCode::WalNotRetained => "WAL_NOT_RETAINED",
            StreamReplicationErrorCode::SnapshotBoundaryMismatch => "SNAPSHOT_BOUNDARY_MISMATCH",
            StreamReplicationErrorCode::InvalidLsn => "INVALID_LSN",
        }
    }

    pub fn recovery_action(self) -> ReplicaRecoveryAction {
        match self {
            StreamReplicationErrorCode::LineageMismatch => ReplicaRecoveryAction::NeedsRestore,
            StreamReplicationErrorCode::WalNotRetained => ReplicaRecoveryAction::NeedsRestore,
            StreamReplicationErrorCode::SnapshotBoundaryMismatch => {
                ReplicaRecoveryAction::NeedsRestore
            }
            StreamReplicationErrorCode::InvalidLsn => ReplicaRecoveryAction::Retry,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamReplicationError {
    LineageMismatch(String),
    WalNotRetained(String),
    SnapshotBoundaryMismatch(String),
    InvalidLsn(String),
}

impl StreamReplicationError {
    pub fn code(&self) -> StreamReplicationErrorCode {
        match self {
            StreamReplicationError::LineageMismatch(_) => {
                StreamReplicationErrorCode::LineageMismatch
            }
            StreamReplicationError::WalNotRetained(_) => StreamReplicationErrorCode::WalNotRetained,
            StreamReplicationError::SnapshotBoundaryMismatch(_) => {
                StreamReplicationErrorCode::SnapshotBoundaryMismatch
            }
            StreamReplicationError::InvalidLsn(_) => StreamReplicationErrorCode::InvalidLsn,
        }
    }

    pub fn recovery_action(&self) -> ReplicaRecoveryAction {
        self.code().recovery_action()
    }
}

impl std::fmt::Display for StreamReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamReplicationError::LineageMismatch(message)
            | StreamReplicationError::WalNotRetained(message)
            | StreamReplicationError::SnapshotBoundaryMismatch(message)
            | StreamReplicationError::InvalidLsn(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for StreamReplicationError {}

impl From<&StreamReplicationError> for StreamReplicationErrorCode {
    fn from(error: &StreamReplicationError) -> Self {
        error.code()
    }
}

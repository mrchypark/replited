use std::path::Path;

use crate::base::Generation;
use crate::database::WalGenerationPos;
use crate::pb::replication::LsnToken;
use crate::pb::replication::StreamError;
use crate::pb::replication::stream_error::Code as StreamErrorCode;
use crate::sync::StreamReplicationError;
use crate::sync::StreamReplicationErrorCode;

pub(crate) fn lsn_token_to_pos(
    token: Option<LsnToken>,
) -> Result<WalGenerationPos, StreamReplicationError> {
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

pub(crate) fn optional_lsn_token_to_pos(
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

pub(crate) fn lsn_token_from_pos(pos: &WalGenerationPos) -> LsnToken {
    LsnToken {
        generation: pos.generation.as_str().to_string(),
        index: pos.index,
        offset: pos.offset,
    }
}

pub(crate) fn meta_dir_for_db_path(db_path: &Path) -> Option<String> {
    let db_name = db_path.file_name()?.to_str()?;
    let dir_path = match db_path.parent() {
        Some(parent) if parent == Path::new("") => Path::new("."),
        Some(parent) => parent,
        None => Path::new("."),
    };
    Some(format!("{}/.{}-replited/", dir_path.to_str()?, db_name))
}

pub(crate) fn stream_error_from_replication_error(
    error: StreamReplicationError,
    retry_after_ms: u32,
) -> StreamError {
    let code = stream_error_code_from_replication_code(error.code());
    StreamError {
        code: code as i32,
        message: error.to_string(),
        retry_after_ms,
    }
}

pub(crate) fn stream_error_code_from_replication_code(
    code: StreamReplicationErrorCode,
) -> StreamErrorCode {
    match code {
        StreamReplicationErrorCode::LineageMismatch => StreamErrorCode::LineageMismatch,
        StreamReplicationErrorCode::WalNotRetained => StreamErrorCode::WalNotRetained,
        StreamReplicationErrorCode::SnapshotBoundaryMismatch => {
            StreamErrorCode::SnapshotBoundaryMismatch
        }
        StreamReplicationErrorCode::InvalidLsn => StreamErrorCode::InvalidLsn,
    }
}

pub(crate) fn replication_error_code_from_stream_error(
    err: &StreamError,
) -> Option<StreamReplicationErrorCode> {
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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::database::WalGenerationPos;
    use crate::pb::replication::LsnToken;
    use crate::sync::StreamReplicationError;
    use crate::sync::StreamReplicationErrorCode;
    use crate::sync::stream_protocol::{
        lsn_token_from_pos, lsn_token_to_pos, meta_dir_for_db_path, optional_lsn_token_to_pos,
        replication_error_code_from_stream_error, stream_error_from_replication_error,
    };

    #[test]
    fn lsn_token_roundtrip() {
        let generation = crate::base::Generation::new();
        let pos = WalGenerationPos {
            generation,
            index: 7,
            offset: 4096,
        };

        let token = lsn_token_from_pos(&pos);
        let parsed = lsn_token_to_pos(Some(token)).unwrap();
        assert_eq!(parsed.generation, pos.generation);
        assert_eq!(parsed.index, pos.index);
        assert_eq!(parsed.offset, pos.offset);
    }

    #[test]
    fn optional_lsn_token_empty_is_none() {
        let token = LsnToken {
            generation: "".to_string(),
            index: 0,
            offset: 0,
        };
        assert!(optional_lsn_token_to_pos(Some(token)).unwrap().is_none());
    }

    #[test]
    fn lsn_token_requires_generation_when_position_nonzero() {
        let token = LsnToken {
            generation: "".to_string(),
            index: 1,
            offset: 0,
        };
        let err = optional_lsn_token_to_pos(Some(token)).unwrap_err();
        assert_eq!(err.code(), StreamReplicationErrorCode::InvalidLsn);
    }

    #[test]
    fn meta_dir_for_relative_db_path() {
        let path = Path::new("data.db");
        assert_eq!(
            meta_dir_for_db_path(path).as_deref(),
            Some("./.data.db-replited/")
        );
    }

    #[test]
    fn stream_error_mapping_roundtrip() {
        let stream_error = stream_error_from_replication_error(
            StreamReplicationError::WalNotRetained("wal missing".to_string()),
            123,
        );
        assert_eq!(stream_error.retry_after_ms, 123);
        assert_eq!(
            replication_error_code_from_stream_error(&stream_error),
            Some(StreamReplicationErrorCode::WalNotRetained)
        );
    }
}

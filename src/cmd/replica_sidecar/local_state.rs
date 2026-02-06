use std::path::{Path, PathBuf};

use crate::base::Generation;
use crate::database::WalGenerationPos;
use crate::error::{Error, Result};
use crate::sync::stream_protocol::meta_dir_for_db_path as protocol_meta_dir_for_db_path;

const REPLICA_ID_FILE: &str = "replica_id";
const LAST_APPLIED_LSN_FILE: &str = "last_applied_lsn";

pub(super) fn ensure_meta_dir(db_path: &str) -> Result<PathBuf> {
    let meta_dir = meta_dir_for_db_path(db_path)?;
    std::fs::create_dir_all(&meta_dir)?;
    Ok(meta_dir)
}

pub(super) fn load_or_create_replica_id(db_path: &str) -> Result<String> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let replica_id_path = meta_dir.join(REPLICA_ID_FILE);

    if let Ok(content) = std::fs::read_to_string(&replica_id_path) {
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    let new_id = new_replica_id();
    write_atomic(&replica_id_path, format!("{new_id}\n").as_bytes())?;
    Ok(new_id)
}

pub(super) fn read_last_applied_lsn(db_path: &str) -> Result<Option<WalGenerationPos>> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let lsn_path = meta_dir.join(LAST_APPLIED_LSN_FILE);
    let content = match std::fs::read_to_string(&lsn_path) {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let mut lines = content.lines().filter(|line| !line.trim().is_empty());
    let generation = lines.next().unwrap_or_default().trim().to_string();
    if generation.is_empty() {
        return Ok(None);
    }
    let index = lines
        .next()
        .ok_or_else(|| Error::InvalidConfig("last_applied_lsn missing index".to_string()))?
        .trim()
        .parse::<u64>()?;
    let offset = lines
        .next()
        .ok_or_else(|| Error::InvalidConfig("last_applied_lsn missing offset".to_string()))?
        .trim()
        .parse::<u64>()?;

    let generation = Generation::try_create(&generation)?;
    Ok(Some(WalGenerationPos {
        generation,
        index,
        offset,
    }))
}

pub(super) fn persist_last_applied_lsn(db_path: &str, pos: &WalGenerationPos) -> Result<()> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let lsn_path = meta_dir.join(LAST_APPLIED_LSN_FILE);
    let payload = format!(
        "{}\n{}\n{}\n",
        pos.generation.as_str(),
        pos.index,
        pos.offset
    );
    write_atomic(&lsn_path, payload.as_bytes())?;
    Ok(())
}

fn meta_dir_for_db_path(db_path: &str) -> Result<PathBuf> {
    let file_path = Path::new(db_path);
    protocol_meta_dir_for_db_path(file_path)
        .map(PathBuf::from)
        .ok_or_else(|| Error::InvalidPath(format!("invalid db path {db_path}")))
}

fn new_replica_id() -> String {
    let timestamp = uuid::timestamp::Timestamp::now(uuid::NoContext);
    uuid::Uuid::new_v7(timestamp).simple().to_string()
}

fn write_atomic(path: &Path, payload: &[u8]) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        use std::io::Write;
        file.write_all(payload)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp_path, path)?;
    Ok(())
}

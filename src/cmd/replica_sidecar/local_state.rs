use std::path::{Path, PathBuf};

use crate::base::Generation;
use crate::database::WalGenerationPos;
use crate::error::{Error, Result};
use crate::sync::stream_protocol::meta_dir_for_db_path as protocol_meta_dir_for_db_path;

const REPLICA_ID_FILE: &str = "replica_id";
const LAST_APPLIED_LSN_FILE: &str = "last_applied_lsn";
#[allow(dead_code)]
const SERVED_LSN_FILE: &str = "served_lsn";

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
    read_lsn_file(db_path, LAST_APPLIED_LSN_FILE)
}

#[allow(dead_code)]
pub(super) fn read_served_lsn(db_path: &str) -> Result<Option<WalGenerationPos>> {
    read_lsn_file(db_path, SERVED_LSN_FILE)
}

fn read_lsn_file(db_path: &str, file_name: &str) -> Result<Option<WalGenerationPos>> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let lsn_path = meta_dir.join(file_name);
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
    persist_lsn_file(db_path, LAST_APPLIED_LSN_FILE, pos)
}

#[allow(dead_code)]
pub(super) fn persist_served_lsn(db_path: &str, pos: &WalGenerationPos) -> Result<()> {
    persist_lsn_file(db_path, SERVED_LSN_FILE, pos)
}

fn persist_lsn_file(db_path: &str, file_name: &str, pos: &WalGenerationPos) -> Result<()> {
    let meta_dir = ensure_meta_dir(db_path)?;
    let lsn_path = meta_dir.join(file_name);
    let payload = format!(
        "{}\n{}\n{}\n",
        pos.generation.as_str(),
        pos.index,
        pos.offset
    );
    write_atomic(&lsn_path, payload.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{
        persist_last_applied_lsn, persist_served_lsn, read_last_applied_lsn, read_served_lsn,
    };
    use crate::base::Generation;
    use crate::database::WalGenerationPos;

    fn test_pos(index: u64, offset: u64) -> WalGenerationPos {
        WalGenerationPos {
            generation: Generation::new(),
            index,
            offset,
        }
    }

    #[test]
    fn served_lsn_is_persisted_independently_from_last_applied_lsn() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("replica.db");
        let db_path = db_path.to_string_lossy();
        let applied = test_pos(7, 4096);
        let served = test_pos(6, 2048);

        persist_last_applied_lsn(&db_path, &applied).expect("persist applied");
        persist_served_lsn(&db_path, &served).expect("persist served");

        assert_eq!(
            read_last_applied_lsn(&db_path).expect("read applied"),
            Some(applied)
        );
        assert_eq!(
            read_served_lsn(&db_path).expect("read served"),
            Some(served)
        );
    }

    #[test]
    fn missing_served_lsn_reads_as_none() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir.path().join("replica.db");
        let db_path = db_path.to_string_lossy();

        assert_eq!(read_served_lsn(&db_path).expect("read served"), None);
    }
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

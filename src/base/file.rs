use std::path::Path;
use std::path::PathBuf;
use std::sync::LazyLock;

use regex::{Captures, Regex};

use crate::error::Error;
use crate::error::Result;

static WAL_EXTENDION: &str = ".wal";
static WAL_REGEX: LazyLock<Regex> = LazyLock::new(|| compile_regex(r"^([0-9]{10})\.wal$"));
static WAL_SEGMENT_EXTENDION: &str = ".wal.zst";
static WAL_SEGMENT_REGEX: LazyLock<Regex> =
    LazyLock::new(|| compile_regex(r"^([0-9]{10})_([0-9]{10})\.wal\.zst$"));
static SNAPSHOT_REGEX: LazyLock<Regex> =
    LazyLock::new(|| compile_regex(r"^([0-9]{10})_([0-9]{10})\.snapshot\.zst$"));
static SNAPSHOT_EXTENDION: &str = ".snapshot.zst";

fn compile_regex(pattern: &str) -> Regex {
    Regex::new(pattern).expect("invalid static regex pattern in base::file")
}

fn parse_capture_u64(captures: &Captures<'_>, group: usize, error_message: &str) -> Result<u64> {
    let value = captures
        .get(group)
        .ok_or_else(|| Error::InvalidPath(error_message.to_string()))?
        .as_str();
    value
        .parse::<u64>()
        .map_err(|_| Error::InvalidPath(error_message.to_string()))
}

fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

// return base name of path
pub fn path_base(path: &str) -> Result<String> {
    let path_buf = PathBuf::from(path);
    path_buf
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or(Error::InvalidPath(format!("invalid path {path}")))
}

pub fn parent_dir(path: &str) -> Option<String> {
    let path = Path::new(path);
    path.parent()
        .map(|parent| parent.to_string_lossy().into_owned())
}

// parse wal file path, return wal index
pub fn parse_wal_path(path: &str) -> Result<u64> {
    let base = path_base(path)?;
    let captures = WAL_REGEX
        .captures(&base)
        .ok_or(Error::InvalidPath(format!("invalid wal path {path}")))?;
    parse_capture_u64(&captures, 1, &format!("invalid wal path {path}"))
}

pub fn format_wal_path(index: u64) -> String {
    format!("{index:0>10}{WAL_EXTENDION}")
}

pub fn parse_wal_segment_path(path: &str) -> Result<(u64, u64)> {
    let base = path_base(path)?;
    let captures = WAL_SEGMENT_REGEX
        .captures(&base)
        .ok_or(Error::InvalidPath(format!(
            "invalid wal segment path {path}"
        )))?;
    let error_message = format!("invalid wal segment path {path}");
    let index = parse_capture_u64(&captures, 1, &error_message)?;
    let offset = parse_capture_u64(&captures, 2, &error_message)?;

    Ok((index, offset))
}

// parse snapshot file path, return snapshot index and offset
pub fn parse_snapshot_path(path: &str) -> Result<(u64, u64)> {
    let base = path_base(path)?;
    let captures = SNAPSHOT_REGEX
        .captures(&base)
        .ok_or(Error::InvalidPath(format!("invalid snapshot path {path}")))?;
    let error_message = format!("invalid snapshot path {path}");
    let index = parse_capture_u64(&captures, 1, &error_message)?;
    let offset = parse_capture_u64(&captures, 2, &error_message)?;
    Ok((index, offset))
}

pub fn format_snapshot_path(index: u64, offset: u64) -> String {
    format!("{index:0>10}_{offset:0>10}{SNAPSHOT_EXTENDION}")
}

pub fn local_generations_dir(meta_dir: &str) -> String {
    path_to_string(Path::new(meta_dir).join("generations"))
}

pub fn remote_generations_dir(db_name: &str) -> String {
    path_to_string(Path::new(db_name).join("generations/"))
}

// returns the path of a single generation.
pub fn generation_dir(meta_dir: &str, generation: &str) -> String {
    path_to_string(Path::new(meta_dir).join("generations").join(generation))
}

pub fn snapshots_dir(db: &str, generation: &str) -> String {
    path_to_string(Path::new(&generation_dir(db, generation)).join("snapshots/"))
}

pub fn snapshot_file(db: &str, generation: &str, index: u64, offset: u64) -> String {
    path_to_string(
        Path::new(&generation_dir(db, generation))
            .join("snapshots")
            .join(format_snapshot_path(index, offset)),
    )
}

pub fn walsegments_dir(db: &str, generation: &str) -> String {
    path_to_string(Path::new(&generation_dir(db, generation)).join("wal/"))
}

pub fn walsegment_file(db: &str, generation: &str, index: u64, offset: u64) -> String {
    path_to_string(
        Path::new(&generation_dir(db, generation))
            .join("wal")
            .join(format_walsegment_path(index, offset)),
    )
}

pub fn format_walsegment_path(index: u64, offset: u64) -> String {
    format!("{index:0>10}_{offset:0>10}{WAL_SEGMENT_EXTENDION}")
}

// returns the path of the name of the current generation.
pub fn generation_file_path(meta_dir: &str) -> String {
    path_to_string(Path::new(meta_dir).join("generation"))
}

pub fn shadow_wal_dir(meta_dir: &str, generation: &str) -> String {
    path_to_string(Path::new(&generation_dir(meta_dir, generation)).join("wal"))
}

pub fn shadow_wal_file(meta_dir: &str, generation: &str, index: u64) -> String {
    path_to_string(Path::new(&shadow_wal_dir(meta_dir, generation)).join(format_wal_path(index)))
}

#[cfg(test)]
mod tests {
    use super::format_snapshot_path;
    use super::format_wal_path;
    use super::format_walsegment_path;
    use super::parent_dir;
    use super::parse_snapshot_path;
    use super::parse_wal_path;
    use super::parse_wal_segment_path;
    use super::path_base;
    use crate::error::Result;

    #[test]
    fn test_path_base() -> Result<()> {
        let path = "a/b/c";
        let base = path_base(path)?;
        assert_eq!(&base, "c");

        let path = "c/";
        let base = path_base(path)?;
        assert_eq!(&base, "c");

        let path = "a-b/..";
        let base = path_base(path);
        assert!(base.is_err());
        let err = base.unwrap_err();
        assert_eq!(err.code(), 54);

        Ok(())
    }

    #[test]
    fn test_parse_wal_path() -> Result<()> {
        let path = "a/b/c/0000000019.wal";
        let index = parse_wal_path(path)?;
        assert_eq!(index, 19);

        let path = "a/b/c/000000019.wal";
        let index = parse_wal_path(path);
        assert!(index.is_err());

        let path = format!("a/b/{}", format_wal_path(19));
        let index = parse_wal_path(&path)?;
        assert_eq!(index, 19);
        Ok(())
    }

    #[test]
    fn test_parse_snapshot_path() -> Result<()> {
        let path = "a/b/c/0000000019_0000000100.snapshot.zst";
        let (index, offset) = parse_snapshot_path(path)?;
        assert_eq!(index, 19);
        assert_eq!(offset, 100);

        let path = "a/b/c/000000019.snapshot.zst";
        let index = parse_snapshot_path(path);
        assert!(index.is_err());

        let path = format!("a/b/{}", format_snapshot_path(19, 100));
        let (index, offset) = parse_snapshot_path(&path)?;
        assert_eq!(index, 19);
        assert_eq!(offset, 100);
        Ok(())
    }

    #[test]
    fn test_parse_walsegment_path() -> Result<()> {
        let path = "a/b/c/0000000019_0000000020.wal.zst";
        let (index, offset) = parse_wal_segment_path(path)?;
        assert_eq!(index, 19);
        assert_eq!(offset, 20);

        let path = format!("a/b/{}", format_walsegment_path(19, 20));
        let (index, offset) = parse_wal_segment_path(&path)?;
        assert_eq!(index, 19);
        assert_eq!(offset, 20);

        Ok(())
    }

    #[test]
    fn test_parent_dir() -> Result<()> {
        let path = "/b/c/0000000019_0000000020.wal.zst";
        let dir = parent_dir(path);
        assert_eq!(dir, Some("/b/c".to_string()));

        Ok(())
    }
}

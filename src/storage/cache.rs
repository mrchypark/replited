use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{Error, Result};

pub const DEFAULT_CACHE_SIZE_LIMIT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct LocalObjectCache {
    root: Arc<PathBuf>,
    max_bytes: u64,
    pinned_keys: Arc<RwLock<HashSet<String>>>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    key: String,
    path: PathBuf,
    size: u64,
    modified: std::time::SystemTime,
}

impl LocalObjectCache {
    pub fn try_create(root: impl Into<PathBuf>, max_bytes: u64) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| {
            Error::StorageError(format!("create cache root {}: {err}", root.display()))
        })?;

        let cache = Self {
            root: Arc::new(root),
            max_bytes,
            pinned_keys: Arc::new(RwLock::new(HashSet::new())),
        };
        cache.cleanup_incomplete_temp_files()?;
        cache.prune_to_budget()?;
        Ok(cache)
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn path_for_key(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path_for_key(key);
        match fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Error::StorageError(format!(
                "read cache object {}: {err}",
                path.display()
            ))),
        }
    }

    pub fn remove(&self, key: &str) -> Result<()> {
        let path = self.path_for_key(key);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::StorageError(format!(
                "remove cache object {}: {err}",
                path.display()
            ))),
        }
    }

    pub fn put(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let path = self.path_for_key(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| {
                Error::StorageError(format!("create cache parent {}: {err}", parent.display()))
            })?;
        }

        let temp_path = path.with_file_name(format!(
            "{}.tmp",
            path.file_name()
                .ok_or_else(|| Error::StorageError(
                    "cache object path missing file name".to_string()
                ))?
                .to_string_lossy()
        ));

        fs::write(&temp_path, bytes).map_err(|err| {
            Error::StorageError(format!(
                "write cache temp object {}: {err}",
                temp_path.display()
            ))
        })?;
        fs::rename(&temp_path, &path).map_err(|err| {
            Error::StorageError(format!(
                "rename cache temp object {} -> {}: {err}",
                temp_path.display(),
                path.display()
            ))
        })?;

        self.prune_to_budget()?;
        Ok(())
    }

    pub fn set_pinned_keys<I>(&self, keys: I)
    where
        I: IntoIterator<Item = String>,
    {
        *self.pinned_keys.write() = keys.into_iter().collect();
    }

    pub fn clear_pinned_keys(&self) {
        self.pinned_keys.write().clear();
    }

    pub fn cleanup_incomplete_temp_files(&self) -> Result<()> {
        self.walk_and_cleanup_tmp(self.root())
    }

    pub fn prune_to_budget(&self) -> Result<()> {
        let mut entries = Vec::new();
        self.collect_entries(self.root(), &mut entries)?;

        let mut total_bytes = entries.iter().map(|entry| entry.size).sum::<u64>();
        if total_bytes <= self.max_bytes {
            return Ok(());
        }

        let pinned = self.pinned_keys.read().clone();
        entries.sort_by_key(|entry| entry.modified);
        for entry in entries {
            if total_bytes <= self.max_bytes {
                break;
            }
            if pinned.contains(&entry.key) {
                continue;
            }
            fs::remove_file(&entry.path).map_err(|err| {
                Error::StorageError(format!(
                    "prune cache object {}: {err}",
                    entry.path.display()
                ))
            })?;
            total_bytes = total_bytes.saturating_sub(entry.size);
        }

        Ok(())
    }

    fn walk_and_cleanup_tmp(&self, dir: &Path) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(|err| {
            Error::StorageError(format!("read cache directory {}: {err}", dir.display()))
        })? {
            let entry = entry.map_err(|err| {
                Error::StorageError(format!("read cache entry in {}: {err}", dir.display()))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                Error::StorageError(format!("read cache file type {}: {err}", path.display()))
            })?;
            if file_type.is_dir() {
                self.walk_and_cleanup_tmp(&path)?;
                continue;
            }
            if path
                .file_name()
                .map(|name| name.to_string_lossy().ends_with(".tmp"))
                .unwrap_or(false)
            {
                fs::remove_file(&path).map_err(|err| {
                    Error::StorageError(format!(
                        "remove incomplete cache temp file {}: {err}",
                        path.display()
                    ))
                })?;
            }
        }
        Ok(())
    }

    fn collect_entries(&self, dir: &Path, out: &mut Vec<CacheEntry>) -> Result<()> {
        for entry in fs::read_dir(dir).map_err(|err| {
            Error::StorageError(format!("read cache directory {}: {err}", dir.display()))
        })? {
            let entry = entry.map_err(|err| {
                Error::StorageError(format!("read cache entry in {}: {err}", dir.display()))
            })?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| {
                Error::StorageError(format!("read cache file type {}: {err}", path.display()))
            })?;
            if file_type.is_dir() {
                self.collect_entries(&path, out)?;
                continue;
            }
            if path
                .file_name()
                .map(|name| name.to_string_lossy().ends_with(".tmp"))
                .unwrap_or(false)
            {
                continue;
            }
            let metadata = fs::metadata(&path).map_err(|err| {
                Error::StorageError(format!("read cache metadata {}: {err}", path.display()))
            })?;
            let relative_key = path
                .strip_prefix(self.root())
                .map_err(|err| {
                    Error::StorageError(format!(
                        "strip cache root {} from {}: {err}",
                        self.root().display(),
                        path.display()
                    ))
                })?
                .to_string_lossy()
                .into_owned();
            out.push(CacheEntry {
                key: relative_key,
                path,
                size: metadata.len(),
                modified: metadata
                    .modified()
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{DEFAULT_CACHE_SIZE_LIMIT_BYTES, LocalObjectCache};

    #[test]
    fn cache_put_get_and_miss_are_exact_keyed() {
        let temp = tempdir().expect("tempdir");
        let cache =
            LocalObjectCache::try_create(temp.path().join("cache"), DEFAULT_CACHE_SIZE_LIMIT_BYTES)
                .expect("cache");

        assert!(cache.get("db/object-1").expect("cache get").is_none());
        cache.put("db/object-1", b"hello").expect("cache put");
        assert_eq!(
            cache.get("db/object-1").expect("cache get"),
            Some(b"hello".to_vec())
        );
        assert!(cache.get("db/object-2").expect("cache get").is_none());
    }

    #[test]
    fn cache_startup_cleanup_removes_incomplete_temp_files() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("cache");
        std::fs::create_dir_all(root.join("db")).expect("create db dir");
        std::fs::write(root.join("db/object.tmp"), b"partial").expect("write tmp");
        std::fs::write(root.join("db/object"), b"complete").expect("write object");

        let cache = LocalObjectCache::try_create(&root, DEFAULT_CACHE_SIZE_LIMIT_BYTES)
            .expect("cache create");

        assert!(!cache.path_for_key("db/object.tmp").exists());
        assert!(cache.path_for_key("db/object").exists());
    }

    #[test]
    fn cache_prune_preserves_pinned_keys() {
        let temp = tempdir().expect("tempdir");
        let cache =
            LocalObjectCache::try_create(temp.path().join("cache"), 8).expect("cache create");

        cache.put("db/old", b"1234").expect("cache put old");
        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.put("db/pinned", b"5678").expect("cache put pinned");
        cache.set_pinned_keys(["db/pinned".to_string()]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.put("db/new", b"9999").expect("cache put new");

        assert!(!cache.path_for_key("db/old").exists());
        assert!(cache.path_for_key("db/pinned").exists());
        assert!(cache.path_for_key("db/new").exists());
    }
}

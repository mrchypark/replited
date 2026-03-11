use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use chrono::DateTime;
use chrono::Utc;
use log::debug;
use log::warn;
use opendal::Metadata;
use opendal::Metakey;
use opendal::Operator;
use sha2::{Digest, Sha256};
use tokio::time::sleep;

use super::init_operator;
use super::LocalObjectCache;
use super::manifest::GenerationManifest;
use super::manifest::LatestPointer;
use super::manifest::ManifestWalPack;
use crate::base::Generation;
use crate::base::parent_dir;
use crate::base::parse_snapshot_path;
use crate::base::parse_wal_segment_path;
use crate::base::path_base;
use crate::base::snapshot_file;
use crate::base::snapshots_dir;
use crate::base::walsegment_file;
use crate::base::walsegments_dir;
use crate::config::StorageConfig;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct StorageClient {
    operator: Operator,
    db_name: String,
    latest_pointer_update_policy: LatestPointerUpdatePolicy,
    restore_request_costs: RestoreRequestCostStats,
    cache: Option<LocalObjectCache>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RestoreRequestCostStats {
    inner: Arc<RestoreRequestCostStatsInner>,
}

#[derive(Debug, Default)]
struct RestoreRequestCostStatsInner {
    latest_pointer_gets: AtomicU64,
    generation_manifest_gets: AtomicU64,
    object_gets: AtomicU64,
    list_calls: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_write_failures: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RestoreRequestCostSnapshot {
    pub(crate) latest_pointer_gets: u64,
    pub(crate) generation_manifest_gets: u64,
    pub(crate) object_gets: u64,
    pub(crate) list_calls: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) cache_write_failures: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestPointerGuard {
    etag: Option<String>,
    content_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LatestPointerUpdatePolicy {
    FsCompareUnderLock { root: PathBuf },
    SingleWriterUnconditional { backend: String },
    Unsupported { backend: String },
}

#[derive(Debug)]
struct FsPathLock {
    path: PathBuf,
}

impl Drop for FsPathLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotInfo {
    pub generation: Generation,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WalSegmentInfo {
    pub generation: Generation,
    pub index: u64,
    pub offset: u64,
    pub size: u64,
    pub created_at: DateTime<Utc>,
}

fn metadata_last_modified_or_epoch(metadata: &Metadata, entry_name: &str) -> DateTime<Utc> {
    match metadata.last_modified() {
        Some(ts) => ts,
        None => {
            warn!(
                "storage metadata missing last_modified for entry {entry_name}, using UNIX_EPOCH",
            );
            DateTime::<Utc>::from(SystemTime::UNIX_EPOCH)
        }
    }
}

#[cfg(test)]
fn snapshot_position_is_newer(index: u64, offset: u64, current: Option<(u64, u64)>) -> bool {
    match current {
        None => true,
        Some((current_index, current_offset)) => {
            index > current_index || (index == current_index && offset > current_offset)
        }
    }
}

impl RestoreRequestCostStats {
    fn record_latest_pointer_get(&self) {
        self.inner
            .latest_pointer_gets
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_generation_manifest_get(&self) {
        self.inner
            .generation_manifest_gets
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_object_get(&self) {
        self.inner.object_gets.fetch_add(1, Ordering::Relaxed);
    }

    fn record_list_call(&self) {
        self.inner.list_calls.fetch_add(1, Ordering::Relaxed);
    }

    fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_cache_write_failure(&self) {
        self.inner
            .cache_write_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> RestoreRequestCostSnapshot {
        RestoreRequestCostSnapshot {
            latest_pointer_gets: self.inner.latest_pointer_gets.load(Ordering::Relaxed),
            generation_manifest_gets: self.inner.generation_manifest_gets.load(Ordering::Relaxed),
            object_gets: self.inner.object_gets.load(Ordering::Relaxed),
            list_calls: self.inner.list_calls.load(Ordering::Relaxed),
            cache_hits: self.inner.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache_misses.load(Ordering::Relaxed),
            cache_write_failures: self.inner.cache_write_failures.load(Ordering::Relaxed),
        }
    }
}

impl StorageClient {
    fn delete_fs_object_if_exists(root: &Path, key: &str) -> Result<bool> {
        let path = root.join(key);
        if !std::fs::exists(&path)? {
            return Ok(false);
        }

        std::fs::remove_file(&path).map_err(|err| {
            Error::StorageError(format!("remove object {}: {err}", path.display()))
        })?;
        Ok(true)
    }

    fn supports_manifest_publish(&self) -> bool {
        matches!(
            self.latest_pointer_update_policy,
            LatestPointerUpdatePolicy::FsCompareUnderLock { .. }
                | LatestPointerUpdatePolicy::SingleWriterUnconditional { .. }
        )
    }

    pub(crate) fn try_create_with_restore_request_costs(
        db_path: String,
        config: StorageConfig,
        restore_request_costs: RestoreRequestCostStats,
    ) -> Result<Self> {
        Self::try_create_with_cache_and_restore_request_costs(
            db_path,
            config,
            None,
            restore_request_costs,
        )
    }

    pub(crate) fn try_create_with_cache_and_restore_request_costs(
        db_path: String,
        config: StorageConfig,
        cache: Option<LocalObjectCache>,
        restore_request_costs: RestoreRequestCostStats,
    ) -> Result<Self> {
        let latest_pointer_update_policy = match &config.params {
            crate::config::StorageParams::Fs(fs) => LatestPointerUpdatePolicy::FsCompareUnderLock {
                root: PathBuf::from(&fs.root),
            },
            crate::config::StorageParams::S3(_)
            | crate::config::StorageParams::Gcs(_)
            | crate::config::StorageParams::Azb(_) => {
                LatestPointerUpdatePolicy::SingleWriterUnconditional {
                    backend: config.name.clone(),
                }
            }
            _ => LatestPointerUpdatePolicy::Unsupported {
                backend: config.name.clone(),
            },
        };
        Ok(Self {
            operator: init_operator(&config.params)?,
            db_name: path_base(&db_path)?,
            latest_pointer_update_policy,
            restore_request_costs,
            cache,
        })
    }

    pub fn try_create(db_path: String, config: StorageConfig) -> Result<Self> {
        Self::try_create_with_cache_and_restore_request_costs(
            db_path,
            config,
            None,
            RestoreRequestCostStats::default(),
        )
    }

    pub fn try_create_with_cache(
        db_path: String,
        config: StorageConfig,
        cache: Option<LocalObjectCache>,
    ) -> Result<Self> {
        Self::try_create_with_cache_and_restore_request_costs(
            db_path,
            config,
            cache,
            RestoreRequestCostStats::default(),
        )
    }

    fn latest_pointer_key(&self) -> String {
        format!("{}/pointers/latest.json", self.db_name)
    }

    fn generation_manifest_key(&self, generation: &str) -> String {
        format!(
            "{}/manifests/generations/{}.manifest.json",
            self.db_name, generation
        )
    }

    fn unsupported_conditional_latest_update_error(&self) -> Error {
        match &self.latest_pointer_update_policy {
            LatestPointerUpdatePolicy::FsCompareUnderLock { .. } => {
                Error::StorageError("fs latest pointer update policy misconfigured".to_string())
            }
            LatestPointerUpdatePolicy::SingleWriterUnconditional { backend } => {
                Error::InvalidConfig(format!(
                    "conditional latest pointer update is unsupported for backend {backend}"
                ))
            }
            LatestPointerUpdatePolicy::Unsupported { backend } => Error::InvalidConfig(format!(
                "conditional latest pointer update is unsupported for backend {backend}"
            )),
        }
    }

    fn latest_pointer_revision(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn latest_pointer_regresses_to_older_generation(
        current: &LatestPointer,
        candidate: &LatestPointer,
    ) -> bool {
        let Ok(current_generation) = Generation::try_create(&current.current_generation) else {
            return false;
        };
        let Ok(candidate_generation) = Generation::try_create(&candidate.current_generation) else {
            return false;
        };

        current_generation > candidate_generation
    }

    fn manifest_snapshot_id(pos: &WalGenerationPos) -> String {
        format!("snapshot-{:010}-{:010}", pos.index, pos.offset)
    }

    fn manifest_lineage_id(generation: &Generation) -> String {
        generation.as_str().to_string()
    }

    fn manifest_wal_pack_key(&self, start: &WalGenerationPos, end: &WalGenerationPos) -> String {
        format!(
            "{}/generations/{}/wal/ranges/{:010}_{:010}_{:010}_{:010}/{:010}_{:010}.wal.zst",
            self.db_name,
            start.generation.as_str(),
            start.index,
            start.offset,
            end.index,
            end.offset,
            start.index,
            start.offset
        )
    }

    fn manifest_publish_lock_path(root: &Path, db_name: &str, generation: &str) -> PathBuf {
        root.join(db_name)
            .join("manifests")
            .join("generations")
            .join(format!("{generation}.publish.lock"))
    }

    fn manifest_publish_root(&self) -> Result<PathBuf> {
        match &self.latest_pointer_update_policy {
            LatestPointerUpdatePolicy::FsCompareUnderLock { root } => Ok(root.clone()),
            LatestPointerUpdatePolicy::SingleWriterUnconditional { .. } => Err(
                Error::InvalidConfig(
                    "manifest publish root is unavailable for non-fs single-writer backends"
                        .to_string(),
                ),
            ),
            LatestPointerUpdatePolicy::Unsupported { backend } => Err(Error::InvalidConfig(
                format!("manifest publish path is unsupported for backend {backend}"),
            )),
        }
    }

    async fn acquire_manifest_publish_lock(
        &self,
        generation: &str,
    ) -> Result<Option<FsPathLock>> {
        match &self.latest_pointer_update_policy {
            LatestPointerUpdatePolicy::FsCompareUnderLock { root } => Ok(Some(
                Self::acquire_fs_path_lock(&Self::manifest_publish_lock_path(
                    root,
                    &self.db_name,
                    generation,
                ))
                .await?,
            )),
            LatestPointerUpdatePolicy::SingleWriterUnconditional { .. } => Ok(None),
            LatestPointerUpdatePolicy::Unsupported { backend } => Err(Error::InvalidConfig(
                format!("manifest publish path is unsupported for backend {backend}"),
            )),
        }
    }

    fn next_manifest_id(existing_packs: usize) -> String {
        format!(
            "manifest-{}-{}",
            Utc::now().timestamp_millis(),
            existing_packs
        )
    }

    async fn write_immutable_object(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.ensure_parent_exist(key).await?;
        self.operator.write(key, bytes.clone()).await?;
        if let Some(cache) = &self.cache
            && let Err(err) = cache.put(key, &bytes)
        {
            self.restore_request_costs.record_cache_write_failure();
            warn!("cache write failed for {key}: {err}");
        }
        Ok(())
    }

    async fn publish_latest_pointer_for_manifest(
        &self,
        generation: &str,
        manifest_bytes: &[u8],
    ) -> Result<()> {
        let pointer = LatestPointer {
            format_version: 1,
            current_generation: generation.to_string(),
            current_manifest_key: self.generation_manifest_key(generation),
            current_manifest_sha256: Self::latest_pointer_revision(manifest_bytes),
            created_at: Utc::now(),
        };

        match self.read_latest_pointer().await? {
            Some((current, guard)) => {
                if Self::latest_pointer_regresses_to_older_generation(&current, &pointer) {
                    return Err(Error::StorageError(format!(
                        "reject latest pointer regression from {} to {}",
                        current.current_generation, pointer.current_generation
                    )));
                }
                self.write_latest_pointer_conditional(&pointer, Some(&guard))
                    .await
            }
            None => self.write_latest_pointer_conditional(&pointer, None).await,
        }
    }

    fn fs_metadata_path(root: &Path, key: &str) -> PathBuf {
        root.join(key)
    }

    fn latest_pointer_lock_path(latest_path: &Path) -> Result<PathBuf> {
        let file_name = latest_path
            .file_name()
            .ok_or_else(|| {
                Error::StorageError("latest pointer path missing file name".to_string())
            })?
            .to_string_lossy()
            .to_string();
        Ok(latest_path.with_file_name(format!("{file_name}.lock")))
    }

    async fn acquire_fs_path_lock(lock_path: &Path) -> Result<FsPathLock> {
        let parent = lock_path
            .parent()
            .ok_or_else(|| Error::StorageError("lock path missing parent".to_string()))?;
        std::fs::create_dir_all(parent).map_err(|err| {
            Error::StorageError(format!("create lock directory {}: {err}", parent.display()))
        })?;

        for _ in 0..200 {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lock_path)
            {
                Ok(_) => {
                    return Ok(FsPathLock {
                        path: lock_path.to_path_buf(),
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    sleep(Duration::from_millis(10)).await;
                }
                Err(err) => {
                    return Err(Error::StorageError(format!(
                        "acquire lock {}: {err}",
                        lock_path.display()
                    )));
                }
            }
        }

        Err(Error::StorageError(format!(
            "timed out waiting for lock {}",
            lock_path.display()
        )))
    }

    async fn acquire_fs_latest_pointer_lock(latest_path: &Path) -> Result<FsPathLock> {
        let lock_path = Self::latest_pointer_lock_path(latest_path)?;
        Self::acquire_fs_path_lock(&lock_path).await
    }

    #[cfg(test)]
    pub(crate) async fn write_latest_pointer(&self, pointer: &LatestPointer) -> Result<()> {
        let key = self.latest_pointer_key();
        let payload = serde_json::to_vec(pointer)
            .map_err(|err| Error::StorageError(format!("serialize latest pointer: {err}")))?;
        self.ensure_parent_exist(&key).await?;
        self.operator.write(&key, payload).await?;
        Ok(())
    }

    pub(crate) async fn read_latest_pointer(
        &self,
    ) -> Result<Option<(LatestPointer, LatestPointerGuard)>> {
        self.restore_request_costs.record_latest_pointer_get();
        let key = self.latest_pointer_key();
        let metadata = match self.operator.stat(&key).await {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let bytes = self.operator.read(&key).await?;
        let raw_bytes = bytes.to_bytes();
        let pointer = serde_json::from_slice::<LatestPointer>(raw_bytes.as_ref())
            .map_err(|err| Error::StorageError(format!("deserialize latest pointer: {err}")))?;
        Ok(Some((
            pointer,
            LatestPointerGuard {
                etag: metadata.etag().map(ToOwned::to_owned),
                content_sha256: Self::latest_pointer_revision(raw_bytes.as_ref()),
            },
        )))
    }

    pub(crate) async fn write_latest_pointer_conditional(
        &self,
        pointer: &LatestPointer,
        guard: Option<&LatestPointerGuard>,
    ) -> Result<()> {
        match &self.latest_pointer_update_policy {
            LatestPointerUpdatePolicy::FsCompareUnderLock { root } => {
                let key = self.latest_pointer_key();
                let path = Self::fs_metadata_path(root, &key);
                let _lock = Self::acquire_fs_latest_pointer_lock(&path).await?;

                let current_bytes = match std::fs::read(&path) {
                    Ok(bytes) => Some(bytes),
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                    Err(err) => {
                        return Err(Error::StorageError(format!(
                            "read latest pointer {}: {err}",
                            path.display()
                        )));
                    }
                };

                if let Some(bytes) = current_bytes.as_ref() {
                    let current_pointer =
                        serde_json::from_slice::<LatestPointer>(bytes).map_err(|err| {
                            Error::StorageError(format!(
                                "deserialize latest pointer under fs lock: {err}"
                            ))
                        })?;
                    if Self::latest_pointer_regresses_to_older_generation(&current_pointer, pointer)
                    {
                        return Err(Error::StorageError(format!(
                            "reject latest pointer regression from {} to {}",
                            current_pointer.current_generation, pointer.current_generation
                        )));
                    }
                }

                match (guard, current_bytes.as_ref()) {
                    (Some(guard), Some(bytes)) => {
                        let current_revision = Self::latest_pointer_revision(bytes);
                        if current_revision != guard.content_sha256 {
                            return Err(Error::StorageError(
                                "conditional latest pointer update rejected on fs backend: stale guard"
                                    .to_string(),
                            ));
                        }
                    }
                    (Some(_), None) => {
                        return Err(Error::StorageError(
                            "conditional latest pointer update rejected on fs backend: latest pointer missing"
                                .to_string(),
                        ))
                    }
                    (None, Some(_)) => {
                        return Err(Error::StorageError(
                            "conditional latest pointer update rejected on fs backend: latest pointer already exists"
                                .to_string(),
                        ))
                    }
                    (None, None) => {}
                }

                let parent = path.parent().ok_or_else(|| {
                    Error::StorageError("latest pointer path missing parent".to_string())
                })?;
                std::fs::create_dir_all(parent).map_err(|err| {
                    Error::StorageError(format!(
                        "create latest pointer directory {}: {err}",
                        parent.display()
                    ))
                })?;

                let payload = serde_json::to_vec(pointer).map_err(|err| {
                    Error::StorageError(format!("serialize latest pointer: {err}"))
                })?;
                let temp_path = path.with_file_name(format!(
                    "{}.tmp",
                    path.file_name()
                        .ok_or_else(|| Error::StorageError(
                            "latest pointer path missing file name".to_string()
                        ))?
                        .to_string_lossy()
                ));
                std::fs::write(&temp_path, payload).map_err(|err| {
                    Error::StorageError(format!(
                        "write temp latest pointer {}: {err}",
                        temp_path.display()
                    ))
                })?;
                std::fs::rename(&temp_path, &path).map_err(|err| {
                    Error::StorageError(format!(
                        "rename temp latest pointer {} -> {}: {err}",
                        temp_path.display(),
                        path.display()
                    ))
                })?;
                Ok(())
            }
            LatestPointerUpdatePolicy::SingleWriterUnconditional { .. } => {
                let key = self.latest_pointer_key();
                let payload = serde_json::to_vec(pointer).map_err(|err| {
                    Error::StorageError(format!("serialize latest pointer: {err}"))
                })?;
                self.ensure_parent_exist(&key).await?;
                self.operator.write(&key, payload).await?;
                Ok(())
            }
            LatestPointerUpdatePolicy::Unsupported { .. } => {
                Err(self.unsupported_conditional_latest_update_error())
            }
        }
    }

    pub(crate) async fn write_generation_manifest(
        &self,
        manifest: &GenerationManifest,
    ) -> Result<()> {
        let key = self.generation_manifest_key(&manifest.generation);
        let payload = serde_json::to_vec(manifest)
            .map_err(|err| Error::StorageError(format!("serialize generation manifest: {err}")))?;
        self.ensure_parent_exist(&key).await?;
        self.operator.write(&key, payload).await?;
        Ok(())
    }

    pub(crate) async fn read_generation_manifest(
        &self,
        generation: &str,
    ) -> Result<Option<GenerationManifest>> {
        self.restore_request_costs.record_generation_manifest_get();
        let key = self.generation_manifest_key(generation);
        let bytes = match self.operator.read(&key).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == opendal::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let manifest =
            serde_json::from_slice::<GenerationManifest>(&bytes.to_bytes()).map_err(|err| {
                Error::StorageError(format!("deserialize generation manifest: {err}"))
            })?;
        Ok(Some(manifest))
    }

    pub(crate) async fn read_manifest_restore_inputs(
        &self,
    ) -> Result<
        Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            Vec<(u64, u64, String, String, u64, String, String)>,
        )>,
    > {
        let (pointer, _guard) = match self.read_latest_pointer().await? {
            Some(pointer) => pointer,
            None => return Ok(None),
        };

        let expected_manifest_key = self.generation_manifest_key(&pointer.current_generation);
        if pointer.current_manifest_key != expected_manifest_key {
            return Err(Error::StorageError(format!(
                "latest pointer manifest key mismatch: expected {}, got {}",
                expected_manifest_key, pointer.current_manifest_key
            )));
        }

        let manifest = self
            .read_generation_manifest(&pointer.current_generation)
            .await?
            .ok_or_else(|| {
                Error::StorageError(format!(
                    "latest pointer references missing generation manifest {}",
                    pointer.current_generation
                ))
            })?;
        if manifest.generation != pointer.current_generation {
            return Err(Error::StorageError(format!(
                "generation manifest generation mismatch: expected {}, got {}",
                pointer.current_generation, manifest.generation
            )));
        }

        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|err| Error::StorageError(format!("serialize generation manifest: {err}")))?;
        let manifest_sha256 = Self::latest_pointer_revision(&manifest_bytes);
        if manifest_sha256 != pointer.current_manifest_sha256 {
            return Err(Error::StorageError(format!(
                "generation manifest sha256 mismatch for {}",
                pointer.current_generation
            )));
        }

        Ok(Some((
            manifest.generation,
            manifest.manifest_id,
            manifest.lineage_id,
            manifest.base_snapshot_id,
            manifest.base_snapshot_sha256,
            manifest.base_snapshot,
            manifest
                .wal_packs
                .into_iter()
                .map(|pack| {
                    (
                        pack.start_lsn,
                        pack.end_lsn,
                        pack.object_key,
                        pack.sha256,
                        pack.size_bytes,
                        pack.lineage_id,
                        pack.base_snapshot_id,
                    )
                })
                .collect(),
        )))
    }

    pub(crate) async fn publish_manifest_snapshot(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        if !self.supports_manifest_publish() {
            return Err(Error::InvalidConfig(
                "manifest publish path is unsupported for this backend".to_string(),
            ));
        }

        let _publish_lock = self
            .acquire_manifest_publish_lock(pos.generation.as_str())
            .await?;

        let snapshot_key = snapshot_file(
            &self.db_name,
            pos.generation.as_str(),
            pos.index,
            pos.offset,
        );
        self.write_immutable_object(&snapshot_key, compressed_data.clone())
            .await?;

        let manifest = GenerationManifest {
            format_version: 1,
            generation: pos.generation.as_str().to_string(),
            manifest_id: Self::next_manifest_id(0),
            lineage_id: Self::manifest_lineage_id(&pos.generation),
            base_snapshot_id: Self::manifest_snapshot_id(pos),
            base_snapshot_sha256: Self::latest_pointer_revision(&compressed_data),
            base_snapshot: snapshot_key,
            wal_packs: vec![],
        };

        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|err| Error::StorageError(format!("serialize generation manifest: {err}")))?;
        self.write_generation_manifest(&manifest).await?;
        self.publish_latest_pointer_for_manifest(pos.generation.as_str(), &manifest_bytes)
            .await?;
        Ok(())
    }

    pub(crate) async fn publish_manifest_wal_pack(
        &self,
        start: &WalGenerationPos,
        end: &WalGenerationPos,
        compressed_data: Vec<u8>,
        uncompressed_size_bytes: usize,
    ) -> Result<()> {
        if !self.supports_manifest_publish() {
            return Err(Error::InvalidConfig(
                "manifest publish path is unsupported for this backend".to_string(),
            ));
        }
        if start.generation != end.generation {
            return Err(Error::StorageError(
                "manifest wal pack generation mismatch".to_string(),
            ));
        }
        let _publish_lock = self
            .acquire_manifest_publish_lock(start.generation.as_str())
            .await?;

        let mut manifest = self
            .read_generation_manifest(start.generation.as_str())
            .await?
            .ok_or_else(|| {
                Error::StorageError(format!(
                    "cannot publish wal pack without base snapshot for generation {}",
                    start.generation.as_str()
                ))
            })?;

        let object_key = self.manifest_wal_pack_key(start, end);
        self.write_immutable_object(&object_key, compressed_data.clone())
            .await?;

        let pack = ManifestWalPack {
            start_lsn: start.offset,
            end_lsn: end.offset,
            object_key,
            sha256: Self::latest_pointer_revision(&compressed_data),
            size_bytes: uncompressed_size_bytes as u64,
            lineage_id: manifest.lineage_id.clone(),
            base_snapshot_id: manifest.base_snapshot_id.clone(),
        };
        if !manifest.wal_packs.iter().any(|existing| {
            existing.object_key == pack.object_key
                && existing.start_lsn == pack.start_lsn
                && existing.end_lsn == pack.end_lsn
                && existing.base_snapshot_id == pack.base_snapshot_id
        }) {
            manifest.wal_packs.push(pack);
        }
        manifest.manifest_id = Self::next_manifest_id(manifest.wal_packs.len());

        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|err| Error::StorageError(format!("serialize generation manifest: {err}")))?;
        self.write_generation_manifest(&manifest).await?;
        self.publish_latest_pointer_for_manifest(start.generation.as_str(), &manifest_bytes)
            .await?;
        Ok(())
    }

    pub(crate) async fn manifest_rollover_required(
        &self,
        generation: &Generation,
        pack_threshold: usize,
        manifest_size_threshold_bytes: usize,
    ) -> Result<bool> {
        let Some((pointer, _guard)) = self.read_latest_pointer().await? else {
            return Ok(false);
        };
        if pointer.current_generation != generation.as_str() {
            return Ok(false);
        }

        let Some(manifest) = self.read_generation_manifest(generation.as_str()).await? else {
            return Ok(false);
        };

        let manifest_size_bytes = serde_json::to_vec(&manifest)
            .map_err(|err| Error::StorageError(format!("serialize generation manifest: {err}")))?
            .len();

        Ok(manifest.wal_packs.len() >= pack_threshold
            || manifest_size_bytes >= manifest_size_threshold_bytes)
    }

    pub(crate) async fn purge_manifest_generation(&self, generation: &str) -> Result<()> {
        if !self.supports_manifest_publish() {
            return Err(Error::InvalidConfig(
                "manual purge only supports manifest-backed fs storage".to_string(),
            ));
        }

        if let Some((pointer, _guard)) = self.read_latest_pointer().await?
            && pointer.current_generation == generation
        {
            return Err(Error::StorageError(format!(
                "refusing to purge current visible generation {generation}"
            )));
        }

        let root = self.manifest_publish_root()?;
        let manifest = self.read_generation_manifest(generation).await?;
        let generation_dir = root.join(format!("{}/generations/{generation}", self.db_name));
        let manifest_path = root.join(self.generation_manifest_key(generation));
        let mut removed_any = false;

        if let Some(manifest) = manifest {
            removed_any |= Self::delete_fs_object_if_exists(&root, &manifest.base_snapshot)?;
            for pack in &manifest.wal_packs {
                removed_any |= Self::delete_fs_object_if_exists(&root, &pack.object_key)?;
            }
        }

        if std::fs::exists(&generation_dir)? {
            std::fs::remove_dir_all(&generation_dir).map_err(|err| {
                Error::StorageError(format!(
                    "remove generation directory {}: {err}",
                    generation_dir.display()
                ))
            })?;
            removed_any = true;
        }

        if std::fs::exists(&manifest_path)? {
            std::fs::remove_file(&manifest_path).map_err(|err| {
                Error::StorageError(format!(
                    "remove generation manifest {}: {err}",
                    manifest_path.display()
                ))
            })?;
            removed_any = true;
        }

        if !removed_any {
            return Err(Error::StorageError(format!(
                "generation {generation} not found in manifest-backed fs storage"
            )));
        }

        Ok(())
    }

    async fn ensure_parent_exist(&self, path: &str) -> Result<()> {
        let parent = parent_dir(path).unwrap_or_else(|| ".".to_string());
        let base = if parent.is_empty() || parent == "." {
            "./".to_string()
        } else {
            format!("{parent}/")
        };

        let mut exist = false;
        match self.operator.is_exist(&base).await {
            Err(e) => {
                debug!("check path {} parent_dir error: {}", path, e.kind())
            }
            Ok(r) => {
                exist = r;
            }
        }

        if !exist {
            debug!("create dir {base}");
            self.operator.create_dir(&base).await?;
        }

        Ok(())
    }

    pub async fn write_wal_segment(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        let file = walsegment_file(
            &self.db_name,
            pos.generation.as_str(),
            pos.index,
            pos.offset,
        );
        let temp_file = format!("{file}.tmp");

        self.ensure_parent_exist(&file).await?;

        self.operator.write(&temp_file, compressed_data).await?;
        self.operator.rename(&temp_file, &file).await?;

        Ok(())
    }

    pub async fn write_snapshot(
        &self,
        pos: &WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<SnapshotInfo> {
        let snapshot_file = snapshot_file(
            &self.db_name,
            pos.generation.as_str(),
            pos.index,
            pos.offset,
        );
        let temp_file = format!("{snapshot_file}.tmp");
        let snapshot_info = SnapshotInfo {
            generation: pos.generation.clone(),
            index: pos.index,
            offset: pos.offset,
            size: compressed_data.len() as u64,
            created_at: Utc::now(),
        };

        self.ensure_parent_exist(&snapshot_file).await?;

        self.operator.write(&temp_file, compressed_data).await?;
        self.operator.rename(&temp_file, &snapshot_file).await?;

        Ok(snapshot_info)
    }

    pub(crate) async fn read_object_by_key_checked(
        &self,
        key: &str,
        expected_sha256: Option<&str>,
    ) -> Result<Vec<u8>> {
        if let Some(cache) = &self.cache {
            match cache.get(key)? {
                Some(bytes) => {
                    if let Some(expected) = expected_sha256 {
                        if Self::latest_pointer_revision(&bytes) == expected {
                            self.restore_request_costs.record_cache_hit();
                            return Ok(bytes);
                        }
                        warn!("cache checksum mismatch for {key}, treating as miss");
                        cache.remove(key)?;
                    } else {
                        self.restore_request_costs.record_cache_hit();
                        return Ok(bytes);
                    }
                    self.restore_request_costs.record_cache_miss();
                }
                None => {
                    self.restore_request_costs.record_cache_miss();
                }
            }
        }

        self.restore_request_costs.record_object_get();
        let data = self.operator.read(key).await?;
        let bytes = data.to_vec();
        if let Some(expected) = expected_sha256 {
            let actual = Self::latest_pointer_revision(&bytes);
            if actual != expected {
                return Err(Error::StorageError(format!(
                    "object checksum mismatch for {key}: expected {expected}, got {actual}"
                )));
            }
        }
        if let Some(cache) = &self.cache
            && let Err(err) = cache.put(key, &bytes)
        {
            self.restore_request_costs.record_cache_write_failure();
            warn!("cache write failed for {key}: {err}");
        }
        Ok(bytes)
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_name, generation.as_str());
        self.restore_request_costs.record_list_call();
        let entries = match self
            .operator
            .list_with(&snapshots_dir)
            .metakey(Metakey::ContentLength)
            .metakey(Metakey::LastModified)
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                debug!(
                    "list snapshots {:?} error kind: {:?}",
                    generation,
                    e.to_string()
                );
                if e.kind() == opendal::ErrorKind::NotFound
                    || e.kind() == opendal::ErrorKind::NotADirectory
                {
                    return Ok(vec![]);
                } else {
                    return Err(e.into());
                }
            }
        };

        let mut snapshots = vec![];
        for entry in entries {
            let metadata = entry.metadata();
            if !metadata.is_file() {
                continue;
            }
            let (index, offset) = match parse_snapshot_path(entry.name()) {
                Ok(v) => v,
                Err(e) if e.code() == Error::INVALID_PATH => continue,
                Err(e) => return Err(e),
            };
            snapshots.push(SnapshotInfo {
                generation: generation.clone(),
                index,
                offset,
                size: metadata.content_length(),
                created_at: metadata_last_modified_or_epoch(metadata, entry.name()),
            })
        }

        Ok(snapshots)
    }

    pub async fn wal_segments(&self, generation: &str) -> Result<Vec<WalSegmentInfo>> {
        let generation = Generation::try_create(generation)?;
        let walsegments_dir = walsegments_dir(&self.db_name, generation.as_str());
        self.restore_request_costs.record_list_call();
        let entries = self
            .operator
            .list_with(&walsegments_dir)
            .metakey(Metakey::ContentLength)
            .metakey(Metakey::LastModified)
            .await?;

        let mut wal_segments = vec![];
        for entry in entries {
            let metadata = entry.metadata();
            if !metadata.is_file() {
                continue;
            }
            let (index, offset) = match parse_wal_segment_path(entry.name()) {
                Ok(v) => v,
                Err(e) if e.code() == Error::INVALID_PATH => continue,
                Err(e) => return Err(e),
            };
            wal_segments.push(WalSegmentInfo {
                generation: generation.clone(),
                index,
                offset,
                size: entry.metadata().content_length(),
                created_at: metadata_last_modified_or_epoch(entry.metadata(), entry.name()),
            })
        }

        Ok(wal_segments)
    }

    pub async fn read_wal_segment(&self, info: &WalSegmentInfo) -> Result<Vec<u8>> {
        self.restore_request_costs.record_object_get();
        let generation = &info.generation;
        let index = info.index;
        let offset = info.offset;

        let wal_segment_file = walsegment_file(&self.db_name, generation.as_str(), index, offset);
        let bytes = self.operator.read(&wal_segment_file).await?.to_vec();
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::LatestPointerUpdatePolicy;
    use super::RestoreRequestCostSnapshot;
    use super::RestoreRequestCostStats;
    use super::StorageClient;
    use super::snapshot_position_is_newer;
    use crate::base::Generation;
    use crate::base::compress_buffer;
    use crate::base::snapshot_file;
    use crate::base::walsegment_file;
    use crate::config::StorageConfig;
    use crate::config::StorageFsConfig;
    use crate::config::StorageParams;
    use crate::database::WalGenerationPos;
    use crate::error::Error;
    use crate::storage::manifest::GenerationManifest;
    use crate::storage::manifest::LatestPointer;
    use crate::storage::manifest::ManifestWalPack;
    use crate::storage::{DEFAULT_CACHE_SIZE_LIMIT_BYTES, LocalObjectCache};
    use chrono::Utc;
    use opendal::Operator;
    use opendal::raw::Access;
    use opendal::raw::Layer;
    use opendal::raw::LayeredAccess;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpWrite;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpWrite;
    use opendal::services;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    #[test]
    fn snapshot_position_prefers_higher_index() {
        assert!(snapshot_position_is_newer(11, 0, Some((10, 9999))));
    }

    #[test]
    fn snapshot_position_breaks_tie_with_offset() {
        assert!(snapshot_position_is_newer(10, 200, Some((10, 100))));
        assert!(!snapshot_position_is_newer(10, 100, Some((10, 200))));
    }

    #[derive(Debug, Clone)]
    struct FailListLayer {
        kind: opendal::ErrorKind,
    }

    impl FailListLayer {
        fn new(kind: opendal::ErrorKind) -> Self {
            Self { kind }
        }
    }

    #[derive(Debug)]
    struct FailListAccessor<A> {
        inner: A,
        kind: opendal::ErrorKind,
    }

    impl<A: Access> Layer<A> for FailListLayer {
        type LayeredAccess = FailListAccessor<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            FailListAccessor {
                inner,
                kind: self.kind,
            }
        }
    }

    impl<A: Access> LayeredAccess for FailListAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type BlockingReader = A::BlockingReader;
        type Writer = A::Writer;
        type BlockingWriter = A::BlockingWriter;
        type Lister = A::Lister;
        type BlockingLister = A::BlockingLister;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            self.inner.read(path, args).await
        }

        fn blocking_read(
            &self,
            path: &str,
            args: OpRead,
        ) -> opendal::Result<(RpRead, Self::BlockingReader)> {
            self.inner.blocking_read(path, args)
        }

        async fn write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::Writer)> {
            self.inner.write(path, args).await
        }

        fn blocking_write(
            &self,
            path: &str,
            args: OpWrite,
        ) -> opendal::Result<(RpWrite, Self::BlockingWriter)> {
            self.inner.blocking_write(path, args)
        }

        async fn list(
            &self,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<(RpList, Self::Lister)> {
            Err(opendal::Error::new(self.kind, "fail list"))
        }

        fn blocking_list(
            &self,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<(RpList, Self::BlockingLister)> {
            Err(opendal::Error::new(self.kind, "fail list"))
        }
    }

    fn storage_client_with_list_error(kind: opendal::ErrorKind) -> StorageClient {
        let op = Operator::new(services::Memory::default())
            .expect("op")
            .layer(FailListLayer::new(kind))
            .finish();
        StorageClient {
            operator: op,
            db_name: "db.db".to_string(),
            latest_pointer_update_policy: LatestPointerUpdatePolicy::Unsupported {
                backend: "memory".to_string(),
            },
            restore_request_costs: RestoreRequestCostStats::default(),
            cache: None,
        }
    }

    fn memory_storage_client() -> StorageClient {
        StorageClient {
            operator: Operator::new(services::Memory::default())
                .expect("op")
                .finish(),
            db_name: "db.db".to_string(),
            latest_pointer_update_policy: LatestPointerUpdatePolicy::Unsupported {
                backend: "memory".to_string(),
            },
            restore_request_costs: RestoreRequestCostStats::default(),
            cache: None,
        }
    }

    fn single_writer_memory_storage_client(cache: Option<LocalObjectCache>) -> StorageClient {
        StorageClient {
            operator: Operator::new(services::Memory::default())
                .expect("op")
                .finish(),
            db_name: "db.db".to_string(),
            latest_pointer_update_policy: LatestPointerUpdatePolicy::SingleWriterUnconditional {
                backend: "s3".to_string(),
            },
            restore_request_costs: RestoreRequestCostStats::default(),
            cache,
        }
    }

    fn fs_storage_client(root: &std::path::Path) -> StorageClient {
        StorageClient::try_create(
            "db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string_lossy().to_string(),
                })),
            },
        )
        .expect("storage client")
    }

    fn sample_latest_pointer(generation: &str) -> LatestPointer {
        LatestPointer {
            format_version: 1,
            current_generation: generation.to_string(),
            current_manifest_key: format!("db.db/manifests/generations/{generation}.manifest.json"),
            current_manifest_sha256: "abc123".to_string(),
            created_at: Utc::now(),
        }
    }

    fn sample_generation_manifest(generation: &str) -> GenerationManifest {
        GenerationManifest {
            format_version: 1,
            generation: generation.to_string(),
            manifest_id: "manifest-01".to_string(),
            lineage_id: "lineage-01".to_string(),
            base_snapshot_id: "snapshot-01".to_string(),
            base_snapshot_sha256: "deadbeef".to_string(),
            base_snapshot: format!("db.db/snapshots/{generation}/snapshot-01.snapshot.zst"),
            wal_packs: vec![ManifestWalPack {
                start_lsn: 0,
                end_lsn: 4096,
                object_key: format!("db.db/wal/packs/{generation}/0_4096_pack-01.wal.zst"),
                sha256: "cafebabe".to_string(),
                size_bytes: 8192,
                lineage_id: "lineage-01".to_string(),
                base_snapshot_id: "snapshot-01".to_string(),
            }],
        }
    }

    #[tokio::test]
    async fn snapshots_treats_not_found_as_empty() {
        let client = storage_client_with_list_error(opendal::ErrorKind::NotFound);
        let generation = Generation::new();

        let snapshots = client
            .snapshots(generation.as_str())
            .await
            .expect("snapshots");
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn latest_pointer_round_trips_without_directory_listing() {
        let client = storage_client_with_list_error(opendal::ErrorKind::Unexpected);
        let pointer = sample_latest_pointer("gen-01");

        client
            .write_latest_pointer(&pointer)
            .await
            .expect("write latest pointer");

        let (decoded, _guard) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");

        assert_eq!(decoded, pointer);
    }

    #[tokio::test]
    async fn generation_manifest_round_trips_without_directory_listing() {
        let client = storage_client_with_list_error(opendal::ErrorKind::Unexpected);
        let manifest = sample_generation_manifest("gen-01");

        client
            .write_generation_manifest(&manifest)
            .await
            .expect("write generation manifest");

        let decoded = client
            .read_generation_manifest(&manifest.generation)
            .await
            .expect("read generation manifest")
            .expect("generation manifest");

        assert_eq!(decoded, manifest);
    }

    #[tokio::test]
    async fn conditional_latest_pointer_updates_on_fs_backend_when_guard_matches() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);

        let initial = sample_latest_pointer("gen-01");
        client
            .write_latest_pointer(&initial)
            .await
            .expect("write initial pointer");
        let (_current, guard) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");

        let updated = sample_latest_pointer("gen-02");
        client
            .write_latest_pointer_conditional(&updated, Some(&guard))
            .await
            .expect("conditional latest pointer update");

        let (decoded, _) = client
            .read_latest_pointer()
            .await
            .expect("read updated latest pointer")
            .expect("updated latest pointer");
        assert_eq!(decoded, updated);
    }

    #[tokio::test]
    async fn conditional_latest_pointer_rejects_stale_guard_on_fs_backend() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);

        client
            .write_latest_pointer(&sample_latest_pointer("gen-01"))
            .await
            .expect("write initial pointer");
        let (_current, stale_guard) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");

        client
            .write_latest_pointer(&sample_latest_pointer("gen-02"))
            .await
            .expect("overwrite latest pointer");

        let err = client
            .write_latest_pointer_conditional(&sample_latest_pointer("gen-03"), Some(&stale_guard))
            .await
            .expect_err("stale guard should be rejected");

        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn conditional_latest_pointer_is_explicitly_unsupported_on_unsupported_backend() {
        let client = memory_storage_client();
        let err = client
            .write_latest_pointer_conditional(&sample_latest_pointer("gen-01"), None)
            .await
            .expect_err("unsupported backend should not pretend conditional safety");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn snapshots_propagates_unexpected_list_error() {
        let client = storage_client_with_list_error(opendal::ErrorKind::Unexpected);
        let generation = Generation::new();

        let err = client
            .snapshots(generation.as_str())
            .await
            .expect_err("snapshots should error");
        assert_eq!(err.code(), Error::OPEN_DAL_ERROR);
    }

    #[tokio::test]
    async fn write_wal_segment_uses_db_name_not_full_db_path() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");

        let client = StorageClient::try_create(
            "nested/db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string_lossy().to_string(),
                })),
            },
        )
        .expect("storage client");

        let generation = Generation::new();
        let pos = crate::database::WalGenerationPos {
            generation: generation.clone(),
            index: 0,
            offset: 0,
        };

        client
            .write_wal_segment(
                &pos,
                crate::base::compress_buffer(b"segment").expect("compress"),
            )
            .await
            .expect("write wal segment");

        let good = root.join(walsegment_file("db.db", generation.as_str(), 0, 0));
        let bad = root.join(walsegment_file("nested/db.db", generation.as_str(), 0, 0));
        assert!(
            std::fs::exists(&good).expect("exists"),
            "wal segment should be written under db name path, expected {}",
            good.to_string_lossy()
        );
        assert!(
            !std::fs::exists(&bad).expect("exists"),
            "wal segment should not be written under full db_path, found {}",
            bad.to_string_lossy()
        );
    }

    #[tokio::test]
    async fn wal_segments_ignores_tmp_files() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");

        let client = StorageClient::try_create(
            "db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string_lossy().to_string(),
                })),
            },
        )
        .expect("storage client");

        let generation = Generation::new();
        let pos = WalGenerationPos {
            generation: generation.clone(),
            index: 15,
            offset: 634512,
        };

        client
            .write_wal_segment(&pos, compress_buffer(b"segment").expect("compress"))
            .await
            .expect("write wal segment");

        // Simulate transient/incomplete upload: `<segment>.tmp`.
        let segment_rel = walsegment_file("db.db", generation.as_str(), pos.index, pos.offset);
        std::fs::write(root.join(format!("{segment_rel}.tmp")), b"incomplete").expect("write tmp");

        // Before fix: this used to error with InvalidPath when listing WAL segments.
        let segments = client
            .wal_segments(generation.as_str())
            .await
            .expect("wal_segments");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].index, 15);
        assert_eq!(segments[0].offset, 634512);
    }

    #[tokio::test]
    async fn snapshots_ignores_tmp_files() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");

        let client = StorageClient::try_create(
            "db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string_lossy().to_string(),
                })),
            },
        )
        .expect("storage client");

        let generation = Generation::new();
        let pos = WalGenerationPos {
            generation: generation.clone(),
            index: 7,
            offset: 1234,
        };

        client
            .write_snapshot(&pos, compress_buffer(b"snapshot").expect("compress"))
            .await
            .expect("write snapshot");

        // Simulate transient/incomplete upload: `<snapshot>.tmp`.
        let snap_rel = snapshot_file("db.db", generation.as_str(), pos.index, pos.offset);
        std::fs::write(root.join(format!("{snap_rel}.tmp")), b"incomplete").expect("write tmp");

        // Before fix: this used to error with InvalidPath when listing snapshots.
        let snaps = client
            .snapshots(generation.as_str())
            .await
            .expect("snapshots");
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].index, 7);
        assert_eq!(snaps[0].offset, 1234);
    }

    #[tokio::test]
    async fn direct_publish_snapshot_writes_final_object_and_updates_visible_manifest() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let generation = Generation::new();
        let pos = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4096,
        };

        client
            .publish_manifest_snapshot(&pos, compress_buffer(b"snapshot").expect("compress"))
            .await
            .expect("publish manifest snapshot");

        let snapshot_rel = snapshot_file("db.db", generation.as_str(), pos.index, pos.offset);
        let snapshot_path = root.join(&snapshot_rel);
        assert!(std::fs::exists(&snapshot_path).expect("exists"));
        assert!(!std::fs::exists(root.join(format!("{snapshot_rel}.tmp"))).expect("exists"));

        let (
            generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            base_snapshot,
            wal_packs,
        ) = client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest metadata");
        assert_eq!(generation_id, generation.as_str());
        assert_eq!(base_snapshot, snapshot_rel);
        assert!(wal_packs.is_empty());

        let (pointer, _) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");
        let manifest_path = root.join(pointer.current_manifest_key);
        assert!(std::fs::exists(&manifest_path).expect("manifest exists"));
    }

    #[tokio::test]
    async fn direct_publish_manifest_path_is_explicitly_unsupported_on_memory_backend() {
        let client = memory_storage_client();
        let generation = Generation::new();
        let err = client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation,
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress"),
            )
            .await
            .expect_err("unsupported backend should reject manifest publish");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn single_writer_object_backend_can_publish_manifest_snapshot() {
        let client = single_writer_memory_storage_client(None);
        let generation = Generation::new();

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("single-writer publish snapshot");

        let (pointer, _guard) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");
        assert_eq!(pointer.current_generation, generation.as_str());
    }

    #[tokio::test]
    async fn cache_first_read_hits_cache_and_skips_remote_get() {
        let temp = tempdir().expect("tempdir");
        let cache = LocalObjectCache::try_create(
            temp.path().join("cache"),
            DEFAULT_CACHE_SIZE_LIMIT_BYTES,
        )
        .expect("cache");
        let client = single_writer_memory_storage_client(Some(cache.clone()));
        let payload = b"hello-cache".to_vec();
        let sha = format!("{:x}", Sha256::digest(&payload));
        cache.put("db.db/objects/item", &payload).expect("seed cache");

        let bytes = client
            .read_object_by_key_checked("db.db/objects/item", Some(&sha))
            .await
            .expect("cache read");

        assert_eq!(bytes, payload);
        assert_eq!(client.restore_request_costs.snapshot().cache_hits, 1);
        assert_eq!(client.restore_request_costs.snapshot().object_gets, 0);
    }

    #[tokio::test]
    async fn cache_first_read_populates_cache_on_miss() {
        let temp = tempdir().expect("tempdir");
        let cache = LocalObjectCache::try_create(
            temp.path().join("cache"),
            DEFAULT_CACHE_SIZE_LIMIT_BYTES,
        )
        .expect("cache");
        let client = single_writer_memory_storage_client(Some(cache.clone()));
        let payload = b"remote-object".to_vec();
        let sha = format!("{:x}", Sha256::digest(&payload));
        client
            .operator
            .write("db.db/objects/item", payload.clone())
            .await
            .expect("seed remote object");

        let bytes = client
            .read_object_by_key_checked("db.db/objects/item", Some(&sha))
            .await
            .expect("remote read");

        assert_eq!(bytes, payload);
        assert_eq!(
            cache.get("db.db/objects/item").expect("cache get"),
            Some(b"remote-object".to_vec())
        );
        assert_eq!(client.restore_request_costs.snapshot().cache_misses, 1);
        assert_eq!(client.restore_request_costs.snapshot().object_gets, 1);
    }

    #[tokio::test]
    async fn cache_first_read_rejects_remote_checksum_mismatch() {
        let temp = tempdir().expect("tempdir");
        let cache = LocalObjectCache::try_create(
            temp.path().join("cache"),
            DEFAULT_CACHE_SIZE_LIMIT_BYTES,
        )
        .expect("cache");
        let client = single_writer_memory_storage_client(Some(cache));
        client
            .operator
            .write("db.db/objects/item", b"wrong".to_vec())
            .await
            .expect("seed remote object");

        let err = client
            .read_object_by_key_checked("db.db/objects/item", Some("deadbeef"))
            .await
            .expect_err("checksum mismatch should fail");

        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn manifest_wal_pack_key_includes_end_boundary_via_unique_range_directory() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let generation = Generation::new();
        let start = WalGenerationPos {
            generation: generation.clone(),
            index: 7,
            offset: 4096,
        };
        let end = WalGenerationPos {
            generation,
            index: 7,
            offset: 8192,
        };

        let key = client.manifest_wal_pack_key(&start, &end);

        assert!(key.contains("/ranges/"));
        assert!(key.contains("0000000007_0000004096_0000000007_0000008192"));
        assert!(key.ends_with("0000000007_0000004096.wal.zst"));
    }

    #[tokio::test]
    async fn concurrent_manifest_pack_publishes_on_fs_do_not_lose_appends() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let generation = Generation::new();
        let snapshot_pos = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4096,
        };
        client
            .publish_manifest_snapshot(
                &snapshot_pos,
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish manifest snapshot");

        let first_start = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4096,
        };
        let first_end = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 6144,
        };
        let second_start = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 6144,
        };
        let second_end = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 8192,
        };

        let first = client.publish_manifest_wal_pack(
            &first_start,
            &first_end,
            compress_buffer(b"first-pack").expect("compress first"),
            b"first-pack".len(),
        );
        let second = client.publish_manifest_wal_pack(
            &second_start,
            &second_end,
            compress_buffer(b"second-pack").expect("compress second"),
            b"second-pack".len(),
        );

        let (first_result, second_result) = tokio::join!(first, second);
        first_result.expect("first publish");
        second_result.expect("second publish");

        let (
            _generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            _base_snapshot,
            wal_packs,
        ) = client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest metadata");

        assert_eq!(wal_packs.len(), 2);
        assert_eq!(wal_packs[0].0, 4096);
        assert_eq!(wal_packs[0].1, 6144);
        assert_eq!(wal_packs[1].0, 6144);
        assert_eq!(wal_packs[1].1, 8192);
    }

    #[tokio::test]
    async fn publish_manifest_snapshot_rejects_latest_pointer_regression_to_older_generation() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let older_generation = Generation::new();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let newer_generation = Generation::new();

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: newer_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"newer-snapshot").expect("compress newer snapshot"),
            )
            .await
            .expect("publish newer snapshot");

        let err = client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: older_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"older-snapshot").expect("compress older snapshot"),
            )
            .await
            .expect_err("older generation should not overwrite latest pointer");

        assert_eq!(err.code(), Error::STORAGE_ERROR);

        let (pointer, _) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");
        assert_eq!(pointer.current_generation, newer_generation.as_str());
    }

    #[tokio::test]
    async fn purge_manifest_generation_rejects_current_visible_generation() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let generation = Generation::new();

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        let err = client
            .purge_manifest_generation(generation.as_str())
            .await
            .expect_err("current visible generation must not be purged");
        assert_eq!(err.code(), Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn purge_manifest_generation_removes_non_current_generation_on_fs() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let older_generation = Generation::new();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let newer_generation = Generation::new();

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: older_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"older snapshot").expect("compress older snapshot"),
            )
            .await
            .expect("publish older snapshot");

        let older_snapshot_key = snapshot_file("db.db", older_generation.as_str(), 1, 4096);
        client
            .publish_manifest_wal_pack(
                &WalGenerationPos {
                    generation: older_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                &WalGenerationPos {
                    generation: older_generation.clone(),
                    index: 1,
                    offset: 8192,
                },
                compress_buffer(b"older wal pack").expect("compress wal pack"),
                b"older wal pack".len(),
            )
            .await
            .expect("publish wal pack");
        let older_pack_key = client.manifest_wal_pack_key(
            &WalGenerationPos {
                generation: older_generation.clone(),
                index: 1,
                offset: 4096,
            },
            &WalGenerationPos {
                generation: older_generation.clone(),
                index: 1,
                offset: 8192,
            },
        );

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: newer_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"newer snapshot").expect("compress newer snapshot"),
            )
            .await
            .expect("publish newer snapshot");

        client
            .purge_manifest_generation(older_generation.as_str())
            .await
            .expect("purge non-current generation");

        let older_manifest_path =
            root.join(client.generation_manifest_key(older_generation.as_str()));
        let older_generation_dir = root.join(format!(
            "{}/generations/{}",
            client.db_name,
            older_generation.as_str()
        ));
        let older_snapshot_path = root.join(&older_snapshot_key);
        let older_pack_path = root.join(&older_pack_key);
        assert!(!std::fs::exists(&older_manifest_path).expect("manifest exists check"));
        assert!(!std::fs::exists(&older_generation_dir).expect("generation dir exists check"));
        assert!(!std::fs::exists(&older_snapshot_path).expect("snapshot exists check"));
        assert!(!std::fs::exists(&older_pack_path).expect("pack exists check"));

        let (pointer, _) = client
            .read_latest_pointer()
            .await
            .expect("read latest pointer")
            .expect("latest pointer");
        assert_eq!(pointer.current_generation, newer_generation.as_str());
    }

    #[tokio::test]
    async fn purge_manifest_generation_fails_clearly_on_unsupported_backend() {
        let client = memory_storage_client();
        let err = client
            .purge_manifest_generation(Generation::new().as_str())
            .await
            .expect_err("unsupported backend should fail clearly");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn manifest_rollover_required_checks_manifest_size_threshold() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let client = fs_storage_client(&root);
        let generation = Generation::new();

        client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        let manifest = client
            .read_generation_manifest(generation.as_str())
            .await
            .expect("read manifest")
            .expect("manifest");
        let manifest_size = serde_json::to_vec(&manifest)
            .expect("serialize manifest")
            .len();

        assert!(
            client
                .manifest_rollover_required(&generation, usize::MAX, manifest_size)
                .await
                .expect("check threshold")
        );
        assert!(
            !client
                .manifest_rollover_required(&generation, usize::MAX, manifest_size + 1)
                .await
                .expect("check threshold")
        );
    }

    #[tokio::test]
    async fn restore_request_cost_stats_count_discovery_and_reads() {
        let temp = tempdir().expect("tempdir");
        let root = temp.path().join("storage");
        let generation = Generation::new();
        let setup_client = fs_storage_client(&root);
        let stats = RestoreRequestCostStats::default();
        let client = StorageClient::try_create_with_restore_request_costs(
            "db.db".to_string(),
            StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: root.to_string_lossy().to_string(),
                })),
            },
            stats.clone(),
        )
        .expect("storage client");

        setup_client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 7,
                    offset: 0,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        setup_client
            .publish_manifest_wal_pack(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 7,
                    offset: 0,
                },
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 7,
                    offset: 3,
                },
                compress_buffer(b"wal").expect("compress wal"),
                3,
            )
            .await
            .expect("publish wal pack");

        let metadata = client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest inputs")
            .expect("manifest metadata");
        let snapshot_key = metadata.5;
        let wal_key = metadata.6.first().expect("wal pack").2.clone();

        client
            .read_object_by_key_checked(&snapshot_key, None)
            .await
            .expect("read snapshot");
        client
            .read_object_by_key_checked(&wal_key, None)
            .await
            .expect("read wal pack");

        assert_eq!(
            stats.snapshot(),
            RestoreRequestCostSnapshot {
                latest_pointer_gets: 1,
                generation_manifest_gets: 1,
                object_gets: 2,
                list_calls: 0,
                cache_hits: 0,
                cache_misses: 0,
                cache_write_failures: 0,
            }
        );
    }
}

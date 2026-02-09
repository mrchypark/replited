use std::collections::BTreeMap;
use std::time::SystemTime;

use chrono::DateTime;
use chrono::Utc;
use log::debug;
use log::error;
use log::warn;
use opendal::Metadata;
use opendal::Metakey;
use opendal::Operator;

use super::init_operator;
use crate::base::Generation;
use crate::base::parent_dir;
use crate::base::parse_snapshot_path;
use crate::base::parse_wal_segment_path;
use crate::base::path_base;
use crate::base::remote_generations_dir;
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

// restore wal_segments formats: vector<index, vector<offsets in order>>
pub type RestoreWalSegments = Vec<(u64, Vec<u64>)>;

#[derive(Debug)]
pub struct RestoreInfo {
    pub snapshot: SnapshotInfo,

    pub wal_segments: RestoreWalSegments,
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

fn snapshot_position_is_newer(index: u64, offset: u64, current: Option<(u64, u64)>) -> bool {
    match current {
        None => true,
        Some((current_index, current_offset)) => {
            index > current_index || (index == current_index && offset > current_offset)
        }
    }
}

impl StorageClient {
    pub fn try_create(db_path: String, config: StorageConfig) -> Result<Self> {
        Ok(Self {
            operator: init_operator(&config.params)?,
            db_name: path_base(&db_path)?,
        })
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

    pub async fn read_snapshot(&self, info: &SnapshotInfo) -> Result<Vec<u8>> {
        let snapshot_file = snapshot_file(
            &self.db_name,
            info.generation.as_str(),
            info.index,
            info.offset,
        );

        let data = self.operator.read(&snapshot_file).await?;

        Ok(data.to_vec())
    }

    pub async fn snapshots(&self, generation: &str) -> Result<Vec<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_name, generation.as_str());
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

    async fn max_snapshot(&self, generation: &str) -> Result<Option<SnapshotInfo>> {
        let generation = Generation::try_create(generation)?;
        let snapshots_dir = snapshots_dir(&self.db_name, generation.as_str());
        debug!("max_snapshot: listing snapshots in {snapshots_dir}");
        let entries = self
            .operator
            .list_with(&snapshots_dir)
            .metakey(Metakey::ContentLength)
            .metakey(Metakey::LastModified)
            .await?;
        debug!("max_snapshot: found {} entries", entries.len());

        let mut snapshot = None;
        let mut max_position: Option<(u64, u64)> = None;
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
            if !snapshot_position_is_newer(index, offset, max_position) {
                continue;
            }

            max_position = Some((index, offset));
            snapshot = Some(SnapshotInfo {
                generation: generation.clone(),
                index,
                offset,
                size: metadata.content_length(),
                created_at: metadata_last_modified_or_epoch(metadata, entry.name()),
            });
        }

        Ok(snapshot)
    }

    pub async fn wal_segments(&self, generation: &str) -> Result<Vec<WalSegmentInfo>> {
        let generation = Generation::try_create(generation)?;
        let walsegments_dir = walsegments_dir(&self.db_name, generation.as_str());
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
        let generation = &info.generation;
        let index = info.index;
        let offset = info.offset;

        let wal_segment_file = walsegment_file(&self.db_name, generation.as_str(), index, offset);
        let bytes = self.operator.read(&wal_segment_file).await?.to_vec();
        Ok(bytes)
    }

    async fn restore_wal_segments_of(
        &self,
        snapshot: &SnapshotInfo,
        limit: Option<DateTime<Utc>>,
    ) -> Result<RestoreWalSegments> {
        let mut wal_segments = self.wal_segments(snapshot.generation.as_str()).await?;

        // filter wal segments by limit
        if let Some(limit) = limit {
            wal_segments.retain(|segment| segment.created_at <= limit);
        }

        // sort wal segments first by index, then offset
        wal_segments.sort_by(|a, b| a.index.cmp(&b.index).then(a.offset.cmp(&b.offset)));

        let mut restore_wal_segments: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

        for wal_segment in wal_segments {
            if wal_segment.index < snapshot.index {
                continue;
            }

            match restore_wal_segments.get_mut(&wal_segment.index) {
                Some(offsets) => {
                    if let Some(last_offset) = offsets.last()
                        && *last_offset >= wal_segment.offset
                    {
                        let msg = format!(
                            "wal segment out of order, generation: {:?}, index: {}, offset: {}",
                            snapshot.generation.as_str(),
                            wal_segment.index,
                            wal_segment.offset
                        );
                        error!("{msg}");
                        return Err(Error::InvalidWalSegmentError(msg));
                    }
                    offsets.push(wal_segment.offset);
                }
                None => {
                    if wal_segment.offset != 0 {
                        let msg = format!(
                            "missing initial wal segment, generation: {:?}, index: {}, offset: {}",
                            snapshot.generation.as_str(),
                            wal_segment.index,
                            wal_segment.offset
                        );
                        error!("{msg}");
                        return Err(Error::InvalidWalSegmentError(msg));
                    }
                    restore_wal_segments.insert(wal_segment.index, vec![wal_segment.offset]);
                }
            }
        }

        Ok(restore_wal_segments.into_iter().collect())
    }

    pub async fn restore_info(&self, limit: Option<DateTime<Utc>>) -> Result<Option<RestoreInfo>> {
        let dir = remote_generations_dir(&self.db_name);
        debug!("restore_info: listing generations in {dir}");
        let entries = self.operator.list(&dir).await?;
        debug!("restore_info: found {} entries", entries.len());

        let mut entry_with_generation = Vec::with_capacity(entries.len());
        for entry in entries {
            let metadata = entry.metadata();
            if !metadata.is_dir() {
                continue;
            }
            let generation = path_base(entry.name())?;

            let generation = match Generation::try_create(&generation) {
                Ok(generation) => generation,
                Err(_e) => {
                    error!("dir {generation} is not valid generation dir",);
                    continue;
                }
            };

            entry_with_generation.push((entry, generation));
        }

        // sort the entries in reverse order
        entry_with_generation.sort_by(|a, b| b.1.as_str().cmp(a.1.as_str()));

        for (_entry, generation) in entry_with_generation {
            let snapshot = match self.max_snapshot(generation.as_str()).await? {
                Some(snapshot) => snapshot,
                // if generation has no snapshot, ignore and skip to the next generation
                None => {
                    error!("dir {generation:?} has no snapshots");
                    continue;
                }
            };

            // return only if wal segments in this snapshot is valid.
            if let Ok(wal_segments) = self.restore_wal_segments_of(&snapshot, limit).await {
                return Ok(Some(RestoreInfo {
                    snapshot,
                    wal_segments,
                }));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
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
}

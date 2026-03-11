use std::sync::Arc;
use std::time::{Duration, Instant};

use log::debug;
use log::error;
use log::info;
use parking_lot::RwLock;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::ShadowWalReader;
use crate::base::Generation;
use crate::base::compress_buffer;
use crate::base::decompressed_data;
use crate::config::StorageConfig;
use crate::database::DatabaseInfo;
use crate::database::DbCommand;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sqlite::align_frame;
use crate::storage::SnapshotInfo;
use crate::storage::StorageClient;
use crate::storage::WalSegmentInfo;
use crate::storage::{DEFAULT_CACHE_SIZE_LIMIT_BYTES, LocalObjectCache};

#[derive(Clone, Debug)]
pub enum ReplicateCommand {
    DbChanged(WalGenerationPos),
    Snapshot((WalGenerationPos, Vec<u8>)),
}

#[derive(Debug, Clone, PartialEq)]
enum ReplicateState {
    WaitDbChanged,
    WaitSnapshot,
}

fn snapshot_position_is_newer(index: u64, offset: u64, current: Option<(u64, u64)>) -> bool {
    match current {
        None => true,
        Some((current_index, current_offset)) => {
            index > current_index || (index == current_index && offset > current_offset)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Replicate {
    db: String,
    index: usize,
    client: StorageClient,
    db_notifier: Sender<DbCommand>,
    position: Arc<RwLock<WalGenerationPos>>,
    state: ReplicateState,
    wait_snapshot_started_at: Option<Instant>,
    info: DatabaseInfo,
    config: StorageConfig,
}

impl Replicate {
    pub(crate) const fn manifest_wal_pack_max_bytes() -> usize {
        8 * 1024 * 1024
    }

    pub(crate) const fn manifest_rollover_pack_threshold() -> usize {
        64
    }

    pub(crate) const fn manifest_rollover_manifest_max_bytes() -> usize {
        128 * 1024
    }

    pub(crate) const fn wait_snapshot_timeout() -> Duration {
        Duration::from_secs(30)
    }

    pub async fn new(
        config: StorageConfig,
        db: String,
        index: usize,
        db_notifier: Sender<DbCommand>,
        info: DatabaseInfo,
        cache_root: String,
    ) -> Result<Self> {
        let cache = LocalObjectCache::try_create(cache_root, DEFAULT_CACHE_SIZE_LIMIT_BYTES)?;
        Ok(Self {
            db: db.clone(),
            index,
            position: Arc::new(RwLock::new(WalGenerationPos::default())),
            db_notifier,
            client: StorageClient::try_create_with_cache(db, config.clone(), Some(cache))?,
            config,
            state: ReplicateState::WaitDbChanged,
            wait_snapshot_started_at: None,
            info,
        })
    }

    #[cfg(test)]
    pub async fn new_for_test(
        config: StorageConfig,
        db: String,
        index: usize,
        db_notifier: Sender<DbCommand>,
        info: DatabaseInfo,
    ) -> Result<Self> {
        let cache_root = std::path::Path::new(&info.meta_dir)
            .join("cache")
            .to_string_lossy()
            .to_string();
        Self::new(config, db, index, db_notifier, info, cache_root).await
    }

    pub fn start(s: Replicate, rx: Receiver<ReplicateCommand>) -> Result<JoinHandle<()>> {
        info!("start replicate {:?} of db {}", s.config, s.db);
        let s = s.clone();
        let handle = tokio::spawn(async move {
            let _ = Replicate::main(s, rx).await;
        });

        Ok(handle)
    }

    pub async fn main(s: Replicate, rx: Receiver<ReplicateCommand>) -> Result<()> {
        let mut rx = rx;
        let mut s = s;
        let mut pending = None;
        loop {
            let cmd = match pending.take() {
                Some(cmd) => Some(cmd),
                None => {
                    select! {
                        cmd = rx.recv() => cmd
                    }
                }
            };

            match cmd {
                Some(ReplicateCommand::DbChanged(pos)) => {
                    let (coalesced, next_pending) =
                        Self::coalesce_db_changed_notifications(&mut rx, pos).await;
                    pending = next_pending;
                    s.command(ReplicateCommand::DbChanged(coalesced)).await?;
                }
                Some(cmd) => {
                    s.command(cmd).await?;
                }
                None => {
                    break;
                }
            }
        }
        Ok(())
    }

    // returns the last snapshot in a generation.
    async fn max_snapshot(&self, generation: &str) -> Result<SnapshotInfo> {
        let snapshots = self.client.snapshots(generation).await?;
        if snapshots.is_empty() {
            return Err(Error::NoSnapshotError(generation));
        }
        let mut max_index = 0;
        let mut max_position: Option<(u64, u64)> = None;
        for (i, snapshot) in snapshots.iter().enumerate() {
            if snapshot_position_is_newer(snapshot.index, snapshot.offset, max_position) {
                max_position = Some((snapshot.index, snapshot.offset));
                max_index = i;
            }
        }

        Ok(snapshots[max_index].clone())
    }

    // returns the highest WAL segment in a generation.
    async fn max_wal_segment(&self, generation: &str) -> Result<WalSegmentInfo> {
        let wal_segments = self.client.wal_segments(generation).await?;
        if wal_segments.is_empty() {
            return Err(Error::NoWalsegmentError(generation));
        }
        let mut max_index = 0;
        let mut max_wg_index = 0;
        let mut max_wg_offset = 0;
        for (i, wg) in wal_segments.iter().enumerate() {
            if wg.index > max_wg_index {
                max_wg_index = wg.index;
                max_wg_offset = wg.offset;
                max_index = i;
            } else if wg.index == max_wg_index && wg.offset > max_wg_offset {
                max_wg_offset = wg.offset;
                max_index = i;
            }
        }

        Ok(wal_segments[max_index].clone())
    }

    async fn calculate_generation_position(&self, generation: &str) -> Result<WalGenerationPos> {
        // Fetch last snapshot. Return error if no snapshots exist.
        let snapshot = self.max_snapshot(generation).await?;
        let generation = Generation::try_create(generation)?;

        // Determine last WAL segment available.
        let segment = self.max_wal_segment(generation.as_str()).await;
        let segment = match segment {
            Err(e) => {
                if e.code() == Error::NO_WALSEGMENT_ERROR {
                    self
                        .client
                        .read_generation_manifest(generation.as_str())
                        .await?
                        .ok_or_else(|| {
                        Error::StorageError(format!(
                            "manifest-only archival runtime cannot resume generation {} without a generation manifest",
                            generation.as_str()
                        ))
                    })?;
                    return Ok(Self::manifest_snapshot_resume_position(&WalGenerationPos {
                        generation,
                        index: snapshot.index,
                        offset: snapshot.offset,
                    }));
                } else {
                    return Err(e);
                }
            }
            Ok(segment) => segment,
        };

        let compressed_data = self.client.read_wal_segment(&segment).await?;
        let decompressed_data = decompressed_data(compressed_data)?;

        Ok(WalGenerationPos {
            generation: segment.generation.clone(),
            index: segment.index,
            offset: segment.offset + decompressed_data.len() as u64,
        })
    }

    async fn sync_wal(&mut self) -> Result<()> {
        let pos = self.position();
        let mut reader = ShadowWalReader::try_create(pos, &self.info)?;

        // Obtain initial position from shadow reader.
        // It may have moved to the next index if previous position was at the end.
        let init_pos = reader.position();
        let mut data = Vec::new();

        debug!("db {} write wal segment position {:?}", self.db, init_pos,);

        // Copy header if at offset zero.
        let mut salt1 = 0;
        let mut salt2 = 0;
        if init_pos.offset == 0 {
            let wal_header = WALHeader::read_from(&mut reader)?;
            salt1 = wal_header.salt1;
            salt2 = wal_header.salt2;
            data.extend_from_slice(&wal_header.data);
        }

        // Copy frames.
        loop {
            if reader.left == 0 {
                break;
            }

            let pos = reader.position();
            debug_assert_eq!(pos.offset, align_frame(self.info.page_size, pos.offset));

            let wal_frame = WALFrame::read(&mut reader, self.info.page_size)?;

            if (salt1 != 0 && salt1 != wal_frame.salt1) || (salt2 != 0 && salt2 != wal_frame.salt2)
            {
                return Err(Error::SqliteInvalidWalFrameError(format!(
                    "db {} Invalid WAL frame at offset {}",
                    self.db, pos.offset
                )));
            }
            salt1 = wal_frame.salt1;
            salt2 = wal_frame.salt2;

            data.extend_from_slice(&wal_frame.data);
        }
        let end_pos = reader.position();
        let compressed_data = compress_buffer(&data)?;

        self.publish_manifest_wal_pack(&init_pos, &end_pos, compressed_data, data.len())
            .await?;

        // update position
        let mut position = self.position.write();
        *position = end_pos;
        Ok(())
    }

    pub fn position(&self) -> WalGenerationPos {
        let position = self.position.read();
        position.clone()
    }

    fn reset_position(&self) {
        let mut position = self.position.write();
        *position = WalGenerationPos::default();
    }

    fn enter_wait_snapshot(&mut self) {
        self.state = ReplicateState::WaitSnapshot;
        self.wait_snapshot_started_at = Some(Instant::now());
    }

    fn leave_wait_snapshot(&mut self) {
        self.state = ReplicateState::WaitDbChanged;
        self.wait_snapshot_started_at = None;
    }

    fn manifest_snapshot_resume_position(pos: &WalGenerationPos) -> WalGenerationPos {
        WalGenerationPos {
            generation: pos.generation.clone(),
            index: pos.index,
            offset: 0,
        }
    }

    async fn command(&mut self, cmd: ReplicateCommand) -> Result<()> {
        match cmd {
            ReplicateCommand::DbChanged(pos) => {
                if let Err(e) = self.sync(pos).await {
                    error!("sync db error: {e:?}");
                    // Clear last position if if an error occurs during sync.
                    self.reset_position();
                    return Err(e);
                }
            }
            ReplicateCommand::Snapshot((pos, compressed_data)) => {
                if let Err(e) = self.sync_snapshot(pos, compressed_data).await {
                    error!("sync db snapshot error: {e:?}");
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn sync_snapshot(
        &mut self,
        pos: WalGenerationPos,
        compressed_data: Vec<u8>,
    ) -> Result<()> {
        info!("db {} sync snapshot {:?}", self.db, pos);
        if self.state != ReplicateState::WaitSnapshot {
            return Err(Error::StorageError(format!(
                "refusing out-of-phase snapshot while state is {:?}",
                self.state
            )));
        }
        if pos.offset == 0 {
            return Ok(());
        }

        self.client
            .publish_manifest_snapshot(&pos, compressed_data)
            .await?;
        *self.position.write() = Self::manifest_snapshot_resume_position(&pos);

        // change state from WaitSnapshot to WaitDbChanged
        self.leave_wait_snapshot();
        self.sync(pos).await
    }

    async fn maybe_request_manifest_rollover(&mut self, generation: &Generation) -> Result<bool> {
        if !self
            .client
            .manifest_rollover_required(
                generation,
                Self::manifest_rollover_pack_threshold(),
                Self::manifest_rollover_manifest_max_bytes(),
            )
            .await?
        {
            return Ok(false);
        }

        self.db_notifier
            .send(DbCommand::SnapshotNewGeneration(self.index))
            .await?;
        self.enter_wait_snapshot();
        Ok(true)
    }

    async fn publish_manifest_wal_pack(
        &self,
        start: &WalGenerationPos,
        end: &WalGenerationPos,
        compressed_data: Vec<u8>,
        uncompressed_size_bytes: usize,
    ) -> Result<()> {
        if uncompressed_size_bytes <= Self::manifest_wal_pack_max_bytes() {
            return self
                .client
                .publish_manifest_wal_pack(start, end, compressed_data, uncompressed_size_bytes)
                .await;
        }

        if start.generation != end.generation || start.index != end.index {
            return Err(Error::StorageError(
                "manifest wal pack split requires same generation/index".to_string(),
            ));
        }

        let data = decompressed_data(compressed_data)?;
        if data.len() != uncompressed_size_bytes {
            return Err(Error::StorageError(format!(
                "manifest wal pack size mismatch: decompressed {} != expected {}",
                data.len(),
                uncompressed_size_bytes
            )));
        }

        let budget = Self::manifest_wal_pack_max_bytes();
        let mut chunk_offset = 0usize;
        while chunk_offset < data.len() {
            let chunk_end = std::cmp::min(chunk_offset + budget, data.len());
            let chunk = &data[chunk_offset..chunk_end];
            let chunk_start = WalGenerationPos {
                generation: start.generation.clone(),
                index: start.index,
                offset: start.offset + chunk_offset as u64,
            };
            let chunk_end_pos = WalGenerationPos {
                generation: start.generation.clone(),
                index: start.index,
                offset: start.offset + chunk_end as u64,
            };
            self.client
                .publish_manifest_wal_pack(
                    &chunk_start,
                    &chunk_end_pos,
                    compress_buffer(chunk)?,
                    chunk.len(),
                )
                .await?;
            chunk_offset = chunk_end;
        }

        Ok(())
    }

    async fn coalesce_db_changed_notifications(
        rx: &mut Receiver<ReplicateCommand>,
        initial: WalGenerationPos,
    ) -> (WalGenerationPos, Option<ReplicateCommand>) {
        let mut latest = initial;

        loop {
            loop {
                match rx.try_recv() {
                    Ok(ReplicateCommand::DbChanged(pos)) => latest = pos,
                    Ok(other) => return (latest, Some(other)),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return (latest, None),
                }
            }

            match timeout(std::time::Duration::from_millis(10), rx.recv()).await {
                Ok(Some(ReplicateCommand::DbChanged(pos))) => {
                    latest = pos;
                }
                Ok(Some(other)) => return (latest, Some(other)),
                Ok(None) => return (latest, None),
                Err(_) => return (latest, None),
            }
        }
    }

    async fn sync(&mut self, pos: WalGenerationPos) -> Result<()> {
        info!(
            "db {} replicate {} replica sync pos: {:?}\n",
            self.db, self.config.name, pos
        );

        if self.state == ReplicateState::WaitSnapshot {
            if self
                .wait_snapshot_started_at
                .is_some_and(|started| started.elapsed() >= Self::wait_snapshot_timeout())
            {
                error!(
                    "db {} wait snapshot timed out after {:?}; retrying snapshot request path",
                    self.db,
                    Self::wait_snapshot_timeout()
                );
                self.leave_wait_snapshot();
            } else {
                return Ok(());
            }
        }

        if pos.offset == 0 {
            return Ok(());
        }

        // Create a new snapshot and update the current replica position if
        // the generation on the database has changed.
        let generation = pos.generation.clone();
        let position = {
            let position = self.position.read();
            position.clone()
        };
        debug!(
            "db {} replicate {} replicate position: {:?}",
            self.db, self.config.name, position
        );
        if generation != position.generation {
            let snapshots = self.client.snapshots(generation.as_str()).await?;
            debug!(
                "db {} replicate {} snapshot {} num: {}",
                self.db,
                self.config.name,
                generation.as_str(),
                snapshots.len()
            );
            if snapshots.is_empty() {
                // Create snapshot if no snapshots exist for generation.
                self.db_notifier
                    .send(DbCommand::Snapshot(self.index))
                    .await?;
                self.enter_wait_snapshot();
                return Ok(());
            }

            let pos = self
                .calculate_generation_position(generation.as_str())
                .await?;
            debug!(
                "db {} replicate {} calc position: {:?}",
                self.db, self.config.name, pos
            );
            *self.position.write() = pos;
        }

        // Read all WAL files since the last position.
        loop {
            let current_generation = self.position().generation.clone();
            if !current_generation.is_empty()
                && self
                    .maybe_request_manifest_rollover(&current_generation)
                    .await?
            {
                return Ok(());
            }

            if let Err(e) = self.sync_wal().await
                && e.code() == Error::UNEXPECTED_EOF_ERROR
            {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ReplicateCommand;
    use super::snapshot_position_is_newer;

    #[test]
    fn snapshot_position_prefers_later_offset_when_index_matches() {
        assert!(snapshot_position_is_newer(3, 900, Some((3, 500))));
        assert!(!snapshot_position_is_newer(3, 500, Some((3, 900))));
    }

    #[test]
    fn manifest_snapshot_resume_position_restarts_same_index_at_zero() {
        use crate::base::Generation;
        use crate::database::WalGenerationPos;

        let pos = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 4152,
        };

        let resumed = super::Replicate::manifest_snapshot_resume_position(&pos);
        assert_eq!(resumed.generation, pos.generation);
        assert_eq!(resumed.index, pos.index);
        assert_eq!(resumed.offset, 0);
    }

    #[test]
    fn replicate_main_exits_when_channel_closed() {
        use tempfile::tempdir;
        use tokio::runtime::Builder;
        use tokio::sync::mpsc;

        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::DatabaseInfo;

        let (done_tx, done_rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime");

            runtime.block_on(async move {
                let temp = tempdir().expect("tempdir");
                let storage_root = temp.path().join("storage");
                let storage_cfg = StorageConfig {
                    name: "fs".to_string(),
                    params: StorageParams::Fs(Box::new(StorageFsConfig {
                        root: storage_root.to_string_lossy().to_string(),
                    })),
                };

                let (db_notifier, _db_rx) = mpsc::channel(1);
                let info = DatabaseInfo {
                    meta_dir: temp.path().to_string_lossy().to_string(),
                    page_size: 4096,
                };

                let replicate =
                    super::Replicate::new_for_test(
                        storage_cfg,
                        "data.db".to_string(),
                        0,
                        db_notifier,
                        info,
                    )
                        .await
                        .expect("create replicate");

                let (tx, rx) = mpsc::channel(1);
                drop(tx); // immediately close channel

                let _ = super::Replicate::main(replicate, rx).await;

                let _ = done_tx.send(());
            });
        });

        // In the broken implementation, this will time out and the test will fail.
        done_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("Replicate::main did not exit after channel closed");

        handle.join().expect("join thread");
    }

    #[tokio::test]
    async fn wal_pack_publish_records_exact_lsn_range_and_is_visible_to_manifest_restore() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");

        let snapshot_pos = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4096,
        };
        replicate
            .client
            .publish_manifest_snapshot(
                &snapshot_pos,
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        let pack_data = b"part-a-part-b".to_vec();
        let start = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: snapshot_pos.offset,
        };
        let end = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: snapshot_pos.offset + pack_data.len() as u64,
        };
        replicate
            .publish_manifest_wal_pack(
                &start,
                &end,
                compress_buffer(&pack_data).expect("compress pack"),
                pack_data.len(),
            )
            .await
            .expect("publish wal pack");

        let (
            _generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            _base_snapshot,
            wal_packs,
        ) = replicate
            .client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest restore inputs");
        assert_eq!(wal_packs.len(), 1);
        assert_eq!(wal_packs[0].0, snapshot_pos.offset);
        assert_eq!(wal_packs[0].1, snapshot_pos.offset + pack_data.len() as u64);
    }

    #[tokio::test]
    async fn calculate_generation_position_uses_zero_offset_for_manifest_snapshot_when_no_wal_exists()
     {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");

        let snapshot_pos = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4152,
        };
        replicate
            .client
            .publish_manifest_snapshot(
                &snapshot_pos,
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )
            .await
            .expect("publish snapshot");

        let position = replicate
            .calculate_generation_position(generation.as_str())
            .await
            .expect("calculate generation position");

        assert_eq!(position.generation, generation);
        assert_eq!(position.index, snapshot_pos.index);
        assert_eq!(position.offset, 0);
    }

    #[tokio::test]
    async fn calculate_generation_position_fails_without_manifest_when_no_wal_exists() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");

        let snapshot_pos = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4152,
        };
        replicate
            .client
            .write_snapshot(
                &snapshot_pos,
                compress_buffer(b"legacy snapshot").expect("compress snapshot"),
            )
            .await
            .expect("write snapshot");

        let err = replicate
            .calculate_generation_position(generation.as_str())
            .await
            .expect_err("manifest-only runtime should fail without generation manifest");

        assert_eq!(err.code(), crate::error::Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn wal_pack_enforces_pack_size_budget() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");

        replicate
            .client
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

        let oversize = vec![7_u8; super::Replicate::manifest_wal_pack_max_bytes() + 1];
        replicate
            .publish_manifest_wal_pack(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 0,
                },
                &WalGenerationPos {
                    generation,
                    index: 1,
                    offset: oversize.len() as u64,
                },
                compress_buffer(&oversize).expect("compress oversize"),
                oversize.len(),
            )
            .await
            .expect("oversize wal pack should split");

        let (
            _generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            _base_snapshot,
            wal_packs,
        ) = replicate
            .client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest metadata");
        assert_eq!(wal_packs.len(), 2);
        assert_eq!(wal_packs[0].0, 0);
        assert_eq!(
            wal_packs[0].1,
            super::Replicate::manifest_wal_pack_max_bytes() as u64
        );
        assert_eq!(
            wal_packs[1].0,
            super::Replicate::manifest_wal_pack_max_bytes() as u64
        );
        assert_eq!(wal_packs[1].1, oversize.len() as u64);
    }

    #[tokio::test]
    async fn coalesced_db_changed_notifications_collapse_to_latest_position() {
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::database::WalGenerationPos;

        let generation = Generation::new();
        let first = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 4096,
        };
        let second = WalGenerationPos {
            generation: generation.clone(),
            index: 1,
            offset: 8192,
        };
        let snapshot = ReplicateCommand::Snapshot((
            WalGenerationPos {
                generation,
                index: 2,
                offset: 0,
            },
            vec![1, 2, 3],
        ));

        let (tx, mut rx) = mpsc::channel(4);
        tx.send(ReplicateCommand::DbChanged(first.clone()))
            .await
            .expect("send first");
        tx.send(ReplicateCommand::DbChanged(second.clone()))
            .await
            .expect("send second");
        tx.send(snapshot.clone()).await.expect("send snapshot");

        let (latest, pending) =
            super::Replicate::coalesce_db_changed_notifications(&mut rx, first).await;

        assert_eq!(latest, second);
        match pending {
            Some(ReplicateCommand::Snapshot((pos, data))) => {
                if let ReplicateCommand::Snapshot((expected_pos, expected_data)) = snapshot {
                    assert_eq!(pos, expected_pos);
                    assert_eq!(data, expected_data);
                } else {
                    unreachable!("snapshot fixture changed");
                }
            }
            other => panic!("expected pending snapshot, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wait_snapshot_timeout_recovers_and_retries_snapshot_request() {
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, DbCommand, WalGenerationPos};

        let temp = tempfile::tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, mut db_rx) = mpsc::channel(4);
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let mut replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");
        replicate.state = super::ReplicateState::WaitSnapshot;
        replicate.wait_snapshot_started_at = Some(
            std::time::Instant::now()
                .checked_sub(super::Replicate::wait_snapshot_timeout() + Duration::from_secs(1))
                .expect("checked sub"),
        );

        let pos = WalGenerationPos {
            generation: Generation::new(),
            index: 1,
            offset: 4096,
        };
        replicate.sync(pos).await.expect("sync after timeout");

        assert_eq!(replicate.state, super::ReplicateState::WaitSnapshot);
        assert!(replicate.wait_snapshot_started_at.is_some());
        assert!(matches!(db_rx.recv().await, Some(DbCommand::Snapshot(0))));
    }

    #[tokio::test]
    async fn wal_pack_split_publish_uses_unique_range_keys() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate =
            super::Replicate::new_for_test(storage_cfg, "data.db".to_string(), 0, db_notifier, info)
                .await
                .expect("create replicate");

        replicate
            .client
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

        replicate
            .publish_manifest_wal_pack(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                &WalGenerationPos {
                    generation,
                    index: 1,
                    offset: 8192,
                },
                compress_buffer(b"range-pack").expect("compress pack"),
                b"range-pack".len(),
            )
            .await
            .expect("publish wal pack");

        let (
            _generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            _base_snapshot,
            wal_packs,
        ) = replicate
            .client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest metadata");
        assert_eq!(wal_packs.len(), 1);
        assert!(wal_packs[0].2.contains("/ranges/"));
        assert!(
            wal_packs[0]
                .2
                .contains("0000000001_0000004096_0000000001_0000008192")
        );
    }

    #[tokio::test]
    async fn sync_snapshot_fails_fast_on_unsupported_manifest_publish_backend() {
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::config::{StorageConfig, StorageFtpConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        use crate::error::Error;

        let (db_notifier, _db_rx) = mpsc::channel(1);
        let info = DatabaseInfo {
            meta_dir: tempfile::tempdir()
                .expect("tempdir")
                .path()
                .to_string_lossy()
                .to_string(),
            page_size: 4096,
        };
        let mut replicate = super::Replicate::new_for_test(
            StorageConfig {
                name: "ftp".to_string(),
                params: StorageParams::Ftp(Box::new(StorageFtpConfig::default())),
            },
            "data.db".to_string(),
            0,
            db_notifier,
            info,
        )
        .await
        .expect("create replicate");
        replicate.state = super::ReplicateState::WaitSnapshot;

        let err = replicate
            .sync_snapshot(
                WalGenerationPos {
                    generation: Generation::new(),
                    index: 1,
                    offset: 4096,
                },
                crate::base::compress_buffer(b"snapshot").expect("compress"),
            )
            .await
            .expect_err("unsupported backend should fail fast");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
        assert_eq!(replicate.position().offset, 0);
    }

    #[tokio::test]
    async fn replicate_main_returns_error_on_snapshot_manifest_publish_failure() {
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::config::{StorageConfig, StorageFtpConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        use crate::error::Error;

        let (db_notifier, _db_rx) = mpsc::channel(1);
        let info = DatabaseInfo {
            meta_dir: tempfile::tempdir()
                .expect("tempdir")
                .path()
                .to_string_lossy()
                .to_string(),
            page_size: 4096,
        };
        let mut replicate = super::Replicate::new_for_test(
            StorageConfig {
                name: "ftp".to_string(),
                params: StorageParams::Ftp(Box::new(StorageFtpConfig::default())),
            },
            "data.db".to_string(),
            0,
            db_notifier,
            info,
        )
        .await
        .expect("create replicate");
        replicate.state = super::ReplicateState::WaitSnapshot;

        let (tx, rx) = mpsc::channel(1);
        tx.send(ReplicateCommand::Snapshot((
            WalGenerationPos {
                generation: Generation::new(),
                index: 1,
                offset: 4096,
            },
            crate::base::compress_buffer(b"snapshot").expect("compress"),
        )))
        .await
        .expect("send snapshot");
        drop(tx);

        let err = super::Replicate::main(replicate, rx)
            .await
            .expect_err("main should escalate manifest publish failure");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn replicate_main_returns_error_when_db_changed_hits_no_manifest_resume_state() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let replicate = super::Replicate::new_for_test(
            storage_cfg.clone(),
            "data.db".to_string(),
            0,
            db_notifier,
            info,
        )
        .await
        .expect("create replicate");

        let generation = Generation::new();
        let setup_client =
            crate::storage::StorageClient::try_create("data.db".to_string(), storage_cfg)
                .expect("storage client");
        setup_client
            .write_snapshot(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"legacy snapshot").expect("compress snapshot"),
            )
            .await
            .expect("write legacy snapshot");

        let (tx, rx) = mpsc::channel(1);
        tx.send(ReplicateCommand::DbChanged(WalGenerationPos {
            generation,
            index: 1,
            offset: 4096,
        }))
        .await
        .expect("send db changed");
        drop(tx);

        let err = super::Replicate::main(replicate, rx)
            .await
            .expect_err("main should escalate no-manifest resume failure");

        assert_eq!(err.code(), crate::error::Error::STORAGE_ERROR);
    }

    #[tokio::test]
    async fn publish_manifest_wal_pack_fails_fast_on_unsupported_backend() {
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::config::{StorageConfig, StorageFtpConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};
        use crate::error::Error;

        let (db_notifier, _db_rx) = mpsc::channel(1);
        let info = DatabaseInfo {
            meta_dir: tempfile::tempdir()
                .expect("tempdir")
                .path()
                .to_string_lossy()
                .to_string(),
            page_size: 4096,
        };
        let replicate = super::Replicate::new_for_test(
            StorageConfig {
                name: "ftp".to_string(),
                params: StorageParams::Ftp(Box::new(StorageFtpConfig::default())),
            },
            "data.db".to_string(),
            0,
            db_notifier,
            info,
        )
        .await
        .expect("create replicate");

        let generation = Generation::new();
        let err = replicate
            .publish_manifest_wal_pack(
                &WalGenerationPos {
                    generation: generation.clone(),
                    index: 1,
                    offset: 0,
                },
                &WalGenerationPos {
                    generation,
                    index: 1,
                    offset: 1024,
                },
                crate::base::compress_buffer(b"wal-pack").expect("compress"),
                b"wal-pack".len(),
            )
            .await
            .expect_err("unsupported backend should fail fast");

        assert_eq!(err.code(), Error::INVALID_CONFIG);
    }

    #[tokio::test]
    async fn out_of_phase_snapshot_command_does_not_publish_manifest_state() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, _db_rx) = mpsc::channel(1);
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let mut replicate =
            super::Replicate::new_for_test(
                storage_cfg,
                "data.db".to_string(),
                0,
                db_notifier,
                info,
            )
                .await
                .expect("create replicate");

        let err = replicate
            .command(ReplicateCommand::Snapshot((
                WalGenerationPos {
                    generation: Generation::new(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"snapshot").expect("compress snapshot"),
            )))
            .await
            .expect_err("out-of-phase snapshot should fail fast");

        assert_eq!(err.code(), crate::error::Error::STORAGE_ERROR);

        assert_eq!(replicate.state, super::ReplicateState::WaitDbChanged);
        assert!(
            replicate
                .client
                .read_latest_pointer()
                .await
                .expect("read latest pointer")
                .is_none()
        );
    }

    #[tokio::test]
    async fn rollover_threshold_requests_forced_new_generation_snapshot() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, DbCommand, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, mut db_rx) = mpsc::channel(4);
        let generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let mut replicate =
            super::Replicate::new_for_test(
                storage_cfg,
                "data.db".to_string(),
                0,
                db_notifier,
                info,
            )
                .await
                .expect("create replicate");

        replicate
            .client
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

        for i in 0..super::Replicate::manifest_rollover_pack_threshold() {
            let start = 4096 + (i as u64 * 1024);
            let end = start + 1024;
            replicate
                .client
                .publish_manifest_wal_pack(
                    &WalGenerationPos {
                        generation: generation.clone(),
                        index: 1,
                        offset: start,
                    },
                    &WalGenerationPos {
                        generation: generation.clone(),
                        index: 1,
                        offset: end,
                    },
                    compress_buffer(&vec![i as u8; 1024]).expect("compress pack"),
                    1024,
                )
                .await
                .expect("publish pack");
        }

        let triggered = replicate
            .maybe_request_manifest_rollover(&generation)
            .await
            .expect("check rollover");

        assert!(triggered);
        assert_eq!(replicate.state, super::ReplicateState::WaitSnapshot);
        assert!(matches!(
            db_rx.recv().await,
            Some(DbCommand::SnapshotNewGeneration(0))
        ));
    }

    #[tokio::test]
    async fn rollover_publish_advances_latest_visible_generation() {
        use tempfile::tempdir;
        use tokio::sync::mpsc;

        use crate::base::Generation;
        use crate::base::compress_buffer;
        use crate::config::{StorageConfig, StorageFsConfig, StorageParams};
        use crate::database::{DatabaseInfo, WalGenerationPos};

        let temp = tempdir().expect("tempdir");
        let storage_root = temp.path().join("storage");
        let storage_cfg = StorageConfig {
            name: "fs".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: storage_root.to_string_lossy().to_string(),
            })),
        };
        let (db_notifier, mut db_rx) = mpsc::channel(4);
        let older_generation = Generation::new();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let newer_generation = Generation::new();
        let info = DatabaseInfo {
            meta_dir: temp.path().to_string_lossy().to_string(),
            page_size: 4096,
        };
        let mut replicate =
            super::Replicate::new_for_test(
                storage_cfg,
                "data.db".to_string(),
                0,
                db_notifier,
                info,
            )
                .await
                .expect("create replicate");

        replicate
            .client
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

        for i in 0..super::Replicate::manifest_rollover_pack_threshold() {
            let start = 4096 + (i as u64 * 1024);
            let end = start + 1024;
            replicate
                .client
                .publish_manifest_wal_pack(
                    &WalGenerationPos {
                        generation: older_generation.clone(),
                        index: 1,
                        offset: start,
                    },
                    &WalGenerationPos {
                        generation: older_generation.clone(),
                        index: 1,
                        offset: end,
                    },
                    compress_buffer(&vec![i as u8; 1024]).expect("compress pack"),
                    1024,
                )
                .await
                .expect("publish pack");
        }

        assert!(
            replicate
                .maybe_request_manifest_rollover(&older_generation)
                .await
                .expect("request rollover")
        );
        assert!(matches!(
            db_rx.recv().await,
            Some(crate::database::DbCommand::SnapshotNewGeneration(0))
        ));

        replicate
            .client
            .publish_manifest_snapshot(
                &WalGenerationPos {
                    generation: newer_generation.clone(),
                    index: 1,
                    offset: 4096,
                },
                compress_buffer(b"newer snapshot").expect("compress newer snapshot"),
            )
            .await
            .expect("publish rollover snapshot");

        let (
            generation_id,
            _manifest_id,
            _lineage_id,
            _base_snapshot_id,
            _base_snapshot_sha256,
            _base_snapshot,
            _wal_packs,
        ) = replicate
            .client
            .read_manifest_restore_inputs()
            .await
            .expect("read manifest restore inputs")
            .expect("manifest metadata");
        assert_eq!(generation_id, newer_generation.as_str());
    }
}

use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

use log::debug;
use log::error;
use log::info;
use rusqlite::Connection;
use rusqlite::DropBehavior;
use tempfile::NamedTempFile;
use tempfile::tempfile;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::base::Generation;
use crate::base::compress_file;
use crate::base::generation_dir;
use crate::base::generation_file_path;
use crate::base::local_generations_dir;
use crate::base::parent_dir;
use crate::base::parse_wal_path;
use crate::base::path_base;
use crate::base::shadow_wal_dir;
use crate::base::shadow_wal_file;
use crate::config::DbConfig;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::CheckpointMode;
use crate::sqlite::WAL_FRAME_HEADER_SIZE;
use crate::sqlite::WAL_HEADER_SIZE;
use crate::sqlite::WALFrame;
use crate::sqlite::WALHeader;
use crate::sqlite::align_frame;
use crate::sqlite::checksum;
use crate::sqlite::read_last_checksum;
use crate::sync::Replicate;
use crate::sync::ReplicateCommand;

const GENERATION_LEN: usize = 32;

// MaxIndex is the maximum possible WAL index.
// If this index is reached then a new generation will be started.
const MAX_WAL_INDEX: u64 = 0x7FFFFFFF;

// Default DB settings.
const DEFAULT_MONITOR_INTERVAL: Duration = Duration::from_secs(1);

mod checkpoint;
mod init;
mod snapshot;

#[derive(Clone, Debug)]
pub enum DbCommand {
    Snapshot(usize),
}

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    // Path to the database metadata.
    pub meta_dir: String,

    pub page_size: u64,
}

pub struct Database {
    config: DbConfig,

    // Path to the database metadata.
    meta_dir: String,

    // full wal file name of db file
    wal_file: String,
    page_size: u64,

    connection: Connection,

    // database connection for transaction, None if there if no tranction
    tx_connection: Option<Connection>,

    // for sync
    sync_notifiers: Vec<Option<Sender<ReplicateCommand>>>,
    syncs: Vec<Replicate>,
}

// position info of wal for a generation
#[derive(Debug, Clone, Default)]
pub struct WalGenerationPos {
    // generation name
    pub generation: Generation,

    // wal file index
    pub index: u64,

    // offset within wal file
    pub offset: u64,
}

impl WalGenerationPos {
    pub fn is_empty(&self) -> bool {
        self.generation.is_empty() && self.index == 0 && self.offset == 0
    }
}

#[derive(Debug, Default)]
struct SyncInfo {
    pub generation: Generation,
    pub db_mod_time: Option<SystemTime>,
    pub wal_size: u64,
    pub shadow_wal_file: String,
    pub shadow_wal_size: u64,
    pub reason: Option<String>,
    pub restart: bool,
}

impl Database {
    // acquire_read_lock begins a read transaction on the database to prevent checkpointing.
    fn acquire_read_lock(&mut self) -> Result<()> {
        if self.tx_connection.is_none() {
            let tx_connection = Connection::open(&self.config.db)?;
            // Execute read query to obtain read lock.
            tx_connection.execute_batch("BEGIN;SELECT COUNT(1) FROM _replited_seq;")?;
            self.tx_connection = Some(tx_connection);
        }

        Ok(())
    }

    fn release_read_lock(&mut self) -> Result<()> {
        // Rollback & clear read transaction.
        if let Some(tx) = &self.tx_connection {
            tx.execute_batch("ROLLBACK;")?;
            self.tx_connection = None;
        }

        Ok(())
    }

    // verify if primary wal and last shadow wal header match
    fn verify_header_match(&self) -> Result<()> {
        let generation = self.current_generation()?;
        if generation.is_empty() {
            return Ok(());
        }

        let shadow_wal_file = self.current_shadow_wal_file(&generation)?;

        let wal_header = WALHeader::read(&self.wal_file)?;
        let shadow_wal_header = WALHeader::read(&shadow_wal_file)?;

        if wal_header != shadow_wal_header {
            return Err(Error::MismatchWalHeaderError(format!(
                "db {} wal header mismatched",
                self.config.db
            )));
        }

        Ok(())
    }

    // copy pending data from wal to shadow wal
    async fn sync(&mut self) -> Result<()> {
        debug!("sync database: {}", self.config.db);

        // make sure wal file has at least one frame in it
        self.ensure_wal_exists()?;

        // Verify our last sync matches the current state of the WAL.
        // This ensures that we have an existing generation & that the last sync
        // position of the real WAL hasn't been overwritten by another process.
        let mut info = self.verify()?;
        debug!("db {} sync info: {:?}", self.config.db, info);

        // Track if anything in the shadow WAL changes and then notify at the end.
        let mut changed =
            info.wal_size != info.shadow_wal_size || info.restart || info.reason.is_some();

        if let Some(reason) = &info.reason {
            // Start new generation & notify user via log message.
            info.generation = self.create_generation().await?;
            info!(
                "db {} sync new generation: {}, reason: {}",
                self.config.db,
                info.generation.as_str(),
                reason
            );

            // Clear shadow wal info.
            info.shadow_wal_file = shadow_wal_file(&self.meta_dir, info.generation.as_str(), 0);
            info.shadow_wal_size = WAL_HEADER_SIZE;
            info.restart = false;
            info.reason = None;
        }

        // Synchronize real WAL with current shadow WAL.
        let (orig_wal_size, new_wal_size) = self.sync_wal(&info)?;
        debug!(
            "db {} sync_wal {}:{}",
            self.config.db, orig_wal_size, new_wal_size
        );

        // decide if need to do checkpoint
        let checkmode = self.decide_checkpoint_mode(orig_wal_size, new_wal_size, &info);
        if let Some(checkmode) = checkmode {
            changed = true;

            self.do_checkpoint(info.generation.as_str(), checkmode.as_str())?;
        }

        // Clean up any old files.
        self.clean()?;

        // notify the database has been changed
        if changed {
            let generation_pos = self.wal_generation_position()?;
            if let Some(notifier) = &self.sync_notifiers[0] {
                notifier
                    .send(ReplicateCommand::DbChanged(generation_pos))
                    .await?;
            }
        }

        debug!("sync db {} ok", self.config.db);
        Ok(())
    }

    pub fn wal_generation_position(&self) -> Result<WalGenerationPos> {
        let generation = Generation::try_create(&self.current_generation()?)?;

        let (index, _total_size) = self.current_shadow_index(generation.as_str())?;

        let shadow_wal_file = self.shadow_wal_file(generation.as_str(), index);

        match fs::exists(&shadow_wal_file) {
            Err(e) => {
                error!(
                    "check db {} shadow_wal file {} err: {}",
                    self.config.db, shadow_wal_file, e,
                );
                return Err(e.into());
            }
            Ok(exist) => {
                // shadow wal file not exists, return pos with offset = 0
                if !exist {
                    return Ok(WalGenerationPos {
                        generation,
                        index,
                        offset: 0,
                    });
                }
            }
        }
        let file_metadata = fs::metadata(&shadow_wal_file)?;

        Ok(WalGenerationPos {
            generation,
            index,
            offset: align_frame(self.page_size, file_metadata.size()),
        })
    }

    // CurrentShadowWALPath returns the path to the last shadow WAL in a generation.
    fn current_shadow_wal_file(&self, generation: &str) -> Result<String> {
        let (index, _total_size) = self.current_shadow_index(generation)?;

        Ok(self.shadow_wal_file(generation, index))
    }

    // returns the path of a single shadow WAL file.
    fn shadow_wal_file(&self, generation: &str, index: u64) -> String {
        shadow_wal_file(&self.meta_dir, generation, index)
    }

    // create_generation initiates a new generation by establishing the generation
    // directory, capturing snapshots for each replica, and refreshing the current
    // generation name.
    async fn create_generation(&mut self) -> Result<Generation> {
        let generation = Generation::new();

        // create a temp file to write new generation
        let temp_file = NamedTempFile::new()?;
        let temp_file_name = temp_file.path().to_str().unwrap().to_string();

        fs::write(&temp_file_name, generation.as_str())?;

        // create new directory.
        let dir = generation_dir(&self.meta_dir, generation.as_str());
        fs::create_dir_all(&dir)?;

        // init first(index 0) shadow wal file
        self.init_shadow_wal_file(&self.shadow_wal_file(generation.as_str(), 0))?;

        // rename the temp file to generation file
        let generation_file = generation_file_path(&self.meta_dir);
        fs::rename(&temp_file_name, &generation_file)?;

        // Remove old generations.
        self.clean()?;

        Ok(generation)
    }

    // copies pending bytes from the real WAL to the shadow WAL.
    fn sync_wal(&self, info: &SyncInfo) -> Result<(u64, u64)> {
        let (orig_size, new_size) = self.copy_to_shadow_wal(&info.shadow_wal_file)?;
        debug!(
            "db {} sync_wal copy_to_shadow_wal: {}, {}",
            self.config.db, orig_size, new_size
        );

        if !info.restart {
            return Ok((orig_size, new_size));
        }

        // Parse index of current shadow WAL file.
        let index = parse_wal_path(&info.shadow_wal_file)?;

        // Start a new shadow WAL file with next index.
        let new_shadow_wal_file =
            shadow_wal_file(&self.meta_dir, info.generation.as_str(), index + 1);
        let new_size = self.init_shadow_wal_file(&new_shadow_wal_file)?;

        Ok((orig_size, new_size))
    }

    // remove old generations files and wal files
    fn clean(&mut self) -> Result<()> {
        self.clean_generations()?;

        self.clean_wal()?;

        Ok(())
    }

    fn clean_generations(&self) -> Result<()> {
        let generation = self.current_generation()?;
        let genetations_dir = local_generations_dir(&self.meta_dir);

        if !fs::exists(&genetations_dir)? {
            return Ok(());
        }
        for entry in fs::read_dir(&genetations_dir)? {
            let entry = entry?;
            let file_name = entry.file_name().as_os_str().to_str().unwrap().to_string();
            let base = path_base(&file_name)?;
            // skip the current generation
            if base == generation {
                continue;
            }
            let path = Path::new(&genetations_dir)
                .join(&file_name)
                .as_path()
                .to_str()
                .unwrap()
                .to_string();
            fs::remove_dir_all(&path)?;
        }
        Ok(())
    }

    // removes WAL files that have been replicated.
    fn clean_wal(&self) -> Result<()> {
        let generation = self.current_generation()?;

        let mut min = None;
        for sync in &self.syncs {
            // let sync = sync.read().await;
            let mut position = sync.position();
            if position.generation.as_str() != generation {
                position = WalGenerationPos::default();
            }
            match min {
                None => min = Some(position.index),
                Some(m) => {
                    if position.index < m {
                        min = Some(position.index);
                    }
                }
            }
        }

        // Skip if our lowest index is too small.
        let mut min = match min {
            None => return Ok(()),
            Some(min) => min,
        };

        if min == 0 {
            return Ok(());
        }
        // Keep retained WAL files.
        min = min.saturating_sub(self.config.wal_retention_count);

        // Remove all WAL files for the generation before the lowest index.
        let dir = shadow_wal_dir(&self.meta_dir, generation.as_str());
        if !fs::exists(&dir)? {
            return Ok(());
        }
        for entry in fs::read_dir(&dir)?.flatten() {
            let file_name = entry.file_name().as_os_str().to_str().unwrap().to_string();
            let index = parse_wal_path(&file_name)?;
            if index >= min {
                continue;
            }
            let path = Path::new(&dir)
                .join(&file_name)
                .as_path()
                .to_str()
                .unwrap()
                .to_string();
            fs::remove_file(&path)?;
        }

        Ok(())
    }

    fn init_shadow_wal_file(&self, shadow_wal: &String) -> Result<u64> {
        debug!("init_shadow_wal_file {shadow_wal}");

        // read wal file header
        let wal_header = WALHeader::read(&self.wal_file)?;
        if wal_header.page_size != self.page_size {
            return Err(Error::SqliteInvalidWalHeaderError("Invalid page size"));
        }

        // create new shadow wal file
        let db_file_metadata = fs::metadata(&self.config.db)?;
        let mode = db_file_metadata.mode();
        let dir = parent_dir(shadow_wal);
        if let Some(dir) = dir {
            fs::create_dir_all(&dir)?;
        } else {
            debug!("db {} cannot find parent dir of shadow wal", self.config.db);
        }
        let mut shadow_wal_file = fs::File::create(shadow_wal)?;
        let mut permissions = shadow_wal_file.metadata()?.permissions();
        permissions.set_mode(mode);
        shadow_wal_file.set_permissions(permissions)?;
        std::os::unix::fs::chown(
            shadow_wal,
            Some(db_file_metadata.uid()),
            Some(db_file_metadata.gid()),
        )?;
        // write wal file header into shadow wal file
        shadow_wal_file.write_all(&wal_header.data)?;
        shadow_wal_file.flush()?;
        debug!("create shadow wal file {shadow_wal}");

        // copy wal file frame into shadow wal file
        let (orig_size, new_size) = self.copy_to_shadow_wal(shadow_wal)?;
        debug!(
            "db {} init_shadow_wal_file copy_to_shadow_wal: {}, {}",
            self.config.db, orig_size, new_size
        );

        Ok(new_size)
    }

    // return original wal file size and new wal size
    fn copy_to_shadow_wal(&self, shadow_wal: &String) -> Result<(u64, u64)> {
        let wal_file_name = &self.wal_file;
        let wal_file_metadata = fs::metadata(wal_file_name)?;
        let orig_wal_size = align_frame(self.page_size, wal_file_metadata.size());

        let shadow_wal_file_metadata = fs::metadata(shadow_wal)?;
        let orig_shadow_wal_size = align_frame(self.page_size, shadow_wal_file_metadata.size());
        debug!(
            "copy_to_shadow_wal orig_wal_size: {orig_wal_size},  orig_shadow_wal_size: {orig_shadow_wal_size}"
        );

        // read shadow wal header
        let wal_header = WALHeader::read(shadow_wal)?;

        // create a temp file to copy wal frames
        let mut temp_file = tempfile()?;

        // seek on real db wal
        let mut wal_file = File::open(wal_file_name)?;
        wal_file.seek(SeekFrom::Start(orig_shadow_wal_size))?;
        // read last checksum of shadow wal file
        let (mut ck1, mut ck2) = read_last_checksum(shadow_wal, self.page_size)?;
        let mut offset = orig_shadow_wal_size;
        let mut last_commit_size = orig_shadow_wal_size;
        // Read through WAL from last position to find the page of the last
        // committed transaction.
        loop {
            let wal_frame = WALFrame::read(&mut wal_file, self.page_size);
            let wal_frame = match wal_frame {
                Ok(wal_frame) => wal_frame,
                Err(e) => {
                    if e.code() == Error::UNEXPECTED_EOF_ERROR {
                        debug!("copy_to_shadow_wal EOF at offset: {offset}");
                        break;
                    } else {
                        error!("copy_to_shadow_wal error {e} at offset {offset}");
                        return Err(e);
                    }
                }
            };

            // compare wal frame salts with wal header salts, break if mismatch
            if wal_frame.salt1 != wal_header.salt1 || wal_frame.salt2 != wal_header.salt2 {
                debug!(
                    "db {} copy shadow wal frame salt mismatch at offset {}",
                    self.config.db, offset
                );
                break;
            }

            // frame header
            (ck1, ck2) = checksum(&wal_frame.data[0..8], ck1, ck2, wal_header.is_big_endian);
            // frame data
            (ck1, ck2) = checksum(&wal_frame.data[24..], ck1, ck2, wal_header.is_big_endian);
            if ck1 != wal_frame.checksum1 || ck2 != wal_frame.checksum2 {
                debug!(
                    "db {} copy shadow wal checksum mismatch at offset {}, check: ({},{}),({},{})",
                    self.config.db, offset, ck1, wal_frame.checksum1, ck2, wal_frame.checksum2,
                );
                break;
            }

            // Write page to temporary WAL file.
            temp_file.write_all(&wal_frame.data)?;

            offset += wal_frame.data.len() as u64;
            if wal_frame.db_size != 0 {
                last_commit_size = offset;
            }
        }

        // If no WAL writes found, exit.
        if last_commit_size == orig_shadow_wal_size {
            return Ok((orig_wal_size, last_commit_size));
        }

        // copy frames from temp file to shadow wal file
        temp_file.flush()?;
        temp_file.seek(SeekFrom::Start(0))?;

        let mut buffer = Vec::new();
        temp_file.read_to_end(&mut buffer)?;

        // append wal frames to end of shadow wal file
        let mut shadow_wal_file = OpenOptions::new().append(true).open(shadow_wal)?;
        shadow_wal_file.write_all(&buffer)?;
        shadow_wal_file.flush()?;

        // in debug mode, assert last frame match
        #[cfg(debug_assertions)]
        {
            let shadow_wal_file = fs::metadata(shadow_wal)?;
            let shadow_wal_size = align_frame(self.page_size, shadow_wal_file.len());

            let offset = shadow_wal_size - self.page_size - WAL_FRAME_HEADER_SIZE;

            let mut shadow_wal_file = OpenOptions::new().read(true).open(shadow_wal)?;
            shadow_wal_file.seek(SeekFrom::Start(offset))?;
            let shadow_wal_last_frame = WALFrame::read(&mut shadow_wal_file, self.page_size)?;

            let mut wal_file = OpenOptions::new().read(true).open(wal_file_name)?;
            wal_file.seek(SeekFrom::Start(offset))?;
            let wal_last_frame = WALFrame::read(&mut wal_file, self.page_size)?;

            assert_eq!(shadow_wal_last_frame, wal_last_frame);
        }
        Ok((orig_wal_size, last_commit_size))
    }

    // make sure wal file has at least one frame in it
    fn ensure_wal_exists(&self) -> Result<()> {
        if fs::exists(&self.wal_file)? {
            let stat = fs::metadata(&self.wal_file)?;
            if !stat.is_file() {
                return Err(Error::SqliteWalError(format!(
                    "wal {} is not a file",
                    self.wal_file,
                )));
            }

            if stat.len() >= WAL_HEADER_SIZE {
                return Ok(());
            }
        }

        // create transaction that updates the internal table.
        self.connection.execute(
            "INSERT INTO _replited_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1;",
            (),
        )?;

        Ok(())
    }

    // current_shadow_index returns the current WAL index & total size.
    fn current_shadow_index(&self, generation: &str) -> Result<(u64, u64)> {
        let wal_dir = shadow_wal_dir(&self.meta_dir, generation);
        if !fs::exists(&wal_dir)? {
            return Ok((0, 0));
        }

        let entries = fs::read_dir(&wal_dir)?;
        let mut total_size = 0;
        let mut index = 0;
        for entry in entries.flatten() {
            let file_type = entry.file_type()?;
            let file_name = entry.file_name().into_string().unwrap();
            let path = Path::new(&wal_dir).join(&file_name);
            if !fs::exists(&path)? {
                // file was deleted after os.ReadDir returned
                debug!(
                    "db {} shadow wal {:?} deleted after read_dir return",
                    self.config.db, path
                );
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let metadata = entry.metadata()?;
            total_size += metadata.size();
            match parse_wal_path(&file_name) {
                Err(e) => {
                    debug!("invalid wal file {file_name:?}, err:{e:?}");
                    continue;
                }
                Ok(i) => {
                    if i > index {
                        index = i;
                    }
                }
            }
        }

        Ok((index, total_size))
    }

    // current_generation returns the name of the generation saved to the "generation"
    // file in the meta data directory.
    // Returns empty string if none exists.
    fn current_generation(&self) -> Result<String> {
        let generation_file = generation_file_path(&self.meta_dir);
        if !fs::exists(&generation_file)? {
            return Ok("".to_string());
        }
        let generation = fs::read_to_string(&generation_file)?;
        if generation.len() != GENERATION_LEN {
            return Ok("".to_string());
        }

        Ok(generation)
    }

    // verify ensures the current shadow WAL state matches where it left off from
    // the real WAL. Returns generation & WAL sync information. If info.reason is
    // not blank, verification failed and a new generation should be started.
    fn verify(&mut self) -> Result<SyncInfo> {
        let mut info = SyncInfo::default();

        // get existing generation
        let generation = self.current_generation()?;
        if generation.is_empty() {
            info.reason = Some("no generation exists".to_string());
            return Ok(info);
        }
        info.generation = Generation::try_create(&generation)?;

        let db_file = fs::metadata(&self.config.db)?;
        if let Ok(db_mod_time) = db_file.modified() {
            info.db_mod_time = Some(db_mod_time);
        } else {
            info.db_mod_time = None;
        }

        // total bytes of real WAL.
        let wal_file = fs::metadata(&self.wal_file)?;
        let wal_size = align_frame(self.page_size, wal_file.len());
        info.wal_size = wal_size;

        // get current shadow wal index
        let (index, _total_size) = self.current_shadow_index(&generation)?;
        if index > MAX_WAL_INDEX {
            info.reason = Some("max index exceeded".to_string());
            return Ok(info);
        }
        info.shadow_wal_file = self.shadow_wal_file(&generation, index);

        // Determine shadow WAL current size.
        if !fs::exists(&info.shadow_wal_file)? {
            info.reason = Some("no shadow wal".to_string());
            return Ok(info);
        }
        let shadow_wal_file = fs::metadata(&info.shadow_wal_file)?;
        info.shadow_wal_size = align_frame(self.page_size, shadow_wal_file.len());
        if info.shadow_wal_size < WAL_HEADER_SIZE {
            info.reason = Some("short shadow wal".to_string());
            return Ok(info);
        }

        if info.shadow_wal_size > info.wal_size {
            info.reason = Some("wal truncated by another process".to_string());
            return Ok(info);
        }

        // Compare WAL headers. Start a new shadow WAL if they are mismatched.
        let wal_header = WALHeader::read(&self.wal_file)?;
        let shadow_wal_header = WALHeader::read(&info.shadow_wal_file)?;
        if wal_header != shadow_wal_header {
            info.restart = true;
        }

        if info.shadow_wal_size == WAL_HEADER_SIZE && info.restart {
            info.reason = Some("wal header only, mismatched".to_string());
            return Ok(info);
        }

        // Verify last page synced still matches.
        if info.shadow_wal_size > WAL_HEADER_SIZE {
            let offset = info.shadow_wal_size - self.page_size - WAL_FRAME_HEADER_SIZE;

            let mut wal_file = OpenOptions::new().read(true).open(&self.wal_file)?;
            wal_file.seek(SeekFrom::Start(offset))?;
            let wal_last_frame = WALFrame::read(&mut wal_file, self.page_size)?;

            let mut shadow_wal_file = OpenOptions::new().read(true).open(&info.shadow_wal_file)?;
            shadow_wal_file.seek(SeekFrom::Start(offset))?;
            let shadow_wal_last_frame = WALFrame::read(&mut shadow_wal_file, self.page_size)?;
            if wal_last_frame != shadow_wal_last_frame {
                debug!(
                    "db {} verify offset: {}, shadow_wal_file: {}, shadow salt1: {}, wal file: {}, wal salt1: {} last frame mismatched",
                    self.config.db,
                    offset,
                    info.shadow_wal_file,
                    shadow_wal_last_frame.salt1,
                    self.wal_file,
                    wal_last_frame.salt1
                );
                info.reason = Some("wal overwritten by another process".to_string());
                return Ok(info);
            }
        }

        Ok(info)
    }

    pub async fn handle_db_command(&mut self, cmd: DbCommand) -> Result<()> {
        match cmd {
            DbCommand::Snapshot(i) => self.handle_db_snapshot_command(i).await?,
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.release_read_lock();
    }
}

pub async fn run_database(config: DbConfig) -> Result<()> {
    let (mut database, mut db_receiver) = match Database::try_create(config.clone()).await {
        Ok((db, receiver)) => (db, receiver),
        Err(e) => {
            error!("run_database for {config:?} error: {e:?}");
            return Err(e);
        }
    };
    loop {
        select! {
            cmd = db_receiver.recv() => {
                 if let Some(cmd) = cmd && let Err(e) = database.handle_db_command(cmd).await {
                     error!("handle_db_command of db {} error: {:?}", database.config.db, e);
                    }
            }
            _ = sleep(DEFAULT_MONITOR_INTERVAL) => {
                if let Err(e) = database.sync().await {
                    error!("sync db {} error: {:?}", database.config.db, e);
                }
            }
        }
    }
}

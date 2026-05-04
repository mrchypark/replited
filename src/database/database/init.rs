use super::*;

impl Database {
    pub(super) fn init_params(db: &str, connection: &Connection) -> Result<()> {
        let max_try_num = 10;
        // busy timeout
        connection.busy_timeout(Duration::from_secs(1))?;

        let mut try_num = 0;
        while try_num < max_try_num {
            try_num += 1;
            // PRAGMA journal_mode = wal;
            if let Err(e) =
                connection.pragma_update_and_check(None, "journal_mode", "WAL", |_param| {
                    // journal_mode param debug logging removed
                    Ok(())
                })
            {
                error!("set journal_mode=wal error: {e:?}");
                continue;
            }
            try_num = 0;
            break;
        }
        if try_num >= max_try_num {
            error!("try set journal_mode=wal failed");
            return Err(Error::SqliteError(format!(
                "set journal_mode=wal for db {db} failed",
            )));
        }

        let mut try_num = 0;
        while try_num < max_try_num {
            try_num += 1;
            // PRAGMA wal_autocheckpoint = 0;
            if let Err(e) =
                connection.pragma_update_and_check(None, "wal_autocheckpoint", "0", |_param| {
                    // wal_autocheckpoint param debug logging removed
                    Ok(())
                })
            {
                error!("set wal_autocheckpoint=0 error: {e:?}");
                continue;
            }
            try_num = 0;
            break;
        }
        if try_num >= max_try_num {
            error!("try set wal_autocheckpoint=0 failed");
            return Err(Error::SqliteError(format!(
                "set wal_autocheckpoint=0 for db {db} failed",
            )));
        }

        Ok(())
    }

    pub(super) fn create_internal_tables(connection: &Connection) -> Result<()> {
        connection.execute(
            "CREATE TABLE IF NOT EXISTS _replited_seq (id INTEGER PRIMARY KEY, seq INTEGER);",
            (),
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS _replited_lock (id INTEGER);",
            (),
        )?;
        Ok(())
    }

    // init replited directory
    pub(super) fn init_directory(config: &DbConfig) -> Result<String> {
        let file_path = PathBuf::from(&config.db);
        let db_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                Error::InvalidPath(format!("db path {} has invalid file name", config.db))
            })?;
        let dir_path = match file_path.parent() {
            Some(p) if p == Path::new("") => Path::new("."),
            Some(p) => p,
            None => Path::new("."),
        };
        let meta_dir_path = dir_path.join(format!(".{db_name}-replited"));
        let meta_dir = format!("{}/", meta_dir_path.display());

        let abs_meta_dir = if Path::new(&meta_dir).is_absolute() {
            PathBuf::from(&meta_dir)
        } else {
            std::env::current_dir()?.join(&meta_dir)
        };

        info!("Creating meta dir at: {abs_meta_dir:?}");
        fs::create_dir_all(&abs_meta_dir)?;

        Ok(meta_dir)
    }

    pub(super) async fn try_create(config: DbConfig) -> Result<(Self, Receiver<DbCommand>)> {
        info!("start database with config: {config:?}\n");
        info!("CWD: {:?}", std::env::current_dir());
        info!("Opening connection to {}", config.db);
        log::debug!("Database::try_create opening connection");
        let connection = Connection::open(&config.db)?;
        log::debug!("Database::try_create connection opened");

        info!("Initializing params");
        Database::init_params(&config.db, &connection)?;

        info!("Creating internal tables");
        Database::create_internal_tables(&connection)?;

        let page_size_i64: i64 =
            connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
        let page_size = u64::try_from(page_size_i64)
            .map_err(|_| Error::InvalidArg(format!("invalid sqlite page_size: {page_size_i64}")))?;
        let wal_file = format!("{}-wal", config.db);

        // init path
        info!("Initializing directory");
        log::debug!("Database::try_create init directory");
        let meta_dir = Database::init_directory(&config)?;
        log::debug!("Database::try_create meta_dir: {meta_dir}");

        // init replicate
        let (db_notifier, db_receiver) = mpsc::channel(16);
        let mut sync_notifiers = Vec::with_capacity(config.replicate.len());
        let mut syncs = Vec::with_capacity(config.replicate.len());
        let info = DatabaseInfo {
            meta_dir: meta_dir.clone(),
            page_size,
        };
        let cache_root = config.cache_root_path()?;
        let db = Path::new(&config.db)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                Error::InvalidPath(format!("db path {} has invalid file name", config.db))
            })?
            .to_string();
        for (index, replicate) in config.replicate.iter().enumerate() {
            if let crate::config::StorageParams::Stream(_) = replicate.params {
                sync_notifiers.push(None);
                continue;
            }
            info!("Initializing replicate {index}");
            let (sync_notifier, sync_receiver) = mpsc::channel(16);
            let s = Replicate::new(
                replicate.clone(),
                db.clone(),
                index,
                db_notifier.clone(),
                info.clone(),
                cache_root.clone(),
            )
            .await?;
            syncs.push(s.clone());
            std::mem::drop(Replicate::start(s, sync_receiver)?);
            sync_notifiers.push(Some(sync_notifier));
        }

        let mut db = Self {
            config: config.clone(),
            connection,
            meta_dir,
            wal_file,
            page_size,
            tx_connection: None,
            sync_notifiers,
            syncs,
            last_retention_log: None,
            last_retention_floor: None,
            last_retention_tail: None,
            last_checkpointed_snapshot_mod_time: None,
        };

        info!("Acquiring read lock");
        db.acquire_read_lock()?;

        // If we have an existing shadow WAL, ensure the headers match.
        if let Err(err) = db.verify_header_match() {
            debug!(
                "db {} cannot determine last wal position, error: {:?}, clearing generation",
                db.config.db, err
            );

            if let Err(e) = fs::remove_file(generation_file_path(&db.meta_dir)) {
                error!("db {} remove generation file error: {:?}", db.config.db, e);
            }
        }

        // Clean up previous generations.
        if let Err(e) = db.clean() {
            error!(
                "db {} clean previous generations error {:?} when startup",
                db.config.db, e
            );
            return Err(e);
        }

        Ok((db, db_receiver))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::Database;
    use crate::config::{DbConfig, StorageConfig, StorageFsConfig, StorageParams};

    #[tokio::test]
    async fn database_try_create_uses_explicit_cache_root_for_archival_runtime() {
        let temp = tempdir().expect("tempdir");
        let db_path = temp.path().join("primary.db");
        let storage_root = temp.path().join("storage");
        let cache_root = temp.path().join("custom-cache");

        let config = DbConfig {
            db: db_path.to_string_lossy().to_string(),
            replicate: vec![StorageConfig {
                name: "fs".to_string(),
                params: StorageParams::Fs(Box::new(StorageFsConfig {
                    root: storage_root.to_string_lossy().to_string(),
                })),
            }],
            cache_root: Some(cache_root.to_string_lossy().to_string()),
            min_checkpoint_page_number: 1000,
            max_checkpoint_page_number: 10000,
            truncate_page_number: 500000,
            checkpoint_interval_secs: 60,
            monitor_interval_ms: 1000,
            apply_checkpoint_frame_interval: 128,
            apply_checkpoint_interval_ms: 2000,
            wal_retention_count: 10,
            max_concurrent_snapshots: 5,
        };

        let (_db, _rx) = Database::try_create(config).await.expect("create database");

        assert!(
            cache_root.exists(),
            "configured cache_root should exist after startup"
        );
        assert!(
            !temp
                .path()
                .join(".primary.db-replited")
                .join("cache")
                .exists(),
            "runtime should not fall back to hardcoded metadata cache when cache_root is configured"
        );
    }
}

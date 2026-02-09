use std::fs;

use replited::base::walsegment_file;
use replited::base::{Generation, compress_buffer, compress_file};
use replited::config::{DbConfig, RestoreOptions, StorageConfig, StorageFsConfig, StorageParams};
use replited::database::WalGenerationPos;
use replited::storage::StorageClient;
use replited::sync::run_restore;
use rusqlite::Connection;
use tempfile::tempdir;

#[tokio::test]
async fn restore_outputs_db_wal_shm_set_and_reads_latest_rows() {
    let dir = tempdir().expect("tempdir");

    // Source database used to generate snapshot + WAL segment bytes.
    let src_db_path = dir.path().join("db.db");
    let src_db_path_str = src_db_path.to_string_lossy().to_string();

    {
        let conn = Connection::open(&src_db_path_str).expect("open src db");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("enable WAL");
        let mode: String = conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("journal_mode");
        assert_eq!(mode.to_lowercase(), "wal");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .expect("create table");
        conn.execute("INSERT INTO t (v) VALUES ('one');", ())
            .expect("insert row1");
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
            .expect("checkpoint row1 into db");
    }

    // Storage backend (fs) where restore reads snapshots/wal segments.
    let storage_root = dir.path().join("storage");
    let storage_config = StorageConfig {
        name: "fs".to_string(),
        params: StorageParams::Fs(Box::new(StorageFsConfig {
            root: storage_root.to_string_lossy().to_string(),
        })),
    };
    let client = StorageClient::try_create("db.db".to_string(), storage_config.clone())
        .expect("storage client");

    let generation = Generation::new();
    let pos = WalGenerationPos {
        generation: generation.clone(),
        index: 0,
        offset: 0,
    };

    // Write snapshot (db file bytes).
    let snapshot_compressed = compress_file(&src_db_path_str).expect("compress snapshot db");
    client
        .write_snapshot(&pos, snapshot_compressed)
        .await
        .expect("write snapshot");

    // Generate WAL bytes by writing a second row without checkpointing.
    let wal_bytes = {
        let conn = Connection::open(&src_db_path_str).expect("open src db for row2");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("ensure WAL");
        let mode: String = conn
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("journal_mode");
        assert_eq!(mode.to_lowercase(), "wal");
        conn.execute("INSERT INTO t (v) VALUES ('two');", ())
            .expect("insert row2");
        fs::read(format!("{src_db_path_str}-wal")).expect("read src wal")
    };
    assert!(
        wal_bytes.len() as u64 > replited::sqlite::WAL_HEADER_SIZE,
        "wal should include header + frames"
    );
    client
        .write_wal_segment(&pos, compress_buffer(&wal_bytes).expect("compress wal"))
        .await
        .expect("write wal segment");
    let expected_wal_segment =
        storage_root.join(walsegment_file("db.db", generation.as_str(), 0, 0));
    assert!(
        fs::exists(&expected_wal_segment).expect("wal segment exists"),
        "expected wal segment at {}",
        expected_wal_segment.to_string_lossy()
    );

    // Restore into output path (non-follow).
    let output_path = dir.path().join("restored.db");
    let output_path_str = output_path.to_string_lossy().to_string();

    let config = DbConfig {
        db: "db.db".to_string(),
        replicate: vec![storage_config],
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
    let options = RestoreOptions {
        db: "db.db".to_string(),
        output: output_path_str.clone(),
        follow: false,
        interval: 1,
        timestamp: String::new(),
    };

    let restore_pos = run_restore(&config, &options)
        .await
        .expect("restore should succeed")
        .expect("non-follow restore should return a position");
    assert!(
        restore_pos.offset > 0,
        "expected non-zero restore offset (WAL applied); pos={restore_pos:?}"
    );

    assert!(fs::exists(&output_path_str).expect("db exists"));
    let wal_candidates: Vec<String> = fs::read_dir(dir.path())
        .expect("read_dir for wal candidates")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|name| name.contains("-wal"))
        .collect();
    assert!(
        fs::exists(format!("{output_path_str}-wal")).expect("wal exists"),
        "non-follow restore must output DB + WAL set; pos={restore_pos:?}; candidates={wal_candidates:?}"
    );
    assert!(
        fs::exists(format!("{output_path_str}-shm")).expect("shm exists"),
        "non-follow restore must output DB + WAL + SHM set"
    );

    // Opening should see both rows and create SHM.
    let conn = Connection::open(&output_path_str).expect("open restored db");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t;", [], |row| row.get(0))
        .expect("query restored db");
    assert_eq!(count, 2, "restored db should include WAL-applied rows");
    assert!(
        fs::exists(format!("{output_path_str}-shm")).expect("shm exists"),
        "opening restored db should create SHM"
    );
}

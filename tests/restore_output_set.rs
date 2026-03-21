use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Utc;
use replited::base::{Generation, compress_buffer, snapshot_file};
use replited::config::{DbConfig, RestoreOptions, StorageConfig, StorageFsConfig, StorageParams};
use replited::sync::run_restore;
use rusqlite::Connection;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

fn render_config(root: &Path, db_path: &Path, backup_root: &Path) -> String {
    format!(
        r#"[log]
level = "Debug"
dir = "{root}"

[[database]]
db = "{db}"
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200
truncate_page_number = 1000
checkpoint_interval_secs = 1
monitor_interval_ms = 200
wal_retention_count = 5
max_concurrent_snapshots = 2

[[database.replicate]]
name = "fs-backup"
[database.replicate.params]
type = "fs"
root = "{backup}"
"#,
        root = root.to_string_lossy(),
        db = db_path.to_string_lossy(),
        backup = backup_root.to_string_lossy(),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn table_digest(db_path: &Path, table: &str) -> String {
    let conn = Connection::open(db_path).expect("open digest db");
    let mut stmt = conn
        .prepare(&format!(
            "SELECT id, v FROM {table} ORDER BY id"
        ))
        .expect("prepare digest query");
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
            ))
        })
        .expect("query digest rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect digest rows");

    let mut hasher = Sha256::new();
    for (id, value) in rows {
        hasher.update(format!("{id}:{value}\n"));
    }
    format!("{:x}", hasher.finalize())
}

#[derive(Serialize)]
struct TestManifestWalPack {
    start_lsn: u64,
    end_lsn: u64,
    object_key: String,
    sha256: String,
    size_bytes: u64,
    lineage_id: String,
    base_snapshot_id: String,
}

#[derive(Serialize)]
struct TestGenerationManifest {
    format_version: u32,
    generation: String,
    manifest_id: String,
    lineage_id: String,
    base_snapshot_id: String,
    base_snapshot_sha256: String,
    base_snapshot: String,
    wal_packs: Vec<TestManifestWalPack>,
}

#[derive(Serialize)]
struct TestLatestPointer {
    format_version: u32,
    current_generation: String,
    current_manifest_key: String,
    current_manifest_sha256: String,
    created_at: chrono::DateTime<Utc>,
}

fn seed_manifest_artifacts(
    backup_root: &Path,
    db_name: &str,
    generation: &Generation,
    snapshot_bytes: &[u8],
    wal_bytes: &[u8],
) {
    let snapshot_key = snapshot_file(db_name, generation.as_str(), 0, 0);
    let wal_key = format!(
        "{db_name}/generations/{}/wal/ranges/0000000000_0000000000_0000000000_{:010}/0000000000_0000000000.wal.zst",
        generation.as_str(),
        wal_bytes.len()
    );
    let manifest_key = format!(
        "{db_name}/manifests/generations/{}.manifest.json",
        generation.as_str()
    );

    let snapshot_path = backup_root.join(&snapshot_key);
    let wal_path = backup_root.join(&wal_key);
    let manifest_path = backup_root.join(&manifest_key);
    let latest_path = backup_root.join(format!("{db_name}/pointers/latest.json"));

    fs::create_dir_all(snapshot_path.parent().expect("snapshot parent")).expect("snapshot dir");
    fs::create_dir_all(wal_path.parent().expect("wal parent")).expect("wal dir");
    fs::create_dir_all(manifest_path.parent().expect("manifest parent")).expect("manifest dir");
    fs::create_dir_all(latest_path.parent().expect("latest parent")).expect("latest dir");

    let compressed_snapshot = compress_buffer(snapshot_bytes).expect("compress snapshot");
    let compressed_wal = compress_buffer(wal_bytes).expect("compress wal");
    fs::write(&snapshot_path, &compressed_snapshot).expect("write snapshot");
    fs::write(&wal_path, &compressed_wal).expect("write wal");

    let lineage_id = format!("lineage-{}", generation.as_str());
    let manifest = TestGenerationManifest {
        format_version: 1,
        generation: generation.as_str().to_string(),
        manifest_id: "manifest-01".to_string(),
        lineage_id,
        base_snapshot_id: "snapshot-01".to_string(),
        base_snapshot_sha256: sha256_hex(&compressed_snapshot),
        base_snapshot: snapshot_key,
        wal_packs: vec![TestManifestWalPack {
            start_lsn: 0,
            end_lsn: wal_bytes.len() as u64,
            object_key: wal_key,
            sha256: sha256_hex(&compressed_wal),
            size_bytes: wal_bytes.len() as u64,
            lineage_id: format!("lineage-{}", generation.as_str()),
            base_snapshot_id: "snapshot-01".to_string(),
        }],
    };
    let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
    fs::write(&manifest_path, &manifest_bytes).expect("write manifest");

    let latest = TestLatestPointer {
        format_version: 1,
        current_generation: generation.as_str().to_string(),
        current_manifest_key: manifest_key,
        current_manifest_sha256: sha256_hex(&manifest_bytes),
        created_at: Utc::now(),
    };
    fs::write(
        latest_path,
        serde_json::to_vec(&latest).expect("serialize latest"),
    )
    .expect("write latest");
}

fn remove_restore_output(output_db: &Path) {
    for candidate in [
        output_db.to_path_buf(),
        output_db.with_file_name(format!(
            "{}-wal",
            output_db.file_name().unwrap().to_string_lossy()
        )),
        output_db.with_file_name(format!(
            "{}-shm",
            output_db.file_name().unwrap().to_string_lossy()
        )),
    ] {
        let _ = fs::remove_file(candidate);
    }
}

#[tokio::test]
async fn restore_outputs_db_wal_shm_set_and_reads_latest_rows() {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("replited.toml");
    let backup_root = dir.path().join("storage");

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

    let wal_bytes = {
        let conn = Connection::open(&src_db_path_str).expect("open src db for row2");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("ensure WAL");
        conn.execute("INSERT INTO t (v) VALUES ('two');", ())
            .expect("insert row2");
        fs::read(format!("{src_db_path_str}-wal")).expect("read source wal")
    };
    let snapshot_bytes = fs::read(&src_db_path).expect("read source snapshot db");
    let generation = Generation::new();
    seed_manifest_artifacts(
        &backup_root,
        "db.db",
        &generation,
        &snapshot_bytes,
        &wal_bytes,
    );

    fs::write(
        &config_path,
        render_config(dir.path(), &src_db_path, &backup_root),
    )
    .expect("write config");

    let storage_config = StorageConfig {
        name: "fs".to_string(),
        params: StorageParams::Fs(Box::new(StorageFsConfig {
            root: backup_root.to_string_lossy().to_string(),
        })),
    };

    let output_path = dir.path().join("restored.db");
    let output_path_str = output_path.to_string_lossy().to_string();
    let config = DbConfig {
        db: "db.db".to_string(),
        replicate: vec![storage_config],
        cache_root: None,
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
        truth_source: String::new(),
    };

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        remove_restore_output(&output_path);
        let _restore_pos = run_restore(&config, &options)
            .await
            .expect("restore should succeed")
            .expect("non-follow restore should return a position");
        let conn = Connection::open(&output_path_str).expect("open restored db");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM t;", [], |row| row.get(0))
            .expect("query restored db");
        drop(conn);

        if count == 2 {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for restore to include WAL-applied row; latest count={count}"
        );
        std::thread::sleep(Duration::from_millis(250));
    }

    remove_restore_output(&output_path);
    let restore_pos = run_restore(&config, &options)
        .await
        .expect("final restore should succeed")
        .expect("final restore should return a position");

    assert!(
        restore_pos.offset > 0,
        "expected non-zero restore offset (WAL applied); pos={restore_pos:?}"
    );
    assert!(fs::exists(&output_path_str).expect("db exists"));
    assert!(
        fs::exists(format!("{output_path_str}-wal")).expect("wal exists"),
        "non-follow restore must output DB + WAL set; pos={restore_pos:?}"
    );
    assert!(
        fs::exists(format!("{output_path_str}-shm")).expect("shm exists"),
        "non-follow restore must output DB + WAL + SHM set"
    );

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

#[tokio::test]
async fn restore_with_large_manifest_wal_pack_matches_source_digest() {
    let dir = tempdir().expect("tempdir");
    let backup_root = dir.path().join("storage");
    let src_db_path = dir.path().join("source.db");
    let src_db_path_str = src_db_path.to_string_lossy().to_string();

    {
        let conn = Connection::open(&src_db_path_str).expect("open source db");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("enable wal");
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL);")
            .expect("create table");
        conn.execute("INSERT INTO t (v) VALUES ('seed');", [])
            .expect("insert seed row");
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
            .expect("checkpoint seed row");
    }

    let conn = Connection::open(&src_db_path_str).expect("reopen source db");
    conn.execute_batch("PRAGMA journal_mode=WAL;")
        .expect("ensure wal");
    conn.execute_batch("PRAGMA wal_autocheckpoint=0;")
        .expect("disable autocheckpoint");
    let tx = conn.unchecked_transaction().expect("begin tx");
    for idx in 0..256_i64 {
        tx.execute(
            "INSERT INTO t (v) VALUES (?1);",
            [format!("value-{idx:04}")],
        )
        .expect("insert row");
    }
    tx.commit().expect("commit rows");

    let snapshot_bytes = fs::read(&src_db_path).expect("read source snapshot");
    let wal_bytes = fs::read(format!("{src_db_path_str}-wal")).expect("read source wal");
    drop(conn);
    let expected_digest = table_digest(&src_db_path, "t");
    let generation = Generation::new();
    seed_manifest_artifacts(
        &backup_root,
        "source.db",
        &generation,
        &snapshot_bytes,
        &wal_bytes,
    );

    let output_path = dir.path().join("restored.db");
    let output_path_str = output_path.to_string_lossy().to_string();
    let config = DbConfig {
        db: "source.db".to_string(),
        replicate: vec![StorageConfig {
            name: "fs-backup".to_string(),
            params: StorageParams::Fs(Box::new(StorageFsConfig {
                root: backup_root.to_string_lossy().to_string(),
            })),
        }],
        cache_root: Some(dir.path().join("cache").to_string_lossy().to_string()),
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
        db: "source.db".to_string(),
        output: output_path_str.clone(),
        follow: false,
        interval: 1,
        timestamp: String::new(),
        truth_source: "fs-backup".to_string(),
    };

    remove_restore_output(&output_path);
    run_restore(&config, &options)
        .await
        .expect("restore should succeed")
        .expect("restore should return position");

    let restored_digest = table_digest(&output_path, "t");
    assert_eq!(restored_digest, expected_digest);
}

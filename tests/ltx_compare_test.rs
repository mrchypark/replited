use std::fs;
use std::path::Path;

use replited::experimental::ltx_compare::{
    decode_ltx_snapshot_to_db, encode_ltx_snapshot_from_db,
};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

fn table_digest(db_path: &Path) -> String {
    let conn = Connection::open(db_path).expect("open digest db");
    let mut stmt = conn
        .prepare("SELECT id, value FROM bench ORDER BY id")
        .expect("prepare digest query");
    let rows = stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))
        .expect("query rows")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect rows");

    let mut hasher = Sha256::new();
    for (id, value) in rows {
        hasher.update(format!("{id}:{value}\n"));
    }
    format!("{:x}", hasher.finalize())
}

#[test]
fn ltx_snapshot_round_trip_preserves_digest_and_integrity() {
    let dir = tempdir().expect("tempdir");
    let source_db = dir.path().join("source.db");
    let restored_db = dir.path().join("restored.db");

    {
        let conn = Connection::open(&source_db).expect("open source db");
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .expect("enable wal");
        conn.execute_batch(
            "CREATE TABLE bench (id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
        )
        .expect("create table");
        for idx in 0..128_i64 {
            conn.execute(
                "INSERT INTO bench (id, value) VALUES (?1, ?2);",
                (idx + 1, format!("value-{idx:04}")),
            )
            .expect("insert row");
        }
    }

    let expected_digest = table_digest(&source_db);
    let encoded =
        encode_ltx_snapshot_from_db(&source_db).expect("encode ltx snapshot from source db");
    assert!(!encoded.is_empty(), "ltx snapshot bytes should not be empty");

    decode_ltx_snapshot_to_db(&encoded, &restored_db).expect("decode ltx snapshot");

    let restored_digest = table_digest(&restored_db);
    assert_eq!(restored_digest, expected_digest);

    let conn = Connection::open(&restored_db).expect("open restored db");
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .expect("integrity check");
    assert_eq!(integrity, "ok");
    assert!(
        fs::metadata(&restored_db).expect("restored metadata").len() > 0,
        "restored db file should exist"
    );
}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::error::Result;

/// Computes a hash of the database schema by reading sqlite_schema.
/// This is application-agnostic and works with any SQLite database.
pub fn compute_schema_hash(db_path: &str) -> Result<u64> {
    let conn = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| {
        crate::error::Error::StorageError(format!("Failed to open DB for schema hash: {e}"))
    })?;

    let mut stmt = conn
        .prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY name")
        .map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to prepare schema query: {e}"))
        })?;

    let mut hasher = DefaultHasher::new();

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|e| crate::error::Error::StorageError(format!("Failed to query schema: {e}")))?;

    for row in rows {
        let data = row.map_err(|e| crate::error::Error::StorageError(format!("Row error: {e}")))?;
        data.hash(&mut hasher);
    }

    Ok(hasher.finish())
}

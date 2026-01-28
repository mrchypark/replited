use std::path::Path;

use crate::base::Generation;
use crate::database::WalGenerationPos;
use crate::error::Result;

pub(super) fn read_generation(db_path: &str) -> Generation {
    let file_path = Path::new(db_path);
    let db_name = file_path.file_name().and_then(|p| p.to_str()).unwrap_or("");
    let dir_path = match file_path.parent() {
        Some(p) if p == Path::new("") => Path::new("."),
        Some(p) => p,
        None => Path::new("."),
    };
    let meta_dir = format!(
        "{}/.{}-replited/",
        dir_path.to_str().unwrap_or("."),
        db_name
    );
    let generation_file = Path::new(&meta_dir).join("generation");

    if let Ok(content) = std::fs::read_to_string(generation_file) {
        if let Ok(parsed) = Generation::try_create(content.trim()) {
            return parsed;
        }
    }

    Generation::default()
}

pub(super) fn get_local_wal_state(db_path: &str) -> Result<(WalGenerationPos, u64)> {
    let wal_path = format!("{db_path}-wal");
    if !Path::new(&wal_path).exists() {
        return Ok((
            WalGenerationPos {
                generation: read_generation(db_path),
                index: 0,
                offset: 0,
            },
            4096,
        ));
    }

    let file_len = std::fs::metadata(&wal_path)?.len();
    if file_len == 0 {
        return Ok((
            WalGenerationPos {
                generation: read_generation(db_path),
                index: 0,
                offset: 0,
            },
            4096,
        ));
    }

    let wal_header = crate::sqlite::WALHeader::read(&wal_path)?;
    let page_size = wal_header.page_size;

    Ok((
        WalGenerationPos {
            generation: read_generation(db_path),
            index: 0,
            offset: file_len,
        },
        page_size,
    ))
}

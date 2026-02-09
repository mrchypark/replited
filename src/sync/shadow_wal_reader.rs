use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::os::unix::fs::MetadataExt;

use crate::base::shadow_wal_file;
use crate::database::DatabaseInfo;
use crate::database::WalGenerationPos;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::align_frame;

pub struct ShadowWalReader {
    pub position: WalGenerationPos,
    pub file: File,
    pub left: u64,
}

impl ShadowWalReader {
    // ShadowWALReader opens a reader for a shadow WAL file at a given position.
    // If the reader is at the end of the file, it attempts to return the next file.
    //
    // The caller should check Pos() & Size() on the returned reader to check offset.
    pub fn try_create(pos: WalGenerationPos, info: &DatabaseInfo) -> Result<ShadowWalReader> {
        let reader = ShadowWalReader::new(pos.clone(), info)?;
        if reader.left > 0 {
            return Ok(reader);
        }

        // no data, try next
        let mut pos = pos;
        pos.index += 1;
        pos.offset = 0;

        match ShadowWalReader::new(pos, info) {
            Err(e) => {
                if e.code() == Error::STORAGE_NOT_FOUND {
                    return Err(Error::from_error_code(
                        Error::UNEXPECTED_EOF_ERROR,
                        "no wal shadow file".to_string(),
                    ));
                }
                Err(e)
            }
            Ok(reader) => Ok(reader),
        }
    }

    pub fn new(pos: WalGenerationPos, info: &DatabaseInfo) -> Result<ShadowWalReader> {
        let file_name = shadow_wal_file(&info.meta_dir, pos.generation.as_str(), pos.index);
        let mut file = OpenOptions::new().read(true).open(file_name)?;
        let size = align_frame(info.page_size, file.metadata()?.size());

        if pos.offset > size {
            return Err(Error::WalReaderOffsetTooHighError(format!(
                "wal reader offset {} > file size {}",
                pos.offset, size
            )));
        }

        // Move file handle to offset position.
        file.seek(SeekFrom::Start(pos.offset))?;
        let left = size - pos.offset;
        Ok(ShadowWalReader {
            position: pos,
            file,
            left,
        })
    }

    pub fn position(&self) -> WalGenerationPos {
        self.position.clone()
    }
}

impl Read for ShadowWalReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.left == 0 {
            // nothing to read
            return Ok(0);
        }
        let max = std::cmp::min(self.left as usize, buf.len());
        let n = self.file.read(&mut buf[..max])?;
        self.left = self.left.saturating_sub(n as u64);
        self.position.offset = self.position.offset.saturating_add(n as u64);

        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::tempdir;

    use crate::base::{Generation, shadow_wal_file};
    use crate::database::{DatabaseInfo, WalGenerationPos};
    use crate::sync::ShadowWalReader;

    #[test]
    fn read_allows_partial_reads_at_end_of_file() {
        let dir = tempdir().expect("tempdir");
        let meta_dir = dir.path().join(".data.db-replited");
        std::fs::create_dir_all(&meta_dir).expect("create meta_dir");
        let meta_dir_str = meta_dir.to_string_lossy().to_string();

        let generation = Generation::new();
        let wal_path = shadow_wal_file(&meta_dir_str, generation.as_str(), 0);
        if let Some(parent) = std::path::Path::new(&wal_path).parent() {
            std::fs::create_dir_all(parent).expect("create wal parent");
        }

        // Create an aligned WAL file: header (32) + 1 frame (24 + page_size).
        let page_size = 1024u64;
        let file_len = 32 + (24 + page_size) as usize;
        let mut bytes = vec![0u8; file_len];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        std::fs::write(&wal_path, &bytes).expect("write wal");

        let info = DatabaseInfo {
            meta_dir: meta_dir_str,
            page_size,
        };

        // Seek near EOF so the remaining bytes are smaller than the read buffer.
        let start_offset = (file_len as u64).saturating_sub(10);
        let pos = WalGenerationPos {
            generation,
            index: 0,
            offset: start_offset,
        };

        let mut reader = ShadowWalReader::new(pos, &info).expect("open reader");
        assert_eq!(reader.left, 10);

        let mut buf = [0u8; 32];
        let n = reader.read(&mut buf).expect("read");
        assert_eq!(n, 10);
        assert_eq!(reader.left, 0);
        assert_eq!(reader.position.offset, file_len as u64);
    }
}

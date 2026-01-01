use std::fs::File;
use std::io::ErrorKind;
use std::io::Read;

use super::from_be_bytes_at;
use crate::base::is_power_of_two;
use crate::error::Error;
use crate::error::Result;
use crate::sqlite::WAL_HEADER_BIG_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_LITTLE_ENDIAN_MAGIC;
use crate::sqlite::WAL_HEADER_SIZE;
use crate::sqlite::checksum;

#[derive(Clone, Debug, PartialEq)]
pub struct WALHeader {
    pub data: Vec<u8>,
    pub salt1: u32,
    pub salt2: u32,
    pub page_size: u64,
    pub is_big_endian: bool,
}

impl WALHeader {
    // see: https://www.sqlite.org/fileformat2.html#walformat
    pub fn read_from<R: Read + ?Sized>(reader: &mut R) -> Result<WALHeader> {
        let mut data: Vec<u8> = vec![0u8; WAL_HEADER_SIZE as usize];
        if let Err(e) = reader.read_exact(&mut data) {
            if e.kind() == ErrorKind::UnexpectedEof {
                return Err(Error::SqliteInvalidWalHeaderError(
                    "Invalid WAL frame header",
                ));
            }

            return Err(e.into());
        }

        let magic: &[u8] = &data[0..4];
        // check magic
        let is_big_endian = if magic == WAL_HEADER_BIG_ENDIAN_MAGIC {
            true
        } else if magic == WAL_HEADER_LITTLE_ENDIAN_MAGIC {
            false
        } else {
            return Err(Error::SqliteInvalidWalHeaderError(
                "Unknown WAL file header magic",
            ));
        };

        // check page size
        let page_size = from_be_bytes_at(&data, 8)? as u64;
        if !is_power_of_two(page_size) || page_size < 1024 {
            return Err(Error::SqliteInvalidWalHeaderError("Invalid page size"));
        }

        // checksum
        let (s1, s2) = checksum(&data[0..24], 0, 0, is_big_endian);
        let checksum1 = from_be_bytes_at(&data, 24)?;
        let checksum2 = from_be_bytes_at(&data, 28)?;
        if checksum1 != s1 || checksum2 != s2 {
            return Err(Error::SqliteInvalidWalHeaderError(
                "Invalid wal header checksum",
            ));
        }

        let salt1 = from_be_bytes_at(&data, 16)?;
        let salt2 = from_be_bytes_at(&data, 20)?;

        Ok(WALHeader {
            data,
            salt1,
            salt2,
            page_size,
            is_big_endian,
        })
    }

    pub fn read(file_path: &str) -> Result<WALHeader> {
        let mut file = File::open(file_path)?;

        Self::read_from(&mut file)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper to create a valid WAL header with little-endian magic
    fn create_valid_le_header(page_size: u32, salt1: u32, salt2: u32) -> Vec<u8> {
        let mut header = vec![0u8; 32];
        // Little-endian magic
        header[0..4].copy_from_slice(&WAL_HEADER_LITTLE_ENDIAN_MAGIC);
        // File format version (always 3007000 for WAL)
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());
        // Page size (big-endian in header)
        header[8..12].copy_from_slice(&page_size.to_be_bytes());
        // Checkpoint sequence number
        header[12..16].copy_from_slice(&1u32.to_be_bytes());
        // Salt1
        header[16..20].copy_from_slice(&salt1.to_be_bytes());
        // Salt2
        header[20..24].copy_from_slice(&salt2.to_be_bytes());
        // Calculate checksum (little-endian mode)
        let (s1, s2) = checksum(&header[0..24], 0, 0, false);
        header[24..28].copy_from_slice(&s1.to_be_bytes());
        header[28..32].copy_from_slice(&s2.to_be_bytes());
        header
    }

    // Helper to create a valid WAL header with big-endian magic
    fn create_valid_be_header(page_size: u32, salt1: u32, salt2: u32) -> Vec<u8> {
        let mut header = vec![0u8; 32];
        // Big-endian magic
        header[0..4].copy_from_slice(&WAL_HEADER_BIG_ENDIAN_MAGIC);
        // File format version
        header[4..8].copy_from_slice(&3007000u32.to_be_bytes());
        // Page size
        header[8..12].copy_from_slice(&page_size.to_be_bytes());
        // Checkpoint sequence number
        header[12..16].copy_from_slice(&1u32.to_be_bytes());
        // Salt1
        header[16..20].copy_from_slice(&salt1.to_be_bytes());
        // Salt2
        header[20..24].copy_from_slice(&salt2.to_be_bytes());
        // Calculate checksum (big-endian mode)
        let (s1, s2) = checksum(&header[0..24], 0, 0, true);
        header[24..28].copy_from_slice(&s1.to_be_bytes());
        header[28..32].copy_from_slice(&s2.to_be_bytes());
        header
    }

    #[test]
    fn test_read_valid_little_endian_header() {
        let header_bytes = create_valid_le_header(4096, 0x12345678, 0xABCDEF01);
        let mut cursor = Cursor::new(&header_bytes);

        let header = WALHeader::read_from(&mut cursor).unwrap();

        assert!(!header.is_big_endian);
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.salt1, 0x12345678);
        assert_eq!(header.salt2, 0xABCDEF01);
        assert_eq!(header.data, header_bytes);
    }

    #[test]
    fn test_read_valid_big_endian_header() {
        let header_bytes = create_valid_be_header(4096, 0x11223344, 0x55667788);
        let mut cursor = Cursor::new(&header_bytes);

        let header = WALHeader::read_from(&mut cursor).unwrap();

        assert!(header.is_big_endian);
        assert_eq!(header.page_size, 4096);
        assert_eq!(header.salt1, 0x11223344);
        assert_eq!(header.salt2, 0x55667788);
    }

    #[test]
    fn test_read_various_page_sizes() {
        // Test valid page sizes: 1024, 2048, 4096, 8192, 16384, 32768, 65536
        for page_size in [1024u32, 2048, 4096, 8192, 16384, 32768, 65536] {
            let header_bytes = create_valid_le_header(page_size, 0, 0);
            let mut cursor = Cursor::new(&header_bytes);

            let header = WALHeader::read_from(&mut cursor).unwrap();
            assert_eq!(
                header.page_size, page_size as u64,
                "Failed for page_size {page_size}"
            );
        }
    }

    #[test]
    fn test_reject_invalid_magic() {
        let mut header = create_valid_le_header(4096, 0, 0);
        // Corrupt magic bytes
        header[0] = 0x00;
        header[1] = 0x00;

        let mut cursor = Cursor::new(&header);
        let result = WALHeader::read_from(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_page_size_not_power_of_two() {
        let mut header = create_valid_le_header(4096, 0, 0);
        // Set page size to 1000 (not a power of two)
        header[8..12].copy_from_slice(&1000u32.to_be_bytes());
        // Recalculate checksum
        let (s1, s2) = checksum(&header[0..24], 0, 0, false);
        header[24..28].copy_from_slice(&s1.to_be_bytes());
        header[28..32].copy_from_slice(&s2.to_be_bytes());

        let mut cursor = Cursor::new(&header);
        let result = WALHeader::read_from(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_reject_page_size_too_small() {
        let mut header = create_valid_le_header(4096, 0, 0);
        // Set page size to 512 (less than 1024)
        header[8..12].copy_from_slice(&512u32.to_be_bytes());
        // Recalculate checksum
        let (s1, s2) = checksum(&header[0..24], 0, 0, false);
        header[24..28].copy_from_slice(&s1.to_be_bytes());
        header[28..32].copy_from_slice(&s2.to_be_bytes());

        let mut cursor = Cursor::new(&header);
        let result = WALHeader::read_from(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_reject_invalid_checksum() {
        let mut header = create_valid_le_header(4096, 0, 0);
        // Corrupt checksum
        header[24] = 0xFF;
        header[25] = 0xFF;

        let mut cursor = Cursor::new(&header);
        let result = WALHeader::read_from(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_reject_truncated_header() {
        let header = vec![0u8; 16]; // Only 16 bytes, need 32

        let mut cursor = Cursor::new(&header);
        let result = WALHeader::read_from(&mut cursor);

        assert!(result.is_err());
    }

    #[test]
    fn test_to_bytes_roundtrip() {
        let original_bytes = create_valid_le_header(4096, 0xDEADBEEF, 0xCAFEBABE);
        let mut cursor = Cursor::new(&original_bytes);

        let header = WALHeader::read_from(&mut cursor).unwrap();
        let roundtrip_bytes = header.to_bytes();

        assert_eq!(original_bytes, roundtrip_bytes);
    }
}

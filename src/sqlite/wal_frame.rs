use std::io::Read;

use super::WAL_FRAME_HEADER_SIZE;
use super::from_be_bytes_at;
use crate::error::Result;

#[derive(Clone, Debug, PartialEq)]
pub struct WALFrame {
    pub data: Vec<u8>,
    pub page_num: u32,
    pub db_size: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WALFrame {
    pub fn read<R: Read + ?Sized>(reader: &mut R, page_size: u64) -> Result<WALFrame> {
        let mut data: Vec<u8> = vec![0u8; (WAL_FRAME_HEADER_SIZE + page_size) as usize];
        reader.read_exact(&mut data)?;

        let page_num = from_be_bytes_at(&data, 0)?;
        let db_size = from_be_bytes_at(&data, 4)?;

        let checksum1 = from_be_bytes_at(&data, 16)?;
        let checksum2 = from_be_bytes_at(&data, 20)?;

        let salt1 = from_be_bytes_at(&data, 8)?;
        let salt2 = from_be_bytes_at(&data, 12)?;

        Ok(WALFrame {
            data,
            page_num,
            db_size,
            salt1,
            salt2,
            checksum1,
            checksum2,
        })
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.data.len());
        bytes.extend_from_slice(&self.page_num.to_be_bytes());
        bytes.extend_from_slice(&self.db_size.to_be_bytes());
        bytes.extend_from_slice(&self.salt1.to_be_bytes());
        bytes.extend_from_slice(&self.salt2.to_be_bytes());
        bytes.extend_from_slice(&self.checksum1.to_be_bytes());
        bytes.extend_from_slice(&self.checksum2.to_be_bytes());
        // The data field in WALFrame already includes the header space (WAL_FRAME_HEADER_SIZE)
        // but read_exact read the header + page data into it.
        // Wait, let's look at read() again.
        // reader.read_exact(&mut data)?;
        // data size is WAL_FRAME_HEADER_SIZE + page_size.
        // So self.data contains the raw bytes of the frame including the header.
        // However, the header fields are parsed from it.
        // If we want to send the raw frame as it appears on disk, we can just return self.data.
        // But wait, the parsed fields might be useful if we were constructing it, but here we are reading it.
        // The read method reads the raw bytes into `data`.
        // So `data` holds the exact on-disk representation.
        self.data.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // Helper to create a valid WAL frame
    fn create_frame(
        page_num: u32,
        db_size: u32,
        salt1: u32,
        salt2: u32,
        checksum1: u32,
        checksum2: u32,
        page_size: u64,
    ) -> Vec<u8> {
        let mut frame = vec![0u8; (WAL_FRAME_HEADER_SIZE + page_size) as usize];
        // Page number
        frame[0..4].copy_from_slice(&page_num.to_be_bytes());
        // Database size (in pages, or 0 if not a commit frame)
        frame[4..8].copy_from_slice(&db_size.to_be_bytes());
        // Salt1
        frame[8..12].copy_from_slice(&salt1.to_be_bytes());
        // Salt2
        frame[12..16].copy_from_slice(&salt2.to_be_bytes());
        // Checksum1
        frame[16..20].copy_from_slice(&checksum1.to_be_bytes());
        // Checksum2
        frame[20..24].copy_from_slice(&checksum2.to_be_bytes());
        // Fill page data with pattern
        for (i, byte) in frame.iter_mut().enumerate().skip(24) {
            *byte = (i % 256) as u8;
        }
        frame
    }

    #[test]
    fn test_read_frame() {
        let page_size = 4096u64;
        let frame_bytes = create_frame(
            1, 10, 0x12345678, 0xABCDEF01, 0x11111111, 0x22222222, page_size,
        );
        let mut cursor = Cursor::new(&frame_bytes);

        let frame = WALFrame::read(&mut cursor, page_size).unwrap();

        assert_eq!(frame.page_num, 1);
        assert_eq!(frame.db_size, 10);
        assert_eq!(frame.salt1, 0x12345678);
        assert_eq!(frame.salt2, 0xABCDEF01);
        assert_eq!(frame.checksum1, 0x11111111);
        assert_eq!(frame.checksum2, 0x22222222);
        assert_eq!(
            frame.data.len(),
            (WAL_FRAME_HEADER_SIZE + page_size) as usize
        );
    }

    #[test]
    fn test_read_commit_frame() {
        let page_size = 4096u64;
        // A commit frame has db_size > 0
        let frame_bytes = create_frame(5, 100, 0, 0, 0, 0, page_size);
        let mut cursor = Cursor::new(&frame_bytes);

        let frame = WALFrame::read(&mut cursor, page_size).unwrap();

        assert_eq!(frame.page_num, 5);
        assert_eq!(frame.db_size, 100); // Non-zero means commit frame
    }

    #[test]
    fn test_read_non_commit_frame() {
        let page_size = 4096u64;
        // A non-commit frame has db_size == 0
        let frame_bytes = create_frame(3, 0, 0, 0, 0, 0, page_size);
        let mut cursor = Cursor::new(&frame_bytes);

        let frame = WALFrame::read(&mut cursor, page_size).unwrap();

        assert_eq!(frame.page_num, 3);
        assert_eq!(frame.db_size, 0);
    }

    #[test]
    fn test_read_different_page_sizes() {
        for page_size in [1024u64, 2048, 4096, 8192, 16384] {
            let frame_bytes = create_frame(1, 0, 0, 0, 0, 0, page_size);
            let mut cursor = Cursor::new(&frame_bytes);

            let frame = WALFrame::read(&mut cursor, page_size).unwrap();
            assert_eq!(
                frame.data.len(),
                (WAL_FRAME_HEADER_SIZE + page_size) as usize
            );
        }
    }

    #[test]
    fn test_to_bytes_returns_original_data() {
        let page_size = 4096u64;
        let original_bytes = create_frame(
            42, 500, 0xDEADBEEF, 0xCAFEBABE, 0xABCD1234, 0x56789ABC, page_size,
        );
        let mut cursor = Cursor::new(&original_bytes);

        let frame = WALFrame::read(&mut cursor, page_size).unwrap();
        let result_bytes = frame.to_bytes();

        // to_bytes returns self.data which is the raw frame data
        assert_eq!(result_bytes, original_bytes);
    }

    #[test]
    fn test_read_truncated_frame_fails() {
        let page_size = 4096u64;
        // Create a frame that's too short
        let frame_bytes = vec![0u8; 100]; // Much less than WAL_FRAME_HEADER_SIZE + page_size

        let mut cursor = Cursor::new(&frame_bytes);
        let result = WALFrame::read(&mut cursor, page_size);

        assert!(result.is_err());
    }

    #[test]
    fn test_frame_page_data_preserved() {
        let page_size = 4096u64;
        let mut frame_bytes = create_frame(1, 0, 0, 0, 0, 0, page_size);
        // Write specific pattern to page data
        let page_data_start = WAL_FRAME_HEADER_SIZE as usize;
        for i in 0..page_size as usize {
            frame_bytes[page_data_start + i] = (i % 256) as u8;
        }

        let mut cursor = Cursor::new(&frame_bytes);
        let frame = WALFrame::read(&mut cursor, page_size).unwrap();

        // Verify page data is preserved
        for i in 0..page_size as usize {
            assert_eq!(frame.data[page_data_start + i], (i % 256) as u8);
        }
    }
}

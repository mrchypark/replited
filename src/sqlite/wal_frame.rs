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

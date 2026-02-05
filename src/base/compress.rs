use std::fs::OpenOptions;
use std::io::Cursor;
use zstd::stream::decode_all;
use zstd::stream::encode_all;

use crate::error::Result;

pub fn compress_buffer(data: &[u8]) -> Result<Vec<u8>> {
    let reader = Cursor::new(data);
    Ok(encode_all(reader, 3)?)
}

pub fn compress_file(file_name: &str) -> Result<Vec<u8>> {
    // Open db file descriptor
    let mut reader = OpenOptions::new().read(true).open(file_name)?;
    let bytes = reader.metadata()?.len() as usize;
    let _ = bytes;
    Ok(encode_all(&mut reader, 3)?)
}

pub fn decompressed_data(compressed_data: Vec<u8>) -> Result<Vec<u8>> {
    let reader = Cursor::new(compressed_data);
    Ok(decode_all(reader)?)
}

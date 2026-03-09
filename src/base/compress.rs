use sha2::{Digest, Sha256};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use zstd::stream::decode_all;
use zstd::stream::encode_all;

use crate::error::Result;

pub struct CompressedArtifactInfo {
    pub compressed_size_bytes: u64,
    pub compressed_sha256: Vec<u8>,
}

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

pub fn compress_path_to_path_and_hash(
    input_path: &Path,
    output_path: &Path,
) -> Result<CompressedArtifactInfo> {
    let mut reader = OpenOptions::new().read(true).open(input_path)?;
    let writer = File::create(output_path)?;
    let mut writer = HashingWriter::new(writer);
    zstd::stream::copy_encode(&mut reader, &mut writer, 3)?;
    let (writer, compressed_size_bytes, compressed_sha256) = writer.finish();
    writer.sync_all()?;
    Ok(CompressedArtifactInfo {
        compressed_size_bytes,
        compressed_sha256,
    })
}

pub fn decompressed_data(compressed_data: Vec<u8>) -> Result<Vec<u8>> {
    let reader = Cursor::new(compressed_data);
    Ok(decode_all(reader)?)
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            bytes_written: 0,
        }
    }

    fn finish(self) -> (W, u64, Vec<u8>) {
        (
            self.inner,
            self.bytes_written,
            self.hasher.finalize().to_vec(),
        )
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes_written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

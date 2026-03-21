use std::fs;
use std::io::Cursor;
use std::path::Path;
use std::time::SystemTime;

use anyhow::anyhow;
use litetx::{Checksum, Decoder, Encoder, Header, HeaderFlags, PageNum, PageSize, TXID};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

use crate::base::{compress_buffer, decompressed_data};
use crate::error::Result;

pub fn encode_manifest_snapshot_from_db(db_path: &Path) -> Result<Vec<u8>> {
    Ok(compress_buffer(&fs::read(db_path)?)?)
}

pub fn decode_manifest_snapshot_to_db(snapshot_bytes: &[u8], output_path: &Path) -> Result<()> {
    let restored = decompressed_data(snapshot_bytes.to_vec())?;
    fs::write(output_path, restored)?;
    Ok(())
}

pub fn encode_ltx_snapshot_from_db(db_path: &Path) -> Result<Vec<u8>> {
    let db_data = fs::read(db_path)?;
    let page_size = sqlite_page_size(db_path)?;
    if db_data.len() % page_size as usize != 0 {
        return Err(anyhow!(
            "sqlite db size {} is not aligned to page size {}",
            db_data.len(),
            page_size
        )
        .into());
    }

    let page_size = PageSize::new(page_size).map_err(|err| anyhow!("invalid page size: {err}"))?;
    let num_pages = db_data.len() / page_size.into_inner() as usize;
    let commit =
        PageNum::new(num_pages as u32).map_err(|err| anyhow!("invalid page count: {err}"))?;
    let header = Header {
        flags: HeaderFlags::COMPRESS_LZ4,
        page_size,
        commit,
        min_txid: TXID::ONE,
        max_txid: TXID::new(1).map_err(|err| anyhow!("invalid txid: {err}"))?,
        timestamp: SystemTime::now(),
        pre_apply_checksum: None,
    };

    let mut output = Vec::new();
    let mut encoder =
        Encoder::new(&mut output, &header).map_err(|err| anyhow!("create encoder: {err}"))?;
    let page_size_bytes = page_size.into_inner() as usize;
    for idx in 0..num_pages {
        let page_num =
            PageNum::new((idx + 1) as u32).map_err(|err| anyhow!("invalid page num: {err}"))?;
        let start = idx * page_size_bytes;
        let end = start + page_size_bytes;
        encoder
            .encode_page(page_num, &db_data[start..end])
            .map_err(|err| anyhow!("encode page {page_num}: {err}"))?;
    }

    let checksum = compute_db_checksum(&db_data);
    encoder
        .finish(checksum)
        .map_err(|err| anyhow!("finish ltx snapshot: {err}"))?;
    Ok(output)
}

pub fn decode_ltx_snapshot_to_db(snapshot_bytes: &[u8], output_path: &Path) -> Result<()> {
    let (mut decoder, header) = Decoder::new(Cursor::new(snapshot_bytes))
        .map_err(|err| anyhow!("create decoder: {err}"))?;
    let page_size = header.page_size.into_inner() as usize;
    let num_pages = header.commit.into_inner() as usize;
    let mut db_data = vec![0_u8; num_pages * page_size];
    let mut page_buf = vec![0_u8; page_size];

    while let Some(page_num) = decoder
        .decode_page(&mut page_buf)
        .map_err(|err| anyhow!("decode page: {err}"))?
    {
        let start = (page_num.into_inner() as usize - 1) * page_size;
        let end = start + page_size;
        db_data[start..end].copy_from_slice(&page_buf);
    }

    decoder
        .finish()
        .map_err(|err| anyhow!("finish decoder: {err}"))?;
    fs::write(output_path, db_data)?;
    Ok(())
}

fn sqlite_page_size(db_path: &Path) -> Result<u32> {
    let conn = Connection::open(db_path)?;
    let page_size: u32 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    Ok(page_size)
}

fn compute_db_checksum(data: &[u8]) -> Checksum {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let hash = u64::from_be_bytes(digest[..8].try_into().expect("8-byte digest prefix"));
    Checksum::new(hash)
}

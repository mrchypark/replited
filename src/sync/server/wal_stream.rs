use std::fs::File;
use std::io::Read;
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::pb::replication::{Handshake, WalPacket};
use crate::sqlite::{
    WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE, WALFrame, WALHeader, align_frame, checksum,
};

pub(super) async fn stream_wal(
    server: &super::ReplicationServer,
    request: Request<Handshake>,
) -> Result<Response<ReceiverStream<Result<WalPacket, Status>>>, Status> {
    let handshake = request.into_inner();
    log::debug!(
        "Received handshake: db_name={}, offset={}",
        handshake.db_name,
        handshake.offset
    );

    let db_path = server
        .db_paths
        .get(&handshake.db_name)
        .ok_or_else(|| Status::not_found(format!("Database {} not found", handshake.db_name)))?
        .clone();

    let mut offset = if handshake.offset == 0 {
        crate::sqlite::WAL_HEADER_SIZE
    } else {
        handshake.offset
    };

    let (tx, rx) = mpsc::channel(1);
    log::debug!("Primary: Created channel, spawning reader task (live WAL tail)...");

    tokio::spawn(async move {
        log::debug!("Primary: Reader task started");
        let wal_path = format!("{}-wal", db_path.display());
        let mut sent_header = false;
        let mut last_checksum: Option<(u32, u32)> = None;

        loop {
            // Read WAL header to get page size (handle WAL reset)
            let mut wal_header = match crate::sqlite::WALHeader::read(&wal_path) {
                Ok(h) => h,
                Err(e) => {
                    log::warn!("Primary: Failed to read WAL header: {e}.");
                    // If we can't read header yet (e.g. empty file at start), we can't stream.
                    // For simplicity, sleep and continue.
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let page_size = wal_header.page_size;
            let header_seed = checksum(
                &wal_header.to_bytes()[0..24],
                0,
                0,
                wal_header.is_big_endian,
            );

            // Send WAL header first if not sent or if we are at the beginning.
            if !sent_header || offset == crate::sqlite::WAL_HEADER_SIZE {
                let header_bytes = wal_header.to_bytes();
                log::debug!(
                    "Primary: Sending WAL header packet ({} bytes)",
                    header_bytes.len()
                );
                let header_packet = WalPacket {
                    payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                        header_bytes,
                    )),
                };
                if let Err(e) = tx.send(Ok(header_packet)).await {
                    log::warn!("Primary: Failed to send header: {e}");
                    return;
                }
                sent_header = true;
                last_checksum = Some(header_seed);
                // Always realign offset to start of frames after a header resend.
                offset = WAL_HEADER_SIZE;
            }

            // Check for WAL Truncation / Restart by Header Salt mismatch.
            let mut header_buf = [0u8; 32];
            let file_result = File::open(&wal_path);

            if let Ok(mut file_handle) = file_result {
                match file_handle.read_exact(&mut header_buf) {
                    Ok(_) => {
                        // WALHeader::read always reads salts as Big Endian (swapped on LE machines).
                        // We must match that behavior here to avoid false mismatches.
                        let new_salt1 = u32::from_be_bytes(header_buf[16..20].try_into().unwrap());

                        if new_salt1 != wal_header.salt1 {
                            log::info!(
                                "Primary: WAL Salt Changed! Old={:#x}, New={:#x}. Resetting...",
                                wal_header.salt1,
                                new_salt1
                            );

                            // Reload header to update "current" salts.
                            let mut cursor = std::io::Cursor::new(&header_buf);
                            if let Ok(new_header) = crate::sqlite::WALHeader::read_from(&mut cursor)
                            {
                                // Acknowledge read but header will be refreshed in next loop iteration.
                                let _ = new_header;
                            }
                            // Update salt to prevent infinite loop on salt mismatch.
                            wal_header.salt1 = new_salt1;

                            offset = WAL_HEADER_SIZE;
                            sent_header = false;
                            last_checksum = None;
                            continue;
                        }
                    }
                    Err(e) => {
                        // If we are expecting a valid WAL (offset > header), and can't read header -> Truncated/Deleted.
                        if offset > WAL_HEADER_SIZE {
                            log::warn!(
                                "Primary: Failed to read WAL Header ({e}). Assuming Truncate/Reset."
                            );
                            offset = WAL_HEADER_SIZE;
                            sent_header = false;
                            last_checksum = None;

                            // Header will be re-read at the start of the next loop iteration.
                            continue;
                        }
                    }
                }
            } else if offset > WAL_HEADER_SIZE {
                // Start from scratch check.
                log::warn!("Primary: Failed to open WAL file. Assuming Truncate/Reset.");
                offset = WAL_HEADER_SIZE;
                sent_header = false;
                last_checksum = None;
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            let wal_len = match std::fs::metadata(&wal_path) {
                Ok(meta) => align_frame(page_size, meta.len()),
                Err(e) => {
                    log::warn!("Primary: wal metadata error: {e}");
                    sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };

            if wal_len < offset {
                log::warn!(
                    "Primary: WAL Truncated! (len {wal_len} < offset {offset}). Resetting to header..."
                );

                // Reset to header.
                offset = WAL_HEADER_SIZE;
                sent_header = false;
                last_checksum = None;

                // Header will be re-read at the start of the next loop iteration.
                let mut head = [0u8; 32];
                let _ = File::open(&wal_path).and_then(|mut f| f.read_exact(&mut head));

                sleep(Duration::from_millis(100)).await;
                continue;
            }

            if wal_len == offset {
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            if let Ok(meta) = std::fs::metadata(&wal_path) {
                let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                log::debug!(
                    "Primary: streaming wal tail offset {offset} -> {wal_len} (len {wal_len}), mtime={mtime:?}"
                );
            }

            if let Ok(mut f) = std::fs::File::open(&wal_path) {
                use std::io::{Seek, SeekFrom};
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }

                while offset < wal_len {
                    let frame = match WALFrame::read(&mut f, page_size) {
                        Ok(frame) => frame,
                        Err(e) => {
                            log::warn!("Primary: WAL read error at offset {offset}: {e}");
                            break;
                        }
                    };

                    // Validate salt and checksum to avoid streaming partially written frames.
                    if frame.salt1 != wal_header.salt1 || frame.salt2 != wal_header.salt2 {
                        // Check if the header has actually changed on disk.
                        let current_header =
                            WALHeader::read(&wal_path).unwrap_or(wal_header.clone());
                        if current_header.salt1 != wal_header.salt1
                            || current_header.salt2 != wal_header.salt2
                        {
                            log::info!("Primary: WAL header changed (new salt), resetting stream");
                            sent_header = false;
                            last_checksum = None;
                            offset = WAL_HEADER_SIZE;
                            break;
                        } else {
                            log::debug!(
                                "Primary: Encountered stale frame (salt mismatch) at offset {offset}, waiting for overwrite..."
                            );
                            break;
                        }
                    }

                    let seeds = last_checksum.unwrap_or(header_seed);
                    let frame_bytes = frame.to_bytes();
                    let mut computed = checksum(
                        &frame_bytes[0..8],
                        seeds.0,
                        seeds.1,
                        wal_header.is_big_endian,
                    );
                    computed = checksum(
                        &frame_bytes[WAL_FRAME_HEADER_SIZE as usize..],
                        computed.0,
                        computed.1,
                        wal_header.is_big_endian,
                    );

                    if computed.0 != frame.checksum1 || computed.1 != frame.checksum2 {
                        let current_header =
                            WALHeader::read(&wal_path).unwrap_or(wal_header.clone());
                        if current_header.salt1 != wal_header.salt1
                            || current_header.salt2 != wal_header.salt2
                        {
                            log::info!(
                                "Primary: WAL header changed (during checksum check), resetting stream"
                            );
                            sent_header = false;
                            last_checksum = None;
                            offset = WAL_HEADER_SIZE;
                            break;
                        } else {
                            log::debug!(
                                "Primary: WAL checksum mismatch at offset {offset} (stale data?), waiting..."
                            );
                            break;
                        }
                    }

                    last_checksum = Some(computed);
                    offset += WAL_FRAME_HEADER_SIZE + page_size;
                    let packet = WalPacket {
                        payload: Some(crate::pb::replication::wal_packet::Payload::FrameData(
                            frame_bytes,
                        )),
                    };
                    if let Err(e) = tx.send(Ok(packet)).await {
                        log::warn!("Primary: Failed to send packet: {e}");
                        return;
                    }
                }
            }
        }
    });

    log::debug!("Primary: Returning stream response");
    Ok(Response::new(ReceiverStream::new(rx)))
}

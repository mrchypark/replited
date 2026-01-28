use std::time::SystemTime;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::pb::replication::snapshot_error::Code;
use crate::pb::replication::snapshot_response::Payload;
use crate::pb::replication::{SnapshotError, SnapshotRequest, SnapshotResponse};

pub(super) async fn stream_snapshot(
    server: &super::ReplicationServer,
    request: Request<SnapshotRequest>,
) -> Result<Response<ReceiverStream<Result<SnapshotResponse, Status>>>, Status> {
    let req = request.into_inner();
    let db_name = req.db_name.clone();

    let semaphore = server
        .snapshot_semaphores
        .get(&db_name)
        .ok_or_else(|| Status::not_found(format!("Database {db_name} not found")))?
        .clone();

    let db_path = server
        .db_paths
        .get(&db_name)
        .ok_or_else(|| Status::not_found(format!("Database {db_name} not found")))?
        .clone();

    // Try acquire permit
    let permit = match semaphore.try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            let (tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Ok(SnapshotResponse {
                    payload: Some(Payload::Error(SnapshotError {
                        code: Code::Busy as i32,
                        message: "Too many concurrent snapshots".to_string(),
                        retry_after_ms: 1000,
                    })),
                }))
                .await;
            return Ok(Response::new(ReceiverStream::new(rx)));
        }
    };

    let (tx, rx) = mpsc::channel(10);

    tokio::spawn(async move {
        let _permit = permit; // Hold permit until task finishes

        // 1. Checkpoint (TRUNCATE) to flush WAL to DB
        match rusqlite::Connection::open(&db_path) {
            Ok(conn) => {
                if let Err(e) = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                    log::error!("Primary: Checkpoint failed: {e}");
                }
            }
            Err(e) => {
                log::error!("Primary: Failed to open DB for checkpoint: {e}");
            }
        }

        // 2. Create temp snapshot file (zstd)
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let snapshot_path = std::env::temp_dir().join(format!("snapshot_{timestamp}.zst"));

        let compression_result = (|| -> std::io::Result<()> {
            let mut source = std::fs::File::open(&db_path)?;
            let mut target = std::fs::File::create(&snapshot_path)?;
            zstd::stream::copy_encode(&mut source, &mut target, 3)?;
            Ok(())
        })();

        match compression_result {
            Ok(_) => {
                // 3. Stream file
                match std::fs::File::open(&snapshot_path) {
                    Ok(mut f) => {
                        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunk
                        loop {
                            use std::io::Read;
                            match f.read(&mut buffer) {
                                Ok(0) => break, // EOF
                                Ok(n) => {
                                    let chunk = buffer[0..n].to_vec();
                                    if tx
                                        .send(Ok(SnapshotResponse {
                                            payload: Some(Payload::Chunk(chunk)),
                                        }))
                                        .await
                                        .is_err()
                                    {
                                        break; // Receiver dropped
                                    }
                                }
                                Err(e) => {
                                    log::error!("Primary: Failed to read snapshot file: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Primary: Failed to open snapshot file: {e}");
                    }
                }

                // Cleanup
                let _ = std::fs::remove_file(snapshot_path);
            }
            Err(e) => {
                log::error!("Primary: zstd compression failed: {e}");
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

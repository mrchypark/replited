use std::collections::HashMap;
use std::path::PathBuf;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::config::{Config, DbConfig};
use crate::pb::replication::replication_server::Replication;
use crate::pb::replication::{
    Handshake, RestoreConfig, RestoreRequest, SnapshotRequest, SnapshotResponse, WalPacket,
};

use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

mod restore_config;
mod snapshot_stream;
mod wal_stream;

pub struct ReplicationServer {
    db_paths: HashMap<String, PathBuf>,
    db_configs: HashMap<String, DbConfig>,
    snapshot_semaphores: HashMap<String, Arc<Semaphore>>,
    _connections: Arc<Mutex<Vec<rusqlite::Connection>>>, // Keep connections open to pin WAL files
}

impl ReplicationServer {
    pub fn new(config: Config) -> Self {
        let mut db_paths = HashMap::new();
        let mut db_configs = HashMap::new();
        let mut snapshot_semaphores = HashMap::new();
        let mut _connections = Vec::new();

        for db in config.database {
            let path = PathBuf::from(&db.db);
            db_paths.insert(db.db.clone(), path.clone());
            snapshot_semaphores.insert(
                db.db.clone(),
                Arc::new(Semaphore::new(db.max_concurrent_snapshots)),
            );
            db_configs.insert(db.db.clone(), db);

            // Open a persistent connection to pin the WAL file
            if let Ok(conn) = rusqlite::Connection::open(&path) {
                // Set WAL mode to ensure it's active
                if let Err(e) = conn.pragma_update(None, "journal_mode", "WAL") {
                    log::warn!(
                        "Primary: Failed to set WAL mode for {}: {}",
                        path.display(),
                        e
                    );
                }
                _connections.push(conn);
                log::debug!(
                    "Primary: Opened persistent connection for {}",
                    path.display()
                );
            } else {
                log::error!(
                    "Primary: Failed to open persistent connection for {}",
                    path.display()
                );
            }
        }
        Self {
            db_paths,
            db_configs,
            snapshot_semaphores,
            _connections: Arc::new(Mutex::new(_connections)),
        }
    }
}

#[tonic::async_trait]
impl Replication for ReplicationServer {
    type StreamWalStream = ReceiverStream<Result<WalPacket, Status>>;

    async fn get_restore_config(
        &self,
        request: Request<RestoreRequest>,
    ) -> Result<Response<RestoreConfig>, Status> {
        restore_config::get_restore_config(self, request).await
    }

    // Stream WAL by directly tailing the WAL file. Shadow WAL is bypassed.
    async fn stream_wal(
        &self,
        request: Request<Handshake>,
    ) -> Result<Response<Self::StreamWalStream>, Status> {
        wal_stream::stream_wal(self, request).await
    }

    type StreamSnapshotStream = ReceiverStream<Result<SnapshotResponse, Status>>;

    async fn stream_snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        snapshot_stream::stream_snapshot(self, request).await
    }
}

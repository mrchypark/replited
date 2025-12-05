use tonic::Streaming;
use tonic::transport::Channel;

use crate::error::Result;

use crate::sync::replication::Handshake;
use crate::sync::replication::WalPacket;
use crate::sync::replication::replication_client::ReplicationClient;

#[derive(Clone, Debug)]
pub struct StreamClient {
    client: ReplicationClient<Channel>,
}

impl StreamClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let client = ReplicationClient::connect(addr).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to connect to stream: {}", e))
        })?;

        Ok(Self { client })
    }

    pub async fn stream_wal(&self, handshake: Handshake) -> Result<Streaming<WalPacket>> {
        let mut client = self.client.clone();
        let response = client.stream_wal(handshake).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to stream wal: {}", e))
        })?;
        Ok(response.into_inner())
    }

    pub async fn get_restore_config(&self, db_name: String) -> Result<crate::config::DbConfig> {
        let mut client = self.client.clone();
        let request = crate::sync::replication::RestoreRequest { db_name };

        let mut req = tonic::Request::new(request);
        req.set_timeout(std::time::Duration::from_secs(5));

        let response = client.get_restore_config(req).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to get restore config: {}", e))
        })?;
        let config_json = response.into_inner().config_json;
        let config: crate::config::DbConfig = serde_json::from_str(&config_json).map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to parse restore config: {}", e))
        })?;
        Ok(config)
    }

    pub async fn download_snapshot(
        &self,
        db_name: String,
        output_path: &std::path::Path,
    ) -> Result<()> {
        let mut client = self.client.clone();

        loop {
            let request = crate::sync::replication::SnapshotRequest {
                db_name: db_name.clone(),
            };
            let mut stream = client
                .stream_snapshot(request)
                .await
                .map_err(|e| {
                    crate::error::Error::StorageError(format!("Failed to stream snapshot: {}", e))
                })?
                .into_inner();

            let mut file = std::fs::File::create(output_path).map_err(|e| {
                crate::error::Error::StorageError(format!("Failed to create snapshot file: {}", e))
            })?;

            let mut retry_after = None;

            while let Some(msg) = stream
                .message()
                .await
                .map_err(|e| crate::error::Error::StorageError(format!("Stream error: {}", e)))?
            {
                match msg.payload {
                    Some(crate::sync::replication::snapshot_response::Payload::Chunk(data)) => {
                        use std::io::Write;
                        file.write_all(&data).map_err(|e| {
                            crate::error::Error::StorageError(format!(
                                "Failed to write snapshot: {}",
                                e
                            ))
                        })?;
                    }
                    Some(crate::sync::replication::snapshot_response::Payload::Error(err)) => {
                        if err.code == crate::sync::replication::snapshot_error::Code::Busy as i32 {
                            retry_after =
                                Some(std::time::Duration::from_millis(err.retry_after_ms as u64));
                            break;
                        } else {
                            return Err(crate::error::Error::StorageError(format!(
                                "Snapshot error: {} ({})",
                                err.message, err.code
                            )));
                        }
                    }
                    None => {}
                }
            }

            if let Some(duration) = retry_after {
                println!(
                    "Primary busy, retrying snapshot download in {:?}...",
                    duration
                );
                tokio::time::sleep(duration).await;
                continue;
            }

            break;
        }
        Ok(())
    }
}

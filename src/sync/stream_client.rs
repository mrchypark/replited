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
}

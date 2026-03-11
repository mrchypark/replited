use tonic::Streaming;
use tonic::transport::Channel;

use crate::error::Result;

use crate::sync::replication::AckLsnRequest;
use crate::sync::replication::AckLsnResponse;
use crate::sync::replication::StreamSnapshotRequest;
use crate::sync::replication::StreamSnapshotResponse;
use crate::sync::replication::StreamWalRequest;
use crate::sync::replication::StreamWalResponse;
use crate::sync::replication::replication_client::ReplicationClient;

#[derive(Clone, Debug)]
pub struct StreamClient {
    client: ReplicationClient<Channel>,
}

impl StreamClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let client = ReplicationClient::connect(addr).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to connect to stream: {e}"))
        })?;

        Ok(Self { client })
    }

    pub async fn stream_wal(
        &self,
        request: StreamWalRequest,
    ) -> Result<Streaming<StreamWalResponse>> {
        let mut client = self.client.clone();
        let response = client
            .stream_wal(request)
            .await
            .map_err(|e| crate::error::Error::StorageError(format!("Failed to stream wal: {e}")))?;
        Ok(response.into_inner())
    }

    pub async fn stream_snapshot(
        &self,
        request: StreamSnapshotRequest,
    ) -> Result<Streaming<StreamSnapshotResponse>> {
        let mut client = self.client.clone();
        let response = client.stream_snapshot(request).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to stream snapshot: {e}"))
        })?;
        Ok(response.into_inner())
    }

    pub async fn ack_lsn(&self, ack: AckLsnRequest) -> Result<AckLsnResponse> {
        let mut client = self.client.clone();
        let response = client
            .ack_lsn(ack)
            .await
            .map_err(|e| crate::error::Error::StorageError(format!("Failed to ack lsn: {e}")))?;
        Ok(response.into_inner())
    }
}

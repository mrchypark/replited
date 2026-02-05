use tonic::Streaming;
use tonic::transport::Channel;

use crate::error::Result;

use crate::sync::replication::AckLsnV2Request;
use crate::sync::replication::AckLsnV2Response;
use crate::sync::replication::StreamSnapshotV2Request;
use crate::sync::replication::StreamSnapshotV2Response;
use crate::sync::replication::StreamWalV2Request;
use crate::sync::replication::StreamWalV2Response;
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

    pub async fn stream_wal_v2(
        &self,
        request: StreamWalV2Request,
    ) -> Result<Streaming<StreamWalV2Response>> {
        let mut client = self.client.clone();
        let response = client.stream_wal_v2(request).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to stream wal v2: {e}"))
        })?;
        Ok(response.into_inner())
    }

    pub async fn stream_snapshot_v2(
        &self,
        request: StreamSnapshotV2Request,
    ) -> Result<Streaming<StreamSnapshotV2Response>> {
        let mut client = self.client.clone();
        let response = client.stream_snapshot_v2(request).await.map_err(|e| {
            crate::error::Error::StorageError(format!("Failed to stream snapshot v2: {e}"))
        })?;
        Ok(response.into_inner())
    }

    pub async fn ack_lsn_v2(&self, ack: AckLsnV2Request) -> Result<AckLsnV2Response> {
        let mut client = self.client.clone();
        let response = client
            .ack_lsn_v2(ack)
            .await
            .map_err(|e| crate::error::Error::StorageError(format!("Failed to ack lsn: {e}")))?;
        Ok(response.into_inner())
    }
}

pub mod replication {
    tonic::include_proto!("replication");
}

#[cfg(test)]
mod tests {
    use super::replication::{
        AckLsnRequest, AckLsnResponse, SnapshotMeta, StreamSnapshotRequest, StreamSnapshotResponse,
        StreamWalRequest, StreamWalResponse, WalChunk,
    };

    #[test]
    fn canonical_stream_proto_symbols_exist() {
        let _ = std::any::TypeId::of::<StreamWalRequest>();
        let _ = std::any::TypeId::of::<WalChunk>();
        let _ = std::any::TypeId::of::<StreamWalResponse>();
        let _ = std::any::TypeId::of::<StreamSnapshotRequest>();
        let _ = std::any::TypeId::of::<SnapshotMeta>();
        let _ = std::any::TypeId::of::<StreamSnapshotResponse>();
        let _ = std::any::TypeId::of::<AckLsnRequest>();
        let _ = std::any::TypeId::of::<AckLsnResponse>();
    }
}

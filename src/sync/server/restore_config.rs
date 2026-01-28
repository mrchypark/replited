use tonic::{Request, Response, Status};

use crate::pb::replication::{RestoreConfig, RestoreRequest};

pub(super) async fn get_restore_config(
    server: &super::ReplicationServer,
    request: Request<RestoreRequest>,
) -> Result<Response<RestoreConfig>, Status> {
    let req = request.into_inner();
    log::debug!(
        "Primary: Received get_restore_config request for db: {}",
        req.db_name
    );

    let db_config = server.db_configs.get(&req.db_name).ok_or_else(|| {
        log::warn!("Primary: Database {} not found", req.db_name);
        Status::not_found(format!("Database {} not found", req.db_name))
    })?;

    let config_json = serde_json::to_string(db_config).map_err(|e| {
        log::error!("Primary: Failed to serialize config: {e}");
        Status::internal(format!("Failed to serialize config: {e}"))
    })?;

    log::debug!("Primary: Sending restore config");
    Ok(Response::new(RestoreConfig { config_json }))
}

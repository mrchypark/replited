use super::command::Command;
use crate::config::Config;
use crate::database::run_database;
use crate::error::Result;
use crate::log::init_log;

pub struct Replicate {
    config: Config,
}

impl Replicate {
    pub fn try_create(config: &str) -> Result<Box<Self>> {
        let config = Config::load(config)?;
        let log_config = config.log.clone();

        init_log(log_config)?;
        Ok(Box::new(Replicate { config }))
    }
}

#[async_trait::async_trait]
impl Command for Replicate {
    async fn run(&mut self) -> Result<()> {
        let mut handles = vec![];
        let mut db_paths = std::collections::HashMap::new();
        let mut stream_addr = None;

        for database in &self.config.database {
            let datatase = database.clone();
            let handle = tokio::spawn(async move {
                let _ = run_database(datatase).await;
            });

            handles.push(handle);

            // Check for stream replication config
            let mut has_stream = false;
            let mut has_storage = false;

            for replicate in &database.replicate {
                println!("Checking replicate: {:?}", replicate);
                match &replicate.params {
                    crate::config::StorageParams::Stream(s) => {
                        println!("Found stream config: {:?}", s);
                        has_stream = true;
                        db_paths.insert(database.db.clone(), database.db.clone());
                        if stream_addr.is_none() {
                            stream_addr = Some(s.addr.clone());
                        }
                    }
                    crate::config::StorageParams::Fs(_)
                    | crate::config::StorageParams::S3(_)
                    | crate::config::StorageParams::Gcs(_)
                    | crate::config::StorageParams::Azb(_)
                    | crate::config::StorageParams::Ftp(_) => {
                        has_storage = true;
                    }
                }
            }

            // Validate: stream replication requires a storage backend for initial snapshot
            if has_stream && !has_storage {
                return Err(crate::error::Error::InvalidConfig(format!(
                    "Database '{}' has stream replication enabled but no storage backend. \
                         Stream replication requires a storage backend (fs, s3, etc.) for initial snapshot restore.",
                    database.db
                )));
            }
        }

        if let Some(addr) = stream_addr {
            if !db_paths.is_empty() {
                use crate::pb::replication::replication_server::ReplicationServer as TonicReplicationServer;
                use crate::sync::ReplicationServer;
                use tonic::transport::Server;

                let server = ReplicationServer::new(self.config.clone());
                // Strip http:// or https:// scheme for SocketAddr parsing
                let addr_str = addr
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                let addr = addr_str.parse().map_err(|e| {
                    crate::error::Error::InvalidArg(format!("Invalid stream address: {}", e))
                })?;

                println!("Starting ReplicationServer at {}", addr);
                log::info!("Starting ReplicationServer at {}", addr);
                let handle = tokio::spawn(async move {
                    println!("ReplicationServer serving at {}", addr);
                    if let Err(e) = Server::builder()
                        .add_service(TonicReplicationServer::new(server))
                        .serve(addr)
                        .await
                    {
                        log::error!("ReplicationServer error: {}", e);
                    }
                });
                handles.push(handle);
            }
        }

        for h in handles {
            h.await.unwrap();
        }
        Ok(())
    }
}

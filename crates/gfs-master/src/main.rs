pub mod chunk_table;
pub mod election;
pub mod heartbeat;
pub mod namespace;
pub mod oplog;
pub mod replication;
pub mod rpc;

use clap::Parser;
use gfs_proto::client_master::client_master_service_server::ClientMasterServiceServer;
use gfs_proto::master_chunkserver::master_chunk_service_server::MasterChunkServiceServer;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    name = "gfs-master",
    author,
    version,
    about = "GFS Master Metadata Server"
)]
pub struct Opts {
    #[arg(long, env = "GFS_LISTEN_ADDR", default_value = "0.0.0.0:50051")]
    pub listen_addr: SocketAddr,

    #[arg(
        long,
        env = "GFS_OPLOG_PATH",
        default_value = "/tmp/gfs-master/oplog.bin"
    )]
    pub oplog_path: PathBuf,

    #[arg(long, env = "GFS_LEASE_NAME", default_value = "gfs-master-lock")]
    pub lease_name: String,

    #[arg(long, env = "GFS_POD_NAMESPACE", default_value = "default")]
    pub namespace: String,

    #[arg(long, env = "POD_NAME", default_value = "master-0")]
    pub pod_name: String,

    #[arg(long, env = "GFS_REPLICATION_FACTOR", default_value = "3")]
    pub replication_factor: usize,

    #[arg(long, env = "GFS_LEASE_DURATION_SECS", default_value = "60")]
    pub lease_duration_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let opts = Opts::parse();
    info!("Starting GFS Master on {}...", opts.listen_addr);

    let cancel_token = CancellationToken::new();

    let namespace = Arc::new(namespace::Namespace::new());
    let chunk_table = Arc::new(chunk_table::ChunkTable::new());
    let node_registry = Arc::new(chunk_table::NodeRegistry::new());
    let oplog = Arc::new(oplog::OpLog::open(&opts.oplog_path)?);

    let replication_mgr =
        replication::ReplicationManager::new(chunk_table.clone(), node_registry.clone());

    let rep_token = cancel_token.clone();
    tokio::spawn(async move {
        replication_mgr
            .run_detector_loop(Duration::from_secs(10), rep_token.clone())
            .await;
    });

    let reap_token = cancel_token.clone();
    let replication_mgr2 =
        replication::ReplicationManager::new(chunk_table.clone(), node_registry.clone());
    tokio::spawn(async move {
        replication_mgr2
            .run_reaper_loop(Duration::from_secs(5), reap_token)
            .await;
    });

    let client_service = rpc::ClientMasterServiceImpl::new(
        namespace.clone(),
        chunk_table.clone(),
        node_registry.clone(),
        oplog.clone(),
        opts.replication_factor,
    );

    let chunk_service = heartbeat::MasterChunkServiceImpl::new(
        chunk_table.clone(),
        node_registry.clone(),
        Duration::from_secs(opts.lease_duration_secs),
    );

    info!("Master gRPC listening on {}", opts.listen_addr);
    tonic::transport::Server::builder()
        .add_service(
            ClientMasterServiceServer::new(client_service)
                .max_decoding_message_size(128 * 1024 * 1024)
                .max_encoding_message_size(128 * 1024 * 1024),
        )
        .add_service(
            MasterChunkServiceServer::new(chunk_service)
                .max_decoding_message_size(128 * 1024 * 1024)
                .max_encoding_message_size(128 * 1024 * 1024),
        )
        .serve_with_shutdown(opts.listen_addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down master server...");
            cancel_token.cancel();
        })
        .await?;

    Ok(())
}

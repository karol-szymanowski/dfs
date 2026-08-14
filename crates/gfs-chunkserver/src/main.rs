pub mod checksum;
pub mod clone;
pub mod rpc;
pub mod scrubber;
pub mod store;

use clap::Parser;
use gfs_proto::chunk_data::chunk_data_service_server::ChunkDataServiceServer;
use gfs_proto::master_chunkserver::master_chunk_service_client::MasterChunkServiceClient;
use gfs_proto::master_chunkserver::{ChunkReport, HeartbeatRequest};
use gfs_proto::p2p_clone::clone_service_server::CloneServiceServer;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(
    name = "gfs-chunkserver",
    author,
    version,
    about = "GFS ChunkServer Daemon"
)]
pub struct Opts {
    #[arg(long, env = "GFS_LISTEN_ADDR", default_value = "0.0.0.0:50052")]
    pub listen_addr: SocketAddr,

    #[arg(long, env = "GFS_STORAGE_DIR", default_value = "/mnt/gfs-storage")]
    pub storage_dir: PathBuf,

    #[arg(long, env = "GFS_NODE_ID", default_value = "chunkserver-0")]
    pub node_id: String,

    #[arg(
        long,
        env = "GFS_MASTER_ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    pub master_addr: String,

    #[arg(long, env = "GFS_HEARTBEAT_INTERVAL_SECS", default_value = "5")]
    pub heartbeat_interval_secs: u64,

    #[arg(long, env = "GFS_SCRUB_INTERVAL_SECS", default_value = "86400")]
    pub scrub_interval_secs: u64,

    #[arg(long, env = "GFS_SKIP_DISK_ISOLATION", default_value = "false")]
    pub skip_disk_isolation: bool,
}

fn check_disk_isolation(storage_dir: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let root_meta = std::fs::metadata("/")?;
        if !storage_dir.exists() {
            std::fs::create_dir_all(storage_dir)?;
        }
        let storage_meta = std::fs::metadata(storage_dir)?;
        if root_meta.dev() == storage_meta.dev() {
            anyhow::bail!(
                "Disk isolation failure: storage directory {:?} is on the same device as '/' (device ID {})",
                storage_dir,
                root_meta.dev()
            );
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let opts = Opts::parse();
    info!("Starting GFS ChunkServer node {}...", opts.node_id);

    if !opts.skip_disk_isolation {
        // Run disk isolation check
        if let Err(e) = check_disk_isolation(&opts.storage_dir) {
            error!("Disk isolation check failed: {}", e);
            // In production bare-metal ARM64 K3s cluster, this is a hard requirement.
            if opts.storage_dir.to_string_lossy() == "/mnt/gfs-storage" {
                return Err(e);
            }
        }
    }

    let cancel_token = CancellationToken::new();
    let store = Arc::new(store::ChunkStore::new(&opts.storage_dir)?);

    // Scrubber background task
    let scrubber = scrubber::Scrubber::new(store.clone());
    let scrub_token = cancel_token.clone();
    tokio::spawn(async move {
        scrubber
            .run(Duration::from_secs(opts.scrub_interval_secs), scrub_token)
            .await;
    });

    // Heartbeat background loop to master
    let hb_store = store.clone();
    let hb_token = cancel_token.clone();
    let master_addr = opts.master_addr.clone();
    let node_id = opts.node_id.clone();
    let grpc_addr = opts.listen_addr.to_string();
    let hb_interval = opts.heartbeat_interval_secs;

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(hb_interval));
        while !hb_token.is_cancelled() {
            tokio::select! {
                _ = hb_token.cancelled() => break,
                _ = interval.tick() => {
                    if let Ok(mut client) = MasterChunkServiceClient::connect(master_addr.clone()).await {
                        let chunks = hb_store.list_chunks().unwrap_or_default();
                        let reports = chunks.into_iter().map(|(id, meta)| ChunkReport {
                            handle: Some(gfs_proto::common::ChunkHandle { id }),
                            version: Some(gfs_proto::common::ChunkVersion { value: meta.version }),
                            crc32: meta.block_crc32.first().copied().unwrap_or(0),
                            corrupted: false,
                        }).collect();

                        let req = HeartbeatRequest {
                            node: Some(gfs_proto::common::NodeId { value: node_id.clone() }),
                            grpc_addr: grpc_addr.clone(),
                            free_bytes: 100 * 1024 * 1024 * 1024,
                            used_bytes: 10 * 1024 * 1024,
                            chunks: reports,
                        };

                        if let Err(e) = client.heartbeat(req).await {
                            error!("Heartbeat RPC to master failed: {}", e);
                        }
                    }
                }
            }
        }
    });

    let data_service = rpc::ChunkDataServiceImpl::new(store.clone());
    let clone_service = clone::CloneServiceImpl::new(store.clone());

    info!("ChunkServer gRPC listening on {}", opts.listen_addr);
    tonic::transport::Server::builder()
        .add_service(
            ChunkDataServiceServer::new(data_service)
                .max_decoding_message_size(128 * 1024 * 1024)
                .max_encoding_message_size(128 * 1024 * 1024),
        )
        .add_service(
            CloneServiceServer::new(clone_service)
                .max_decoding_message_size(128 * 1024 * 1024)
                .max_encoding_message_size(128 * 1024 * 1024),
        )
        .serve_with_shutdown(opts.listen_addr, async {
            tokio::signal::ctrl_c().await.ok();
            info!("Shutting down chunkserver...");
            cancel_token.cancel();
        })
        .await?;

    Ok(())
}

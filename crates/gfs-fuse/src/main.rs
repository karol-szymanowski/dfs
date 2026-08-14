pub mod fs;
pub mod inode;

use clap::Parser;
use gfs_client::GfsClient;
use std::path::PathBuf;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "gfs-fuse", author, version, about = "GFS FUSE Daemon")]
pub struct Opts {
    #[arg(long, env = "GFS_MOUNT_POINT", default_value = "/mnt/gfs")]
    pub mount_point: PathBuf,

    #[arg(
        long,
        env = "GFS_MASTER_ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    pub master_addr: String,
}

fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let opts = Opts::parse();
    info!(
        "Initializing GFS FUSE mount point at {:?} via master {}",
        opts.mount_point, opts.master_addr
    );

    let rt = tokio::runtime::Runtime::new()?;
    let client = GfsClient::new(opts.master_addr);

    #[cfg(target_os = "linux")]
    {
        use fuser::MountOption;
        let fs = fs::GfsFilesystem::new(client, rt.handle().clone());
        let mount_options = vec![
            MountOption::RW,
            MountOption::FSName("gfs".to_string()),
            MountOption::AutoUnmount,
        ];
        fuser::mount2(fs, opts.mount_point, &mount_options)?;
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (rt, client);
        info!("Note: Direct FUSE mounting requires Linux kernel /dev/fuse. Run via Docker / K8s DaemonSet in production.");
    }

    Ok(())
}

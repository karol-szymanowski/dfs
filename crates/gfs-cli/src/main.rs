pub mod commands;

use clap::{Parser, Subcommand};
use gfs_client::GfsClient;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "gfs-cli", author, version, about = "GFS Admin & User CLI")]
pub struct Cli {
    #[arg(
        long,
        env = "GFS_MASTER_ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    pub master: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(about = "Upload a local file to GFS")]
    Put {
        local_path: PathBuf,
        remote_path: String,
    },
    #[command(about = "Download a file from GFS")]
    Get {
        remote_path: String,
        local_path: PathBuf,
        #[arg(long, default_value = "0")]
        offset: u64,
        #[arg(long, default_value = "0")] // 0 = read all to EOF
        length: u32,
    },
    #[command(about = "List directory contents")]
    Ls {
        #[arg(default_value = "/")]
        remote_path: String,
    },
    #[command(about = "Check cluster health")]
    Health,
    #[command(about = "Remove a file from GFS")]
    Rm { remote_path: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = GfsClient::new(cli.master.clone());

    match cli.command {
        Commands::Put {
            local_path,
            remote_path,
        } => {
            commands::put::run(&client, &local_path, &remote_path).await?;
        }
        Commands::Get {
            remote_path,
            local_path,
            offset,
            length,
        } => {
            commands::get::run(&client, &remote_path, &local_path, offset, length).await?;
        }
        Commands::Ls { remote_path } => {
            commands::ls::run(&client, &remote_path).await?;
        }
        Commands::Health => {
            commands::health::run(&cli.master).await?;
        }
        Commands::Rm { remote_path } => {
            commands::rm::run(&client, &remote_path).await?;
        }
    }

    Ok(())
}

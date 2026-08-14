use clap::Parser;
use gfs_client::GfsClient;
use serde::Serialize;
use std::time::Instant;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "gfs-bench", author, version, about = "GFS Benchmark Suite")]
pub struct Opts {
    #[arg(
        long,
        env = "GFS_MASTER_ADDR",
        default_value = "http://127.0.0.1:50051"
    )]
    pub master: String,

    #[arg(long, default_value = "100")]
    pub operations: usize,

    #[arg(long, default_value = "1048576")] // 1 MB
    pub record_size: usize,

    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize, Debug)]
pub struct BenchReport {
    pub operations: usize,
    pub record_size_bytes: usize,
    pub total_time_ms: u128,
    pub throughput_mbs: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let opts = Opts::parse();
    info!(
        "Starting benchmark with {} operations of {} bytes...",
        opts.operations, opts.record_size
    );

    let client = GfsClient::new(opts.master.clone());
    let test_file = "/bench_test.dat";
    let payload = bytes::Bytes::from(vec![0xAA; opts.record_size]);

    let _ = client.create_file(test_file).await;

    let start = Instant::now();
    let mut successful_ops = 0;

    for _ in 0..opts.operations {
        if client.append(test_file, payload.clone()).await.is_ok() {
            successful_ops += 1;
        }
    }

    let elapsed = start.elapsed();
    let total_bytes = successful_ops * opts.record_size;
    let throughput_mbs = if elapsed.as_secs_f64() > 0.0 {
        (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64()
    } else {
        0.0
    };

    let report = BenchReport {
        operations: successful_ops,
        record_size_bytes: opts.record_size,
        total_time_ms: elapsed.as_millis(),
        throughput_mbs,
    };

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("\n=== GFS Benchmark Results ===");
        println!("Successful Ops: {}", report.operations);
        println!("Record Size:    {} KB", report.record_size_bytes / 1024);
        println!("Total Time:     {} ms", report.total_time_ms);
        println!("Throughput:     {:.2} MB/s", report.throughput_mbs);
    }

    Ok(())
}

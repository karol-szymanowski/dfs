use bytes::Bytes;
use gfs_client::GfsClient;
use std::path::Path;

pub async fn run(client: &GfsClient, local_path: &Path, remote_path: &str) -> anyhow::Result<()> {
    let data = std::fs::read(local_path)?;
    client.create_file(remote_path).await?;
    let offset = client.append(remote_path, Bytes::from(data)).await?;
    println!(
        "Uploaded {:?} to {} at offset {}",
        local_path, remote_path, offset
    );
    Ok(())
}

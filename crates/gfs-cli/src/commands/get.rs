use gfs_client::GfsClient;
use std::path::Path;

pub async fn run(
    client: &GfsClient,
    remote_path: &str,
    local_path: &Path,
    offset: u64,
    length: u32,
) -> anyhow::Result<()> {
    let data = client.read(remote_path, offset, length).await?;
    std::fs::write(local_path, &data)?;
    println!(
        "Downloaded {} bytes from {} to {:?}",
        data.len(),
        remote_path,
        local_path
    );
    Ok(())
}

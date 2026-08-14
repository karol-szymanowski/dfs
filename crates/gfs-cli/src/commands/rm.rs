use gfs_client::GfsClient;

pub async fn run(client: &GfsClient, remote_path: &str) -> anyhow::Result<()> {
    client.delete(remote_path).await?;
    println!("Deleted {}", remote_path);
    Ok(())
}

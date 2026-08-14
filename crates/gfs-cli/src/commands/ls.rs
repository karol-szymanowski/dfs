use gfs_client::GfsClient;

pub async fn run(client: &GfsClient, remote_path: &str) -> anyhow::Result<()> {
    let entries = client.list(remote_path).await?;
    println!("Entries in {}:", remote_path);
    for entry in entries {
        println!("  {}", entry);
    }
    Ok(())
}

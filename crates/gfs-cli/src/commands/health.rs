use gfs_proto::client_master::client_master_service_client::ClientMasterServiceClient;
use gfs_proto::client_master::PathRequest;

pub async fn run(master_addr: &str) -> anyhow::Result<()> {
    let mut client = ClientMasterServiceClient::connect(master_addr.to_string()).await?;
    let res = client
        .get_file_info(PathRequest {
            path: "/".to_string(),
        })
        .await;

    match res {
        Ok(_) => {
            println!("Cluster Status: HEALTHY");
            println!("Connected to Master at {}", master_addr);
        }
        Err(e) => {
            println!("Cluster Status: DEGRADED / UNHEALTHY");
            println!("Master error: {}", e);
        }
    }
    Ok(())
}

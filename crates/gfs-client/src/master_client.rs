use gfs_proto::client_master::client_master_service_client::ClientMasterServiceClient;
use gfs_proto::client_master::{
    AllocateChunkRequest, ChunkLocationsResponse, CreateFileRequest, Empty, FileInfo,
    ListDirectoryResponse, PathRequest,
};
use gfs_proto::common::ChunkHandle;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Status;
use tracing::warn;

#[derive(Clone)]
pub struct MasterClient {
    master_addr: String,
}

impl MasterClient {
    pub fn new(master_addr: String) -> Self {
        Self { master_addr }
    }

    async fn connect(&self) -> Result<ClientMasterServiceClient<Channel>, Status> {
        let mut retries = 3;
        let mut delay = Duration::from_millis(100);

        loop {
            match ClientMasterServiceClient::connect(self.master_addr.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    retries -= 1;
                    if retries == 0 {
                        return Err(Status::unavailable(format!(
                            "Failed to connect to master at {}: {}",
                            self.master_addr, e
                        )));
                    }
                    warn!("Master connect failed, retrying in {:?}: {}", delay, e);
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
            }
        }
    }

    pub async fn create_file(&self, path: &str) -> Result<bool, Status> {
        let mut client = self.connect().await?;
        let res = client
            .create_file(CreateFileRequest {
                path: path.to_string(),
            })
            .await?;
        Ok(res.into_inner().success)
    }

    pub async fn get_file_info(&self, path: &str) -> Result<FileInfo, Status> {
        let mut client = self.connect().await?;
        let res = client
            .get_file_info(PathRequest {
                path: path.to_string(),
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn get_chunk_locations(&self, handle: u64) -> Result<ChunkLocationsResponse, Status> {
        let mut client = self.connect().await?;
        let res = client
            .get_chunk_locations(ChunkHandle { id: handle })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn allocate_chunk(
        &self,
        path: &str,
        chunk_index: u32,
    ) -> Result<ChunkLocationsResponse, Status> {
        let mut client = self.connect().await?;
        let res = client
            .allocate_chunk(AllocateChunkRequest {
                path: path.to_string(),
                chunk_index,
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn list_directory(&self, path: &str) -> Result<ListDirectoryResponse, Status> {
        let mut client = self.connect().await?;
        let res = client
            .list_directory(PathRequest {
                path: path.to_string(),
            })
            .await?;
        Ok(res.into_inner())
    }

    pub async fn delete_file(&self, path: &str) -> Result<Empty, Status> {
        let mut client = self.connect().await?;
        let res = client
            .delete_file(PathRequest {
                path: path.to_string(),
            })
            .await?;
        Ok(res.into_inner())
    }
}

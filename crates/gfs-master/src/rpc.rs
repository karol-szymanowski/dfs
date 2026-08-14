use crate::chunk_table::{ChunkMetadata, ChunkTable, NodeRegistry};
use crate::namespace::Namespace;
use crate::oplog::OpLog;
use gfs_proto::client_master::client_master_service_server::ClientMasterService;
use gfs_proto::client_master::{
    AllocateChunkRequest, ChunkLocationsResponse, CreateFileRequest, CreateFileResponse, Empty,
    FileInfo, ListDirectoryResponse, PathRequest, SyncLogEntry, SyncLogRequest,
};
use gfs_proto::common::{ChunkHandle, ChunkLocation, ChunkVersion, NodeId, Timestamp};
use std::collections::HashSet;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tracing::instrument;

pub struct ClientMasterServiceImpl {
    pub namespace: Arc<Namespace>,
    pub chunk_table: Arc<ChunkTable>,
    pub node_registry: Arc<NodeRegistry>,
    pub oplog: Arc<OpLog>,
    pub replication_factor: usize,
}

impl ClientMasterServiceImpl {
    pub fn new(
        namespace: Arc<Namespace>,
        chunk_table: Arc<ChunkTable>,
        node_registry: Arc<NodeRegistry>,
        oplog: Arc<OpLog>,
        replication_factor: usize,
    ) -> Self {
        Self {
            namespace,
            chunk_table,
            node_registry,
            oplog,
            replication_factor,
        }
    }
}

#[tonic::async_trait]
impl ClientMasterService for ClientMasterServiceImpl {
    #[instrument(skip(self, request))]
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        let path = Path::new(&req.path);
        match self.namespace.create_file(path) {
            Ok(()) => Ok(Response::new(CreateFileResponse { success: true })),
            Err(e) => Err(Status::already_exists(e.to_string())),
        }
    }

    #[instrument(skip(self, request))]
    async fn get_file_info(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<FileInfo>, Status> {
        let req = request.into_inner();
        let path = Path::new(&req.path);
        let meta = self
            .namespace
            .get_file_info(path)
            .map_err(|e| Status::not_found(e.to_string()))?;

        Ok(Response::new(FileInfo {
            path: req.path,
            size: meta.size,
            chunks: meta
                .chunks
                .into_iter()
                .map(|id| ChunkHandle { id })
                .collect(),
            mtime: Some(Timestamp::from(meta.mtime)),
            ctime: Some(Timestamp::from(meta.ctime)),
            is_directory: meta.is_directory,
        }))
    }

    #[instrument(skip(self, request))]
    async fn get_chunk_locations(
        &self,
        request: Request<ChunkHandle>,
    ) -> Result<Response<ChunkLocationsResponse>, Status> {
        let req = request.into_inner();
        let chunk_meta = self
            .chunk_table
            .get(req.id)
            .ok_or_else(|| Status::not_found(format!("Chunk {} not found", req.id)))?;

        let mut locations = Vec::new();
        for node_id in &chunk_meta.locations {
            if let Some(entry) = self.node_registry.inner.get(node_id) {
                locations.push(ChunkLocation {
                    node: Some(NodeId {
                        value: node_id.clone(),
                    }),
                    grpc_addr: entry.addr.clone(),
                });
            }
        }

        let primary = chunk_meta.primary.as_ref().and_then(|p_id| {
            self.node_registry
                .inner
                .get(p_id)
                .map(|entry| ChunkLocation {
                    node: Some(NodeId {
                        value: p_id.clone(),
                    }),
                    grpc_addr: entry.addr.clone(),
                })
        });

        let lease_expiry = chunk_meta
            .lease_expiry
            .map(|exp| {
                let now = std::time::Instant::now();
                if exp > now {
                    (exp - now).as_millis() as i64
                } else {
                    0
                }
            })
            .unwrap_or(0);

        Ok(Response::new(ChunkLocationsResponse {
            locations,
            primary,
            version: Some(ChunkVersion {
                value: chunk_meta.version,
            }),
            lease_expiry_unix_millis: lease_expiry,
            handle: Some(ChunkHandle { id: req.id }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn allocate_chunk(
        &self,
        request: Request<AllocateChunkRequest>,
    ) -> Result<Response<ChunkLocationsResponse>, Status> {
        let req = request.into_inner();
        let path = Path::new(&req.path);

        let nodes = self
            .node_registry
            .pick_least_loaded(self.replication_factor, Duration::from_secs(20));
        if nodes.is_empty() {
            return Err(Status::resource_exhausted("No chunkservers available"));
        }

        let handle = self.chunk_table.allocate_handle();
        let mut locations_set = HashSet::new();
        let mut proto_locations = Vec::new();

        for (node_id, state) in &nodes {
            locations_set.insert(node_id.clone());
            proto_locations.push(ChunkLocation {
                node: Some(NodeId {
                    value: node_id.clone(),
                }),
                grpc_addr: state.addr.clone(),
            });
        }

        let primary_id = nodes.first().map(|(id, _)| id.clone());
        let primary_loc = proto_locations.first().cloned();

        self.chunk_table.insert(
            handle,
            ChunkMetadata {
                version: 1,
                locations: locations_set,
                primary: primary_id,
                lease_expiry: Some(std::time::Instant::now() + Duration::from_secs(60)),
                pending_delete: false,
            },
        );

        self.namespace
            .append_chunk(path, handle)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ChunkLocationsResponse {
            locations: proto_locations,
            primary: primary_loc,
            version: Some(ChunkVersion { value: 1 }),
            lease_expiry_unix_millis: 60_000,
            handle: Some(ChunkHandle { id: handle }),
        }))
    }

    #[instrument(skip(self, request))]
    async fn list_directory(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<ListDirectoryResponse>, Status> {
        let req = request.into_inner();
        let path = Path::new(&req.path);
        let entries = self
            .namespace
            .list_directory(path)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let proto_entries = entries
            .into_iter()
            .map(|(p, meta)| FileInfo {
                path: p.display().to_string(),
                size: meta.size,
                chunks: meta
                    .chunks
                    .into_iter()
                    .map(|id| ChunkHandle { id })
                    .collect(),
                mtime: Some(Timestamp::from(meta.mtime)),
                ctime: Some(Timestamp::from(meta.ctime)),
                is_directory: meta.is_directory,
            })
            .collect();

        Ok(Response::new(ListDirectoryResponse {
            entries: proto_entries,
        }))
    }

    #[instrument(skip(self, request))]
    async fn delete_file(&self, request: Request<PathRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let path = Path::new(&req.path);
        let meta = self
            .namespace
            .delete_file(path)
            .map_err(|e| Status::not_found(e.to_string()))?;

        for handle in meta.chunks {
            if let Some(mut chunk_meta) = self.chunk_table.inner.get_mut(&handle) {
                chunk_meta.pending_delete = true;
            }
        }

        Ok(Response::new(Empty {}))
    }

    type SyncLogStream = Pin<Box<dyn Stream<Item = Result<SyncLogEntry, Status>> + Send + 'static>>;

    #[instrument(skip(self, _request))]
    async fn sync_log(
        &self,
        _request: Request<SyncLogRequest>,
    ) -> Result<Response<Self::SyncLogStream>, Status> {
        let empty_stream = tokio_stream::empty();
        Ok(Response::new(Box::pin(empty_stream)))
    }
}

use bytes::Bytes;
use crc32fast::Hasher;
use gfs_proto::chunk_data::chunk_data_service_client::ChunkDataServiceClient;
use gfs_proto::chunk_data::{DataPacket, ReadRequest, RecordAppendRequest, WriteChunkRequest};
use gfs_proto::common::{ChunkHandle, ChunkLocation};
use thiserror::Error;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Tonic RPC error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("Tonic transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("No replicas available")]
    NoReplicas,
    #[error("Primary node not designated")]
    NoPrimary,
    #[error("Checksum mismatch during client read: expected {expected:#x}, got {computed:#x}")]
    ChecksumMismatch { expected: u32, computed: u32 },
}

pub struct ChunkPipeline;

impl ChunkPipeline {
    pub async fn push_data_to_all(
        locations: &[ChunkLocation],
        handle: u64,
        data_id: &[u8],
        data: &Bytes,
    ) -> Result<(), PipelineError> {
        let mut futures = Vec::new();
        let data_id_vec = data_id.to_vec();
        let frame_size = 1024 * 1024; // 1MB streaming frames
        let packets: Vec<DataPacket> = data
            .chunks(frame_size)
            .enumerate()
            .map(|(i, chunk_slice)| {
                let mut h = Hasher::new();
                h.update(chunk_slice);
                DataPacket {
                    data_id: data_id_vec.clone(),
                    chunk: Some(ChunkHandle { id: handle }),
                    payload: chunk_slice.to_vec(),
                    crc32: h.finalize(),
                    offset: (i * frame_size) as u64,
                }
            })
            .collect();

        for loc in locations {
            let addr = format!("http://{}", loc.grpc_addr);
            let packet_list = packets.clone();

            futures.push(tokio::spawn(async move {
                let mut client = ChunkDataServiceClient::connect(addr)
                    .await
                    .map_err(|e| tonic::Status::unavailable(e.to_string()))?
                    .max_decoding_message_size(128 * 1024 * 1024)
                    .max_encoding_message_size(128 * 1024 * 1024);
                let stream = tokio_stream::iter(packet_list);
                client.push_data(stream).await?;
                Ok::<(), tonic::Status>(())
            }));
        }

        for fut in futures {
            fut.await
                .map_err(|e| tonic::Status::internal(e.to_string()))??;
        }

        Ok(())
    }

    pub async fn write(
        primary: &ChunkLocation,
        secondaries: &[ChunkLocation],
        handle: u64,
        offset: u64,
        data: Bytes,
    ) -> Result<u32, PipelineError> {
        let data_id = Uuid::new_v4().as_bytes().to_vec();

        let mut all_locs = vec![primary.clone()];
        all_locs.extend_from_slice(secondaries);

        // 1. Parallel data push to all replicas
        Self::push_data_to_all(&all_locs, handle, &data_id, &data).await?;

        // 2. Control RPC to primary only
        let primary_addr = format!("http://{}", primary.grpc_addr);
        let mut primary_client = ChunkDataServiceClient::connect(primary_addr)
            .await?
            .max_decoding_message_size(128 * 1024 * 1024)
            .max_encoding_message_size(128 * 1024 * 1024);

        let req = WriteChunkRequest {
            data_id,
            chunk: Some(ChunkHandle { id: handle }),
            offset,
            secondaries: secondaries.to_vec(),
        };

        let resp = primary_client.write_chunk(req).await?.into_inner();
        Ok(resp.bytes_written)
    }

    pub async fn record_append(
        primary: &ChunkLocation,
        secondaries: &[ChunkLocation],
        handle: u64,
        data: Bytes,
    ) -> Result<(u64, bool), PipelineError> {
        let data_id = Uuid::new_v4().as_bytes().to_vec();

        let mut all_locs = vec![primary.clone()];
        all_locs.extend_from_slice(secondaries);

        Self::push_data_to_all(&all_locs, handle, &data_id, &data).await?;

        let primary_addr = format!("http://{}", primary.grpc_addr);
        let mut primary_client = ChunkDataServiceClient::connect(primary_addr)
            .await?
            .max_decoding_message_size(128 * 1024 * 1024)
            .max_encoding_message_size(128 * 1024 * 1024);

        let req = RecordAppendRequest {
            data_id,
            chunk: Some(ChunkHandle { id: handle }),
            secondaries: secondaries.to_vec(),
        };

        let resp = primary_client.record_append(req).await?.into_inner();
        Ok((resp.offset, resp.padded))
    }

    pub async fn read(
        locations: &[ChunkLocation],
        handle: u64,
        offset: u64,
        length: u32,
    ) -> Result<Bytes, PipelineError> {
        if locations.is_empty() {
            return Err(PipelineError::NoReplicas);
        }

        let mut last_err = None;
        for loc in locations {
            let addr = format!("http://{}", loc.grpc_addr);
            if let Ok(client) = ChunkDataServiceClient::<Channel>::connect(addr).await {
                let mut client = client
                    .max_decoding_message_size(128 * 1024 * 1024)
                    .max_encoding_message_size(128 * 1024 * 1024);
                let req = ReadRequest {
                    chunk: Some(ChunkHandle { id: handle }),
                    offset,
                    length,
                };
                match client.read(req).await {
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        let mut full_bytes = Vec::new();
                        while let Ok(Some(item)) = stream.message().await {
                            let mut hasher = Hasher::new();
                            hasher.update(&item.payload);
                            let computed = hasher.finalize();
                            if computed != item.crc32 {
                                return Err(PipelineError::ChecksumMismatch {
                                    expected: item.crc32,
                                    computed,
                                });
                            }
                            full_bytes.extend_from_slice(&item.payload);
                        }
                        return Ok(Bytes::from(full_bytes));
                    }
                    Err(e) => {
                        last_err = Some(e);
                    }
                }
            }
        }

        Err(last_err
            .map(PipelineError::Rpc)
            .unwrap_or(PipelineError::NoReplicas))
    }
}

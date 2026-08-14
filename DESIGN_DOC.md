# GFS-RS Architecture & Design Document

## 1. System Overview

**GFS-RS** is a production-grade reimplementation of the Google File System tailored for bare-metal ARM64 Raspberry Pi clusters orchestrated by K3s. It provides scalable, fault-tolerant, append-optimized distributed storage.

---

## 2. Core Components

### 2.1 Metadata Engine (`gfs-master`)
- **Namespace**: Hierarchical path mapping stored in an in-memory `RwLock<HashMap<PathBuf, FileMetadata>>`.
- **Chunk Table**: Lock-free concurrent chunk catalog via `DashMap<u64, ChunkMetadata>`.
- **Node Registry**: Tracks live chunkservers, capacity (`free_bytes`, `used_bytes`), and heartbeat timestamps.
- **Leader Election**: Active/standby high-availability via Kubernetes `coordination.k8s.io/v1` `Lease` API.
  - Active replica continuously renews the lease (5s interval).
  - Standby monitors lease status and assumes leadership if renewal expires.
- **Replication Manager**:
  - Periodically scans chunk table for under-replicated chunks (`replicas < 3`).
  - Emits `CLONE_TO` commands to healthy source nodes to replicate to the least-loaded target node.
  - Dead node reaper strips nodes missing heartbeats beyond `HEARTBEAT_TIMEOUT` (20s).
- **Operation Log (OpLog)**: Append-only persistent recovery log protected with per-entry CRC32 checks. Standby instances tail the log via streaming RPC.

### 2.2 Storage Engine (`gfs-chunkserver`)
- **Storage Layout**:
  ```
  /mnt/gfs-storage/
  └── chunks/
      └── <handle % 256>/
          └── <handle>/
              ├── chunk_<handle>.bin     # 64MB raw chunk data
              └── chunk_<handle>.meta    # Bincode ChunkMeta with block CRCs
  ```
- **Block-Granular Checksumming**: 64KB block granularity allowing ranged reads to verify only touched blocks with `crc32fast`.
- **Background Scrubber**: Periodic task scanning on-disk blocks, comparing against `.meta`, and flagging corruption to Master via heartbeat reports.
- **P2P Chunk Cloning**: Directly streams chunks between chunkservers on under-replication triggers.
- **Disk Isolation Guard**: Verifies at daemon startup via `statvfs`/`dev()` that `/mnt/gfs-storage` does not share a device with `/`.

### 2.3 Client SDK & Pipeline (`gfs-client`)
- **Decoupled Data & Control Flow**:
  1. Data is pushed concurrently to all replica chunkservers via `PushData` (optimizing LAN bandwidth).
  2. Control mutation (`WriteChunk` or `RecordAppend`) is issued strictly to the **Primary** chunkserver.
  3. Primary assigns sequence order, writes locally, and pipelines `ApplyMutation` across secondaries.
- **Location Cache**: Thread-safe TTL cache for chunk locations bounded by master lease expiry timestamps.
- **Offset Mapping**: Translates global file offsets to `(chunk_index, chunk_offset)` pairs.

### 2.4 POSIX FUSE Daemon (`gfs-fuse`)
- Implements `fuser::Filesystem`.
- Maps POSIX file operations (`getattr`, `readdir`, `read`, `write`, `unlink`) to asynchronous `gfs-client` routines using `tokio::runtime::Handle::block_on`.
- Inode table maintains bidirectional mapping between 64-bit Inodes and canonical `PathBuf` entries.

---

## 3. Communication Protocols

| Service | Proto Contract | Roles |
|---|---|---|
| `MasterChunkService` | `master_chunkserver.proto` | Chunkserver heartbeat reports, lease acquisition, master commands (`CLONE_TO`, `DELETE_CHUNK`, `INVALIDATE_CHUNK`). |
| `ClientMasterService` | `client_master.proto` | Namespace operations (`CreateFile`, `GetFileInfo`, `AllocateChunk`, `ListDirectory`, `DeleteFile`, `SyncLog`). |
| `ChunkDataService` | `chunk_data.proto` | Client/Chunkserver data pipeline (`PushData`, `WriteChunk`, `RecordAppend`, `ApplyMutation`, `Read`). |
| `CloneService` | `p2p_clone.proto` | Chunkserver-to-chunkserver P2P replica transfers. |

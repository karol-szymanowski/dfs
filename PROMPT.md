# GFS-RS — Antigravity Agent Build Charter
### Production-grade Google File System clone in Rust, for bare-metal ARM64 K3s

---

## 0. ROLE & MISSION

You are operating as an **autonomous Principal Rust Systems Engineer** inside the Antigravity agent framework. Your mission is to design, implement, test, containerize, and ship **GFS-RS** — a lightweight, production-grade reimplementation of the Google File System, written entirely in Rust, natively targeting a bare-metal Raspberry Pi ARM64 cluster orchestrated by K3s.

You will work through **six sequential phases** (Section 3). Each phase has a hard **exit gate**: you may not begin the next phase until the current phase compiles, passes `cargo clippy -- -D warnings`, and passes its own test suite. Treat this document as your spec-of-record. Where this document is silent, make the most GFS-faithful, Raspberry-Pi-appropriate engineering decision, document it in the crate's `README.md`, and proceed — do not stall waiting for clarification.

**You must never emit placeholder code.** Every function body must be real, compilable, and behaviorally correct. `todo!()`, `unimplemented!()`, `panic!("not implemented")`, and stub `Ok(())` returns that skip actual logic are all forbidden in anything under `crates/*/src` outside of `#[cfg(test)]` blocks.

---

## 1. NON-NEGOTIABLE ENGINEERING STANDARDS

Apply these rules to every crate, in every phase, with no exceptions:

| Rule | Requirement |
|---|---|
| **Panics** | No `unwrap()`, `expect()` (outside tests/benches), or `panic!()` on any I/O, network, lock, or parse path. Every fallible call returns `Result`. |
| **Error handling** | Library crates (`gfs-proto`, `gfs-master`, `gfs-chunkserver`, `gfs-client`) define typed errors with `thiserror`. Binary crates (`gfs-fuse`, `gfs-cli`) aggregate with `anyhow::Result` at the `main()` boundary only. |
| **Buffers** | All chunk payloads and RPC bodies move as `bytes::Bytes` / `bytes::BytesMut`. No `Vec<u8>` clones across the client→primary→secondary pipeline. |
| **Concurrency** | `tokio` multi-threaded runtime everywhere. Every long-running task (heartbeat loop, lease renewal, scrubber, replication monitor) is spawned with a `tokio_util::sync::CancellationToken` and is `.await`-joined on shutdown — no orphaned tasks. |
| **Locking** | Master namespace/chunk-table uses `DashMap` for hot paths; anything requiring multi-key atomicity uses a single `parking_lot::RwLock` guarding a struct, never nested locks. |
| **Logging** | `tracing` spans on every RPC handler (`#[tracing::instrument]`), correlation via chunk handle / request ID. `tracing-subscriber` with `EnvFilter` + JSON formatter in release builds. |
| **Checksums** | `crc32fast` for all on-disk and on-wire chunk block integrity. Never trust a read without verifying it. |
| **Config** | All binaries take config via `clap` args + optional YAML/env override. No hardcoded ports, paths, or timeouts. |
| **Clippy** | `cargo clippy --workspace --all-targets -- -D warnings` must be clean. `cargo fmt --check` must be clean. |
| **Unsafe** | Zero `unsafe` blocks except inside `memmap2` wrapper functions, each with a `// SAFETY:` comment justifying invariants. |

---

## 2. CARGO WORKSPACE LAYOUT

```
gfs-rs/
├── Cargo.toml                  # workspace root
├── Makefile
├── README.md
├── .cargo/config.toml          # cross-compile target config (aarch64/x86_64 musl)
├── deny.toml                   # cargo-deny license/advisory policy
├── crates/
│   ├── gfs-proto/              # tonic-build generated gRPC contracts
│   │   ├── build.rs
│   │   ├── proto/
│   │   │   ├── common.proto
│   │   │   ├── master_chunkserver.proto
│   │   │   ├── client_master.proto
│   │   │   ├── chunk_data.proto
│   │   │   └── p2p_clone.proto
│   │   └── src/lib.rs
│   ├── gfs-master/             # metadata server + leader election
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── namespace.rs
│   │   │   ├── chunk_table.rs
│   │   │   ├── election.rs     # kube-rs Lease election
│   │   │   ├── heartbeat.rs
│   │   │   ├── replication.rs  # under-replication detector + reaper
│   │   │   ├── oplog.rs        # append-only recovery log
│   │   │   └── rpc.rs          # tonic service impls
│   │   └── tests/
│   ├── gfs-chunkserver/        # local disk chunk engine + gRPC service
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── store.rs        # chunk file layout, mmap I/O
│   │   │   ├── checksum.rs
│   │   │   ├── scrubber.rs
│   │   │   ├── clone.rs        # P2P clone sender/receiver
│   │   │   └── rpc.rs
│   │   └── tests/
│   ├── gfs-client/             # client library: chunk lookup + pipeline
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── master_client.rs
│   │   │   ├── chunk_pipeline.rs
│   │   │   ├── offset_map.rs
│   │   │   └── cache.rs        # chunk-location TTL cache
│   │   └── tests/
│   ├── gfs-fuse/                # FUSE daemon binary
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── fs.rs            # fuser::Filesystem impl
│   │   │   └── inode.rs
│   │   └── tests/
│   └── gfs-cli/                 # admin CLI
│       ├── src/
│       │   ├── main.rs
│       │   └── commands/{put,get,ls,health,rm}.rs
│       └── tests/
├── deploy/
│   ├── docker/
│   │   ├── Dockerfile.master
│   │   ├── Dockerfile.chunkserver
│   │   └── Dockerfile.fuse
│   └── k8s/
│       ├── rbac.yaml
│       ├── master-deployment.yaml
│       ├── master-service.yaml
│       ├── master-pdb.yaml
│       ├── chunkserver-daemonset.yaml
│       └── configmap.yaml
└── tests/
    ├── chaos/                   # multi-node simulation harness
    └── bench/                   # throughput benchmark binary
```

---

## 3. PHASE-BY-PHASE ROADMAP

### PHASE 1 — `gfs-proto`: Protobuf & RPC Contracts

Define four `.proto` files, compiled via `tonic-build` in `build.rs`. Field-level contract (implement exactly, extend only if needed):

**`common.proto`**
```protobuf
message ChunkHandle   { uint64 id = 1; }
message ChunkVersion  { uint64 value = 1; }
message NodeId        { string value = 1; }             // e.g. "chunksrv-<pod-uid>"
message ChunkLocation { NodeId node = 1; string grpc_addr = 2; }
message Timestamp     { int64 unix_millis = 1; }
```

**`master_chunkserver.proto`** — `service MasterChunkService`
- `rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)`
  `HeartbeatRequest { NodeId node = 1; uint64 free_bytes = 2; uint64 used_bytes = 3; repeated ChunkReport chunks = 4; }`
  `ChunkReport { ChunkHandle handle = 1; ChunkVersion version = 2; uint32 crc32 = 3; }`
  `HeartbeatResponse { repeated MasterCommand commands = 1; }` — commands: `INVALIDATE_CHUNK`, `CLONE_TO`, `DELETE_CHUNK`
- `rpc RequestLease(LeaseRequest) returns (LeaseResponse)` — chunkserver asks master to become primary for a chunk it already holds.

**`client_master.proto`** — `service ClientMasterService`
- `rpc CreateFile(CreateFileRequest) returns (CreateFileResponse)`
- `rpc GetFileInfo(PathRequest) returns (FileInfo)` — `FileInfo { uint64 size = 1; repeated ChunkHandle chunks = 2; Timestamp mtime = 3; }`
- `rpc GetChunkLocations(ChunkHandle) returns (ChunkLocationsResponse)` — `{ repeated ChunkLocation locations = 1; optional ChunkLocation primary = 2; ChunkVersion version = 3; int64 lease_expiry_unix_millis = 4; }`
- `rpc AllocateChunk(AllocateChunkRequest) returns (ChunkLocationsResponse)` — used on append when current last chunk is full.
- `rpc ListDirectory(PathRequest) returns (ListDirectoryResponse)`
- `rpc DeleteFile(PathRequest) returns (Empty)`

**`chunk_data.proto`** — `service ChunkDataService` (client ⇄ chunkserver, and primary ⇄ secondary)
- `rpc PushData(stream DataPacket) returns (PushDataAck)` — unordered data push to a replica, keyed by `data_id` (client-generated UUID), buffered until a control RPC references it. `DataPacket { bytes data_id = 1; ChunkHandle chunk = 2; bytes payload = 3; uint32 crc32 = 4; }`
- `rpc WriteChunk(WriteChunkRequest) returns (WriteChunkResponse)` — sent to primary only; primary forwards ordered mutation to secondaries via internal client calls to their `ApplyMutation`.
- `rpc RecordAppend(RecordAppendRequest) returns (RecordAppendResponse)` — returns the offset the primary assigned (may include `padded: bool` if the record didn't fit and chunk was padded to boundary).
- `rpc ApplyMutation(ApplyMutationRequest) returns (ApplyMutationResponse)` — primary → secondary, carries serial mutation order.
- `rpc Read(ReadRequest) returns (stream ReadChunkResponse)` — ranged read, `ReadRequest { ChunkHandle chunk = 1; uint64 offset = 2; uint32 length = 3; }`.

**`p2p_clone.proto`** — `service CloneService`
- `rpc ClonePush(stream CloneChunkRequest) returns (CloneChunkResponse)` — source chunkserver streams full chunk (data + meta) to a target chunkserver, issued when master detects under-replication.

Deliverable: all five `.proto` files, `build.rs` wired to `tonic_build::configure().build_server(true).build_client(true)`, and `lib.rs` re-exporting generated modules plus hand-written `From` impls converting proto types ⇄ internal domain types (`u64` handles, `std::net::SocketAddr`, `std::time::SystemTime`).

---

### PHASE 2 — `gfs-chunkserver`: ChunkServer Daemon

**On-disk layout** (rooted at `/mnt/gfs-storage`, config-overridable, and the daemon must refuse to start if this path is not a distinct mount point from `/`):
```
/mnt/gfs-storage/
└── chunks/
    └── <handle % 256>/               # 2-level fan-out to bound dir entries
        └── <handle>/
            ├── chunk_<handle>.bin     # raw 16MB (or partial) chunk data
            └── chunk_<handle>.meta    # bincode: ChunkMeta
```
`ChunkMeta { version: u64, size: u32, block_size: u32 (default 65536), block_crc32: Vec<u32>, created_at: SystemTime, last_scrubbed: SystemTime }`

**Requirements:**
1. `store.rs`: `ChunkStore` type wrapping `memmap2::MmapMut` for reads and `tokio::fs::File` + `write_at`-style positioned writes for mutation (never mmap for writes — avoid silent data loss on power cut on a Pi with no BBU). Every write appends to a WAL-style temp file first (`chunk_<handle>.bin.tmp`) then is fsynced and renamed atomically only for full-chunk clone; in-place range writes go through `pwrite` + explicit `fsync` at flush points, not per-byte.
2. `checksum.rs`: block-granular CRC32 (default 64KB blocks) so a partial-chunk read only needs to verify the blocks it touched, not the whole 16MB chunk. Verification failure returns a typed `ChecksumMismatch { chunk, block_index }` error which the RPC layer turns into a gRPC `DATA_LOSS` status; the master must react to this exact status by triggering re-replication (wire this handoff explicitly).
3. `scrubber.rs`: background `tokio::task` on a jittered interval (config default 24h, overridable to seconds for tests) that walks every chunk directory, recomputes all block CRCs, compares against `.meta`, and reports mismatches to the active master via the heartbeat's `ChunkReport` (send a version of `0xFFFFFFFFFFFFFFFF` sentinel or a dedicated `corrupted: bool` field — extend `ChunkReport` with `bool corrupted = 4;`).
4. `clone.rs`: implements both sides of `CloneService`. Sender streams the chunk in 1MB frames with running CRC; receiver writes to `.tmp`, verifies full-chunk CRC after last frame, then atomically renames into place and immediately sends a heartbeat-triggering `ChunkReport` so the master's replication table updates without waiting for the next tick.
5. `rpc.rs`: implements `ChunkDataService` and the chunkserver side of `MasterChunkService` (heartbeat sender is a client role — implement as a `tokio::task` in `main.rs` looping `Heartbeat` calls to the master's advertised leader address, re-resolving via K8s Service DNS or via a small watch client on the Master's Lease `holderIdentity` annotation).
6. Concurrency: one `ChunkStore` handle per chunk directory guarded by an `RwLock` (many concurrent readers, single writer per chunk at a time — matches GFS's single-primary-per-chunk model at the storage layer too).
7. **Disk isolation guard**: at startup, `main.rs` must `statvfs` both `/mnt/gfs-storage` and `/` and hard-exit with a clear error if they resolve to the same device ID (`st_dev`). This is a mandatory safety check, not optional.

---

### PHASE 3 — `gfs-master`: Metadata Engine & Leader Election

**Data structures:**
```rust
struct FileMetadata { chunks: Vec<u64>, size: u64, mtime: SystemTime, ctime: SystemTime }
struct ChunkMetadata {
    version: u64,
    locations: HashSet<NodeId>,      // current known replicas
    primary: Option<NodeId>,
    lease_expiry: Option<Instant>,
    pending_delete: bool,
}
struct Namespace { tree: RwLock<HashMap<PathBuf, FileMetadata>> }   // simple flat map keyed by canonical path is acceptable for v1; document trie upgrade path in README
struct ChunkTable { inner: DashMap<u64, ChunkMetadata> }
struct NodeRegistry { inner: DashMap<NodeId, NodeState> }  // NodeState { last_heartbeat: Instant, free_bytes: u64, addr: String }
```

1. **`election.rs`** — leader election via `kube-rs` `Lease` objects (`coordination.k8s.io/v1`), namespace-scoped, lease name configurable (default `gfs-master-lock`). Implement the standard renew/acquire loop:
   - Lease duration 15s, renew interval 5s, retry on contention with jitter.
   - `holderIdentity` = `${POD_NAME}` (from downward API env var).
   - On acquiring leadership: replay `oplog.rs` from the shared metadata volume (or from the other master pod via a `SyncNamespace` RPC if you implement master-to-master snapshot transfer — document whichever you choose), then start serving mutating RPCs.
   - On losing leadership (renew failure past deadline): immediately stop accepting mutating RPCs (`CreateFile`, `AllocateChunk`, `DeleteFile`), keep read-only RPCs alive at most a few seconds as a grace drain, then fully step down.
   - Use a `CancellationToken` per leadership term so all spawned per-term tasks (heartbeat listener bind, replication monitor, reaper) are cleanly cancelled on step-down and restarted fresh on the next acquisition — never let two terms' background tasks run concurrently.
2. **`oplog.rs`** — every namespace/chunk-table mutation is serialized (protobuf, reuse `gfs-proto` messages) and appended with `fsync` to a local file on a PersistentVolume shared/replicated between master replicas (document the chosen approach: either both pods mount the same RWX volume, or each writes locally and the standby tails the active pod's log via a `SyncLog` streaming RPC — pick the streaming RPC approach for a Pi cluster since RWX storage is often unavailable; implement it).
3. **`heartbeat.rs`** — gRPC server handling `MasterChunkService::Heartbeat`. On each report: update `NodeRegistry`, reconcile `ChunkTable` locations, clear any `MasterCommand`s that have been satisfied, and emit new commands (`CLONE_TO`, `DELETE_CHUNK` for orphaned chunks after a file delete + GC grace period).
4. **`replication.rs`** — two background loops:
   - **Under-replication detector**: every N seconds (config, default 10s), scan `ChunkTable` for chunks with `locations.len() < REPLICATION_FACTOR (3)` and no in-flight clone command already issued; pick a healthy source (random among current locations) and a target (least-loaded live node not already holding the chunk, via `NodeRegistry`); stash a `CLONE_TO` command to be delivered on that source's next heartbeat response (or push immediately via a direct RPC to the source if low-latency healing is required — implement direct push for the "kill mid-transfer" chaos test to pass quickly).
   - **Dead node reaper**: nodes with `last_heartbeat` older than `HEARTBEAT_TIMEOUT` (default 20s, 4x heartbeat interval) are removed from `NodeRegistry` and their `NodeId` stripped from every chunk's `locations`, immediately making those chunks eligible for the under-replication detector above.
5. **`rpc.rs`** — implements `ClientMasterService`. `AllocateChunk`/`CreateFile` chunk-placement policy: pick 3 distinct nodes from `NodeRegistry`, sorted by ascending `used_bytes / (free_bytes + used_bytes)`, breaking ties randomly to avoid herding. Grant the primary lease to the first placed replica for `LEASE_DURATION` (default 60s), renewable via `RequestLease` when the client's next mutation targets that chunk and the lease is within its last third of validity (standard GFS lease-extension-on-write behavior).

---

### PHASE 4 — `gfs-client` & `gfs-fuse`

**`gfs-client`**
1. `offset_map.rs`: pure function `fn chunk_index_and_offset(file_offset: u64, chunk_size: u32) -> (u32, u32)`; fully unit-tested including boundary offsets.
2. `master_client.rs`: thin typed wrapper over the generated `ClientMasterServiceClient`, with retry-with-backoff on `UNAVAILABLE` (covers master failover — must re-resolve the leader via K8s Service ClusterIP, which always routes to whichever pod holds the lease if you additionally have the losing pod fail its readiness probe on step-down; implement that readiness-probe-flip in the `election.rs` step-down path too).
3. `cache.rs`: `moka`-or-hand-rolled TTL cache (`HashMap<u64, (ChunkLocationsResponse, Instant)>` behind `RwLock` is acceptable) for chunk-location lookups, TTL bounded by `lease_expiry` when present.
4. `chunk_pipeline.rs`: implements the write path exactly as specified —
   - Client pushes identical data to **all** replicas via `PushData` (parallel, not chained — this matches real GFS's "any order, closest-first" data push, which minimizes tail latency on a LAN of Pis).
   - Client then sends the control RPC (`WriteChunk` or `RecordAppend`) to the **primary only**, referencing the `data_id`.
   - Primary assigns mutation order, applies locally, forwards `ApplyMutation` to secondaries **in pipeline order** (Primary → Secondary1 → Secondary2), collects acks, and only then acks the client.
   - On secondary failure mid-pipeline: primary returns a partial-failure status to the client; client's job is to retry the whole record append (idempotent by construction since GFS record append can duplicate/pad, matching upstream GFS semantics — document this explicitly, do not try to fake exactly-once).
   - Reads: parallel range reads issued to any replica (prefer node returned first by the master, fall back round-robin on error), with block CRC verified client-side too (defense in depth) before returning bytes to the caller.

**`gfs-fuse`**
`fs.rs` implements `fuser::Filesystem` (use the async-friendly pattern: spawn a `tokio::runtime::Handle` from `main.rs` and `block_on` per-callback bridging into async `gfs-client` calls, since `fuser`'s trait is sync):
- `lookup`, `getattr`: resolve via `inode.rs`'s bidirectional `Inode <-> PathBuf` table (`DashMap`), calling `GetFileInfo` and mapping to `fuser::FileAttr`.
- `read`: computes chunk range via `offset_map`, issues parallel chunk reads through `gfs-client`, assembles into the FUSE reply buffer.
- `write`: buffers into chunk-aligned segments client-side, calls `chunk_pipeline` per full/partial chunk; `flush`/`fsync` force any buffered partial chunk out.
- `readdir`, `mkdir`, `create`, `unlink`, `rmdir`, `rename`: map 1:1 onto `ClientMasterService` namespace RPCs.
- Mount options: `-o allow_other,default_permissions`, configurable via `gfs-fuse --mount-point /mnt/gfs --master <addr>`.

---

### PHASE 5 — Docker & Kubernetes Packaging

**Cross-compilation:** use `cargo-zigbuild` (preferred on Pi/CI for musl+aarch64 without a full cross toolchain) targeting `aarch64-unknown-linux-musl` (primary, for the Pi nodes) and `x86_64-unknown-linux-musl` (secondary, for dev-machine/CI parity). Static-link `openssl`-free — use `rustls` throughout `tonic`/`kube-rs` (`tonic::transport::Channel` with `rustls` feature, `kube` with `rustls-tls` feature) to avoid musl OpenSSL pain entirely.

**Dockerfiles** (one per binary — `Dockerfile.master`, `Dockerfile.chunkserver`, `Dockerfile.fuse`), each multi-stage:
```dockerfile
# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM rust:1.82-slim AS builder
ARG TARGETARCH
RUN apt-get update && apt-get install -y musl-tools clang && rustup target add aarch64-unknown-linux-musl x86_64-unknown-linux-musl
WORKDIR /build
COPY . .
RUN case "$TARGETARCH" in \
      arm64) TARGET=aarch64-unknown-linux-musl ;; \
      amd64) TARGET=x86_64-unknown-linux-musl ;; \
    esac && \
    cargo build --release --target $TARGET -p gfs-master && \
    cp target/$TARGET/release/gfs-master /build/out

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /build/out /usr/local/bin/gfs-master
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/gfs-master"]
```
Target final image size ≤ ~15MB per binary. `gfs-chunkserver`'s image must run as a user with access to the hostPath mount (document the UID/GID strategy — either `nonroot` matching a fixed UID you also `chown` the hostPath to via an init container, or run privileged-minimal with `fsGroup` in the pod spec).

**K8s manifests (`deploy/k8s/`):**
- `rbac.yaml`: `ServiceAccount gfs-master-sa`; `Role` granting `get/list/watch/create/update` on `leases.coordination.k8s.io` in-namespace; `RoleBinding`.
- `master-deployment.yaml`: `replicas: 2`, `podAntiAffinity` (required, by hostname) so both replicas never land on the same Pi, `serviceAccountName: gfs-master-sa`, `readinessProbe` gRPC health check that reflects current leadership state (fails readiness when standby), resource `requests`/`limits` sized for Pi 4/5 (e.g. `200m`/`256Mi` request, `500m`/`512Mi` limit), `nodeSelector: {kubernetes.io/arch: arm64}` (templated so amd64 dev clusters can override).
- `master-pdb.yaml`: `PodDisruptionBudget` `minAvailable: 1`.
- `master-service.yaml`: `ClusterIP` service selecting both replicas — readiness-gated so only the leader receives traffic.
- `chunkserver-daemonset.yaml`: `hostNetwork: true`, `hostPath` volume `/mnt/gfs-storage` (type `Directory`, and the DaemonSet must document that operators pre-provision this mount — do not auto-format disks), `tolerations` for control-plane-tainted Pi nodes if the cluster is small enough to need chunkservers there too, resource limits tuned tighter than master's (chunkservers are I/O-bound, not CPU-bound).
- `configmap.yaml`: chunk size, replication factor, heartbeat interval, lease durations — all env-injected, no rebuild required to tune.

---

### PHASE 6 — Verification & Chaos Test Suite

1. **Unit tests**: every pure function (`offset_map`, checksum block math, chunk placement scoring) gets `#[test]` coverage colocated in its module.
2. **Integration tests** (`crates/*/tests/`, `tokio::test`): spin up an in-process master (bypassing K8s Lease — inject a `LeaderElector` trait with a `StaticLeader` test impl) plus 3 in-process chunkservers on ephemeral ports and `tempfile::tempdir()` storage roots. Cover: create → write → read-back byte-for-byte + CRC match; concurrent `RecordAppend` from multiple clients producing non-overlapping records; lease expiry and re-grant.
3. **Chaos harness** (`tests/chaos/`): out-of-process simulation — spawn real `gfs-chunkserver` binaries as child processes (`std::process::Command`, one per simulated node, distinct tmp dirs/ports), drive load through `gfs-client`, then `SIGKILL` a chunkserver mid-multi-chunk-write. Assert:
   - the write pipeline either completes via the two surviving replicas or the client correctly retries and succeeds after re-placement,
   - within `HEARTBEAT_TIMEOUT + replication_scan_interval + clone_time_bound`, every affected chunk's `locations.len()` returns to 3 (poll the master's admin/debug RPC or `gfs-cli health` output),
   - no data loss: full-file checksum after healing matches the checksum computed before the kill.
   Also cover a **master failover** scenario: kill the leader master process, assert the standby acquires the Lease and resumes serving within the lease-duration bound, and that in-flight client operations retry successfully against the new leader.
4. **Benchmark binary** (`tests/bench/`, `cargo run --release --bin gfs-bench`): sequential and parallel-stream write/read throughput against a live cluster (real or `docker-compose`-simulated), reporting MB/s, p50/p99 latency per RecordAppend, and supporting an optional `tc netem` pre-hook to cap simulated link speed to gigabit for realistic Pi-cluster numbers. Output as both human-readable table and `--json` for CI trend tracking.

---

## 4. DELIVERABLE CONSTRAINTS

- **Root `Cargo.toml`**: workspace with `resolver = "2"`, all six crates as members, `[workspace.dependencies]` pinning shared versions (`tokio`, `tonic`, `prost`, `kube`, `k8s-openapi`, `fuser`, `crc32fast`, `memmap2`, `tracing`, `tracing-subscriber`, `thiserror`, `anyhow`, `bytes`, `clap`, `tokio-util`, `serde`, `bincode`), `[profile.release]` with `lto = "thin"`, `codegen-units = 1`, `strip = true` (binary size matters on a Pi).
- **`Makefile`** targets: `build`, `build-arm64`, `build-amd64`, `test`, `test-chaos`, `bench`, `fmt`, `lint` (clippy + fmt --check), `docker-build`, `docker-push`, `k3s-deploy`, `k3s-teardown`, `clean`.
- **`README.md`** covering: architecture diagram (ASCII or Mermaid), prerequisites (Rust toolchain + targets, `cargo-zigbuild`, Docker buildx, a running K3s cluster/kubeconfig), local dev loop (`make test`, `make test-chaos`), full deploy walkthrough (`make docker-build docker-push k3s-deploy`), how to mount the FUSE filesystem from a client machine, troubleshooting section (common failure signatures: lease contention, mismatched CRC, hostPath permission errors).
- **No placeholders anywhere** in `crates/*/src`. Tests may use fixtures/mocks, but production code paths must be real.
- Every phase's exit gate (Section "PHASE-BY-PHASE ROADMAP" preamble) must be satisfied — do not proceed to Phase *n+1* with a red build or red clippy on Phase *n*.

---

## 5. DEFINITION OF DONE — ACCEPTANCE CHECKLIST

- [ ] `cargo build --workspace` succeeds on stable Rust for both `aarch64-unknown-linux-musl` and `x86_64-unknown-linux-musl`.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` are clean.
- [ ] `gfs-cli put`/`get`/`ls`/`health`/`rm` work end-to-end against a 3-chunkserver + 2-master local cluster.
- [ ] FUSE mount supports at minimum: `ls`, `cat`, `cp` into and out of the mount, `mkdir`, `rm`.
- [ ] Chaos test: killing one of three chunkservers mid-write results in verified re-replication back to 3x with zero data loss, within the documented time bound.
- [ ] Chaos test: killing the leader master results in the standby taking over and client operations succeeding post-failover.
- [ ] `gfs-master` refuses to run two active leaders simultaneously under network partition (verify via forced split-brain test against the Lease mechanism).
- [ ] `gfs-chunkserver` refuses to start if `/mnt/gfs-storage` is not a distinct mount from `/`.
- [ ] All Docker images build via `make docker-build` and are ≤ ~15MB each.
- [ ] `make k3s-deploy` applies cleanly to a real or `kind`/`k3d`-simulated ARM64-labeled cluster.
- [ ] Benchmark binary produces throughput numbers on a 3-node simulated cluster and the numbers are recorded in `README.md` or a `BENCHMARKS.md`.

---

## 6. EXECUTION DIRECTIVES FOR THE AGENT

1. Work phase-by-phase in the order given. Commit (or checkpoint) at each phase boundary with a message summarizing what was built and what was verified.
2. When a design decision isn't fully pinned down above (e.g., exact oplog sync mechanism between master replicas, exact FUSE async-bridging pattern), make the decision, implement it fully, and record the rationale in that crate's `README.md` — do not leave it as an open question in code comments.
3. Prefer real, running tests over assertions-in-comments. If a chaos scenario is hard to automate fully, automate as much as possible and clearly document the manual verification steps for the remainder — but attempt full automation first.
4. Treat the Raspberry Pi target as a hard constraint, not an afterthought: watch binary size, memory footprint, and avoid any dependency that pulls in a heavy C toolchain incompatible with musl cross-compilation.
5. Stop and flag (rather than silently guessing) only for genuinely destructive ambiguities (e.g., "should this wipe existing chunk data on disk") — everything else, decide and proceed.

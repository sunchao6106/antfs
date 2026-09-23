<p align="center">
  <img alt="antfs logo" src="docs/assets/logo.svg" width="440">
</p>

<p align="center">
  <strong>A distributed blob storage system in Rust, purpose-built for massive small files.</strong>
</p>

<p align="center">
  <a href="https://github.com/sunchao6106/antfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/sunchao6106/antfs/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <a href="https://github.com/sunchao6106/antfs/pkgs/container/antfs"><img alt="Container image" src="https://img.shields.io/badge/ghcr.io-antfs-2496ED?logo=docker&logoColor=white"></a>
  <img alt="Rust" src="https://img.shields.io/badge/rust-stable-orange?logo=rust">
  <a href="docs/design/README.md"><img alt="Docs" src="https://img.shields.io/badge/docs-design%20docs-green"></a>
  <img alt="PRs Welcome" src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg">
</p>

<p align="center">
  English | <a href="README_zh.md">简体中文</a>
</p>

---

## What is antfs?

**antfs** takes its name from the **ant**: tiny individuals, astonishing numbers, reliable collaboration — exactly what a store for massive small files should be.

The design borrows from [LinkedIn Ambry](https://github.com/linkedin/ambry), [SeaweedFS](https://github.com/seaweedfs/seaweedfs), and [CubeFS](https://github.com/cubefs/cubefs): small files are appended into a log and located by an index, so reads and writes each cost **O(1) disk seeks**. Cluster metadata is managed by a master with a **built-in Raft** group — no external coordination service (ZooKeeper / etcd) required. The project moves in two phases:

- **Phase 1 (current): a faithful Rust port** — mirror Ambry's architecture and semantics 1:1 in Rust, substituting modern implementations only where strictly equivalent (e.g. Binary Fuse filters in place of Bloom filters).
- **Phase 2: optimize on top of the Rust base** — once the port is complete and correctness is locked down, push on performance, concurrency, and storage density.

## Features

- **O(1) small-file I/O** — append-only log + index; no per-object filesystem metadata, no random seeks.
- **Built-in Raft control plane** — placement, node state machine, reconciliation, decommission, and blobId range allocation, all inside the master; nothing external to operate.
- **Self-contained storage engine** — `Log` / `LogSegment` / `PersistentIndex` / `BlobStore`: immutable objects, irreversible deletes (tombstones), compaction, crash recovery, and a monotonic tombstone-wins replication merge.
- **Fast sealed-segment indexing** — mmap'd sealed segments with persistent **Binary Fuse filters** for negative lookups.
- **Quorum router SDK** — blobId range leasing from the master, W-majority writes, R=1 reads, streaming for large blobs; shared by the frontend and the benchmarks.
- **REST API** — an HTTP frontend exposing `/blobs` (read / write / delete / TTL / chunked assembly) plus `/metrics`.
- **Observability first** — Prometheus metrics everywhere, plus an admin web dashboard for topology, nodes, disks, partitions, and metrics.
- **One-command local cluster** — spin up a full dev cluster (Raft master + 3 stores + admin web) with a single `cargo run`.

## Architecture

```text
                          ┌────────────┐
                          │ Admin Web  │  topology · nodes · disks · partitions · metrics
                          └─────┬──────┘
                                │
   clients ── REST /blobs ──▶ Frontend ── embeds ──▶ Router SDK ── lease blobId ranges ──▶ ┌────────────────┐
                                  │                   (W-quorum writes, R=1 reads)          │ Master (Raft)  │
                                  │                            │                            │ placement      │
                                  │                            ▼                            │ reconcile      │
                                  │                ┌───────────────────────┐   heartbeat    │ decommission   │
                                  │                │ Store nodes (× N)     │ ◀───────────── │ blobId ranges  │
                                  └── replicate ──▶│  └─ blob engine:      │               └────────────────┘
                                                   │    Log / LogSegment / │
                                                   │    PersistentIndex    │
                                                   └───────────────────────┘
```

| Component | Description | Status |
|---|---|---|
| `common` | Shared libraries: layered `config` (serde + config), `metric` (Prometheus). Counterpart of Ambry's `config` package + Dropwizard `MetricRegistry` | 🚧 In progress |
| `blob` | Single-node storage engine: `Log` / `LogSegment` / `PersistentIndex` / `BlobStore` (put / get / delete / compaction / recovery + replication apply). Counterpart of `ambry-store` | 🚧 In progress |
| `master` | Cluster brain: built-in Raft (`openraft`) for placement, node state, reconciliation, decommission, and blobId range issuance. Replaces the Helix + ZooKeeper stack | 🚧 In progress |
| `store` | Data node (store server): multi-replica orchestration + HTTP data plane + heartbeat/disk monitoring + pull replication; hosts the `blob` engine | 🚧 In progress |
| `router` | Client SDK (`crates/common/router`): blobId range leasing, W-majority writes, R=1 reads, large-blob streaming; shared by frontend and benchmarks. Counterpart of Ambry's `NonBlockingRouter` | 🚧 In progress |
| `frontend` | HTTP REST object layer (`/blobs` + `/metrics`). Counterpart of `ambry-frontend` | 🚧 In progress |
| `admin` | Admin web: ops actions + control-plane dashboard (topology / nodes / disks / partitions / metrics) | 🚧 In progress |
| S3 gateway / multi-tenant ACL, quota | Gateway layer (same tier as frontend) | 📋 Planned |

> Naming: `blob` = the single-node blob storage engine; `store` = the data node server.

Storage engine highlights:

- **I/O**: synchronous blocking `std::fs` + buffered (OS page cache), aligned with original Ambry.
- **Index**: concurrent active segments + **mmap'd sealed segments + persistent Binary Fuse filters**.

Design docs live in [`docs/design/`](docs/design/) (overview + per-module); decision archives live in [`openspec/changes/`](openspec/changes/).

## Quick Start

Requires a [rustup](https://rustup.rs/) toolchain (stable). Cross-platform on Unix: Linux and macOS.

```bash
cargo build
cargo test
cargo run -p antfs-smoke        # storage-engine smoke test
```

Run a full local cluster — 1 Raft master + 3 store nodes + admin web, all in-process, with addresses printed on startup:

```bash
cargo run -p antfs-devcluster
```

### Prebuilt binaries

Every `v*` tag produces tarballs for Linux (x86_64 / aarch64) and macOS (arm64 / x86_64) on the [Releases page](https://github.com/sunchao6106/antfs/releases). Each tarball contains `antfs-master`, `antfs-store`, `antfs-frontend`, `antfs-admin` and `antfs-devcluster`:

```bash
target=x86_64-unknown-linux-gnu    # or aarch64-unknown-linux-gnu / aarch64-apple-darwin / x86_64-apple-darwin
base=https://github.com/sunchao6106/antfs/releases/latest/download
curl -LO "$base/antfs-$target.tar.gz" && curl -LO "$base/SHA256SUMS"
sha256sum "antfs-$target.tar.gz"   # macOS: shasum -a 256 — compare with the matching line in SHA256SUMS
tar xzf "antfs-$target.tar.gz" && cd "antfs-$target" && ./antfs-devcluster
```

### Run with Docker

A container image is published to the GitHub Container Registry on every push to `main` (tag `latest`) and on `v*` tags (semver tags):

```bash
docker run --rm -it \
  -p 7300:7300 -p 7400:7400 \
  -v antfs-data:/data \
  ghcr.io/sunchao6106/antfs:latest
```

It starts the same one-command dev cluster (1 Raft master + 3 store nodes + admin web + REST frontend). The startup log prints every endpoint and the admin tokens; from the host you get:

- admin web console — <http://127.0.0.1:7300>
- REST API — <http://127.0.0.1:7400/blobs>, Prometheus metrics at `/metrics`

Prefer Compose:

```bash
docker compose up -d
```

The image runs as a non-root user (UID 10001), so if you bind-mount a host directory instead of the named volume, make the directory writable first:

```bash
mkdir -p data && sudo chown -R 10001:10001 data
docker run --rm -p 7300:7300 -p 7400:7400 -v "$PWD/data:/data" ghcr.io/sunchao6106/antfs:latest
```

The image also ships the individual components (`antfs-master`, `antfs-store`, `antfs-frontend`, `antfs-admin`), so you can run one of them instead of the whole cluster:

```bash
docker run --rm --entrypoint antfs-master -p 7101:7101 ghcr.io/sunchao6106/antfs:latest
```

Or bake a benchmark run into the container startup:

```bash
docker run --rm ghcr.io/sunchao6106/antfs:latest --run-bench router --bench-ops 50000
```

To build the image locally: `docker build -t antfs:dev .`

### Optional: Linux-fidelity runs (colima VM)

The production target is Linux ext4. For fidelity, large-data, or performance testing on the real target OS, use the bundled colima VM wrapper:

```bash
./vm-cargo.sh build   # forwards cargo into the VM (sources over virtiofs, target/ & data on native ext4)
./vm-cargo.sh test
./vm-cargo.sh run -p antfs-smoke
```

## Documentation

- 📐 [Design docs](docs/design/) — architecture overview and per-module designs
- 📓 [DEVLOG](docs/DEVLOG.md) — step-by-step implementation log: what was done, key decisions, verification evidence
- 🗂 [OpenSpec changes](openspec/changes/) — design decision archive
- 🤝 [Contributing](CONTRIBUTING.md) — how to build, test, and propose changes

## Roadmap

- [x] Single-node blob engine (log + index + compaction + recovery)
- [x] Master with built-in Raft (placement / reconcile / decommission / blobId ranges)
- [x] Store server (replication, heartbeat, disk monitoring)
- [x] Router SDK + REST frontend
- [x] Admin web dashboard
- [ ] Finish the faithful Ambry port and lock correctness (Phase 1 exit)
- [ ] Performance / concurrency / storage-density optimizations (Phase 2)
- [ ] S3 gateway, multi-tenant ACL and quota

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first, and feel free to open an [issue](https://github.com/sunchao6106/antfs/issues) or a pull request.

## License

antfs is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgements

- Architecture inspired by [LinkedIn Ambry](https://github.com/linkedin/ambry), [SeaweedFS](https://github.com/seaweedfs/seaweedfs), and [CubeFS](https://github.com/cubefs/cubefs).
- The antfs logo was drawn in-house as SVG — tiny ant, big dreams.

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

- **Phase 1 (complete — exited at v0.0.4): a faithful Rust port** — mirror Ambry's architecture and semantics 1:1 in Rust, substituting modern implementations only where strictly equivalent (e.g. Binary Fuse filters in place of Bloom filters). Scope rulings (ported / deliberately diverged / deferred) are archived in [`openspec/changes/`](openspec/changes/).
- **Phase 2 (current): optimize on top of the Rust base** — with the port complete and correctness locked down, push on performance, concurrency, and storage density.

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
   REST clients               S3 clients (aws-cli / AWS SDKs)
   curl · HTTP · bench        PutObject · multipart · ListObjectsV2 · SigV4
        │                              │
        ▼                              ▼  name ↔ blobId          ┌─────────────────────┐
┌────────────────────────┐  ┌────────────────────────┐ ─────── ─▶ │ MetaStore (external)│
│ Frontend — REST        │  │ S3 gateway             │            │ PostgreSQL / MySQL  │
│ · /blobs · TTL · DELETE│  │ · buckets · objects ·  │            │ gateway is stateless│
│ · auto-chunk > 4 MiB:  │  │   multipart · list     │            └─────────────────────┘
│   chunks + stitch      │  │ · SigV4 (header ·      │
│   descriptor (GET/HEAD │  │   presigned) · creds   │
│   stitch transparently)│  │   admin token          │
└───────────┬────────────┘  └───────────┬────────────┘
            │                           │
            └─────────────┬─────────────┘
                          ▼  both gateways embed the same
        ┌────────────────────────────────────────┐  blobId ranges · topology (gRPC)
        │ Router SDK                             │ ─────────────────────────┐
        │ · W-quorum writes (acks return early,  │◀─────────────────────────┐
        │   3rd copy converges via replication)  │  ranges · placements    │ ┌──────────────────┐
        │ · R=1 reads · latency-aware ordering   │                          │ │ Master (Raft)    │
        │ · partition-full (507) → switch        │                          │ │ · placement      │
        │   partition & auto-retry               │                          │ │ · reconcile      │
        │ · large-blob streaming                 │                          │ │ · decommission   │
        └────────────────────┬───────────────────┘                          │ │ · blobId ranges  │
                             │ HTTP data plane (streaming PUT/GET)          │ └────────┬─────────┘
                             ▼                                              │          │
        ┌────────────────────────────────────────┐  heartbeat · disk reports│          │ admin API
        │ Store nodes (× N)                      │◀─────────────────────────┘          │
        │ · multi-disk · bounded per-disk pools  │                                     │
        │ · blob engine: Log / LogSegment /      │   ┌───────────────────────────┐     │
        │   PersistentIndex + binary-fuse filter │   │ Admin Web (port 7300)     │◀────┘
        │ · compaction · crash recovery          │   │ topology · nodes · disks  │
        │   multipart/auto-chunk chunks are      │   │ partitions · metrics      │
        │   plain blobs — assembled on read      │   └───────────────────────────┘
        │ ◀── pull replication (tombstone-wins   │
        │     merge · aligned segments · retries │
        │     are safe & idempotent) ──────────▶ │
        └────────────────────────────────────────┘
```

Layered reading: **gateways** (Frontend / S3) are stateless translators — every object id is self-locating (blobId, D3), no per-object metadata inside the cluster; the **Router SDK** is the only place W/R semantics live; the **master** owns topology via built-in Raft (no ZooKeeper); **store nodes** hold the append-only blob engine, where large objects land as ordinary chunks plus a stitch/multipart descriptor that reads back as one logical stream.

| Component | Description | Status |
|---|---|---|
| `common` | Shared libraries: layered `config` (serde + config), `metric` (Prometheus). Counterpart of Ambry's `config` package + Dropwizard `MetricRegistry` | ✅ Implemented |
| `blob` | Single-node storage engine: `Log` / `LogSegment` / `PersistentIndex` / `BlobStore` (put / get / delete / compaction / recovery + replication apply). Counterpart of `ambry-store` | ✅ Implemented |
| `master` | Cluster brain: built-in Raft (`openraft`) for placement, node state, reconciliation, decommission, and blobId range issuance. Replaces the Helix + ZooKeeper stack | ✅ Implemented |
| `store` | Data node (store server): multi-replica orchestration + HTTP data plane + heartbeat/disk monitoring + pull replication; hosts the `blob` engine | ✅ Implemented |
| `router` | Client SDK (`crates/common/router`): blobId range leasing, W-majority writes, R=1 reads, large-blob streaming; shared by frontend and benchmarks. Counterpart of Ambry's `NonBlockingRouter` | ✅ Implemented |
| `frontend` | HTTP REST object layer (`/blobs` + `/metrics`). Counterpart of `ambry-frontend` | ✅ Implemented |
| `admin` | Admin web: ops actions + control-plane dashboard (topology / nodes / disks / partitions / metrics) | ✅ Implemented |
| `s3` | S3-compatible gateway: PutObject / GetObject / DeleteObject / HeadObject (incl. object metadata: `Content-Type` + `x-amz-meta-*`), multipart upload, ListObjectsV2, bucket management, AWS SigV4 auth. Stateless (metadata in external PostgreSQL or MySQL). Same tier as `frontend` | ✅ Implemented |
| Multi-tenant S3 ACL (private-bucket semantics + bucket quota) | Gateway-layer enhancements | ✅ Implemented |

> Naming: `blob` = the single-node blob storage engine; `store` = the data node server.

Storage engine highlights:

- **I/O**: synchronous blocking `std::fs` + buffered (OS page cache), aligned with original Ambry.
  *Where* those blocking calls run changed in Phase 1: each disk owns a **bounded pool** (its own worker threads
  + a bounded queue), so one wedged disk can no longer drain the shared blocking pool and stall healthy disks;
  pool saturation fails fast with `503` + `Retry-After`. The IO primitive itself is untouched — no async/AIO.
  On top of that, each disk now has a **serviceability gate** (a wedged disk's requests fail fast instead of
  queueing forever) and an **IO watchdog** (in-closure syscall timing, independent of the active-probe switch).
  See [`docs/design/store.md` §12.11](docs/design/store.md) and [§12.12](docs/design/store.md) (both marked as
  foundational changes) and `docs/design/cluster.md` §6 **N4**.
- **Disk-fault containment (single process, many disks)**: one bad disk is downgraded from "the whole node is
  unusable" to "that disk is isolated, its replicas get replaced". Four layers, all opt-in-safe by default:

  | Layer | What it does | Status |
  |---|---|---|
  | **D1** per-disk bounded pool | one wedged disk can only consume *its own* worker threads; saturation ⇒ `503 + Retry-After` | ✅ landed (store) |
  | **D2** serviceability gate | disk judged `UNAVAILABLE` ⇒ all `/partitions/**` requests on it fail fast, incl. replication reads / `replica-metadata` / `segments`; `/metrics` and `/admin/**` are **never** short-circuited (a node must stay remotely recoverable) | ✅ landed (store) |
  | **D3** IO watchdog | watches the **longest in-flight syscall** per disk (queueing excluded); trips `IO_TIMEOUT` ⇒ same hysteresis ⇒ `UNAVAILABLE`. Independent of `disk_health_enabled` (which defaults to off) | ✅ landed (store) |
  | **D4** evacuation | move replicas off a bad disk — copy first, remove the source **only after** the successor is caught up (never lose the last copy) | ✅ landed (master) |
  | **D5** liveness debounce | a node needs K consecutive missed ticks to be declared down, so one hiccup no longer means "node dead ⇒ re-replicate everything" | ✅ landed (master) |
  | **D6** re-replication budget | cap how many under-replicated partitions start moving per tick; the throttled remainder is exposed as a gauge | ✅ landed (master) |

  Health is **hysteretic** (N bad rounds / T seconds, whichever first; stricter on the way back) so a single
  hiccup never evicts a good disk. "Never observed" is reported as `UNKNOWN` — never as healthy, and never as an
  empty string. Full design, config keys, metric names, test-evidence tables and residual risks:
  [`docs/design/store.md` §12.12](docs/design/store.md).

- **Slow disks (as opposed to dead ones)**: "slow ≠ dead", so slow disks are *not* evicted — they get three
  cheaper treatments instead:
  1. **Per-op watchdog deadlines** (`disk_watchdog_ms_by_op`) — a compaction copying a whole segment is orders of
     magnitude slower than a 4 KiB read; sharing one deadline forces you to choose between "a truly dead disk
     isn't isolated for ages" and "healthy compactions trip the watchdog daily".
  2. **`DEGRADED` tier** (`disk_degraded_io_ms`, **off by default**) — a disk whose *in-closure* slow-IO ratio
     stays above a threshold is reported as `DEGRADED`: it **still serves (never 503)**, master **stops placing
     new replicas** on it, and it is **not evacuated** (evacuation only reacts to `UNAVAILABLE`).
  3. **Latency-aware read routing** (client side, `router.latency_aware_routing`, on by default) — reads are
     R=1 (first success wins), so *which replica is tried first* directly decides read latency. Replicas are
     ordered by `up` → observed latency (EWMA) → master order. It only changes the **attempt order, never the
     candidate set**, so W/R and conflict resolution are untouched, and with no measurements yet it is the
     identity. See [`docs/design/router.md`](docs/design/router.md) and `docs/design/cluster.md` §9.3.

  Threshold calibration for all of the above (12 numbers, none of them benchmarked yet) is a written
  procedure with queries and decision rules: `docs/design/cluster.md` §9.4. What has to happen **on real
  hardware** before a rollout — device-level fail-fast config, fault-injection acceptance, alert wiring —
  is a checklist in §9.5.
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

Local knobs (ports, partition count, `disk_near_full_ratio`, …) can live in a `devcluster.toml` in the
current directory — it is read automatically when present, and CLI flags override it. See
`devcluster.toml.example` for every key.

### Prebuilt binaries

Every `v*` tag produces tarballs for Linux (x86_64 / aarch64) and macOS (arm64 / x86_64) on the [Releases page](https://github.com/sunchao6106/antfs/releases). Each tarball contains `antfs-master`, `antfs-store`, `antfs-frontend`, `antfs-s3`, `antfs-admin` and `antfs-devcluster`:

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

The image also ships the individual components (`antfs-master`, `antfs-store`, `antfs-frontend`, `antfs-s3`, `antfs-admin`), so you can run one of them instead of the whole cluster:

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
- [x] S3-compatible gateway (objects / multipart / SigV4 / buckets, external PostgreSQL or MySQL MetaStore)
- [x] Finish the faithful Ambry port and lock correctness (Phase 1 exit — v0.0.4; rulings archived under `openspec/changes/`, PG integration tests CI-gated)
- [ ] Performance / concurrency / storage-density optimizations (Phase 2)
- [x] Multi-tenant S3 ACL — private-bucket semantics (cross-tenant 403) + bucket-level storage quota shipped; versioning, undelete and billing permanently out of scope, and external credential sources (LDAP/AD/OIDC/IAM federation) are out too; virtual-host-style routing shipped the same day (reversal 2026-10-15, `s3.virtual_host_domain` off by default — see docs/design/s3.md)

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first, and feel free to open an [issue](https://github.com/sunchao6106/antfs/issues) or a pull request.

## License

antfs is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgements

- Architecture inspired by [LinkedIn Ambry](https://github.com/linkedin/ambry), [SeaweedFS](https://github.com/seaweedfs/seaweedfs), and [CubeFS](https://github.com/cubefs/cubefs).
- The antfs logo was drawn in-house as SVG — tiny ant, big dreams.

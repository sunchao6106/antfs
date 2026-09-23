# Contributing to antfs

Thanks for your interest in antfs! This document covers the development workflow. For architecture background, read the [design docs](docs/design/) first — antfs is currently a faithful Rust port of [LinkedIn Ambry](https://github.com/linkedin/ambry), so changes should either preserve Ambry semantics (Phase 1) or be explicitly marked as Phase 2 optimizations.

## Development setup

You need a [rustup](https://rustup.rs/) toolchain (stable). antfs is cross-platform on Unix (Linux and macOS):

```bash
cargo build
cargo test
cargo run -p antfs-smoke        # storage-engine smoke test
cargo run -p antfs-devcluster   # full local cluster: Raft master + 3 stores + admin web
```

For Linux-fidelity or performance runs on the production target (Linux ext4), use the colima VM wrapper:

```bash
./vm-cargo.sh build
./vm-cargo.sh test
```

## Before submitting a PR

1. **Format**: `cargo fmt --all`
2. **Lint**: `cargo clippy --workspace --all-targets -- -D warnings`
3. **Test**: `cargo test --workspace`

CI runs all three on every pull request (see `.github/workflows/ci.yml`), plus a build + test matrix on Ubuntu and macOS.

## Project conventions

- **Design decisions** are archived under [`openspec/changes/`](openspec/changes/). If your change alters architecture or on-disk formats, propose it there (or in an issue) before writing lots of code.
- **Design docs** in [`docs/design/`](docs/design/) describe current behavior — keep them in sync with the code.
- **DEVLOG** ([`docs/DEVLOG.md`](docs/DEVLOG.md)) records implementation steps with verification evidence; add an entry for notable work.
- Comments and identifiers follow the style of the surrounding code.

## Releases

Releases are cut from tags. Pushing a `v*` tag (or running the **Release** workflow manually with an existing tag) builds the server binaries for four targets — `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu` (cross-compiled with `cargo-zigbuild`), `aarch64-apple-darwin` and `x86_64-apple-darwin` — packages each as `antfs-<version>-<target>.tar.gz` plus a version-less alias, writes a `SHA256SUMS`, and publishes everything to [GitHub Releases](https://github.com/sunchao6106/antfs/releases).

```bash
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

The packaging step is a plain script, so you can reproduce it locally:

```bash
cargo build --release --locked -p antfs-master -p antfs-store -p antfs-frontend -p antfs-admin -p antfs-devcluster
scripts/release/package_binaries.sh 0.1.0 aarch64-apple-darwin   # 用本机 target triple
shasum -a 256 -c dist/SHA256SUMS.aarch64-apple-darwin            # Linux 用 sha256sum -c
```

Container images are built and pushed to GHCR by `.github/workflows/docker.yml`.

## Reporting bugs & proposing features

Use the [issue templates](https://github.com/sunchao6106/antfs/issues/new/choose). A minimal reproduction and your environment (OS, rustc version, commit) make bugs much faster to fix.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).

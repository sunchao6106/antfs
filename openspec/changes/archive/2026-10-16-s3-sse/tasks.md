# Tasks: s3-sse

## 1. Mechanism

- [x] 1.1 `crates/s3/src/sse.rs`: master key load, frame/read codec + unit
  matrix (roundtrip 0/1/64KiB boundary/2.5 chunks; tamper magic/wrap/chunk/tail;
  wrong master key; counter bounds)
- [x] 1.2 migration 004 both dialects; `sse: Option<String>` on
  `PutObjectMeta`/`ObjectMeta`; pg/mysql INSERT/SELECT/FromRow/into_meta +
  complete-multipart construction sites
- [x] 1.3 config `s3.sse.master_key_file` (default ""); AppState
  `sse: Option<Arc<MasterKey>>` + main.rs startup load
- [x] 1.4 handlers: put (opt-in + frame + echo), get (transparent decrypt +
  echo), head (echo), copy (dest opt-in, 501 on multipart+header),
  create/upload part 501

## 2. Verification

- [x] 2.1 e2e `tests/sse.rs` (dual dialect, fresh-dirty): roundtrip equal bytes
  + headers; blob-store bytes differ from plaintext (frame magic head);
  unconfigured fixture 501; bad value 400; multipart 501; quota arc; plaintext
  coexistence; copy re-key paths
- [x] 2.2 full regression zero change; fmt/clippy; workspace

## 3. Ship

- [x] 3.1 docs (config row, matrix row, SSE section), README x2, DEVLOG 62,
  archive + push + CI hard verify

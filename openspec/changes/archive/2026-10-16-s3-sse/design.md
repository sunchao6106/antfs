# Design: s3-sse

## Frame layout (v1, self-describing)

```
magic     b"ANTFSSE1"                         8
key_id    8 random-derived master-key id      16 hex ASCII
wrap      AES256-GCM(master, N_w)[DEK]        12 + 32 + 16
base      random chunk-nonce prefix           8
chunk_sz  u32 BE (== 65536, read verifies)    4
chunks    ceil(len/64KiB) x [ct || tag(16)]   nonce = base || counter(BE u32)
```

- DEK is random per object => chunk relocation across objects impossible
  (different key) and reordering/duplication within an object fails the
  per-chunk tag (tag verifies only under its counter nonce).
- Corruption anywhere -> `InternalError` (500, loud; never silently swallowed).
- Empty object: zero chunks (header only).

## Crypto decisions

- `aes-gcm` (RustCrypto, same family as our sha2/hmac). Randomness from
  `getrandom` (already in the tree via uuid v4).
- Master key file: 64 hex chars; key_id = hex(sha256(key)[..8]). Startup fails
  fast on malformed length/hex. Single key, rotation offline (config-table note).
- AAD empty by design (reasoning above); wrap nonce random per object.

## Persistence (red-line review)

- Migration 004 (both dialects): `ALTER TABLE named_objects ADD COLUMN sse
  VARCHAR(256) NULL`. NULL = plaintext. Old rows, old binaries, old readers:
  untouched. Forward: new readers tolerate NULL.
- Transaction envelope unchanged: framing happens BEFORE `router.put`, the row
  write still binds blob_id + logical size + etag + sse descriptor in the same
  tx; quota accounts plaintext (logical) size. Crash between blob write and row
  commit = existing orphan-blob story (GC/tombstone idempotence unchanged).
- meta.size/etag = plaintext semantics (documented ETag divergence).

## Handler wiring

- `AppState.sse: Option<Arc<MasterKey>>`; None + SSE header -> 501
  `NotImplemented` (capability not deployed), value != AES256 -> 400.
- put_object: frame after read (post aws-chunked decode), pass descriptor into
  `PutObjectMeta.sse`, echo header on 200.
- get_object: descriptor Some -> `read_object_frame` -> plaintext; head_object
  + GET echo header.
- copy_object: source read path is plaintext (transparent), destination frames
  iff the copy request carries the header; multipart-source copy + header ->
  501; create_multipart/upload_part with header -> 501 (explicit).
- multipart objects with sse descriptor cannot exist (501 at intake), so the
  multipart GET path never meets a framed manifest part.

## Mechanics lessons (post-implementation, to fill on ship)

- (recorded during implementation)

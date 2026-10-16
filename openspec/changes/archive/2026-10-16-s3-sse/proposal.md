# Proposal: SSE server-side encryption (`s3-sse`)

## Why

User queue item #3. Blob bytes on store nodes are plaintext today; SSE-S3 style
encryption (AES256, envelope with a local master key) makes at-rest compromise
of store disks not equal to object disclosure.

## What Changes

- **Freeze boundary (disk format untouched)**: encryption happens in the
  gateway layer; encrypted bytes are plain blobs to the engine. No schema
  change inside the blob engine, zero engine-format risk.
- **Envelope geometry**: per-object random DEK (AES-256-GCM), wrapped by the
  master key; frame layout `ANTFSSE1 | key_id | wrap(nonce+ct+tag) | base(8) |
  chunk_size | chunks(ct+tag)`; 64 KiB chunks, nonce = `base||counter`.
- **Persistence**: new nullable `named_objects.sse` column (migration 004,
  both dialects) carrying `{"alg":"AES256","key_id":"..."}`. NULL = plaintext
  (all legacy rows keep reading unchanged).
- **API surface**: PutObject with `x-amz-server-side-encryption: AES256` opts
  the object in (gateway unconfigured -> 501; other values -> 400). GET
  decrypts transparently; PUT/GET/HEAD/copy responses echo the header.
  Multipart with the header (create or part) -> explicit 501 this round.
  Copy of an encrypted source reads plaintext and re-keys per destination
  request (encryption is per-write opt-in, S3 semantics).
- **Config**: `s3.sse.master_key_file` (default "" = SSE off). Single key;
  key_id derived (sha256 prefix). Rotation = offline re-encrypt (documented
  limitation).

## Impact

- MetaStore: `PutObjectMeta/ObjectMeta` gain `sse: Option<String>`; both
  backends' INSERT/SELECT/row-mapping + complete-multipart construction sites.
- All non-SSE paths byte-identical; legacy rows transparent (NULL).
- ETag stays MD5(plaintext) (documented divergence from AWS's opaque
  SSE-S3 ETags).

## Non-goals

- SSE-KMS (no KMS component exists in antfs), SSE-C (client-supplied keys),
  bucket-default encryption (`?encryption` subresource), multipart SSE,
  key-id ring/rotation machinery, metadata integrity AAD beyond per-chunk tags
  (per-object DEK uniqueness + GCM tags already block cross-object relocation
  and reorder within an object).

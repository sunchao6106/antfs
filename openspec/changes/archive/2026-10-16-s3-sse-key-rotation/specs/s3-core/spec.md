## ADDED Requirements

### Requirement: server-side encryption master key rotation

The gateway binary SHALL provide an offline rotation mode invoked as `antfs-s3 --rotate-sse-key <new-key-file>`: it SHALL build the same state as normal startup (same config file, same metastore and router connections, old master key from `s3.sse_master_key_file`), run the rotation to completion, print a report (`scanned`, `rotated`, `skipped-other-key`) to stdout, and exit — without starting the GC loop or the HTTP listener. Rotation SHALL be run only while the gateway is stopped: the tool prints an offline-mode warning on start and takes no other concurrency precaution (a concurrent PutObject during rotation may lose that write).

For every `state=1` row with a non-NULL sse descriptor, the tool SHALL: abort loudly on an unparseable descriptor, an algorithm other than `AES256`, an sse descriptor combined with `parts_manifest` (unreachable state), or any blob read / decrypt / re-encrypt / row-write failure — reporting the progress at that point (rows rotated so far plus the offending bucket/key). A row whose descriptor `key_id` differs from the old master key's id SHALL be left byte-for-byte untouched (skipped and counted). Rotated rows SHALL preserve every ObjectMeta field verbatim (etag, size, content_type, user metadata, checksum) and swap exactly `blob_id` (new blob carrying the re-encrypted frame) and the sse descriptor (`key_id` of the new master key). Shadowed old blobs SHALL be tombstoned and their reclaim-WAL rows resolved via the existing overwrite arc, and a failed row-write after a successful new-blob write SHALL tombstone the new blob in the same invocation (quota-rejection arc).

Rotation SHALL be resumable by construction (no tool-side progress state): re-running it with the same old/new pair skips already-rotated rows (their descriptors now carry the new key id) and converges without double-writing.

#### Scenario: full rotation swaps the master and reads back

- **WHEN** SSE objects are written under master key A, the tool runs with new key B, the key file is swapped to B, and the gateway restarts
- **THEN** GET/HEAD of every rotated object return identical bytes, etag, size, and metadata as before rotation, still echoing `x-amz-server-side-encryption: AES256`, and the old blobs are tombstoned with no pending reclaim rows left

#### Scenario: re-run after an abort resumes without double work

- **WHEN** a previous rotation pass aborted (or completed) and the tool runs again with the same old/new pair
- **THEN** rows already carrying the new key id are reported as `skipped-other-key` and are not re-encrypted

#### Scenario: descriptor pointing at a foreign key id stays untouched

- **WHEN** a row's descriptor bears a key id that is neither the old nor the new key
- **THEN** the row's version, blob id, and descriptor are unchanged

#### Scenario: corrupt row aborts with progress

- **WHEN** a row's descriptor is unparseable, or declares an unknown algorithm, or combines sse with a parts manifest, or its blob/decrypt/re-encrypt/row-write fails mid-run
- **THEN** the tool stops at the first such row, reports rows rotated so far and the offending bucket/key, and exits non-zero

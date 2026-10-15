## ADDED Requirements

### Requirement: Overwritten versions are reclaimed immediately
When a PutObject (or CompleteMultipartUpload) writes a new version of a key, the gateway SHALL atomically shadow-mark all previously live versions of that key (superseded state with a delete timestamp) in the same store operation, and SHALL then delete each shadowed version's blob via the Router SDK. Repeated deletion of the same blob id SHALL be safe (tombstone idempotency).

#### Scenario: Overwrite reclaims the old blob
- **WHEN** client overwrites an existing key with a new PutObject
- **THEN** the previous version's row is shadow-marked, its blob is deleted from the storage cluster, and no un-referenced blob remains for that key before the new version is visible

#### Scenario: Multipart overwrite reclaims the old blob
- **WHEN** CompleteMultipartUpload lands a new version over an existing key
- **THEN** same immediate reclamation semantics as plain overwrite

### Requirement: GC purges soft-deleted rows after retention
A background GC loop (configurable enable/interval/retention; default on) SHALL periodically delete shadow-marked rows whose delete timestamp is older than the retention period. The GC SHALL NOT touch blob data at purge time (blobs were already deleted at shadow/delete time).

#### Scenario: Row within retention is kept
- **WHEN** a row was shadow-marked less than `deletion_retention_secs` ago
- **THEN** the GC loop leaves the row in place

#### Scenario: Row past retention is purged
- **WHEN** a row's delete timestamp is older than the retention period
- **THEN** the GC loop removes the row from MetaStore and counts it in `s3_gc_purged_rows_total`

### Requirement: Orphan multipart uploads expire via TTL
The GC loop SHALL expire multipart sessions older than `upload_ttl_secs` that were never completed, reclaiming each session by the abort semantics (delete every uploaded part blob, remove session and part rows).

#### Scenario: Stale session reclaimed
- **WHEN** a multipart upload was created but not completed within the TTL
- **THEN** its part blobs are deleted, its rows removed, and the counts land in `s3_gc_orphan_uploads_total` / `s3_gc_orphan_parts_blobs_total`

#### Scenario: In-flight session untouched
- **WHEN** a multipart session is younger than the TTL
- **THEN** GC leaves it fully intact

### Requirement: GC is safe under concurrent gateway instances
Multiple stateless gateway instances running GC concurrently SHALL not double-handle any row or session: work items are claimed atomically by single-statement row-level semantics; any blob-delete overlap remains safe by engine idempotency.

#### Scenario: Two instances scan the same table
- **WHEN** two gateway GC cycles race over the same candidate rows
- **THEN** each row/session is processed by exactly one instance (claim-by-delete/UPDATE semantics)

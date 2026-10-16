# s3-gc Specification

## Purpose
TBD - created by archiving change s3-gc. Update Purpose after archive.
## Requirements
### Requirement: Overwritten versions are reclaimed immediately
When a PutObject (or CompleteMultipartUpload) writes a new version of a key, the gateway SHALL atomically shadow-mark all previously live versions of that key (superseded state with a delete timestamp) in the same store operation, queue each shadowed version's blob into the reclaim WAL **within that same transaction**, and SHALL then delete each shadowed version's blob via the Router SDK, resolving its WAL row on success. Repeated deletion of the same blob id SHALL be safe (tombstone idempotency). A crash between commit and tombstone, or a failed tombstone, SHALL leave the WAL row for the GC loop to reclaim (no unreferenced, unrecorded blob remains).

#### Scenario: Overwrite reclaims the old blob
- **WHEN** client overwrites an existing key with a new PutObject
- **THEN** the previous version's row is shadow-marked, its blob is deleted from the storage cluster, and no un-referenced blob remains for that key before the new version is visible

#### Scenario: Multipart overwrite reclaims the old blob
- **WHEN** CompleteMultipartUpload lands a new version over an existing key
- **THEN** same immediate reclamation semantics as plain overwrite

#### Scenario: Crash before the tombstone is later closed by GC
- **WHEN** a gateway crashes after committing the shadowing transaction but before the tombstone lands
- **THEN** the WAL row survives; the next GC cycle deletes the blob and resolves the row

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

### Requirement: GC reclaims WAL rows left by crash windows
The GC loop SHALL process the `pending_reclaims` table as its third face: a bounded, non-destructive batch read; for each blob, a Router-SDK delete; on success a resolve (row deletion) counted in `s3_gc_pending_resolved_total`; on failure the row is left for retry in a later cycle. Each cycle SHALL sample the remaining row count into the `s3_gc_pending_backlog` gauge. Rows are never removed by anything other than a successful tombstone, so a crash at any point during GC removes nothing that a later cycle cannot retry; concurrent instances MAY overlap on the same row (double engine deletes are idempotent).

#### Scenario: Crash-window residue is reclaimed
- **WHEN** a WAL row exists for a blob whose tombstone never landed
- **THEN** the GC cycle deletes the blob, resolves the row, and increments `s3_gc_pending_resolved_total` on the gateway's real Registry

#### Scenario: Failed tombstone is retried next cycle
- **WHEN** the Router-SDK delete for a pending blob fails
- **THEN** the row remains, the error lands in `s3_gc_errors_total{kind}`, and a later cycle retries

#### Scenario: Backlog gauge exposes stuck reclaims
- **WHEN** a cycle finishes
- **THEN** `s3_gc_pending_backlog` reflects the remaining pending row count (0 in steady state)

#### Scenario: Concurrent GC overlap stays safe
- **WHEN** two gateway GC cycles race over the same pending row
- **THEN** both may delete the same blob id (idempotent) and both attempts resolve safely; no leak and no stuck state


## ADDED Requirements

### Requirement: SSE row scan for offline key rotation

The MetaStore trait SHALL expose `scan_sse_objects(after, limit)` returning full `ObjectMeta` rows for live encrypted objects: `state = 1`, `deleted_at IS NULL`, `sse IS NOT NULL`, ordered by `(bucket, key)` ascending, with an exclusive cursor `after: Option<(bucket, key)>` and a bounded `limit`. The scan SHALL be co-transactional-free read-only (no row state is modified). Pagination SHALL be stable under in-place row rewrites at or behind the cursor: a rewritten row (same bucket/key, higher version) is never re-returned by later pages, because the exclusive `(bucket, key) > after` comparison cannot match its key pair again. Both PostgreSQL and MySQL implementations SHALL be behaviorally identical (same ordering, filtering, and cursor semantics).

#### Scenario: bounded pages with an exclusive cursor

- **WHEN** the store holds more than `limit` live sse rows and the caller pages with the last row of each page as the cursor
- **THEN** every live sse row is returned exactly once, in `(bucket, key)` order, and non-sse or soft-deleted rows are never returned

#### Scenario: rewriting a scanned row does not loop pagination

- **WHEN** a row at or behind the cursor is superseded in place (same bucket/key, new version, old row shadowed) by the tool that is iterating the scan
- **THEN** subsequent pages do not return that key pair again (no phantom loop)

# store-composed-backfill Specification

## Purpose
TBD - created by archiving change store-composed-backfill. Update Purpose after archive.
## Requirements
### Requirement: Engine-level composed marking by record replacement
The blob engine SHALL expose `mark_composed(key)`, which under the existing write lock re-appends the current live winner record for that key with `FLAG_COMPOSED` set (identical bytes, size, and expiration) and republishes the index entry newest-wins, mirroring the existing replacement discipline of `apply_replicated` (old segment live→dead, live-stats swap). It SHALL NOT modify bytes in place, SHALL NOT resurrect deleted keys (`Deleted` outcome), SHALL be idempotent (`Already` when the bit is present), and the marking SHALL survive both index reopen and full log-based index rebuild. The engine SHALL also expose a winner-only index iteration (`for_each_entry_winner`) that yields only entries whose `find(key).offset` equals the entry's offset (the same winner criterion compaction uses).

#### Scenario: Marking survives crash-recovery paths
- **WHEN** a key is marked, the store is reopened from disk, and then again with the index deleted (rebuild scanning the log)
- **THEN** `stat` reports the key as composed in both cases, and the log contains exactly one live winner record for the key

#### Scenario: Delete after marking stays deleted
- **WHEN** a key is marked composed and then deleted
- **THEN** the tombstone remains the winner and a re-run of the marking reports `Deleted` without resurrecting the record

### Requirement: Store admin backfill endpoint
The store server SHALL expose `POST /admin/backfill-composed` (admin route family, never gated by the serviceability rule): a single-flight synchronous sweep over all local replicas that, for each current winner entry that is not deleted, not expired, carries no composed bit, and is within the descriptor size bound, reads the record bytes and — only if the bytes strictly parse as a stitch descriptor via the shared descriptor codec — marks it composed. The response SHALL carry a report of scanned/candidates/migrated/already/deleted/invalid/expired/failed counts. Concurrent invocations SHALL be rejected as busy; re-running SHALL be idempotent by construction (backfilled keys surface as `already`). Ordinary blobs whose bytes merely start with the descriptor magic but fail strict parsing SHALL NOT be marked.

#### Scenario: Legacy descriptor becomes trusted, magic-collision stays plain
- **WHEN** a plain (no-bit) legacy descriptor and an ordinary `ASTC`-prefixed non-descriptor blob both exist locally, and the backfill runs
- **THEN** the legacy descriptor is migrated (report counts one `migrated`) and its metadata reports `composed: true`, while the ordinary blob stays unmarked (counted `invalid`) and later reads serve its original bytes under the flag-only policy

#### Scenario: No resurrection of previously deleted keys
- **WHEN** a legacy descriptor was tombstoned before the backfill sweep reaches it
- **THEN** the sweep counts it as `deleted`, writes nothing, and the key remains deleted

### Requirement: Frontend flag-only read policy (operator covenant, default off)
The frontend SHALL gain config `frontend.composed_flag_only` (default `false`). When enabled, blobs without the composed bit SHALL be served as-is (GET returns original bytes without consulting the descriptor magic; HEAD skips the descriptor-body read and reports the raw size), while blobs with the bit SHALL keep the existing authoritative assembly semantics unchanged (including the loud 502 on over-limit or corrupt descriptors). The docs SHALL state the covenant in terms the operator cannot miss: the switch may only be enabled after the backfill has run and been verified on every store, otherwise pre-existing legacy descriptors would be served as their raw descriptor bytes.

#### Scenario: Magic-collision distortion is repaired under flag-only
- **WHEN** an ordinary small blob whose bytes begin with `ASTC` (but do not parse as a descriptor) is fetched with the flag-only policy enabled
- **THEN** the response is 200 with the original bytes, where the default (sniffing) policy would answer 502

#### Scenario: Bit-set stitch keeps authoritative assembly under both policies
- **WHEN** a stitch object written through the current frontend (composed bit set) is fetched with the flag-only policy enabled
- **THEN** the response is the assembled logical byte stream, identical to the default policy


## ADDED Requirements

### Requirement: Rollover must not fsync on the write critical path
The blob engine's segment rollover SHALL NOT flush (write header + fsync) the sealed segment while holding the write-path critical section. When an append or reserve needs a new segment, the engine SHALL enqueue the sealed segment for deferred flush and synchronously create only the new segment. The write-path rollover cost SHALL remain at new-segment-creation order (preallocation + header + fsync of the fresh near-empty segment), not sealed-segment fsync order.

#### Scenario: Rollover defers the sealed segment's fsync
- **WHEN** a put crosses a segment boundary and triggers rollover (segment filled, 4 GiB production size)
- **THEN** the triggering put and all concurrent writers are not blocked by the sealed segment's fsync; the sealed segment header's persisted `data_end` stays at its pre-rollover value until a flush point

#### Scenario: Sealed segment recovers to the old boundary on crash before flush
- **WHEN** a crash occurs after a rollover but before the sealed segment's deferred flush
- **THEN** recovery still resolves that segment via its header `data_end` exactly as the active segment does today (no new recovery form)

### Requirement: Flush is the durability boundary for deferred seals
`BlobStore::flush()` SHALL, before returning successfully, make durable every record written prior to the call — including records sitting in sealed-but-not-yet-flushed segments. `Log::flush()`, `flush_all()`, and `close()` SHALL likewise drain all pending sealed segments. The store's periodic flush worker is the production convergence channel for deferred seals.

#### Scenario: flush covers sealed segments
- **WHEN** records were written across a rollover and `flush()` is called
- **THEN** the sealed segment's on-disk header `data_end` advances to its in-memory value (byte-level observable), and a subsequent reopen sees all those records

#### Scenario: close drains pending seals
- **WHEN** the store rolls over segments and is closed without an explicit flush
- **THEN** pending sealed segments are flushed (reopened store reads back the records)

### Requirement: Log durability precedes index flush
The deferred-seal drain MUST complete (incrementally, within the write-lock section) before the index flush and watermark write inside `flush_inner`, preserving the invariant that the index never persists references to log offsets beyond the log's persisted extent. The heavy drain may run outside the write lock so that writers are not blocked by the sealed-segment fsync.

#### Scenario: Writers keep flowing while a flush drains a 4 GiB sealed segment
- **WHEN** the flush worker drains a large sealed segment's fsync
- **THEN** concurrent puts continue (the drain holds no write-path lock)

### Requirement: Flush failures feed the IO-error self-close chain
A failed `BlobStore::flush()` — including failures raised by deferred-seal drain — SHALL feed the same consecutive-IO-error accounting as data-path ops, preserving the EIO → engine self-close → replica ERROR → re-replication chain.

#### Scenario: deferred seal flush EIO counts
- **WHEN** a deferred sealed-segment fsync fails during flush
- **THEN** `io_error_count` increments (and the self-close threshold behavior applies unchanged)

### Requirement: Deferral is configurable; sync mode preserves legacy behavior
The engine SHALL expose a `store.defer_seal_flush` boolean config (default `true`). When set to `false`, rollover SHALL synchronously flush the sealed segment at the seal point (the legacy behavior: the sealed segment is durable immediately, and the write path bears the full-segment fsync stall). Both modes SHALL keep the flush-is-durability-boundary contract.

#### Scenario: sync mode flushes at rollover
- **WHEN** `defer_seal_flush = false` and a put triggers a rollover
- **THEN** the sealed segment's on-disk header `data_end` is advanced immediately (byte-level observable before any flush call)

#### Scenario: default is deferred
- **WHEN** no config is provided (engine defaults, e.g. bare constructions and tests)
- **THEN** rollover defers the sealed-segment flush (new behavior is the default)

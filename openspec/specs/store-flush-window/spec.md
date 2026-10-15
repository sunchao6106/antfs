# store-flush-window Specification

## Purpose
TBD - created by archiving change flush-bounded-dirty-window. Update Purpose after archive.
## Requirements
### Requirement: Bounded dirty window for background flush
The local maintenance flush worker SHALL reduce multi-second fsync stalls by waking early: aside from the fixed-period full round (`store_server.flush_interval_secs`), it SHALL, on a short check tick, flush individual replicas whose unsynced ("dirty") byte count since the last flush attempt reaches `store_server.flush_dirty_threshold_bytes`. The flush operation, the persistence-boundary contract ("flush is the only persistence boundary; a segment header's `data_end` advances only at fsync points"), and the "log fully persisted before index flush" ordering SHALL remain exactly unchanged — the bounded window only increases the frequency of boundary advancement. The dirty counter SHALL be advisory only (correctness never depends on it), reset per flush attempt (success or failure, preventing retry storms), and count both record publish paths (direct append and reserved chunked commit). `store_server.flush_dirty_threshold_bytes = 0` SHALL disable early wakes entirely (pure periodic behavior).

#### Scenario: Dirty watermark triggers an early flush
- **WHEN** a replica accumulates at least `store_server.flush_dirty_threshold_bytes` dirty bytes between periodic ticks
- **THEN** the local worker flushes exactly that replica early, submitted through that replica's disk pool with the same availability-skip semantics as periodic rounds, and `store_server_flush_early_wakes_total` increments

#### Scenario: Threshold zero keeps pure periodic behavior
- **WHEN** `store_server.flush_dirty_threshold_bytes = 0`
- **THEN** flush rounds happen only at `store_server.flush_interval_secs` ticks; dirty bytes are not polled for early wakes

#### Scenario: Persistence boundary unchanged
- **WHEN** the feature is on
- **THEN** `flush()` remains the sole persistence boundary (crash recovery truncates to the last flush exactly as before), early flushes execute the same two-phase flush with the log-before-index ordering, and the existing crash-consistency property tests pass unmodified

#### Scenario: Failed early flush does not storm
- **WHEN** an early flush attempt for a replica fails (disk error)
- **THEN** that replica's dirty accounting is reset by the attempt; re-arming requires another full threshold of newly written bytes, while the periodic round remains the retry backstop

#### Scenario: Accounting covers both publish paths and resets at flush
- **WHEN** records are published via the small-record append path or via the reserved chunked path (multipart / replication apply), and a log-level flush completes
- **THEN** the counter included both paths (counted at commit, reserved-but-uncommitted space excluded) and returns to zero after the flush attempt


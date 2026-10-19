# store-scrub-background Specification

## Purpose
TBD - created by archiving change store-scrub-background. Update Purpose after archive.
## Requirements
### Requirement: Background integrity scrub crawl
The store node SHALL, when `store_server.scrub_background_enabled = true`, run a resident background loop that periodically performs tolerant per-record CRC deep scans (`verify_from`) over every local replica it hosts, advancing an in-memory resume cursor per replica (start a fresh cycle from the log head after a cycle completes; restart-from-head after process restart). The loop is read-only (never modifies on-disk bytes), reports damage loudly (WARN log + dedicated `store_server_scrub_background_bad_records_total` counter), and SHALL NOT auto-repair — remediation stays an operator action (manual verify / RepairPartition / MoveReplica).

#### Scenario: Crawl covers a replica automatically in budget-sized batches
- **WHEN** the loop runs with `scrub_max_batch_bytes` smaller than the replica's log
- **THEN** each tick completes at most one batch per lane, the resume cursor advances to the first unread byte (`end_cursor`), total counter bytes equal the covered byte sum with no overlap, and after the budget-exhaustion-free completion the cursor resets and the next cycle restarts from the head

#### Scenario: A corrupted byte on a cold replica is discovered and the crawl continues
- **WHEN** a record's magic byte is flipped on disk while the loop is crawling
- **THEN** the batch counting it increments `store_server_scrub_background_bad_records_total`, a WARN is logged, the loop resyncs past the bad record by declared length and keeps advancing in subsequent ticks (no stall, no repeated counting of the same damage within a cycle)

#### Scenario: Disabled by default means byte-identical behavior
- **WHEN** `scrub_background_enabled = false` (default)
- **THEN** the loop is never spawned, no scrub bandwidth is consumed, and counters stay at zero; enabling later requires an explicit config change and restart

### Requirement: Anti-thrash pacing and concurrency caps
The background scrub loop SHALL bound its own IO footprint on four axes: (a) per-disk serialization — every batch is submitted through that disk's bounded pool as op `verify` (same path as the manual endpoint), pool saturation makes the tick skip that disk with back-off instead of retry hammering; (b) node-wide cap — at most `scrub_background_max_in_flight` (default 1) concurrent batches; (c) pacing — between ticks it sleeps `scrub_background_interval_secs` with ±25% jitter (dephasing from the compaction clock and peer nodes); (d) health gating — UNAVAILABLE/DEGRADED disks are skipped and counted in `store_server_disk_worker_skipped_total{task="scrub"}`; in cluster mode non-caught-up (BOOTSTRAP) replicas are skipped, while SEALED partition replicas remain candidates (read-only scan is precisely the guard for cold sealed data).

#### Scenario: Saturated disk pool backs off, does not hang or storm
- **WHEN** the single worker of a replica's disk pool is occupied and queue capacity is zero
- **THEN** the scrub round returns promptly having completed zero batches for that disk, and once the worker is released the same pool serves subsequent batches normally

#### Scenario: Jitter stays in the quarter band
- **WHEN** ticks are scheduled for any configured interval ≥ 4 s
- **THEN** every realized sleep lies within [0.75×, 1.25×] of the base interval, and intervals below 4 s pass through unjittered

#### Scenario: Sealed and caught-up gating follows candidate rules
- **WHEN** a replica's partition is SEALED, or the replica is BOOTSTRAP while master state is known
- **THEN** the SEALED replica is still scanned, the BOOTSTRAP replica is skipped this tick, and standalone (no master state) replicas are always scanned

### Requirement: Operator pause without disarming inspection
The store SHALL expose `GET|POST /admin/scrub-control` that pauses/resumes the background loop at runtime (flag is not persisted; resume continues from the preserved in-memory cursors). Manual `GET /partitions/{pid}/verify` and `GET /partitions/{pid}/scrub-report` SHALL be unaffected by the pause flag — emergency inspection capability must always remain available. The manual verify endpoint response SHALL additionally expose `bytes_read` and `end_cursor` (null when a scan completes within budget) so operators can chain manual batches.

#### Scenario: Pause freezes the crawl without disarming manual verify
- **WHEN** an operator POSTs `{"paused":true}` and two ticks elapse, then GETs the manual verify endpoint
- **THEN** the background batch counter shows zero growth, the manual verify call succeeds and echoes resume fields, and POSTing `{"paused":false}` lets the crawl continue from where it stopped


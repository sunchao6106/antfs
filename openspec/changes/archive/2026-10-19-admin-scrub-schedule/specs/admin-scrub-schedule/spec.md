## ADDED Requirements

### Requirement: Scheduled replica-comparison loop
When `admin.scrub_schedule_enabled = true`, the admin process SHALL run a resident loop that, every `admin.scrub_schedule_interval_secs` (validated ≥ 1, default 600), compares exactly one partition chosen round-robin from the schedulable set (partitions with `healthy_replicas ≥ 2`), executing the same shared core as the manual endpoint (`run_scrub`) with `deep = false` and `origin = "scheduled"`. Results SHALL enter the existing in-memory ring and reuse the existing scrub counters; responses from `POST /api/partitions/{id}/scrub` and `GET /api/scrub` SHALL carry the new additive `origin` field (`manual`/`scheduled`). The loop reports only — it SHALL NOT remediate; divergences surface through the ring, metrics, and logs, and the rotating schedule naturally re-compares each partition on later rounds.

#### Scenario: Scheduled round appears in the recent list with origin
- **WHEN** the loop is enabled and a tick fires on a healthy partition with two consistent replicas
- **THEN** the ring (and `GET /api/scrub`) gains an entry for that partition with `origin = "scheduled"` and a consistent verdict, while a manually triggered comparison responds with `origin = "manual"`

#### Scenario: Unhealthy partitions are not scheduled
- **WHEN** a partition's `healthy_replicas` is 0 or 1 (under-replicated, being repaired)
- **THEN** the scheduler never picks it this round, avoiding inconclusive noise in the ring

#### Scenario: Default off keeps behavior byte-identical
- **WHEN** `admin.scrub_schedule_enabled = false` (default)
- **THEN** no loop is spawned and no scheduled entries ever appear; only manual scrubs do

### Requirement: Operator pause spares the manual path
The admin SHALL expose `GET|POST /api/scrub-schedule-control` (POST operator-only, GET viewer-readable) that pauses/resumes the scheduled loop at runtime: flag not persisted (restart resumes the rotation), rotation position preserved (resume continues from where it stopped), and the gauge `admin_scrub_schedule_paused` set accordingly. The manual scrub endpoints SHALL remain fully functional while the loop is paused.

#### Scenario: Pause freezes the loop without disarming manual inspection
- **WHEN** an operator POSTs `{"paused":true}` and two ticks elapse, then a manual scrub is triggered
- **THEN** the ring shows zero growth from the loop across those ticks, the manual comparison succeeds and lands a `manual`-origin entry, and the gauge reads 1; POSTing `{"paused":false}` lets the rotation continue

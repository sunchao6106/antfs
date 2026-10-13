## ADDED Requirements

### Requirement: Compaction policy is configurable
The system SHALL support three compaction scheduling policies selectable via config key `compaction_policy`: `incremental`, `cost-benefit`, `hybrid`. The default SHALL be `hybrid`.

#### Scenario: Default policy is hybrid
- **WHEN** no `compaction_policy` config is specified
- **THEN** the worker uses hybrid scheduling (cost-benefit daily + CompactAll every N days)

#### Scenario: Explicit incremental policy
- **WHEN** `compaction_policy = "incremental"` is configured
- **THEN** the worker calls `compact_if_needed` (existing behavior) for every scheduled round

#### Scenario: Explicit cost-benefit policy
- **WHEN** `compaction_policy = "cost-benefit"` is configured
- **THEN** the worker calls `compact_cost_benefit` for every scheduled round, never doing CompactAll automatically

#### Scenario: Invalid policy value rejected at config validation
- **WHEN** `compaction_policy` is set to an unrecognized value (e.g. `"magic"`)
- **THEN** config validation SHALL fail with a descriptive error before the store server starts

### Requirement: Hybrid policy alternates cost-benefit with periodic CompactAll
In `hybrid` mode, the system SHALL run `compact_cost_benefit` on most scheduled rounds. When the elapsed time since the last CompactAll exceeds `compaction_policy_switch_days` (default 6), the next round SHALL run `compact()` (CompactAll) instead.

#### Scenario: Normal round uses cost-benefit
- **WHEN** hybrid policy is active AND the last CompactAll was within `compaction_policy_switch_days`
- **THEN** the worker calls `compact_cost_benefit` for each eligible replica

#### Scenario: CompactAll triggered after switch period expires
- **WHEN** hybrid policy is active AND the time since last CompactAll ≥ `compaction_policy_switch_days`
- **THEN** the worker calls `compact()` (CompactAll) for each eligible replica AND resets the last-CompactAll timestamp

#### Scenario: No CompactAll history exists
- **WHEN** hybrid policy is active AND `COMPACT_POLICY_STATE` file does not exist (first run or file deleted)
- **THEN** the system treats it as "never done CompactAll" AND the first eligible round triggers CompactAll

### Requirement: Hybrid CompactAll timestamp persists across restarts
The system SHALL persist the epoch-seconds of the last CompactAll to a file `COMPACT_POLICY_STATE` in each replica's data directory. The file format SHALL be JSON: `{"last_compact_all_epoch_secs": <integer>}`.

#### Scenario: Timestamp written after CompactAll completes
- **WHEN** a CompactAll finishes successfully for a replica
- **THEN** the system writes (or overwrites) the `COMPACT_POLICY_STATE` file in that replica's directory with the current epoch-seconds

#### Scenario: Timestamp survives process restart
- **WHEN** the store server restarts AND a `COMPACT_POLICY_STATE` file exists with a valid timestamp
- **THEN** the hybrid scheduler reads that timestamp and does NOT trigger CompactAll until the switch period has elapsed

#### Scenario: Corrupt or missing state file treated safely
- **WHEN** the `COMPACT_POLICY_STATE` file is missing, empty, or unparseable
- **THEN** the system treats it as "never done CompactAll" (CompactAll on next eligible round)

### Requirement: cost-benefit min_reclaim is configurable
The config key `compaction_min_log_segment_count_to_reclaim` (default 1) SHALL control the minimum number of segments that a cost-benefit compaction must be able to net-reclaim to proceed. If no candidate range meets this threshold, `compact_cost_benefit` returns `None` and no compaction happens that round.

#### Scenario: Default min_reclaim of 1
- **WHEN** `compaction_min_log_segment_count_to_reclaim` is not configured
- **THEN** cost-benefit proceeds if it can net-reduce at least 1 segment file

#### Scenario: Higher min_reclaim skips marginal ranges
- **WHEN** `compaction_min_log_segment_count_to_reclaim = 3` AND the best candidate range can only net-reduce 2 segments
- **THEN** cost-benefit returns `None` and no compaction happens that round

### Requirement: Compaction policy usage is observable via metrics
The system SHALL expose a counter `store_server_compaction_policy_used` with label `policy` (`incremental` / `cost_benefit` / `compact_all`). Each time a compaction actually executes for a replica, the counter for the policy used SHALL increment by 1.

#### Scenario: Hybrid round using cost-benefit increments correct counter
- **WHEN** hybrid mode runs a cost-benefit compaction round
- **THEN** `store_server_compaction_policy_used{policy="cost_benefit"}` increments by the number of replicas compacted

#### Scenario: Hybrid round using CompactAll increments correct counter
- **WHEN** hybrid mode runs a CompactAll round
- **THEN** `store_server_compaction_policy_used{policy="compact_all"}` increments by the number of replicas compacted

### Requirement: Existing candidate-set gates are unchanged
The scheduling policy selection SHALL NOT alter the existing candidate-set logic: partition writable state filtering (SEALED excluded, RO always included, RW by config), BOOTSTRAP skip, disk health gate, tombstone reclaim gate. These gates apply identically regardless of which policy is active.

#### Scenario: SEALED partition skipped under all policies
- **WHEN** a partition is in `SEALED` state
- **THEN** it is excluded from compaction regardless of whether the active policy is `incremental`, `cost-benefit`, or `hybrid`

#### Scenario: BOOTSTRAP replica skipped under all policies
- **WHEN** a replica is in `BOOTSTRAP` state (not yet caught up) AND master has sent state info
- **THEN** the worker skips compaction for it regardless of active policy

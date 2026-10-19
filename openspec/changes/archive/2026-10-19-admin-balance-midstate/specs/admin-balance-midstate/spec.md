## ADDED Requirements

### Requirement: Balance mid-move visibility via GetBalanceState
The master's `GetBalanceState` RPC SHALL additionally return, derived purely from the hard state (`moving` bookkeeping + `move_targets` + the current plan, nothing persisted for it), one entry per `(partition, from)` currently in the moving set — i.e. "target placed, source not yet removed". Entries SHALL be deterministically ordered and each SHALL carry `partition_id`, `from_node_id`, `to_node_id` (0 when `move_targets` has no successor record), a `status` of `failed` / `in_flight` / `unplanned`, and `error` (failed moves only). `failed` SHALL take precedence and SHALL surface the current plan's recorded error regardless of the plan's status; `in_flight` SHALL mean a RUNNING plan claims the entry; `unplanned` SHALL mean no plan claims it (manual moves, leftovers after cancel/replace/DONE). The response SHALL include moving entries even when no balance plan exists. The admin's `GET /api/balance` SHALL pass the list through to operator-and-viewer-readable clients, and the dashboard SHALL render it with status badges and a hint stating the true convergence semantics: sources are removed automatically once the successor replica catches up; re-evaluating a plan does NOT clear such entries (the planner skips partitions currently in the moving set); a lingering entry means the operator should investigate the target node's health.

#### Scenario: Manual move shows up as unplanned without any plan
- **WHEN** an operator moves a replica manually (`MoveReplica`) and no balance plan exists
- **THEN** `GetBalanceState` (and `GET /api/balance` / the dashboard's mid-move section) shows the entry with `status = "unplanned"` and the correct successor node, while the plan field stays absent

#### Scenario: Failed-but-stuck move keeps its mid-state and error visible
- **WHEN** a plan's in-flight move is judged failed (stuck over `balance_move_timeout_secs`) while the entry still sits in `moving`
- **THEN** the moving view reports `status = "failed"` with the recorded error, and the entry continues converging through the unconditional `plan_move` path once the successor eventually catches up

#### Scenario: Mid-move list empties as natural convergence completes
- **WHEN** the successor replica reports ACTIVE and non-source healthy replicas reach RF
- **THEN** the reconciler removes the source (`RemoveReplica` clears the moving bookkeeping), and the entry disappears from the moving view — proving the lens reflects hard state, not its own cache

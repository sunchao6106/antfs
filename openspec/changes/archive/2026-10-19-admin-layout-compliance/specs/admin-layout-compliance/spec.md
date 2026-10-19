## ADDED Requirements

### Requirement: Layout reverse-lookup report
The admin web SHALL expose `GET /api/layout` (viewer-readable, on-demand, read-only) that joins the full partition table (paged from the existing master `ListPartitions` API by the admin server until `total` items or an empty page) with the node registry (existing `ListNodes`) and returns: per-node inventory (`node_id`, `addr`, `up`, `replica_count`, `replicas: [{partition_id, disk_id, target_state}]` sorted by partition_id, nodes with zero replicas included), totals (`total_partitions`, `total_replicas`), and `generated_at_ms`. The report SHALL be stateless (no caching, no history) and SHALL NOT trigger any write on any component.

#### Scenario: Reverse lookup aggregates replicas per node
- **WHEN** partition 1 has replicas on nodes 2 and 3, and partition 2 has one replica on node 2
- **THEN** node 2's inventory lists two replica entries (partition 1 and 2, sorted), node 3 lists one, and a registered node 4 with no replicas appears with `replica_count = 0`

#### Scenario: Multi-page partition tables are fully joined
- **WHEN** the master reports more partitions than one fetch page
- **THEN** the admin server keeps paging (page size 500) until `total` items are collected or a page comes back empty, and the report totals reflect all partitions and replicas

### Requirement: Placement compliance scan
The layout report SHALL include a `violations` array and a `compliant` flag, computed by the pure function in `crates/admin/src/layout.rs` over exactly the constraints placement actually enforces, plus join integrity: (a) `same_node` — a partition holding two or more replicas on one node; (b) `unknown_node` — a replica referencing a node absent from the registry; (c) `unknown_disk` — a replica whose disk is absent from that node's registered disk list. Operational states (node down, BOOTSTRAP, DEGRADED, OFFLINE remnants) SHALL NOT be counted as violations; an empty violations array yields `compliant = true`. The report is advisory only — no automatic remediation.

#### Scenario: Same-node anti-affinity violation is flagged
- **WHEN** some partition has two replicas whose `node_id` is equal
- **THEN** the report carries one `same_node` violation for that partition (with the node id) and `compliant = false`

#### Scenario: Stale registry references are flagged
- **WHEN** a replica references a node id that is not registered, or a disk id that its node does not list
- **THEN** the report carries an `unknown_node` (respectively `unknown_disk`) violation with the offending identifiers

#### Scenario: Clean cluster is reported compliant
- **WHEN** every partition's replicas sit on distinct registered nodes with registered disks
- **THEN** `violations` is empty and `compliant = true`, including clusters where some nodes are down or some replicas are BOOTSTRAP (states are facts, not violations)

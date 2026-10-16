# s3-metastore 变更(s3-reclaim-wal)

## ADDED Requirements

### Requirement: Reclaim WAL records blobs pending tombstone
Every semantic action whose commit removes or replaces a blob's last referencing row SHALL record that blob id into a `pending_reclaims` table **inside the same transaction** (migration `006_add_pending_reclaims`, PostgreSQL and MySQL; `blob_id` primary key, `queued_at` for observability only; inserts idempotent on conflict). Recorded actions: PutObject overwrite shadowing (including the nested put of CompleteMultipartUpload), DeleteObject soft-delete, UploadPart retransmit replacement, CompleteMultipartUpload discarded (unlisted) parts, AbortMultipartUpload all parts. An insert failure SHALL abort the surrounding semantic transaction — a committed action without its WAL rows SHALL NOT exist. The WAL SHALL expose read-and-resolve primitives: a bounded batch read (ascending by nothing in particular, non-destructive) and a `resolve` deletion by blob ids used only after a successful engine tombstone; rows SHALL only ever disappear via resolve. The table is expected to be empty in steady state (handlers resolve immediately after their same-request tombstones).

#### Scenario: Overwrite queues the shadowed blob transactionally
- **WHEN** a PutObject overwrite shadows a previous version
- **THEN** the shadowed blob id is inserted into `pending_reclaims` in the same committed transaction

#### Scenario: Retransmit queues the replaced part blob
- **WHEN** an UploadPart re-upload replaces a part row
- **THEN** the replaced blob id is queued in the same transaction

#### Scenario: Abort and complete-discard queue their blobs
- **WHEN** an abort deletes parts rows, or a complete discards unlisted parts
- **THEN** each affected blob id is queued in the same transaction

#### Scenario: WAL insert failure rolls the action back
- **WHEN** the WAL insert fails
- **THEN** the semantic transaction aborts; no commit without the WAL record

#### Scenario: Steady state is an empty table
- **WHEN** handlers complete their same-request tombstones successfully
- **THEN** each resolved blob's WAL row is deleted and the table holds nothing

#### Scenario: Duplicated queue attempts deduplicate
- **WHEN** the same blob id is queued twice
- **THEN** a single WAL row exists (primary-key idempotent insert)

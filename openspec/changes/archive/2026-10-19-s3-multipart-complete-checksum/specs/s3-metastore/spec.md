## ADDED Requirements

### Requirement: Part checksum persistence and object checksum kind (migration 007)

Migration `007_add_part_checksum.sql` (PostgreSQL and MySQL, idempotent) SHALL extend the checksum persistence contract: `multipart_parts` gains `part_checksum_algo VARCHAR(8) NULL` + `part_checksum_b64 VARCHAR(64) NULL` holding the verified part-level checksum (the strongest claim when several algorithms were declared at UploadPart, mirroring object-row semantics); `named_objects` gains `checksum_type VARCHAR(12) NULL` (`FULL_OBJECT` | `COMPOSITE`) and `checksum_b64` is widened to TEXT (composite values reach ~53 KB at the 10 000-part cap). NULL `checksum_type` SHALL read as `FULL_OBJECT` (pre-007 rows) so `StoredChecksum` reconstruction stays fully backward compatible. Read helpers (`from_pair` family) SHALL treat half-written column sets as absent with a warning, exactly like the migration-005 contract. Both dialect implementations SHALL be behaviorally identical, including migration ordering after `006_add_pending_reclaims`.

#### Scenario: Old rows read as FULL_OBJECT after migration

- **WHEN** a row written before migration 007 has `checksum_algo`/`checksum_b64` set and `checksum_type` NULL
- **THEN** the reconstructed `StoredChecksum` reports kind FULL_OBJECT and echoes exactly as before

#### Scenario: Part rows round-trip the strongest claim

- **WHEN** an UploadPart declares both crc32 and sha256 (both verified)
- **THEN** the persisted part row carries the sha256 pair (strongest-one-wins, same ranking as object rows)

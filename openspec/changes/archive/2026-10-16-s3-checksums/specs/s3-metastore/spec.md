# s3-metastore 变更(s3-checksums)

## ADDED Requirements

### Requirement: Object checksum persistence
The MetaStore SHALL persist an optional flexible checksum with each `named_objects` row via migration `005_add_object_checksum` (PostgreSQL and MySQL): nullable columns `checksum_algo VARCHAR(8)` and `checksum_b64 VARCHAR(64)`, storing at most one algorithm name (`CRC32`/`CRC32C`/`SHA1`/`SHA256`) and its Base64 value over the object's plaintext. Existing rows, multipart-assembled rows, and copy products carry NULL (no backfill). `put_object` SHALL insert the provided pair; `get_object`/`head_object` readers SHALL surface the pair on `ObjectMeta` when present. NULL columns SHALL NOT affect any existing query, listing, pagination, ordering, or quota behavior.

#### Scenario: Migration is additive and idempotent
- **WHEN** a gateway with existing data starts against either dialect
- **THEN** migration 005 adds the two nullable columns; all pre-existing rows read back with no checksum; no other behavior changes

#### Scenario: Put and read round-trip the checksum
- **WHEN** a PutObject uploads with `x-amz-checksum-sha256` and the object is read back
- **THEN** `get_object` returns the pair (`SHA256`, Base64 value) on the metadata struct

#### Scenario: Overwrite replaces the checksum
- **WHEN** a key with a stored CRC32 checksum is overwritten without any checksum header
- **THEN** the latest row carries no checksum (echo surfaces nothing)

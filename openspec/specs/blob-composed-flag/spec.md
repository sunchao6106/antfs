# blob-composed-flag Specification

## Purpose
TBD - created by archiving change blob-composed-flag. Update Purpose after archive.
## Requirements
### Requirement: Record-level composed flag persists through all storage paths
The blob engine SHALL support a composed type bit `FLAG_COMPOSED = 1 << 4` in the log record flags byte and the IndexValue flags byte (bits 1-7 were reserved; no layout change). A record written via the composed entry points SHALL carry the bit in both its log record and its index value; plain entry points (`put`, `put_ttl`, `put_from_reader`) SHALL behave byte-identically to before (flags without the bit). The bit SHALL be preserved by index persistence (`.idx` roundtrip), crash-recovery index rebuild (log scan), replication apply, and compaction, since all of these copy record/index flags through unchanged. `stat` SHALL report `composed: bool` on `BlobMeta`, sourced from the index value. Composed records are ordinary blobs otherwise: `get` returns their bytes identically, and tombstones never carry the bit (delete is a separate write).

#### Scenario: Composed roundtrip
- **WHEN** a blob is written via `put_composed_ttl` and read back
- **THEN** `get` returns the exact bytes and `stat` reports `composed: true`

#### Scenario: Plain writes stay unflagged
- **WHEN** a blob is written via the pre-existing `put_ttl`
- **THEN** `stat` reports `composed: false` and the record bytes are identical to pre-change behavior

#### Scenario: Recovery and replication preserve the bit
- **WHEN** an index is rebuilt from the log after `open_rebuild_index`, or a composed record replicates to a peer
- **THEN** `stat` still reports `composed: true` on the rebuilt/replica index

#### Scenario: Old-format values decode as not composed
- **WHEN** an IndexValue written before this change (bit 4 = 0) is decoded
- **THEN** `is_composed()` is false and the legacy read path is taken

### Requirement: Store data plane carries the composed bit
The store HTTP data plane SHALL accept `PUT /partitions/{pid}/blobs/{key}?composed=true`, threading the flag into both the inline and staged-streaming write paths (absent/false keeps existing behavior). `GET /partitions/{pid}/blobs/{key}/metadata` SHALL include a `composed` boolean in its JSON response, sourced from the engine `stat`.

#### Scenario: Composed PUT lands the flag
- **WHEN** a data-plane PUT carries `?composed=true`
- **THEN** a following metadata GET reports `"composed": true`

#### Scenario: Plain PUT unchanged
- **WHEN** a PUT omits the parameter
- **THEN** metadata reports `"composed": false` (rolling upgrade: old routers never send it)

#### Scenario: Old store tolerates the new query parameter
- **WHEN** a new router sends `?composed=true` to a store binary that predates the flag
- **THEN** the parameter is ignored (serde default) and the blob is stored unflagged — reads fall back to the legacy path

### Requirement: Router SDK composed writes and tolerant stat
The Router SDK SHALL offer composed write methods (`put_composed`, `put_composed_with_ttl`, `put_auto_composed_with_ttl`) delegating to the same quorum / 507-retry mechanics as their plain counterparts, with the composed flag appended to the data-plane PUT URL. `BlobStat` SHALL carry a `composed` field that deserializes tolerantly (`serde` default false) so a new router accepts metadata responses from older stores lacking the field.

#### Scenario: Composed descriptor write through router
- **WHEN** a caller uses `put_auto_composed_with_ttl` (or `put_composed`)
- **THEN** the data-plane PUT URL includes `composed=true`, quorum/ack semantics are identical to the plain method, and a following `stat` reports composed

#### Scenario: Stat tolerates old-store JSON
- **WHEN** a metadata response lacks the `composed` field
- **THEN** `BlobStat.composed` deserializes as false without error

### Requirement: Frontend trusts the flag; the flag is the sole type marker
The frontend SHALL write the stitch descriptor (both `POST /stitch` and the auto-chunk pipeline's descriptor) via composed router writes. The GET read path SHALL dispatch on the composed flag alone: a `composed: true` stat enters the descriptor assembly path unconditionally (an over-limit or undecodable composed blob is a 502 upstream error, never a silent fallback to plain); rows without the flag are plain objects — served byte-as-stored, with no content probing of any kind. HEAD SHALL likewise probe and decode the descriptor body only for `composed: true` rows (logical size + stitch headers); flag-less rows report the raw stat size without reading the body. There is no publisher-side sniffing, no magic-based classification, and no optional policy toggle: the flag is the sole type marker.

#### Scenario: Plain blob with any byte prefix is served as-is
- **WHEN** an ordinary small blob whose leading bytes are anything (including former descriptor-magic-looking prefixes) is fetched
- **THEN** the response is 200 with the original bytes — flag-less rows are never parsed, classified, or expanded

#### Scenario: Flagged blob keeps authoritative assembly semantics
- **WHEN** a descriptor written through the current frontend (composed flag set) is fetched
- **THEN** the response is the assembled logical byte stream, and an over-limit or structurally invalid composed blob yields a loud 502 upstream error, byte-identical to the pre-change flag path


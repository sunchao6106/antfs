## MODIFIED Requirements

### Requirement: Frontend trusts the flag first and keeps legacy sniff fallback
The frontend SHALL write the stitch descriptor (both `POST /stitch` and the auto-chunk pipeline's descriptor) via composed router writes. The GET read path SHALL dispatch on the composed flag alone: a `composed: true` stat enters the descriptor assembly path unconditionally (an over-limit or undecodable composed blob is a 502 upstream error, never a silent fallback to plain); rows without the flag are plain objects — served byte-as-stored, with no content probing of any kind. HEAD SHALL likewise probe and decode the descriptor body only for `composed: true` rows (logical size + stitch headers); flag-less rows report the raw stat size without reading the body. There is no publisher-side sniffing, no magic-based classification, and no optional policy toggle: the flag is the sole type marker.

#### Scenario: Plain blob with any byte prefix is served as-is
- **WHEN** an ordinary small blob whose leading bytes are anything (including former descriptor-magic-looking prefixes) is fetched
- **THEN** the response is 200 with the original bytes — flag-less rows are never parsed, classified, or expanded

#### Scenario: Flagged blob keeps authoritative assembly semantics
- **WHEN** a descriptor written through the current frontend (composed flag set) is fetched
- **THEN** the response is the assembled logical byte stream, and an over-limit or structurally invalid composed blob yields a loud 502 upstream error, byte-identical to the pre-change flag path

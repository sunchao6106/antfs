# s3-multipart 变更(s3-range-streaming)

## ADDED Requirements

### Requirement: Assembled reads keep memory bounded and support ranged reads
GetObject on a multipart object SHALL assemble the manifest parts **sequentially through a streaming response body** — the gateway SHALL NOT materialize the whole object (or any concatenation of all parts) in memory; at most ONE part's bytes (each part is bounded by `max_body_bytes`) SHALL be buffered at a time. The response's `Content-Length` SHALL be the named-object row's `size`; before responding, the gateway SHALL verify that the sum of manifest part sizes equals that row size and SHALL fail loudly with `InternalError` (500) on any mismatch (never serve a short/long body against a declared Content-Length).

When the request carries a satisfiable `Range`, the gateway SHALL translate it via manifest part sizes into a covering window: only parts intersecting the window SHALL be fetched (non-covering parts induce zero network reads), with the head/tail parts byte-sliced; the response SHALL be 206 per the `s3-core` ranged-read semantics.

#### Scenario: Full multipart read streams without whole-object buffering
- **WHEN** a 60-part object (64 KiB each) is read with a plain GET
- **THEN** gateway returns 200 with exactly the concatenated part bytes and `Content-Length` = Σ parts; the read path buffers at most one part in memory (implementation-level: sequential part stream, no `Vec` accumulation)

#### Scenario: Ranged read across parts fetches only covering parts
- **WHEN** an object has 3 parts of 1 MiB each and client sends `Range: bytes=1048576-2097151` (exactly part 2)
- **THEN** gateway returns 206 with `Content-Range: bytes 1048576-2097151/3145728` and part 2's bytes verbatim; only part 2's blob is fetched from the cluster

#### Scenario: Ranged read spanning a part boundary slices head and tail
- **WHEN** 3 parts of 1 MiB each and client sends `Range: bytes=1048500-1048700` (spans the part 1/2 boundary)
- **THEN** gateway returns 206 with body = last 76 bytes of part 1 followed by first 124 bytes of part 2

#### Scenario: Manifest-row size mismatch fails loud
- **WHEN** a row's `size` disagrees with the sum of its manifest part sizes (corrupt metadata)
- **THEN** gateway returns `InternalError` (500) instead of serving a body that contradicts its `Content-Length`

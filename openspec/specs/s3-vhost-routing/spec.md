# s3-vhost-routing Specification

## Purpose
TBD - created by archiving change s3-vhost-routing. Update Purpose after archive.
## Requirements
### Requirement: Virtual-host style bucket addressing
The S3 gateway SHALL optionally accept virtual-host style requests when `s3.virtual_host_domain` is configured (default empty = the feature is off and behavior is unchanged). A request whose `Host` header is `{bucket}.{virtual_host_domain}` (with an optional trailing `:port`) SHALL be routed exactly as the equivalent path-style request on `/{bucket}{path}` — with the path portion (including embedded slashes and trailing slashes) and query string preserved verbatim, and with all existing authentication, ACL, quota and business semantics applying unchanged. Signature verification SHALL see the request's ORIGINAL path and Host (authentication runs before the rewrite), so header-signed and presigned virtual-host requests verify naturally without any compensation logic. Requests whose Host is the bare base domain, does not match the suffix, or carries an empty/invalid bucket label SHALL pass through unchanged (path-style semantics; the admin and metrics surfaces therefore remain reachable only on the base domain).

#### Scenario: Virtual-host object round-trip
- **WHEN** `s3.virtual_host_domain = "s3.example.com"` and the owner issues a header-signed `PUT`/`GET`/`HEAD` to `/k.bin` with `Host: mybucket.s3.example.com` (path signed vhost-style, without the bucket)
- **THEN** all three behave exactly as the path-style requests to `/mybucket/k.bin` (200 + ETag / 200 + body / 200)

#### Scenario: Virtual-host bucket-level operations
- **WHEN** a signed request hits `/` (with `?list-type=2`, or as PUT create) on `Host: mybucket.s3.example.com`
- **THEN** it routes to the bucket handler for `mybucket` (listing XML includes the keys; create follows existing claiming semantics)

#### Scenario: Presigned and SigV4 verify without compensation
- **WHEN** any vhost-style request carries a valid signature computed over the original Host and bucket-less path
- **THEN** verification succeeds — the rewrite never affects canonical-request computation

#### Scenario: Auth, ACL and quota semantics carry over
- **WHEN** vhost-style requests hit a private bucket (anonymous or cross-tenant), a public-read bucket (anonymous GET), an unknown bucket, or an over-quota write
- **THEN** responses (403 / 200 / 404 NoSuchBucket / 507) are identical to the path-style equivalents

#### Scenario: Port-bearing Host
- **WHEN** the Host header carries a trailing port (`mybucket.s3.example.com:7500`)
- **THEN** the port is stripped for matching and the request is rewritten normally

#### Scenario: Pass-through cases
- **WHEN** the Host is the bare base domain, an unrelated host, or `{invalid}.{suffix}` with an empty or non-ASCII-lowercase bucket label
- **THEN** the request is routed unchanged (path-style semantics; admin/metrics stay on the base domain)

#### Scenario: Disabled by default
- **WHEN** `s3.virtual_host_domain` is empty
- **THEN** no request is rewritten and behavior is byte-identical to before this change


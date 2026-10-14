## ADDED Requirements

### Requirement: Frontend liveness endpoint
The frontend SHALL expose `GET /healthCheck` returning HTTP 200 with body `GOOD` and `content-type: text/plain`. The endpoint SHALL require no authentication and SHALL perform no side effects (no metrics mutation, no cluster interaction). The response SHALL reflect frontend **process liveness only**: it MUST NOT depend on router, master, or store connectivity, and MUST NOT be affected by backend failures.

#### Scenario: Unauthenticated probe returns GOOD
- **WHEN** any client sends `GET /healthCheck` with no credentials or special headers
- **THEN** frontend returns 200, `content-type: text/plain`, body `GOOD`

#### Scenario: Liveness independent of cluster state
- **WHEN** master is unreachable or store backends are failing
- **THEN** `GET /healthCheck` still returns 200 `GOOD` (the endpoint never touches the router or cluster)

#### Scenario: No metric pollution from probes
- **WHEN** `/healthCheck` is called repeatedly (e.g. LB probing every 10s)
- **THEN** frontend business metrics (upload/download/delete/mutation counters, in-flight gauges) are unchanged by these requests

#### Scenario: Route coexists with object API
- **WHEN** `GET /healthCheck` is called between normal `POST /blobs` / `GET /blobs/{id}` traffic
- **THEN** both the probe and the object operations succeed; no interference in either direction

# Feature Specification: UC REST API Layer

**Feature Branch**: `017-uc-rest-api-layer`  
**Created**: 2026-04-24  
**Status**: Draft  
**Input**: User description: "I want to implement a REST API layer for all the functionalities in UC1, UC2, UC3 and UC4."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Trigger and Monitor a Pipeline Run (Priority: P1)

An operator wants to kick off a data pipeline run against a source file or GCS URI, track its progress, and retrieve the result — all without touching the CLI or Streamlit UI.

**Why this priority**: The pipeline (UC1) is the core product. All other use cases depend on data it produces. Exposing it via API unlocks automation, scheduling, and CI integration.

**Independent Test**: Submit a run request with a valid source URI and domain, poll status until completed, verify output metadata is returned. Delivers end-to-end value with no other endpoints needed.

**Acceptance Scenarios**:

1. **Given** a valid source path and domain, **When** a run is submitted, **Then** a unique run ID is returned immediately and the run begins asynchronously.
2. **Given** a run ID, **When** status is polled, **Then** current stage, progress, and any errors are returned.
3. **Given** a completed run ID, **When** result is fetched, **Then** output file location, row counts, DQ scores, and block audit trail are returned.
4. **Given** an in-progress run, **When** a resume request is sent, **Then** the run continues from the last completed checkpoint.
5. **Given** an invalid source path, **When** a run is submitted, **Then** a descriptive error is returned and no run is created.

---

### User Story 2 - Query Observability Data (Priority: P2)

A data engineer or automated monitoring system wants to query pipeline run history, block traces, anomalies, quarantine records, and cost metrics without hitting the MCP server directly.

**Why this priority**: UC2 observability is already partially exposed via the MCP server; this story unifies and extends that surface with proper routing and authentication.

**Independent Test**: Query `/observability/runs` for recent runs and `/observability/anomalies` for flagged sources. Can be tested against seeded log data with no pipeline runs needed.

**Acceptance Scenarios**:

1. **Given** completed pipeline runs exist, **When** run history is queried with optional filters (source, date range, domain), **Then** a paginated list of run summaries is returned.
2. **Given** a specific run ID, **When** block trace is requested, **Then** per-block row counts, timing, and audit entries are returned.
3. **Given** anomaly detection has run, **When** anomalies endpoint is queried, **Then** flagged sources with anomaly scores and timestamps are returned.
4. **Given** quarantine records exist, **When** quarantine endpoint is called, **Then** rejected rows with rejection reasons and source run ID are returned.
5. **Given** cost data is available in Prometheus, **When** cost report is requested, **Then** token usage and estimated cost per source and model tier are returned.

---

### User Story 3 - Search the Product Catalog (Priority: P3)

A downstream application or analyst wants to perform keyword or semantic search over the enriched product catalog produced by the pipeline.

**Why this priority**: UC3 search is currently unimplemented scaffolding. Exposing it as an API endpoint establishes the contract; implementation can land incrementally behind a readiness check.

**Independent Test**: Submit a search query and receive a ranked list of products. If the index is empty, a clear "not ready" response is returned rather than an error.

**Acceptance Scenarios**:

1. **Given** the search index is populated, **When** a query string is submitted, **Then** a ranked list of matching products with scores is returned.
2. **Given** optional filters (domain, category), **When** applied to a search, **Then** results are scoped accordingly.
3. **Given** the search index is not yet built, **When** a search is submitted, **Then** a 503 with a descriptive "index not ready" message is returned.
4. **Given** an empty query, **When** submitted, **Then** a validation error is returned immediately.

---

### User Story 4 - Get Product Recommendations (Priority: P4)

A downstream application wants to retrieve "also bought" or "you might like" recommendations for a given product from the UC4 recommendation engine.

**Why this priority**: UC4 is scaffolding; the API contract should be defined now so downstream consumers can integrate without waiting for the full recommendation engine.

**Independent Test**: Request recommendations for a known product ID. If the graph is not built, return a clear "not ready" response.

**Acceptance Scenarios**:

1. **Given** the recommendation graph is populated, **When** recommendations are requested for a product ID, **Then** a ranked list of related products is returned.
2. **Given** an unknown product ID, **When** recommendations are requested, **Then** a 404 with a clear message is returned.
3. **Given** the recommendation graph has not been built, **When** any recommendation endpoint is called, **Then** a 503 "graph not ready" is returned.

---

### User Story 5 - Cache and Schema Management (Priority: P4)

An operator wants to inspect or flush pipeline caches and query domain schemas without SSHing into the server or using the CLI.

**Why this priority**: Operational convenience; lower priority than data-path endpoints.

**Independent Test**: Flush Redis cache keys for a domain, then verify the cache stats endpoint shows reduced key count.

**Acceptance Scenarios**:

1. **Given** Redis is running, **When** cache stats are requested, **Then** key counts and TTL summaries per prefix are returned.
2. **Given** stale cache entries, **When** a flush is requested for a domain, **Then** only matching keys are deleted and a count is returned.
3. **Given** a domain name, **When** schema is requested, **Then** the canonical column set and types for that domain are returned.

---

### Edge Cases

- What happens when a pipeline run is submitted while another run for the same source is in progress? (return existing run ID or reject with 409)
- Concurrent runs for different sources are allowed, capped at `MAX_CONCURRENT_RUNS` (default 2). A submission that would exceed the cap returns 429 with a `retry_after` hint. Same-source conflicts still return 409.
- How does the API behave when Redis is unavailable? (fall through to SQLite, reflect degraded status in health endpoint)
- How does the API handle GCS URIs versus local paths for source submission?
- What if a run is submitted for a domain whose schema does not yet exist (first run)?
- How are partial/failed runs represented in the status endpoint? Failed runs include `error` field with reason. On API server startup, any checkpoint records in `"running"` state are transitioned to `"failed"` with `error: "server_restart"` — clients detect the drop and issue a resume request explicitly.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST expose an asynchronous pipeline run submission endpoint that returns a run ID immediately. Concurrent runs for different sources are allowed up to `MAX_CONCURRENT_RUNS` (default 2, env-configurable); submissions exceeding the cap return 429 with a `retry_after` hint.
- **FR-002**: System MUST expose a run status endpoint that reflects current stage, completed chunk index, and errors. Progress percentage is not exposed — total chunk count is not knowable upfront for GCS glob sources.
- **FR-003**: System MUST expose a run result endpoint returning output metadata, row counts, DQ scores, and block audit trail.
- **FR-004**: System MUST expose a resume endpoint that continues a run from its last checkpoint.
- **FR-005**: System MUST expose observability endpoints for run history, block traces, anomalies, quarantine records, and cost reports.
- **FR-006**: System MUST expose a search endpoint backed by UC3's hybrid search (BM25 + semantic); return 503 when index is not ready.
- **FR-007**: System MUST expose recommendation endpoints (`also-bought`, `you-might-like`) backed by UC4; return 503 when graph is not ready.
- **FR-008**: System MUST expose cache management endpoints (stats, flush by domain/prefix).
- **FR-009**: System MUST expose a domain schema endpoint returning the canonical column set for a given domain.
- **FR-010**: System MUST expose a `/health` endpoint reflecting readiness of all dependent services (Redis, Postgres, Prometheus, FAISS index, recommendation graph).
- **FR-011**: All endpoints MUST return structured JSON responses with consistent error envelopes (`error`, `detail`, `run_id` where applicable).
- **FR-012**: All endpoints MUST be versioned under a `/v1/` prefix. Pipeline run endpoints under `/v1/pipeline`; observability under `/v1/observability`; search under `/v1/search`; recommendations under `/v1/recommendations`; cache/schema under `/v1/ops`. The MCP sub-app mounts at `/mcp` (unversioned, preserving existing paths).
- **FR-013**: The existing MCP server endpoints (`/tools/*`) MUST remain accessible and unmodified; new routers are additive.
- **FR-014**: Destructive operations (cache flush, run cancel) MUST require explicit confirmation in the request body.
- **FR-015**: `POST /v1/pipeline/runs` MUST enforce a per-IP rate limit of 10 submissions/minute (configurable via `PIPELINE_RATE_LIMIT` env var). Requests exceeding the limit return 429 with a `Retry-After` header.

### Pipeline Governance Constraints

- The API layer must not bypass the YAML-only transform constraint — run submission goes through the existing graph, not new code paths.
- The API must not expose direct manipulation of `config/schemas/<domain>_schema.json`; schema writes remain the sole domain of Agent 1.
- Enrichment endpoints, if any, must not accept or return safety fields (`allergens`, `is_organic`, `dietary_tags`) as writable inputs — those are extraction-only.
- The S3/S2 safety boundary (safety fields are S1-only) must be documented in the API contract and enforced at the response layer.

### Key Entities

- **RunRequest**: source URI, domain, pipeline mode, flags (with-critic, force-fresh, no-cache)
- **RunStatus**: run ID, stage, progress, started/updated timestamps, errors
- **RunResult**: run ID, output location, row counts, DQ scores pre/post, block audit entries
- **ObservabilityQuery**: filters for source, domain, date range, run ID
- **SearchRequest**: query string, domain filter, category filter, top-k
- **RecommendationRequest**: product ID, recommendation type (also-bought / you-might-like), top-k
- **CacheFlushRequest**: prefix or domain, confirmation token

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Pipeline runs can be submitted and monitored without CLI or Streamlit access; status polling reflects real-time stage transitions.
- **SC-002**: All 7 existing MCP observability tools are reachable via the new `/observability` router with no change in response shape.
- **SC-003**: Search and recommendation endpoints return a well-formed 503 response (not an unhandled exception) when the underlying index or graph is not built.
- **SC-004**: A complete pipeline submission-to-result cycle (submit → poll → result) completes in under 5 API calls.
- **SC-005**: Health endpoint reflects degraded state within 5 seconds of a dependent service becoming unavailable.
- **SC-006**: No existing CLI, Streamlit, or graph invocation behavior changes as a result of adding the API layer.

## Clarifications

### Session 2026-04-24

- Q: FR-002 says "progress percentage" but RunStatus model has `chunk_index`; which is authoritative? → A: `chunk_index` only; percentage removed from FR-002 — total chunk count is not knowable upfront for GCS glob sources.
- Q: Can multiple pipeline runs execute simultaneously? → A: Concurrent runs for different sources allowed, capped at `MAX_CONCURRENT_RUNS` env var (default 2); exceeding cap returns 429 with retry_after; same-source conflict stays 409.
- Q: What happens to in-progress background runs on server restart? → A: On startup, any checkpoint records in `"running"` state are transitioned to `"failed"` with `error: "server_restart"`; clients resume explicitly.
- Q: Should endpoints use a `/v1/` prefix? → A: Yes — all routers versioned under `/v1/`; MCP sub-app at `/mcp` stays unversioned.
- Q: Should pipeline submission be rate-limited? → A: Rate limit `POST /v1/pipeline/runs` only — 10/minute per IP (configurable via `PIPELINE_RATE_LIMIT`); returns 429 + Retry-After header.

## Assumptions

- UC3 (hybrid search) and UC4 (recommendations) remain scaffolded; API endpoints for them define the contract and return 503 until implementations are wired in.
- Authentication and authorization are out of scope for this iteration; all endpoints are unauthenticated on the local network.
- The API runs in the same process/host as the pipeline, sharing the existing Redis, Postgres, and Prometheus connections.
- Long-running pipeline runs execute as background tasks; the API does not need a separate worker queue for this phase. On startup, orphaned `"running"` records are marked `"failed"` (reason: `"server_restart"`); resume is explicit via client action.
- The existing MCP server (`mcp_server.py`) is not refactored; new routers are added alongside it or in a new `src/api/` entry point.
- Mobile/browser clients are not a primary consumer; JSON REST is sufficient (no GraphQL, no WebSocket streaming for this phase).

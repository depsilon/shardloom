# RFC 0035: REST, Event, and Remote API Surface

## Purpose

Define CG-23 as the remote API surface that makes ShardLoom callable from
applications, agents, notebooks, CI, dashboards, services, and orchestrators
without changing the core execution identity.

The key architectural distinction:

```text
REST is ShardLoom's control plane, proof surface, orchestration API, and
small-result API. REST is not the only high-throughput data plane.
```

That gives ShardLoom a friendly HTTP API while still allowing Vortex artifacts,
Arrow IPC, object-store references, Arrow Flight, ADBC, JSON Lines, or future
streaming formats for larger analytical data.

This RFC is intentionally content-rich. It should remain stable before broad
cross-document refactors fold its details into CG-11, CG-16, CG-19, CG-20,
CG-21, CG-22, CG-4, CG-8, CG-9, CG-10, and the implementation plan.

## Status

Accepted as CG-23 intake material.

This RFC does not add an HTTP server, dependencies, runtime behavior, readers,
writers, adapters, SQL execution, DataFrame runtime, UDF runtime, streaming
runtime, object-store access, catalog access, benchmark execution, package
publication, superiority claims, best-default claims, or fallback execution.

The REST/API language in this RFC is a contract north star, not a product
claim. Public claims remain blocked until correctness, benchmark, native I/O,
execution-certificate, workload-scope, API-surface, security/governance, and
no-fallback evidence exists.

## CG-23 definition

CG-23: REST, Event, and Remote API Surface

ShardLoom is CG-23-certified for a declared workload only when remote clients
can discover capabilities, submit or reference plans, validate support, explain
and estimate execution, execute certified paths, monitor progress, cancel or
retry safely, fetch results through explicit delivery policies, inspect
certificates, export lineage/provenance, and audit governance decisions without
delegating execution to external engines or hiding materialization/fidelity
boundaries.

CG-23 is logically after CG-22. CG-21 defines the complete user data workflow.
CG-22 defines batch/live/hybrid engine modes beneath that workflow. CG-23
defines the remote control, event, result, lineage, and agent API surface
around those modes.

## Core idea

The API should not be a remote way to cheat. It should be a remote way to ask
ShardLoom:

```text
What can you do?
Can this plan run?
Which engine would run it?
What data would it read?
What would be materialized?
Can you execute it?
Where are the results?
Where are the certificates?
Was fallback attempted?
```

REST belongs primarily to the user-facing API surface while being tied to the
existing proof gates:

| Area | Primary owner | API relationship |
| --- | --- | --- |
| User API and capability surface | CG-20 / CG-21 | REST exposes the same capability workflow remotely |
| CLI/API protocol compatibility | CG-11 | REST must preserve stable machine-readable contracts |
| Engine modes | CG-22 | REST requests carry batch/live/hybrid/auto engine policy |
| Execution certificates | CG-16 | Executed paths link to execution certificates |
| Native I/O certificates | CG-19 | Source/sink/result paths link to Native I/O evidence |
| Commit/recovery | CG-4 | Writes, retries, cancellation, cleanup, and idempotency |
| Streaming/backpressure/liveness | CG-8 | Events, live subscriptions, progress, and liveness |
| Table/catalog semantics | CG-9 | Tables, snapshots, schemas, partitions, deletes, catalogs |
| Object-store/distributed behavior | CG-10 | Object references, range reads, remote commits, distributed jobs |
| Benchmark evidence | CG-6 | Remote benchmark and comparison reports remain evidence-gated |

## Same UX, not a separate product

Users should be able to choose local Python, CLI, or REST without changing the
mental model:

```python
import shardloom as sl

ctx = sl.context(engine="hybrid")
# or:
ctx = sl.remote("https://shardloom.company.internal", engine="hybrid")

orders = ctx.read_vortex("s3://lake/orders/")
result = (
    orders
    .filter(sl.col("status") == "complete")
    .group_by("customer_id")
    .agg(total=sl.sum("amount"))
)

result.explain()
result.certify()
result.write_vortex("s3://lake/customer_totals/")
```

Under the hood:

```text
local Python -> CLI / local protocol
remote Python -> REST control plane
large result -> Vortex artifact / Arrow stream / page token / Flight ticket
```

## API planes

### Control plane: REST + JSON

The REST control plane handles:

```text
capability discovery
adapter discovery
schema discovery
plan validation
SQL parse/bind/lower
DataFrame/query-builder plan submission
dry runs
execution submission
job status
cancel/retry/resume
certificates
profiles
benchmarks
migration reports
lineage/provenance reports
deployment health
```

The control-plane contract should use OpenAPI. OpenAPI defines a
language-agnostic HTTP API description that allows humans and machines to
discover service capabilities without inspecting source code or network
traffic.

### Data plane: not only REST JSON

REST JSON is appropriate for control-plane responses, diagnostics, small
previews, and small result sets. It must not be the only result transport for
analytical workloads.

Explicit result modes:

```text
inline_json       small diagnostics / tiny result previews
paged_json        small-to-medium tabular results
jsonl_ndjson      streaming row-oriented diagnostics, logs, or result rows
arrow_ipc         decoded columnar result boundary
vortex_artifact   highest-fidelity native output
object_reference  output written to object store / table path
flight_ticket     high-throughput Arrow Flight result stream
adbc_endpoint     future Arrow-native database connectivity endpoint
```

Arrow Flight and Flight SQL are important future data-plane references for
high-throughput columnar remote access. ADBC is also worth tracking because it
uses Arrow-native result streams instead of row-by-row APIs.

For ShardLoom, Arrow transfers are explicit boundaries unless a later native
boundary proves otherwise. REST must never silently normalize all results to
decoded Arrow or row-wise JSON.

### Event plane: SSE, WebSocket, AsyncAPI, CloudEvents

REST request/response alone is not enough for job progress, live queries,
hybrid freshness updates, continuous materialized views, and stream
subscriptions.

Recommended event-plane pieces:

```text
SSE          one-way job progress and certificate events
WebSocket    bidirectional live subscriptions / interactive control
AsyncAPI     machine-readable contract for event-driven APIs
CloudEvents  standard envelope for events
```

SSE is a good default for one-way progress over HTTP. WebSocket is appropriate
when bidirectional live interaction is required. AsyncAPI should describe
event channels and CloudEvents should provide a common event envelope.

## REST endpoint shape

The API must expose the full lifecycle. It should not collapse everything into:

```text
POST /execute
```

### Capability and discovery

```text
GET  /v1/health
GET  /v1/version
GET  /v1/capabilities
GET  /v1/capabilities/engines
GET  /v1/capabilities/operators
GET  /v1/capabilities/functions
GET  /v1/capabilities/sql
GET  /v1/capabilities/adapters
GET  /v1/capabilities/deployment
GET  /v1/adapters
GET  /v1/sources
GET  /v1/sinks
```

Discovery endpoints must be side-effect-free. They must not probe files,
object stores, catalogs, credentials, remote systems, or datasets unless the
user explicitly requests a scoped adapter-specific discovery operation.

Typical response shape:

```json
{
  "capability": "filter_predicate",
  "engine_modes": {
    "batch": "encoded_capable",
    "live": "planned",
    "hybrid": "planned"
  },
  "certification": {
    "correctness": "fixture_certified",
    "benchmark": "not_claim_grade",
    "execution_certificate": "available",
    "native_io_certificate": "required_per_path"
  },
  "known_limits": [
    "generalized_nonlocal_vortex_reader_wiring_not_certified"
  ],
  "fallback_attempted": false,
  "external_engine_available_as_fallback": false
}
```

### Plan, explain, and dry run

```text
POST /v1/plans
GET  /v1/plans/{plan_id}
POST /v1/plans/{plan_id}/validate
POST /v1/plans/{plan_id}/explain
POST /v1/plans/{plan_id}/estimate
POST /v1/plans/{plan_id}/certification-preview
```

REST is valuable because services, notebooks, agents, or CI workflows can ask
ShardLoom whether a workload is safe before executing it.

Example request:

```json
{
  "engine": "auto",
  "allowed_engines": ["batch", "hybrid"],
  "forbid_external_fallback": true,
  "input_refs": [
    {
      "name": "orders",
      "kind": "vortex",
      "uri": "s3://lake/orders/"
    }
  ],
  "sql": "SELECT customer_id, COUNT(*) FROM orders WHERE status = 'complete' GROUP BY customer_id",
  "result_policy": {
    "preferred_formats": ["vortex_artifact", "arrow_ipc", "paged_json"],
    "inline_row_limit": 1000
  }
}
```

Example response:

```json
{
  "plan_id": "pln_01hx",
  "selected_engine": "batch",
  "engine_selection_reason": "bounded_vortex_input",
  "sql_frontend_stage": "native_logical_plan",
  "native_physical_plan": "available",
  "unsupported_constructs": [],
  "materialization_boundaries": [],
  "source_paths": [
    {
      "name": "orders",
      "native_io_certificate_required": true,
      "representation_state": "vortex_encoded"
    }
  ],
  "fallback_attempted": false,
  "execution_ready": true
}
```

### Query and job lifecycle

Use an asynchronous job model by default:

```text
POST   /v1/queries
GET    /v1/queries/{query_id}
DELETE /v1/queries/{query_id}
POST   /v1/queries/{query_id}/cancel
POST   /v1/queries/{query_id}/retry
GET    /v1/queries/{query_id}/events
GET    /v1/queries/{query_id}/profile
GET    /v1/queries/{query_id}/certificates
GET    /v1/queries/{query_id}/lineage
GET    /v1/queries/{query_id}/results
GET    /v1/queries/{query_id}/results/pages/{page_token}
GET    /v1/queries/{query_id}/artifacts
```

Analytical systems commonly use submit/status/result/cancel lifecycles. For
ShardLoom, the lifecycle pattern is useful, but the execution philosophy stays
ShardLoom-native and no-fallback.

### Results

Result responses must expose the data-delivery choice:

```json
{
  "query_id": "qry_01hx",
  "status": "succeeded",
  "result_sets": [
    {
      "result_id": "res_01hx",
      "format": "vortex_artifact",
      "uri": "s3://lake/output/customer_totals/",
      "native_representation_preserved": true,
      "row_count": 1042391,
      "certificate_refs": ["cert_exec_01hx", "cert_io_01hx"]
    },
    {
      "result_id": "res_preview_01hx",
      "format": "paged_json",
      "rows_inline": 100,
      "materialization_boundary": "preview_only"
    }
  ],
  "fallback_attempted": false
}
```

Preview output can be row-wise JSON. Large analytical results should usually be
Vortex outputs, Arrow streams, object references, or Flight tickets.

## Error design

REST errors should use `application/problem+json` problem details with
ShardLoom diagnostic extensions.

Example:

```json
{
  "type": "https://shardloom.dev/problems/unsupported-operator",
  "title": "Operator is not certified for the requested engine",
  "status": 422,
  "detail": "Global sort is not supported for live unbounded input.",
  "instance": "/v1/plans/pln_01hx/validate",
  "shardloom": {
    "operation": "sort",
    "requested_engine": "live",
    "supported_engines": ["batch"],
    "required_gate": "CG-20 operator coverage + CG-8 streaming semantics",
    "rewrite_suggestions": [
      "Use a bounded windowed sort",
      "Run the workload in batch mode",
      "Add LIMIT with an explicit ordering and bounded source"
    ],
    "fallback_attempted": false,
    "external_engine_invoked": false
  }
}
```

This preserves ShardLoom's deterministic unsupported-diagnostics posture over
HTTP.

## Engine-aware API support

The API should make engine choice explicit:

```json
{
  "engine": "hybrid",
  "freshness": {
    "target_ms": 30000,
    "max_lag_ms": 120000
  },
  "consistency": "snapshot",
  "output_mode": "materialized_view"
}
```

Supported engine values:

```text
batch
live
hybrid
auto
```

### Batch behavior

Batch is finite and snapshot-oriented:

```text
bounded source
finite plan
finite result
certified output artifacts
benchmarkable runtime
```

### Live behavior

Live is subscription-oriented:

```text
unbounded source
watermarks
checkpoints
state
late data policy
output changelog
continuous materialized view
```

Useful endpoints:

```text
POST   /v1/live/subscriptions
GET    /v1/live/subscriptions/{id}
GET    /v1/live/subscriptions/{id}/events
POST   /v1/live/subscriptions/{id}/pause
POST   /v1/live/subscriptions/{id}/resume
DELETE /v1/live/subscriptions/{id}
```

Events should support CloudEvents-style envelopes:

```json
{
  "specversion": "1.0",
  "type": "dev.shardloom.query.progress",
  "source": "shardloom://queries/qry_01hx",
  "id": "evt_01hx",
  "time": "2026-05-10T15:40:11Z",
  "data": {
    "status": "running",
    "watermark": "2026-05-10T15:39:30Z",
    "state_bytes": 8823112,
    "fallback_attempted": false
  }
}
```

### Hybrid behavior

Hybrid should expose hot/warm/cold contribution:

```json
{
  "selected_engine": "hybrid",
  "snapshot_epoch": "epoch_1842938123",
  "base_snapshot": "snap_01hx",
  "hot_delta": {
    "rows": 18922,
    "changelog_range": ["off_123", "off_491"]
  },
  "warm_micro_segments": 14,
  "cold_vortex_segments": 823,
  "tombstones_applied": 331,
  "freshness_lag_ms": 17200,
  "fallback_attempted": false
}
```

This is the differentiated story: not just "query is running", but proof of
which temporal layers contributed to the result.

## REST API maturity ladder

CG-23 adds an API-specific maturity ladder:

```text
API-A0 declared only
API-A1 OpenAPI contract, no server
API-A2 local loopback server, discovery only
API-A3 plan/explain/validate/certify preview
API-A4 async query lifecycle for local certified batch paths
API-A5 result delivery: inline JSON, paged JSON, JSONL, Arrow IPC, Vortex artifact refs
API-A6 source/sink adapter API with Native I/O certificates
API-A7 live/hybrid event APIs with AsyncAPI + CloudEvents
API-A8 security/governance/quotas/audit
API-A9 production-certified API for declared workload constitution
```

An endpoint existing does not mean production support. Maturity is evidence
scoped.

## Proposed implementation lanes

### CG-23A: REST API contract surface

Scope:

```text
Define ShardLoom REST API resources, OpenAPI schema, versioning rules, error
format, no-fallback fields, and certificate/result link conventions.
```

Acceptance:

```text
- OpenAPI document exists for /v1.
- Capabilities, plans, queries, results, adapters, certificates, profiles,
  benchmarks, and migration reports are represented.
- Every execution-capable request includes engine mode, fallback policy,
  materialization policy, result policy, and evidence policy.
- Every response includes fallback_attempted=false or explicit unsupported/failure reason.
- Errors use application/problem+json with ShardLoom diagnostic extensions.
- No HTTP server implementation required yet.
- No SQL/DataFrame/runtime expansion.
- No external-engine fallback.
```

### CG-23B: REST discovery server

Scope:

```text
Add a local HTTP server mode for health, version, capabilities, adapters,
deployment readiness, and no-dataset smoke checks.
```

Example:

```bash
shardloom serve --bind 127.0.0.1:8787 --mode discovery
```

Acceptance:

```text
- GET /v1/health works.
- GET /v1/version works.
- GET /v1/capabilities works.
- GET /v1/adapters works.
- No dataset probing.
- No query execution.
- No object-store access.
- No catalog access.
- No fallback.
```

### CG-23C: Plan/explain/validate API

Scope:

```text
Expose plan validation, SQL frontend staging, unsupported diagnostics,
materialization preview, and engine-selection preview over REST.
```

Acceptance:

```text
- POST /v1/plans creates a plan handle.
- POST /v1/plans/{id}/validate validates capability without executing.
- POST /v1/plans/{id}/explain returns logical/physical/certification preview.
- Unsupported constructs return deterministic problem+json.
- Parser/binder/native plan stages are distinct.
- No execution delegation.
```

### CG-23D: Async query lifecycle API

Scope:

```text
Expose execute/status/cancel/result/certificate lifecycle for already-certified
local batch paths.
```

Acceptance:

```text
- POST /v1/queries returns query_id and status.
- GET /v1/queries/{id} returns state, engine, progress, diagnostics.
- DELETE or POST cancel terminates running query deterministically.
- Results are accessed through explicit result handles.
- Certificates and Native I/O evidence are linked.
- Non-certified paths remain blocked or explicitly uncertified.
```

### CG-23E: Result delivery and spooling

Scope:

```text
Support multiple result delivery policies without forcing large results into
JSON.
```

Acceptance:

```text
- Small preview results can be inline JSON.
- Larger row-oriented output can be paged JSON or JSON Lines.
- Columnar/materialized output can be Arrow IPC only as an explicit decoded-columnar boundary.
- Highest-fidelity native output is Vortex artifact/reference.
- Every result declares representation state and materialization/fidelity boundaries.
- Result TTL, retention, and cleanup are explicit.
```

### CG-23F: Live/hybrid event API

Scope:

```text
Expose job progress, live subscriptions, continuous materialized view updates,
watermarks, checkpoints, state, and hybrid hot/cold contribution events.
```

Acceptance:

```text
- SSE endpoint exists for query/job progress.
- AsyncAPI document describes event channels.
- CloudEvents envelope is available for progress/state/certificate events.
- Live/hybrid endpoints do not claim exactly-once, freshness, or checkpoint certification without CG-8/CG-4 evidence.
- No Kafka/Flink/Materialize fallback.
```

### CG-23G: Security, governance, and API policy

Scope:

```text
Make remote execution safe.
```

Acceptance:

```text
- Auth mode is explicit: local-only, token, mTLS, OIDC, or service-account integration.
- Scopes distinguish read, plan, execute, write, cancel, admin, benchmark, and agent operations.
- Credentials are references, not raw leaked values.
- Diagnostics redact secrets and sensitive values.
- Destructive operations require explicit policy.
- Audit events exist for plan/execute/write/cancel/certify.
```

### CG-23H: Arrow Flight / ADBC data-plane bridge

Scope:

```text
Keep REST as control plane while enabling high-throughput columnar remote access later.
```

Acceptance:

```text
- REST can return a Flight endpoint/ticket for result transfer.
- Flight/ADBC remains optional, not required for basic REST.
- Any Arrow transfer is reported as decoded-columnar unless a later native boundary proves otherwise.
- REST certificates still govern what happened.
```

### CG-23I: MCP agent API

Scope:

```text
Expose safe ShardLoom capabilities to AI agents without giving them unrestricted
execution power.
```

Acceptance:

```text
- MCP resources expose capabilities, schemas, plans, certificates, benchmark reports, and diagnostics.
- MCP tools are limited to dry-run/explain/certify by default.
- Execute/write/cancel require explicit policy and credentials.
- Agent-facing diagnostics preserve no-fallback evidence.
```

MCP should be a thin agent surface over the same REST/control-plane semantics,
not a separate execution path.

## What REST gives ShardLoom

REST support lets ShardLoom become:

```text
importable locally
callable remotely
usable by notebooks
usable by web apps
usable by CI
usable by workflow orchestrators
usable by AI agents
usable by benchmark harnesses
usable by data catalogs
usable by dashboards
```

The stronger version is not merely:

```text
run query over HTTP
```

It is:

```text
plan, explain, execute, monitor, cancel, fetch, certify, benchmark, migrate,
govern, and audit over HTTP.
```

## Technology and standards to track

### OpenAPI

Use OpenAPI for the REST contract. It gives ShardLoom generated clients,
schema validation, test generation, documentation, and a stable
machine-readable API surface.

### AsyncAPI

Use AsyncAPI for live/hybrid event APIs. REST describes request/response;
AsyncAPI describes streams, subscriptions, and event channels.

### CloudEvents

Use CloudEvents for progress, state, certificate, watermark, checkpoint,
lineage, and benchmark events.

### RFC 9457 Problem Details

Use problem details for HTTP errors. ShardLoom's deterministic unsupported
diagnostics fit naturally inside `application/problem+json` with ShardLoom
extensions.

### OpenTelemetry / OTLP

ShardLoom should export traces, metrics, and logs through OpenTelemetry where
enabled.

Concept mapping:

```text
query_id -> trace_id
operator -> span
source read -> span
sink write -> span
certificate -> span event / artifact link
fallback_attempted=false -> span attribute
rows_scanned / bytes_read / segments_pruned -> metrics
```

### OpenLineage

ShardLoom should be able to emit lineage events with custom facets:

```text
ShardLoomExecutionCertificateFacet
ShardLoomNativeIoCertificateFacet
ShardLoomMaterializationBoundaryFacet
ShardLoomNoFallbackFacet
ShardLoomRepresentationStateFacet
```

### Apache Iceberg REST Catalog, Polaris, and Gravitino

Catalog REST should not be ignored. ShardLoom should distinguish:

```text
ShardLoom REST API   -> execution/certification API
Iceberg REST Catalog -> table/catalog metadata API
Polaris/Gravitino    -> external catalog integrations
```

Do not reinvent every catalog API. Integrate where standards exist.

### Delta Sharing

Delta Sharing is a useful pattern even if ShardLoom does not become a Delta
Sharing server immediately:

```text
REST authorizes and describes data.
Cloud/object storage transfers the large payload.
```

That is the right pattern for ShardLoom result artifacts.

### Substrait

Substrait remains useful as:

```text
import/export plan representation
migration analysis input
SQL/DataFrame frontend interchange
benchmark workload description
```

It must not become a fallback bridge to other execution engines.

### Arrow Flight SQL and ADBC

REST is not enough for serious remote analytics. Arrow Flight SQL and ADBC are
important adjacent protocols for columnar remote data access.

Recommended sequence:

```text
Phase 1: REST control plane
Phase 2: REST result artifacts
Phase 3: Arrow IPC result option
Phase 4: Flight/ADBC data plane
```

### Arrow C Stream / PyCapsule interface

For Python interoperability, Arrow's C Stream and PyCapsule interfaces matter.
They are relevant to `to_arrow()`, `from_arrow()`, `to_pandas()`, notebook
preview, and remote result streaming.

### WebAssembly Component Model / WASI

WebAssembly remains a candidate for safer long-term UDFs or adapter plugins:

```text
Rust-native UDFs first
WASM component UDFs later
Python/external UDFs only as explicit materialization/effect boundaries
```

### Apache Paimon and Apache Fluss

Paimon and Fluss are relevant design references for the live/hybrid engine
idea:

```text
streaming freshness
+ lakehouse persistence
+ analytical reads
+ real-time updates
```

ShardLoom's difference should be Vortex-native execution plus certificates.

### NATS JetStream / Redpanda / Kafka-compatible substrates

If ShardLoom eventually needs event ingestion, replay, or live-state inputs, it
should not immediately build a broker. Durable event systems should be adapter
or reference substrates, not ShardLoom core dependencies.

### MCP for agents

MCP is worth tracking for agent-facing access:

```text
resources:
  capabilities
  schemas
  plans
  certificates
  benchmark reports
  diagnostics

tools:
  validate_plan
  explain_plan
  estimate_plan
  certify_query
  run_benchmark_dry_run
```

Execution and writes must be disabled by default for agents.

## What to avoid

- Do not make REST return huge analytical result sets as row-wise JSON by
  default.
- Do not make REST execution a new fallback path.
- Do not make the REST server required for local `import shardloom`.
- Do not treat Arrow output as native unless the representation transition is
  reported.
- Do not collapse API status into "supported."
- Do not make external engines runtime dependencies through server-side
  convenience.
- Do not hide object-store, catalog, credential, or network side effects.
- Do not expose execution/write/cancel MCP tools by default.

Use the same maturity vocabulary as other capability surfaces:

```text
declared
documented
discoverable
plan-only
dry-run
local executable
fixture-certified
workload-certified
production-certified
```

## Certification blockers

The following block CG-23 certification for a declared workload:

- no OpenAPI contract for the relevant endpoint set
- endpoint status presented as production support without evidence
- discovery endpoint performs filesystem, network, catalog, or adapter probes
  without explicit user request
- execution-capable request lacks engine mode or fallback policy
- result delivery policy is absent
- large result is forced into row-wise JSON without explicit preview/materialization
  boundary
- Arrow result is presented as native without representation transition evidence
- error response lacks deterministic ShardLoom diagnostics
- unsupported request returns generic `500` instead of problem details
- executed path lacks execution certificate links
- source/sink path lacks Native I/O certificate links
- materialization or fidelity loss is hidden
- live/hybrid API claims freshness, checkpoint, or exactly-once behavior without
  CG-8/CG-4 evidence
- remote writes lack commit/recovery/idempotency evidence
- security/governance/audit policy is absent for remote execution
- agent API exposes execute/write/cancel by default
- external engine is invoked as fallback
- missing `fallback_attempted=false`

## Shared policy, lifecycle, and parity contracts

CG-23 must not invent a separate remote execution model. REST, event, and
future agent surfaces should expose the same policy, lifecycle, evidence, and
diagnostic fields used by CLI and Python:

- `ShardLoomExecutionPolicy` is the execution-capable request policy object.
- `QueryLifecycleContract` is the state machine for async query, live
  subscription, hybrid materialized-view, cancellation, retry, result retention,
  certificate retention, cleanup, and side-effect status.
- `EvidenceArtifactEnvelope` is the reference wrapper for certificates,
  profiles, result artifacts, lineage events, benchmark rows, and diagnostics.
- `EvidenceArtifactSafety` controls redaction, retention, export, and
  agent-visible evidence.
- `ProtocolSurfaceParityReport` checks that CLI JSON, Python, REST/OpenAPI,
  future MCP, and future Flight/ADBC metadata expose consistent fields or
  explicit unavailable reasons.

Protocol parity is a certification requirement. A REST endpoint cannot report a
feature as supported if the underlying CLI/Python capability report remains
planned, unsupported, or missing required certificate evidence.

## References

- OpenAPI Specification:
  `https://spec.openapis.org/oas/v3.2.0.html`
- AsyncAPI Specification:
  `https://www.asyncapi.com/docs/reference/specification/v3.1.0`
- CloudEvents:
  `https://cloudevents.io/`
- RFC 9457 Problem Details for HTTP APIs:
  `https://www.rfc-editor.org/rfc/rfc9457.html`
- Apache Arrow Flight:
  `https://arrow.apache.org/docs/format/Flight.html`
- Apache Arrow Flight SQL:
  `https://arrow.apache.org/docs/format/FlightSql.html`
- Apache Arrow ADBC:
  `https://arrow.apache.org/docs/format/ADBC.html`
- Arrow C Stream Interface:
  `https://arrow.apache.org/docs/format/CStreamInterface.html`
- OpenTelemetry OTLP:
  `https://opentelemetry.io/docs/specs/otlp/`
- OpenLineage:
  `https://openlineage.io/`
- Apache Iceberg REST Catalog:
  `https://iceberg.apache.org/rest-catalog-spec/`
- Apache Polaris:
  `https://polaris.apache.org/`
- Apache Gravitino:
  `https://gravitino.apache.org/`
- Delta Sharing:
  `https://github.com/delta-io/delta-sharing`
- Substrait:
  `https://substrait.io/`
- JSON Lines:
  `https://jsonlines.org/`
- WHATWG Server-Sent Events:
  `https://html.spec.whatwg.org/multipage/server-sent-events.html`
- RFC 6455 WebSocket Protocol:
  `https://www.rfc-editor.org/rfc/rfc6455.html`
- WASI interfaces:
  `https://wasi.dev/interfaces`
- Apache Paimon:
  `https://paimon.apache.org/docs/master/`
- Apache Fluss:
  `https://fluss.apache.org/`
- NATS JetStream:
  `https://docs.nats.io/nats-concepts/jetstream`
- Model Context Protocol:
  `https://modelcontextprotocol.io/specification/2025-06-18`
- Universal Native I/O Envelope:
  `docs/rfcs/0031-universal-native-io-envelope.md`
- World-Class SQL, Operator, Function, Adapter, and User Capability Surface:
  `docs/rfcs/0032-world-class-sql-operators-functions-adapters-user-capability.md`
- User Data Workflow and ETL Surface:
  `docs/rfcs/0033-user-data-workflow-etl-surface.md`
- Three-Engine Certified Data Execution Fabric:
  `docs/rfcs/0034-three-engine-certified-data-execution-fabric.md`

## Recommendation

Add REST as ShardLoom's remote control plane, proof surface, orchestration API,
and small-result API. Pair it with OpenAPI, Problem Details, OpenTelemetry,
OpenLineage, AsyncAPI, CloudEvents, and future Flight/ADBC data-plane bridges.

The strongest API story is:

```text
You can install me locally.
You can import me in Python.
You can call me over REST.
You can submit SQL/DataFrame/native plans.
You can choose batch/live/hybrid.
You can stream progress.
You can fetch results in the right format.
You can inspect certificates.
You can export lineage.
You can benchmark against other engines.
You can prove no fallback happened.
```

That is much more powerful than "ShardLoom has an HTTP endpoint."

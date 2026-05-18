# REST Server Runtime Unsupported Contract

## Purpose

`GAR-0035-A` adds the report-only
`shardloom.rest_api_runtime_unsupported_contract.v1` contract for ShardLoom's CG-23 REST, event,
and remote API surface. The contract makes the current public boundary explicit: checked-in
OpenAPI/AsyncAPI documents and CLI/Python reports exist, but no HTTP listener, remote execution
runtime, Flight/ADBC bridge, broker integration, or dependency-expanded server package is
supported.

This is a capability and diagnostic surface only. It does not start a server, open sockets, execute
remote plans, stream data, resolve credentials, invoke external engines, publish packages, or
attempt fallback.

## User-Visible Surfaces

- `shardloom rest-api-contract-plan --format json`
- `shardloom serve --mode discovery --format json`
- Python `ctx.rest_api_contract_plan()` through `RestApiContractPlan`
- checked-in `docs/api/shardloom-openapi-v1.yaml`
- checked-in `docs/api/shardloom-asyncapi-events-v1.yaml`
- this architecture reference

The CLI and Python surfaces expose:

- `rest_runtime_unsupported_schema_version=shardloom.rest_api_runtime_unsupported_contract.v1`
- `rest_runtime_unsupported_report_id=gar-0035-a.rest_api_runtime_unsupported_contract`
- `rest_runtime_unsupported_claim_gate_status=not_claim_grade`
- `rest_runtime_server_started=false`
- `rest_runtime_network_listener_opened=false`
- `rest_runtime_external_engine_invoked=false`
- `rest_runtime_fallback_attempted=false`

## Gate Rows

| Row | Status | Diagnostic | Boundary |
| --- | --- | --- | --- |
| `http_listener_runtime` | `blocked` | `SL_REST_SERVER_UNSUPPORTED` | No HTTP listener or production REST server runtime claim. |
| `remote_execution_runtime` | `blocked` | `SL_REMOTE_EXECUTION_UNSUPPORTED` | No remote execution claim without workload-scoped execution and certificate evidence. |
| `flight_adbc_transport_runtime` | `blocked` | `SL_COLUMNAR_TRANSPORT_UNSUPPORTED` | No Flight or ADBC transport runtime claim. |
| `external_broker_integration` | `blocked` | `SL_EXTERNAL_BROKER_UNSUPPORTED` | No external broker integration or event-plane runtime claim. |
| `dependency_expanded_server` | `blocked` | `SL_SERVER_DEPENDENCY_UNSUPPORTED` | No dependency-expanded server package or release claim. |
| `openapi_discovery_contract` | `report_only` | `SL_REPORT_ONLY_SURFACE` | OpenAPI discovery is contract-only and does not start a server. |
| `plan_preview_contract` | `report_only` | `SL_REPORT_ONLY_SURFACE` | Plan preview reports deterministic support posture without executing remotely. |
| `result_delivery_contract` | `report_only` | `SL_REPORT_ONLY_SURFACE` | Result delivery modes are declared as contract boundaries, not transport runtime. |

The stable summary counts for this slice are:

- `rest_runtime_unsupported_blocked_row_count=5`
- `rest_runtime_unsupported_report_only_row_count=3`

## Evidence Required Before Promotion

Future slices cannot promote a row from blocked/report-only posture without workload-scoped evidence.
The gate names the evidence classes that must exist before a REST runtime claim can be considered:

- `server_dependency_audit`
- `listener_lifecycle_evidence`
- `workload_certificate`
- `execution_certificate`
- `native_io_certificate`
- `security_policy`
- `columnar_transport_certificate`
- `broker_policy`
- `dependency_audit`
- `no_fallback_evidence`

## Claim Boundary

The current claim boundary is:

```text
OpenAPI and report-only REST planning are contract surfaces only; HTTP listener, remote execution,
Flight/ADBC, broker integration, dependency-expanded server, production API, benchmark, and
Spark-displacement claims remain blocked
```

The following booleans remain `false`:

- `rest_runtime_http_listener_supported`
- `rest_runtime_remote_execution_supported`
- `rest_runtime_flight_adbc_transport_supported`
- `rest_runtime_external_broker_supported`
- `rest_runtime_dependency_expansion_allowed`
- `rest_runtime_server_started`
- `rest_runtime_network_listener_opened`
- `rest_runtime_execution`
- `rest_runtime_query_execution`
- `rest_runtime_write_io`
- `rest_runtime_object_store_io`
- `rest_runtime_catalog_probe`
- `rest_runtime_credential_resolution`
- `rest_runtime_external_engine_invoked`
- `rest_runtime_fallback_attempted`

## Non-Goals

This slice does not add:

- HTTP listener or server lifecycle runtime
- remote execution
- Flight or ADBC server/transport runtime
- broker, WebSocket, or SSE runtime
- dependency-expanded web framework/server packaging
- package publication or release claim
- object-store/table/catalog runtime
- production API support
- benchmark, performance, or Spark replacement claims

## Fallback Boundary

Every REST runtime gate row preserves:

```text
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

External systems may appear only as future transports, baselines, or integration references. They
cannot satisfy ShardLoom execution evidence and cannot act as fallback engines.

# UniversalIngress And Vortex Ingest Route Taxonomy

Status: planned/runtime-safety prerequisite for `GAR-RUNTIME-IMPL-4F0`.

## Decision

`prepared_vortex` does not read arbitrary non-Vortex input directly.

`prepared_vortex` executes from `VortexPreparedState`. Non-Vortex input reaches that mode only
through:

```text
UniversalIngress / InputAdapter
-> SourceState
-> vortex_ingest
-> VortexPreparedState
-> prepared_vortex execution
-> OutputPlan
-> SinkArtifact / evidence
```

`compatibility_import_certified` uses the same `UniversalIngress` and `vortex_ingest` machinery, but
it is the certified cold route. It requires source, ingest, Vortex write/reopen, scan, output/replay,
certificate, no-fallback, and claim-gate evidence before a certified ingest/stage claim is allowed.

The two routes must recognize the same potential non-Vortex source universe. A source may be
`runtime_supported`, `smoke_supported`, `report_only`, `blocked`, `unsupported`, or `not_planned`,
but it must not appear in one route and silently disappear from the other.

## Route Layers

| Layer | Question | Canonical examples |
| --- | --- | --- |
| Access surface | How does the user express the work? | CLI, Python, SQL, future DataFrame/API. |
| Source route | What kind of source is being admitted? | Local file, existing Vortex, generated source, object-store/table/platform source. |
| Ingress route | How is the source admitted or prepared? | `vortex_ingest`, `certified_vortex_ingest`, `native_vortex_existing`, `generated_source`, internal local-source smoke. |
| Execution mode | What compute route executes? | `prepared_vortex`, `native_vortex`, `compatibility_import_certified`; `internal_local_source_smoke` is internal smoke-only. |
| Output route | Where does the result go? | Local JSONL/CSV/Parquet/Vortex, output fanout, future object-store/table/Foundry sinks. |
| Evidence policy | How much proof is required? | `minimal_runtime`, `certified`, `full_replay`. |

## Canonical Route Labels

| Label | Canonical route | Meaning |
| --- | --- | --- |
| Vortex ingest / prepare once route | `vortex_ingest` | Converts an admitted non-Vortex `SourceState` into a reusable `VortexPreparedState`. |
| Prepared Vortex route | `prepared_vortex` | Executes from `VortexPreparedState`; it does not read source files or rows directly. |
| Certified import/stage route | `compatibility_import_certified` | Cold end-to-end certified route over `UniversalIngress` and `vortex_ingest`. |
| Native Vortex route | `native_vortex` | Executes from existing Vortex input or admitted native Vortex state. |
| Internal local-source smoke route | `internal_local_source_smoke` | Internal lower-level safeguard only; public local-file workflows use `vortex_middle` policy and prepare into Vortex or use native Vortex input. |
| Source-free generated route | generated-source route | Creates rows without source I/O and emits generated-source/output evidence. |
| Output fanout route | output route | Reuses admitted source/prepared/result state for one or more sink artifacts. |

## Timing Scopes

| Timing scope | Use |
| --- | --- |
| `ingest_only` | Measures only source admission/preparation work. |
| `cold_certified_end_to_end` | Measures source read, parse, Vortex ingest, write/reopen, scan, compute, sink, and evidence. |
| `warm_prepared_query` | Measures query/runtime work after `VortexPreparedState` already exists. |
| `native_query` | Measures query/runtime over existing Vortex input. |
| `internal_direct_one_shot` | Internal diagnostic timing for read/parse plus direct ShardLoom-owned compute/output; not a public workflow route, not a product runtime lane, and not a replacement for Vortex preparation/native Vortex execution. |
| `generated_output` | Measures generated rows plus output/evidence; source read is zero. |
| `output_fanout` | Measures query/reuse plus per-output planning/write/replay. |

## Required Route Evidence Fields

Runtime and benchmark envelopes should expose these fields where applicable:

```text
source_kind
source_format
source_adapter_id
source_adapter_status
source_adapter_blocker_id
ingress_route
ingress_route_label
ingress_status
ingress_certification_level
vortex_ingest_performed
vortex_ingest_status
vortex_ingest_blocker_id
prepared_state_id
prepared_state_digest
prepared_state_created
prepared_state_reused
prepared_state_reuse_hit
execution_mode
selected_execution_mode
execution_route_label
timing_scope
certification_policy
certification_status
certification_blocker_id
output_route
output_format
output_plan_id
output_plan_status
claim_gate_status
fallback_attempted=false
external_engine_invoked=false
```

## Source Universe

The companion machine-readable taxonomy is:

```text
docs/architecture/universal-ingress-route-taxonomy.json
```

It projects the existing universal compatibility scoreboard into route status rows for:

- local CSV, JSONL/NDJSON, flat JSON, Parquet, Arrow IPC, Avro, ORC, Excel
- SQLite/local database files, Postgres, MySQL, JDBC/ODBC, Snowflake, BigQuery, Databricks SQL
- S3, GCS, ADLS
- Iceberg, Delta, Hudi
- existing Vortex artifacts
- generated/user rows, range, sequence, literal table, calendar
- SQL VALUES, literal SELECT, generate_series/range
- REST, Flight, ADBC, API/event/SaaS adapters, unstructured/media references
- Foundry datasets

Recognized does not mean supported. Unsupported rows must carry deterministic blocker IDs, preserve
`fallback_attempted=false`, and preserve `external_engine_invoked=false`.

SQLite/local database files now have a narrow local adapter smoke: `sqlite-local-import-export-smoke`
table-scans a named table from a local SQLite file, writes a workspace-safe JSONL export, and creates
a roundtrip local SQLite artifact with row-count replay evidence. The optional `--order-by` flag is
post-scan fixture ordering in ShardLoom, not SQLite query pushdown, and BLOB schemas/values are
blocked. That does not admit arbitrary SQL, query pushdown, Vortex ingest, network databases,
warehouses, credentials, extension loading, fallback execution, or performance claims.

## Benchmark Interpretation

Benchmark pages and docs should compare routes, not front doors:

| User-facing comparison | Internal interpretation |
| --- | --- |
| Certified cold route | `compatibility_import_certified` cold end-to-end ingest/stage/certification timing. |
| Prepared warm route | `prepared_vortex` query/runtime timing after `VortexPreparedState` exists. |
| Native Vortex route | `native_vortex` query/runtime timing from existing Vortex input. |
| Internal local-source smoke route | `internal_local_source_smoke` internal smoke timing only; not Vortex-native and not a public workflow route. |
| Source-free generated route | generated rows plus output/evidence, no source read. |

Prepared rows must warn when preparation is excluded from timing. They are not evidence that CSV,
Parquet, JSONL, or another non-Vortex source was read directly by `prepared_vortex`.

## Validation

`scripts/check_universal_ingress_routes.py` enforces the current taxonomy:

- every required source alias is represented
- every row reports `UniversalIngress`, `vortex_ingest`, and certified route status where applicable
- `prepared_vortex_direct_source_input_allowed=false`
- `prepared_vortex_requires_prepared_state=true`
- every non-supported source has a deterministic blocker ID
- no row reports fallback or external engine invocation

## Non-Goals

- No new object-store runtime.
- No table/lakehouse runtime.
- No Foundry production support.
- No public package publication.
- No performance, superiority, Spark-replacement, or production claim.
- No fallback execution through pandas, Polars, DuckDB, DataFusion, Spark, Dask, Ray, databases,
  warehouses, managed platforms, or Vortex query-engine integrations.

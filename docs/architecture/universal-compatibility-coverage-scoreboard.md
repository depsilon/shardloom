# Universal Compatibility Coverage Scoreboard

## Purpose

This document is the report-only compatibility map for ShardLoom source, sink, adapter, and
user-facing data creation surfaces.

The machine-readable projection lives beside this document:

```text
docs/architecture/universal-compatibility-coverage-scoreboard.json
schema_version=shardloom.universal_compatibility_coverage_scoreboard.v1
```

The route-status projection lives beside it:

```text
docs/architecture/universal-ingress-route-taxonomy.json
schema_version=shardloom.universal_ingress_route_taxonomy.v1
```

Website/status, CLI capability JSON, and Python typed capability views must use typed fields from
the machine-readable projection or capability envelopes rather than scraping the Markdown prose.

It answers one question:

```text
Which compatibility surfaces are runtime-supported today, which are only smoke/report surfaces,
which are blocked, and what evidence would be required before any support claim is allowed?
```

This scoreboard is not a production support claim, a performance claim, a Spark replacement claim,
or a live object-store, lakehouse, or Foundry runtime claim.

This scoreboard also does not make `prepared_vortex` a reader for non-Vortex sources.
`prepared_vortex` executes from `VortexPreparedState`. Non-Vortex sources reach it through
`UniversalIngress / InputAdapter -> SourceState -> vortex_ingest -> VortexPreparedState`.
`compatibility_import_certified` uses the same source universe and preparation machinery, but with
certified cold-route evidence requirements.

## Status Vocabulary

| Status | Meaning |
| --- | --- |
| `runtime-supported` | A scoped local runtime path exists with evidence for the named surface. The row still needs its own claim boundary. |
| `smoke-supported` | A narrow fixture or local smoke path exists. It does not imply broad runtime support. |
| `report-only` | ShardLoom can describe, diagnose, or plan the surface without executing it. |
| `blocked` | The surface must fail or remain unavailable until a future evidence-bearing slice lands. |
| `not-planned` | The surface is intentionally out of scope unless a future RFC changes that decision. |

Every row must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

External engines such as Spark, DataFusion, DuckDB, Polars, and database or warehouse systems may
be baselines, oracles, migration references, or import/export endpoints only. They are not ShardLoom
fallback execution.

## Coverage Scoreboard

| Surface | Family | Current status | Runtime coverage | Plan/report coverage | Native I/O and output status | Blockers / next evidence | Claim boundary |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CSV | Local compatibility file | `runtime-supported` | Local CSV now routes through the shared SourceState adapter, direct-transient runtime, `vortex_ingest`, and local output evidence for admitted flat schemas. | Coverage rows distinguish compatibility import, prepared/native Vortex execution, and claim boundaries. | SourceState, Vortex ingest, and local output evidence are scoped to admitted local workflows. | Object-store/table and production claim evidence. | Local CSV runtime for admitted flat schemas only; no object-store/table or production claim. |
| JSONL / NDJSON | Local compatibility file | `runtime-supported` | Local flat JSONL/NDJSON now routes through the shared SourceState adapter, direct-transient runtime, `vortex_ingest`, and JSONL output evidence. | Capability rows distinguish line-record JSONL/NDJSON from nested/general JSON semantics. | SourceState, Vortex ingest, and local JSONL output evidence are scoped to admitted local workflows. | Nested JSON, JSONPath, object-store/table, and production evidence. | Local flat JSONL/NDJSON runtime only. |
| JSON | Local compatibility file | `runtime-supported` | Local flat top-level JSON object/array input now routes through SourceState and `vortex_ingest` evidence. | Capability rows keep JSON input separate from JSONL/NDJSON line records and JSON sink support. | SourceState and Vortex ingest evidence exist; JSON output remains outside this row. | Nested JSON, JSONPath, JSON sink, object-store/table, and production evidence. | Local flat JSON input runtime only. |
| Parquet | Local compatibility file | `runtime-supported` | Feature-gated flat scalar local Parquet now routes through columnar SourceState, direct-transient runtime, `vortex_ingest`, and local sink evidence. | Rows separate Parquet preparation/runtime from Vortex-native execution and table/lakehouse claims. | Columnar SourceState, Vortex ingest, and local output evidence are scoped to admitted flat schemas. | Nested types, metadata fidelity, table/object-store, and production evidence. | Feature-gated flat scalar local Parquet runtime only. |
| Arrow IPC | Local compatibility file | `runtime-supported` | Feature-gated flat scalar local Arrow IPC now routes through columnar SourceState, direct-transient runtime, `vortex_ingest`, and local sink evidence. | Arrow boundaries remain compatibility/interop surfaces, not the internal execution substrate claim. | Native I/O evidence records the decode/materialization boundary. | Zero-copy, streaming, nested type, default-build, object-store, and production evidence. | Feature-gated flat scalar local Arrow IPC runtime only. |
| Avro | Local compatibility file | `runtime-supported` | Feature-gated flat scalar local Avro now routes through columnar SourceState, direct-transient runtime, `vortex_ingest`, and local sink evidence. | Avro remains a compatibility preparation/runtime surface. | Native I/O evidence is scoped to the admitted path and cannot imply native Avro execution. | Schema evolution, logical type completeness, object-store/table, and production evidence. | Feature-gated flat scalar local Avro runtime only. |
| ORC | Local compatibility file | `runtime-supported` | Feature-gated flat scalar local ORC now routes through columnar SourceState, direct-transient runtime, `vortex_ingest`, and local sink evidence. | ORC remains a compatibility preparation/runtime surface. | Native I/O evidence is scoped to the admitted path and cannot imply native ORC execution. | Stripe/statistics, nested type, object-store/table, and production evidence. | Feature-gated flat scalar local ORC runtime only. |
| Excel | Local desktop/document file | `blocked` | No first-class Excel runtime source or sink is supported. | Future report rows may classify workbook, sheet, range, type inference, and formula policy. | No Native I/O certificate may be claimed. | Parser/dependency approval, license review, deterministic schema policy, formula/effect policy. | Not supported. |
| SQLite | Database file | `smoke-supported` | `sqlite-local-import-export-smoke` table-scans a named local SQLite table, writes a workspace-safe JSONL export, and creates a roundtrip local SQLite artifact with replay evidence. | SQLite query pushdown, arbitrary SQL, and Vortex ingest remain separately blocked. | Native I/O evidence is fixture-scoped to the local import/export smoke. | Vortex ingest route, transaction snapshot contract, query-pushdown blockers, and production connector evidence. | Local SQLite file import/export fixture smoke only; no arbitrary SQL, query pushdown, Vortex ingest, network database, warehouse, production connector, performance, or fallback claim. |
| Postgres / MySQL | Database service | `report-only` | No first-class runtime connector is supported. | Future rows must separate import/export from remote query pushdown and baseline/oracle usage. | No network or credential I/O is performed. | Credential policy, network policy, snapshot semantics, import/export evidence. | External databases are not fallback engines. |
| JDBC / ODBC | Connector bridge | `report-only` | No JDBC or ODBC runtime bridge is supported. | Future rows must classify bridge maturity, driver dependency, credentials, and query pushdown boundaries. | No bridge certificate may be claimed. | Dependency/license policy, driver loading, credentials, diagnostics, imported schema evidence. | Connector availability is not claimed. |
| S3 / GCS / ADLS | Object store | `smoke-supported` | The public no-credential fixture profile parses provider URIs and reads explicit local fixture bytes only. Live provider runtime remains blocked. | GAR-COMPAT-1C owns the runtime admission ladder and separates fixture admission from live provider gates. | Native I/O evidence is fixture-scoped; credential resolution, network probes, provider probes, cache writes, cloud writes, and commits remain disabled. | Credential policy, authenticated reads, live byte-range/full-file provider reads, cache, write staging, commit protocol. | Public fixture object-store read smoke only; no live provider, table/lakehouse, production, performance, or Spark-replacement claim. |
| Iceberg / Delta / Hudi | Table/lakehouse format | `report-only` | Table/lakehouse commit runtime is not supported. | GAR-COMPAT-1D owns table scan, metadata, snapshot, delete/tombstone, append, merge/update/delete, commit, and rollback classification. | Local table metadata smoke does not imply table-format runtime or commit semantics. | Table metadata readers, snapshot semantics, delete/tombstone handling, object-store/catalog integration, commit/rollback evidence. | No production lakehouse claim. |
| Vortex | Native file/layout | `runtime-supported` | Scoped local Vortex runtime paths exist for evidence-backed workloads. | Coverage rows must state execution mode, provider, materialization/decode fields, and claim gate. | Vortex is the highest-fidelity source and sink target when the path is evidence-backed. | Broader Source/Split/Sink, object-store Vortex I/O, encoded-native operators, and output fidelity evidence. | Scoped local Vortex evidence only; no universal runtime or performance claim. |
| Generated / source-free outputs | Generated source | `smoke-supported` | `shardloom.generated_source_certificate_contract.v1` exposes no-dataset, user-generated, engine-native generated-source, and platform-adjacent proof rows. Scoped local smokes exist for `ctx.from_rows([...]).write(...)`, `ctx.literal_table([...]).write(...)`, `ctx.calendar(...).write(...)`, `ctx.range(...).write(...)`, `ctx.sequence(...).write(...)`, `ctx.sql_values(...).write(...)`, `ctx.sql_literal_select(...).write(...)`, `ctx.sql("SELECT * FROM generate_series/range(...)").write(...)`, scoped `ctx.sql("SELECT value AS id, value + 1 AS next FROM range(...)").write(...)` range projections, scoped generated `with_column(...)`, local-emulator `ctx.generated_output_to_object_store(...)`, and local Foundry-style `ctx.foundry_generated_output(...)` helpers. | GAR-GEN-1A/1B own the GeneratedSourceCertificate contract; GAR-GEN-1C/1D own local generated-output families; GAR-GEN-1E owns source-free API admission rows; Foundry/object-store proof rows stay scoped to existing local fixture/dev-stack evidence; GAR-COMPAT-1B projects those rows into this compatibility map. | No source Native I/O certificate is claimed when no source dataset is read; output, replay, and fidelity evidence remain required. | Synthetic generators, broad SQL/DataFrame runtime, live object-store providers, real Foundry APIs, lakehouse/table output, broader output formats. | Scoped local generated-output fixture-smoke and local platform-proof runtime only; no live provider, real Foundry, production, package, or performance claim. |
| Python rows / DataFrame | User API | `smoke-supported` | Python `ctx.from_rows([...]).write(...)`, `ctx.from_rows([...]).with_column(literal).write(...)`, `ctx.literal_table([...]).write(...)`, `ctx.calendar(...).write(...)`, `ctx.range(...).write(...)`, `ctx.range(...).with_column(int64_expression).write(...)`, `ctx.sequence(...).write(...)`, `ctx.sql_values(...).write(...)`, `ctx.sql_literal_select(...).write(...)`, `ctx.sql("SELECT * FROM generate_series/range(...)").write(...)`, scoped `ctx.sql("SELECT value AS id, value + 1 AS next FROM range(...)").write(...)`, scoped `ctx.dataframe_source_free_projection("lit(...).alias('name')").write(...)`, and scoped `ctx.dataframe_generated_with_column("name", "lit(...)").write(...)` can run local JSONL/CSV generated-output smokes; broad DataFrame runtime is not supported. | `shardloom.generated_source_api_admission.v1` classifies `python_ctx_from_rows`, `python_ctx_literal_table`, `python_ctx_calendar`, `python_ctx_range`, `python_ctx_sequence`, `python_generated_source_write`, SQL rows, scoped DataFrame literal projection, scoped generated `with_column`, and evidence. | No hidden pandas/Polars/DuckDB/DataFusion execution may occur. | Typed API admission contract, generated-source evidence, local sink evidence, no-fallback tests. | User-layer generated-output posture only; no broad DataFrame runtime claim. |
| SQL `VALUES` / literals / range generators | SQL frontend | `smoke-supported` | Source-free SQL literal `SELECT`, SQL `VALUES`, `SELECT * FROM generate_series/range(...)`, and scoped `value` column/int64 arithmetic projections from `generate_series/range(...)` can run scoped local JSONL/CSV generated-output smokes with admitted literal or int64 generator arguments. Scoped local-source SQL runtime families are tracked in `shardloom.sql_frontend_runtime_ladder.v1`; broad catalogs, CTEs, set operations, recursive SQL, correlated/broad subqueries, object-store/table SQL, fallback-engine SQL, arbitrary functions, and UDFs remain report-only or blocked. | GAR-GEN-1E owns the source-free SQL admission rows; GAR-RUNTIME-IMPL-5B owns the scoped SQL frontend runtime ladder and deterministic broad-SQL blockers. Future slices must add evidence for specific broader SQL/runtime families before they can move out of blocked/report-only posture. | SQL parser, binder, planner, generated-source certificate, output Native I/O certificate, and execution certificate are claimed only for the scoped local literal/VALUES/range-generator smokes and the explicitly admitted ladder rows. | Broad catalog/CTE/set-op/subquery/table SQL evidence, broader expression semantics, generated-source certificate expansion, output evidence. | Only source-free SQL `VALUES`/literal/range-generator, scoped range-projection, and explicitly admitted SQL runtime-ladder fixture smokes are admitted; no broad SQL runtime claim. |
| REST / Flight / ADBC | Remote and data-plane APIs | `report-only` | REST/event contracts exist as report-only surfaces; Flight/ADBC runtime bridges are not supported. | Future rows must separate control-plane discovery from data-plane runtime. | No server, remote execution, or data-plane bridge may be claimed. | Transport contract, auth policy, lifecycle, result delivery, and no-fallback parity. | No production API claim. |
| Foundry | Platform integration | `report-only` | Foundry proof remains local/proof-only; real platform runtime is not certified. | Future rows must separate no-dataset smoke, staged local paths, Foundry output APIs, and platform evidence. | Direct S3/object-store writes do not count as Foundry generated-output proof. | Real Foundry environment proof, output API evidence, governance/lineage policy, no external compute proof. | Future validation target only; no endorsement or production claim. |

## Required Scoreboard Fields For Future Machine Surfaces

Future CLI, Python, and website/status projections should preserve these fields rather than scraping
paragraph text:

```text
surface_id
surface_family
direction=read|write|read_write|generated|api
support_status=runtime-supported|smoke-supported|report-only|blocked|not-planned
runtime_supported
smoke_supported
report_only
credential_required
network_required
source_io_performed
output_io_performed
native_io_certificate_status
generated_source_certificate_status
fallback_attempted=false
external_engine_invoked=false
claim_gate_status
blocker_id
required_future_evidence
claim_boundary
```

Route projections must also preserve these fields:

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
vortex_ingest_status
vortex_ingest_blocker_id
compatibility_import_certified_status
compatibility_import_certified_blocker_id
prepared_vortex_requires_prepared_state=true
prepared_vortex_direct_source_input_allowed=false
prepared_vortex_input_contract
prepared_vortex_timing_scope
compatibility_import_certified_timing_scope
```

For every non-Vortex source family, `vortex_ingest_status` and
`compatibility_import_certified_status` must be present. They usually share the same
`UniversalIngress` adapter status; the certified route may still carry stricter evidence blockers
when source/output/replay/certificate proof is incomplete.

## Object-Store And Lakehouse Boundary

Object-store live-provider and table/lakehouse entries remain blocked or report-only until evidence
exists for the specific runtime action being claimed. The only admitted object-store runtime action
is the explicit public no-credential fixture profile, which parses S3/GCS/ADLS URIs and reads
caller-supplied local bytes without credentials, network, provider probes, cache writes, cloud
writes, or commits.

Do not collapse these steps into one support flag:

```text
object-store URI parse
credential policy
public no-credential fixture read
signed or authenticated provider read
byte-range read
full-file read
local cache
write staging
commit protocol
table metadata read
snapshot or time-travel read
delete/tombstone handling
append
merge/update/delete
rollback
```

Read support and write/commit support must stay separate. Public no-credential fixture reads, live
public provider reads, and authenticated reads must stay separate. Metadata smoke must not imply
table scan or table commit runtime.

## Generated-Output Boundary

Source-free generated output is separate from no-dataset smoke:

```text
no_dataset_smoke
  -> status/capability/proof only
  -> no generated rows
  -> no output data claim

user_generated_source
  -> user Python code creates rows
  -> ShardLoom consumes rows as a generated/literal source
  -> scoped local JSONL/CSV fixture smoke is supported for ctx.from_rows(...).write(...),
     ctx.literal_table(...).write(...), ctx.calendar(...).write(...),
     ctx.sql_values(...).write(...), ctx.sql_literal_select(...).write(...), and
     ctx.sql("SELECT * FROM generate_series/range(...)").write(...)
     ctx.sql("SELECT value AS id, value + 1 AS next FROM range(...)").write(...)

engine_native_generated_source
  -> ShardLoom plan contains generator nodes such as range, sequence, values, literal_table,
     calendar/date dimension, or deterministic synthetic profile
  -> scoped local JSONL/CSV fixture smoke is supported for ctx.range(...).write(...)
     and ctx.sequence(...).write(...)
  -> engine-native values, synthetic, and broad DataFrame generation remain report-only
  -> local output evidence is still required for every supported slice
```

No source Native I/O certificate is claimed when no source dataset was read. A local generated
output claim requires `GeneratedSourceCertificate` evidence plus output Native I/O evidence.

### Compatibility-Level Generated-Output Rows

The JSON projection now includes
`source_free_generated_output_contract.schema_version=shardloom.universal_compatibility.generated_output_contract.v1`
so agents, Python callers, and the website can inspect source-free generated-output posture without
joining GAR-GEN docs by hand.

| Row | Status | Runtime execution | Output I/O | Boundary |
| --- | --- | --- | --- | --- |
| `no_dataset_smoke` | `smoke-supported` | `false` | `false` | Status/capability proof only; not generated-output execution. |
| `python_ctx_from_rows` | `smoke-supported` | `true` | `true` | Scoped local user-row JSONL/CSV generated-output fixture smoke only. |
| `python_ctx_range` | `smoke-supported` | `true` | `true` | Scoped local range JSONL/CSV generated-output fixture smoke only. |
| `python_ctx_sequence` | `smoke-supported` | `true` | `true` | Scoped local sequence JSONL/CSV generated-output fixture smoke only. |
| `python_ctx_literal_table` | `smoke-supported` | `true` | `true` | Supported only for scoped local literal-table JSONL/CSV smokes. |
| `python_ctx_calendar` | `smoke-supported` | `true` | `true` | Supported only for scoped local calendar/date-dimension JSONL/CSV smokes. |
| `python_generated_source_write` | `smoke-supported` | `true` | `true` | Supported only for local user-row, literal-table, calendar, range, sequence, SQL `VALUES`, SQL literal `SELECT`, SQL `generate_series`/`range`, scoped range projection, and generated `with_column` JSONL/CSV smokes. |
| `local_output_only_generated_source_posture` | `report-only` | `false` | `false` | Generated output remains local-output-only; object-store, lakehouse, and Foundry sinks stay blocked/report-only. |
| `sql_literal_select` | `smoke-supported` | `true` | `true` | Scoped source-free literal `SELECT` local JSONL/CSV fixture smoke only. |
| `sql_values` | `smoke-supported` | `true` | `true` | Scoped source-free SQL `VALUES` local JSONL/CSV fixture smoke only. |
| `sql_source_free_projection` | `smoke-supported` | `true` | `true` | Scoped range-generator `value` column/int64 projection local JSONL/CSV fixture smoke only; arbitrary source-free SQL projection remains blocked. |
| `sql_generate_series_range` | `smoke-supported` | `true` | `true` | Scoped `SELECT *` plus `value` column/int64 arithmetic projections from `generate_series/range(...)` local JSONL/CSV fixture smoke only. |
| `dataframe_source_free_projection` | `smoke-supported` | `true` | `true` | Scoped DataFrame literal projection rows to local JSONL/CSV generated-output fixture smoke only; broad expression-backed DataFrame generation remains blocked. |
| `dataframe_generated_with_column` | `smoke-supported` | `true` | `true` | Scoped generated-row literal columns and generated range int64 expression columns before local output only; broad expression-backed generated columns remain blocked. |
| `object_store_local_emulator_generated_output` | `smoke-supported` | `true` | `true` | Scoped local-emulator flat and partitioned object-store generated-output proof only; partitioned proof uses local key=value discovery, while live S3/GCS/ADLS, credentials, provider probes, table/lakehouse commits, and production claims remain blocked. |
| `object_store_live_provider_generated_output` | `blocked` | `false` | `false` | Live object-store generated-output writes require credential, network-effect, commit, replay, fidelity, and no-fallback evidence before admission. |
| `foundry_style_generated_output` | `smoke-supported` | `true` | `true` | Local Foundry-style result/evidence dataset proof only; real Foundry runtime/output APIs and Spark remain blocked. |
| `foundry_live_platform_generated_output` | `blocked` | `false` | `false` | Real Foundry generated-output runtime remains blocked until explicit platform integration writes result/evidence datasets without Spark or external-engine fallback. |

Every compatibility-level generated-output row preserves:

```text
source_io_performed=false
fallback_attempted=false
external_engine_invoked=false
```

The contract also publishes fail-closed summary fields:

```text
universal_compatibility_generated_output_no_dataset_smoke_separate=true
universal_compatibility_generated_output_local_output_only=true
universal_compatibility_generated_output_output_certificate_required=true
universal_compatibility_generated_output_object_store_runtime_supported=false
universal_compatibility_generated_output_object_store_local_emulator_runtime_supported=true
universal_compatibility_generated_output_foundry_runtime_supported=false
universal_compatibility_generated_output_foundry_style_runtime_supported=true
universal_compatibility_generated_output_live_platform_api_supported=false
universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed=false
```

### S3/GCS/ADLS Object-Store Admission Ladder

The JSON projection also includes
`object_store_admission_ladder.schema_version=shardloom.universal_compatibility.object_store_admission_ladder.v1`
for the GAR-COMPAT-1C object-store runtime admission ladder. The ladder is provider-neutral across
S3, GCS, and ADLS, and it keeps URI recognition, credential policy, public no-credential fixture
read, authenticated read, live byte-range read, live full-file read, local cache, write staging, and
commit protocol as separate claim gates.

| Row | Status | Credential policy | Read/write allowed | Boundary |
| --- | --- | --- | --- | --- |
| `object_store_uri_parse` | `report-only` | `not_required_for_parse` | no read/write | URI recognition only; no provider, credential, network, read, write, or commit effect. |
| `credential_policy` | `blocked` | `required_not_admitted` | no read/write | Credential resolution remains blocked; no secrets are read. |
| `public_no_credential_read` | `smoke-supported` | `public_no_credential_fixture_admitted` | fixture read only | Parses S3/GCS/ADLS URIs and reads caller-supplied local fixture bytes; live provider reads remain blocked. |
| `authenticated_read` | `blocked` | `authenticated_read_policy_required` | no read/write | Credentialed reads are a separate blocked gate from public reads. |
| `byte_range_read` | `blocked` | `read_policy_required` | no read/write | Byte-range reads remain blocked despite planning evidence. |
| `full_file_read` | `blocked` | `read_policy_required` | no read/write | Full-file reads remain blocked and distinct from byte-range reads. |
| `local_cache` | `blocked` | `cache_source_policy_required` | no read/write | Cache planning is not a runtime cache or support claim. |
| `write_staging` | `blocked` | `write_policy_required` | no read/write | Write staging remains blocked and separate from read support. |
| `commit_protocol` | `blocked` | `commit_policy_required` | no read/write | Object-store commit remains blocked and does not imply table/lakehouse commit support. |

Every object-store ladder row preserves:

```text
credential_resolution_performed=false
network_probe_allowed=false
provider_probe_allowed=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
```

The fixture-admitted row also reports:

```text
object_store_io=true
native_io_certificate_status=public_fixture_smoke_only
claim_gate_status=public_fixture_smoke_only
```

The ladder exposes admission status plus fixture-scoped read evidence. It does not authorize
credential resolution, provider probing, live S3/GCS/ADLS reads, local cache runtime, writes,
commit protocol execution, table/lakehouse runtime, production use, performance claims, or external
fallback execution.

### Iceberg/Delta/Hudi Table-Format Boundary Matrix

The JSON projection includes
`table_format_boundary_matrix.schema_version=shardloom.universal_compatibility.table_format_boundary_matrix.v1`
for the GAR-COMPAT-1D table-format boundary matrix. The matrix keeps Iceberg, Delta, and Hudi
behaviors separate from local manifest metadata smoke and from generic output-file support.

| Row | Status | Related local smoke | I/O allowed | Boundary |
| --- | --- | --- | --- | --- |
| `table_metadata_read` | `report-only` | yes | no table/catalog/object-store I/O | Local manifest metadata smoke is related evidence only; format metadata runtime is not supported. |
| `table_scan` | `blocked` | no | no data read | Table scan/data read remains blocked. |
| `snapshot_time_travel` | `blocked` | no | no metadata/data read | Snapshot and time-travel semantics are not runtime-supported. |
| `partition_evolution` | `report-only` | yes | no runtime I/O | Partition-evolution compatibility remains planning/report evidence only. |
| `delete_tombstone` | `report-only` | yes | no delete runtime | Local delete/tombstone fixture smoke is related evidence only. |
| `append` | `blocked` | no | no write/commit | Table append remains blocked and does not follow from output-file writer support. |
| `merge_update_delete` | `blocked` | no | no write/commit | Merge/update/delete table operations remain blocked. |
| `commit` | `blocked` | no | no commit | Table commit remains blocked and separate from metadata planning and object-store commit posture. |
| `rollback` | `blocked` | no | no rollback | Rollback and recovery semantics remain blocked. |
| `catalog_interaction` | `blocked` | no | no catalog I/O | External catalog interaction remains blocked. |
| `object_store_coupling` | `blocked` | no | no object-store I/O | Object-store-backed table runtime remains blocked until object-store gates are independently admitted. |

Every table-format matrix row preserves:

```text
catalog_io_allowed=false
object_store_io_allowed=false
table_metadata_read_allowed=false
table_data_read_allowed=false
write_io_allowed=false
commit_allowed=false
rollback_allowed=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

The matrix is a boundary/status surface only. It does not authorize external table-format
dependencies, Iceberg/Delta/Hudi metadata runtime, table scans, snapshot/time-travel runtime,
delete/tombstone runtime, appends, merge/update/delete, commits, rollbacks, catalog interaction,
object-store-backed table runtime, production lakehouse claims, performance claims, or fallback
execution.

### Database/Warehouse Import-Export Boundary Matrix

The JSON projection includes
`database_warehouse_boundary_matrix.schema_version=shardloom.universal_compatibility.database_warehouse_boundary_matrix.v1`
for the GAR-COMPAT-1E database and warehouse import/export boundary. The matrix now admits only the
local SQLite file fixture import/export smoke. It keeps network databases, driver bridges, and cloud
warehouses visible without treating them as ShardLoom runtime connectors or fallback engines.

| Row | Status | Connector type | Credentials/network | Runtime posture | Boundary |
| --- | --- | --- | --- | --- | --- |
| `sqlite_file` | `smoke-supported` | embedded file database | no credentials/network | local named-table import/export smoke only; no query pushdown | SQLite import/export is smoke-supported only through `sqlite-local-import-export-smoke`: a named local table scan to workspace-safe JSONL plus roundtrip SQLite replay. |
| `postgres` | `blocked` | network database | credentials and network required | no import/export/query pushdown | Postgres is not a fallback engine or query-pushdown runtime. |
| `mysql` | `blocked` | network database | credentials and network required | no import/export/query pushdown | MySQL is not a fallback engine or query-pushdown runtime. |
| `jdbc_odbc` | `blocked` | driver bridge | credentials, network, and driver policy required | no bridge runtime | JDBC/ODBC drivers are not loaded and bridge availability is not claimed. |
| `snowflake` | `blocked` | cloud warehouse | credentials and network required | external baseline/future endpoint only | Snowflake has no warehouse runtime or pushdown claim. |
| `bigquery` | `blocked` | cloud warehouse | credentials and network required | external baseline/future endpoint only | BigQuery has no warehouse runtime or pushdown claim. |
| `databricks_sql` | `blocked` | cloud warehouse | credentials and network required | external baseline/future endpoint only | Databricks SQL has no warehouse runtime, Spark fallback, or pushdown claim. |

Every database/warehouse matrix row preserves:

```text
credential_resolution_performed=false
network_probe_performed=false
driver_loaded=false
query_pushdown_supported=false
fallback_attempted=false
external_engine_invoked=false
```

SQLite import/export is the only fixture-smoke exception and keeps
`claim_gate_status=fixture_smoke_only`. Matrix summary fields therefore report:

```text
import_runtime_supported=true
export_runtime_supported=true
query_pushdown_supported=false
```

The matrix does not authorize Postgres, MySQL, JDBC/ODBC, Snowflake, BigQuery, Databricks SQL,
credential resolution, network probes, driver loading, network connector import/export, query
pushdown, production connector claims, performance claims, Spark replacement claims, or external
fallback execution.

## Acceptance

- Runtime coverage and plan/report coverage are distinct.
- Unsupported sources, sinks, adapters, and user APIs are never advertised as supported.
- External databases and engines are never fallback engines.
- S3/GCS/ADLS remain blocked until specific read/write proof exists.
- Table-format metadata, scan, delete/tombstone, write, commit, and rollback semantics remain
  separately classified.
- Database and warehouse import/export, query pushdown, credential resolution, network probes, and
  driver loading remain separately classified.
- Foundry remains a future validation target unless real platform evidence exists.

## Verification

Use the phase-plan item `GAR-COMPAT-1` for actionable work. The current report-only doc can be
validated with:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```

# Universal Compatibility Coverage Scoreboard

## Purpose

This document is the report-only compatibility map for ShardLoom source, sink, adapter, and
user-facing data creation surfaces.

The machine-readable projection lives beside this document:

```text
docs/architecture/universal-compatibility-coverage-scoreboard.json
schema_version=shardloom.universal_compatibility_coverage_scoreboard.v1
```

Website/status, CLI capability JSON, and Python typed capability views must use typed fields from
the machine-readable projection or capability envelopes rather than scraping the Markdown prose.

It answers one question:

```text
Which compatibility surfaces are runtime-supported today, which are only smoke/report surfaces,
which are blocked, and what evidence would be required before any support claim is allowed?
```

This scoreboard is not a production support claim, a performance claim, a Spark replacement claim,
or an object-store/lakehouse/Foundry runtime claim.

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
| CSV | Local compatibility file | `smoke-supported` | Local compatibility import can stage CSV into Vortex-backed evidence paths for scoped workloads. | Coverage rows distinguish compatibility import from prepared/native Vortex execution. | Source Native I/O evidence is scoped to the certified compatibility path; output evidence is separate. | Broader schema, malformed-input, write, and runtime operator coverage. | Compatibility import evidence only; not pure query speed or universal CSV runtime. |
| JSONL / NDJSON / JSON | Local compatibility file | `smoke-supported` | JSONL/NDJSON compatibility import is scoped to local benchmark/workflow evidence. General JSON document/database behavior is not runtime-supported. | Capability rows must distinguish JSONL line records from nested/general JSON semantics. | Source/sink evidence is scoped; output evidence is separate. | Broader nested JSON execution, schema drift, writer, and diagnostics coverage. | Scoped local compatibility evidence only. |
| Parquet | Local compatibility file | `smoke-supported` | Local compatibility import can prepare Parquet inputs for scoped evidence paths. | Rows must separate Parquet preparation from Vortex-native execution. | Parquet is compatibility input/output, not ShardLoom's highest-fidelity native execution format. | Broader pushdown, writer, metadata, and table-format boundary evidence. | No lakehouse/table runtime claim. |
| Arrow IPC | Local compatibility file | `smoke-supported` | Local compatibility import can prepare Arrow IPC inputs for scoped evidence paths. | Arrow boundaries are compatibility/interop surfaces, not the internal execution substrate claim. | Native I/O evidence must say what decoded/materialized at the boundary. | Broader zero-copy, streaming, and output fidelity evidence. | No blanket Arrow-native runtime claim. |
| Avro | Local compatibility file | `smoke-supported` | Feature-gated local compatibility import coverage exists for scoped benchmark evidence. | Avro remains a compatibility preparation surface. | Native I/O evidence is scoped to the path and cannot imply native Avro execution. | Broader schema evolution, logical type, and writer evidence. | Scoped compatibility import evidence only. |
| ORC | Local compatibility file | `smoke-supported` | Feature-gated local compatibility import coverage exists for scoped benchmark evidence. | ORC remains a compatibility preparation surface. | Native I/O evidence is scoped to the path and cannot imply native ORC execution. | Broader predicate/stripe/statistics and writer evidence. | Scoped compatibility import evidence only. |
| Excel | Local desktop/document file | `blocked` | No first-class Excel runtime source or sink is supported. | Future report rows may classify workbook, sheet, range, type inference, and formula policy. | No Native I/O certificate may be claimed. | Parser/dependency approval, license review, deterministic schema policy, formula/effect policy. | Not supported. |
| SQLite | Database file | `report-only` | No first-class SQLite import/export runtime path is supported. | Future rows must separate file import/export from query pushdown. | No database Native I/O certificate may be claimed. | Connector policy, SQL dialect boundary, transaction snapshot evidence, no-fallback diagnostics. | Database endpoint support is not claimed. |
| Postgres / MySQL | Database service | `report-only` | No first-class runtime connector is supported. | Future rows must separate import/export from remote query pushdown and baseline/oracle usage. | No network or credential I/O is performed. | Credential policy, network policy, snapshot semantics, import/export evidence. | External databases are not fallback engines. |
| JDBC / ODBC | Connector bridge | `report-only` | No JDBC or ODBC runtime bridge is supported. | Future rows must classify bridge maturity, driver dependency, credentials, and query pushdown boundaries. | No bridge certificate may be claimed. | Dependency/license policy, driver loading, credentials, diagnostics, imported schema evidence. | Connector availability is not claimed. |
| S3 / GCS / ADLS | Object store | `blocked` | Object-store range planning/report surfaces exist, but runtime object-store I/O is blocked. | GAR-COMPAT-1C owns the runtime admission ladder. | No credential resolution, network probe, byte-range read, full-file read, write, or commit may run in this posture. | URI parse, credential policy, public read, authenticated read, byte-range read, full-file read, cache, write staging, commit protocol. | No object-store runtime claim. |
| Iceberg / Delta / Hudi | Table/lakehouse format | `report-only` | Table/lakehouse commit runtime is not supported. | GAR-COMPAT-1D owns table scan, metadata, snapshot, delete/tombstone, append, merge/update/delete, commit, and rollback classification. | Local table metadata smoke does not imply table-format runtime or commit semantics. | Table metadata readers, snapshot semantics, delete/tombstone handling, object-store/catalog integration, commit/rollback evidence. | No production lakehouse claim. |
| Vortex | Native file/layout | `runtime-supported` | Scoped local Vortex runtime paths exist for evidence-backed workloads. | Coverage rows must state execution mode, provider, materialization/decode fields, and claim gate. | Vortex is the highest-fidelity source and sink target when the path is evidence-backed. | Broader Source/Split/Sink, object-store Vortex I/O, encoded-native operators, and output fidelity evidence. | Scoped local Vortex evidence only; no universal runtime or performance claim. |
| Generated / source-free outputs | Generated source | `smoke-supported` | `shardloom.generated_source_certificate_contract.v1` exposes no-dataset, user-generated, and engine-native generated-source contract rows. Scoped local JSONL smokes exist for `ctx.from_rows([...]).write(...)` and `ctx.range(...).write(...)`. | GAR-GEN-1A/1B own the report-only GeneratedSourceCertificate contract; GAR-GEN-1C owns local user-row output; GAR-GEN-1D owns local range output; GAR-GEN-1E owns source-free API admission rows; GAR-COMPAT-1B projects those rows into this compatibility map. | No source Native I/O certificate is claimed when no source dataset is read; output evidence remains required. | Sequence/values/literal-table/calendar/synthetic generators, SQL/DataFrame runtime, object-store/Foundry output proof, broader output formats. | Local user-row and range JSONL fixture-smoke generated-output runtime only. |
| Python rows / DataFrame | User API | `smoke-supported` | Python `ctx.from_rows([...]).write(...)` and `ctx.range(...).write(...)` can run scoped local JSONL generated-output smokes; broad DataFrame runtime is not supported. | `shardloom.generated_source_api_admission.v1` classifies `python_ctx_from_rows`, `python_ctx_range`, `python_generated_source_write`, `ctx.literal_table`, `ctx.calendar`, SQL rows, DataFrame source-free projection, blockers, and evidence. | No hidden pandas/Polars/DuckDB/DataFusion execution may occur. | Typed API admission contract, generated-source evidence, local sink evidence, no-fallback tests. | User-layer posture only; no broad DataFrame runtime claim. |
| SQL `VALUES` / literals | SQL frontend | `report-only` | SQL source-free execution is not supported; SQL literal `SELECT`, SQL `VALUES`, source-free projection, and `generate_series`/`range` vocabulary are admission rows with deterministic blockers. | GAR-GEN-1E owns the report-only SQL admission rows; future slices must classify parse, bind, plan, source-free projection, and unsupported diagnostics before runtime. | No SQL parser, binder, planner, runtime, generated-source certificate, or SQL execution certificate may be claimed. | Parser/binder policy, literal expression semantics, generated-source certificate, output evidence. | No broad SQL runtime claim. |
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

## Object-Store And Lakehouse Boundary

Object-store and table/lakehouse entries remain blocked or report-only until evidence exists for the
specific runtime action being claimed.

Do not collapse these steps into one support flag:

```text
object-store URI parse
credential policy
signed or public no-credential read
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

Read support and write/commit support must stay separate. Public no-credential reads and
authenticated reads must stay separate. Metadata smoke must not imply table scan or table commit
runtime.

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
  -> scoped local JSONL fixture smoke is supported for ctx.from_rows(...).write(...)

engine_native_generated_source
  -> ShardLoom plan contains generator nodes such as range, sequence, values, literal_table,
     calendar/date dimension, or deterministic synthetic profile
  -> scoped local JSONL fixture smoke is supported for ctx.range(...).write(...)
  -> sequence, values, literal_table, calendar, synthetic, SQL, and broad DataFrame generation remain report-only
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
| `python_ctx_from_rows` | `smoke-supported` | `true` | `true` | Scoped local user-row JSONL generated-output fixture smoke only. |
| `python_ctx_range` | `smoke-supported` | `true` | `true` | Scoped local range JSONL generated-output fixture smoke only. |
| `python_ctx_literal_table` | `report-only` | `false` | `false` | Literal-table generation is vocabulary only until runtime evidence exists. |
| `python_ctx_calendar` | `report-only` | `false` | `false` | Calendar/date dimension generation is vocabulary only until runtime evidence exists. |
| `python_generated_source_write` | `smoke-supported` | `true` | `true` | Supported only for local user-row and range JSONL smokes. |
| `local_output_only_generated_source_posture` | `report-only` | `false` | `false` | Generated output remains local-output-only; object-store, lakehouse, and Foundry sinks stay blocked/report-only. |
| `sql_literal_select` | `report-only` | `false` | `false` | SQL literal `SELECT` has no parser, binder, planner, runtime, row generation, or output write. |
| `sql_values` | `report-only` | `false` | `false` | SQL `VALUES` has no parser, binder, planner, runtime, row generation, or output write. |
| `sql_source_free_projection` | `report-only` | `false` | `false` | Source-free SQL projection is report-only. |
| `sql_generate_series_range` | `report-only` | `false` | `false` | SQL `generate_series`/`range` is vocabulary only; use Python `ctx.range` for the scoped smoke. |
| `dataframe_source_free_projection` | `report-only` | `false` | `false` | DataFrame source-free projection remains report-only outside scoped local generated-output smokes. |
| `dataframe_generated_with_column` | `report-only` | `false` | `false` | Expression-backed generated columns are not runtime-supported. |

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
universal_compatibility_generated_output_foundry_runtime_supported=false
universal_compatibility_generated_output_broad_sql_dataframe_claim_allowed=false
```

## Acceptance

- Runtime coverage and plan/report coverage are distinct.
- Unsupported sources, sinks, adapters, and user APIs are never advertised as supported.
- External databases and engines are never fallback engines.
- S3/GCS/ADLS remain blocked until specific read/write proof exists.
- Table-format metadata, scan, delete/tombstone, write, commit, and rollback semantics remain
  separately classified.
- Foundry remains a future validation target unless real platform evidence exists.

## Verification

Use the phase-plan item `GAR-COMPAT-1` for actionable work. The current report-only doc can be
validated with:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
python scripts/check_website_readiness.py
git diff --check
```

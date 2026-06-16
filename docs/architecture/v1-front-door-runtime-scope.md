<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Front-Door Runtime Scope

Status: canonical v1 front-door runtime scope.

Schema marker: `shardloom.v1_front_door_runtime_scope.v1`.

This document defines the exact local user-facing SQL, Python, and DataFrame-style surface that can
be used for ShardLoom v1 examples. It is intentionally narrower than broad SQL/DataFrame parity. A
workflow is in v1 only when it lowers to a ShardLoom-native route, exposes deterministic evidence,
and preserves:

```text
fallback_attempted=false
external_engine_invoked=false
```

## Source Of Truth

The machine-readable sources for this scope are:

- `docs/reference/shardloom-user-surface-index.json`
- `ShardLoomContext.front_door_parity_matrix()`
- `ShardLoomContext.user_route_capability_report()`
- `examples/local-python-benchmark-scenarios/run.py`
- `examples/local-python-benchmark-scenarios/timing_review.py`
- `scripts/check_v1_front_door_runtime_scope.py`

The human-readable all-surface index is `docs/reference/shardloom-user-surface-index.md`.
Public docs, website snippets, and README examples must link to this file when they describe the
v1-supported front door. They may summarize it, but they must not broaden it.

## Supported V1 Forms

The v1 front-door scope is local and bounded by default.

| Surface | Supported v1 forms | Runtime boundary |
| --- | --- | --- |
| Python context | `context()`, `context(repo_root=...)`, `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_parquet(...)`, `ctx.read_arrow_ipc(...)`, `ctx.read_avro(...)`, `ctx.read_orc(...)`, `ctx.read_vortex(...)`, source-free helpers such as `ctx.from_rows(...)`, `ctx.range(...)`, `ctx.sequence(...)`, and `ctx.calendar(...)`. `ctx.read(...)` infers `.csv`, `.json`, `.jsonl`, `.ndjson`, `.parquet`, `.arrow`, `.ipc`, `.feather`, `.avro`, `.orc`, and `.vortex` local adapters. | ShardLoom CLI JSON commands through `ShardLoomClient`; no native Python execution engine. Format-specific readers are input adapters only, not separate execution engines. Feature-gated structured readers return deterministic blockers when their adapter is not enabled. |
| Query builder | `filter`, `where`, `select`, `project`, `limit`, bounded `collect`, `group_by(...).agg(...)`, scoped `join(..., on=..., how="inner")`, `sort`/`order_by`, `nlargest`, `dropna`, `astype`, `with_column`, and local writes such as `write_jsonl`, `write_csv`, and feature-gated `write_vortex`. | Product local workflow route, prepared route, generated-source route, or local Vortex primitive route as reported by the capability matrices. Runtime support is selected from the normalized ShardLoom plan, not from independent CSV/JSON/DataFrame execution stacks. |
| SQL frontend | Scoped local-source SQL over local file references, source-free literal/VALUES output, and scoped local `.vortex` primitive SQL shapes. | SQL is a frontend into ShardLoom planning and execution, not DataFusion, DuckDB, Spark, pandas, Polars, or another engine. |
| DataFrame-style aliases | Familiar aliases such as `where`, `groupby`, `sort_values`, `head`, `take`, `query` without unsupported keyword arguments, bounded display/materialization helpers, and explicit unsupported reports for non-admitted methods. | Same ShardLoom route as the corresponding SQL/Python workflow or deterministic unsupported report. |
| Benchmark ETL snippets | `selective_filter`, `filter_projection_limit`, `group_by_aggregation`, `hash_join`, `global_top_n`, `clean_cast_filter_write`, `malformed_timestamp_cast`, `null_heavy_aggregate`, and `nested_json_field_scan` in `examples/local-python-benchmark-scenarios`. | Sequential local Python execution through the same product local workflow facade as normal user snippets. Timing claims come only from promoted benchmark artifacts. |

## Format-Neutral Route Model

The public front door has one logical workflow shape:

```text
input adapter -> ShardLoom logical plan -> admitted ShardLoom/Vortex runtime -> output sink
```

CSV, JSON/JSONL, Parquet, Arrow IPC, Avro, ORC, and Vortex inputs should be unique only in the
adapter step. Python, SQL, and DataFrame-style builders should lower to the same logical plan once
the adapter has produced an admitted source state. Output formats should be unique only in sink
translation and metadata-preservation evidence.

`sql-local-source-smoke` remains a capped smoke safeguard. Normal user-facing local workflows route
through product-local or native Vortex routes and must not inherit smoke-only synthetic row, byte,
output, or join-candidate caps. The product-local compatibility-source route is the cap-removal
boundary; native Vortex-middle convergence for all compatibility inputs remains tracked separately
under `PY-VORTEX-ROUTE-UNIFY-1`. Unsupported shapes must fail deterministically instead of routing
to an external engine.

For direct `.vortex` input, the currently admitted primitive path is already shared across
Python/DataFrame-style and SQL front doors: filter, projection, source-order limit, count, and
filter-project chains enter the public workflow facade with the real calling surface and attached
plan/SQL evidence before dispatching to the scoped local Vortex primitive commands. Post-shaped
Python/DataFrame-style operators for the benchmark-family shapes are also admitted when the CLI is
built with `vortex-traditional-analytics-benchmark`: grouped count/sum, null-heavy aggregate, hash
join with a declared right Vortex input, global top-N, clean/cast/filter, malformed timestamp cast,
substring contains, and native `write_vortex` result sinks lower through
`traditional-analytics-vortex-run` with provider scenario evidence. General Vortex SQL/DataFrame
planning and compatibility exports such as `write_jsonl()` from direct Vortex workflows remain
explicitly blocked until their own native route and decode/export contracts exist. The route report
and run envelope identify all of this with `native_vortex_operation_family`,
`native_vortex_provider_scenario`, `native_vortex_capability_status`, `typed_result_contract`,
`typed_sink_contract`, and `decode_materialization_boundary` fields so agents and users can tell
supported routes from planned operator families without probing data or invoking external engines.
`ShardLoomContext.native_vortex_provider_route_certificate_report()` is the side-effect-free
certificate inventory for those exact provider-backed Python and SQL shapes.

## Support Status Vocabulary

| Status | Meaning |
| --- | --- |
| `smoke_supported` / `smoke-supported` | Narrow fixture or smoke route; synthetic safeguards such as row, byte, or output caps may be intentional. |
| `scoped_runtime_supported` / `runtime-supported` | Runtime-backed scoped capability with explicit claim boundary; not automatically broad product workflow support. |
| `feature_gated` | Requires an explicit build/runtime gate such as `universal-format-io`, `vortex-write`, or `vortex-traditional-analytics-benchmark`. |
| `production_admitted_local_workflow` | Product local workflow route admitted for normal local Python/SQL/DataFrame-facing usage without smoke-only synthetic caps, while still bounded by local v1 scope and no-fallback evidence. |

## Unsupported V1 Forms

The following are outside the v1 front-door runtime claim unless a later phase-plan item closes them
with runtime and release evidence:

- Arbitrary ANSI SQL, CTE/recursive SQL, arbitrary subqueries, arbitrary functions, unsupported
  nested accessors/casts, broad SQL grammar coverage, and broad semantic parity.
- Full pandas, Polars, Spark, DataFusion, DuckDB, PySpark, or dataframe-library API parity.
- Hidden execution in pandas, Polars, DuckDB, Spark, DataFusion, Velox, or another engine.
- Unbounded materialization as a convenience path.
- Unsupported joins, subqueries, windows, UDFs, plugins, LLM/API calls, embeddings, vector search,
  external writes, or effectful operations outside their explicit supported fixture paths.
- Object-store, lakehouse/table, catalog, remote API, Foundry, live/hybrid, distributed, and
  production platform workflows unless the matching v1 candidate item is completed with evidence.

Unsupported forms must fail before data is read, materialized, written, or delegated. The report
must include deterministic blocker information and preserve:

```text
runtime_execution=false
data_read=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
```

## Technique Review

ShardLoom-specific runtime techniques apply at the route boundary, not by widening the public API:

- Dynamic admission checks the requested source, operations, output, and evidence level before
  execution.
- `metadata-first` checks are allowed for capabilities, explain, route, estimate, and unsupported
  diagnostics without reading user data.
- Capillary work units apply inside local-source preparation, scan, and write paths where the
  runtime route already supports bounded local work.
- PulseWeave-style controls apply only where route evidence shows prepared-state reuse or bounded
  runtime shaping; they are not a reason to claim unsupported broad parity.
- Timing-surface and evidence-tier separation remain mandatory. Example snippets are route-use
  examples; performance claims require promoted benchmark artifacts with explicit
  `timing_surface` and `claim_gate_status`.

## Example Contract

The README and benchmark website may show the benchmark ETL scenario snippets only if the local
scenario runner remains the executable source:

```powershell
python examples\local-python-benchmark-scenarios\run.py --repo-root .
python examples\local-python-benchmark-scenarios\timing_review.py --repo-root .
```

The runner is sequential and local. Its expected scenario ids are:

```text
selective_filter
filter_projection_limit
group_by_aggregation
hash_join
global_top_n
clean_cast_filter_write
malformed_timestamp_cast
null_heavy_aggregate
nested_json_field_scan
```

`malformed_timestamp_cast` is intentionally expected to fail closed when the current cast path
rejects the data. That expected failure is still successful v1 behavior when the evidence shows no
fallback or external engine invocation.

## Claim Boundary

After this scope is closed, ShardLoom may claim scoped local front-door support for the supported v1
forms above. It still may not claim:

- broad SQL/DataFrame parity;
- front-door performance equivalence;
- production readiness;
- package release/publication;
- Spark/DataFusion/DuckDB/Polars replacement; or
- object-store/table/remote/Foundry/live/distributed production support.

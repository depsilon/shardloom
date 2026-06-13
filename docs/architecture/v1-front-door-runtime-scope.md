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

- `ShardLoomContext.front_door_parity_matrix()`
- `ShardLoomContext.user_route_capability_report()`
- `examples/local-python-benchmark-scenarios/run.py`
- `examples/local-python-benchmark-scenarios/timing_review.py`
- `scripts/check_v1_front_door_runtime_scope.py`

Public docs, website snippets, and README examples must link to this file when they describe the
v1-supported front door. They may summarize it, but they must not broaden it.

## Supported V1 Forms

The v1 front-door scope is local and bounded by default.

| Surface | Supported v1 forms | Runtime boundary |
| --- | --- | --- |
| Python context | `context()`, `context(repo_root=...)`, `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_vortex(...)`, source-free helpers such as `ctx.from_rows(...)`, `ctx.range(...)`, `ctx.sequence(...)`, and `ctx.calendar(...)`. | ShardLoom CLI JSON commands through `ShardLoomClient`; no native Python execution engine. |
| Query builder | `filter`, `where`, `select`, `project`, `limit`, bounded `collect`, `group_by(...).agg(...)`, scoped `join(..., on=..., how="inner")`, `sort`/`order_by`, `nlargest`, `dropna`, `astype`, `with_column`, and local writes such as `write_jsonl`, `write_csv`, and feature-gated `write_vortex`. | Local-source route, prepared route, generated-source route, or local Vortex primitive route as reported by the capability matrices. |
| SQL frontend | Scoped local-source SQL over local file references, source-free literal/VALUES output, and scoped local `.vortex` primitive SQL shapes. | SQL is a frontend into ShardLoom planning and execution, not DataFusion, DuckDB, Spark, pandas, Polars, or another engine. |
| DataFrame-style aliases | Familiar aliases such as `where`, `groupby`, `sort_values`, `head`, `take`, `query` without unsupported keyword arguments, bounded display/materialization helpers, and explicit unsupported reports for non-admitted methods. | Same ShardLoom route as the corresponding SQL/Python workflow or deterministic unsupported report. |
| Benchmark ETL snippets | `selective_filter`, `filter_projection_limit`, `group_by_aggregation`, `hash_join`, `global_top_n`, `clean_cast_filter_write`, `malformed_timestamp_cast`, `null_heavy_aggregate`, and `nested_json_field_scan` in `examples/local-python-benchmark-scenarios`. | Sequential local Python execution over small fixtures; timing claims come only from promoted benchmark artifacts. |

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

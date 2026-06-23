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
- `ShardLoomContext.front_door_semantic_surface_matrix()`
- `ShardLoomContext.user_route_capability_report()`
- `examples/local-python-benchmark-scenarios/run.py`
- `examples/local-python-benchmark-scenarios/timing_review.py`
- `benchmarks/clickbench/queries.sql`
- `scripts/check_clickbench_olap_runtime_coverage.py`
- `scripts/check_v1_front_door_runtime_scope.py`

The human-readable all-surface index is `docs/reference/shardloom-user-surface-index.md`.
Public docs, website snippets, and README examples must link to this file when they describe the
v1-supported front door. They may summarize it, but they must not broaden it.

## Supported V1 Forms

The v1 front-door scope is local and bounded by default.

| Surface | Supported v1 forms | Runtime boundary |
| --- | --- | --- |
| Python context | `context()`, `context(repo_root=...)`, `ctx.read(...)`, `ctx.read_csv(...)`, `ctx.read_json(...)`, `ctx.read_parquet(...)`, `ctx.read_arrow_ipc(...)`, `ctx.read_avro(...)`, `ctx.read_orc(...)`, `ctx.read_vortex(...)`, source-free helpers such as `ctx.from_rows(...)`, `ctx.range(...)`, `ctx.sequence(...)`, and `ctx.calendar(...)`. `ctx.read(...)` infers `.csv`, `.json`, `.jsonl`, `.ndjson`, `.parquet`, `.arrow`, `.ipc`, `.feather`, `.avro`, `.orc`, `.vortex`, and `.vortex-manifest` local adapters. Native Vortex input can also bind a local directory of `.vortex` parts for partitioned local execution. | ShardLoom CLI JSON commands through `ShardLoomClient`; no native Python execution engine. Format-specific readers are input adapters only, not separate execution engines. Feature-gated structured readers return deterministic blockers when their adapter is not enabled. |
| Query builder | `filter`, `where`, `select`, `project`, `limit`, bounded `tail`, deterministic row-count `sample(n=..., seed or random_state=<int>, weights=<numeric column>, replace=False or True)` and fractional `sample(frac or fraction=..., seed or random_state=<int>, weights=<numeric column>, replace=False or True)`, scoped `rolling(...).sum/mean/count/min/max(...)` with valid-observation null handling, scalar `mask(...)` / `replace(...)` / string-replace and UTF-8 regex-replace expression rewrites, scoped numeric `eval(...)` / `transform(...)`, declarative `map(...)` / `applymap(...)` / `map_rows(...)`, scoped `duplicated(subset=..., keep="first" or "last" or False)`, scoped retained-row `drop_duplicates(subset=..., keep="first" or "last" or False)`, scoped heterogeneous-scalar `melt` with explicit source-order row numbers for `ignore_index=False`, single-column and same-length multi-column list/fixed-size-list `explode` with scalar, nullable, list, and struct element values plus single-level `explode("items.field")` list-of-struct projection, scoped `pivot`, scoped `pivot_table` with `fill_value`/`dropna`/`margins` output policy, plan-transform `apply` / `pipe`, bounded `collect`, admitted aggregates, scoped joins, sort/top-N, `dropna`, `astype`, `with_column`, metadata-first `profile()`, no-argument `distinct()` / `unique()`, source-free/generated helpers, feature-gated `write_vortex`, and scoped Vortex-derived JSONL/CSV writes and staged local fanout. Residual helpers such as transformed row profiling, opaque RNG objects, arbitrary Python callables/data UDFs, broad eval variants, multi-level nested-field accessor explode, broad pandas mask/replace/duplicated/drop-duplicate/reshape/window variants, `quarantine`, non-JSONL/CSV compatibility sinks, and bounded materialization helpers must select an admitted native/prepared route or return deterministic blockers. | Prepared route, generated-source route, local Vortex primitive/provider/profile route, native Vortex sink route, native Vortex primitive row-export route, or deterministic blocker as reported by the capability matrices. Direct decoded local-source runtime is not a public product route; the public path prepares through Vortex first and never reports product-local post-prepare compatibility execution as native runtime. Runtime support is selected from the normalized ShardLoom plan, not from independent CSV/JSON/DataFrame execution stacks. |
| SQL frontend | Scoped local-source SQL over local file references, source-free literal/VALUES output, `ctx.sql(..., input=...)` declared-input binding, and scoped local `.vortex` primitive SQL shapes, including count/filter/project/limit and integer equality/not-equality/range predicates. | SQL is a frontend into ShardLoom planning and execution, not DataFusion, DuckDB, Spark, pandas, Polars, or another engine. |
| DataFrame-style aliases | Familiar aliases such as `where`, `groupby`, `sort_values`, scoped `set_index(..., drop=False)`, `reset_index()` with visible source-order row-number materialization when a stable source/projection schema is known, `reset_index(drop=True)` metadata removal, bounded explicit-index `sort_index(...).limit(...)` through native Vortex `sort_rows`, source-order-preserving no-index `sort_index(ascending=True)`, `head`, `take`, `query` without unsupported keyword arguments, bounded display/materialization helpers, and explicit unsupported reports for non-admitted methods. | Same ShardLoom route as the corresponding SQL/Python workflow or deterministic unsupported report. |
| Benchmark ETL snippets | `selective_filter`, `filter_projection_limit`, `group_by_aggregation`, `hash_join`, `global_top_n`, `clean_cast_filter_write`, `malformed_timestamp_cast`, `null_heavy_aggregate`, and `nested_json_field_scan` in `examples/local-python-benchmark-scenarios`. | Sequential local Python execution must use an admitted Vortex-prepared/native route or fail closed with deterministic diagnostics. Timing claims come only from promoted benchmark artifacts. |

Resolved Python clients may keep a private local `python-worker` process open to amortize CLI
startup across repeated calls. That worker is transport-only: it dispatches the same `route`, `run`,
`prepare`, and Vortex commands, returns their normal typed JSON envelopes, and does not change the
execution route, fallback boundary, or native Vortex evidence.

## Format-Neutral Route Model

The public front door has one logical workflow shape:

```text
input adapter -> ShardLoom logical plan -> admitted ShardLoom/Vortex runtime -> output sink
```

CSV, JSON/JSONL, Parquet, Arrow IPC, Avro, ORC, and Vortex inputs should be unique only in the
adapter step. Python, SQL, and DataFrame-style builders should lower to the same logical plan once
the adapter has produced an admitted source state. Output formats should be unique only in sink
translation and metadata-preservation evidence.

For local compatibility sources, universal ingest owns schema hints, format inference, and payload
normalization before Vortex preparation. `.jsonl` and `.ndjson` stay distinct input formats; all-null
text-source columns without explicit dtype default to nullable UTF-8; mixed integer/float text
columns promote to float64; and selected nested JSON object/array cells may be preserved as UTF-8
JSON payload strings. These are ingress contracts, not separate JSON/CSV compute routes or broad
nested JSON semantics.

`local-source-runtime` is the ShardLoom-owned local compatibility runtime used by diagnostics and
preparation internals. Normal user-facing local workflows must route through product-profile Vortex
preparation/prepared execution or native Vortex input, and must not execute a decoded direct
compatibility middle as the public route. Product-profile preparation disables the diagnostic row,
byte, output, and join-candidate caps and emits `public_workflow_preparation_*` evidence proving the
selected cap posture. If the required Vortex preparation/runtime feature is unavailable, `auto`
fails closed with deterministic diagnostics, and `direct` is blocked for public local-file
workflows. Unsupported shapes must fail deterministically instead of routing to an external engine.

For direct `.vortex` input, `.vortex-manifest` files, and local directories containing `.vortex`
parts, the currently admitted primitive path is already shared across
Python/DataFrame-style and SQL front doors: filter, projection, source-order limit, count,
filter-project, integer `=`, `<>`/`!=`, `<`, `<=`, `>`, and `>=` predicate lowering,
scoped row-level distinct, bounded source-order tail, deterministic bounded
sample chains, and scoped source-order rolling sum/mean/count/min/max routes enter the public
workflow facade with the real
calling surface and attached plan/SQL evidence before dispatching to the scoped local Vortex
primitive commands. Partitioned input binding reports `native_vortex_input_binding_mode`,
`native_vortex_input_binding_count`, `native_vortex_partitioned_input_binding`,
`native_vortex_input_binding_strategy`, and `native_vortex_input_binding_sources`. Post-shaped
Python/DataFrame-style operators for the benchmark-family shapes are also admitted when the CLI is
built with `vortex-production-runtime` or `release-user-surfaces`: grouped count/sum, null-heavy aggregate, hash
join with a declared right Vortex input, global top-N, clean/cast/filter, malformed timestamp cast,
substring contains, and native `write_vortex` result sinks lower through
`vortex-production-runtime-run` with provider scenario evidence. General Vortex SQL/DataFrame
planning now uses the same `native_vortex_unified_plan` contract for documented primitive,
provider, profile, and declared sink capillaries; arbitrary or unshaped SQL/DataFrame breadth
remains explicitly blocked until the specific operator contract exists. Exact provider-backed
result summaries can export bounded `result_json` to workspace-safe `write_jsonl()`/`write_csv()`
sinks with explicit decode/materialization evidence. Primitive filter/project/filter-project,
row-level distinct, bounded source-order tail, deterministic row-count or fractional sample, scoped rolling-window row-streams,
and scalar aggregate one-row results can export workspace-safe JSONL/CSV, including staged JSONL+CSV
fanout with target-level commit/cleanup evidence, through
`native_vortex_primitive_row_export` with explicit selected-column
decode/materialization evidence. Broad compatibility exports outside these Vortex-derived contracts
remain blocked. Local
compatibility-file residual workflows that first normalize through Vortex preparation also block
when the remaining transformed operator, row-level materialization, or compatibility sink lacks a native
Vortex-derived route; they must not execute `local-source-runtime` as the public runtime middle.
The route report and run envelope identify all of this with
`native_vortex_plan_contract_schema_version`,
`native_vortex_plan_route_family`,
`native_vortex_plan_payload_kind`,
`native_vortex_plan_operator_capillaries`,
`native_vortex_operation_family`,
`native_vortex_provider_scenario`, `native_vortex_capability_status`, `typed_result_contract`,
`typed_sink_contract`, and `decode_materialization_boundary` fields so agents and users can tell
supported routes from planned operator families without probing data or invoking external engines.
Materializing native primitive collect routes and primitive row-export routes also emit the same
state-budget evidence fields automatically:
`local_primitive_state_budget_schema_version`, `local_primitive_state_budget_required`,
`local_primitive_state_budget_status`, `local_primitive_state_pressure_class`,
`local_primitive_state_family`, `local_primitive_capillary_work_units`,
`local_primitive_pulseweave_pressure_signals`, `local_primitive_budget_scope`,
`local_primitive_spill_policy`, `local_primitive_spill_io_performed`,
`local_primitive_fail_closed_if_spill_required`, and
`local_primitive_state_budget_next_action`. Runtime envelopes also state their timing surface:
public run routes are `hot_runtime`/`metadata_sink`, route inspection remains no-timing evidence,
and no result-sink or human evidence-render timing is folded into a query-runtime claim. Users
should not need to call a separate PulseWeave, capillary, or dynamic-work-shaping command to get the
admitted route-control evidence for normal Python/SQL/DataFrame execution.
`ShardLoomContext.native_vortex_provider_route_certificate_report()` is the side-effect-free
certificate inventory for those exact provider-backed Python and SQL shapes.
The DataFrame method capability matrix also exposes `future_contract_blocker_ids` for scoped method
variants that remain outside the admitted runtime contract. This keeps sampling/index/reshape,
rolling/window, mask/replace, null semantics, expression/UDF, and fanout boundaries
machine-readable without reintroducing active method blockers or hidden pandas/Polars execution.

The ClickBench OLAP fixture under `benchmarks/clickbench/queries.sql` is now the broad SQL coverage
driver. `scripts/check_clickbench_olap_runtime_coverage.py` classifies all 43 canonical `hits`
queries into admitted primitive SQL rows or reusable implementation families. The current local
coverage map validates 43 admitted rows and 0 implementation-required rows through shared native
Vortex aggregate, grouped expression, predicate, and sorted-row route families. The current native
aggregate family includes direct typed/dictionary scalar `count`/`sum`/`avg`/`min`/`max` and exact
`count_distinct`, compact grouped count/sum/avg state, exact `length(...)` measures, transformed
dictionary URL-domain/length grouping, typed numeric-pair state, and typed numeric/minute/string
state. The checker is a readiness map, not a runtime benchmark or public performance claim. The
canonical report exposes
`clickbench_olap_readiness_status`, `route_family_counts`,
`memory_spill_diagnostic_status`, `admitted_query_count`,
`implementation_required_count`, and `site_readiness_claim_boundary` so docs and benchmark-site
surfaces cannot silently reinterpret route readiness as timing evidence.

## Support Status Vocabulary

| Status | Meaning |
| --- | --- |
| `smoke_supported` / `smoke-supported` | Narrow fixture or smoke route; synthetic safeguards such as row, byte, or output caps may be intentional. |
| `global_runtime_supported` / `runtime-supported` | Runtime-backed capability that is globally reusable across supported SQL/Python/DataFrame/CLI surfaces with explicit semantic and claim boundaries. |
| `feature_gated` | Requires an explicit build/runtime gate such as `universal-format-io`, `vortex-write`, `vortex-production-runtime`, or the aggregate release set `release-user-surfaces`. |
| `production_admitted_local_workflow` | Product local workflow route admitted for normal local Python/SQL/DataFrame-facing usage without smoke-only synthetic caps, while still bounded by local v1 scope and no-fallback evidence. |

## Semantic Claim Vocabulary

ShardLoom does not use broad pandas, Polars, DataFrame, or SQL-standard/ANSI-style compatibility
labels for v1. The supported language is:

- `Python/DataFrame-style front door`: admitted operations lower into ShardLoom-native/Vortex-native
  routes, while unsupported pandas/Polars-style behavior returns deterministic diagnostics with
  `fallback_attempted=false` and `external_engine_invoked=false`.
- `Documented DataFrame-style subset`: admitted operations have equivalent semantics for the
  scoped shapes documented by `ShardLoomContext.front_door_semantic_surface_matrix()`.
- `SQL-standard-inspired SELECT-query subset`: admitted SQL parser, binder, type, NULL, relational,
  operator, aggregate, join, subquery, window, ordering, and error semantics are documented with
  deviations and deterministic blockers.

## Unsupported V1 Forms

The following are outside the v1 front-door runtime claim unless a later phase-plan item closes them
with runtime and release evidence:

- Broad SQL-standard/ANSI-style compliance, CTE/recursive SQL, arbitrary subqueries, arbitrary
  functions, unsupported nested accessors/casts, broad SQL grammar coverage, and broad semantic
  parity.
- Broad pandas, Polars, Spark, DataFusion, DuckDB, PySpark, or dataframe-library API parity.
- Hidden execution in pandas, Polars, DuckDB, Spark, DataFusion, Velox, or another engine.
- Unbounded materialization as a convenience path.
- Unsupported joins, subqueries, non-admitted window frames beyond scoped source-order rolling sum/mean/count/min/max, UDFs, plugins, LLM/API calls, embeddings, vector search,
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

`malformed_timestamp_cast` now runs through the prepared Vortex replay fixture as an admitted
scenario with valid-row evidence. If a future source contains values outside the admitted cast
contract, that path must fail closed with deterministic diagnostics and no fallback or external
engine invocation.

## Claim Boundary

After this scope is closed, ShardLoom may claim global front-door support for the supported v1
forms above. It still may not claim:

- broad SQL/DataFrame parity;
- front-door performance equivalence;
- production readiness;
- package release/publication;
- Spark/DataFusion/DuckDB/Polars replacement; or
- object-store/table/remote/Foundry/live/distributed production support.

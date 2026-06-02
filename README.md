# ShardLoom

[shardloom.io](https://shardloom.io) is the public, claim-safe interpretation layer for the
project. The repository is the source of truth for code, architecture docs, phase plans, use cases,
benchmarks, and release evidence.

ShardLoom is a pre-release, Vortex-first, no-fallback local compute engine foundation. It is being
built around explicit routes, deterministic blockers, and evidence fields that show what ran:
source admission, Vortex preparation, execution mode, output planning, certificates, fallback
status, and claim gate status.

ShardLoom is not an official Vortex project and is not Vortex-endorsed. It does not claim
production readiness, public performance superiority, Apache Spark-displacement, production
SQL/DataFrame support, production object-store/lakehouse support, production Foundry support,
package publication readiness, or hidden external fallback.

## What It Does

ShardLoom is shaped around this route model:

```text
front door -> source route -> preparation route -> execution route -> output route -> evidence
```

For non-Vortex inputs, the prepared path is explicit:

```text
UniversalIngress / InputAdapter
-> SourceState
-> vortex_ingest
-> VortexPreparedState
-> prepared_vortex
-> OutputPlan
-> SinkArtifact / evidence
```

`prepared_vortex` executes from `VortexPreparedState`; it does not read CSV, JSONL, Parquet,
database rows, object-store objects, or generated rows directly. Compatibility import is the
certified cold ingest/stage route, not a pure query-speed route.

Public benchmark and evidence surfaces use two views:

| View | Meaning |
| --- | --- |
| Route lanes | What users compare end to end: raw source to result, prepare-once to result, warm prepared query, native Vortex query, direct transient route, or external baseline. |
| Stage pieces | Why a route took that time: admission, read, parse/decode, SourceState build, Vortex array build/write/reopen, prepared-state lookup, scan, operator compute, sink write, and evidence render. |

The primary local non-Vortex route is `ShardLoom Prepare-Once First Query`: raw compatibility input
is admitted into `SourceState`, prepared once into `VortexPreparedState`, queried, and written with
evidence. `ShardLoom Warm Prepared Query` starts after that prepared state already exists, so it is
useful runtime evidence but not a raw-source end-to-end comparison by itself.

## What Is Usable Today

Current runtime support is intentionally scoped and evidence-gated:

- local first-10-minutes smoke and release dry-run workflows;
- Python and CLI front doors for local CSV, JSONL/NDJSON, flat JSON, generated-source, local
  Vortex, and feature-gated Parquet/Arrow IPC/Avro/ORC runtime paths;
- scoped SQL local-source execution for projection, row-level `SELECT DISTINCT` over projection,
  aggregate/HAVING, join, and window output rows, filter, limit,
  scalar aggregates, multi-key group-by, single-key top-N, selected
  casts/date/timestamp/temporal-difference/string/LIKE/regex/IN predicates, scoped
  `INTERVAL '<n>' DAY|HOUR|MINUTE|SECOND` literals inside temporal helper functions, scalar and
  row-value literal `IN`/`NOT IN`, bounded scalar and row-value local-source
  `IN (SELECT ...)` / `NOT IN (SELECT ...)`, scoped local-source
  `EXISTS (SELECT ...)` / `NOT EXISTS (...)` presence predicates, scoped quantified
  `ANY` / `ALL (SELECT ...)` predicates over bounded local scalar sources, scoped local-source
  inner/outer/semi/anti equi-joins, cross joins, scoped
  column-comparison/generic numeric expression ON joins, computed projections and single-key top-N
  over joined rows, scoped scalar/grouped join aggregates, and post-aggregate `HAVING` filters over
  aggregate output rows including admitted bounded `IN`, `EXISTS`, and quantified `ANY`/`ALL`
  subqueries;
- source-free generated local outputs through user rows, ranges, sequences, calendars, SQL `VALUES`,
  literal `SELECT`, and `generate_series`/`range` smokes, including feature-gated local Vortex
  output;
- scoped local-source output/fanout to JSONL/CSV, feature-gated Parquet/Arrow IPC/Avro/ORC, and
  feature-gated local Vortex sinks with local replay/fidelity evidence;
- fixture-scoped object-store URI parsing for S3/GCS/ADLS, public no-credential local-fixture
  reads, and local-emulator read/write smokes with credential, network, and provider probes
  disabled;
- local Vortex/prepared-native benchmark evidence for selected traditional analytics scenarios;
- feature-gated local `vortex_ingest` runtime that prepares admitted flat scalar local sources into a
  local `.vortex` artifact and emits `VortexPreparedState` evidence with explicit
  `ingest_minimal` / `ingest_certified` certification-depth semantics;
- Python and SQL workflows that expose normal read/filter/select/write calls while preserving
  internal SourceState, Vortex preparation, OutputPlan, replay, reuse, and no-fallback evidence
  behind the user surface;
- familiar Python/DataFrame aliases such as `project`, `with_columns`, `assign`, `groupby`,
  `order_by`, `sort_by`, `sort_values`, `distinct`, `drop_duplicates`, and `unique` when they lower
  to those same ShardLoom runtime paths;
- report-only or blocked status for broader SQL/DataFrame, live/authenticated object-store
  providers, lakehouse/table commits, distributed, live/hybrid production, Foundry production, and
  package-publication claims.

Unsupported work must emit a deterministic blocker instead of delegating execution to Spark,
DataFusion, DuckDB, Polars, Dask, Ray, pandas, Velox, Trino, a database, a warehouse, or another
fallback engine. External engines may appear only as baselines, oracles, or migration references.

## Try It

Start here:

- [`docs/getting-started/install.md`](docs/getting-started/install.md)
- [`docs/getting-started/first-10-minutes.md`](docs/getting-started/first-10-minutes.md)
- [`docs/getting-started/examples.md`](docs/getting-started/examples.md)
- [`docs/getting-started/certified-local-workload.md`](docs/getting-started/certified-local-workload.md)
- [`python/README.md`](python/README.md)

Typical local orientation:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
python scripts\check_production_usability_gate.py
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root .
```

Normal Python use:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("target/orders.csv")
    .filter(sl.col("amount") >= 10)
    .select("id", "amount")
    .limit(100)
    .write_jsonl("target/orders-out.jsonl", allow_overwrite=True)
)

print(result.output_row_count)
print(result.first_result_row)
print(result.evidence_summary.output_path)
print(result.claim_summary.claim_gate_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

`sl.context()` is the normal entry point. Source-tree or CI runs can set `SHARDLOOM_BIN` or
`SHARDLOOM_REPO_ROOT` in the environment when the CLI is not on `PATH`; ordinary Python snippets
should not need `repo_root` or build-profile arguments.

Scoped local-source Python/DataFrame and SQL workflows can use either `.limit(n).collect()` or
`collect(limit=n)`; raw SQL can also carry `LIMIT` in the statement. Unbounded local-source collect
returns a deterministic no-fallback diagnostic instead of accidentally reading an unbounded result.
For familiar Python/DataFrame code, aliases such as `.project(...)`, `.with_columns(...)`,
`.assign(...)`, `.groupby(...)`, `.order_by(...)`, `.sort_by(...)`, `.sort_values(...)`,
`.distinct()`, `.drop_duplicates()`, `.unique()`, `.union(...)`, and `.union_all(...)` are accepted
only as thin names over
the admitted ShardLoom `select`, `with_column`, `group_by`, `agg/count`, `sort`, LIKE/regex/string
predicates, interval-backed temporal helper predicates, scoped `UNHEX(<utf8-column>)` /
`FROM_BASE64(<utf8-column>)` binary helper projections, join/window, source-backed `IN` /
`EXISTS` / `ANY` / `ALL` including grouped/HAVING projected source-subquery tails, row-level
`SELECT DISTINCT`, scoped SQL `UNION` / `UNION ALL`, and bounded terminal paths.
Bounded `schema()`, `schema_contract(...)`, `data_quality_*`, `profile(...)`, and
`quarantine(...)` helpers use the same local-source runtime evidence; `profile()` reports
row/field/null-count observability from the bounded inline JSONL result, and pushdownable
`quarantine()` not-null rows can write to an admitted local sink without invoking pandas, Polars,
or another engine.
Generated rows can also be staged into the scoped local-emulator object-store route with
`ctx.generated_output_to_object_store(...)`, which chains ShardLoom's generated-source local output
and object-store write smokes while keeping live cloud providers, table commits, and production
claims gated.
For Foundry-shaped development, `ctx.foundry_generated_output(...)` supports the local dev-stack
result/evidence dataset proof without invoking real Foundry runtime, Foundry Spark, object stores,
or external engines.

The Python and SQL front doors stay format-neutral after the read/ingest boundary. `ctx.read(path)`
infers the local source adapter from the file extension; explicit helpers such as `read_csv(...)`
remain aliases for code that wants them. A caller writes to a requested sink and lets ShardLoom
manage SourceState, Vortex preparation, execution, OutputPlan, replay, reuse, certificates, and
no-fallback evidence internally. Lower-level two-path `prepare_vortex(source, target_vortex)`
ingest-smoke calls, runtime-envelope inspection, and session evidence are engine-development and
diagnostic surfaces.

When a user specifically wants the benchmark-range prepare-once route from compatibility files into
prepared Vortex artifacts, `ctx.prepare_vortex(...)` exposes that route directly and names the
timing boundary:

```python
prepared = ctx.prepare_vortex(
    "target/fact.csv",
    dim="target/dim.csv",
    workspace="target/shardloom-prepared",
    input_format="csv",
    evidence_level="certified",
)

result = prepared.query("join_aggregate").collect()
batch = prepared.run_batch(["group_by_aggregation", "sort_top_k"])

print(prepared.route_fields())
print(result.lifecycle_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

This is the explicit `compatibility_import_certified -> prepared_vortex` route. It starts from raw
compatibility input, prepares once into `VortexPreparedState`, then runs the prepared query or batch;
it does not treat `prepared_vortex` as a direct CSV/Parquet/JSONL reader.
The Python route writes a caller-owned workspace manifest under
`<workspace>/.shardloom/prepared-vortex-reuse-manifest.json`; repeated compatible calls reuse the
existing local Vortex artifacts through the real prepared Vortex batch command when source,
prepared-artifact, and prepare-policy fingerprints still match. Reuse reports
`prepared_state_reuse_hit`, `prepared_state_reuse_reason`,
`prepared_state_reuse_manifest_digest`, and `invalidation_reason`, and source drift triggers a
normal re-prepare instead of silent stale reuse.
The Rust CLI reports the same reuse vocabulary directly in `compute_flow_evidence`: cold
first-preparation rows say `prepared_state_created_not_reused`, warm prepared rows say
`explicit_prepared_state_input`, native `.vortex` rows say `not_applicable_native_vortex_input`, and
single-process prepare/batch rows say `in_process_prepared_batch_vortex_artifacts`. The
feature-gated `traditional-analytics-prepare-batch-run` command now also uses the same
caller-owned workspace manifest directly: a repeated compatible CLI call can skip compatibility
preparation, run `traditional-analytics-vortex-batch-run` over the existing local Vortex artifacts,
and report `workspace_manifest_local_vortex_artifacts` with hit/reason/digest/invalidation fields.
The feature-gated `vortex-ingest-smoke` command also writes an artifact-adjacent prepared-state
reuse manifest next to local `.vortex` outputs. A repeated identical local ingest reuses the
manifest-backed artifact and reports `vortex_ingest_performed=false`,
`prepared_state_reuse_scope=artifact_adjacent_manifest_local_vortex_artifacts`,
`prepared_state_reuse_hit=true`, and `invalidation_reason=none`; source drift fails closed and
re-enters the normal writer only when overwrite is explicitly allowed.

For native `.vortex` benchmark-range inputs, use the route-level handle when the desired workflow is
the same native/prepared Vortex runtime family represented in benchmark rows:

```python
native = ctx.native_vortex_route(
    "target/fact.vortex",
    "target/dim.vortex",
    execution_mode="native_vortex",
    memory_gb=4,
    max_parallelism=1,
)

result = native.query("selective filter").collect()
sink = native.query("selective filter").write_vortex("target/native-result")

print(native.route_fields())
print(result.field("selected_execution_mode"))
print(result.fallback.attempted)
```

`ctx.read_vortex(...).count/filter/select/limit/collect` remains the scoped primitive/query-builder
surface. `ctx.native_vortex_route(...)` is the route-comparable benchmark-family surface: source,
execution mode, scenario, resource policy, result-sink choice, and no-fallback evidence are all
explicit.

For route selection, use the side-effect-free route capability report instead of guessing from
benchmark lane names or scattered status text:

```python
routes = ctx.user_route_capability_report()
route = routes.route("local_file_prepare_once_first_query")
print(route.vortex_normalization_point)
print(route.execution_mode, route.output_route)
print(route.prepared_state_reuse_scope, route.prepared_state_reuse_manifest_path)
print(route.claim_gate_status, route.fallback_attempted, route.external_engine_invoked)

vortex_primitives = ctx.local_vortex_primitive_route_report()
print(vortex_primitives.route("vortex_filter_project_limit_collect").cli_command)

benchmark_routes = ctx.local_file_benchmark_route_report()
print(benchmark_routes.scenario("join_aggregate").route_runtime_status)
```

The report answers which route to use for a declared input/output pair, where the input crosses
into Vortex-preparable or Vortex-native state, what executes, what may be decoded or materialized,
and which evidence/claim boundary applies. For local `.vortex` inputs, the primitive route report
maps each scoped SQL/Python/DataFrame/session form to the exact ShardLoom Vortex primitive command
instead of implying a broad read-transform-write or result-sink route.
For local compatibility-file benchmark families, the benchmark route report maps each named
scenario to a direct or prepare-once ShardLoom route and keeps fixture-scoped nested JSON, CDC
overlay, many-small-files, partition, dirty-data, sort/window, join, and aggregate coverage
separate from broad production or performance claims.

Unbounded convenience materializations return deterministic evidence instead of delegating to
pandas, Polars, Spark, DataFusion, DuckDB, or another engine. Bounded local-source workflows can
opt into explicit decoded containers through the Python materialization helpers:

```python
materialization_report = ctx.read("target/orders.csv").select("id").to_pandas()
print(materialization_report.blocker_id)
print(materialization_report.fallback_attempted, materialization_report.external_engine_invoked)
```

Exact smoke commands, feature flags, expected outputs, and claim boundaries live in the linked
getting-started docs.

## Evidence And Benchmarks

Benchmarks are evidence, not a leaderboard. ShardLoom separates certified cold route timing from
prepared warm route timing and keeps external engines labeled as baseline context only.

Public rows should be read through these fields before comparing numbers:

- `route_runtime_status` says whether the user workflow is a scoped runtime route, fixture smoke,
  gated feature, external baseline, or deterministic policy/no-route diagnostic.
- `claim_gate_status` says whether the evidence for that row is claim-grade; it is not a synonym
  for production readiness or a speed claim.
- `performance_claim_allowed`, `production_claim_allowed`, and
  `spark_replacement_claim_allowed` must remain explicit and false unless a later claim gate
  authorizes them.
- ShardLoom route rows and external baseline rows are separate. Baseline gaps are not ShardLoom
  runtime gaps.
- Cold prepare rows expose capillary work shaping with
  `vortex_capillary_preparation_execution_window_count`,
  `vortex_capillary_preparation_scheduler_applied`,
  `vortex_capillary_preparation_prewrite_status`, pre-write gate fields, and prefixed
  PulseWeave/ProofBound fields, so readers can distinguish a skipped tiny fixture from an admitted
  bounded work-window plan that gated the local prepare/write/reopen route.

The route labels to expect are `ShardLoom Cold Certified Route`,
`ShardLoom Prepare-Once First Query`, `ShardLoom Prepare-Once Batch`,
`ShardLoom Warm Prepared Query`, `ShardLoom Native Vortex Query`,
`ShardLoom Direct Transient Route`, and `External Baseline End-to-End`.

Use:

- [`docs/benchmarks/local-taxonomy-benchmark.md`](docs/benchmarks/local-taxonomy-benchmark.md)
- [`docs/benchmarks/baseline-comparison-boundary.md`](docs/benchmarks/baseline-comparison-boundary.md)
- [`benchmarks/traditional_analytics/README.md`](benchmarks/traditional_analytics/README.md)
- [shardloom.io/benchmarks](https://shardloom.io/benchmarks)

## Architecture

The human-readable route map is on [shardloom.io/compute-engine-flow](https://shardloom.io/compute-engine-flow).
The canonical Markdown source is
[`docs/architecture/compute-engine-flow-reference.md`](docs/architecture/compute-engine-flow-reference.md).

Other useful anchors:

- [`docs/architecture/canonical-terminology.md`](docs/architecture/canonical-terminology.md)
- [`docs/architecture/universal-ingress-route-taxonomy.md`](docs/architecture/universal-ingress-route-taxonomy.md)
- [`docs/use-cases/README.md`](docs/use-cases/README.md)
- [`docs/use-cases/recipes/README.md`](docs/use-cases/recipes/README.md)

## Project Status

Active work is tracked in
[`docs/architecture/phased-execution-plan.md`](docs/architecture/phased-execution-plan.md).
Completed implementation history lives in
[`docs/architecture/phased-execution-completed-ledger.md`](docs/architecture/phased-execution-completed-ledger.md).

Release and package posture:

- public packages are not yet released;
- local production-usability rehearsal evidence is validated by
  [`docs/release/production-usability-gate.md`](docs/release/production-usability-gate.md), while
  `public_release_claim_allowed=false` and `public_package_claim_allowed=false` remain required;
- package-channel evidence is still gated by
  [`docs/release/package-channel-readiness-matrix.md`](docs/release/package-channel-readiness-matrix.md);
- Foundry docs describe local/dev-stack generated-output and staged-transform proof boundaries,
  not production Foundry support.

## Development

Useful checks while working locally:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
Push-Location website-src
npm run build
npm run check
Pop-Location
python scripts\check_website_readiness.py
node website\validate_static_assets.js
git diff --check
```

The public website is generated from `website-src/` Astro/Starlight source and committed static
assets under `website/`. `npm run sync-content` copies canonical docs, use-case/status rows, and
benchmark artifacts into the site build; do not hand-edit generated website copies independently.
The site should stay compact, light-mode, benchmark/compute-flow/repo centered, and claim-safe.

## License

ShardLoom is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE).

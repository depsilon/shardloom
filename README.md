# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/depsilon/shardloom?include_prereleases&label=release)](https://github.com/depsilon/shardloom/releases)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Posture](https://img.shields.io/badge/posture-no--fallback%20technical--preview-0f766e)](docs/release/public-status-matrix.md)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#what-makes-shardloom-different)

[shardloom.io](https://shardloom.io) is the public, claim-safe interpretation layer for this
repository. The repo remains the source of truth for code, architecture docs, phase plans, use
cases, benchmarks, and release evidence.

ShardLoom's latest proof-backed technical-preview package release is published across GitHub
pre-release assets, TestPyPI, PyPI, and the `depsilon/tap` Homebrew formula. ShardLoom is a
Vortex-first, no-fallback local compute engine foundation built around explicit routes and evidence
fields that show what ran: source admission, Vortex preparation, execution mode, output planning,
certificates, fallback status, and claim-gate status.

ShardLoom is not an official Vortex project and is not Vortex-endorsed. It does not claim production
readiness, public performance superiority, broad engine replacement or Apache Spark displacement,
broad SQL/DataFrame support, production object-store or lakehouse support, production Foundry
support, package access as a production support claim, or hidden external fallback.

## What Makes ShardLoom Different

ShardLoom's differentiators are execution and evidence contracts around Vortex-native data, not
blanket performance claims:

- **Vortex-native middle, no hidden fallback**: public compatibility workflows normalize into an
  admitted Vortex route or fail closed, native Vortex inputs stay native, and non-admitted plans
  emit deterministic diagnostics instead of running through Spark, DataFusion, DuckDB, Polars,
  pandas, or another engine.
  - compatibility inputs are source adapters, not alternate execution engines.
  - native/prepared Vortex routes must report their route ID, feature gate, and runtime mode.
  - direct local smoke paths stay internal safeguards and cannot masquerade as product runtime.
- **Evidence-certified routes**: every public workflow is expected to expose what actually ran:
  source admission, Vortex preparation, execution mode, output planning, certificate state,
  fallback/external-engine status, and claim posture.
  - `SourceState` records the admitted input boundary.
  - `VortexPreparedState` records the native prepared middle.
  - route certificates connect execution evidence to output artifacts and claim gates.
- **PulseWeave**: ShardLoom's route-control vocabulary for bounded local work shaping.
  - `FlowInventory` tracks in-flight source, execution, and writer work.
  - `ScarcityLedger` records memory, decode, sink, and pressure signals.
  - `EndoPulse` applies run-local feedback without delegating to another engine.
  - `ProofBound` blocks adaptive behavior until the route has certificate evidence.
- **Capillary work units**: ingest, preparation, execution, and output work can be split into small
  typed units instead of opaque tasks.
  - each unit carries source range, projection/filter mask, and target artifact references.
  - each unit records materialization posture, retry/idempotency state, sink pressure, memory
    pressure, and no-fallback evidence.
  - units can be coalesced, split, retried, reused, or audited without hiding execution boundaries.
- **Dynamic work shaping**: metadata, workload shape, route evidence, and measured feedback guide
  how ShardLoom sizes work.
  - small units can be coalesced when scheduling overhead dominates.
  - large units can be split when memory, decode, sink, or source pressure requires it.
  - hard proof lanes remain separate from fast lanes so CI, benchmarks, and release gates stay
    evidence-preserving.
- **Metadata-first, late-materialized execution**: ShardLoom tries to answer from metadata, prune
  segments, compute over encoded Vortex data, decode only what is needed, and materialize at
  explicit output boundaries.
  - metadata and statistics checks run before row reads where the route supports them.
  - segment pruning and encoded kernels are preferred before decode.
  - collect and compatibility writes must report bounded decode/materialization evidence.
- **Timing-surface discipline**: hot runtime, replay proof, and publication proof are separated so
  proof-heavy evidence work does not silently become a query-runtime claim.
  - `hot_runtime` covers the query/runtime lane.
  - `full_replay_proof` covers replayable machine proof.
  - `publication_proof` covers result-sink and human evidence rendering work.
- **Patent-pending design notice**: PulseWeave, capillary work units, dynamic work shaping, and
  related route/evidence/certificate machinery include patent-pending design elements. ShardLoom
  remains distributed under Apache-2.0; this notice is informational, preserves attribution, and
  deters bad-faith copying without expanding the technical-preview support claim.

## First Read

Use this README as the entry point, then follow the source that matches your question.

| If you want to know... | Start here |
| --- | --- |
| What ShardLoom is and is not | [About](https://shardloom.io/about) |
| How to install and run a local smoke | [Install](docs/getting-started/install.md), [source checkout](docs/getting-started/source-checkout-install.md), [first 10 minutes](docs/getting-started/first-10-minutes.md), [examples](docs/getting-started/examples.md) |
| Whether package install commands are live | [Package user install status](docs/getting-started/package-user-install.md) |
| What is currently supported or blocked | [V1 supported/unsupported surface](docs/getting-started/v1-supported-unsupported.md) |
| How to diagnose a run | [Troubleshooting and support bundle](docs/getting-started/troubleshooting-support.md) |
| How routes, evidence, and claims fit together | [Compute flow](https://shardloom.io/compute-engine-flow), [canonical compute-flow reference](docs/architecture/compute-engine-flow-reference.md) |
| What public support claims are currently allowed | [Public status matrix](docs/release/public-status-matrix.md) |
| What finished-product v1 currently means | [Finished product scope](docs/release/finished-product-scope.md) |
| What local/source/package release track is selected | [V1 local source/package release track](docs/release/v1-local-source-package-release.md) |
| What the benchmark page is actually showing | [Benchmarks](https://shardloom.io/benchmarks), [local benchmark taxonomy](docs/benchmarks/local-taxonomy-benchmark.md) |
| What is planned or incomplete | [Phased execution plan](docs/architecture/phased-execution-plan.md) |
| What has already landed | [Completed ledger](docs/architecture/phased-execution-completed-ledger.md) |

GitHub renders repository READMEs as the first project surface, so this file stays compact and
links to the detailed references instead of duplicating every implementation status row.

## Core Contract

ShardLoom's route model is:

```text
front door -> source route -> preparation route -> execution route -> output route -> evidence
```

For non-Vortex inputs, prepared execution is explicit:

```text
UniversalIngress / InputAdapter
-> SourceState
-> vortex_ingest
-> VortexPreparedState
-> prepared_vortex
-> OutputPlan
-> SinkArtifact / evidence
```

`prepared_vortex` starts from `VortexPreparedState`; it does not read CSV, JSONL, Parquet, database
rows, object-store objects, or generated rows directly. Compatibility import is the certified cold
ingest/stage route, not a pure query-speed route.

Unsupported work must emit deterministic blocker diagnostics instead of delegating execution to
Spark, DataFusion, DuckDB, Polars, Dask, Ray, pandas, Velox, Trino, a database, a warehouse, or
another fallback engine. External engines may appear only as baselines, test oracles, or migration
references.

## Current Support Posture

Current runtime support is intentionally scoped and evidence-gated.

This table is a README summary; the canonical public status matrix and claim boundary lives in
[docs/release/public-status-matrix.md](docs/release/public-status-matrix.md).

| Surface | Current posture | Claim boundary |
| --- | --- | --- |
| Local first-10-minutes smoke | Supported through local dry-run and Python examples. | Local technical-preview evidence only. |
| CLI and Python front doors | Scoped local CSV, JSONL/NDJSON, flat JSON, generated rows, local Vortex, and selected feature-gated file/sink paths. | No broad SQL/DataFrame, package, production, or performance claim. |
| SQL/DataFrame-style use | Many scoped local-source projections, filters, joins, aggregates, subqueries, aliases, bounded collects, metadata profiles, native Vortex writes, and scoped Vortex-derived JSONL/CSV row exports are admitted through ShardLoom routes. | Arbitrary compatibility exports still require a native Vortex-derived export contract; not PySpark/pandas/Polars parity and not broad production SQL/DataFrame support. |
| OLAP query-family coverage | ClickBench query-family readiness is tracked by `benchmarks/clickbench/queries.sql` and `scripts/check_clickbench_olap_runtime_coverage.py`; the current local map validates 43 admitted rows and 0 implementation-required rows. Rows lower through reusable native Vortex SQL primitive routes for aggregate, grouped expression, predicate, and sorted-row families, with capillary work-unit, PulseWeave pressure, scale-fixture, and fail-closed spill fields. | Coverage map only; no ClickBench performance or superiority claim without a promoted benchmark artifact. |
| Vortex preparation | Feature-gated local `vortex_ingest` creates local `.vortex` artifacts with SourceState and VortexPreparedState evidence. | Scoped local flat-schema evidence; no broad writer, object-store, table, or performance claim. |
| Local output/sink scope | `write_vortex(...)` is the highest-fidelity admitted native local sink for provider-backed routes and scoped structured Vortex-derived exports. Exact provider-backed Vortex result summaries can export bounded `result_json` to workspace-safe `write_jsonl(...)` and `write_csv(...)`; scoped primitive filter/project/filter-project/distinct/tail/sample/expression-project/melt/explode/pivot/rolling-window row streams and scalar/grouped aggregate result rows can export JSONL/CSV and JSONL+CSV fanout through `native_vortex_primitive_row_export`. Scoped structured expression-project row streams can export Vortex, Parquet, Arrow IPC, and Avro with explicit materialization evidence. Scoped pivot/pivot_table JSONL emits sparse wide cells as `null`; CSV emits sparse cells as empty fields. Broader `write(...)`, unsupported formats, unsafe fanout, ORC nested output, and arbitrary compatibility exports block until a native Vortex-derived export contract exists. | Local artifacts only; no append, object-store paths, table/catalog writes, production sink, or performance claim. |
| Prepared/native benchmark routes | Local benchmark artifacts expose cold, prepare-once, warm prepared, native Vortex, direct transient, and external-baseline lanes. | Claims depend on the selected timing surface and claim gate. |
| Object store, lakehouse, Foundry, live/hybrid | Mostly fixture-scoped with report-only or blocked status for broader platform routes. | No production platform claim. |
| Package/release status | The latest published technical-preview package is available through GitHub pre-release assets, TestPyPI, PyPI, and the `depsilon/tap` Homebrew formula with checked-in channel proof. | No production/platform, performance, or broad runtime claim. |

User surface graduation is tracked separately from runtime breadth. Public CLI/Python surfaces are
classified as `high_level_context`, `client_only`, `diagnostic_only`, `feature_gated`, or
`not_user_facing` by the user surface graduation matrix. A surface name alone is not a runtime
claim.

### Python/DataFrame and SQL Semantics

ShardLoom does not claim broad pandas, Polars, DataFrame, or SQL-standard/ANSI-style compatibility
labels. It claims a documented subset: admitted Python/DataFrame-style
operations and admitted SQL-standard-inspired SELECT-query forms lower into
ShardLoom-native/Vortex-native routes, while non-admitted behavior returns deterministic diagnostics
with `fallback_attempted=false` and `external_engine_invoked=false`.

The human-readable semantic surface lives in
[`docs/architecture/sql-python-dataframe-front-door-parity.md`](docs/architecture/sql-python-dataframe-front-door-parity.md).
The agent-facing source is `ctx.front_door_semantic_surface_matrix()`, which covers read APIs,
projection, filtering, types, casts, missing data, aggregates, joins, ordering/window-ish behavior,
reshaping, materialization, index semantics, callable/expression APIs, determinism, errors,
fallback boundaries, and SQL parser/binder/null/subquery/window/operator semantics.

## Try It

Start with:

- [docs/getting-started/install.md](docs/getting-started/install.md)
- [docs/getting-started/source-checkout-install.md](docs/getting-started/source-checkout-install.md)
- [docs/getting-started/package-user-install.md](docs/getting-started/package-user-install.md)
- [docs/getting-started/first-10-minutes.md](docs/getting-started/first-10-minutes.md)
- [docs/getting-started/v1-supported-unsupported.md](docs/getting-started/v1-supported-unsupported.md)
- [docs/getting-started/troubleshooting-support.md](docs/getting-started/troubleshooting-support.md)
- [docs/getting-started/certified-local-workload.md](docs/getting-started/certified-local-workload.md)
- [python/README.md](python/README.md)

Typical local orientation:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
python scripts\check_production_usability_gate.py
python examples\local-python-smoke\run.py --repo-root .
```

The selected local/source/package v1 release track is source checkout plus GitHub pre-release,
TestPyPI, PyPI, and Homebrew. Source is currently at v0.1.10 release prep. GitHub pre-release, TestPyPI, PyPI, and Homebrew are published for the latest checked-in selected-channel proof
(`package_channel_status=published_v0.1.10_selected_channels`) and remain proof-backed by the
checked-in channel transcripts.
Scoop/winget/conda are feasible later channels, while real production object-store, lakehouse,
distributed, live/hybrid, and Foundry claims stay fail-closed without environments.

Package installs:

```sh
python -m pip install shardloom
brew install depsilon/tap/shardloom
```

The Python package is a thin client surface over the ShardLoom CLI. Published packages resolve the
CLI from explicit binary/env/source configuration first, then from bundled resources in supported
platform wheels, and finally from `shardloom` on `PATH`. Managed Python environments can use
`sl.context()` without passing binary paths when they install a supported bundled wheel.

Normal Python use:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("data/orders.csv")
       .filter(sl.col("status") == "paid")
       .limit(10)
       .collect()
)

print(result.output_row_count)
print(result.first_result_row)
print(result.activation_summary.native_vortex_status)
print(result.activation_summary.execution_mode, result.activation_summary.applied_parallelism)
print(result.claim_summary.claim_gate_status)
print(result.fallback_attempted, result.external_engine_invoked)
```

`sl.context()` is the ordinary user-facing entry point. `repo_root`, `profile_order`, explicit
schemas, and format-specific helpers such as `read_csv(...)` remain useful for source-checkout,
CI, benchmark, and reproducibility flows, but they are not required for normal local package code.
Every normal result exposes `activation_summary` so users can see which route ran, whether native
Vortex was active, requested/applied parallelism, pushdown and materialization signals when
available, fallback/external-engine status, and claim-gate posture without scraping the full
envelope.

`ctx.read(path)` infers the local source adapter for `.csv`, `.json`, `.jsonl`, `.ndjson`,
`.parquet`, `.arrow`, `.ipc`, `.feather`, `.avro`, `.orc`, and `.vortex` paths. CSV, flat
JSON/JSONL/NDJSON, generated rows, and scoped local Vortex inputs are the default public examples.
Parquet, Arrow IPC/Feather, Avro, and ORC are admitted scoped local-format surfaces when the
matching feature-gated build is present; builds without those readers return deterministic adapter
blockers instead of invoking another engine.
Universal ingest owns source-specific details before Vortex preparation: schema hints, JSONL/NDJSON
format identity, all-null text-column defaults, mixed numeric promotion, and nested JSON payload
normalization happen at the SourceState boundary, not as separate CSV/JSON compute routes.

The benchmark-page ETL scenarios use the same primary ShardLoom front door from Python, but they are
schema-pinned source-checkout reproduction snippets rather than the minimal application-start code.
They show the scoped v1 front door defined in
[`docs/architecture/v1-front-door-runtime-scope.md`](docs/architecture/v1-front-door-runtime-scope.md);
measured route timing comes from the promoted benchmark artifact and remains claim-gated.
The v1 Vortex runtime scope is separately defined in
[`docs/architecture/v1-vortex-runtime-scope.md`](docs/architecture/v1-vortex-runtime-scope.md):
it admits feature-gated local Vortex primitives, prepared Vortex state, prepared compatibility
artifacts, and generated local Vortex artifacts without claiming broad Vortex support.
For direct `.vortex` inputs, ShardLoom admits documented benchmark-family Python and SQL shapes for
filters/projections, grouped aggregation, hash join, global top-N, cast/try-cast, substring
contains, scoped distinct/deduplication, deterministic sampling, scalar/null expression rewrites,
forward fill, melt/explode/pivot, rolling windows, explicit plan transforms, native `write_vortex`
sinks, scoped structured exports, and bounded provider-backed JSONL/CSV exports. The released
`route()` and `run()` facades infer the real native Vortex primitive/provider payloads for admitted
shapes; normal `ctx.read_vortex(...).select(...).limit(...).route()`, equivalent SQL/Python paths,
and admitted local SQL paths with declared input format can prepare into Vortex without manual
`--vortex-primitive` or `--native-vortex-provider-scenario` wiring.

The detailed surface is intentionally kept in canonical reference files instead of this README:

- Human/runtime scope: [docs/architecture/v1-vortex-runtime-scope.md](docs/architecture/v1-vortex-runtime-scope.md)
- Agent command/API index: [docs/reference/shardloom-user-surface-index.md](docs/reference/shardloom-user-surface-index.md)
- Machine-readable index: [docs/reference/shardloom-user-surface-index.json](docs/reference/shardloom-user-surface-index.json)
- Route certificates: `ctx.native_vortex_provider_route_certificate_report()`

Broader arbitrary Vortex SQL/DataFrame planning and pandas-style percentile/options summaries remain
outside the v1 support claim and return deterministic route diagnostics until they have route
certificates.
The v1 SourceState and prepared-state reuse boundary is defined in
[`docs/architecture/v1-source-prepared-state-scope.md`](docs/architecture/v1-source-prepared-state-scope.md):
it owns the scoped `UniversalIngress -> SourceState -> vortex_ingest -> VortexPreparedState`
normalization path, direct transient boundary, reuse invalidation matrix, and no-fallback evidence.
Agents and automation should start from the canonical all-surface index in
[`docs/reference/shardloom-user-surface-index.md`](docs/reference/shardloom-user-surface-index.md)
and `docs/reference/shardloom-user-surface-index.json` before guessing available Python, SQL, CLI,
generated-source, or blocker surfaces.

To run these local scenario snippets from a source checkout and inspect timing components:

```powershell
python examples\local-python-benchmark-scenarios\run.py --repo-root .
python examples\local-python-benchmark-scenarios\timing_review.py --repo-root .
```

```python
import shardloom as sl

ctx = sl.context(repo_root="/path/to/shardloom", profile_order=("release", "debug"))

prepared = ctx.prepare_vortex(
    "data/fact.csv",
    dim="data/dim.csv",
    workspace="target/shardloom-prepared",
    input_format="csv",
    result_workspace="target/shardloom-results",
    evidence_level="certified",
    max_parallelism=1,
)

# selective filter
result = prepared.query("selective filter").collect()

# filter + projection + limit
prepared.query("filter + projection + limit").collect()

# group by aggregation
prepared.query("group by aggregation").collect()

# hash join
prepared.query("hash join").collect()

# global top-N
prepared.query("sort and top-k").collect()

# clean/cast/filter/write
prepared.query("clean/cast/filter/write").collect()

# malformed timestamp / dirty CSV
prepared.query("malformed timestamp / dirty CSV").collect()

# null-heavy aggregate
prepared.query("null-heavy aggregate").collect()

# nested JSON field scan
prepared.query("nested JSON field scan").collect()

print(result.batch.field("scenario_selective-filter_fallback_attempted"))
print(result.batch.field("scenario_selective-filter_external_engine_invoked"))
```

Raw compatibility sources are normalized into VortexPreparedState before these scenarios execute.
Direct one-shot local CSV/JSON execution is an internal smoke route only; normal public workflows
use Vortex-prepared or native Vortex routes, or fail closed with deterministic diagnostics.
Unbounded convenience materialization returns deterministic evidence rather than invoking another
engine:

```python
materialization_report = ctx.read("data/orders.csv", schema={"id": "int64"}).select("id").to_pandas()
print(materialization_report.blocker_id)
print(materialization_report.fallback_attempted, materialization_report.external_engine_invoked)
```

## Route And Evidence Vocabulary

Read public benchmark and docs rows through these fields before comparing numbers:

| Field | Meaning |
| --- | --- |
| `route_runtime_status` | Whether scoped runtime support, smoke support, feature-gated rows, blocked, and unsupported paths are classified as route evidence or external-baseline-only evidence. |
| `claim_gate_status` | Whether that row's evidence is claim-grade for its narrow scope. It is not a production or speed claim. |
| `timing_surface` | Which total is being interpreted: `hot_runtime`, `full_replay_proof`, or `publication_proof`. |
| `performance_claim_allowed` | Must remain false unless a promoted benchmark artifact and release gate authorize a scoped claim. |
| `production_claim_allowed` | Must remain false unless a later production gate authorizes the specific workload. |
| `fallback_attempted` / `external_engine_invoked` | Must be false for ShardLoom execution evidence. |

The route labels to expect are:

- `ShardLoom Cold Certified Route`
- `ShardLoom Prepare-Once First Query`
- `ShardLoom Prepare-Once Batch`
- `ShardLoom Warm Prepared Query`
- `ShardLoom Native Vortex Query`
- `ShardLoom Direct Transient Route`
- `External Baseline End-to-End`

Hot runtime, replay proof, and publication proof are separate timing surfaces. A proof-heavy
publication row must not silently replace a hot-runtime route row.

## Architecture Map

The human-readable route map is on
[shardloom.io/compute-engine-flow](https://shardloom.io/compute-engine-flow). The canonical
Markdown source is
[docs/architecture/compute-engine-flow-reference.md](docs/architecture/compute-engine-flow-reference.md).

Useful reference anchors:

- [canonical terminology](docs/architecture/canonical-terminology.md)
- [UniversalIngress route taxonomy](docs/architecture/universal-ingress-route-taxonomy.md)
- [universal compatibility coverage scoreboard](docs/architecture/universal-compatibility-coverage-scoreboard.md)
- [v1 local output/sink scope](docs/architecture/v1-local-output-sink-scope.md)
- [user use-case atlas](docs/use-cases/README.md)
- [workflow recipes](docs/use-cases/recipes/README.md)
- [unsupported-claim rows and known paths](docs/release/known-unsupported-paths.md)

## Benchmarks

Benchmarks are evidence, not a leaderboard. ShardLoom separates:

- route lanes, which are what users compare end to end;
- timing surfaces, which say what is included in the route total;
- stage pieces, which explain where time was spent;
- claim gates, which decide what can be said publicly.

Use:

- [docs/benchmarks/local-taxonomy-benchmark.md](docs/benchmarks/local-taxonomy-benchmark.md)
- [docs/benchmarks/baseline-comparison-boundary.md](docs/benchmarks/baseline-comparison-boundary.md)
- [benchmarks/traditional_analytics/README.md](benchmarks/traditional_analytics/README.md)
- [benchmarks/clickbench/README.md](benchmarks/clickbench/README.md)
- [shardloom.io/benchmarks](https://shardloom.io/benchmarks)

ClickBench OLAP coverage is tracked as a runtime-readiness map, not a benchmark-result row:

```bash
python3 scripts/check_clickbench_olap_runtime_coverage.py
```

The generated coverage report includes route readiness, state-budget families, capillary work
units, PulseWeave pressure signals, fail-closed spill posture, and the small/medium/full fixture
strategy. The report-level `route_family_counts`, `memory_spill_diagnostic_status`, and
`site_readiness_claim_boundary` fields exist for the benchmark site and release gates; they do not
contain ClickBench timing results.

Current promoted local snapshot:

- Profile: `full_local`.
- Generated UTC: `2026-06-13T11:33:10.063090+00:00`.
- Source revision: `5743638a9225f479a0096f1c6db51a0068cac68f`.
- Published rows: `1920` normalized rows, including `600` ShardLoom `hot_runtime` rows, `600`
  ShardLoom `publication_proof` rows, and `720` external-baseline rows.
- Claim boundary: `performance_claim_allowed=false`; the run reports common-run slowdown across
  external control lanes, so it is current evidence and optimization direction, not a public
  performance-improvement claim.

Every performance claim must be backed by a reproducible benchmark artifact with workload,
hardware/runtime context, correctness evidence, timing-surface semantics, no-fallback evidence, and
claim-gate status.

## Release And CI Gates

CI is a release and trust gate. It does not publish packages, create tags, use signing keys, upload
artifacts to package channels, expand runtime support, or authorize production/performance claims.

Key gates:

- [CI gate matrix](docs/release/ci-gate-matrix.md)
- [production usability gate](docs/release/production-usability-gate.md)
- [hard release readiness gate](docs/release/hard-release-readiness-gate.md)
- [package channel readiness matrix](docs/release/package-channel-readiness-matrix.md)

The hard release gate is evidence aggregation. It must keep:

```text
public_release_claim_allowed=false
public_package_claim_allowed=false
publication_attempted=false
tag_created=false
secrets_required=false
fallback_attempted=false
external_engine_invoked=false
```

## Development

Useful local checks:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python scripts\check_ci_gate_matrix.py
python scripts\check_website_readiness.py
node website\validate_static_assets.js
git diff --check
```

Full default validation for non-trivial implementation changes remains:

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

The public website is generated from `website-src/` Astro/Starlight source and committed static
assets under `website/`. `npm run sync-content` copies the canonical compute-flow snapshot and
benchmark artifacts into the site build; repository use-case and status records remain source docs,
not generated public website browsers. Do not hand-edit generated website copies independently.

## License

ShardLoom is licensed under the Apache License 2.0. See [LICENSE](LICENSE).

# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/depsilon/shardloom?include_prereleases&label=release)](https://github.com/depsilon/shardloom/releases)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![Runtime](https://img.shields.io/badge/runtime-Vortex--native-0f766e)](#core-contract)
[![No Fallback](https://img.shields.io/badge/policy-no%20external%20fallback-991b1b)](#core-contract)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#what-makes-shardloom-different)

ShardLoom is a Vortex-first local compute engine foundation. Its public Python, SQL, and CLI front
doors lower admitted work into ShardLoom-native and Vortex-native routes, emit machine-readable
evidence about what ran, and fail closed instead of using hidden pandas, Polars, DuckDB,
DataFusion, Spark, or other execution fallback.

[shardloom.io](https://shardloom.io) is the public interpretation layer. This repository remains
the source of truth for code, architecture, release evidence, benchmark artifacts, and support
boundaries.

ShardLoom is not an official Vortex project and is not Vortex-endorsed.

## What Makes ShardLoom Different

ShardLoom's differentiators are execution and evidence contracts around Vortex-native data, not
blanket performance claims:

- **Vortex-native middle, no hidden fallback**: public compatibility workflows normalize into an
  admitted Vortex route or fail closed, native Vortex inputs stay native, and non-admitted plans
  emit deterministic diagnostics instead of running through Spark, DataFusion, DuckDB, Polars,
  pandas, or another engine.
  - Compatibility inputs are source adapters, not alternate execution engines.
  - Native/prepared Vortex routes report their route ID, feature gate, and runtime mode.
  - Direct local diagnostic paths stay internal safeguards and cannot masquerade as product
    runtime.
- **Evidence-certified routes**: every public workflow is expected to expose what actually ran:
  source admission, Vortex preparation, execution mode, output planning, certificate state,
  fallback/external-engine status, and claim posture.
  - `SourceState` records the admitted input boundary.
  - `VortexPreparedState` records the native prepared middle.
  - Route certificates connect execution evidence to output artifacts and claim gates.
- **PulseWeave**: ShardLoom's route-control vocabulary for bounded local work shaping.
  - `FlowInventory` tracks in-flight source, execution, and writer work.
  - `ScarcityLedger` records memory, decode, sink, and pressure signals.
  - `EndoPulse` applies run-local feedback without delegating to another engine.
  - `ProofBound` blocks adaptive behavior until the route has certificate evidence.
- **Capillary work units**: ingest, preparation, execution, and output work can be split into small
  typed units instead of opaque tasks.
  - Each unit carries source range, projection/filter mask, and target artifact references.
  - Each unit records materialization posture, retry/idempotency state, sink pressure, memory
    pressure, and no-fallback evidence.
  - Units can be coalesced, split, retried, reused, or audited without hiding execution
    boundaries.
- **Dynamic work shaping**: metadata, workload shape, route evidence, and measured feedback guide
  how ShardLoom sizes work.
  - Small units can be coalesced when scheduling overhead dominates.
  - Large units can be split when memory, decode, sink, or source pressure requires it.
  - Hard proof lanes remain separate from fast lanes so CI, benchmarks, and release gates stay
    evidence-preserving.
- **Metadata-first, late-materialized execution**: ShardLoom tries to answer from metadata, prune
  segments, compute over encoded Vortex data, decode only what is needed, and materialize at
  explicit output boundaries.
  - Metadata and statistics checks run before row reads where the route supports them.
  - Segment pruning and encoded kernels are preferred before decode.
  - Collect and compatibility writes report bounded decode/materialization evidence.
- **Timing-surface discipline**: hot runtime, replay proof, and publication proof are separated so
  proof-heavy evidence work does not silently become a query-runtime claim.
  - `hot_runtime` covers the query/runtime lane.
  - `full_replay_proof` covers replayable machine proof.
  - `publication_proof` covers result-sink and human evidence rendering work.
- **Patent-pending design notice**: PulseWeave, capillary work units, dynamic work shaping, and
  related route/evidence/certificate machinery include patent-pending design elements. ShardLoom
  remains distributed under Apache-2.0; this notice is informational, preserves attribution, and
  deters bad-faith copying without expanding the technical-preview support claim.

## Quick Start

Install from PyPI or Homebrew:

```sh
python -m pip install shardloom
brew install depsilon/tap/shardloom
```

Source checkout release proof is available through `python scripts/release_dry_run_proof.py --rows 64 --iterations 1`.

Normal Python use starts with `sl.context()` and `ctx.read(...)`:

```python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("orders.csv")
       .filter(sl.col("status") == "paid")
       .limit(10)
       .collect()
)

print(result.output_row_count)
print(result.first_result_row)
print(result.activation_summary.execution_mode)
print(result.fallback_attempted, result.external_engine_invoked)
```

`ctx.read(path)` infers local `.csv`, `.json`, `.jsonl`, `.ndjson`, `.parquet`, `.arrow`, `.ipc`,
`.feather`, `.avro`, `.orc`, `.vortex`, and `.vortex-manifest` adapters. Native Vortex routes can
also bind local directories of `.vortex` parts when the route requests native Vortex input.
Format-specific helpers such as
`read_csv(...)` and explicit schemas remain available for benchmark, CI, and reproducibility flows.
Normal Python contexts reuse a local ShardLoom worker transport when available, so repeated admitted
queries avoid per-call CLI process startup while preserving the same route/evidence envelopes.
SQL workflows can also bind a declared input when the query uses a logical table name:

```python
ctx.sql("SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'", input="hits.vortex").collect()
```

## Core Contract

ShardLoom's route model is:

```text
front door
-> input adapter / SourceState
-> Vortex preparation / VortexPreparedState
-> ShardLoom-native or Vortex-native execution
-> OutputPlan / SinkArtifact
-> evidence
-> claim gate
```

Compatibility formats are input/output boundaries. They are not execution fallbacks. Public local
CSV/JSONL/Parquet-style workflows prepare into Vortex or fail with deterministic diagnostics.
Native Vortex input stays native.

Every ShardLoom execution claim must preserve:

```text
fallback_attempted=false
external_engine_invoked=false
```

## Current Support Posture

ShardLoom is a technical-preview compute engine with scoped local runtime support. It does not claim
broad pandas/Polars/DataFrame parity, broad ANSI SQL compliance, production object-store or
lakehouse support, production Foundry support, Spark replacement, or public performance
superiority.
`production_claim_allowed` Must remain false unless a later production gate authorizes the specific workload.

Use these canonical references instead of reading support claims out of README prose:

| Need | Canonical source |
| --- | --- |
| Install paths | [docs/getting-started/install.md](docs/getting-started/install.md) |
| Source checkout install | [docs/getting-started/source-checkout-install.md](docs/getting-started/source-checkout-install.md) |
| Package user install | [docs/getting-started/package-user-install.md](docs/getting-started/package-user-install.md) |
| First 10 minutes | [docs/getting-started/first-10-minutes.md](docs/getting-started/first-10-minutes.md) |
| User examples | [docs/getting-started/examples.md](docs/getting-started/examples.md) |
| Certified local workload details | [docs/getting-started/certified-local-workload.md](docs/getting-started/certified-local-workload.md) |
| Troubleshooting and support bundles | [docs/getting-started/troubleshooting-support.md](docs/getting-started/troubleshooting-support.md) |
| V1 supported/unsupported surface | [docs/getting-started/v1-supported-unsupported.md](docs/getting-started/v1-supported-unsupported.md) |
| Current public support status | [docs/release/public-status-matrix.md](docs/release/public-status-matrix.md) |
| Finished product scope | [docs/release/finished-product-scope.md](docs/release/finished-product-scope.md) |
| Python, SQL, CLI, and agent-facing surfaces | [human](docs/reference/shardloom-user-surface-index.md), [agent JSON](docs/reference/shardloom-user-surface-index.json) |
| V1 front-door runtime scope | [docs/architecture/v1-front-door-runtime-scope.md](docs/architecture/v1-front-door-runtime-scope.md) |
| v1 Vortex runtime scope | [docs/architecture/v1-vortex-runtime-scope.md](docs/architecture/v1-vortex-runtime-scope.md) |
| Source/prepared-state scope | [docs/architecture/v1-source-prepared-state-scope.md](docs/architecture/v1-source-prepared-state-scope.md) |
| Local output/sink scope | [docs/architecture/v1-local-output-sink-scope.md](docs/architecture/v1-local-output-sink-scope.md) |
| Compute-flow model | [docs/architecture/compute-engine-flow-reference.md](docs/architecture/compute-engine-flow-reference.md) |
| Benchmark comparison boundary | [docs/benchmarks/baseline-comparison-boundary.md](docs/benchmarks/baseline-comparison-boundary.md) |
| Release/package channel state | [docs/release/v1-local-source-package-release.md](docs/release/v1-local-source-package-release.md) |
| Planned and completed work | [docs/architecture/phased-execution-plan.md](docs/architecture/phased-execution-plan.md) |

The user surface graduation posture is reported with the vocabulary `high_level_context`,
`client_only`, `diagnostic_only`, `feature_gated`, and `not_user_facing`. The feature-gated local Vortex
runtime and output paths include explicit evidence; `write_vortex(...)` is the highest fidelity
local sink when admitted. Benchmark scenario examples live at
`examples/local-python-benchmark-scenarios/run.py`.

Replay the local Python examples from a source checkout:

```sh
python examples/local-python-smoke/run.py --repo-root .
python examples/local-python-benchmark-scenarios/run.py --repo-root .
python examples/local-python-benchmark-scenarios/timing_review.py --repo-root .
```

The selected local/source/package v1 release track is proof-backed for package access only. GitHub pre-release, TestPyPI, PyPI, and Homebrew are published for the current technical-preview channel; that
does not authorize production, broad compatibility, or performance-superiority claims.

Prepared local workflow examples use the same Vortex-prepared middle as the route evidence:

```python
prepared = ctx.prepare_vortex(
    "target/orders.csv",
    "target/orders.vortex",
    allow_overwrite=True,
)
prepared.query("selective filter").collect()
prepared.query("clean/cast/filter/write").collect()
```

Representative evidence fields include `scenario_selective-filter_fallback_attempted` and
`materialization_report.blocker_id`.

## Benchmarks

Benchmarks are evidence, not leaderboard claims. Route lanes, timing surfaces, stage attribution,
and claim gates must be read together.

- Public site: [shardloom.io/benchmarks](https://shardloom.io/benchmarks)
- Local taxonomy: [docs/benchmarks/local-taxonomy-benchmark.md](docs/benchmarks/local-taxonomy-benchmark.md)
- ClickBench coverage map: [benchmarks/clickbench/README.md](benchmarks/clickbench/README.md)
- ClickBench 100M local UAT burndown:
  [docs/benchmarks/clickbench-100m-uat-burndown.json](docs/benchmarks/clickbench-100m-uat-burndown.json)

Check ClickBench OLAP route coverage locally:

```sh
python3 scripts/check_clickbench_olap_runtime_coverage.py
```

No performance, superiority, or Spark-replacement claim is allowed unless a promoted benchmark
artifact explicitly permits it.

## Development

Focused checks should run before broad gates. Use the focused runner for exact local checks:

```sh
python3 scripts/run_focused_checks.py --list
python3 scripts/run_focused_checks.py --profile rust-cli-bin --filter route_infers_vortex_manifest_as_native_vortex_input
python3 scripts/run_focused_checks.py --profile rust-cli-test --target public_workflow_route --filter partitioned
python3 scripts/run_focused_checks.py --profile python-unittest --filter python.tests.test_query_builder.LazyWorkflowBuilderTests.test_context_sql_vortex_manifest_source_binds_native_vortex_collect
```

For Rust unit filters, target the exact crate surface: `--bin <name>` for binary crates and
`--lib` for library crates. For integration filters, use
`cargo test -p <crate> --test <target> <filter>`. Avoid bare package-level filters for focused
checks because Cargo still enumerates every integration target.

The full workspace gate for substantial implementation work is:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Useful targeted checks:

```sh
python3 scripts/check_workspace_version_sources.py
python3 scripts/check_v1_local_source_package_release.py
python3 scripts/check_website_readiness.py
```

The website is generated from `website-src/`; do not hand-edit generated website output
independently.

## Release Notes

The latest published technical-preview package is proof-backed through GitHub release assets, PyPI,
TestPyPI, and Homebrew channel transcripts under `docs/release/channel-proofs/`.

For the next release train, `v0.2.0` should use a signed annotated Git tag so GitHub can show a
verified tag badge when the local maintainer signing key is configured:

```sh
git tag -s v0.2.0 -m "ShardLoom v0.2.0"
git tag -v v0.2.0
```

If signing is unavailable, stop before publishing and fix the maintainer signing setup rather than
creating a lightweight or unsigned replacement tag.

## License

ShardLoom is licensed under the Apache License 2.0. See [LICENSE](LICENSE).

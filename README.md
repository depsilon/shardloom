# ShardLoom

[![CI](https://github.com/depsilon/shardloom/actions/workflows/ci.yml/badge.svg)](https://github.com/depsilon/shardloom/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/depsilon/shardloom?include_prereleases&label=release)](https://github.com/depsilon/shardloom/releases)
[![PyPI](https://img.shields.io/pypi/v/shardloom?label=PyPI)](https://pypi.org/project/shardloom/)
[![Homebrew](https://img.shields.io/badge/Homebrew-depsilon%2Ftap%2Fshardloom-2f4f4f)](https://github.com/depsilon/homebrew-tap)
[![Runtime](https://img.shields.io/badge/runtime-Vortex--native-0f766e)](#core-contract)
[![No Fallback](https://img.shields.io/badge/policy-no%20external%20fallback-991b1b)](#core-contract)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Patent Pending](https://img.shields.io/badge/patent--pending-designs-7c3aed)](#what-makes-shardloom-different)

ShardLoom is a local encoded-columnar compute engine built around Vortex. Its public Python, SQL, and CLI front
doors lower admitted work into ShardLoom-native and Vortex-native routes, emit machine-readable
evidence about what ran, and fail closed instead of using hidden pandas, Polars, DuckDB,
DataFusion, Spark, or other execution fallback.

[shardloom.io](https://shardloom.io) is the public interpretation layer. This repository remains
the source of truth for code, architecture, release evidence, benchmark artifacts, and support
boundaries.

ShardLoom is not an official Vortex project and is not Vortex-endorsed.

## What Makes ShardLoom Different

ShardLoom brings compressed execution, reusable local sessions, owned native results, and
inspectable resource decisions into one Vortex-native workflow. The capabilities below apply to
admitted local routes; their linked evidence defines the supported shapes and remaining limits.

- **One native execution contract across Python, SQL, and CLI.** Compatibility inputs enter through
  source adapters and Vortex preparation; native Vortex inputs stay native.
  Unsupported work must emit deterministic diagnostics with no hidden external-engine execution.
  `SourceState` and `VortexPreparedState` make the input and preparation boundaries inspectable.
  See the [front-door contract](docs/architecture/v1-front-door-runtime-scope.md).
- **Avoid data work before adding compute.** Supported routes answer from exact metadata, prune
  segments, consume encoded values, and defer payload materialization until the result needs it.
  Constant and run-end numeric reductions can work on values and repetition counts; bounded
  top-N projections retain row references and order keys before fetching final payloads.
  Runtime evidence distinguishes native dictionary access, dictionaries built from decoded UTF-8,
  typed numeric decode, and materialized access. See the
  [encoded numeric consumers](docs/architecture/perf-encoded-numeric-reductions-2026-09-06.md) and
  [runtime scope](docs/architecture/v1-vortex-runtime-scope.md).
- **Exact aggregation that uses repetition and delays expensive measures.** Admitted kernels
  aggregate dictionary codes and weighted values, fuse repeated numeric SUM/AVG expressions,
  and preserve exact DISTINCT and complete grouping-key equality. Selected grouped top-K routes
  finish from complete exact key partitions or identify candidates before exact recount or late
  measure evaluation; ordering, ties, NULLs, and floating accumulation retain their route's declared
  semantics. See the [filtered-count and derived-key evidence](docs/architecture/performance-ship-drop-2026-09-19.md)
  and [complete triple-key count partitions](docs/architecture/q19-complete-key-partitions-2026-09-19.md).
  Near-unique integer pairs can use [exact partition sorting and reduction](docs/architecture/q33-exact-partition-reduction-2026-09-19.md)
  to avoid a nearly one-entry-per-row hash directory before evaluating retained measures.
  The [performance plan](docs/architecture/phased-execution-plan.md) records both retained
  implementations and experiments that did not earn retention.
- **Reusable structure stays with the data.** Prepared local OLAP workflows use a single `.vortex`
  artifact containing data, native layouts, statistics, and admitted derived metadata. Consumers
  can reuse embedded string-length or time-bucket helpers without exposing implementation fields
  in ordinary `select *` output. Vortex remains the highest-fidelity persistence target;
  compatibility export reports its own fidelity and materialization boundary.
  See the [source/prepared-state scope](docs/architecture/v1-source-prepared-state-scope.md).
- **Prepare once; execute each call with fresh state.** Resident sessions retain source handles,
  generation identity, and prepared lowering for supported operations. Python contexts can reuse
  a local worker to avoid per-call process startup. Ordinary native aggregates retain the same
  lowering across text, numeric and nullable schemas, derived keys, transformed measures, and wide
  measure sets. Explicit COUNT/DISTINCT spill reuses the source and creates fresh run state on each
  call. Source-generation checks reject detected replacement or mutation. See the
  [runtime completion scope and evidence](docs/architecture/native-runtime-completion-2026-09-20.md).
- **Results remain executable native data.** Admitted source and computed aggregate results own
  Vortex arrays, validity, and memory credits. Supported owned COUNT and grouped DISTINCT results
  can reach native Vortex, Arrow IPC, or Parquet sinks without a row/JSON reconstruction roundtrip;
  owned payloads can outlive the input source. Bounded Rust workflows can pass owned arrays
  directly into the existing prepared aggregate family without serializing an intermediate file.
  See the
  [result ownership contract](docs/reference/resident-native-results.md) and
  [local sink scope](docs/architecture/v1-local-output-sink-scope.md).
- **Resource ownership follows the work.** Shared workers, bounded queues, reservations, and
  cancellation cleanup govern admitted native operations. An explicit resident serving policy
  bounds concurrent calls, CPU grants and positional I/O, with a reserved metadata lane when
  enabled. See the [serving contract and bounded load evidence](docs/architecture/concurrent-native-serving-2026-09-20.md).
  The non-null UTF8 COUNT worker path
  can transfer committed state into native temporary runs under memory pressure. Spill support
  remains operator-specific, and reservations do not cover every provider allocation or establish
  a process RSS ceiling. See the [resource contract](docs/rfcs/0044-resident-runtime-resource-ownership.md)
  and [implemented spill boundary](docs/benchmarks/native-completion-boundaries-2026-09-12.md).
- **PulseWeave and capillary work units make control decisions inspectable.** Typed units carry
  source ranges, projection/filter and artifact references, ownership, and execution evidence.
  PulseWeave combines `FlowInventory`, `ScarcityLedger`, `EndoPulse`, and `ProofBound` to describe
  bounded work, pressure, feedback, and permission to apply a policy. Dynamic work shaping is
  scoped to admitted preparation and native batch routes; a policy report alone does not prove
  that execution changed. Broader topology/coalescing experiments remain parked after regressions.
  See the [control model](docs/architecture/pulseweave-runtime-control.md) and
  [current execution priorities](docs/architecture/phased-execution-plan.md#planned).
- **Evidence that developers and agents can inspect.** Route certificates connect source
  admission, provider/feature selection, execution, and output artifacts. Structured diagnostics
  explain rejected work and expose `fallback_attempted=false` and
  `external_engine_invoked=false`. The
  [user-surface index](docs/reference/shardloom-user-surface-index.md) provides a shared entry point
  for humans and automation. `hot_runtime`, `full_replay_proof`, and `publication_proof` name
  separate timing surfaces; benchmarks must also state whether startup, transport, and complete
  output are included.

These are technical-preview capabilities and design contracts, not a claim of performance
superiority or complete production support. Consult the
[public support matrix](docs/release/public-status-matrix.md) for release scope and the
[canonical terminology](docs/architecture/canonical-terminology.md) for deeper definitions.

**Patent-pending design notice:** PulseWeave, capillary work units, dynamic work shaping, and
related route/evidence/certificate machinery include patent-pending design elements. ShardLoom
remains distributed under Apache-2.0; this informational notice does not expand its support claims.

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
Set `SHARDLOOM_PERSISTENT_WORKER=0` only when you need one-shot subprocess diagnostics.
Public local workflows default to `SHARDLOOM_MAX_PARALLELISM=2` and `SHARDLOOM_MEMORY_GB=4`; set
those environment variables or pass explicit `max_parallelism` / `memory_gb` values when a larger
local resource envelope is appropriate.
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

ShardLoom is a technical-preview compute engine with a globally reusable local Vortex runtime for
admitted operations. It does not claim broad pandas/Polars/DataFrame parity, broad ANSI SQL
compliance, production object-store or lakehouse support, production Foundry support, Spark
replacement, or public performance superiority.
`production_claim_allowed`: Must remain false unless a later production gate authorizes the specific workload.

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

- Public comparison destination: [ClickBench](https://benchmark.clickhouse.com/)
- Local taxonomy: [docs/benchmarks/local-taxonomy-benchmark.md](docs/benchmarks/local-taxonomy-benchmark.md)
- ClickBench coverage map: [benchmarks/clickbench/README.md](benchmarks/clickbench/README.md)
- ClickBench 100M local UAT burndown:
  [docs/benchmarks/clickbench-100m-uat-burndown.json](docs/benchmarks/clickbench-100m-uat-burndown.json)
- Source-bound local correctness and timing evidence:
  [combined performance UAT](docs/benchmarks/combined-performance-uat-2026-09-12.md)
- Current profiling hypotheses and material ship/drop gates:
  [performance research](docs/architecture/performance-domain-transfer-2026-09-19.md)

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

Published technical-preview packages are proof-backed through GitHub release assets, PyPI,
TestPyPI, and Homebrew channel transcripts under `docs/release/channel-proofs/`.

Release channel proof records the tag ref type, target commit, and commit verification state.
Release trains should prefer a signed annotated tag when a maintainer signing key is configured;
until then, tags should point at GitHub-verified merge commits.

## License

ShardLoom is licensed under the Apache License 2.0. See [LICENSE](LICENSE).

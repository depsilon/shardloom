# ShardLoom

[shardloom.io](https://shardloom.io) is the public, claim-safe interpretation layer for the
project. The repository is the source of truth for code, architecture docs, phase plans, use cases,
benchmarks, and release evidence.

ShardLoom is a pre-release, Vortex-first, no-fallback local compute engine foundation. It is being
built around explicit routes, deterministic blockers, and evidence fields that show what ran:
source admission, Vortex preparation, execution mode, output planning, certificates, fallback
status, and claim gate status.

ShardLoom is not an official Vortex project and is not Vortex-endorsed. It does not claim
production readiness, public performance superiority, Apache Spark replacement, production
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

## What Is Usable Today

Current runtime support is intentionally scoped and evidence-gated:

- local first-10-minutes smoke and release dry-run workflows;
- Python and CLI front doors for selected local CSV, JSONL/NDJSON, flat JSON, generated-source, and
  feature-gated Parquet/Arrow IPC/Avro/ORC and Vortex smokes;
- scoped SQL local-source execution for projection, filter, limit, scalar aggregates, one-column
  group-by, single-key top-N, selected casts/date/string/IN predicates, and one explicit local CSV
  equi-join shape;
- source-free generated local outputs through user rows, ranges, sequences, calendars, SQL `VALUES`,
  literal `SELECT`, and `generate_series`/`range` smokes, including feature-gated local Vortex
  output;
- scoped local-source output/fanout to JSONL/CSV, feature-gated Parquet/Arrow IPC/Avro/ORC, and
  feature-gated local Vortex sinks with local replay/fidelity evidence;
- local Vortex/prepared-native benchmark evidence for selected traditional analytics scenarios;
- feature-gated local `vortex_ingest` smoke that prepares admitted flat scalar local sources into a
  local `.vortex` artifact and emits `VortexPreparedState` evidence with explicit
  `ingest_minimal` / `ingest_certified` certification-depth semantics;
- report-only or blocked status for broader SQL/DataFrame, object-store, lakehouse/table,
  distributed, live/hybrid production, Foundry production, and package-publication claims.

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
$env:PYTHONPATH = "python\src"
python examples\local-python-smoke\run.py --repo-root .
```

Exact smoke commands, feature flags, expected outputs, and claim boundaries live in the linked
getting-started docs.

## Evidence And Benchmarks

Benchmarks are evidence, not a leaderboard. ShardLoom separates certified cold route timing from
prepared warm route timing and keeps external engines labeled as baseline context only.

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
- package-channel evidence is still gated by
  [`docs/release/package-channel-readiness-matrix.md`](docs/release/package-channel-readiness-matrix.md);
- Foundry docs describe local/dev-stack proof boundaries, not production Foundry support.

## Development

Useful checks while working locally:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
python website\build_static_pages.py
python scripts\check_website_readiness.py
node website\validate_static_assets.js
git diff --check
```

The public website is generated from `website/build_static_pages.py` and committed static assets.
It should stay compact, light-mode, benchmark/compute-flow/repo centered, and claim-safe.

## License

ShardLoom is licensed under the Apache License 2.0. See [`LICENSE`](LICENSE).

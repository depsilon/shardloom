# ShardLoom

Website: [shardloom.io](https://shardloom.io) (pre-release, claim-safe static site deployed from
`website/`).

Public technical-preview posture: ShardLoom is a pre-release, Vortex-first, no-fallback local
compute engine foundation with evidence-certified local execution slices. It is an independent
downstream workflow layer over Vortex-native data, not an official Vortex project and not
Vortex-endorsed. It does not claim production platform readiness, public performance superiority,
Apache Spark substitution, production SQL/DataFrame support, production object-store/lakehouse
support, production Foundry support, package publication readiness, or Foundry vendor endorsement.

The website includes rendered snapshots of this README, the compute-engine flow reference, the Use
Case Atlas, and the current prepared/native benchmark evidence. Those pages are documentation and
evidence surfaces, not performance, Apache Spark substitute, production SQL/DataFrame, object-store,
lakehouse, or Foundry claims.

ShardLoom is a standalone encoded-columnar execution engine for computing directly over
Vortex-native layouts, preserving encoded representation where possible, and producing Vortex-native
or lakehouse-compatible outputs without delegating execution to Spark, DataFusion, DuckDB, Polars,
Velox, or another fallback engine.

"Standalone" means standalone from external query-engine fallback, not isolated
from Vortex itself. ShardLoom may use upstream Vortex array, compute, scan,
source, and sink APIs as native execution providers when they are admitted
through ShardLoom policy and reported through ShardLoom certificates. That is
distinct from delegating unsupported work to DataFusion, DuckDB, Spark, Polars,
Velox, or Vortex query-engine integrations.

Longer term, ShardLoom is being shaped into a certified data execution fabric: a Vortex-native
engine with user-friendly Python, SQL, DataFrame, ETL, adapter, live/hybrid, and remote API surfaces
that all preserve the same no-fallback evidence model.

## Mission

Compute less. Decode later. Materialize only at explicit boundaries.

## What ShardLoom Optimizes For

- Vortex-native input and output
- encoded-columnar execution
- metadata-first and segment-pruned planning
- late materialization with explicit materialization boundaries
- deterministic diagnostics for unsupported work
- object-store-aware planning and commit semantics
- correctness evidence, execution certificates, and benchmark-gated claims
- common analytical capability through SQL, Python/API, UDF, adapter, ETL, and unstructured/media
  surfaces over time
- batch, live, and hybrid engine modes only after they are ShardLoom-native and
  certificate-backed
- REST/event/remote APIs as proof and orchestration surfaces, not hidden execution delegation

## Scope

ShardLoom is being built as a certified data execution engine, not a wrapper around incumbent query
engines. New surfaces count as supported only when their execution path or explicit
materialization/source/sink boundary has diagnostics, certificates, correctness evidence, and
benchmark or workload evidence for the declared workload.

The intended public shape includes Python, SQL/DataFrame-style workflows, ETL, adapters, batch
engine execution, live/hybrid engine-mode contracts, remote proof/control APIs, and optional
platform integrations. Those surfaces are evidence-gated; unsupported work must remain explicit and
non-delegating.

## Runtime Snapshot

ShardLoom tracks two different mode families. Execution modes describe the source/preparation lane;
engine modes describe workload semantics such as bounded batch, live change streams, or
base-plus-delta hybrid overlays.

User-facing APIs are front doors, not execution routes. A user may enter through Python, SQL, CLI,
or a future DataFrame-style surface, but ShardLoom still records the same route model:

```text
front door -> source route -> preparation route -> execution route -> output route -> evidence route
```

That is why public docs use friendly labels beside the canonical fields: certified import/stage
route (`compatibility_import_certified`), prepared Vortex steady-state route (`prepared_vortex`),
already-Vortex route (`native_vortex`), direct one-shot route
(`direct_compatibility_transient`), source-free generated-output route, and multi-output fanout
route.

| Area | Current repo state | Planned or gated updates |
| --- | --- | --- |
| Execution modes | `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, `direct_compatibility_transient`, and `auto` are represented in benchmark/report fields. | More prepared/native Vortex paths; `auto` must keep reporting the selected concrete mode. |
| Batch engine mode | Current practical foundation for bounded local Vortex analytics and benchmark evidence. | Broader operator coverage, source/sink certification, and claim-grade workload evidence. |
| Live engine mode | `engine-selection-plan`, `engine-capability-matrix`, `live-change-contract-plan`, Python helpers, and scoped in-memory `live-fixture-run` reports exist. | Durable state/checkpoints, broker/source adapters, freshness evidence, and workload certification. |
| Hybrid engine mode | `engine-selection-plan`, `engine-capability-matrix`, Python helpers, and scoped in-memory `hybrid-overlay-run` reports exist. | Durable micro-segment flush, object-store/table commit, catalog snapshot discovery, and hot/cold benchmark evidence. |
| Streaming/zero-copy/backpressure | `streaming-plan`, `streaming-batch-plan`, `backpressure-plan`, `engine-capability-matrix`, and `capabilities engines` expose a GAR-0013 matrix for local fixture evidence, object-store streaming read blockers, zero-decode, zero-copy/materialization boundaries, bounded backpressure, and live/hybrid broker runtime blockers. | Object-store streaming reads, durable broker adapters, runtime backpressure enforcement, broader operator/source/sink evidence, and claim-grade workload certification. |
| Expression/operator semantics | `shardloom-core` now has a scoped native semantics baseline for literals, column references, aliases, casts, boolean logic, null predicates, comparisons, projection, filter selection, and limit row-count behavior over in-memory rows, with deterministic blockers and `fallback_attempted=false`. | Wire the shared baseline into SQL local-source execution, Python query-builder workflows, prepared/native operator promotion, and broader operator families only with source/sink evidence. |
| SQL local-source smoke | `sql-local-source-smoke` admits scoped local CSV/JSONL projection/optional-filter/limit, scalar aggregate, one-column group-by aggregate, single-key numeric `ORDER BY ... LIMIT ...` top-N, scoped `CAST(column AS dtype)` predicates for `int64`, `float64`, `utf8`, `boolean`, and `date32`, and bounded `column IN (<literal>,...)` predicates over local rows, plus one explicit local CSV inner equi-join shape with aliases. The aggregate slices cover `COUNT(*)`, `COUNT(column)`, `SUM`, `AVG`, `MIN`, and `MAX` across all rows or after a scoped `WHERE` predicate; group-by currently requires the selected group column to exactly match the `GROUP BY` column. The top-N slice admits one numeric non-null sort key with `ASC` or `DESC`; null ordering, expressions, collation, and multi-key sorts are deterministic blockers. The `IN` predicate slice admits up to 32 non-null literals of one scalar family, including `DATE 'YYYY-MM-DD'` lists, and emits `in_predicate_runtime_execution` plus `in_list_value_count`; empty, NULL, mixed DATE/non-DATE, and oversized lists are deterministic blockers. The join slice admits `FROM '<left.csv>' AS f [INNER] JOIN '<right.csv>' AS d ON f.key = d.key [WHERE <qualified predicate>] LIMIT <n>` and emits left/right source, key, row-count, matched-row, output-row, memory-estimate, execution-certificate, no-fallback, and claim-gate evidence. It parses, binds, plans, reads local rows, applies ShardLoom-owned projection/optional-filter/limit, cast/IN predicate, aggregate, top-N, or join semantics, prints bounded inline JSONL or writes optional local JSONL/CSV output with format-specific sink certificate fields, and emits source, output, execution, materialization/decode, no-fallback, and claim-gate evidence. Python wraps the scoped CSV and flat JSONL/NDJSON projection, scalar aggregate, one-column group-by, preview/select-star, and single-key numeric top-N paths, including local JSONL writes and `write_csv(...)` for the scoped local CSV sink; the lower-level Python client exposes typed IN/join evidence for direct `sql_local_source_smoke(...)` calls. | Broader SQL over Parquet/Vortex, Python/DataFrame joins, Parquet/Arrow/Avro/ORC/Vortex output sinks, multi-output fanout, multi-key/expression/outer/semi/anti/cross joins, multi-key/grouped aggregate generality, named grouped aggregate aliases, functions, subqueries, catalogs, object-store/table sources, collation/null-ordering parity, window ranking, and production SQL/DataFrame support remain gated. |
| Source-free generated output | Scoped local JSONL/CSV smokes exist for caller-provided rows through `generated-source-user-rows-smoke` / `ctx.from_rows([...]).write(...)`, Python literal/calendar helpers through `ctx.literal_table([...]).write(...)` and `ctx.calendar(...).write(...)`, ShardLoom-native range/sequence generators through `generated-source-range-smoke` / `ctx.range(...).write(...)` and `generated-source-sequence-smoke` / `ctx.sequence(...).write(...)`, and source-free SQL `VALUES`, literal `SELECT`, and `SELECT * FROM generate_series/range(...)` through `generated-source-sql-smoke` / `ctx.sql_values(...).write(...)` / `ctx.sql_literal_select(...).write(...)` / `ctx.sql(...).write(...)`, with generated-source and output evidence and no source Native I/O certificate. | Broad SQL/DataFrame runtime, object-store writes, and Foundry generated-output runtime remain gated. |
| Foundry-style starter | `docs/foundry/dev-stack-starter-kit.md` gives a local-only dev-stack path for CLI/package resolution, staged input posture, generated-output blockers, and local certificate-style evidence. | Real Foundry runtime, Foundry compute/Spark, Foundry output APIs, result/evidence datasets, Marketplace/package proof, and production Foundry claims remain gated. |
| Prepared/native Vortex runtime | Scoped residual-native paths now avoid full fact-table materialization for selected local benchmark scenarios, including local distinct-count, null-heavy aggregate, clean/cast/filter/write, malformed timestamp / dirty CSV, nested JSON field scan, CDC-overlay small change over large base, global sort/top-k, and partition-pruning/date-range scan evidence. Prepared/native rows also emit explicit `source_backed_scan_*` fields for source roles, projected columns, residual executor, Native I/O certificate status, materialization boundary, and no-fallback evidence. The `selective filter` row now emits `encoded_predicate_provider_*` v4 fields: it records projected chunks such as `metric:vortex.filter`, separately probes real `flag,value` reader chunks without decode/materialization, lowers observed `flag:fastlanes.bitpacked` and `value:fastlanes.bitpacked` chunks into admitted reader-generated encoded kernel inputs, intersects their selection vectors, and consumes the admitted selection vector for scoped metric `row_count`/`metric_sum` evidence. The row remains residual-native because selected metric aggregation is still scoped ShardLoom-native residual logic, not a generalized encoded aggregation kernel. CPU specialization reporting records side-effect-free host feature probes and a blocked filter/encoded vector-kernel admission diagnostic. | Next planned work is generalized encoded/native operator coverage, prepared/native batch harness integration, and claim-grade correctness/benchmark gates; encoded-native, SIMD dispatch, and performance claims remain evidence-gated. |

## Current State

ShardLoom is pre-release. The current repository is strongest as an evidence-first Vortex-native
engine prototype plus protocol/benchmark scaffolding, not as a general SQL/DataFrame/REST platform
yet.

The current `local_vortex_analytics_v1` path is now a credible local workload-certified
compute-engine slice: it can import local compatibility data into Vortex artifacts, execute the
supported local analytics path, write a Vortex result artifact, replay source and result artifacts,
emit execution and Native I/O certificates, record scheduler/memory evidence, and preserve
no-fallback policy fields. That certification is intentionally scoped to the named local workload;
it is not a broad SQL engine, production platform, Apache Spark substitute, or general DataFrame
runtime claim.

Currently wired surfaces include:

- typed CLI JSON envelopes and Python parsing for result refs, artifact refs, certificates,
  diagnostics, lifecycle, policy, and fallback status
- side-effect-free capability, semantic, workflow, and evidence reports for current and blocked
  surfaces
- local Vortex traditional-analytics execution with optional source replay, result-sink replay,
  scheduler/memory evidence, execution certificates, Native I/O certificates, and no-fallback fields
- explicit execution-mode evidence for `compatibility_import_certified`, `prepared_vortex`,
  `native_vortex`, `direct_compatibility_transient`, and `auto`
- scoped source-free user-row, literal-table, calendar/date-dimension, range, sequence, SQL
  `VALUES`, SQL literal `SELECT`, and SQL `generate_series`/`range` generated output to
  local JSONL/CSV with generated-source certificate status, output Native I/O certificate status, output
  digest, and no-fallback/no-external-engine evidence
- scoped local CSV/JSONL SQL execution through `sql-local-source-smoke` for projection/optional-filter/limit,
  scalar aggregate, one-column group-by aggregate, single-key numeric `ORDER BY ... LIMIT ...`
  top-N, scoped `CAST(column AS dtype)` predicates for `int64`, `float64`, `utf8`, `boolean`, and
  `date32`, and one explicit
  local CSV inner equi-join shape, with parser, binder, planner, local
  source-read, ShardLoom-native projection/optional-filter/limit or `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` or top-N
  or hash-join semantics, bounded inline JSONL result evidence, optional local JSONL or CSV output
  evidence with format-specific sink certificate refs, materialization/decode fields, and no-fallback/no-external-engine evidence; Python
  query-builder
  `ctx.read_csv(...).select(...).limit().collect()/write(...)` with optional `filter(...)`,
  `ctx.read_csv(...).preview()`, and
  `ctx.read_json(...).select(...).limit().collect()/write(...)` with optional `filter(...)` for local flat
  `.jsonl`/`.ndjson` sources, plus CSV or flat JSONL/NDJSON
  optional-filter `aggregate(...).limit(1).collect()/write(...)`,
  optional-filter `group_by(...).agg(...).limit(n).collect()/write(...)`, and
  `select(...).sort(...).limit(n).collect()/write(...)` with optional `filter(...)` wrap these same scoped
  projection/scalar/grouped/top-N paths; direct Python client calls can inspect typed
  join evidence from the CLI join smoke. This is not broad SQL/DataFrame runtime, Python/DataFrame
  join support, nested JSON runtime, generalized join/grouped aggregate/ordering support, a SQL
  compatibility claim, object-store/table support, or a production claim
- scoped prepared/native Vortex query paths for `selective filter`, `wide projection`,
  `filter + projection + limit`, `group by aggregation`, `multi-key group by`, `hash join`, and
  `join + aggregate`, `sort and top-k`, `top-N per group`, `row number window`, and
  `high-cardinality string group/distinct`, `distinct count`, `null-heavy aggregate`,
  `clean/cast/filter/write`, `malformed timestamp / dirty CSV`, `nested JSON field scan`,
  `small change over large base`, plus scoped local `partition pruning` date-range scans;
  these avoid full fact-table materialization for the prepared/native row while remaining
  residual-native, not encoded-native operator claims; prepared/native rows expose
  `source_backed_scan_*` evidence for source role, projected-column, residual, materialization, and
  no-fallback review; the `selective filter` row also exposes
  `encoded_predicate_provider_*` fields that record projected reader chunks such as
  `metric:vortex.filter` when a filtered scan emits rows, separately probe real filter-only
  `flag,value` chunks without decode/materialization, identify the reader-generated conjunctive
  selection-vector bridge contract, lower observed `flag:fastlanes.bitpacked` /
  `value:fastlanes.bitpacked` chunks into admitted kernel inputs, consume the admitted selection vector
  for scoped metric `row_count`/`metric_sum` evidence, and keep encoded-native claims blocked until
  generalized encoded aggregation has correctness, certificate, benchmark, and no-fallback evidence
- a scoped `traditional-analytics-vortex-batch-run` command that runs multiple prepared/native
  Vortex scenarios in one ShardLoom process while preserving per-scenario typed evidence,
  Native I/O/operator blocker fields, explicit
  `evidence_level=minimal_runtime|certified|full_replay`, and `fallback_attempted=false` /
  `external_engine_invoked=false`; `minimal_runtime` remains `not_claim_grade`, `certified` emits
  normal certificates without replay by default, and `full_replay` requires result-sink replay
  proof. This is process reuse and proof-depth attribution for local prepared/native evidence, not
  a persistent service, hidden fast mode, performance claim, SQL/DataFrame claim, object-store
  claim, or Apache Spark substitute claim
- batch/live/hybrid engine-mode contracts through `engine-selection-plan`,
  `engine-capability-matrix`, `live-change-contract-plan`, Python context helpers, and scoped
  in-memory `live-fixture-run` / `hybrid-overlay-run` fixture reports
- streaming/zero-copy/backpressure capability diagnostics through `streaming-plan`,
  `streaming-batch-plan`, `backpressure-plan`, `engine-capability-matrix`, `capabilities engines`,
  and Python `EngineCapabilityMatrix` accessors; object-store streaming reads and broker-backed
  live/hybrid runtime are blocked/report-only, not claim-grade runtime support
- side-effect-free CPU specialization diagnostics through `cpu-specialization-plan`, including host
  CPU feature labels and a blocked filter/encoded vector-kernel admission status with no runtime
  dispatch
- a predicate/DType coverage table in `compute-capability-matrix` for predicate, DType,
  null-semantics, nested-shape, and statistics families, with support status, evidence gaps,
  deterministic unsupported diagnostics, and no-fallback/no-external-engine fields
- an object-store byte-range provider gate in `object-store-request-plan` and
  `cg10-object-store-runtime-gate` that names credential, retry, idempotency, Native I/O,
  execution-certificate, and benchmark evidence before any future object-store read can run
- an object-store runtime blocker matrix for coordinator start, worker start, task execution,
  checkpoint writes, retry attempts, cleanup execution, and commit-record writes
- a local benchmark harness with taxonomy metadata, separate timing and coverage tables,
  reproducibility checks, local optional baselines, and explicit unsupported/blocked rows
- a static website under `website/` that renders the root README, the compute-engine flow reference,
  and a claim-safe prepared/native benchmark evidence snapshot for Cloudflare Workers Static Assets
- Vortex-first architecture docs and guardrails for treating upstream Vortex APIs as native providers
  only when they are policy-admitted and certificate-backed

Some broad surfaces are intentionally report-only or unsupported in this pre-release repository. See
the phase plan and completed ledger for exact implementation status.

For a non-expert "Can ShardLoom do my thing?" map, start with
[`docs/use-cases/README.md`](docs/use-cases/README.md) or the website Use Case Atlas at
[shardloom.io/use-cases](https://shardloom.io/use-cases/).
Practical copyable recipes live in
[`docs/use-cases/recipes/README.md`](docs/use-cases/recipes/README.md) and are indexed by
[`docs/use-cases/recipes/recipe-index.json`](docs/use-cases/recipes/recipe-index.json).

## Modes At A Glance

ShardLoom currently uses two separate mode vocabularies:

| Vocabulary | Values | What it answers | Current posture |
| --- | --- | --- | --- |
| Execution mode | `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, `direct_compatibility_transient`, `auto` | How the request crosses source/preparation boundaries and how timings should be interpreted. | Current benchmark/report fields; prepared/native work is being expanded scenario by scenario. |
| Engine mode | `batch`, `live`, `hybrid`, `auto` | What workload semantics the plan has: bounded batch, live change stream, or base-plus-delta hybrid state. | Contract and fixture evidence exists; production live/hybrid claims remain not claim-grade. |

`batch` is the current practical foundation for local Vortex analytics. `live` and `hybrid` are
represented by selection reports, capability matrices, Python context helpers, live-change
contracts, and scoped in-memory fixture reports with certificate fields and
`fallback_attempted=false`. They are not yet broker-backed, object-store-backed, or production
live/hybrid engines.

Streaming-related surfaces are currently explicit capability and planning reports. The GAR-0013
matrix marks local streaming and zero-decode rows as scoped fixture-smoke/report-only evidence,
compatibility zero-copy boundaries as materializing/copying, and object-store streaming plus
broker-backed live/hybrid runtime as blocked with deterministic diagnostics. These reports do not
create a broad streaming runtime claim.

The next prepared/native and runtime work remains intentionally concrete and phase-plan driven:
encoded/provider promotion for narrow paths, facade coverage, and evidence hardening, with
encoded-native, SIMD dispatch, production, SQL/DataFrame, object-store, and performance claims still
gated by workload evidence.

## Core Concepts

The full vocabulary lives in
[`docs/architecture/canonical-terminology.md`](docs/architecture/canonical-terminology.md). These
terms are the shortest orientation path:

- **native Vortex input/output**: Vortex is the highest-fidelity input and persistence target.
- **Vortex-native execution provider**: upstream Vortex or ShardLoom-owned Vortex-aware compute
  admitted through ShardLoom policy and certificate evidence; this is native execution, not external
  fallback.
- **encoded-columnar execution**: operators should preserve encoded representation when capability
  evidence allows it.
- **metadata-first planning**: answer or prune from metadata before reading data bytes.
- **zero-decode**: execute over encoded Vortex representation without decoding values.
- **late materialization**: concrete rows/columns appear only at explicit materialization
  boundaries.
- **compatibility output**: Parquet, Arrow IPC, Iceberg-compatible, Delta-compatible, JSONL, and CSV
  are translation/export targets, not fallback execution engines.
- **no-fallback execution**: unsupported plans fail with deterministic diagnostics instead of
  delegating runtime work to another engine.
- **capability certification**: SQL, operator, function, adapter, Python/API, ETL, and
  unstructured/media support must be evidence-backed before claims are made.

## Project Status

Active implementation state is tracked in
[`docs/architecture/phased-execution-plan.md`](docs/architecture/phased-execution-plan.md).
Completed implementation history lives in
[`docs/architecture/phased-execution-completed-ledger.md`](docs/architecture/phased-execution-completed-ledger.md).

This README intentionally does not duplicate active status checklists, phase references, or completed
session history. Supporting RFCs live in [`docs/rfcs`](docs/rfcs), and phase/RFC mapping lives in
[`docs/architecture/rfc-phase-traceability.md`](docs/architecture/rfc-phase-traceability.md).

External systems lessons and technique-transfer notes live in
[`docs/architecture/systems-learning-map.md`](docs/architecture/systems-learning-map.md). RFCs own
deep contracts and acceptance criteria.

First-user docs:

- [`docs/getting-started/install.md`](docs/getting-started/install.md)
- [`docs/getting-started/first-10-minutes.md`](docs/getting-started/first-10-minutes.md) includes
  the one-command local release dry run: local wheel install in a clean environment, Python/CLI
  smoke checks, generated-source local output smokes, and a compatibility/prepared-Vortex benchmark
  smoke without publishing packages.
- [`docs/getting-started/examples.md`](docs/getting-started/examples.md)
- [`docs/getting-started/certified-local-workload.md`](docs/getting-started/certified-local-workload.md)
- [`docs/benchmarks/local-taxonomy-benchmark.md`](docs/benchmarks/local-taxonomy-benchmark.md)
- [`docs/benchmarks/baseline-comparison-boundary.md`](docs/benchmarks/baseline-comparison-boundary.md)
- [`docs/architecture/compute-engine-flow-reference.md`](docs/architecture/compute-engine-flow-reference.md)
- [`docs/release/package-channel-readiness-matrix.md`](docs/release/package-channel-readiness-matrix.md)
  shows why public package channels remain blocked until channel-specific install, smoke,
  provenance, and rollback evidence exists.

## No-Fallback Policy

ShardLoom must fail explicitly for unsupported execution paths. Spark, DataFusion, DuckDB, Polars,
Velox, and similar engines may be used only as conceptual references, migration baselines,
correctness oracles, or benchmark baselines. They must not execute ShardLoom runtime paths as
fallback engines.

## Engineering Principles

Engineering work is organized around Vortex-native execution, correctness fixtures, deterministic
unsupported diagnostics, execution certificates, reproducibility evidence, and explicit
materialization/source/sink boundaries. Performance, superiority, or best-default-engine claims
require correctness and benchmark evidence before publication.

## Python Client

The source-tree Python client in [`python/`](python/) is a thin wrapper over the
ShardLoom CLI JSON protocol. It parses `shardloom.output.v2` envelopes and
preserves typed result/artifact/certificate payloads, diagnostics, fallback
status, and the temporary legacy field mirror. It is not a native binding,
DataFrame runtime, SQL engine, UDF runtime, package publication, or fallback
execution path.

It also exposes report-only workflow diagnostics, local smoke helpers, and a scoped
`ctx.from_rows([...]).write("target/generated.jsonl")` and `ctx.range(0, 10).write(...)`
generated-output smokes for explicit testing. The
Python client can be installed in editable mode, configured through environment variables, run a
no-dataset smoke check, query the capability surfaces, and invoke the current local Vortex workflow
without turning pandas, Arrow, SQL, or another engine into a fallback runtime. See
[`python/README.md`](python/README.md) for local usage.

## License

ShardLoom is licensed under the Apache License 2.0.

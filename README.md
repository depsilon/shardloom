# ShardLoom

Website: [shardloom.io](https://shardloom.io) (domain reserved; public site content pending).

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

| Area | Current repo state | Planned or gated updates |
| --- | --- | --- |
| Execution modes | `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, `direct_compatibility_transient`, and `auto` are represented in benchmark/report fields. | More prepared/native Vortex paths; `auto` must keep reporting the selected concrete mode. |
| Batch engine mode | Current practical foundation for bounded local Vortex analytics and benchmark evidence. | Broader operator coverage, source/sink certification, and claim-grade workload evidence. |
| Live engine mode | `engine-selection-plan`, `engine-capability-matrix`, `live-change-contract-plan`, Python helpers, and scoped in-memory `live-fixture-run` reports exist. | Durable state/checkpoints, broker/source adapters, freshness evidence, and workload certification. |
| Hybrid engine mode | `engine-selection-plan`, `engine-capability-matrix`, Python helpers, and scoped in-memory `hybrid-overlay-run` reports exist. | Durable micro-segment flush, object-store/table commit, catalog snapshot discovery, and hot/cold benchmark evidence. |
| Streaming/zero-copy/backpressure | `streaming-plan`, `streaming-batch-plan`, `backpressure-plan`, `engine-capability-matrix`, and `capabilities engines` expose a GAR-0013 matrix for local fixture evidence, object-store streaming read blockers, zero-decode, zero-copy/materialization boundaries, bounded backpressure, and live/hybrid broker runtime blockers. | Object-store streaming reads, durable broker adapters, runtime backpressure enforcement, broader operator/source/sink evidence, and claim-grade workload certification. |
| Prepared/native Vortex runtime | Scoped residual-native paths now avoid full fact-table materialization for selected local benchmark scenarios, including local distinct-count, null-heavy aggregate, clean/cast/filter/write, malformed timestamp / dirty CSV, nested JSON field scan, global sort/top-k, and partition-pruning/date-range scan evidence. CPU specialization reporting now records side-effect-free host feature probes and a blocked filter/encoded vector-kernel admission diagnostic. | Next planned work follows the phase-plan queue for kernel, source-backed API, facade, and evidence-gated expansion; encoded-native, SIMD dispatch, and performance claims remain evidence-gated. |

## Current State

ShardLoom is pre-release. The current repository is strongest as an evidence-first Vortex-native
engine prototype plus protocol/benchmark scaffolding, not as a general SQL/DataFrame/REST platform
yet.

The current `local_vortex_analytics_v1` path is now a credible local workload-certified
compute-engine slice: it can import local compatibility data into Vortex artifacts, execute the
supported local analytics path, write a Vortex result artifact, replay source and result artifacts,
emit execution and Native I/O certificates, record scheduler/memory evidence, and preserve
no-fallback policy fields. That certification is intentionally scoped to the named local workload;
it is not a broad SQL engine, production platform, Spark replacement, or general DataFrame runtime
claim.

Currently wired surfaces include:

- typed CLI JSON envelopes and Python parsing for result refs, artifact refs, certificates,
  diagnostics, lifecycle, policy, and fallback status
- side-effect-free capability, semantic, workflow, and evidence reports for current and blocked
  surfaces
- local Vortex traditional-analytics execution with optional source replay, result-sink replay,
  scheduler/memory evidence, execution certificates, Native I/O certificates, and no-fallback fields
- explicit execution-mode evidence for `compatibility_import_certified`, `prepared_vortex`,
  `native_vortex`, `direct_compatibility_transient`, and `auto`
- scoped prepared/native Vortex query paths for `selective filter`, `wide projection`,
  `filter + projection + limit`, `group by aggregation`, `multi-key group by`, `hash join`, and
  `join + aggregate`, `sort and top-k`, `top-N per group`, `row number window`, and
  `high-cardinality string group/distinct`, `distinct count`, `null-heavy aggregate`,
  `clean/cast/filter/write`, `malformed timestamp / dirty CSV`, `nested JSON field scan`, plus
  scoped local `partition pruning` date-range scans;
  these avoid full
  fact-table materialization for the prepared/native row while remaining residual-native, not
  encoded-native operator claims
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
- Vortex-first architecture docs and guardrails for treating upstream Vortex APIs as native providers
  only when they are policy-admitted and certificate-backed

Some broad surfaces are intentionally report-only or unsupported in this pre-release repository. See
the phase plan and completed ledger for exact implementation status.

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
kernel/provider expansion, source-backed API follow-through, facade coverage, and evidence
hardening, with encoded-native, SIMD dispatch, production, SQL/DataFrame, object-store, and
performance claims still gated by workload evidence.

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
- [`docs/getting-started/first-10-minutes.md`](docs/getting-started/first-10-minutes.md)
- [`docs/getting-started/examples.md`](docs/getting-started/examples.md)
- [`docs/getting-started/certified-local-workload.md`](docs/getting-started/certified-local-workload.md)
- [`docs/benchmarks/local-taxonomy-benchmark.md`](docs/benchmarks/local-taxonomy-benchmark.md)
- [`docs/benchmarks/baseline-comparison-boundary.md`](docs/benchmarks/baseline-comparison-boundary.md)
- [`docs/architecture/compute-engine-flow-reference.md`](docs/architecture/compute-engine-flow-reference.md)

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

It also exposes report-only workflow diagnostics and local smoke helpers for explicit testing. The
Python client can be installed in editable mode, configured through environment variables, run a
no-dataset smoke check, query the capability surfaces, and invoke the current local Vortex workflow
without turning pandas, Arrow, SQL, or another engine into a fallback runtime. See
[`python/README.md`](python/README.md) for local usage.

## License

ShardLoom is licensed under the Apache License 2.0.

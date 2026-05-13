# ShardLoom

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
- batch, live, and hybrid execution modes only after they are ShardLoom-native and
  certificate-backed
- REST/event/remote APIs as proof and orchestration surfaces, not hidden execution delegation

## Product Direction

ShardLoom is not intended to stop at a narrow local accelerator. The roadmap expands the same
native, inspectable execution contract into:

- **complete user data workflows**: install, import, discover capabilities, read data, validate
  schemas, transform, write outputs, explain, certify, benchmark, and diagnose.
- **three ShardLoom-native engine modes**: batch for finite analytical work, live for continuous
  incremental computation, and hybrid for fresh analytical state over Vortex base data plus explicit
  deltas.
- **remote API access**: REST for control/proof/orchestration and small results; event APIs for
  progress and live/hybrid updates; explicit data-plane choices such as Vortex artifacts, object
  references, Arrow IPC boundaries, JSON Lines, Flight, or ADBC where approved.
- **availability and platform integration**: Conda-first release proof, PyPI-friendly Python
  packaging, provenance-backed GitHub releases, and optional Foundry integration that treats Foundry
  datasets and virtual tables as governed workflow handles rather than ShardLoom execution
  shortcuts.

These are roadmap targets, not blanket support claims. A surface counts as supported only when its
native execution path or explicit materialization/source/sink boundary, diagnostics, certificates,
correctness evidence, and benchmark or workload evidence are present for the declared workload.

## Current State

ShardLoom is pre-release. The current repository is strongest as an evidence-first Vortex-native
engine prototype plus protocol/benchmark scaffolding, not as a general SQL/DataFrame/REST platform
yet.

Implemented or actively wired surfaces include:

- typed top-level plan variants and artifact-rich execution results for current Vortex primitive,
  prepared encoded, source-backed, reader-backed, and report-only plan shapes
- a Vortex top-level execution provider bridge that preserves current local, prepared encoded,
  source-backed, and reader-backed provider reports as typed evidence instead of lossy strings
- `shardloom.output.v2`, a typed CLI JSON envelope with result refs, artifact refs, certificates,
  policy, lifecycle, capability snapshot, diagnostics, fallback status, and a temporary legacy
  `fields` mirror
- modular CLI handler families for status/capabilities, input planning, REST planning,
  packaging/deployment, benchmark planning/runtime, diagnostics, evidence/certificates,
  workflow/table planning, engine/runtime planning, and operational hardening
- report-only CG-21/CG-22/CG-23 parity surfaces: `workflow-unsupported-plan`, scoped
  `capabilities workflow|engines|remote-api|cross-cg`, `workload-certification-dossier`, and
  `claim-gate-closeout` explain which workflow, engine-mode, API, package, integration,
  certificate, Native I/O, correctness, and benchmark evidence exists or is still blocked without
  running workloads
- `compute-capability-matrix`, a report-only P7.4 matrix that distinguishes unsupported, planned,
  report-only, executable-uncertified, fixture-certified, workload-certified, and
  production-certified compute surfaces across provider kind, engine mode, semantic profile,
  materialization/decode boundaries, memory/spill requirements, evidence refs, and no-fallback
  status
- `semantic-conformance-suite`, a P7.4 ShardLoomNative semantic fixture surface that executes the
  currently supported in-memory semantic checks, records planned/blocked dimensions, and preserves
  external-oracle-free/no-fallback evidence in the typed CLI envelope
- top-level execution-result evidence slots for provider version, lifecycle status, result refs,
  artifact refs, inline artifacts, execution/Native I/O certificates, materialization/residual
  boundaries, representation transitions, source/split refs, and fallback status, with missing
  required slots reported as evidence-incomplete
- Python lazy workflow helpers for current report-only UX, including source declaration, explain,
  estimate, certification inspection, unsupported pandas/Arrow/NumPy/Python-object materialization
  boundaries, DataFrame `with_column`/`group_by`/`agg`/`sort` diagnostics, SQL
  parse/bind/plan/execute diagnostics, schema/data-quality/quarantine reports, and notebook
  preview/display diagnostics
- local traditional-analytics benchmark harness support with a machine-readable scenario catalog,
  taxonomy metadata, benchmark constitution fields, executable generated profiles for wide,
  very-wide, null-heavy, many/few file-shape, date-partitioned, clustered, schema-drift, dirty CSV,
  nested JSON, CDC overlay, and skewed local data shapes, plus support/coverage output separate from
  timing rows
- opt-in `local_vortex_analytics_v1` replay and result-sink verification for the current local
  traditional analytics path, including Vortex source/result artifact digests, schema summaries,
  benchmark/coverage row refs, source replay and result-sink Native I/O certificate refs, separate
  compute/write timing fields, local task-graph scheduler evidence, bounded queue/backpressure
  fields, memory reservation/release evidence, retry/cancellation gate status, runtime execution
  certificate status, operator spill blockers, commit/cleanup status, and `fallback_attempted=false`
- ShardLoom-native benchmark coverage for base-schema taxonomy scenarios beyond the default local
  suite: filter/projection/limit, multi-key group by, join+aggregate, row-number window,
  high-cardinality string group/distinct, and top-N per group, with the same replay/result-sink
  evidence path available through the traditional analytics harness
- source-backed benchmark matrix smoke measurement for eligible prepared, source-bound, and
  reader-backed constant/dictionary/run-end encoded filter/projection/filter-project rows, with
  provider refs, certificate refs, Native I/O refs, representation transitions, reproducibility
  refs, and no-fallback evidence while still blocking performance claims
- Vortex-first guardrails and runtime-utilization audit docs covering arrays, layouts, Scan
  Source/Sink/Split concepts, field masks, predicate ordering, I/O evidence, sessions/registries,
  device posture, and extension-type posture
- pre-P9 report-only Foundry unstructured/media posture that names media-set, virtual media-set,
  extraction, model-call, embedding, AIP, and unstructured workflow certificate boundaries without
  invoking Foundry or model/media runtimes

Still planned or gated:

- broad SQL, DataFrame, notebook, UDF, adapter, object-store, catalog/table, live/hybrid, REST
  server, generated-client, Foundry, and Marketplace surfaces
- production package publication and public performance/superiority claims
- claim-grade compute-engine completion: source-backed measured rows beyond fixture-smoke evidence,
  Vortex layout/write advisor feedback, remaining extra-column/multi-file/incremental taxonomy
  support, and benchmark promotion
- full comparative benchmark reruns, write/incremental benchmark promotion, clean/cast/filter/write
  execution, and claim-grade source-backed benchmark promotion
- hard release-readiness gates and public first-10-minutes proof from release artifacts
- Foundry proof-of-use certification that preserves Foundry as an optional integration boundary
- any external engine execution as ShardLoom fallback

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

## Roadmap Source Of Truth

Active implementation state is tracked in
[`docs/architecture/phased-execution-plan.md`](docs/architecture/phased-execution-plan.md).

The competitive roadmap is organized as CG-1 through CG-23. CG-21 defines the user data workflow and
ETL surface, CG-22 defines the three-engine certified data execution fabric, and CG-23 defines the
REST, event, and remote API surface. RFC 0036 defines the optional Foundry integration pack and
late-stage availability surface around those gates; it is not a new core engine gate. Supporting
RFCs live in [`docs/rfcs`](docs/rfcs), and phase/RFC mapping lives in
[`docs/architecture/rfc-phase-traceability.md`](docs/architecture/rfc-phase-traceability.md).

This README intentionally does not duplicate active status checklists.

External systems lessons and technique-transfer notes live in
[`docs/architecture/systems-learning-map.md`](docs/architecture/systems-learning-map.md). RFCs own
deep contracts and acceptance criteria.

## No-Fallback Policy

ShardLoom must fail explicitly for unsupported execution paths. Spark, DataFusion, DuckDB, Polars,
Velox, and similar engines may be used only as conceptual references, migration baselines,
correctness oracles, or benchmark baselines. They must not execute ShardLoom runtime paths as
fallback engines.

## Engineering Direction

Engineering work is organized around evidence-backed gates:

- real Vortex-native read and query primitive execution paths
- native Vortex output payloads and local commit/rollback evidence
- correctness fixtures and no-fallback invariants
- execution certificates and reproducibility evidence
- capability surfaces for SQL, operators, functions, adapters, Python/API, DataFrame/query builder,
  notebook, UDF, common ETL, universal adapters, and unstructured/media workflows

Performance, superiority, or best-default-engine claims require correctness and benchmark evidence
before publication.

## Python Client

The source-tree Python client in [`python/`](python/) is a thin wrapper over the
ShardLoom CLI JSON protocol. It parses `shardloom.output.v2` envelopes and
preserves typed result/artifact/certificate payloads, diagnostics, fallback
status, and the temporary legacy field mirror. It is not a native binding,
DataFrame runtime, SQL engine, UDF runtime, package publication, or fallback
execution path.

It also exposes current report-only workflow diagnostics that are useful before broader DataFrame
runtime support exists: `workflow-unsupported-plan` and Python `LazyFrame` helpers return stable
blocker IDs, required evidence, next actions, and no-runtime/no-fallback fields for pandas/Arrow
interop, NumPy/Python-object materialization, DataFrame expression/grouping/sort gaps, SQL
parse/bind/plan/execute stages, schema/data-quality/quarantine behavior, and notebook preview and
display boundaries.

For release posture, `claim-gate-closeout` and `ShardLoomClient.claim_gate_closeout()` summarize the
allowed report/local fixture claims, blocked production/API/package/benchmark claims, and
out-of-scope integration claims before Priority 8/9 evidence exists.

For P7.4 compute closeout, `compute-capability-matrix` and `semantic-conformance-suite` expose the
current capability ladder and ShardLoomNative semantic fixture status without reading data, running
SQL/DataFrame workloads, invoking external engines, or attempting fallback. Top-level execution
results now preserve provider evidence through typed refs, inline artifacts, evidence-slot status,
provider metadata, lifecycle fields, certificate refs, materialization/representation refs, and
no-fallback policy fields so clients can distinguish complete evidence from evidence-incomplete
execution.

It also exposes the current local live ETL smoke commands for explicit testing:
CSV-to-Vortex through `traditional-analytics-run`, optionally with
`--verify-native-replay --write-result-vortex` to re-open the emitted Vortex
source artifacts, write a computed `result.vortex` sink artifact, replay it, and
emit artifact digest/schema/certificate/timing fields plus local scheduler,
memory reservation/release, retry/cancellation, spill-blocker, and runtime
execution-certificate evidence. Existing native Vortex inputs run through
`traditional-analytics-vortex-run`. The Python client can also be
installed in editable mode, configured through environment variables, run a
no-dataset smoke check, and query the side-effect-free universal input adapter
registry, where common structured formats, lakehouse/table refs, object-store
adapters, catalogs, effectful inputs, and unstructured/media inputs are tracked
as explicit planned/enablement statuses before runtime readers exist. See
[`python/README.md`](python/README.md) for local usage.

## License

ShardLoom is licensed under the Apache License 2.0.

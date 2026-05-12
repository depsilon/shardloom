# ShardLoom

ShardLoom is a standalone encoded-columnar execution engine for computing directly over Vortex-native layouts, preserving encoded representation where possible, and producing Vortex-native or lakehouse-compatible outputs without delegating execution to Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

"Standalone" means standalone from external query-engine fallback, not isolated
from Vortex itself. ShardLoom may use upstream Vortex array, compute, scan,
source, and sink APIs as native execution providers when they are admitted
through ShardLoom policy and reported through ShardLoom certificates. That is
distinct from delegating unsupported work to DataFusion, DuckDB, Spark, Polars,
Velox, or Vortex query-engine integrations.

Longer term, ShardLoom is being shaped into a certified data execution fabric: a Vortex-native engine with user-friendly Python, SQL, DataFrame, ETL, adapter, live/hybrid, and remote API surfaces that all preserve the same no-fallback evidence model.

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
- common analytical capability through SQL, Python/API, UDF, adapter, ETL, and unstructured/media surfaces over time
- batch, live, and hybrid execution modes only after they are ShardLoom-native and certificate-backed
- REST/event/remote APIs as proof and orchestration surfaces, not hidden execution delegation

## Product Direction

ShardLoom is not intended to stop at a narrow local accelerator. The roadmap expands the same native, inspectable execution contract into:

- **complete user data workflows**: install, import, discover capabilities, read data, validate schemas, transform, write outputs, explain, certify, benchmark, and diagnose.
- **three ShardLoom-native engine modes**: batch for finite analytical work, live for continuous incremental computation, and hybrid for fresh analytical state over Vortex base data plus explicit deltas.
- **remote API access**: REST for control/proof/orchestration and small results; event APIs for progress and live/hybrid updates; explicit data-plane choices such as Vortex artifacts, object references, Arrow IPC boundaries, JSON Lines, Flight, or ADBC where approved.
- **availability and platform integration**: Conda-first release proof, PyPI-friendly Python packaging, provenance-backed GitHub releases, and optional Foundry integration that treats Foundry datasets and virtual tables as governed workflow handles rather than ShardLoom execution shortcuts.

These are roadmap targets, not blanket support claims. A surface counts as supported only when its native execution path or explicit materialization/source/sink boundary, diagnostics, certificates, correctness evidence, and benchmark or workload evidence are present for the declared workload.

## Core Concepts

The full vocabulary lives in [`docs/architecture/canonical-terminology.md`](docs/architecture/canonical-terminology.md). These terms are the shortest orientation path:

- **native Vortex input/output**: Vortex is the highest-fidelity input and persistence target.
- **Vortex-native execution provider**: upstream Vortex or ShardLoom-owned Vortex-aware compute admitted through ShardLoom policy and certificate evidence; this is native execution, not external fallback.
- **encoded-columnar execution**: operators should preserve encoded representation when capability evidence allows it.
- **metadata-first planning**: answer or prune from metadata before reading data bytes.
- **zero-decode**: execute over encoded Vortex representation without decoding values.
- **late materialization**: concrete rows/columns appear only at explicit materialization boundaries.
- **compatibility output**: Parquet, Arrow IPC, Iceberg-compatible, Delta-compatible, JSONL, and CSV are translation/export targets, not fallback execution engines.
- **no-fallback execution**: unsupported plans fail with deterministic diagnostics instead of delegating runtime work to another engine.
- **capability certification**: SQL, operator, function, adapter, Python/API, ETL, and unstructured/media support must be evidence-backed before claims are made.

## Roadmap Source Of Truth

Active implementation state is tracked in [`docs/architecture/phased-execution-plan.md`](docs/architecture/phased-execution-plan.md).

The competitive roadmap is organized as CG-1 through CG-23. CG-21 defines the user data workflow and ETL surface, CG-22 defines the three-engine certified data execution fabric, and CG-23 defines the REST, event, and remote API surface. RFC 0036 defines the optional Foundry integration pack and late-stage availability surface around those gates; it is not a new core engine gate. Supporting RFCs live in [`docs/rfcs`](docs/rfcs), and phase/RFC mapping lives in [`docs/architecture/rfc-phase-traceability.md`](docs/architecture/rfc-phase-traceability.md).

This README intentionally does not duplicate active status checklists.

External systems lessons and technique-transfer notes live in [`docs/architecture/systems-learning-map.md`](docs/architecture/systems-learning-map.md). RFCs own deep contracts and acceptance criteria.

## No-Fallback Policy

ShardLoom must fail explicitly for unsupported execution paths. Spark, DataFusion, DuckDB, Polars, Velox, and similar engines may be used only as conceptual references, migration baselines, correctness oracles, or benchmark baselines. They must not execute ShardLoom runtime paths as fallback engines.

## Engineering Direction

Engineering work is organized around evidence-backed gates:

- real Vortex-native read and query primitive execution paths
- native Vortex output payloads and local commit/rollback evidence
- correctness fixtures and no-fallback invariants
- execution certificates and reproducibility evidence
- capability surfaces for SQL, operators, functions, adapters, Python/API, DataFrame/query builder, notebook, UDF, common ETL, universal adapters, and unstructured/media workflows

Performance, superiority, or best-default-engine claims require correctness and benchmark evidence before publication.

## Python Client

The source-tree Python client in [`python/`](python/) is a thin wrapper over the
ShardLoom CLI JSON protocol. It parses `OutputEnvelope` responses and preserves
diagnostics, fields, and fallback status. It is not a native binding, DataFrame
runtime, SQL engine, UDF runtime, package publication, or fallback execution
path.

It also exposes the current local live ETL smoke commands for explicit testing:
CSV-to-Vortex through `traditional-analytics-run` and existing native Vortex
inputs through `traditional-analytics-vortex-run`. The Python client can also
be installed in editable mode, configured through environment variables, run a
no-dataset smoke check, and query the side-effect-free universal input adapter
registry, where common structured formats, lakehouse/table refs, object-store
adapters, catalogs, effectful inputs, and unstructured/media inputs are tracked
as explicit planned/enablement statuses before runtime readers exist. See
[`python/README.md`](python/README.md) for local usage.

## License

ShardLoom is licensed under the Apache License 2.0.

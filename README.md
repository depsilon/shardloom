# ShardLoom

ShardLoom is a standalone encoded-columnar execution engine for computing directly over Vortex-native layouts, preserving encoded representation where possible, and producing Vortex-native or lakehouse-compatible outputs without delegating execution to Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

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

## Core Concepts

The full vocabulary lives in [`docs/architecture/canonical-terminology.md`](docs/architecture/canonical-terminology.md). These terms are the shortest orientation path:

- **native Vortex input/output**: Vortex is the highest-fidelity input and persistence target.
- **encoded-columnar execution**: operators should preserve encoded representation when capability evidence allows it.
- **metadata-first planning**: answer or prune from metadata before reading data bytes.
- **zero-decode**: execute over encoded Vortex representation without decoding values.
- **late materialization**: concrete rows/columns appear only at explicit materialization boundaries.
- **compatibility output**: Parquet, Arrow IPC, Iceberg-compatible, Delta-compatible, JSONL, and CSV are translation/export targets, not fallback execution engines.
- **no-fallback execution**: unsupported plans fail with deterministic diagnostics instead of delegating runtime work to another engine.
- **capability certification**: SQL, operator, function, adapter, Python/API, ETL, and unstructured/media support must be evidence-backed before claims are made.

## Roadmap Source Of Truth

Active implementation state is tracked in [`docs/architecture/phased-execution-plan.md`](docs/architecture/phased-execution-plan.md).

The competitive roadmap is organized as CG-1 through CG-20. Supporting RFCs live in [`docs/rfcs`](docs/rfcs), and phase/RFC mapping lives in [`docs/architecture/rfc-phase-traceability.md`](docs/architecture/rfc-phase-traceability.md).

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

## License

ShardLoom is licensed under the Apache License 2.0.

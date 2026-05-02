# ShardLoom Skills

This directory contains project-specific operating procedures for Codex, other agents, and human contributors working on ShardLoom.

These files are not generic advice. They are intended to preserve ShardLoom's core architecture while the project grows.

## Core project constraints

ShardLoom is:

- A standalone encoded-columnar execution engine.
- Vortex-native for input and output.
- Designed to compute over encoded layouts where possible.
- Designed to avoid unnecessary decoding, materialization, movement, and shuffle.
- Designed to produce Vortex as the highest-fidelity persistence target.
- Designed to export to lakehouse-compatible formats without using those formats as execution fallbacks.

ShardLoom is not:

- A Spark plugin.
- A DataFusion wrapper.
- A DuckDB extension.
- A Polars wrapper.
- A new file format.
- A lakehouse table format replacement.
- An execution layer that silently delegates unsupported plans to other engines.

## How to use these skills

Before starting a task, identify the relevant skill documents.

For example:

- Editing Rust crates: read `rust-systems-engineering.md`.
- Adding dependencies or using AI-generated code: read `license-provenance.md`.
- Modeling Vortex files, arrays, metadata, layouts, or output: read `vortex-internals.md`.
- Implementing pruning, encoded predicates, or late materialization: read `encoded-execution.md`.
- Designing logical or physical plans: read `planner-optimizer.md`.
- Writing Vortex, Parquet, Arrow IPC, Iceberg-compatible, or Delta-compatible outputs: read `translation-layer.md`.
- Making performance claims: read `benchmarking.md`.
- Adding tests or changing behavior: read `testing-correctness.md`.
- Working on object-store or distributed runtime features: read `object-store-runtime.md`.
- Creating RFCs or architecture docs: read `documentation-rfc.md`.

## Required validation

For implementation PRs, run:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`

Documentation-only PRs should still avoid changing Rust code unless necessary.

## Review standard

A good ShardLoom PR should answer:

- Does this preserve standalone execution?
- Does this avoid Spark and DataFusion fallback?
- Does this preserve Vortex-native input/output where relevant?
- Does this fail explicitly for unsupported behavior?
- Does this improve correctness, clarity, or measurable performance?
- Are claims backed by tests, benchmarks, or RFC rationale?

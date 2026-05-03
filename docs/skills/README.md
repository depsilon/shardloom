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
- Designing expression IR, function signatures, kernel capabilities, or kernel selection policy: read `expression-kernel-registry.md`.
- Designing plan IR layering, plan import/export constraints, or Substrait-compatible interoperability direction: read `plan-ir-interoperability.md`.
- Designing extension/plugin ABI, manifests, capability declarations, sandboxing, permission/effect models, or agent-safe plugin inspection: read `extension-plugin-sandboxing.md`.
- Designing release engineering policy, packaging discipline, API/schema compatibility tiers, or release-governance controls: read `release-engineering-packaging.md`.
- Writing Vortex, Parquet, Arrow IPC, Iceberg-compatible, or Delta-compatible outputs: read `translation-layer.md`.
- Making performance claims: read `benchmarking.md`.
- Adding tests or changing behavior: read `testing-correctness.md`.
- Working on object-store or distributed runtime features: read `object-store-runtime.md`.
- Creating RFCs or architecture docs: read `documentation-rfc.md`.


## Vortex-specific skills

Vortex work requires more detailed guidance than the general `vortex-internals.md` file.

Use the detailed Vortex skill pack under `docs/skills/vortex/` for:

- DTypes and logical/physical modeling.
- Vortex file IO.
- Encodings and layouts.
- Statistics and pruning.
- Vortex-native output.
- Scan API boundaries.
- Arrow interoperability.
- Upstream Vortex versioning.

For any Vortex-related implementation, read `docs/skills/vortex-internals.md` first, then read the relevant detailed files in `docs/skills/vortex/`.

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

## Developer and agent experience

Use `developer-agent-experience.md` for public APIs, CLI commands, diagnostics, explain output, estimate output, examples, config files, and agent-facing workflows.

ShardLoom should be internally complex but externally simple, familiar, deterministic, and safe for both humans and LLM agents.

## Modular extensibility

Use `modular-extensibility.md` for SQL, UDFs, unstructured data, LLM/API calls, embeddings, vector search, connectors, and capability discovery.

ShardLoom should remain Vortex-native and high-performance while exposing flexible, familiar, explicit extension points for adjacent workflows. External effects must be explicit, safe, and machine-readable for agents.

## Diagnostics and capabilities

Use `diagnostics-capabilities.md` for errors, diagnostics, explain output, estimate output, doctor checks, capability discovery, translation reports, and machine-readable agent-facing output.

ShardLoom should provide stable, deterministic, actionable diagnostics for humans and LLM agents. Unsupported behavior must be explicit, and fallback status must be visible.

## Streaming, zero-copy, and zero-decode

Use `streaming-zero-copy.md` for streaming execution, zero-copy boundaries, zero-decode execution, sink-driven planning, Arrow-like interoperability, materialization boundaries, backpressure, and bounded-memory behavior.

ShardLoom should prefer metadata-only, pruning, encoded execution, zero-copy boundaries, partial decode, late materialization, shuffle avoidance, and distribution avoidance in that order.

## Memory, spill, and OOM safety

Use `memory-spill-oom.md` for memory budgets, reservations, memory pressure, spill policies, spill files, spillable operators, cleanup, stateful operator memory behavior, and OOM diagnostics.

ShardLoom should avoid memory pressure through pruning, streaming, zero-decode, late materialization, and adaptive sizing, then survive memory pressure through reservations and native spill where necessary.

## Correctness, optimizer, and fault tolerance

Use `correctness-testing.md` for tests, semantics, differential testing, fuzzing, decoded reference behavior, unsupported diagnostics, and benchmark correctness validation.

Use `optimizer-adaptive-execution.md` for optimizer rules, adaptive execution, runtime filters, dynamic pruning, join/aggregation strategy, skew handling, sink-driven planning, and memory/spill-aware planning.

Use `fault-tolerance-recovery.md` for task attempts, retries, cancellation, recovery, idempotency, partial output cleanup, ambiguous commits, spill cleanup, and external effect failure behavior.

ShardLoom should be correct before fast, adaptive before wasteful, and recoverable before massive.

## Observability, security, and schema/table compatibility

## Canonical terminology

Use docs/architecture/canonical-terminology.md when adding or changing domain types, diagnostics, CLI labels, JSON fields, or RFC language.

## Universal input contract

Use docs/architecture/universal-input-contract.md for universal input source/adapter contracts, capability status, fidelity, metadata availability, materialization risk, and effect-level modeling.


## Vortex dependency readiness

Use docs/dependencies/vortex-upstream-review.md and docs/architecture/vortex-adapter-integration-plan.md before adding upstream Vortex dependencies or adapter code.

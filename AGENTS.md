# ShardLoom Agent Instructions

ShardLoom is a standalone encoded-columnar execution engine designed to compute directly over Vortex-native layouts and produce Vortex-native and lakehouse-compatible outputs.

ShardLoom's core identity is:

- Standalone execution.
- Vortex-native input and output.
- Encoded-columnar execution.
- Late materialization.
- Segment-level pruning.
- Object-store-native planning.
- Modular storage translation.
- No Spark fallback.
- No DataFusion fallback.

## Non-negotiable requirements

Agents and contributors must follow these requirements:

- Do not add Apache Spark as an execution dependency or fallback.
- Do not add Apache DataFusion as an execution dependency or fallback.
- Do not silently delegate unsupported execution plans to DuckDB, Polars, Velox, Spark, DataFusion, or another engine.
- Unsupported execution paths must fail explicitly with clear, deterministic diagnostics.
- Vortex must be treated as a first-class native input target.
- Vortex must be treated as a first-class native output target.
- Vortex output is the highest-fidelity persistence target.
- Parquet, Arrow IPC, Iceberg-compatible files, and Delta-compatible files are translation/export targets, not execution fallbacks.
- Keep the core engine standalone.
- Prefer original implementation over copying from existing engines.
- Do not copy implementation code from GPL, AGPL, SSPL, BUSL, proprietary, source-available, or unknown-license projects.
- Use Apache-2.0-compatible dependencies only unless explicitly approved through an RFC.
- Prioritize correctness before performance claims.
- Every performance claim must be backed by a reproducible benchmark.
- Every non-trivial implementation change should include tests.

## Architecture principles

ShardLoom should avoid work before optimizing work.

Execution priority:

1. Answer from metadata when possible.
2. Prune segments using statistics.
3. Compute against encoded data when possible.
4. Decode partially when necessary.
5. Materialize late.
6. Distribute only when single-node execution is insufficient.
7. Avoid shuffle whenever possible.
8. Fail explicitly instead of silently delegating to another engine.

## Required workflow for implementation tasks

Before making implementation changes:

1. Read the relevant skill documents in `docs/skills`.
2. Confirm the change does not violate the no-fallback policy.
3. Confirm Vortex-native input/output is preserved where relevant.
4. Keep the change small and reviewable.
5. Do not publish packages or create releases.

After making implementation changes:

1. Add or update tests for behavior changes.
2. Run the required validation commands.

Required validation commands:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`

## Skill routing

Use these skill documents by task type:

- Rust workspace, crate, API, or runtime changes: `docs/skills/rust-systems-engineering.md`
- Dependency, license, copied-code, or generated-code questions: `docs/skills/license-provenance.md`
- Vortex-native input/output, segment modeling, or encoded layout inspection: `docs/skills/vortex-internals.md`
- Encoded kernels, pruning, selection vectors, or late materialization: `docs/skills/encoded-execution.md`
- Logical planning, physical planning, cost model, or optimization: `docs/skills/planner-optimizer.md`
- Vortex, Parquet, Arrow IPC, Iceberg-compatible, or Delta-compatible outputs: `docs/skills/translation-layer.md`
- Benchmarks or performance claims: `docs/skills/benchmarking.md`
- Tests, fuzzing, reference checks, and correctness validation: `docs/skills/testing-correctness.md`
- Object-store IO, distributed runtime, task scheduling, retries, or commits: `docs/skills/object-store-runtime.md`
- RFCs, architecture docs, design decisions, or project policy: `docs/skills/documentation-rfc.md`


### Vortex-specific routing

For any Vortex-related work, first read `docs/skills/vortex-internals.md`.

Then read the relevant detailed Vortex skills:

- Vortex concepts, DTypes, arrays, encodings, and layouts: `docs/skills/vortex/vortex-concepts.md`
- Vortex file reads/writes and metadata: `docs/skills/vortex/vortex-file-io.md`
- Encoded operations and physical layouts: `docs/skills/vortex/vortex-encodings-layouts.md`
- Statistics, metadata-only answers, and pruning: `docs/skills/vortex/vortex-stats-pruning.md`
- Vortex-native persistence: `docs/skills/vortex/vortex-native-output.md`
- Scan API and source/sink boundaries: `docs/skills/vortex/vortex-scan-api.md`
- Arrow compatibility and decoded reference boundaries: `docs/skills/vortex/vortex-arrow-interop.md`
- Upstream Vortex dependency/version behavior: `docs/skills/vortex/vortex-versioning-upstream.md`

## Current phase

ShardLoom is in early architecture and skeleton setup.

Do not overbuild.

Prefer small pull requests that establish clear contracts:

- No-fallback execution policy.
- Vortex-native input/output contract.
- Encoded segment model.
- Translation-layer contracts.
- Correctness-first testing.
- Benchmark methodology.

Implementation depth should increase only after the relevant RFC or design document exists.

## Developer and agent experience

ShardLoom should be highly performant internally while remaining flexible, familiar, and easy to use.

Before public API, CLI, diagnostics, explain, estimate, config, or documentation changes, read:

- `docs/rfcs/0010-developer-experience-agent-usability.md`
- `docs/skills/developer-agent-experience.md`

Important principles:

- Internal complexity should produce external simplicity.
- Human developers should get familiar APIs.
- LLM agents should get deterministic, machine-readable diagnostics.
- Simple usage should not require advanced Vortex knowledge.
- Native Vortex output should be easy to select.
- Unsupported behavior must be explicit and actionable.
- No Spark or DataFusion fallback is allowed for convenience.

# RFC 0025 — Competitive Engine Track and No-Fallback Replacement Strategy

## Purpose

Define the expanded Competitive Engine Track before CG-1 implementation starts, while preserving ShardLoom’s no-fallback architecture and Vortex-native execution direction.

## Strategic target

ShardLoom targets wholesale replacement of Spark, Polars, DataFusion, and Arrow-adjacent execution stacks for supported Vortex-native lakehouse workloads.

Replacement is achieved through ShardLoom-native, Vortex-native execution. It is not achieved through fallback, delegation, or hidden handoff to external engines.

## Non-goals

- Implementing encoded reads, query execution, output payload writes, or benchmark harnesses in this RFC.
- Adding Spark, DataFusion, vortex-datafusion, DuckDB, Polars, Velox, or any fallback execution path.
- Introducing dependency changes.

## Competitive Engine Track

Competitive success gates (CG) are roadmap gates, not canonical phase-ID aliases:

1. CG-1: encoded read boundary and real encoded reads.
2. CG-2: real query primitive execution over Vortex data.
3. CG-3: output payload write path.
4. CG-4: commit protocol execution.
5. CG-5: correctness/differential harness.
6. CG-6: benchmark harness.
7. CG-7: physical operators/kernels.
8. CG-8: streaming/parallel/adaptive execution.
9. CG-9: lakehouse/table intelligence.
10. CG-10: object-store/distributed execution.
11. CG-11: Python/API surface later.
12. CG-12: plan portability / semantic IR.
13. CG-13: encoded-native compressed execution.
14. CG-14: runtime-adaptive optimizer and execution memory.
15. CG-15: CPU operator specialization.
16. CG-16: evidence-first execution certificates.
17. CG-17: stateful result reuse / incremental execution.
18. CG-18: universal import/deployment/baseline harness.

## No-fallback policy

- No runtime fallback/delegation to Spark, DataFusion, Polars, DuckDB, Velox, vortex-datafusion, or any other external execution engine.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Arrow interop remains an explicit boundary, not an implicit default execution substrate.

## External baseline policy

Spark, DataFusion, Polars, DuckDB, Velox, and vortex-datafusion may be used only as external baselines for correctness/differential checks and benchmarks. They must never execute ShardLoom runtime paths as fallback engines.

## Success criteria

- Competitive gates CG-1 through CG-18 remain visible and complete in planning artifacts.
- Execution posture remains Vortex-native and no-fallback.
- External baseline usage remains explicit and non-runtime.
- Superiority claims are disallowed until both CG-5 (correctness) and CG-6 (benchmarks) are satisfied.

## Risks

- Competitive-gate drift where CG items are treated as implementation phase aliases.
- Hidden fallback pressure from integration convenience.
- Premature superiority claims before reproducible correctness and benchmark evidence.
- Scope creep into runtime implementation before gate-specific implementation PRs.

## Validation requirements

- Keep this RFC and downstream architecture docs consistent on CG-1 through CG-18.
- Preserve explicit no-fallback wording.
- Preserve external engines as baseline-only wording.
- Preserve CG-18 as universal import/deployment/baseline harness with Foundry only as optional deployment/comparison example.

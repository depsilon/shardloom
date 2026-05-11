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
11. CG-11: Python/API foundation surface later.
12. CG-12: plan portability / semantic IR.
13. CG-13: encoded-native compressed execution.
14. CG-14: runtime-adaptive optimizer and execution memory.
15. CG-15: CPU operator specialization.
16. CG-16: evidence-first execution certificates.
17. CG-17: stateful result reuse / incremental execution.
18. CG-18: universal import/deployment/baseline harness.
19. CG-19: universal native I/O envelope.
20. CG-20: world-class SQL, operator, function, adapter, and user capability surface.
21. CG-21: user data workflow and ETL surface.
22. CG-22: placeholder for incoming content-rich gate 2.
23. CG-23: placeholder for incoming content-rich gate 3.

CG-21 is defined by RFC 0033. CG-22 and CG-23 remain reserved placeholders until
their source files land. CG-21 through CG-23 are logically after the current
CG-1 through CG-20 plan and do not authorize runtime behavior, dependencies,
fallback execution, or claims by themselves.

## No-fallback policy

- No runtime fallback/delegation to Spark, DataFusion, Polars, DuckDB, Velox, vortex-datafusion, or any other external execution engine.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Arrow interop remains an explicit boundary, not an implicit default execution substrate.

## External baseline policy

Spark, DataFusion, Polars, DuckDB, Velox, and vortex-datafusion may be used only as external baselines for correctness/differential checks and benchmarks. They must never execute ShardLoom runtime paths as fallback engines.


## CG-3 completion clarification

- Placeholder/local output payload artifacts are scaffolding and readiness evidence only.
- CG-3 is not complete until at least one real executable Vortex-native output payload write path exists for a supported workload.
- The real CG-3 path must be feature-gated, ShardLoom-owned, and no-fallback.
- Arrow conversion must not become the default execution path for CG-3 completion.
- Competitive claims remain disallowed until CG-5 correctness and CG-6 benchmarks are satisfied.

## Success criteria

- Competitive gates CG-1 through CG-23 remain visible and complete in planning artifacts.
- Execution posture remains Vortex-native and no-fallback.
- External baseline usage remains explicit and non-runtime.
- Superiority claims are disallowed until both CG-5 (correctness) and CG-6 (benchmarks) are satisfied.
- Best-default-engine claims are disallowed until CG-20 emits a workload-scoped sufficiency report backed by CG-5 correctness, CG-6 benchmark, CG-16 certificate, CG-19 native I/O certificate, and CG-20 capability evidence. User data-workflow claims additionally require CG-21 workflow evidence for the declared workload.

## Best-default evidence gate

CG-20 is not complete from capability breadth alone. A final "best default" posture requires an explicit evidence bundle for each declared workload constitution.

Required evidence before any best-default, best-choice, replacement, superiority, faster, cheaper, or world-class public claim:

- `WorkloadConstitution` names the workload categories, required SQL features, operators, functions, adapters, semantic profiles, API surfaces, source/sink paths, scale shape, budgets, fixtures, benchmarks, and out-of-scope items.
- `BestDefaultCertificationDossier` reports correctness, semantic conformance, benchmarks, operator/function/adapter certification, native I/O certificates, memory/spill safety, observability, migration, API ergonomics, deployment, dependency policy, and no-fallback integrity.
- `WorldClassSufficiencyReport` records whether the CG-20 contract set is sufficient for the workload and lists blocking gaps when it is not.
- CG-19 emits per-source/sink-path `NativeIoCertificate` evidence for every required adapter path.
- CG-16 execution certificate evidence exists for every supported execution path in the workload.
- External engines appear only as labeled correctness, migration, or benchmark baselines.
- Unsupported, planned-only, or test-reference-only entries remain visible and cannot be counted as production support.
- `fallback_attempted=false` is present across the evidence chain.

If any required evidence is absent, the only allowed publication status is `not_certified` or `partial_for_workload` with explicit blockers and known limits.

## Risks

- Competitive-gate drift where CG items are treated as implementation phase aliases.
- Hidden fallback pressure from integration convenience.
- Premature superiority claims before reproducible correctness and benchmark evidence.
- Scope creep into runtime implementation before gate-specific implementation PRs.

## Validation requirements

- Keep this RFC and downstream architecture docs consistent on CG-1 through CG-23.
- Preserve explicit no-fallback wording.
- Preserve external engines as baseline-only wording.
- Preserve CG-18 as universal import/deployment/baseline harness with Foundry only as optional deployment/comparison example.


## Final competitive gate clarifications

### CG-19 — Universal Native I/O Envelope
Define and adopt ShardLoom-native universal I/O contracts that preserve encoded representation, statistics, selection vectors, pushdown proof, materialization state, and sink requirements without defaulting to decoded Arrow batches.

### CG-20 — World-Class SQL, Operator, Function, Adapter, and User Capability Surface
Define and validate a full capability-certification surface across SQL, operators, functions, adapters, semantic compatibility, migration tooling, Python/API, UDFs, common data/ETL, universal adapters, unstructured/media data, and user capability discovery.

CG-20 is the final user-capability gate. It is broader than SQL support alone.

CG-11 can establish stable API/protocol foundations, but mature Python wrapper, DataFrame/query-builder, notebook, Python UDF, packaging, common ETL, and universal-adapter certification belong to CG-20.

ShardLoom's competitive target is best-default-engine certification for declared workloads, not merely a narrow Vortex accelerator. That certification target must remain evidence-gated and workload-scoped.

No-fallback policy remains unchanged: ShardLoom must not delegate runtime execution to Spark, DataFusion, DuckDB, Polars, Velox, or other external engines.

No superiority claims are allowed before correctness and benchmark gates are satisfied (CG-5 and CG-6).

### CG-21 - User Data Workflow and ETL Surface
Define a complete, inspectable, certified user data workflow around ShardLoom:
install, import, discover capabilities, read, validate, transform, write,
explain, certify, benchmark, and diagnose unsupported cases.

CG-21 is governed by `docs/rfcs/0033-user-data-workflow-etl-surface.md`.
It extends the CG-20 user capability surface into scenario-driven ETL workflow
certification while preserving ShardLoom-native execution, explicit
materialization/source/sink boundaries, external-baseline-only comparisons, and
no fallback execution.

### CG-22 through CG-23 - Reserved Future Gates
CG-22 and CG-23 remain reserved for incoming content-rich gate documents. The
remaining placeholder RFC files are:

- `docs/rfcs/0034-cg22-placeholder.md`
- `docs/rfcs/0035-cg23-placeholder.md`

The incoming files define the real titles, scopes, and acceptance criteria for
those gates. The placeholder gates exist only to keep roadmap numbering,
traceability, and phase planning stable before those files land.

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

## Phase source of truth

ShardLoom's active implementation and cleanup queue is tracked in `docs/architecture/phased-execution-plan.md`.

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

## Modular extensibility

ShardLoom should be designed to gracefully support common and adjacent workloads such as SQL, UDFs, unstructured data, LLM calls, API calls, embeddings, and vector search through modular extension points.

Before work involving SQL, UDFs, unstructured data, LLM/API calls, embeddings, vector search, connectors, or capability discovery, read:

- `docs/rfcs/0011-modular-extensibility-sql-udf-unstructured-llm-api-embeddings.md`
- `docs/skills/modular-extensibility.md`

Important principles:

- SQL is a frontend into ShardLoom planning, not fallback execution.
- UDFs must be typed and explicit about determinism, null behavior, effects, encoded capability, and materialization.
- Unstructured data should use typed references, chunks, extracted fields, and manifests.
- LLM calls, API calls, and embedding generation are explicit effectful operations, not hidden side effects.
- External writes require explicit enablement and safe planning.
- Agent-facing capability discovery should be deterministic and machine-readable.
- No Spark or DataFusion fallback is allowed for convenience.

## User workflow, engine modes, and remote APIs

Before work involving broad user workflows, ETL UX, Python/DataFrame surfaces, batch/live/hybrid engine modes, live or hybrid state, REST/event APIs, remote result delivery, lineage/governance export, or agent API surfaces, read:

- `docs/rfcs/0033-user-data-workflow-etl-surface.md`
- `docs/rfcs/0034-three-engine-certified-data-execution-fabric.md`
- `docs/rfcs/0035-rest-event-remote-api-surface.md`

Important principles:

- CG-21 certifies complete user workflows; it does not authorize hidden pandas, Polars, Spark, DataFusion, DuckDB, or other execution fallback.
- CG-22 engine selection is internal ShardLoom-native batch/live/hybrid selection, not delegation to external streaming, database, or lakehouse systems.
- CG-23 REST is a control plane, proof surface, orchestration API, and small-result API; large data transfer must use explicit data-plane boundaries.
- Discovery, explain, estimate, capability, and agent surfaces must be side-effect-free by default.
- Remote execution, writes, external effects, credentials, and destructive operations require explicit policy, diagnostics, certificate linkage, and no-fallback evidence.

## Diagnostics, explain, estimate, doctor, and capabilities

ShardLoom should expose structured, deterministic, human-friendly and agent-friendly diagnostics.

Before work involving errors, diagnostics, explain output, estimate output, doctor checks, capability discovery, translation reports, or machine-readable CLI/API output, read:

- `docs/rfcs/0012-diagnostics-explain-estimate-capabilities.md`
- `docs/skills/diagnostics-capabilities.md`

Important principles:

- Unsupported behavior must fail explicitly with stable diagnostic codes.
- Fallback status must be explicit and false by default.
- Explain output should expose execution boundaries.
- Estimate output should represent uncertainty honestly.
- Doctor checks should be safe and avoid side effects.
- Capability discovery should be deterministic and machine-readable.
- Effectful operations must not run during explain, estimate, doctor, or capabilities.
- No Spark or DataFusion fallback is allowed.
- Feature-footprint/doctor work should centralize status reporting before changing behavior.
- Do not implement filesystem/network probing in doctor/capabilities without an explicit phase.
- Do not treat external baseline availability as fallback execution availability.

## Streaming, zero-copy, and zero-decode

ShardLoom should minimize data work in this order:

1. Do not read.
2. Do not decode.
3. Do not copy.
4. Do not materialize.
5. Do not shuffle.
6. Do not distribute unless necessary.

Before work involving streaming execution, zero-copy boundaries, zero-decode execution, sink-driven planning, Arrow-like interoperability, materialization boundaries, backpressure, or bounded-memory behavior, read:

- `docs/rfcs/0013-streaming-zero-copy-boundary-interoperability.md`
- `docs/skills/streaming-zero-copy.md`

Important principles:

- Zero-decode Vortex-native execution is more important than zero-copy boundaries.
- Arrow-like interoperability is a boundary, not the internal execution substrate.
- Streaming must not silently fall back to full in-memory execution.
- Sink requirements should influence materialization and metadata preservation.
- Vortex output remains highest-fidelity.
- Compatibility outputs must report metadata loss.
- No Spark or DataFusion fallback is allowed.

## Memory, spill, and OOM safety

ShardLoom must treat memory management and spill as first-class design concerns for Spark-displacement workloads.

Before work involving memory budgets, reservations, memory pressure, spill, spill files, spillable operators, stateful operators, shuffle buffers, sink buffers, or OOM diagnostics, read:

- `docs/rfcs/0014-memory-management-spill-oom-safety.md`
- `docs/skills/memory-spill-oom.md`

Important principles:

- Adaptive sizing and streaming reduce OOM risk but are not sufficient.
- Stateful operators need memory reservations and spill contracts.
- Spill must be ShardLoom-native.
- Prefer columnar and Vortex-native spill where practical.
- Unsupported spill behavior must fail deterministically before process OOM where possible.
- Spill files must have cleanup semantics.
- No Spark or DataFusion fallback is allowed.

## Correctness, optimizer, and fault tolerance

ShardLoom must be correct, explainable, adaptive, and recoverable before it can credibly challenge massive Spark-like workloads.

Before work involving correctness, tests, semantics, differential testing, fuzzing, optimizer rules, adaptive execution, runtime filters, skew handling, retries, cancellation, recovery, commits, or cleanup, read the relevant docs:

- `docs/rfcs/0015-correctness-semantics-differential-testing-fuzzing.md`
- `docs/rfcs/0016-optimizer-adaptive-execution-runtime-filters-skew.md`
- `docs/rfcs/0017-fault-tolerance-cancellation-recovery.md`
- `docs/skills/correctness-testing.md`
- `docs/skills/optimizer-adaptive-execution.md`
- `docs/skills/fault-tolerance-recovery.md`

Important principles:

- Correctness comes before performance.
- Encoded execution must be checked against decoded reference behavior where appropriate.
- Differential testing may use external engines only as testing/comparison oracles, not fallback execution.
- Optimizer decisions must be semantics-preserving and diagnosable.
- Runtime filters must be conservative.
- Adaptive execution must preserve correctness.
- Fault states, retries, cancellation, commits, and cleanup must be explicit.
- No Spark or DataFusion fallback is allowed.


## Expression engine and plan interoperability

Before work involving expression IR, kernel registries, kernel selection policy, plan IR layering, plan import/export, or Substrait-compatible interoperability direction, read:

- `docs/rfcs/0021-expression-engine-kernel-registry.md`
- `docs/rfcs/0022-plan-ir-substrait-compatible-interoperability.md`
- `docs/skills/expression-kernel-registry.md`
- `docs/skills/plan-ir-interoperability.md`

Important principles:

- Expression and kernel selection must be native, deterministic, null-safe, type-aware, Vortex-aware, and no-fallback.
- Plan interoperability is allowed, but imported plans must pass ShardLoom capability checks.
- Substrait-compatible thinking is an interoperability direction, not a dependency or execution engine.
- No Spark or DataFusion fallback is allowed.

## Observability, security, and schema/table compatibility

ShardLoom must be observable, secure, governable, and compatible with evolving schemas and existing table ecosystems before it can credibly support production workloads.


## Extension/plugin safety and release engineering

Before work involving extension/plugin ABI, sandboxing, release engineering, packaging, or public API stability, read:

- `docs/rfcs/0023-extension-plugin-abi-sandboxing.md`
- `docs/rfcs/0024-release-engineering-api-compatibility-packaging.md`
- `docs/skills/extension-plugin-sandboxing.md`
- `docs/skills/release-engineering-packaging.md`

Important principles:

- Extensions/plugins must declare capabilities, permissions, effects, materialization requirements, sandboxing constraints, and license/provenance metadata.
- Extension inspection must not execute extension code.
- Release engineering must preserve Apache-2.0 compatibility, dependency hygiene, API stability discipline, and no-fallback architecture.
- Do not publish packages or create releases without explicit human approval.
- No Spark or DataFusion fallback is allowed.

## Canonical terminology

Before introducing new public terms, domain types, CLI labels, diagnostics, or JSON fields, read:
- docs/architecture/canonical-terminology.md

Rules:
- Use canonical ShardLoom vocabulary.
- Do not use "fallback" to mean translation/export.
- Do not use "zero-copy" to mean zero-decode.
- Keep native Vortex output distinct from compatibility output.
- Prefer mapping helpers over premature type consolidation.

## Vortex dependency readiness

Before adding any upstream Vortex dependency, read:
- docs/dependencies/vortex-upstream-review.md
- docs/architecture/vortex-adapter-integration-plan.md
- docs/skills/vortex/vortex-versioning-upstream.md
- docs/skills/license-provenance.md

Rules:
- Do not add upstream Vortex without license/provenance review.
- Keep upstream Vortex API usage isolated in shardloom-vortex.
- Do not use DataFusion/Spark/DuckDB/Polars/Velox as Vortex helpers.
- Do not default to decode-to-Arrow execution.
- Preserve Vortex as native input and output.
- Unsupported upstream features must fail explicitly.


## Universal input contract

Before adding input formats, readers, connectors, catalog sources, API/LLM/embedding/vector inputs, or unstructured ingestion, read:
- docs/architecture/universal-input-contract.md

Rules:
- Vortex is native input.
- Compatibility inputs must be explicit and feature-gated.
- Effectful inputs require explicit enablement.
- Input adapters must not imply fallback execution.
- No silent decode/materialization.
- Default build stays lightweight.

## Phased execution and incumbent gap map

Before adding engine behavior, read:
- docs/architecture/phased-execution-plan.md
- docs/architecture/incumbent-gap-opportunity-map.md
- docs/architecture/capability-certification-sequencing.md when work touches CG-20 SQL/operator/function/adapter/user capability surfaces

Rules:
- Keep work aligned to the current phase.
- Do not jump to object-store, writes, spill, distributed execution, or fallback engines early.
- Cross-cutting epics should be attached to phases, not implemented as unrelated sprawl.
- ShardLoom should solve incumbent pain points through Vortex-native, no-fallback, metadata-first design.

## RFC and phase traceability

Before starting a new phase, consult:
- `docs/architecture/phased-execution-plan.md`
- `docs/architecture/rfc-phase-traceability.md`

Do not re-read every RFC for every PR. Use targeted RFC checks based on the current phase.

## Phase and epic checklist

Before proposing next implementation work:
- Check `docs/architecture/phased-execution-plan.md`.
- Include current phase status and relevant epics in the prompt.
- Do not treat synthetic spill payload support as query/Vortex data spill permission.
- Attach cross-cutting epic obligations to the current phase.
- Reviews should happen at phase boundaries or when entering a new behavior class, not after every small PR.


## Competitive engine roadmap

When proposing next work:
- do not create duplicate phase IDs (keep canonical implementation phases separate from CG-* competitive gates)
- include both the current implementation phase and relevant CG-* competitive gates in phase checklists
- do not introduce Spark/DataFusion/Polars/DuckDB/Velox/vortex-datafusion as fallback
- external baselines are allowed only for correctness/benchmark references
- do not make superiority claims until CG-5 and CG-6 are satisfied
- preserve Vortex-native, no-fallback, explicit-side-effect contracts
- keep the Competitive Engine Track visible in phase proposals
- do not reduce or omit CG-1 through CG-23
- Foundry belongs under CG-18 as an optional deployment/comparison target, not as the primary engine target
- external engines are baselines only
- no runtime fallback/delegation
- do not make superiority claims until CG-5 and CG-6 are satisfied
- commodity CPU vectorized execution is a first-class target
- GPU/FPGA acceleration is not required for the primary competitive claim

## Competitive gate guardrail

- Do not mark CG-3 complete from placeholder output payload artifacts alone.
- In responses/checklists, explicitly distinguish placeholder artifact path status from real Vortex output payload path status.
- Competitive claims require CG-5 correctness and CG-6 benchmark evidence.


## Systems-learning-map guardrails

- Keep systems-learning-map lessons conceptual unless a later RFC explicitly approves implementation.
- Do not add Trino/Dask/Ray/DuckDB/Calcite/Acero/Substrait dependencies.
- Do not use any of them as runtime fallback engines.
- Keep Competitive Engine Track visible and complete in phase proposals.
- Place active refactor/docs queue above CG implementation queue when docs are out of sync.

## R3 cleanup sequencing guardrails

- For R3 cleanup tasks, prioritize inventory/audit PRs first, then small targeted cleanup PRs.
- Do not bundle CLI registry implementation, diagnostics normalization, terminology renames, and feature-footprint implementation into one PR.
- Do not rename public types or commands without an explicit compatibility plan.
- Keep CG implementation paused while the active R3 cleanup queue is current. After R3.1 lands, proceed to the next planned R3 cleanup item, starting with R3.2, unless the user explicitly overrides the docs/refactor queue and resumes CG implementation.
- Do not skip from R3.1 directly to R4/CG work by default.
- User override is allowed, but it must be explicit and should be reflected in the next phase prompt.

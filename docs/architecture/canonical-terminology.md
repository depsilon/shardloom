# Canonical Terminology

## Purpose

ShardLoom intentionally keeps related concepts at different layers (planning, execution, streaming boundaries, translation, and adapters). This document defines canonical meanings so terminology stays consistent without collapsing useful layer boundaries too early.

This document owns vocabulary definitions, not phase status. Active implementation status and queue placement live in `docs/architecture/phased-execution-plan.md`.

## Core principles

- Keep layer-specific types when they model different decisions.
- Add mapping helpers instead of prematurely merging types.
- Use "native Vortex" for highest-fidelity input/output.
- Use "compatibility output" for Parquet/Arrow/Iceberg/Delta/JSONL/CSV exports.
- Use "fallback execution" only for prohibited delegation to another engine.
- Use "translation" for output conversion, not execution.
- Use "zero-decode" for Vortex-native encoded execution.
- Use "zero-copy" for boundary/interoperability sharing.
- Use "materialization" only when values/rows/columns become concrete for an operator/sink/boundary.

## Materialization family

- `MaterializationPolicy`: user/planner intent from encoded execution layer.
- `MaterializationRequirement`: translation/output requirement from a sink or target.
- `MaterializationBoundary`: streaming/runtime boundary where materialization becomes required.

These remain separate because they represent intent, contractual requirements, and runtime boundary points at different layers. Mapping helpers should exist between these concepts where needed.

## Fidelity family

- `FidelityLevel`: canonical core output fidelity concept.
- `VortexOutputFidelity`: Vortex adapter-local fidelity concept.

`VortexOutputFidelity` should map into `FidelityLevel`. Vortex native full fidelity corresponds to `FidelityLevel::NativeFullFidelity`.

## Execution/work state family

- `ExecutionState`: canonical plan/execution-state label.
- `DataWorkLevel`: streaming/data-work ranking where lower rank means less work.

`DataWorkLevel` is an optimization/work ranking, while `ExecutionState` is a descriptive execution state. Streaming code can map `DataWorkLevel` into `ExecutionState` when needed.

## Dataset/output format family

- `DatasetFormat`: input/reference format identification.
- `OutputTargetKind`: requested output target.

`DatasetFormat` can map into `OutputTargetKind`, but they remain separate because input references and output contracts represent different boundaries.

## Memory/resource family

- `ResourceBudget`: runtime task-level limits.
- `AdaptiveSizingPolicy`: task sizing policy.
- `BoundedMemoryPolicy`: streaming bounded-memory requirement.
- `MemoryBudget`: memory pool/spill/OOM budget.

These concepts remain separate but should expose clear summaries and mapping helpers where useful.

## Agent-facing vocabulary

Preferred terms:

- `native_vortex_input`
- `native_vortex_output`
- `compatibility_output`
- `fallback_execution_allowed`
- `fallback_attempted`
- `metadata_only`
- `segment_pruning`
- `zero_decode`
- `zero_copy_boundary`
- `partial_decode`
- `late_materialization`
- `full_materialization`
- `spill_required`
- `unsupported`

## Do not use

Discouraged terms:

- "fallback" when meaning "translation"
- "Arrow-native" for internal execution
- "Spark-compatible fallback"
- "free execution" without benchmark/cost context
- "zero-copy" when data is actually decoded
- "native output" when target is compatibility export

## Cleanup backlog terminology families

R3.1 inventory keeps existing public type names stable while prioritizing mapping/helper cleanup.

- **Materialization family** (`MaterializationPolicy`, `MaterializationRequirement`, `MaterializationBoundary`): keep distinct by layer; add mapping helpers where cross-layer rendering is needed.
- **Execution/work family** (`ExecutionState`, `DataWorkLevel`): keep both; strengthen deterministic mapping helpers for agent/CLI/report consistency.
- **Format/target family** (`DatasetFormat`, `OutputTargetKind`): keep separate input/output boundaries; add explicit mapping where interoperability summaries require it.
- **Fidelity family** (`FidelityLevel`, `VortexOutputFidelity`): keep canonical + adapter-local split; improve explicit mapping helpers and report parity checks.
- **Resource/memory family** (`ResourceBudget`, `MemoryBudget`, `BoundedMemoryPolicy`): keep policy boundaries explicit; avoid premature consolidation.
- **Plan skeleton family** (`RuntimePlanSkeleton`, `StreamingPlanSkeleton`, `ScanPlanSkeleton`): keep separate contracts for now; evaluate shared field helpers in targeted cleanup PRs.

## Terminology consolidation backlog

The R3.4 audit keeps terminology families distinct while documenting mapping-helper-first follow-ups.

- Materialization (`MaterializationPolicy`, `MaterializationRequirement`, `MaterializationBoundary`)
- Execution/data-work (`ExecutionState`, `DataWorkLevel`)
- Input/output/fidelity (`DatasetFormat`, `OutputTargetKind`, `FidelityLevel`, `VortexOutputFidelity`)
- Resource/memory (`ResourceBudget`, `MemoryBudget`, `BoundedMemoryPolicy`)
- Plan skeletons (`RuntimePlanSkeleton`, `StreamingPlanSkeleton`, `ScanPlanSkeleton`)
- Status/report suffixes (`Plan`, `Request`, `Report`, `Status`, `Mode`, `Signal`, `Effect`, `Ref`, `Id`)

See `docs/architecture/terminology-consolidation-backlog.md` for the full audit backlog and helper candidates.

## R5.1 glossary additions

- **fallback execution**: prohibited runtime delegation to an external engine.
- **compatibility baseline**: non-native engine/output reference used for comparison or interoperability checks, never as runtime delegation.
- **external baseline**: external system used only for correctness/benchmark reference, not execution fallback.
- **pushdown proof**: structured evidence describing whether a predicate/projection was pushed exactly, with residual, or rejected.
- **residual expression**: the remainder that must still run natively after partial pushdown.
- **lowering provenance**: trace of high-level plan constructs lowered into lower-level task/operator forms.
- **portability loss**: explicit representational loss when mapping native plan semantics to an interchange form.
- **intermediate artifact**: explicit typed runtime/planning artifact (spill, exchange, runtime-filter, staged-commit, profile sample) with stable identity.



## R5.2 glossary additions

- **capability certification surface**: the complete user-visible capability area used to evaluate evidence-backed best-default-engine certification for declared workloads.
- **capability supremacy surface**: older shorthand for capability certification surface; prefer the evidence-gated term in new docs.
- **universal native I/O envelope**: A ShardLoom-native contract that preserves representation state, pushdown evidence, and sink constraints without default decode.
- **native work envelope**: A single unit of planned/executed work carrying representation, stats, boundaries, and diagnostics.
- **foreign encoded**: Non-Vortex encoded representation preserved as encoded data when possible, without implying fallback execution.
- **semantic profile**: A named semantics compatibility target (for example ShardLoomNative, SparkCompatible, DataFusionCompatible).
- **compatibility baseline**: External engine or format behavior used for comparison and conformance checks, never runtime fallback.
- **external baseline**: Non-ShardLoom comparison oracle used for correctness/benchmark evidence only.
- **certification report**: Machine-readable evidence artifact describing tested capability level and conformance boundaries.
- **best-choice scorecard**: Evidence-backed summary of why ShardLoom is or is not the best default for a declared workload constitution.


## Additional capability-certification terms
- **workload constitution**: the declared workload categories used to scope claims and certification.
- **operator certification**: status evidence that an operator family meets correctness, semantics, and performance gates for a claim level.
- **function certification**: status evidence that a function definition meets semantic and execution-contract requirements for a claim level.
- **SQL coverage tier**: staged status level (for example S0-S7) describing SQL capability maturity.
- **adapter maturity level**: staged adapter certification level (for example A0-A7) for discovery/read/write/commit and evidence depth.
- **semantic compatibility profile**: named semantic mode (for example ShardLoomNative, SparkCompatible) with explicit behavior dimensions.
- **migration compatibility report**: structured report mapping supported/unsupported constructs and semantic deltas for migration planning.
- **source pushdown exactness**: the declared guarantee quality for source pushdown (exact, exact with residual, conservative, unsupported, unsafe rejected).
- **native I/O certificate**: structured evidence object capturing source/sink capability, transitions, materialization boundaries, and no-fallback status.
- **data/ETL capability report**: structured evidence for ingestion, transformation, cleaning, incremental processing, write/export, memory/spill, and pipeline observability coverage.
- **Python surface report**: structured evidence for the Python wrapper/API, including protocol version, notebook/DataFrame status, materialization boundaries, packaging, diagnostics, and no-fallback behavior.
- **unstructured media capability report**: structured evidence for document, text, image, audio, video, archive, extracted-field, chunk, and metadata handling with explicit effect/materialization boundaries.
- **universal adapter catalog**: workload-scoped inventory of source, sink, catalog, object-store, relational, warehouse, client/server, and unstructured/media adapters with maturity and certificate status.
- **API surface certification**: evidence that a CLI, Rust, Python, DataFrame, notebook, server, BI, or agent surface exposes native capability safely and explicitly.
- **observability certification**: evidence that explain, estimate, profile, metrics, certificates, and diagnostics reveal what ShardLoom did, avoided, or rejected.
- **deployment readiness report**: structured evidence for packaging, configuration, resource limits, reproducibility, compatibility, and operational constraints.
- **extension capability report**: structured UDF/plugin evidence for type metadata, effect level, sandboxing, permissions, materialization, and no-fallback behavior.
- **security governance report**: structured evidence for credentials, permissions, external effects, destructive-operation policy, redaction, audit, and agent safety.

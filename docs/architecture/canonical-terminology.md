# Canonical Terminology

## Purpose

ShardLoom intentionally keeps related concepts at different layers (planning, execution, streaming boundaries, translation, and adapters). This document defines canonical meanings so terminology stays consistent without collapsing useful layer boundaries too early.

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

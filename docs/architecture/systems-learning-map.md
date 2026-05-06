# Systems Learning Map

## Purpose

This document captures lessons from mature systems as conceptual pressure tests for ShardLoom-native contracts.

- These systems are conceptual references, not dependencies.
- They are not fallback engines.
- ShardLoom remains Vortex-native and no-fallback.
- External engines can be used later only as correctness/benchmark baselines.

## Trino lessons

- Connector capabilities should map to explicit capability checks and deterministic unsupported diagnostics.
- Split/task/driver/operator lifecycle concepts can inform native runtime stage boundaries.
- PushdownProof should explain exactly what was pushed down and what remained residual.
- DynamicFilter lifecycle should be explicit from plan-time intent through runtime application.
- IntermediateArtifactRef taxonomy should classify temporary artifacts by lifecycle, ownership, and cleanup class.
- System introspection datasets should expose machine-readable runtime/planning surfaces.

Trino remains a conceptual reference only:
- no Trino dependency
- no Trino execution
- no SQL delegation

## Dask lessons

- Distinguish high-level graph intent from low-level task graph execution detail.
- Preserve lowering provenance from logical graph decisions into task graph nodes.
- Define task granularity policy explicitly, not implicitly.
- Separate scheduler contracts from planning contracts.
- Keep optional external scheduler seams as future concepts only.

Dask remains a conceptual reference only:
- no Dask dependency
- no Dask scheduler execution

## Ray lessons

- Resource vectors should make CPU/memory/IO constraints explicit per task class.
- Placement/locality hints should be explicit inputs to scheduling choices.
- Distinguish lineage reconstruction from retry strategy semantics.
- Distributed object/reconstruction concepts can inform future CG-10 design inputs.

Ray remains a conceptual reference only:
- no Ray dependency
- no Ray scheduler execution

## DuckDB lessons

- Operator-level vectorized profiles should be first-class diagnostics.
- Planned-vs-actual execution profile should be deterministic and machine-readable.
- Pipeline breaker classification should be explicit and queryable.
- Explain/analyze clarity should be preserved for both humans and agents.

DuckDB remains a conceptual reference only:
- no DuckDB dependency
- DuckDB may be benchmark baseline only

## Calcite lessons

- Keep SQL parse/bind/validate/plan boundaries explicit.
- Treat SQL as a frontend, not the execution brain.
- Unsupported SQL must produce explicit deterministic diagnostics.
- No delegated SQL execution is allowed.

Calcite remains a conceptual reference only:
- no Calcite/parser dependency in this PR

## Arrow Acero/Substrait lessons

- Execution graph portability should be expressed as plan portability contracts.
- Validation without execution should remain possible for imported/exported plans.
- Portability and metadata-loss reporting should be explicit.
- Arrow boundaries should remain explicit interop boundaries.

Arrow Acero/Substrait remain conceptual references only:
- no Arrow-default execution
- no Acero/Substrait dependency in this PR

## ShardLoom-native contracts to add later

- PushdownProof
- RuntimeFilterLifecycle
- LoweringTrace
- TaskGranularityPolicy
- IntermediateArtifactRef
- ResourceVector
- PlacementHint
- RecoveryStrategy
- OperatorProfile
- PipelineBreakerKind
- PlanPortabilityReport
- system introspection virtual datasets


## Contract extraction status

- mapped_to_rfc_0012
- mapped_to_rfc_0008
- mapped_to_rfc_0016
- mapped_to_rfc_0022
- mapped_to_rfc_0030

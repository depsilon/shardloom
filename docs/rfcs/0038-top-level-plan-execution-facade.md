# RFC 0038: Top-Level Plan and Execution Facade

## Purpose

Bring the generic ShardLoom plan and execution facade up to the current Vortex-native execution
surface.

The implementation has outgrown the early skeleton where a top-level plan could be represented as
one placeholder `NativeVortexScan` kind and top-level execution could return success without
evidence. ShardLoom is not released yet, so this RFC permits replacing those early shapes rather
than preserving backward-compatible legacy APIs.

## Status

Accepted as implementation-shape guidance.

This RFC does not authorize SQL/DataFrame runtime, broad adapter execution, object-store runtime,
writes, external engine invocation, package publication, performance claims, or fallback execution.

## Problem

Current Vortex-specific crates already expose evidence-rich local, prepared encoded, source-bound,
and reader-backed primitive paths. The top-level facade must not discard that evidence or return
no-op success for a placeholder plan.

The top-level model must represent the executable surfaces that exist and block everything else
with deterministic diagnostics.

## Plan Model

Replace the minimal top-level plan skeleton with typed plan variants for:

```text
VortexPrimitivePlan
PreparedEncodedPlan
SourceBackedEncodedPlan
ReaderBackedEncodedPlan
ReportOnlyPlan
```

Required request families:

```text
local Vortex CountAll
local Vortex CountWhere
local Vortex FilterPredicate
local Vortex ProjectColumns
local Vortex FilterAndProject
prepared encoded filter
prepared encoded projection
prepared encoded filter-project
source-bound encoded filter/projection/filter-project
reader-backed encoded filter/projection/filter-project where evidence exists
```

Plan variants must carry enough admission data to avoid hidden probing:

```text
plan_id / plan_ref
operation kind
source refs
split refs
provider boundary refs
residual boundary refs
encoded batch refs where applicable
materialization policy
fallback policy
evidence requirements
diagnostics
```

## Execution Result

Replace top-level `execute(&Plan) -> Result<()>` with a typed execution result.

Required result fields:

```text
status
plan_id / plan_ref
engine_mode
execution_provider_kind
provider_api_surface
source_refs
split_refs
result_refs
artifact_refs
execution_certificate_refs
native_io_certificate_refs
materialization_boundary_refs
residual_boundary_refs
representation_transitions
diagnostics
fallback_attempted
external_engine_invoked
```

Vortex-specific execution reports may be attached as typed artifacts or converted into this shared
shape. They must not be flattened until provider, residual, certificate, and Native I/O evidence is
lost.

P7.4.3 implements the artifact-rich form for the current top-level execution result surface. The
shared result can now preserve provider version, lifecycle status, inline artifacts, explicit
evidence-slot status, certificate refs, Native I/O refs, materialization and residual boundary refs,
representation transitions, source/split refs, and no-fallback policy fields. Missing required
evidence is represented as `evidence_incomplete`; it is not silently omitted.

## No-Op Prohibition

No top-level execution path may report success unless it has either:

```text
evidence-backed execution result
explicit report-only/no-execution contract
deterministic blocked/unsupported diagnostic
```

The early placeholder success path must be replaced before release.

## Non-Goals

```text
SQL parser/runtime
DataFrame runtime
object-store runtime
write runtime
external query-engine delegation
fallback execution
legacy facade compatibility
```

## Acceptance

```text
Plan variants represent current executable Vortex primitive and encoded paths.
Execution dispatch reaches the corresponding admitted provider surfaces.
Execution results preserve certificates, Native I/O refs, residual boundaries, and diagnostics.
Execution results preserve provider versions, lifecycle status, inline artifacts, and evidence-slot
completeness status.
Unsupported plans are blocked deterministically.
fallback_attempted=false remains visible.
external_engine_invoked=false remains visible for ShardLoom execution.
```

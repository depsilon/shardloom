# RFC 0022: Plan IR and Substrait-Compatible Interoperability

## Status

Draft

## Summary

This RFC defines ShardLoom's native plan IR and future interoperability direction.

ShardLoom needs a stable internal representation for logical plans, physical plans, encoded execution plans, streaming plans, effect boundaries, translation boundaries, runtime adaptation, and diagnostics. The design should keep future Substrait-compatible interoperability in mind without adding Substrait as a dependency or allowing imported plans to become fallback execution.

## Context

ShardLoom planning spans multiple outputs and lifecycle stages:

- Scan requests.
- Explain reports.
- Estimate reports.
- Encoded segment models.
- Translation plans.
- Streaming plans.
- Runtime task graphs.
- Memory/spill/OOM plans.
- Future SQL/UDF/effect expressions.
- Future optimizer/adaptive execution decisions.
- Future table/catalog compatibility.

A native-first plan IR contract is needed before public APIs, imported plans, plugin frontends, and production execution semantics can safely stabilize.

## Goals

- Define plan IR layers.
- Define stable plan node identity.
- Define logical vs physical vs encoded plan boundaries.
- Define effect and translation boundaries.
- Define plan serialization direction.
- Define Substrait-compatible thinking without dependency commitment.
- Define plan import/export constraints.
- Define agent-readable plan behavior.
- Preserve no-fallback execution policy.

## Non-goals

- Implement plan IR.
- Add Substrait dependency.
- Implement plan serialization/import/export.
- Add Spark/DataFusion or any other fallback execution engine.
- Guarantee full Substrait compatibility.

## Core principle

ShardLoom's plan IR should be native first and interoperability-aware second.

Imported plans, exported plans, or Substrait-compatible representations must not bypass ShardLoom's native capability checks.

## Detailed design

### Plan layers

Plan layers should include:

- UserIntentPlan.
- LogicalPlan.
- OptimizedLogicalPlan.
- PhysicalPlan.
- EncodedPhysicalPlan.
- StreamingPlan.
- RuntimeTaskGraph.
- AdaptiveRuntimePlan.
- ExecutedPlanReport.

### Plan node identity

Every significant plan node should eventually have:

- Stable node id.
- Node kind.
- Input schema.
- Output schema.
- Diagnostics.
- Execution state.
- Estimated metrics.
- Actual metrics if executed.
- Parent/child relationships.
- Materialization boundaries.
- Effect boundaries.
- Translation boundaries.

### Plan node kinds

Plan node kinds should include:

- Scan.
- Filter.
- Projection.
- Aggregate.
- Join.
- Sort.
- Limit.
- UDF.
- ExternalRead.
- ExternalWrite.
- ModelCall.
- EmbeddingGeneration.
- VectorSearch.
- Translation.
- Write.
- Commit.
- Unsupported.

### Plan capabilities

Plan nodes should declare requirements such as:

- Requires Vortex native input.
- Requires statistics.
- Requires byte ranges.
- Requires encoded kernel.
- Requires partial decode.
- Requires materialization.
- Requires spill support.
- Requires external credentials.
- Requires explicit effect enablement.
- Requires compatibility output.
- Requires native Vortex output.

### Effect boundaries

Effectful operations must be explicit and include API calls, LLM calls, embedding generation, vector search against external systems, external writes, and side-effecting UDFs.

### Translation boundaries

Translation boundaries should be explicit:

- Vortex native output.
- Arrow IPC compatibility output.
- Parquet compatibility output.
- Iceberg-compatible output.
- Delta-compatible output.
- JSONL/CSV utility output.

### Substrait-compatible thinking

ShardLoom should design plan IR so future Substrait-compatible import/export is possible, while maintaining these constraints:

- No Substrait dependency now.
- No full Substrait compatibility guarantee.
- Substrait-like import must not imply fallback execution.
- Unsupported imported plans must fail explicitly.

### Plan import

Future import paths may support:

- ShardLoom-native plan format.
- Agent-generated plan specs.
- Substrait-like logical plan subset.
- Config-defined jobs.

Imported plans must be validated for supported operations, functions, types, capabilities, effects, outputs, and no-fallback policy.

### Plan export

Future export paths may support:

- ShardLoom-native plan JSON.
- Explain report.
- Estimate report.
- Agent-readable plan.
- Substrait-like export where supported.

Export must not leak secrets.

### Plan validation

Validation should check:

- Node ids.
- Schema compatibility.
- Expression compatibility.
- Function availability.
- Kernel availability.
- Vortex IO requirements.
- Memory/spill requirements.
- Effect permissions.
- Output compatibility.
- No-fallback policy.

### Plan versioning

Machine-readable plan schemas should be versioned before stability promises are made.

### Diagnostics

Plan IR diagnostics should include unsupported node/expression/function/type/effect/output conditions, missing capability, metadata loss, fallback attempted false, and suggested next step.

## Failure behavior

Unsupported plan behavior must fail explicitly with deterministic diagnostics and must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or any fallback execution engine.

## Alternatives considered

- Use Substrait as internal plan IR immediately: rejected.
- Use DataFusion plan types: rejected.
- Use only ad hoc structs: rejected.
- Delay plan IR design: rejected.

## Risks

- Premature schema stabilization may restrict optimizer evolution.
- Plan import pressure may encourage unsupported-node permissiveness.
- Agent-facing exports can leak secrets if redaction contracts are incomplete.
- Layer boundaries can blur without strict validation and diagnostics.

## Acceptance criteria

- RFC-approved native plan layer model is documented.
- Node identity and boundary metadata expectations are explicit.
- Import/export constraints preserve no-fallback policy.
- Substrait-compatible direction is documented without dependency commitment.
- Validation, diagnostics, and failure behavior expectations are explicit.

## Verification plan

- Review for consistency with no-fallback and Vortex-first RFCs.
- Review for consistency with diagnostics/capabilities, modular extensibility, streaming, memory/spill, and correctness RFCs.
- Define follow-up implementation issues for node schema, validation passes, and diagnostic codes.
- Verify that import/export direction does not imply execution fallback.

## Open questions

- Which plan layer should be the first machine-readable external format?
- What minimum redaction policy is required for exported agent-readable plans?
- How should adaptive runtime updates map back to stable node ids across retries?
- What subset of Substrait-like constructs should be considered for first interoperability experiments?

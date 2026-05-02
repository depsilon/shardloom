# RFC 0021: Expression Engine and Kernel Registry

## Status

Draft

## Summary

This RFC defines ShardLoom's native expression engine and kernel registry.

ShardLoom needs a native expression and kernel system that can evaluate filters, projections, aggregates, UDFs, encoded kernels, decoded reference kernels, and future vector/model/API-related functions without relying on Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

## Context

ShardLoom's execution advantage depends on avoiding unnecessary read, decode, copy, materialization, shuffle, and distribution.

Expressions are where these decisions become concrete. Expression evaluation must understand whether a result can be produced from metadata, encoded Vortex-native data, partial decode, full materialization, pure UDF evaluation, effectful operation boundaries, or explicit unsupported diagnostics.

Without a clear expression/kernel contract, future SQL frontends, UDF APIs, plugins, bindings, plan import/export, and production execution will drift toward hidden fallback behavior. That is explicitly disallowed.

## Goals

- Define a native expression IR.
- Define expression categories and expected semantics.
- Define scalar values and type behavior.
- Define function signatures and capability metadata.
- Define kernel capability declarations.
- Define encoded and decoded kernel roles.
- Define null semantics categories.
- Define explicit type coercion policy.
- Define kernel registry behavior.
- Define deterministic kernel selection behavior.
- Define reference-kernel testing behavior.
- Define UDF integration boundaries.
- Preserve no-fallback execution policy.

## Non-goals

- Implement expression evaluation in this RFC.
- Add dependencies.
- Define final SQL parsing behavior.
- Implement UDF runtimes.
- Implement SIMD/JIT execution.
- Add Spark/DataFusion or any other fallback execution engine.

## Core principle

ShardLoom's expression engine should select the cheapest correct evaluation mode:

1. Metadata-only kernel.
2. Encoded Vortex-native kernel.
3. Partial-decode kernel.
4. Late-materialized decoded kernel.
5. Full-materialization kernel.
6. Explicit unsupported diagnostic.

External engine fallback is not allowed.

## Detailed design

### Expression IR

Expression IR should include typed, null-aware, deterministic/effect-aware nodes that are serializable or explainable and independent of external execution engines.

Expected node families:

- Literal.
- Column reference.
- Alias.
- Cast.
- Unary operation.
- Binary operation.
- Comparison.
- Boolean conjunction/disjunction.
- IsNull.
- IsNotNull.
- Scalar function call.
- Aggregate function call.
- Table function call.
- UDF call.
- External effect call.
- Model call.
- Embedding call.
- Vector search call.
- Unsupported.

### Scalar values

Initial scalar model should include:

- Null.
- Boolean.
- Int64.
- UInt64.
- Float64.
- Utf8.
- Binary.
- Date32.
- TimestampMicros.
- Decimal when supported.
- Struct/List when supported.
- Extension.

### Type system

The engine should preserve explicit distinctions between:

- Logical DType.
- Physical encoding.
- Physical layout.
- Nullability.
- Extension type.
- Compatibility output type.

Type coercion must be explicit, deterministic, and diagnosable. Unsupported coercion must fail deterministically.

### Null semantics

Kernels should declare one of these null behavior categories:

- NullPropagating.
- NullIgnoring.
- NullAware.
- NullRejecting.
- Custom.
- Unsupported.

### Function signatures

Each function definition should declare:

- Name.
- Input types.
- Output type.
- Null behavior.
- Determinism.
- Purity/effect level.
- Variadic behavior.
- Encoded capability.
- Materialization requirement.
- Cost hints.
- Unsupported diagnostics.

### Function categories

Function categories should include:

- Scalar.
- Predicate.
- Aggregate.
- Window.
- Table.
- UDF.
- Translation.
- External read.
- External write.
- Model call.
- Embedding generation.
- Vector search.

### Kernel capability declarations

Each kernel registration should include:

- Supported logical DTypes.
- Supported encodings.
- Supported layouts.
- Nullability behavior.
- Selection vector support.
- Streaming support.
- Materialization requirement.
- Memory class.
- Spill support.
- Determinism.
- Effect level.
- Output representation.
- Benchmark status.
- Test coverage status.

### Kernel kinds

Kernel kinds should include:

- Metadata kernel.
- Encoded kernel.
- Partial-decode kernel.
- Decoded reference kernel.
- Compatibility kernel.
- Effect kernel.

Decoded reference kernels are allowed for tests and explicit native paths, but must not act as hidden fallback execution.

### Kernel registry

The registry should support:

- Register kernel.
- List kernels.
- Query by function name.
- Query by DType.
- Query by encoding/layout.
- Query by effect level.
- Select best kernel.
- Produce unsupported diagnostic.
- Produce capability report.
- Produce benchmark/test coverage report.

### Kernel selection

Selection should consider:

- Expression.
- Input DTypes.
- Encodings.
- Layouts.
- Nullability.
- Statistics.
- Materialization policy.
- Streaming requirements.
- Memory budget.
- Spill policy.
- Sink requirements.
- Effect permissions.
- User configuration.
- Safety policy.

Selection must be deterministic for a fixed plan, configuration, and capability set.

### UDF integration boundaries

UDF definitions should declare:

- Name.
- Signature.
- Runtime.
- Null behavior.
- Determinism.
- Effect level.
- Encoded support.
- Materialization requirement.
- Resource limits.
- Safety/sandboxing requirements.
- License/provenance notes.

UDF registration without required metadata should fail explicitly.

### Effect integration

LLM/API/embedding/vector calls should be represented as explicit effectful expressions and effect kernels.

Effectful kernels must not run during explain, estimate, doctor, or capabilities flows.

### Testing requirements

Every kernel should eventually include:

- Unit tests.
- Null tests.
- Empty-input tests.
- All-null tests.
- Selection-vector tests.
- Unsupported-case tests.
- Decoded reference comparison.
- Differential tests where appropriate.
- Benchmark coverage if performance is claimed.

### Diagnostics

Kernel diagnostics should include:

- Function name.
- Expression summary.
- DType.
- Encoding.
- Layout.
- Null behavior.
- Selected kernel.
- Rejected kernels if useful.
- Materialization requirement.
- Unsupported reason.
- Fallback attempted false.

## Failure behavior

Unsupported expression/kernel behavior must fail explicitly with deterministic diagnostics and must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or any fallback execution engine.

## Alternatives considered

- Use another engine's expression system: rejected.
- Implement decoded kernels only first: partially accepted for tests/reference paths, but architecture prioritizes metadata/encoded execution.
- Add JIT early: rejected for now.
- Treat UDFs as opaque black boxes: rejected.

## Risks

- Capability metadata may become inconsistent without strict validation.
- Kernel-selection logic can become hard to reason about if cost hints are underspecified.
- Early over-generalization may slow incremental implementation.
- Effectful function growth can erode safety if boundaries are not strictly enforced.

## Acceptance criteria

- RFC-approved expression IR node categories exist as architecture contract.
- RFC-approved function and kernel metadata contract exists.
- Kernel kind and selection order policy is explicit and no-fallback.
- UDF/effect boundaries are explicit and diagnosable.
- Diagnostics and testing expectations are defined.

## Verification plan

- Review for consistency with RFC 0002 no-fallback policy.
- Review for consistency with Vortex-first IO principles.
- Cross-check with diagnostics, modular extensibility, and testing RFCs.
- Add implementation tracking issues for IR structs, registry APIs, and diagnostic codes.
- Validate that no requirement implies external execution fallback.

## Open questions

- Which cost model dimensions are mandatory in the first kernel selector iteration?
- How should extension scalar types be versioned for plugin/UDF ecosystems?
- What minimum effect policy model is required before external-call expressions are enabled?
- Which decoded reference kernels should be required before each encoded kernel can be marked production-ready?

# RFC 0002: No-Fallback Execution and Native Vortex I/O Contract

## Summary

This RFC defines a hard architectural boundary for ShardLoom:

- ShardLoom is standalone from external query-engine fallback.
- ShardLoom must not use Spark, DataFusion, DuckDB, Polars, Velox, Trino,
  Dask, Ray, or Vortex query-engine integrations as fallback execution engines.
- Upstream Vortex array, compute, scan, source, and sink APIs may be native
  providers when approved, feature-gated, version-recorded, policy-admitted,
  and certificate-backed.
- Unsupported execution plans must fail explicitly with clear diagnostics.
- Vortex is a first-class native input format and first-class native output format.
- Vortex output is the highest-fidelity persistence target.
- Parquet, Arrow IPC, Iceberg-compatible files, and Delta-compatible files are translation/export targets that may degrade physical optimization metadata.

This document is architecture policy only. It does not define implementation details.

## Motivation

ShardLoom exists to execute directly on Vortex-native layouts with predictable behavior and optimization semantics. Silent delegation to external engines would weaken correctness guarantees, hide capability gaps, and make performance or behavior claims difficult to reason about.

A strict no-fallback policy preserves architectural clarity:

- Capability boundaries are explicit.
- Missing features are visible and actionable.
- Native Vortex semantics remain the primary design center.

## Scope

This RFC governs:

- Execution policy for query and plan evaluation inside ShardLoom.
- Contract for native input/output formats.
- Allowed roles for external engines.

This RFC does not govern:

- Detailed planner/operator implementation.
- Wire protocol and client API design.
- Release, packaging, or deployment procedures.

## Definitions

### Fallback execution

Fallback execution is any runtime behavior where ShardLoom, after accepting a plan, delegates all or part of plan execution to another execution engine because ShardLoom cannot execute that plan natively.

Examples of fallback execution (disallowed):

- Routing unsupported operators to Spark.
- Translating plan fragments to DataFusion for execution.
- Handing unresolved plan nodes to another engine transparently.

### Format translation

Format translation is the explicit conversion of already-computed ShardLoom results into another persistence or interchange format.

Examples of format translation (allowed):

- Writing computed results to Parquet.
- Exporting result batches through Arrow IPC.
- Emitting Iceberg-compatible or Delta-compatible files from ShardLoom-produced results.

Format translation is not execution fallback as long as execution itself remains native to ShardLoom.

### Benchmark comparison

Benchmark comparison is offline or test-time measurement that compares ShardLoom behavior, correctness, or performance against other systems.

Examples of benchmark comparison (allowed):

- Running equivalent workloads on Spark, DataFusion, DuckDB, Polars, or Velox for baseline metrics.
- Using external systems as migration references to evaluate parity.

Benchmark comparison does not grant permission to execute production plans through those systems from within ShardLoom.

## Architecture policy

### 1) Standalone engine requirement

ShardLoom must remain standalone from external query-engine fallback. It owns
admission, plan compilation, optimization policy, diagnostics, capability
status, and certificate semantics.

Standalone does not mean isolated from upstream Vortex compute. Upstream Vortex
array, compute, scan, source, and sink APIs may serve as Vortex-native execution
providers when they are invoked through approved ShardLoom boundaries,
feature-gated, version-recorded, policy-admitted, and certificate-backed.

### 2) No Spark or DataFusion execution fallback

ShardLoom must not use Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask,
Ray, Vortex query-engine integrations, or similar systems as execution fallback
engines under any mode, including "temporary", "compatibility", or
"best-effort" modes.

### 3) Explicit failure for unsupported execution plans

If a plan cannot be executed natively, ShardLoom must fail explicitly and deterministically.

Failure diagnostics should be clear enough for operators and developers to act on, including:

- Which plan/operator feature is unsupported.
- Why native execution cannot proceed.
- A stable error category/code suitable for automation.

### 4) Allowed role of external systems

Spark, DataFusion, DuckDB, Polars, and Velox may be used only for:

- Benchmark baselines.
- Migration references.
- Optional external interoperability targets.

They are not allowed as hidden or automatic execution backends for accepted ShardLoom plans.

### 5) Vortex native input contract

Vortex is a first-class native input format. ShardLoom planning and execution architecture should treat Vortex as a primary source format, not an adapter-only path.

### 6) Vortex native output contract

Vortex is a first-class native output format and the highest-fidelity persistence target for ShardLoom-native execution semantics and optimization metadata.

### 7) Translation/export output contract

Parquet, Arrow IPC, Iceberg-compatible files, and Delta-compatible files are translation/export targets.

These targets may not preserve all Vortex-native physical optimization metadata. Loss or transformation of physical-level metadata in export paths is acceptable when explicitly treated as translation.

## Non-goals

- Defining a temporary hybrid fallback mode.
- Defining automatic operator offload to external engines.
- Standardizing feature-parity timelines with Spark, DataFusion, DuckDB, Polars, or Velox.
- Defining physical encoding details for Vortex files.
- Defining implementation APIs or operator internals in this RFC.

## Acceptance criteria for future PRs

A future PR aligns with this RFC only if all of the following are true:

1. **No hidden delegation:** It does not introduce direct or indirect fallback execution to Spark or DataFusion.
2. **Explicit unsupported behavior:** Any unsupported native plan path fails explicitly with a clear diagnostic.
3. **Native execution ownership:** ShardLoom remains the owner of plan execution semantics.
4. **Vortex-first I/O posture:** It preserves Vortex as first-class native input and output.
5. **Export clarity:** Any Parquet/Arrow IPC/Iceberg-compatible/Delta-compatible write path is framed as translation/export, not native-equivalent persistence.
6. **External-engine boundaries:** Usage of Spark, DataFusion, DuckDB, Polars, or Velox is limited to benchmark baseline, migration reference, or optional interoperability roles.
7. **No implementation creep in policy RFCs:** Architecture-policy documents avoid embedding premature implementation details.

## Risks and tradeoffs

- Short-term feature coverage may be narrower because unsupported plans fail instead of delegating.
- User onboarding may require clearer messaging around “unsupported” states.
- Engineering pressure may increase to close native capability gaps quickly.

These tradeoffs are intentional to maintain correctness, architectural integrity, and transparent capability evolution.

## Status

Proposed.

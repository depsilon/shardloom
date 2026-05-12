# RFC 0003: Encoded Segment Model

## Status

Proposed.

## Summary

This RFC defines ShardLoom's architecture-level encoded segment abstraction before Rust trait and
type implementation. The model establishes the contract for segment-oriented planning and execution
across metadata-only evaluation, pruning, encoded evaluation, partial decoding, and full
materialization.

This RFC preserves ShardLoom's core boundaries:

- Standalone execution only.
- No Spark fallback.
- No DataFusion fallback.
- No fallback execution to any external engine.
- Vortex as first-class native input and first-class native output.

## Context

RFC 0001 established the high-level architecture and RFC 0002 established the no-fallback and
Vortex-native I/O policy. The next architectural dependency is a precise segment model that planner
and runtime work can target consistently.

Without an explicit segment model, encoded execution, pruning, and materialization decisions risk
becoming ad hoc. This RFC defines conceptual contracts only; implementation details (traits,
structs, APIs, and kernels) are intentionally deferred.

## Goals

- Define `EncodedSegment` as the core execution abstraction.
- Define segment metadata boundaries needed for planning and pruning.
- Define execution states from metadata-only to fully materialized.
- Define null-handling expectations across encoded and decoded paths.
- Define explicit failure behavior for unsupported encoded operations.
- Define the allowed role of decoded reference behavior in testing.
- Preserve Vortex-native input/output and no-fallback architecture.

## Non-goals

- Implement Rust traits, structs, kernels, or planner nodes.
- Define binary layout details of every Vortex encoding.
- Define distributed scheduling protocol.
- Define storage adapter APIs in full detail.
- Introduce Spark, DataFusion, or any external execution fallback.
- Add dependencies, publish packages, or cut releases.

## Decision

ShardLoom will treat execution data as a stream of `EncodedSegment` units with attached
`SegmentStats`, `SegmentLayout`, and capability metadata. Planning and execution must prefer
lower-cost states (metadata-only, then pruned, then encoded) before decoding. When decoding is
required, it must be scoped by `SelectionVector` and `MaterializationPolicy`.

Unsupported encoded operations must fail explicitly with deterministic diagnostics. Decoded
reference behavior may be used only for correctness validation in test and benchmark harnesses, and
never as production fallback execution.

## Detailed design

### 1. EncodedSegment (concept)

`EncodedSegment` is the minimal independent unit of encoded-columnar execution. Conceptually, it
represents:

- A bounded row domain.
- One or more encoded column representations for that row domain.
- Segment-local metadata required for pruning and capability checks.
- Stable identity needed for deterministic diagnostics and tracing.

Architectural requirements:

- Must be executable without delegating to external engines.
- Must preserve enough structure for encoded predicates/expressions when supported.
- Must support late materialization decisions per requested projection.
- Must be producible from Vortex-native inputs and writable to Vortex-native outputs.

### 2. SegmentStats (concept)

`SegmentStats` represents planner/runtime-visible summary metadata used for metadata-only answers
and segment pruning.

Conceptual fields include (non-exhaustive):

- Row count and null count information.
- Per-column min/max where semantically valid.
- Optional distinctness/cardinality sketches.
- Optional bloom/filter metadata.
- Encoding-level guarantees (e.g., sortedness hints, monotonic domains).
- Confidence/validity markers for optional statistics.

Architectural requirements:

- Statistics semantics must be explicit and deterministic.
- Unknown or missing stats must be represented explicitly (never assumed).
- Stats must be safe for pruning only when correctness is preserved.
- Null-aware semantics must be defined for each statistic kind.

### 3. SegmentLayout (concept)

`SegmentLayout` describes how encoded data and metadata are organized for a segment, independent of
concrete Rust representation.

Conceptual components include:

- Segment key/identity.
- Row range and logical schema binding.
- Column-to-encoding mapping.
- Pointer/locator metadata for encoded payloads.
- Optional auxiliary indexes/sketches.
- Version/capability markers.

Architectural requirements:

- Layout metadata must be sufficient for planning decisions before decode.
- Layout evolution must preserve deterministic capability checks.
- Layout must not imply execution fallback to external engines.

### 4. SelectionVector (concept)

`SelectionVector` is the row-selection contract used to carry surviving row positions between
execution stages.

Conceptual behavior:

- Represents a subset of rows within a segment domain.
- May originate from metadata pruning, encoded predicate evaluation, or decoded evaluation.
- Can be composed/intersected across predicates.
- Enables selective decoding and late materialization.

Architectural requirements:

- Selection semantics must preserve row identity deterministically.
- Selection behavior must define stable ordering guarantees.
- Selection must be compatible with null semantics and tri-valued predicate logic.

### 5. MaterializationPolicy (concept)

`MaterializationPolicy` governs if/when/how encoded values are decoded into materialized column
vectors.

Conceptual policy dimensions:

- Required projection columns.
- Required expression outputs.
- Decode granularity (column, page/chunk, or row-subset guided by selection).
- Decode timing (eager for required correctness vs delayed for cost reduction).

Architectural requirements:

- Policy must prioritize late materialization.
- Policy must support partial decode constrained by `SelectionVector` where feasible.
- Policy must preserve deterministic semantics independent of chosen decode boundary.

### 6. EncodedEvalCapability (concept)

`EncodedEvalCapability` declares which operations can execute directly on encoded representations
for a given segment/column/expression class.

Conceptual examples:

- Predicate classes supported in encoded form.
- Comparison domains supported without decode.
- Aggregations supported metadata-only or encoded.
- Operations requiring partial/full decode.

Architectural requirements:

- Capability determination must be explicit and queryable.
- Unsupported operations must fail explicitly if no native path exists.
- Capability checks must not trigger hidden delegation to external engines.

### 7. Execution state model

ShardLoom execution over segments progresses through one of the following states, selected per
operator/segment:

1. **Metadata-only**: result derived entirely from metadata/statistics (no payload decode).
2. **Pruned**: segment eliminated from further work by safe statistics or metadata conditions.
3. **Encoded**: operation executed directly over encoded representation.
4. **Partially decoded**: only required columns/rows are decoded, guided by `SelectionVector` and
   `MaterializationPolicy`.
5. **Fully materialized**: all required rows/columns for downstream semantics are decoded.

Requirements for state transitions:

- Prefer lower-cost states while preserving correctness.
- Transition to higher-cost states only when required by capability or semantics.
- Record deterministic reason for state escalation (for observability and testing).

### 8. Null semantics

Null handling must be first-class in all states:

- Predicate logic must follow explicit three-valued semantics (true/false/unknown).
- Statistics-based pruning must be null-safe; null presence cannot be ignored.
- Encoded predicate evaluation must preserve null semantics exactly.
- `SelectionVector` must represent null-driven filtering outcomes deterministically.
- Metadata-only answers must specify when null semantics make the result indeterminate without
  additional evaluation.

No optimization may change null-observable query semantics.

### 9. Unsupported encoded operations and failure behavior

If an operation is not supported in encoded form for a segment, ShardLoom may:

- Escalate to partial/full decode **within ShardLoom native execution**, if a defined native decoded
  path exists.
- Otherwise fail explicitly with deterministic diagnostics.

Failure diagnostics should include:

- Stable error category/code.
- Operator/expression kind.
- Segment/layout/capability context.
- Why encoded or decoded native evaluation cannot proceed.

Disallowed behavior:

- Silent or automatic delegation to Spark, DataFusion, DuckDB, Polars, Velox, or other engines.

### 10. Decoded reference behavior in testing

Decoded reference behavior may be used in tests to validate correctness of encoded paths, including:

- Reference-equality checks between encoded and decoded native ShardLoom outcomes.
- Differential tests that compare encoded kernels against deterministic decoded reference
  evaluators.

Constraints:

- Reference evaluators are validation tools, not production fallback execution.
- Production execution must not route unsupported plans into reference decoded paths unless those
  paths are defined native execution operators in ShardLoom.

## Alternatives considered

1. **Row-group-only model without explicit encoded capability metadata**
   - Rejected: weakens planner ability to reason about encoded execution and decode escalation.

2. **Always decode early for semantic simplicity**
   - Rejected: conflicts with encoded execution and late materialization architecture goals.

3. **Fallback to external engines for unsupported encoded operations**
   - Rejected: violates RFC 0002 no-fallback policy and weakens deterministic behavior.

4. **Treat Vortex as adapter-only input/output**
   - Rejected: violates Vortex-native first-class contract.

## Risks

- Over-constraining conceptual contracts may slow early implementation.
- Under-specifying capability surfaces may cause inconsistent behavior across operators.
- Null-semantics mistakes in metadata pruning could cause correctness bugs.
- Observability burden may increase due to required deterministic diagnostics.

## Compatibility impact

- Aligns with RFC 0001 and RFC 0002.
- Establishes architecture contracts for future planner/runtime/translation RFCs.
- Does not introduce API, dependency, or binary compatibility changes yet.

## Acceptance criteria

Future implementation PRs claiming compliance with this RFC must satisfy all of the following:

1. Define concrete types/traits for segment, stats, layout, capability, selection, and
   materialization policy concepts.
2. Implement explicit execution-state transitions (metadata-only, pruned, encoded, partial decode,
   full materialization).
3. Demonstrate null-safe behavior across pruning and encoded/decode evaluation paths.
4. Emit deterministic diagnostics for unsupported encoded operations.
5. Prove no external fallback execution path is introduced.
6. Preserve Vortex-native input and Vortex-native output as first-class paths.
7. Include behavior tests for success and failure semantics.

## Verification plan

- RFC review for architecture consistency with RFC 0001 and RFC 0002.
- Design-review checklist verifying no-fallback and Vortex-native contracts.
- Future implementation validation with required repository commands:
  - `cargo fmt --all -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --all-targets`
- Future correctness tests should include:
  - Metadata-only correctness cases.
  - Pruning correctness with null-containing segments.
  - Encoded vs decoded reference equivalence tests.
  - Unsupported-operation deterministic failure tests.

## Open questions

- What minimum capability taxonomy is sufficient for MVP planning decisions?
- Which statistics are mandatory versus optional for first implementation milestones?
- What observability schema should encode execution-state transition reasons?
- How should segment identity be represented across object-store partition boundaries?

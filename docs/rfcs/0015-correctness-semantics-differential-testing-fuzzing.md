# RFC 0015: Correctness, Semantics, Differential Testing, and Fuzzing

## Status

Draft

## Summary

This RFC defines ShardLoom's correctness, semantics, differential testing, and fuzzing strategy.

ShardLoom must be correct before it is fast. Fast wrong answers are unacceptable.

ShardLoom's execution model is ambitious: Vortex-native encoded execution, metadata-only answers, pruning, partial decode, late materialization, streaming, spill, UDFs, SQL, unstructured data, LLM/API effects, embeddings, and compatibility exports.

That complexity requires a rigorous correctness strategy before serious execution kernels are implemented.

## Context

ShardLoom is designed to avoid unnecessary reads, decoding, copying, materialization, shuffle, and distribution. These optimizations can create correctness risk if they are not tested against stable semantics.

Examples of correctness risks:

- Incorrect segment pruning.
- Incorrect null handling.
- Incorrect type coercion.
- Incorrect min/max interpretation.
- Incorrect dictionary-encoded comparison.
- Incorrect partial decode selection.
- Incorrect late materialization.
- Incorrect metadata-only answer.
- Incorrect output translation.
- Incorrect spill/restore behavior.
- Incorrect UDF null behavior.
- Incorrect SQL semantics.
- Incorrect timestamp/decimal behavior.
- Incorrect unsupported diagnostics.

ShardLoom needs a correctness system that catches these before performance claims are made.

## Goals

- Define correctness as a first-class engineering goal.
- Define semantic areas that require explicit tests.
- Define decoded reference behavior.
- Define differential testing against external engines as a test oracle only.
- Define SQLLogicTest-style fixture strategy.
- Define randomized and fuzz testing strategy.
- Define edge-case requirements.
- Define unsupported behavior testing.
- Define output translation correctness.
- Preserve no-fallback execution.

## Non-goals

- Do not implement tests in this RFC.
- Do not add test dependencies in this RFC.
- Do not define final SQL semantics in full.
- Do not implement SQL parsing.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not require every future test strategy in the first implementation.
- Do not use another engine as production execution fallback.

## Core principle

ShardLoom should never trade correctness for performance.

The execution optimizer may avoid work only when correctness can be proven or conservatively preserved.

Incorrect pruning, incorrect metadata-only answers, incorrect null handling, and incorrect materialization boundaries are correctness bugs.

## Semantic areas

ShardLoom must eventually define and test semantics for these areas.

### Null semantics

Null behavior must be explicit for:

- IS NULL.
- IS NOT NULL.
- Equality.
- Inequality.
- Range comparisons.
- Aggregates.
- Joins.
- UDFs.
- Predicate pruning.
- Metadata-only answers.
- Encoded kernels.
- Output translation.

Null semantics must be tested for:

- Empty input.
- All-null input.
- No-null input.
- Mixed-null input.
- Nulls with min/max statistics.
- Nulls in dictionary or encoded layouts.

### Type semantics

Type behavior must be explicit for:

- Boolean.
- Signed integers.
- Unsigned integers.
- Floating point.
- UTF-8 strings.
- Binary.
- Dates.
- Timestamps.
- Decimals when supported.
- Structs.
- Lists.
- Extension types.

Type coercion must not be guessed silently.

Unsupported coercions must fail explicitly.

### Floating point semantics

Floating-point behavior must account for:

- NaN.
- Infinity.
- Negative zero.
- Precision.
- Ordering assumptions.
- Aggregation stability.

If ShardLoom does not support a float semantic safely, it must say so.

### Temporal semantics

Date and timestamp behavior must account for:

- Time units.
- Time zones if supported.
- Date ranges.
- Timestamp precision.
- Comparison semantics.
- Output translation loss.

### String semantics

String behavior must account for:

- UTF-8 validity.
- Binary vs text.
- Case sensitivity.
- Collation assumptions.
- Prefix/contains predicates if supported.
- Encoded string representations.

### Nested data semantics

Struct/list behavior must account for:

- Null parent vs null child.
- Empty list vs null list.
- Nested field projection.
- Nested field pruning.
- Unsupported nested operations.

## Metadata-only correctness

Metadata-only execution is allowed only when the metadata is sufficient and exact enough.

Examples:

- Count rows from known row count.
- Count nulls from exact null count.
- Min/max from exact min/max statistics.
- Predicate truth from constant segment metadata.
- Prune from exact range proof.

Metadata-only execution must not assume missing statistics prove absence.

## Pruning correctness

Pruning is only correct when a segment can be proven irrelevant.

Pruning rules must be conservative.

If statistics are missing, approximate, or insufficient, ShardLoom must use native execution or fail explicitly if native execution is unsupported.

Incorrect pruning is a critical correctness bug.

## Encoded execution correctness

Encoded execution must be compared against decoded reference behavior.

Every encoded kernel should eventually have:

- Supported DTypes.
- Supported encodings.
- Supported layouts.
- Null behavior.
- Selection vector behavior.
- Reference comparison.
- Edge-case tests.
- Unsupported diagnostics.

## Decoded reference behavior

ShardLoom may use decoded reference implementations for testing.

Decoded reference behavior is allowed for:

- Unit tests.
- Fuzz tests.
- Differential checks.
- Debugging.
- Correctness validation.

Decoded reference behavior must not become hidden production fallback execution.

## Differential testing

ShardLoom may compare results against external engines for correctness testing.

Allowed as test/comparison oracles:

- Spark.
- DataFusion.
- DuckDB.
- Polars.
- Velox.
- Other relevant systems.

These engines must not be used as runtime fallback execution.

Differential testing should record:

- Engine name.
- Engine version.
- Query or operation.
- Input dataset.
- Expected result.
- Actual result.
- Semantic differences.
- Known incompatibilities.
- Repro instructions.

If external engines disagree, ShardLoom must not blindly choose one. The semantic contract must be clarified.

## SQLLogicTest-style fixtures

ShardLoom should eventually support text fixtures for query/result testing.

Fixtures should include:

- Simple scans.
- Filters.
- Projections.
- Aggregates.
- Joins when supported.
- Nulls.
- Empty inputs.
- Unsupported diagnostics.
- Output translation behavior.

SQLLogicTest-style testing is valuable because it makes many small query correctness cases easy to read and maintain.

## Randomized testing

ShardLoom should eventually support generated data and randomized operation testing.

Randomized tests should vary:

- Row counts.
- Null density.
- Cardinality.
- Encodings.
- Segment sizes.
- Sort order.
- Statistics availability.
- Predicate forms.
- Projection width.
- Materialization policy.
- Output target.

Randomized tests should use deterministic seeds and report reproduction instructions.

## Fuzz testing

ShardLoom should eventually use fuzzing for:

- Expression evaluation.
- Encoded predicates.
- Selection vectors.
- Pruning decisions.
- Statistics interpretation.
- Type coercion.
- Translation reports.
- Manifest parsing when implemented.
- Vortex adapter boundaries when implemented.

Fuzz failures must produce reproducible seeds.

## Unsupported behavior testing

Unsupported behavior must be tested.

Unsupported tests should verify:

- Deterministic diagnostic code.
- Clear human message.
- Fallback attempted is false.
- Suggested next step exists when possible.
- No external engine is invoked.
- No partial side effect occurs.

## Output translation correctness

Output translation tests should verify:

- Values are preserved.
- Nullability is preserved.
- Schema is preserved or explicitly changed.
- Metadata preservation/loss is reported.
- Vortex output remains highest-fidelity.
- Compatibility output loss is explicit.
- Unsupported output fails deterministically.

## Spill and recovery correctness

When spill is implemented, tests must verify:

- Spill preserves values.
- Spill preserves grouping/join semantics.
- Spill cleanup occurs.
- Spill diagnostics are deterministic.
- Memory pressure does not corrupt results.
- OOM-safe failure occurs before unsafe behavior where possible.

## LLM/API/effect correctness

Effectful operations must be tested differently from deterministic execution.

Tests should verify:

- Effects do not run during explain.
- Effects do not run during estimate.
- Effects do not run during dry run.
- External writes require explicit enablement.
- Cost/timeouts/retries are represented.
- Output schemas are validated.
- Diagnostics are stable.

## Test categories

ShardLoom should eventually maintain these categories:

- Unit tests.
- Integration tests.
- Fixture tests.
- SQLLogicTest-style tests.
- Differential tests.
- Property tests.
- Fuzz tests.
- Golden diagnostics tests.
- Benchmark correctness checks.
- Translation round-trip tests.

## Golden diagnostics

Diagnostics should be tested as stable behavior.

Golden diagnostics can verify:

- Diagnostic code.
- Category.
- Severity.
- Fallback status.
- Feature.
- Reason.
- Suggested next step.

The full human message can evolve, but machine-readable fields should be stable.

## Failure behavior

When correctness cannot be guaranteed, ShardLoom must fail explicitly.

Failures must not call Spark, DataFusion, DuckDB, Polars, Velox, or another engine as fallback.

## Alternatives considered

### Prioritize performance first and correctness later

Rejected.

This leads to fast wrong answers and destroys trust.

### Use another engine as the correctness runtime

Rejected for production execution.

External engines can be test oracles, not fallback execution.

### Avoid SQL-style testing until SQL is implemented

Rejected.

ShardLoom can still use fixture-based operation testing before full SQL exists.

### Skip fuzzing early

Partially accepted.

Fuzzing does not need to be first, but the architecture should prepare for it.

## Risks

- Correctness work slows early performance development.
- External engines may disagree on semantics.
- Fuzzing can uncover complex bugs that are time-consuming.
- Golden tests can become brittle if over-specified.
- SQL semantics may expand scope.
- Testing encoded paths requires careful reference behavior.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Correctness is first-class.
- Encoded execution must be checked against decoded reference behavior.
- Differential testing is allowed only as testing/comparison.
- External engines are not fallback execution.
- Null/type/edge semantics require explicit tests.
- Unsupported diagnostics require tests.
- Metadata-only and pruning behavior must be conservative.
- Performance claims require correctness validation.

## Verification plan

Future implementation PRs should verify:

- New kernels include edge-case tests.
- New pruning rules include conservative correctness tests.
- New diagnostics include stable code/category/fallback status tests.
- New translation behavior includes value and metadata tests.
- New benchmarks validate correctness before reporting performance.
- No Spark or DataFusion dependency is introduced for fallback execution.

## Open questions

- Which fixture format should ShardLoom implement first?
- Should SQLLogicTest-style fixtures be introduced before SQL parsing?
- Which external engine should be the first differential oracle?
- Should property testing be added before fuzzing?
- How should semantic differences across engines be recorded?
- What should be the first fuzz target?

# RFC 0006: Statistics, Pruning, and Metadata-Only Execution

## Status

Draft

## Summary

This RFC defines ShardLoom's statistics, pruning, and metadata-only execution model.

ShardLoom should avoid work before optimizing work. The first path to large performance and cost gains is not faster compute. It is eliminating unnecessary reads, decode, materialization, and movement.

## Context

Spark and other large execution engines often perform substantial work before discovering that much of the data was unnecessary.

Vortex-native execution gives ShardLoom an opportunity to plan from metadata and encoded segment statistics before reading data. This can make some workloads nearly free relative to distributed compute.

ShardLoom must be conservative. Incorrect pruning is a correctness bug.

## Goals

- Define metadata-only execution.
- Define segment pruning.
- Define statistics-aware predicate evaluation.
- Define conservative proof requirements.
- Define pruning diagnostics.
- Define execution states.
- Define failure behavior when statistics are missing or insufficient.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement Rust code in this RFC.
- Do not define all possible statistics.
- Do not define final cost model behavior.
- Do not add Spark.
- Do not add DataFusion.
- Do not require every Vortex file to have every statistic.
- Do not allow incorrect pruning for performance.

## Decision

ShardLoom should introduce a statistics and pruning model based on conservative decisions.

A segment may be:

- Answered from metadata.
- Pruned.
- Read in encoded form.
- Partially decoded.
- Fully materialized.

Pruning decisions must be explainable and must not change query results.

## Core concepts

### SegmentStats

Statistics associated with an encoded segment or column.

Possible statistics include:

- Row count.
- Null count.
- Min value.
- Max value.
- True count.
- False count.
- Distinct count estimate.
- Run count.
- Constant-value indicator.
- Sorted indicator.
- Byte size.
- Encoded size.
- Uncompressed size estimate.
- Value range.
- Bloom filters or future predicate indexes.

This RFC does not require all statistics to exist.

### PredicateProof

A conservative proof about a predicate and a segment.

Possible outcomes:

- PredicateAlwaysTrue.
- PredicateAlwaysFalse.
- PredicateMayMatch.
- PredicateUnknown.
- PredicateUnsupported.

Only proven false predicates may prune data.

Only proven true or computable metadata cases may avoid data reads.

### PruningDecision

A decision made by the planner or scan layer.

Possible decisions:

- ReadSegment.
- PruneSegment.
- MetadataOnlyAnswer.
- NeedEncodedEvaluation.
- NeedPartialDecode.
- NeedMaterialization.
- Unsupported.

### MetadataAnswer

A result computed entirely from metadata.

Examples:

- Count rows from row count.
- Count nulls from null count.
- Determine min/max from valid statistics.
- Determine boolean count from true/false counts.
- Determine equality on constant segments.

Metadata answers are allowed only when metadata is exact enough for the requested result.

### ExecutionState

A diagnostic state describing how much work was performed.

Possible states:

- MetadataOnly.
- Pruned.
- EncodedEvaluated.
- PartiallyDecoded.
- FullyMaterialized.
- Unsupported.

ExecutionState should be visible in plan diagnostics and eventually benchmark output.

## Rules

- Check metadata-only answers before reading data.
- Check pruning before reading segment bytes.
- Use statistics conservatively.
- Missing statistics must not be treated as proof.
- Null semantics must be handled explicitly.
- If pruning cannot be proven safe, execute through a ShardLoom-native path.
- Do not call another engine for unpruned or unsupported segments.
- Track why each segment was read, pruned, or answered from metadata.
- Preserve statistics when writing Vortex output where possible.

## Null semantics

Null handling must be explicit.

Pruning logic must account for:

- All-null segments.
- Mixed-null segments.
- Non-null predicates.
- Null-sensitive comparisons.
- IS NULL.
- IS NOT NULL.
- Three-valued logic if SQL semantics are introduced.

Incorrect null handling can produce wrong answers and must be treated as a correctness bug.

## Predicate examples

### Range predicate

For a predicate such as:

value > 100

A segment may be pruned only if statistics prove max(value) <= 100 and null semantics do not require reading the segment.

### Equality predicate

For a predicate such as:

status = 'closed'

A segment may be pruned if statistics or dictionary metadata prove that 'closed' cannot appear.

A constant segment may be answered from metadata if it is known to contain only 'closed' or known not to contain 'closed'.

### IS NULL predicate

If null_count is zero, IS NULL can prune the segment.

If null_count equals row_count, IS NULL may match all rows.

If null_count is missing, the segment must be evaluated through a native path.

## Diagnostics

Pruning and metadata-only behavior should be explainable.

Diagnostics should eventually report:

- Number of segments considered.
- Number of segments pruned.
- Number of segments metadata-answered.
- Number of segments read.
- Number of bytes avoided.
- Reason for pruning.
- Reason pruning was not possible.
- Missing statistics.

## Failure behavior

Unsupported statistics behavior must fail explicitly if the query requires it.

Examples:

- Required statistic unavailable.
- Statistic present but not exact enough.
- Unsupported predicate type.
- Unsupported DType comparison.
- Unsupported null semantics.
- Unsupported nested-field pruning.

Failures must not invoke Spark, DataFusion, DuckDB, Polars, or another fallback engine.

## Alternatives considered

### Always read and filter encoded data

Rejected as default.

Encoded evaluation is useful, but metadata and pruning should happen first.

### Always decode and filter

Rejected.

This defeats ShardLoom's purpose.

### Trust statistics optimistically

Rejected.

Incorrect pruning is a correctness bug.

### Use Spark/DataFusion when pruning is unsupported

Rejected.

This violates the no-fallback execution policy.

## Risks

- Statistics may be missing or inconsistent.
- Null semantics may be difficult.
- Conservative pruning may leave performance on the table.
- Too many diagnostics may complicate APIs.
- Some metadata-only answers may require exactness guarantees not available from all sources.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Metadata-only execution is a first-class path.
- Segment pruning is a first-class path.
- Statistics must be used conservatively.
- Missing stats do not prove absence.
- Null semantics are required for correctness.
- Execution states must be diagnosable.
- Unsupported pruning behavior fails explicitly or uses ShardLoom-native execution.
- Spark/DataFusion fallback remains prohibited.

## Verification plan

Future implementation PRs should test:

- Metadata-only row counts.
- Empty segments.
- All-null segments.
- Mixed-null segments.
- Constant segments.
- Sorted/ranged segments.
- Missing statistics.
- Unsupported predicates.
- Conservative fallback to ShardLoom-native execution.
- Deterministic diagnostics.
- No external engine fallback.

## Open questions

- Which statistics should be required for first implementation?
- How should exact vs approximate statistics be represented?
- Should pruning decisions be part of explain output immediately?
- How should nested fields participate in pruning?
- How should statistics be preserved in Vortex output?

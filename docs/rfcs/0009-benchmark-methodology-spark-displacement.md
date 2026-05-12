# RFC 0009: Benchmark Methodology and Spark-Displacement Workloads

## Status

Draft

## Summary

This RFC defines ShardLoom's benchmark methodology and Spark-displacement workload categories.

ShardLoom should not claim to overtake Spark, DataFusion, DuckDB, Polars, Velox, or other engines
without reproducible evidence. Performance claims must be specific, measured, and tied to workload
classes.

## Context

ShardLoom's near-term competition with single-node engines may be easier than full Spark
displacement.

Spark displacement requires more than fast scans. It requires credible results on:

- Massive object-store scans.
- Incremental workloads.
- Large joins.
- Aggregations.
- Feature generation.
- Wide-table projections.
- Changed-segment recomputation.
- Native Vortex output.
- Large writes.
- Retryable execution.
- Cost-sensitive workloads.

ShardLoom must establish benchmark credibility early.

## Goals

- Define benchmark principles.
- Define benchmark workload classes.
- Define required metrics.
- Define baseline rules.
- Define reproducibility requirements.
- Define Spark-displacement claims.
- Define single-node competition claims.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement benchmarks in this RFC.
- Do not make performance claims in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not add any execution fallback.
- Do not require all benchmarks before implementation starts.
- Do not define marketing claims.

## Decision

ShardLoom should use a benchmark methodology that measures:

- Correctness.
- Bytes read.
- Bytes decoded.
- Rows scanned.
- Rows materialized.
- Segments pruned.
- Metadata-only answers.
- CPU time.
- Wall time.
- Peak memory.
- Allocations.
- Object-store requests.
- Output bytes.
- Cloud-cost proxy.
- Cold-cache and warm-cache behavior.

Benchmarks should compare against relevant systems as baselines only.

## Baseline policy

The following systems may be used as benchmark baselines:

- Spark.
- DataFusion.
- DuckDB.
- Polars.
- Velox.
- Vortex integrations.
- Other relevant engines.

These systems must not be used as fallback execution engines.

Benchmark baselines must include:

- Version.
- Configuration.
- Hardware.
- Dataset format.
- Query.
- Runtime settings.
- Cold-cache or warm-cache status.

## Benchmark workload classes

### Single-node encoded execution

Purpose:

Measure whether ShardLoom can challenge DataFusion, DuckDB, Polars, or Vortex integrations on local
or medium-scale data.

Example workloads:

- Projection-heavy scans.
- Filter-heavy scans.
- Wide-table column pruning.
- Low-cardinality predicates.
- Metadata-only counts.
- Min/max queries.
- Simple group-by.
- Native Vortex output.

### Massive object-store scans

Purpose:

Measure performance on object-store-backed datasets where IO, metadata, range reads, and pruning
dominate.

Example workloads:

- Large fact table scans.
- Date-range filters.
- Wide tables with sparse projection.
- Segment pruning.
- Cold-cache reads.
- Warm-cache reads.
- Object-store request count.

### Incremental recomputation

Purpose:

Measure whether ShardLoom can avoid full recomputation for changed data.

Example workloads:

- Append-only updates.
- Replaced segment updates.
- Changed partition updates.
- Feature regeneration for changed segments.
- Manifest diff planning.
- Reuse of unchanged segments.

### Large joins

Purpose:

Measure whether ShardLoom can reduce shuffle and movement.

Example workloads:

- Large fact to small dimension join.
- Large fact to medium dimension join.
- Skewed key join.
- Broadcast-eligible join.
- Range-aware join.
- Semi-join reduction.

### Aggregation and grouping

Purpose:

Measure encoded and partial aggregation behavior.

Example workloads:

- Count.
- Count distinct approximation if supported later.
- Sum by low-cardinality key.
- Sum by high-cardinality key.
- Group-by on dictionary-compatible values.
- Segment-local partial aggregation.

### Native output and translation

Purpose:

Measure output cost and fidelity.

Example workloads:

- Vortex output.
- Arrow IPC output.
- Parquet output.
- Metadata preservation.
- Translation loss reporting.
- Materialization cost.

### Failure and unsupported behavior

Purpose:

Measure diagnostic clarity and correctness of unsupported behavior.

Example workloads:

- Unsupported encoding.
- Unsupported DType.
- Unsupported plan shape.
- Missing statistics.
- Shuffle required but unsupported.
- Distributed required but unavailable.

## Required benchmark metadata

Every published benchmark must include:

- Dataset name.
- Dataset scale.
- Schema.
- Row count.
- Column count.
- File count.
- Segment count if available.
- Storage format.
- Compression/encoding.
- Query or workload.
- Engine versions.
- Hardware.
- Operating system.
- Runtime configuration.
- Object-store provider if applicable.
- Cold-cache or warm-cache status.
- Number of runs.
- Summary statistics.
- Correctness validation method.
- Limitations.

## Required metrics

Benchmarks should collect as many of these as possible:

- Wall time.
- CPU time.
- Peak memory.
- Allocations.
- Bytes read.
- Bytes decoded.
- Bytes written.
- Rows scanned.
- Rows materialized.
- Segments considered.
- Segments pruned.
- Segments metadata-answered.
- Object-store requests.
- Output files.
- Output bytes.
- Cost proxy.
- Error diagnostics for unsupported workloads.

## Claim levels

### Local competition claim

ShardLoom may claim local or single-node competitiveness only for workloads measured against
relevant local engines with reproducible results.

### Spark-displacement claim

ShardLoom may claim Spark displacement only for specific workload classes where it demonstrates:

- Correctness.
- Comparable or better wall time.
- Lower bytes read or decoded.
- Lower memory or resource use.
- Lower cost proxy.
- Successful output behavior.
- No fallback execution.
- Reproducibility.

A broad claim such as "ShardLoom replaces Spark" is not acceptable without narrowing the workload.

### Cost claim

ShardLoom may claim lower cost only when a cost proxy is defined.

A cost proxy may include:

- Object-store requests.
- Bytes read.
- Compute time.
- Memory.
- Output bytes.
- Worker count.
- Runtime duration.

## Correctness requirements

Benchmarks must validate correctness.

Acceptable approaches include:

- Hand-verified expected result.
- Deterministic reference output.
- Comparison against another engine as a benchmark oracle.
- Property-based or generated test data.

Comparison against another engine is allowed for benchmark validation. It is not fallback execution.

## Failure behavior benchmarks

Unsupported behavior should be benchmarked for diagnostics.

For unsupported workloads, ShardLoom should show:

- Clear error.
- Deterministic error.
- No fallback engine invocation.
- Explanation of missing capability.
- Suggested future capability if appropriate.

## Alternatives considered

### Publish performance claims early

Rejected.

ShardLoom should not make claims before benchmark methodology exists.

### Benchmark only against Spark

Rejected.

ShardLoom must also understand single-node and native-engine baselines.

### Benchmark only wall-clock time

Rejected.

ShardLoom's thesis depends on reducing reads, decode, movement, materialization, and cost, not only
wall time.

### Use baselines as fallback engines

Rejected.

Benchmark comparison is allowed. Fallback execution is not.

## Risks

- Benchmarks may become too complex too early.
- Baselines may be configured unfairly.
- Workloads may be cherry-picked.
- Hardware differences may obscure results.
- Object-store benchmarks may be noisy.
- Spark-displacement claims may be overbroad.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Performance claims require reproducible benchmarks.
- Benchmarks must include correctness validation.
- Metrics must include more than wall time.
- Spark, DataFusion, DuckDB, Polars, and Velox are baselines only.
- Spark-displacement claims must be workload-specific.
- No fallback execution is permitted.

## Verification plan

Future benchmark PRs should include:

- Reproduction script.
- Dataset description.
- Query/workload definitions.
- Engine versions.
- Hardware/runtime configuration.
- Correctness validation.
- Metrics collected.
- Limitations.
- No fallback execution.

## Open questions

- Which benchmark should be implemented first?
- What synthetic datasets should ShardLoom generate?
- When should object-store benchmarks begin?
- What is the first credible Spark-displacement workload?
- How should cloud-cost proxy be computed?
- Should benchmark results live in docs, CI artifacts, or a separate benchmark dashboard?

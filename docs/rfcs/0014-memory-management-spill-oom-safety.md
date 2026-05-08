# RFC 0014: Memory Management, Spill, and OOM Safety

## Status

Draft

## Summary

This RFC defines ShardLoom's memory management, spill, and out-of-memory safety design.

ShardLoom should not rely only on adaptive sizing, streaming, pruning, and zero-decode execution to avoid memory pressure. Those techniques reduce memory pressure, but they do not eliminate it.

ShardLoom must eventually support native memory accounting, memory reservations, memory pressure diagnostics, spillable operators, spill files, cleanup, and deterministic failure before process OOM.

## Context

ShardLoom aims to challenge Spark-like workloads without Spark fallback.

Spark remains useful for massive workloads partly because it handles memory pressure, shuffle, spilling, task retries, and large stateful operations. Lightweight engines can be faster and cheaper, but they often fail or become difficult to use when joins, aggregations, sorts, or exports exceed memory.

ShardLoom needs both:

- OOM avoidance.
- OOM survival.

OOM avoidance includes:

- Metadata-only answers.
- Segment pruning.
- Zero-decode execution.
- Partial decode.
- Late materialization.
- Adaptive sizing.
- Streaming.
- Backpressure.

OOM survival includes:

- Memory reservations.
- Operator memory accounting.
- Spill decisions.
- Spill files.
- Spill cleanup.
- Spill-aware operators.
- Deterministic OOM-safe diagnostics.
- Retryable task behavior.

## Goals

- Define ShardLoom's memory management model.
- Define memory reservations.
- Define memory pressure levels.
- Define spill policies.
- Define spill manager concepts.
- Define spill file concepts.
- Define spillable operator contracts.
- Define OOM-safe failure behavior.
- Define diagnostics for memory pressure and spill.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement spill in this RFC.
- Do not add dependencies in this RFC.
- Do not define final spill file format.
- Do not implement sort, join, group-by, shuffle, or window spill.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not require all operators to spill immediately.
- Do not guarantee no process OOM in early versions.

## Core principle

ShardLoom should not discover memory pressure only when the process panics or is killed.

Every memory-heavy operator should eventually be able to:

1. Estimate memory.
2. Reserve memory.
3. Report memory use.
4. Detect pressure.
5. Reduce memory.
6. Spill if supported.
7. Throttle if needed.
8. Fail deterministically before process OOM when spill is unsupported.

## Memory work hierarchy

ShardLoom should minimize memory pressure in this order:

1. Do not read.
2. Do not decode.
3. Do not copy.
4. Do not materialize.
5. Stream.
6. Bound parallelism.
7. Spill state.
8. Repartition or split work.
9. Fail deterministically before process OOM.
10. Never silently delegate to another engine.

## Core concepts

### GlobalMemoryBudget

The total memory budget available to ShardLoom execution.

It may be configured by:

- User config.
- CLI flag.
- Environment.
- Runtime default.
- Host-memory detection in a later implementation.

### MemoryPool

A pool that tracks memory reservations.

A MemoryPool should eventually support:

- Total budget.
- Reserved memory.
- Available memory.
- Per-operator accounting.
- Reservation attempts.
- Reservation release.
- Pressure detection.
- Diagnostics.

### MemoryReservation

A reservation held by an operator or task.

It should include:

- Reservation id.
- Owner task/operator.
- Reserved bytes.
- Used bytes if known.
- Spillability.
- Release behavior.
- Diagnostics.

### OperatorMemoryClass

Operators should be classified by memory behavior.

Suggested classes:

- Scan.
- Filter.
- Projection.
- Aggregate.
- Sort.
- Join.
- Window.
- Repartition.
- Shuffle.
- UDF.
- Translation.
- Sink.
- ExternalEffect.
- Unknown.

### MemoryPressureLevel

Suggested pressure levels:

- Normal.
- Elevated.
- High.
- Critical.
- Exhausted.

Pressure levels should influence planning and execution.

Examples:

- Elevated: reduce prefetch or parallelism.
- High: ask spillable operators to spill.
- Critical: force spill or fail unsupported operators.
- Exhausted: deterministic failure if memory cannot be released.

### SpillPolicy

Suggested policies:

- Never.
- BestEffort.
- Required.
- ForceBeforeOom.
- DisabledForOperator.

Policy meaning:

- Never: do not spill; fail if memory is insufficient.
- BestEffort: spill when beneficial or under pressure.
- Required: operator must be spill-capable.
- ForceBeforeOom: spill aggressively before OOM.
- DisabledForOperator: operator does not support spilling.

### SpillManager

A SpillManager coordinates spill files.

It should eventually manage:

- Spill root directory.
- Spill file ids.
- Maximum spill bytes.
- Spill compression.
- Spill file format.
- Temporary paths.
- Cleanup.
- Task ownership.
- Commit/abort behavior.
- Diagnostics.

### SpillFileRef

A reference to a spill file.

It should include:

- Spill id.
- Path.
- Owner task.
- Format.
- Compression.
- Size.
- Partition id if applicable.
- Cleanup status.

### SpillPartition

A spilled partition or run.

It should include:

- Partition id.
- Spill files.
- Row count estimate.
- Encoded size.
- Decoded size estimate.
- Sort/group/join key metadata if applicable.

### SpillFormat

Potential formats:

- VortexNativeSpill.
- ArrowIpcSpill.
- RowBinarySpill.
- KeyValueRunSpill.
- Unknown.

ShardLoom should prefer spill formats that preserve columnar and encoded information where possible.

Vortex-native spill may be a major differentiator.

### SpillCompression

Potential values:

- None.
- Lz4Like.
- ZstdLike.
- NativeVortex.
- Unknown.

This RFC does not require compression implementation.

### SpillableOperator

A conceptual contract for memory-heavy operators.

A spillable operator should eventually support:

- Current memory use.
- Estimated memory use.
- Can spill.
- Spill priority.
- Prepare spill.
- Spill.
- Restore/read spilled state.
- Release memory.
- Cleanup.
- Diagnostics.

### SpillDecision

A decision made under memory pressure.

Possible decisions:

- KeepInMemory.
- SpillNow.
- SpillLater.
- ReduceParallelism.
- SplitTask.
- FailBeforeOom.
- Unsupported.

### SpillPlan

A plan for spilling state.

It should include:

- Operator.
- Task.
- Memory target.
- Spill partitions.
- Spill format.
- Compression.
- Temporary paths.
- Cleanup expectations.
- Diagnostics.

### SpillReport

A report after spill.

It should include:

- Bytes spilled.
- Files created.
- Memory released.
- Time spent.
- Compression used.
- Cleanup status.
- Diagnostics.

## Spillable operators

ShardLoom should eventually support spill-aware versions of these operators.

### Sort

Sort can spill sorted runs, then merge them.

Requirements:

- Memory reservation.
- Run creation.
- Spill file creation.
- Merge plan.
- Cleanup.
- Bounded memory merge.

### Hash aggregate

Hash aggregate can spill partial aggregates.

Requirements:

- Memory reservation.
- Group partitioning.
- Partial aggregate state spill.
- Merge/finalize plan.
- Key metadata.
- Null semantics.

### Hash join

Hash join can spill build-side partitions or switch strategy.

Requirements:

- Build-side memory accounting.
- Partitioning.
- Spill partitions.
- Probe-side coordination.
- Join correctness.
- Null semantics.

### Sort-merge join

Sort-merge join can use spilled sorted runs.

Requirements:

- External sort support.
- Merge streaming.
- Join state bounds.
- Output streaming.

### Repartition and shuffle

Shuffle can spill partitions.

Requirements:

- Partition buffers.
- Spill file rotation.
- Compression.
- Checksum or consistency metadata later.
- Retry behavior.
- Cleanup.

### Window

Window functions may require state.

Requirements:

- Partition memory accounting.
- Frame memory accounting.
- Spill support or deterministic unsupported diagnostics.

### Translation and sinks

Some compatibility outputs require materialization.

Requirements:

- Sink memory accounting.
- Buffered output limits.
- Streaming writes where possible.
- Spill or backpressure if buffers grow.

## OOM-safe diagnostics

When memory pressure cannot be resolved, ShardLoom should fail before process OOM where possible.

Diagnostics should include:

- Memory budget.
- Reserved memory.
- Requested memory.
- Operator.
- Task id.
- Spill support.
- Spill policy.
- Why spill was unavailable.
- Suggested next step.
- Fallback attempted: false.

Example diagnostic code ideas:

- `SL_MEMORY_BUDGET_EXCEEDED`
- `SL_MEMORY_RESERVATION_FAILED`
- `SL_SPILL_REQUIRED`
- `SL_SPILL_UNSUPPORTED`
- `SL_SPILL_LIMIT_EXCEEDED`
- `SL_TEMP_DIR_UNAVAILABLE`
- `SL_OOM_PREVENTED`
- `SL_OPERATOR_NOT_SPILLABLE`

## Interaction with adaptive sizing

Adaptive sizing avoids memory pressure by choosing smaller tasks.

Memory management and spill survive memory pressure when estimates are wrong or operators need state.

The planner should combine:

- Adaptive task sizing.
- Memory budgets.
- Streaming.
- Spill policies.
- Operator memory class.
- Sink requirements.

### CG-14.1 adaptive memory boundary evidence

The first CG-14 memory/optimizer integration is report-only. It records the
bounded-memory and spill-policy requirements that runtime adaptation must obey,
but it does not allocate, reserve, spill, execute operators, or rewrite a plan.

`AdaptiveOptimizerMemoryReport` must require:
- memory budget declaration.
- bounded-memory declaration.
- spill policy declaration.
- deterministic OOM boundary.
- sink requirement boundary.
- runtime facts before adaptation.
- `spill_io_performed=false`.
- `runtime_adaptation_applied=false`.
- `fallback_attempted=false`.

The report may identify candidate adaptations such as reduced parallelism,
runtime-filter application, dynamic pruning, or skew handling, but all such
decisions remain evidence until later execution phases add native runtime
behavior and correctness checks.

## Interaction with streaming

Streaming reduces peak memory, but not all operators are streaming.

If an operator cannot stream and cannot spill, ShardLoom must explain why.

Streaming should not silently fall back to full materialization.

## Interaction with Vortex

Vortex can reduce memory pressure by preserving compressed/encoded representation.

ShardLoom should prefer:

- Vortex-native spill where possible.
- Columnar spill over row spill.
- Encoded spill over decoded spill.
- Statistics-preserving spill when possible.

Vortex-native spill may let ShardLoom re-use the same encoded execution advantages after spilling.

## Interaction with object storage

Spill files are not the same as durable outputs.

Spill files should usually be temporary.

Spill may use:

- Local disk.
- Ephemeral worker storage.
- Future remote spill storage.

Remote spill requires careful design and is not defined here.

## Safety and cleanup

Spill files must eventually support cleanup.

Cleanup should handle:

- Successful task completion.
- Task failure.
- Query cancellation.
- Process restart where possible.
- Ambiguous cleanup state.

Early versions may only support best-effort cleanup, but this limitation must be documented.

## Failure behavior

Unsupported memory/spill behavior must fail explicitly.

Examples:

- Operator cannot spill.
- Temp directory unavailable.
- Spill limit exceeded.
- Memory reservation failed.
- Spill file write failed.
- Spill file read failed.
- Cleanup failed.
- Remote spill unsupported.
- Full materialization required but memory budget insufficient.

Failures must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

## Alternatives considered

### Rely only on streaming and adaptive sizing

Rejected.

Streaming and adaptive sizing reduce memory risk but cannot handle all stateful operators.

### Rely on OS OOM behavior

Rejected.

ShardLoom should fail deterministically before process OOM where possible.

### Add Spark fallback for large or spilling workloads

Rejected.

This violates ShardLoom's core architecture.

### Use DataFusion spill implementation

Rejected as a core dependency.

ShardLoom may learn from other systems, but spill must be ShardLoom-native.

### Always spill eagerly

Rejected.

Spill is slower than memory. The engine should spill when needed, not always.

## Risks

- Spill implementation is complex.
- Spill can hurt performance if overused.
- Spill file cleanup is easy to get wrong.
- Operator-specific spill behavior is difficult.
- Vortex-native spill may require careful encoding and metadata design.
- Memory estimates may be wrong.
- Remote/distributed spill adds major complexity.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- OOM safety is a first-class design goal.
- Adaptive sizing alone is insufficient.
- Streaming alone is insufficient.
- Memory reservations are required.
- Spillable operators are required for Spark-displacement workloads.
- Spill must be ShardLoom-native.
- Vortex-native spill should be considered a differentiator.
- Unsupported spill behavior must fail before process OOM where possible.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Memory budgets can be represented.
- Memory reservations can be represented.
- Memory pressure levels can be represented.
- Spill policies can be represented.
- Spill file references can be represented.
- Spill decisions can be represented.
- Spill reports can be represented.
- Unsupported spill behavior produces deterministic diagnostics.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What should be the first spillable operator?
- Should spill format initially be Vortex-native, Arrow IPC-like, or a simpler internal format?
- Should local spill be implemented before remote spill?
- How should memory estimates be calibrated?
- How should spill interact with adaptive sizing?
- How should spill interact with streaming sinks?
- Should spill file cleanup be tracked in manifests or runtime diagnostics?

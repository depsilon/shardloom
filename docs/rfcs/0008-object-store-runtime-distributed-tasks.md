# RFC 0008: Object-Store Runtime and Distributed Task Model

## Status

Draft

## Summary

This RFC defines ShardLoom's object-store-native runtime and future distributed task model.

ShardLoom's goal is not only to compete with single-node engines. To displace Spark for massive workloads, ShardLoom needs a runtime model for object-store reads, segment-level tasks, retries, idempotency, bounded resource use, shuffle avoidance, and eventually distributed execution.

This RFC defines the architecture direction without implementing distributed execution yet.

## Context

Spark remains useful because it handles scale, retries, distributed tasks, shuffle, and large writes.

ShardLoom must eventually support large workloads without Spark fallback. That requires treating object storage as the primary data environment and encoded Vortex segments as the primary scheduling unit.

ShardLoom should avoid becoming "Spark but in Rust." It should preserve its encoded-columnar, metadata-first, and shuffle-avoidant design.

## Goals

- Define object-store-native runtime principles.
- Define segment-level task planning.
- Define future coordinator/worker concepts.
- Define retry and idempotency expectations.
- Define bounded resource behavior.
- Define spill and backpressure expectations.
- Define shuffle avoidance principles.
- Define output commit expectations.
- Preserve no-fallback architecture.

## Non-goals

- Do not implement distributed execution in this RFC.
- Do not add Spark.
- Do not add DataFusion.
- Do not define a full cluster manager.
- Do not define a shuffle service implementation.
- Do not define all object-store providers.
- Do not require distributed execution for early versions.
- Do not define lakehouse transaction semantics.

## Decision

ShardLoom should design toward a runtime with these concepts:

- ObjectStoreRef.
- ByteRangeRequest.
- SegmentTask.
- TaskGraph.
- Coordinator.
- Worker.
- TaskAttempt.
- RetryPolicy.
- ResourceBudget.
- SpillPlan.
- ShufflePlan.
- CommitPlan.
- RuntimeDiagnostics.

Distributed execution should be introduced only after the single-node encoded execution contracts are clear.

## Runtime principles

ShardLoom runtime should prioritize:

1. Metadata before reads.
2. Byte ranges before whole files.
3. Segment tasks before file tasks.
4. Local multicore before distributed execution.
5. Shuffle avoidance before shuffle optimization.
6. Idempotent tasks before complex recovery.
7. Bounded resources before maximum parallelism.
8. Explicit unsupported diagnostics before fallback.

## Object-store IO

Object-store IO should support:

- Metadata reads.
- Byte-range reads.
- Retryable reads.
- Read diagnostics.
- Bounded concurrency.
- Backpressure.
- Optional caching.
- Error propagation.
- Provider abstraction.

ShardLoom should avoid full-file reads unless required.

## SegmentTask

A SegmentTask is a unit of execution over one or more encoded segments.

A SegmentTask should include:

- Input segment descriptors.
- Required columns.
- Predicate plan.
- Materialization policy.
- Byte ranges.
- Expected output partition.
- Resource budget.
- Retry identity.
- Snapshot identity.
- Diagnostics settings.

Segment tasks should be idempotent where possible.

## TaskGraph

A TaskGraph represents the execution dependencies for a query or transformation.

It may include:

- Scan tasks.
- Pruning tasks.
- Encoded evaluation tasks.
- Partial decode tasks.
- Aggregate tasks.
- Join tasks.
- Repartition tasks.
- Output write tasks.
- Commit tasks.

The graph should make shuffle boundaries explicit.

## Coordinator

A future coordinator may:

- Build task graphs.
- Assign tasks.
- Track attempts.
- Handle retries.
- Track progress.
- Enforce resource budgets.
- Manage commits.
- Emit diagnostics.

The coordinator must not delegate execution to Spark or DataFusion.

## Worker

A future worker may:

- Execute segment tasks.
- Read object-store byte ranges.
- Evaluate encoded predicates.
- Materialize required outputs.
- Spill if necessary.
- Write temporary outputs.
- Report diagnostics.

Workers should not require JVM or Spark infrastructure.

## Retry and idempotency

Tasks should be retryable when possible.

A task should have:

- Stable task id.
- Stable input snapshot.
- Stable input segment descriptors.
- Stable output intent.
- Attempt id.
- Idempotency key for writes.

Retries must not duplicate committed outputs.

## Spill and resource budgets

Large workloads require bounded resource behavior.

ShardLoom should plan for:

- Memory budgets.
- Disk spill budgets.
- Output buffer budgets.
- Concurrency limits.
- Backpressure.
- Cancellation.

Unbounded memory use is not acceptable for Spark-displacement workloads.

## Shuffle model

Shuffle should be treated as expensive and avoided where possible.

ShardLoom should prefer:

- Broadcast small side.
- Segment-local aggregation.
- Pre-partitioned or clustered layouts.
- Range partitioning when statistics support it.
- Dynamic filtering.
- Semi-join reduction.
- Incremental aggregation.
- Reuse of existing segment organization.

When shuffle is unavoidable, the plan should make it explicit.

## Output commit behavior

Output commit behavior should eventually support:

- Temporary writes.
- Validation.
- Manifest update.
- Snapshot pointer update.
- Commit record.
- Cleanup.
- Failure diagnostics.

Atomicity depends on storage/catalog capabilities. If atomic commit cannot be guaranteed, ShardLoom must document the limitation.

## Failure behavior

Unsupported runtime behavior must fail explicitly.

Examples:

- Unsupported object-store URI.
- Unsupported range-read behavior.
- Missing segment bytes.
- Retry exhaustion.
- Resource budget exceeded.
- Spill unsupported.
- Shuffle required but unsupported.
- Atomic commit required but unavailable.
- Distributed execution required but unavailable.

Failures must not invoke Spark, DataFusion, or another engine.

## Alternatives considered

### Use Spark for distributed execution

Rejected.

This violates the core no-fallback policy.

### Use DataFusion or Ballista for distributed execution

Rejected for core execution.

ShardLoom may study or benchmark against other systems, but must own its runtime.

### Build distributed runtime first

Rejected.

ShardLoom should establish single-node encoded execution, Vortex IO, statistics/pruning, and translation contracts first.

### Schedule by file instead of segment

Rejected as the primary model.

File-level scheduling may be useful for coarse planning, but segment-level planning better fits Vortex-native execution.

## Risks

- Distributed runtime is a large undertaking.
- Object-store behavior is provider-specific.
- Shuffle implementation may be difficult.
- Fault tolerance may become complex.
- Overbuilding runtime before core execution works would slow progress.
- Segment-level scheduling may require careful metadata design.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Object storage is a primary runtime environment.
- Segment-level tasks are the preferred scheduling unit.
- Distributed execution is future work but must be planned.
- Tasks should be retryable and idempotent where possible.
- Shuffle should be avoided and explicit.
- Resource usage must be bounded.
- Spark/DataFusion fallback remains prohibited.

## Verification plan

Future implementation PRs should verify:

- Object-store references can be modeled.
- Byte-range reads can be represented.
- Segment tasks can be represented.
- Runtime diagnostics can describe task states.
- Resource limits can be represented.
- Unsupported distributed behavior fails clearly.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What object-store abstraction should ShardLoom use first?
- Should distributed workers be processes, containers, serverless tasks, or all of the above?
- What is the first shuffle strategy to support?
- What spill format should be used?
- How should runtime diagnostics be exposed?
- How should task graphs integrate with RFC 0004 snapshots and RFC 0007 translation?


### Future runtime vocabulary

The following runtime vocabulary is conceptual contract direction only and does not authorize distributed execution in the current phase.

#### SplitSource

Allowed split-source kinds:
- `local_file`
- `object_store_range`
- `manifest_segment`
- `metadata_only`
- `runtime_filter`
- `intermediate_artifact`

#### TaskLease

Required fields:
- `task_id`
- `attempt_id`
- `worker_id`
- `lease_deadline`
- `resource_vector`
- `cancellation_token`
- `idempotency_key`

#### PlacementHint

Required fields:
- `locality`
- `co_locate_with`
- `avoid_node`
- `memory_affinity`
- `object_store_affinity`
- `soft_or_hard`

#### IntermediateArtifactRef

Artifact kinds:
- `exchange`
- `spill`
- `partial_sink`
- `commit_staging`
- `runtime_filter`
- `profile_sample`

Required fields:
- `artifact_id`
- `kind`
- `recoverability`
- `deterministic_recompute`
- `content_addressed`
- `cleanup_policy`

#### ExchangeSpoolPolicy

Include:
- `disabled`
- `local_only`
- `object_store_deferred`
- `content_addressed_required`
- `cleanup_required`

#### TaskGranularityPolicy

Include:
- `min_encoded_bytes_per_task`
- `target_encoded_bytes_per_task`
- `max_encoded_bytes_per_task`
- `min_segments_per_task`
- `max_segments_per_task`
- `max_tasks_per_stage`
- `allow_fusion`
- `allow_fission`
- `skew_split_threshold`

#### RecoveryStrategy

Recovery strategy kinds:
- `retry_same_input`
- `reconstruct_from_lineage`
- `reuse_intermediate_artifact`
- `abort_with_diagnostic`

Clarifications:
- None of these vocabulary additions authorize distributed execution yet.
- This RFC section does not authorize adding Dask, Ray, or Trino dependencies.
- No runtime fallback/delegation is permitted.


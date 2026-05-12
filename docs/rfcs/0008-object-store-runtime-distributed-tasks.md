# RFC 0008: Object-Store Runtime and Distributed Task Model

## Status

Draft

## Summary

This RFC defines ShardLoom's object-store-native runtime and future distributed task model.

ShardLoom's goal is not only to compete with single-node engines. To displace Spark for massive
workloads, ShardLoom needs a runtime model for object-store reads, segment-level tasks, retries,
idempotency, bounded resource use, shuffle avoidance, and eventually distributed execution.

This RFC defines the architecture direction without implementing distributed execution yet.

## Context

Spark remains useful because it handles scale, retries, distributed tasks, shuffle, and large
writes.

ShardLoom must eventually support large workloads without Spark fallback. That requires treating
object storage as the primary data environment and encoded Vortex segments as the primary scheduling
unit.

ShardLoom should avoid becoming "Spark but in Rust." It should preserve its encoded-columnar,
metadata-first, and shuffle-avoidant design.

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

Distributed execution should be introduced only after the single-node encoded execution contracts
are clear.

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

### Object-store range planning report

Object-store range planning is the report-only bridge between declared manifest byte ranges
and future object-store reads. It may produce request-shape evidence from already-declared
S3/GCS/ADLS segment byte ranges, but it must not contact object stores, probe networks, read
files, materialize data, retry requests, or execute fallback behavior.

The CG-10 object-store range evidence surface is `ObjectStoreRangePlanningReport`.

Required fields:

- `manifest`.
- `policy`.
- `status`.
- `requests`.
- `diagnostics`.
- `file_count`.
- `segment_count`.
- `object_store_file_count`.
- `non_object_store_file_count`.
- `ranged_segment_count`.
- `missing_byte_range_segment_count`.
- `invalid_range_count`.
- `oversized_range_count`.
- `planned_request_count`.
- `planned_range_count`.
- `coalesced_range_count`.
- `estimated_request_bytes`.
- `requires_byte_ranges`.
- `requires_request_budget_review`.
- `full_file_read_required`.
- `full_file_read_allowed=false`.
- `can_plan_without_io=true`.
- `data_read=false`.
- `object_store_io=false`.
- `write_io=false`.
- `fallback_execution_allowed=false`.

`ObjectStoreRangePlanningStatus` should identify at least:

- `planned`.
- `blocked_missing_byte_ranges`.
- `blocked_invalid_ranges`.
- `blocked_request_budget`.
- `blocked_non_object_store`.
- `unsupported`.

Object-store range planning may coalesce adjacent ranges when the request budget allows it.
Coalescing is planning evidence only; it does not execute reads or imply provider capability.
Missing byte ranges must not silently degrade into full-file reads. Full-file reads require a
separate native approval gate and must remain disallowed in this report.

### Object-store request coalescing report

Request coalescing planning is the report-only comparison between uncoalesced and coalesced
range request shapes. It may show how many declared byte ranges can be reduced into fewer
object-store requests under a policy budget, but it must not execute reads, retry requests,
probe provider behavior, or claim measured latency/cost improvement.

The CG-10 coalescing evidence surface is `ObjectStoreRequestCoalescingReport`.

Required fields:

- `uncoalesced_range_report`.
- `coalesced_range_report`.
- `status`.
- `decisions`.
- `diagnostics`.
- `input_request_count`.
- `output_request_count`.
- `request_reduction_count`.
- `input_range_count`.
- `coalesced_range_count`.
- `estimated_request_bytes_before`.
- `estimated_request_bytes_after`.
- `coalescing_applied`.
- `can_plan_without_io=true`.
- `data_read=false`.
- `object_store_io=false`.
- `write_io=false`.
- `fallback_execution_allowed=false`.

`ObjectStoreRequestCoalescingStatus` should identify at least:

- `planned`.
- `no_coalescing_needed`.
- `blocked_by_range_planning`.

Coalescing must be blocked whenever range planning is blocked by missing byte ranges, invalid
ranges, request-budget violations, or non-object-store inputs. Coalescing evidence is not a
benchmark claim and must not be used as a cost/performance claim before CG-6 benchmark gates.

### Object-store commit protocol planning report

Object-store commit protocol planning is the report-only readiness bridge between future
object-store output writes and a safe commit protocol. It may validate declared commit-plan
evidence, but it must not write files, contact object stores, probe provider behavior, execute
cleanup, run recovery, or commit manifests.

The CG-10 commit protocol evidence surface is `ObjectStoreCommitProtocolReport`.

Required fields:

- `input`.
- `status`.
- `diagnostics`.
- `object_store_target`.
- `requires_staging_prefix`.
- `requires_manifest_pointer_update`.
- `requires_commit_record`.
- `requires_idempotency_key`.
- `requires_cleanup_plan`.
- `requires_atomic_commit_evidence`.
- `commit_execution_allowed=false`.
- `can_plan_without_io=true`.
- `object_store_io=false`.
- `write_io=false`.
- `fallback_execution_allowed=false`.

`ObjectStoreCommitProtocolStatus` should identify at least:

- `ready`.
- `blocked_non_object_store`.
- `blocked_missing_staging`.
- `blocked_missing_manifest_pointer`.
- `blocked_missing_commit_record`.
- `blocked_missing_idempotency`.
- `blocked_missing_cleanup`.
- `blocked_atomicity`.

The `requires_*` fields represent unmet readiness evidence in the current report, not blanket
permission to perform the corresponding operation. A ready report means the declared evidence is
coherent enough for a later implementation phase to consider commit execution; it does not execute
the commit. Object-store commit execution, provider-specific atomicity, recovery cleanup, and
distributed commit coordination remain separate gates.

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

### Object-store distributed scheduling planning report

Object-store distributed scheduling planning is the report-only bridge between request-shape
evidence and future coordinator/worker execution. It may group successful object-store request
coalescing evidence into stable task shapes, but it must not start a coordinator, start workers,
execute tasks, read object-store ranges, write checkpoint artifacts, retry requests, or invoke
fallback execution.

The CG-10 distributed scheduling evidence surface is
`ObjectStoreDistributedSchedulingReport`.

Required fields:

- `coalescing_report`.
- `policy`.
- `status`.
- `tasks`.
- `diagnostics`.
- `input_request_count`.
- `planned_task_count`.
- `estimated_request_bytes`.
- `requires_checkpoint_plan`.
- `requires_retry_policy`.
- `requires_idempotency_keys`.
- `scheduler_execution_allowed=false`.
- `coordinator_started=false`.
- `worker_started=false`.
- `task_execution_allowed=false`.
- `can_plan_without_io=true`.
- `object_store_io=false`.
- `write_io=false`.
- `fallback_execution_allowed=false`.

`ObjectStoreDistributedSchedulingStatus` should identify at least:

- `planned`.
- `blocked_by_coalescing`.
- `blocked_empty_requests`.
- `blocked_task_budget`.
- `blocked_invalid_policy`.

`ObjectStoreDistributedTaskPlan` should identify at least:

- `task_id`.
- `request_start_index`.
- `request_count`.
- `range_count`.
- `uri_count`.
- `estimated_request_bytes`.
- `requires_retry_identity`.
- `requires_checkpoint_record`.
- `requires_idempotency_key`.
- `task_execution_allowed=false`.
- `object_store_io=false`.
- `write_io=false`.

Scheduling evidence may require checkpoint, retry, and idempotency plans, but this report does not
complete those plans. Checkpoint/retry/idempotency readiness remains a separate CG-10 gate before
distributed task execution can be considered.

### Object-store checkpoint/retry/idempotency planning report

Object-store checkpoint/retry/idempotency planning is the report-only readiness bridge between
distributed task shapes and future retry/checkpoint execution. It may validate declared task
reliability evidence, but it must not execute retries, write checkpoint records, write attempt
records, clean up failed attempts, start a coordinator, start workers, contact object stores, or
invoke fallback execution.

The CG-10 reliability evidence surface is `ObjectStoreCheckpointRetryReport`.

Required fields:

- `input`.
- `status`.
- `diagnostics`.
- `task_count`.
- `retryable_task_count`.
- `planned_checkpoint_record_count`.
- `planned_attempt_record_count`.
- `requires_retry_policy`.
- `requires_checkpoint_plan`.
- `requires_idempotency_keys`.
- `requires_attempt_records`.
- `requires_cleanup_policy`.
- `retry_execution_allowed=false`.
- `checkpoint_write_allowed=false`.
- `cleanup_execution_allowed=false`.
- `coordinator_started=false`.
- `worker_started=false`.
- `object_store_io=false`.
- `write_io=false`.
- `fallback_execution_allowed=false`.

`ObjectStoreCheckpointRetryStatus` should identify at least:

- `ready`.
- `blocked_by_scheduling`.
- `blocked_missing_retry_policy`.
- `blocked_missing_checkpoint_plan`.
- `blocked_missing_idempotency`.
- `blocked_missing_attempt_record`.
- `blocked_missing_cleanup_policy`.

A ready report means the reliability evidence is coherent enough for a later implementation phase
to consider distributed retry/checkpoint behavior. It does not execute retries, write checkpoints,
or create durable attempt records.

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

Atomicity depends on storage/catalog capabilities. If atomic commit cannot be guaranteed, ShardLoom
must document the limitation.

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

ShardLoom should establish single-node encoded execution, Vortex IO, statistics/pruning, and
translation contracts first.

### Schedule by file instead of segment

Rejected as the primary model.

File-level scheduling may be useful for coarse planning, but segment-level planning better fits
Vortex-native execution.

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

The following runtime vocabulary is conceptual contract direction only and does not authorize
distributed execution in the current phase.

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

## Systems-learning conceptual runtime vocabulary (R5.1)

Conceptual terms added for future runtime contracts:
- `SplitSource`
- `TaskLease`
- `PlacementHint`
- `IntermediateArtifactKind`
- `IntermediateArtifactRef`
- `ExchangeSpoolPolicy`
- `RecoveryStrategy`
- `TaskGranularityPolicy`

Artifact taxonomy note:
- spill, exchange, runtime filters, commit staging, and profile samples are distinct artifact
  classes.

R5.1 scope note:
- no object-store IO is implemented in this pass.
- no distributed execution is implemented in this pass.


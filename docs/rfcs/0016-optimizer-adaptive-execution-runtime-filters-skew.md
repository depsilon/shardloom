# RFC 0016: Optimizer, Adaptive Execution, Runtime Filters, and Skew Handling

## Status

Draft

## Summary

This RFC defines ShardLoom's optimizer, adaptive execution, runtime filter, and skew handling design.

ShardLoom cannot displace Spark-like workloads only by being fast on scans. It needs an optimizer that uses Vortex-native statistics, encoded execution capabilities, memory pressure, sink requirements, streaming support, and runtime feedback to adapt plans safely.

## Context

ShardLoom has or plans foundational concepts for:

- Encoded segments.
- Statistics and pruning.
- Scan requests.
- Explain and estimate.
- Adaptive sizing.
- Streaming.
- Memory/spill/OOM safety.
- Task graphs.
- Dataset manifests and snapshots.
- Vortex-native input/output.
- Translation reports.

The next step is to define how the optimizer chooses among these options.

A world-class optimizer should reduce work, avoid memory pressure, avoid shuffle, exploit runtime filters, handle skew, and adapt based on observed data.

## Goals

- Define ShardLoom's optimizer architecture.
- Define logical and physical optimization phases.
- Define cost model inputs.
- Define adaptive execution.
- Define runtime filters.
- Define dynamic pruning.
- Define skew detection and handling.
- Define sink-driven optimization.
- Define memory/spill-aware optimization.
- Define diagnostics for optimizer decisions.
- Preserve no-fallback execution.

## Non-goals

- Do not implement optimizer code in this RFC.
- Do not implement SQL parsing.
- Do not implement joins or aggregates.
- Do not add Spark.
- Do not add DataFusion.
- Do not add fallback execution.
- Do not define final cost equations.
- Do not implement runtime filters yet.
- Do not implement distributed execution yet.

## Core principle

ShardLoom's optimizer should avoid work before making work faster.

Optimization priority:

1. Metadata-only answers.
2. Segment pruning.
3. Projection pruning.
4. Encoded execution.
5. Partial decode.
6. Late materialization.
7. Streaming.
8. Memory/spill-aware operator choice.
9. Runtime filtering.
10. Shuffle avoidance.
11. Distributed execution only when needed.

## Optimizer phases

ShardLoom should eventually use multiple optimizer phases.

### Frontend normalization

Converts user input into logical form.

Possible sources:

- SQL.
- DataFrame API.
- CLI command.
- Agent-generated plan.
- Future Substrait-like plan.

### Logical optimization

Semantic rewrites that do not depend on physical encodings.

Examples:

- Predicate simplification.
- Projection pruning.
- Constant folding.
- Predicate pushdown.
- Redundant expression elimination.
- Limit pushdown when safe.
- Unsupported shape detection.

### Vortex-native physical optimization

Uses Vortex-specific information.

Examples:

- Segment pruning.
- Encoding-aware predicate selection.
- Layout-aware scan planning.
- Statistics-only answers.
- Byte-range planning.
- Encoded group-by or join eligibility.
- Late materialization boundaries.
- Native Vortex output preservation.

### Runtime optimization

Uses information discovered during execution.

Examples:

- Actual row counts.
- Actual selected rows.
- Actual memory pressure.
- Actual skew.
- Runtime filters.
- Spill pressure.
- Sink backpressure.
- Object-store latency.

### Post-execution diagnostics

Captures what happened.

Examples:

- Planned vs actual bytes read.
- Planned vs actual bytes decoded.
- Segments pruned.
- Segments materialized.
- Spill events.
- Runtime filters applied.
- Skew handled.
- Output fidelity.

## Cost model inputs

The optimizer should eventually consider:

- Row count.
- Encoded bytes.
- Estimated decoded bytes.
- Column count.
- Selected column count.
- Null count.
- Cardinality estimate.
- Min/max.
- Sort order.
- Run count.
- Constant segment status.
- Byte ranges.
- Object-store request cost.
- Memory budget.
- Spill policy.
- Streaming capability.
- Sink requirements.
- Output fidelity.
- External effect cost.
- Model/API call cost.
- Runtime filter availability.
- Skew risk.
- Join side size.
- Partition count.
- Snapshot/change-set information.

### Layout health planning report

Layout-health planning is the report-only bridge between manifest intelligence and later
maintenance planning. It should use declared manifest, file, segment, statistics, encoding,
layout, and byte-range metadata already available in memory. It must not construct Vortex
layout readers, inspect table metadata, read data files, contact object stores, write files,
run compaction, or attempt fallback execution.

The CG-9 layout-health evidence surface is `LayoutHealthReport`.

Required fields:

- `manifest`.
- `policy`.
- `status`.
- `issues`.
- `diagnostics`.
- `file_count`.
- `segment_count`.
- `native_vortex_file_count`.
- `non_native_data_file_count`.
- `small_file_count`.
- `small_segment_count`.
- `missing_statistics_segment_count`.
- `missing_byte_range_segment_count`.
- `unique_format_count`.
- `unique_encoding_count`.
- `unique_layout_count`.
- `compaction_candidate_count`.
- `requires_statistics_refresh`.
- `requires_byte_range_index`.
- `requires_layout_review`.
- `recommends_compaction`.
- `can_plan_without_io=true`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `compaction_execution_allowed=false`.
- `fallback_execution_allowed=false`.

`LayoutHealthStatus` should identify at least:

- `healthy`.
- `needs_attention`.
- `compaction_recommended`.
- `unsupported`.

Layout-health diagnostics should identify small files, small segments, missing statistics,
missing byte ranges, mixed file formats, mixed encodings, mixed layouts, non-native data
files, and empty manifests. Compaction recommendations are planning evidence only; actual
rewrite, clustering, delete/tombstone application, manifest update, and commit behavior remain
separate gated work.

### Compaction planning report

Compaction planning is the report-only continuation of layout-health evidence. It may group
declared small-file and small-segment candidates into future maintenance recommendations, but
it must not read table metadata, inspect files, write compacted files, update manifests,
commit, contact object stores, or execute compaction.

The CG-9 compaction evidence surface is `CompactionPlanningReport`.

Required fields:

- `layout_health`.
- `policy`.
- `status`.
- `actions`.
- `diagnostics`.
- `file_count`.
- `segment_count`.
- `candidate_file_count`.
- `candidate_segment_count`.
- `candidate_count`.
- `blocked_candidate_count`.
- `estimated_compaction_group_count`.
- `missing_statistics_segment_count`.
- `missing_byte_range_segment_count`.
- `non_native_data_file_count`.
- `requires_statistics_refresh`.
- `requires_byte_range_index`.
- `requires_layout_review`.
- `requires_native_input_review`.
- `compaction_recommended`.
- `recommendation_emitted`.
- `can_plan_without_io=true`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `compaction_execution_allowed=false`.
- `fallback_execution_allowed=false`.

`CompactionPlanningStatus` should identify at least:

- `not_needed`.
- `planning_ready`.
- `blocked_by_metadata`.
- `blocked_by_layout_review`.
- `unsupported`.

Compaction planning may emit future actions such as `merge_small_files`,
`merge_small_segments`, `refresh_statistics`, `build_byte_range_index`,
`review_mixed_formats`, `review_mixed_encodings`, `review_mixed_layouts`, and
`review_non_native_data_files`. These are recommendations only. They are not executable
tasks, write intents, commit intents, or object-store operations until later gated phases
authorize native maintenance execution.

## Plan states

ShardLoom should make plan states explicit.

Suggested states:

- LogicalPlan.
- OptimizedLogicalPlan.
- PhysicalPlan.
- EncodedPhysicalPlan.
- StreamingPhysicalPlan.
- RuntimeAdaptivePlan.
- FinalExecutedPlan.
- UnsupportedPlan.

## Adaptive execution

Adaptive execution means ShardLoom may refine a plan based on runtime facts.

Examples:

- Reduce parallelism under memory pressure.
- Increase parallelism when IO-bound and safe.
- Switch join strategy if one side is smaller than estimated.
- Apply runtime filter from a build side.
- Coalesce small tasks.
- Split large tasks.
- Replan output materialization based on sink requirements.
- Trigger spill for stateful operator.
- Avoid shuffle based on runtime partition statistics.

Adaptive execution must preserve correctness.

Adaptive changes must appear in diagnostics.

## Runtime filters

Runtime filters are filters produced during execution and pushed into upstream or parallel scans.

Potential runtime filters:

- Bloom-like membership filter.
- Dictionary id filter.
- Range filter.
- Constant filter.
- Null-aware filter.
- Semi-join reduction filter.
- Dynamic partition filter.

Runtime filters can reduce:

- Rows scanned.
- Bytes decoded.
- Segments read.
- Shuffle volume.
- Join probe work.

Runtime filters must be conservative. Incorrect filtering is a correctness bug.

## Dynamic pruning

Dynamic pruning uses runtime information to skip data.

Examples:

- Join build side discovers allowed keys.
- Scan side prunes segments whose dictionary/range cannot match.
- Snapshot/change-set says unchanged segments can be reused.
- Runtime statistics show a partition is empty.

Dynamic pruning must be explainable.

## Join strategy

ShardLoom should eventually choose among join strategies.

Possible strategies:

- Broadcast small side.
- Hash join.
- Sort-merge join.
- Runtime-filtered join.
- Semi-join reduction.
- Range-aware join.
- Dictionary-aware join.
- Unsupported.

Join planning should consider:

- Build side size.
- Probe side size.
- Memory budget.
- Spill support.
- Skew.
- Sort order.
- Encoding.
- Runtime filter eligibility.
- Output cardinality.
- Shuffle requirement.

## Aggregation strategy

ShardLoom should eventually choose among aggregation strategies.

Possible strategies:

- Metadata-only aggregate.
- Segment-local partial aggregate.
- Encoded aggregate.
- Hash aggregate.
- Sort aggregate.
- Spillable aggregate.
- Streaming aggregate.
- Unsupported.

Aggregation planning should consider:

- Group cardinality.
- Key encoding.
- Segment locality.
- Memory budget.
- Spill policy.
- Output target.
- Runtime skew.

## Skew detection

Skew occurs when data or keys are unevenly distributed.

ShardLoom should eventually detect skew using:

- Segment row counts.
- Segment byte sizes.
- Key frequency estimates.
- Dictionary statistics.
- Runtime partition sizes.
- Spill pressure.
- Task duration.
- Output size.
- Object-store request latency.

## Skew handling

ShardLoom should eventually handle skew with:

- Split large segments.
- Split hot keys.
- Broadcast small side.
- Salted partitioning when safe.
- Range partitioning.
- Skew-aware task scheduling.
- Spill-heavy partition isolation.
- Dynamic repartitioning.
- Conservative fallback to unsupported diagnostic if correctness cannot be preserved.

Skew handling must not silently call Spark or another engine.

## Sink-driven optimization

Output targets influence planning.

Examples:

- Vortex native output may preserve encoded segments.
- Parquet output may force materialization.
- Arrow-like boundaries may require decoded columnar values.
- Compatibility output may lose physical metadata.
- Streaming sinks may constrain memory.

The optimizer should account for sink requirements before choosing materialization boundaries.

## Memory and spill-aware optimization

The optimizer should use:

- Memory budgets.
- Memory pressure.
- Spill policy.
- Spill capability.
- Operator memory class.
- Adaptive sizing.
- Streaming capability.
- Sink buffering.

If an operator requires memory and cannot spill, the optimizer must either choose a different strategy or fail deterministically.

## Object-store-aware optimization

The optimizer should consider:

- Byte ranges.
- Object-store request count.
- Metadata fetches.
- Manifest size.
- Small-file overhead.
- Cold vs warm cache.
- Parallel read pressure.
- Retry cost.

Optimization should avoid reading whole files when byte ranges and metadata are enough.

## Diagnostics

Optimizer diagnostics should include:

- Optimization rule applied.
- Reason rule applied.
- Reason rule did not apply.
- Estimated work avoided.
- Runtime adaptation applied.
- Runtime filter generated.
- Runtime filter pushed down.
- Skew detected.
- Strategy selected.
- Strategy rejected.
- Unsupported reason.
- Fallback attempted false.

### CG-14.1 adaptive optimizer memory report

`AdaptiveOptimizerMemoryReport` is the initial report-only CG-14 evidence
surface. It does not run the optimizer or adapt a plan. It records the gates that
must be satisfied before runtime-adaptive behavior can become executable.

Required fields:
- schema/report identity.
- optimizer phase.
- status.
- deferred optimizer rule decisions.
- conservative runtime-filter candidates.
- dynamic-pruning proof status.
- adaptive decision candidates.
- skew signal representation.
- memory-budget, bounded-memory, spill-policy, sink-boundary, and deterministic
  OOM boundary requirements.
- side-effect fields for optimizer execution, runtime adaptation application,
  runtime filter build/apply, plan rewrite, data read, decode, materialization,
  row read, Arrow conversion, object-store IO, writes, spill IO, external engine
  execution, fallback allowance, fallback attempt, and production claim
  allowance.

Acceptance boundaries:
- Runtime filters must be conservative candidates only until proof exists.
- Dynamic pruning must remain candidate-only until runtime proof exists.
- Adaptive decisions may be listed as candidates, but `runtime_adaptation_applied`
  must remain false until a later CG-14 execution step.
- Memory/spill-aware planning may require budgets and spill policy, but must not
  perform spill IO in this report.
- The report must emit `fallback_attempted=false` and
  `fallback_execution_allowed=false`.

## Failure behavior

Unsupported optimization behavior must fail explicitly if required for correctness.

Examples:

- Join strategy unsupported.
- Runtime filter unsupported.
- Skew handling unsupported.
- Spill required but unsupported.
- Sink requires materialization but memory budget insufficient.
- Statistics missing for requested metadata-only answer.
- Cost model cannot safely choose a plan.

Failures must not invoke Spark, DataFusion, DuckDB, Polars, Velox, or another fallback engine.

## Alternatives considered

### Use Spark AQE-style fallback

Rejected.

ShardLoom may learn from adaptive query execution patterns, but execution must remain ShardLoom-native.

### Use DataFusion optimizer as internal optimizer

Rejected for core execution.

ShardLoom needs Vortex-native physical planning and no fallback execution.

### Avoid adaptive execution initially

Partially accepted.

Adaptive execution can be implemented later, but the plan model should prepare for it.

### Optimize only wall-clock time

Rejected.

ShardLoom optimizes bytes read, bytes decoded, memory, materialization, shuffle, object-store requests, output fidelity, and cost.

## Risks

- Optimizer complexity may grow quickly.
- Runtime adaptation can make behavior harder to reason about.
- Incorrect runtime filters can cause wrong answers.
- Skew handling can be complex.
- Cost estimates may be wrong.
- Sink-driven planning may complicate APIs.
- Adaptive decisions may interact with spill and streaming in subtle ways.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- Optimizer design is first-class.
- Adaptive execution is a future requirement.
- Runtime filters and dynamic pruning are important.
- Skew detection and handling are required for Spark-displacement workloads.
- Sink requirements must influence planning.
- Memory/spill pressure must influence planning.
- Optimizer diagnostics must be structured and explainable.
- No fallback execution is permitted.

## Verification plan

Future implementation PRs should verify:

- Optimization decisions can be represented.
- Runtime filters can be represented.
- Adaptive plan changes can be represented.
- Skew diagnostics can be represented.
- Sink-driven materialization choices can be represented.
- Memory/spill requirements can influence plans.
- Unsupported optimizer behavior fails deterministically.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What is the first optimizer rule to implement?
- Should the optimizer be rule-based first, cost-based first, or hybrid?
- What runtime filter should be implemented first?
- What join strategy should be implemented first?
- What aggregate strategy should be implemented first?
- How should adaptive plan changes be represented in explain output?
- How should optimizer metrics feed benchmark reports?


### Future optimizer decision kinds

#### OptimizerDecisionReport

Required fields:
- `decision_kind`
- `rule_id`
- `input_nodes`
- `output_nodes`
- `proof_basis`
- `estimated_work_avoided`
- `required_capabilities`
- `residual_work`
- `correctness_guarantee`
- `diagnostics`

Decision kinds:
- `PrunedByMetadata`: A subtree was removed based on metadata/statistics proofs; report proof basis, avoided work, and correctness guarantee.
- `PushedDownExactly`: A filter/projection/limit was pushed fully to a lower boundary with exact semantics; report rule/proof and zero residual work.
- `PushedDownWithResidual`: A pushdown was partially accepted and residual work remains; report accepted scope and explicit residual work.
- `RejectedPushdown`: A pushdown candidate was refused due to capability/safety/correctness constraints; report rejection diagnostics and required missing capabilities.
- `FusedTasks`: Multiple tasks/operators were fused for efficiency under bounded-resource policy; report inputs/outputs and preserved correctness guarantees.
- `SplitSkewedTask`: A task/operator was split to mitigate skew or memory pressure; report skew basis, resulting nodes, and residual risks.
- `BuiltRuntimeFilter`: A runtime filter was constructed from a source side; report filter kind, correctness guarantee, and expected avoided work.
- `AppliedRuntimeFilter`: A runtime filter was applied to target nodes/segments; report application boundary and measured/estimated work avoided.
- `ChoseDecode`: The optimizer selected decode for a boundary that cannot remain encoded; report required capability gap and decode scope.
- `ChoseEncodedKernel`: The optimizer selected encoded-native kernel execution; report required capabilities and expected decode/materialization avoided.
- `RefusedDistributedExecution`: Distributed execution was explicitly refused; report refusal reason, safety basis, and resulting local-only residual work.

## Systems-learning optimizer vocabulary (R5.1)

`OptimizerDecisionKind` conceptual variants:
- `PrunedByMetadata`
- `PushedDownExactly`
- `PushedDownWithResidual`
- `RejectedPushdown`
- `FusedTasks`
- `SplitSkewedTask`
- `BuiltRuntimeFilter`
- `AppliedRuntimeFilter`
- `RejectedRuntimeFilter`
- `ChoseEncodedKernel`
- `ChoseDecode`
- `RefusedDistributedExecution`

Additional conceptual terms:
- `RuntimeFilterLifecycle`
- `PushdownGuarantee`
- `ProofBasis`
- split/coalesce/fuse decision reporting


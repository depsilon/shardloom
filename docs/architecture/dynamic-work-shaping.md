# Dynamic Work Shaping

## Purpose

`DynamicWorkShapingReport` is the CG-8 aggregate surface for connecting
adaptive sizing, runtime feedback signals, bounded-memory backpressure, and
scheduler queue policy before ShardLoom mutates execution policy at runtime.

The report is intentionally advisory. It does not apply feedback, resize live
tasks, execute streams, read data, perform object-store I/O, spill, write, or
delegate to another engine.

## Command

```text
shardloom dynamic-work-shaping-plan [balanced|memory-pressure|object-store-throttled|small-tasks|repeated-independent-shards]
shardloom cg8-runtime-promotion-gate
```

The profiles are deterministic examples:

- `balanced`: stable feedback with bounded backpressure.
- `memory-pressure`: target-task-byte reduction from high-memory-pressure feedback.
- `object-store-throttled`: target-task-byte increase/coalescing pressure from object-store
  throttling.
- `small-tasks`: target-task-byte increase from scheduler overhead pressure.
- `repeated-independent-shards`: target-task-byte increase for a repeated independent shard-task
  workload where automatic shaping should coalesce small shards before a future runtime applies the
  policy.

## Evidence Surfaces

The report tracks these surfaces in deterministic order:

- `adaptive_sizing_policy`
- `feedback_signals`
- `target_task_policy`
- `backpressure_policy`
- `bounded_memory_policy`
- `scheduler_queue_policy`
- `runtime_application_loop`
- `benchmark_evidence`
- `no_fallback_policy`

The current report marks sizing, feedback, target policy, backpressure, bounded
memory, scheduler queue policy, and no-fallback policy as planned surfaces.
Runtime application and benchmark evidence remain blocked.

The report now also derives a deterministic plan-only automatic work-shaping decision for the
canonical `repeated_independent_shard_tasks` workload kind. The decision can be
`keep_current_shape`, `split_large_shards`, `coalesce_small_shards`,
`coalesce_for_request_budget`, `mixed_signal_review`, or `blocked_by_invalid_feedback`. These fields
are advisory only: `automatic_work_shaping_applied=false`,
`automatic_work_shaping_claim_allowed=false`, `policy_mutated=false`, and `tasks_executed=false`
until a later runtime promotion gate admits policy mutation and certificate evidence.

`docs/architecture/pulseweave-runtime-control.md` defines the first runtime follow-through for that
promotion gate. `GAR-RUNTIME-IMPL-5R` scopes PulseWeave to prepared/local Vortex batch routes and
decomposes runtime application into FlowInventory bounded work-in-progress control,
ScarcityLedger resource-scarcity accounting, EndoPulse run-local feedback, and ProofBound evidence
gating. That implementation does not authorize object-store, distributed, live/hybrid, real
query-data spill, AI, or fallback execution.

## Runtime Boundary

Dynamic work shaping cannot mutate live execution policy until future runtime
work provides:

- observed task/runtime metrics
- conservative feedback provenance
- policy mutation rules
- scheduler requeue semantics
- memory/spill safety proof
- benchmark evidence for the adaptive loop
- updated execution certificates

Until those exist, `runtime_feedback_loop_ready=false`,
`policy_application_ready=false`, and `benchmark_evidence_ready=false`.

## Promotion Gate

`DynamicRuntimePromotionGateReport` is the report-only CG-8 gate for the step after advisory
planning. It keeps these runtime promotions blocked:

- dynamic sizing feedback application
- bounded parallel encoded read runtime
- source-backed reader split parallelism
- scheduler requeue policy
- bounded queue/backpressure runtime
- memory/spill reservation runtime
- object-store request-budget runtime
- benchmark and certificate closeout

The gate deliberately treats existing local streaming scan, bounded metadata/no-op, and local
filter-project bounded scan evidence as narrow local evidence only. Runtime policy mutation and
broader parallel source-backed reads require runtime metrics, target-task policy, scheduler queue
policy, memory/spill reservation evidence, backpressure evidence, cancellation/retry evidence,
execution certificates, Native I/O certificates, benchmark evidence, and no-fallback proof.

`cg8-runtime-promotion-gate` performs no runtime execution, task execution, reads,
materialization, object-store I/O, writes, spill I/O, feedback application, policy mutation, or
fallback execution.

## No-Fallback Policy

External systems may be benchmark or correctness references only. Dynamic work
shaping must never use Spark, DataFusion, DuckDB, Polars, Dask, Velox, or any
other engine to run or adapt ShardLoom work.

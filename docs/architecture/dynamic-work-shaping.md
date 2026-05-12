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
shardloom dynamic-work-shaping-plan [balanced|memory-pressure|object-store-throttled|small-tasks]
```

The profiles are deterministic examples:

- `balanced`: stable feedback with bounded backpressure.
- `memory-pressure`: target-task-byte reduction from high-memory-pressure feedback.
- `object-store-throttled`: target-task-byte increase/coalescing pressure from object-store
  throttling.
- `small-tasks`: target-task-byte increase from scheduler overhead pressure.

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

## No-Fallback Policy

External systems may be benchmark or correctness references only. Dynamic work
shaping must never use Spark, DataFusion, DuckDB, Polars, Dask, Velox, or any
other engine to run or adapt ShardLoom work.

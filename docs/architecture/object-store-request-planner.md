# Object Store Request Planner

This document defines the CG-10 aggregate surface that keeps object-store range, coalescing,
scheduling, checkpoint/retry, and commit evidence together before ShardLoom performs object-store IO
or distributed runtime work.

The first implementation is `ObjectStoreRequestPlannerReport`, exposed through:

```powershell
shardloom object-store-request-plan --format json
```

## Scope

- [x] Aggregate object-store byte-range planning evidence.
- [x] Aggregate request coalescing evidence.
- [x] Aggregate distributed task-shape scheduling evidence.
- [x] Aggregate checkpoint/retry/idempotency readiness evidence.
- [x] Aggregate object-store commit protocol readiness evidence.
- [ ] Execute byte-range reads.
- [ ] Start a coordinator or worker.
- [ ] Execute distributed tasks.
- [ ] Write checkpoint or attempt records.
- [ ] Execute object-store commits.

## Default Policy

- `full_file_read_allowed=false`
- `coordinator_started=false`
- `worker_started=false`
- `task_execution_allowed=false`
- `retry_execution_allowed=false`
- `checkpoint_write_allowed=false`
- `cleanup_execution_allowed=false`
- `commit_execution_allowed=false`
- `data_read=false`
- `object_store_io=false`
- `write_io=false`
- `fallback_execution_allowed=false`

The aggregate report is request-planning evidence only. It does not certify object-store runtime
execution, distributed execution, object-store writes, table-format commit execution, provider
probing, cloud credentials, or fallback behavior.

## Surface Order

1. `range_planning`
2. `request_coalescing`
3. `distributed_scheduling`
4. `checkpoint_retry`
5. `commit_protocol`

## Acceptance Boundaries

- [x] Every existing CG-10 planning surface is represented in one deterministic report.
- [x] The report keeps blocked component status visible instead of hiding it behind a generic
      unsupported result.
- [x] The CLI emits machine-readable JSON fields for component statuses, request/task/retry/commit
      counts, required evidence, side-effect flags, diagnostics, and no-fallback status.
- [x] Snapshot and contract tests assert the aggregate report is side-effect-free.
- [ ] Future object-store read execution must update this report before enabling object-store IO.
- [ ] Future distributed execution must update this report before coordinator/worker/task execution.
- [ ] Future object-store commit execution must update this report before writes or
      provider-specific behavior.

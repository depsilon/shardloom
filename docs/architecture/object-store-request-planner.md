# Object Store Request Planner

This document defines the CG-10 aggregate surface that keeps object-store range, coalescing,
scheduling, checkpoint/retry, and commit evidence together before ShardLoom performs object-store IO
or distributed runtime work.

The first implementation is `ObjectStoreRequestPlannerReport`, exposed through:

```powershell
shardloom object-store-request-plan --format json
```

The object-store/distributed runtime promotion gate is `ObjectStoreRuntimePromotionGateReport`,
exposed through:

```powershell
shardloom cg10-object-store-runtime-gate --format json
```

## Scope

- [x] Aggregate object-store byte-range planning evidence.
- [x] Aggregate request coalescing evidence.
- [x] Aggregate distributed task-shape scheduling evidence.
- [x] Aggregate checkpoint/retry/idempotency readiness evidence.
- [x] Aggregate object-store commit protocol readiness evidence.
- [x] Gate future byte-range provider reads through
      `ObjectStoreByteRangeProviderGateReport` with credential, retry, idempotency, provider
      capability, execution-certificate, Native I/O, and benchmark evidence requirements.
- [x] Gate coordinator start, worker start, task execution, checkpoint writes, retry attempts,
      cleanup execution, and commit-record writes through
      `ObjectStoreRuntimeBlockerMatrixRow` entries.
- [x] Gate object-store and distributed runtime execution through
      `ObjectStoreRuntimePromotionGateReport` before enabling runtime object-store IO.
Out of scope until promoted GAR slices complete:

- Byte-range read execution remains blocked after `GAR-0008-A`; that slice adds the provider gate
  only.
- Coordinator/worker start, distributed tasks, checkpoint/attempt records, retry execution, cleanup,
  and object-store commits remain blocked after `GAR-0008-B`; that slice adds the blocker matrix
  only. `GAR-0017-A` exposes the fault-tolerance execution gate and `GAR-0028-A` exposes the
  object-store/lakehouse commit-semantics gate; runtime promotion still requires future provider,
  credential, execution-certificate, Native I/O, benchmark, and no-fallback evidence.

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

For the byte-range provider gate:

- `byte_range_provider_gate_status=blocked_until_certified`
- `byte_range_provider_gate_range_read_execution_allowed=false`
- `byte_range_provider_gate_full_file_read_allowed=false`
- `byte_range_provider_gate_credential_resolution_allowed=false`
- `byte_range_provider_gate_credentials_resolved=false`
- `byte_range_provider_gate_retry_execution_allowed=false`
- `byte_range_provider_gate_provider_probe=false`
- `byte_range_provider_gate_network_probe=false`
- `byte_range_provider_gate_data_read=false`
- `byte_range_provider_gate_object_store_io=false`
- `byte_range_provider_gate_write_io=false`
- `byte_range_provider_gate_fallback_attempted=false`
- `byte_range_provider_gate_fallback_execution_allowed=false`
- `byte_range_provider_gate_external_engine_invoked=false`
- `byte_range_provider_gate_claim_gate_status=not_claim_grade`

The provider gate requires provider capability policy, credential-effect policy, request-budget
policy, retry policy, idempotency-key contract, execution certificate, Native I/O certificate, and
benchmark evidence before future byte-range reads may be promoted.

For the object-store runtime blocker matrix:

- `runtime_blocker_matrix_status=blocked_until_certified`
- `runtime_blocker_matrix_row_order=coordinator_start,worker_start,task_execution,checkpoint_write,retry_attempt,cleanup_execution,commit_record_write`
- `runtime_blocker_matrix_diagnostics_propagated=true`
- `runtime_blocker_matrix_diagnostic_count=7`
- `runtime_blocker_matrix_diagnostic_category_order=object_store,object_store,object_store,object_store,object_store,object_store,object_store`
- `runtime_blocker_matrix_diagnostic_severity_order=info,info,info,info,info,info,info`
- `runtime_blocker_matrix_envelope_status=success`
- `runtime_blocker_matrix_all_allowed_false=true`
- `runtime_blocker_matrix_all_no_io=true`
- `runtime_blocker_matrix_all_no_fallback=true`
- `runtime_blocker_matrix_all_no_external_engine=true`

Every row carries `diagnostic_code=SL_OBJECT_STORE_UNSUPPORTED`,
`claim_gate_status=not_claim_grade`, `allowed=false`, `data_read=false`,
`object_store_io=false`, `write_io=false`, `fallback_attempted=false`,
`fallback_execution_allowed=false`, and `external_engine_invoked=false`, plus a row-specific
blocker ID and required-evidence list.

The runtime promotion gate also copies every blocker row into the typed output envelope diagnostics
array as `severity=info`, `category=object_store`, `code=SL_OBJECT_STORE_UNSUPPORTED`, and
`fallback.attempted=false`. The command remains `status=success` because this surface is a
report-only promotion gate; the info diagnostics document blocked runtime families without
attempting execution or forcing agents to scrape human text.

For the CG-10 runtime promotion gate:

- `range_read_execution_allowed=false`
- `full_file_read_allowed=false`
- `request_coalescing_runtime_allowed=false`
- `coordinator_start_allowed=false`
- `worker_start_allowed=false`
- `task_execution_allowed=false`
- `retry_execution_allowed=false`
- `checkpoint_write_allowed=false`
- `cleanup_execution_allowed=false`
- `commit_execution_allowed=false`
- `credential_resolution_allowed=false`
- `object_store_io_allowed=false`
- `data_read_allowed=false`
- `write_io_allowed=false`
- `object_store_runtime_claim_allowed=false`
- `distributed_runtime_claim_allowed=false`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`

The aggregate report is request-planning evidence only. It does not certify object-store runtime
execution, distributed execution, object-store writes, table-format commit execution, provider
probing, cloud credentials, or fallback behavior.

## GAR-COMPAT-1C Universal Compatibility Admission Ladder

The universal compatibility scoreboard projects the same fail-closed posture through
`shardloom.universal_compatibility.object_store_admission_ladder.v1` so user-facing status,
Python typed accessors, and website/status pages can answer "Can I use S3/GCS/ADLS?" without
scraping this planner document.

The ladder order is:

```text
object_store_uri_parse
credential_policy
public_no_credential_read
authenticated_read
byte_range_read
full_file_read
local_cache
write_staging
commit_protocol
```

Every ladder row keeps:

```text
credential_resolution_performed=false
network_probe_allowed=false
provider_probe_allowed=false
object_store_io=false
write_io=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

`object_store_uri_parse` is report-only URI vocabulary. Public no-credential reads,
authenticated reads, byte-range reads, full-file reads, local cache, write staging, and commit
protocol remain blocked until separate runtime evidence exists. The ladder is status visibility
only; it does not authorize credential lookup, provider probes, network traffic, object-store
reads/writes, local cache runtime, commit protocol execution, table/lakehouse runtime, production
use, performance claims, or fallback execution.

## Surface Order

1. `range_planning`
2. `request_coalescing`
3. `distributed_scheduling`
4. `checkpoint_retry`
5. `commit_protocol`

The CG-10 runtime promotion gate also lists `byte_range_provider_gate` as existing report-only
evidence before `range_read_execution`; range-read execution itself remains blocked.

## Acceptance Boundaries

- [x] Every existing CG-10 planning surface is represented in one deterministic report.
- [x] The byte-range provider gate is represented as report-only evidence and keeps credential
      resolution, provider probes, network probes, range reads, retry execution, object-store I/O,
      write I/O, external engines, and fallback disabled by default.
- [x] The runtime blocker matrix is represented as report-only evidence and keeps coordinator,
      worker, task, checkpoint, retry, cleanup, and commit-record actions disabled by default.
- [x] The report keeps blocked component status visible instead of hiding it behind a generic
      unsupported result.
- [x] The CLI emits machine-readable JSON fields for component statuses, request/task/retry/commit
      counts, required evidence, side-effect flags, diagnostics, and no-fallback status.
- [x] Snapshot and contract tests assert the aggregate report is side-effect-free.
- [x] Future object-store read execution must update this report before enabling object-store IO.
- [x] Future distributed execution must update this report before coordinator/worker/task execution.
- [x] Future object-store commit execution must update this report before writes or
      provider-specific behavior.

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
- [x] Admit `public-no-credential-fixture` as a no-network read profile that parses S3/GCS/ADLS
      object URIs and reads explicit local fixture bytes with SourceState and Native I/O evidence.
Out of scope until promoted GAR slices complete:

- Live-provider byte-range read execution remains blocked after `GAR-0008-A`; that slice adds the
  provider gate only. `GAR-RUNTIME-IMPL-5K` admits public no-credential fixture bytes without
  provider/network I/O.
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
- `byte_range_provider_gate_public_no_credential_fixture_profile_admitted=true`
- `byte_range_provider_gate_public_no_credential_fixture_read_allowed=true`
- `byte_range_provider_gate_public_no_credential_fixture_listing_allowed=true`
- `byte_range_provider_gate_public_no_credential_fixture_cache_write_allowed=false`
- `byte_range_provider_gate_live_provider_network_read_allowed=false`
- `byte_range_provider_gate_fallback_attempted=false`
- `byte_range_provider_gate_fallback_execution_allowed=false`
- `byte_range_provider_gate_external_engine_invoked=false`
- `byte_range_provider_gate_claim_gate_status=not_claim_grade`

The provider gate requires provider capability policy, credential-effect policy, request-budget
policy, retry policy, idempotency-key contract, execution certificate, Native I/O certificate, and
benchmark evidence before future live-provider byte-range reads may be promoted. The
`public-no-credential-fixture` profile is separately admitted as a no-network fixture path: it may
parse S3/GCS/ADLS object URIs and read explicit local fixture bytes, but it does not authorize live
provider fetches, credential resolution, network probes, provider probes, cache writes, or provider
listing.

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

## GAR-RUNTIME-IMPL-4N Local-Emulator Read Smoke

`GAR-RUNTIME-IMPL-4N` admits one runtime read profile:

```powershell
shardloom object-store-read-smoke <local-object-path> --profile local-emulator [--range offset:length] --format json
```

This is not real S3/GCS/ADLS support. The profile treats a local file as an object-store emulator
fixture so the runtime can prove URI/path admission, byte-range or full-file read behavior,
SourceState evidence, Native I/O certificate posture, and no-fallback fields without resolving
credentials or probing a network provider.

Successful local-emulator rows emit:

```text
provider_profile=local-emulator
object_store_read_status=succeeded
byte_range_read_status=performed_local_emulator | not_requested
full_file_read_status=performed_local_emulator | not_requested
source_state_id
source_state_digest
source_fingerprint_kind
source_content_digest
credential_resolution_performed=false
network_probe_performed=false
provider_probe_performed=false
native_io_certificate_status=fixture_smoke_only
claim_gate_status=fixture_smoke_only
object_store_io=true
object_store_read_io=true
object_store_write_io=false
fallback_attempted=false
external_engine_invoked=false
```

Remote provider URIs such as `s3://`, `gs://`, `abfs://`, and `abfss://` remain blocked by this
command with deterministic `SL_OBJECT_STORE_UNSUPPORTED` diagnostics and with
`credential_resolution_performed=false`, `network_probe_performed=false`, `object_store_io=false`,
`fallback_attempted=false`, and `external_engine_invoked=false`.

The local-emulator smoke does not authorize credential lookup, provider listing, public-object
reads, authenticated reads, object-store writes, table/lakehouse commits, distributed execution,
performance claims, production use, or external-engine fallback.

## GAR-RUNTIME-IMPL-5K Public No-Credential Fixture Read Smoke

`GAR-RUNTIME-IMPL-5K` admits a second runtime read profile:

```powershell
shardloom object-store-read-smoke <s3|gs|gcs|abfs|abfss URI> --profile public-no-credential-fixture --public-fixture-path <local-fixture-path> [--fixture-listing] [--range offset:length] --format json
```

The profile parses supported S3/GCS/ADLS object URIs and then reads caller-supplied local fixture
bytes. It is the first public no-credential admission proof, but still not a live cloud-provider
network read. The command rejects missing fixture paths, unsupported URI schemes, URI query strings
or fragments, URI userinfo/credentials, empty bucket/container names, empty object keys, missing
fixture files, invalid ranges, and unreadable fixture bytes with deterministic diagnostics. It does
not resolve credentials, probe a provider, open a network connection, write a local cache entry, or
invoke any external query engine.

Successful public fixture rows emit:

```text
provider_profile=public-no-credential-fixture
object_store_provider=s3 | gcs | adls
object_store_bucket
object_store_key
object_store_uri_parse_status=parsed_public_no_credential_fixture_uri
requested_uri_redaction_status
public_fixture_path
byte_range_read_status=performed_public_no_credential_fixture | not_requested
full_file_read_status=performed_public_no_credential_fixture | not_requested
listing_status=performed_public_fixture_single_object | not_requested_public_fixture
object_etag
object_version
source_state_id
source_state_digest
source_fingerprint_kind=public_no_credential_fixture_uri_metadata_range_digest
source_content_digest
credential_policy_status=public_no_credential_fixture_admitted
credential_resolution_performed=false
network_probe_performed=false
provider_probe_performed=false
local_cache_status=not_performed_public_fixture_read_through
native_io_certificate_status=public_fixture_smoke_only
claim_gate_status=public_fixture_smoke_only
public_no_credential_fixture_claim_allowed=true
public_object_store_claim_allowed=false
production_object_store_claim_allowed=false
object_store_io=true
object_store_read_io=true
object_store_write_io=false
fallback_attempted=false
external_engine_invoked=false
```

The public fixture profile does not authorize live public bucket reads, signed URLs, authenticated
reads, provider listing, local cache writes, cloud writes, table/lakehouse commits, distributed
runtime, performance claims, production use, or external-engine fallback.

## GAR-RUNTIME-IMPL-4O Local-Emulator Write/Commit Smoke

`GAR-RUNTIME-IMPL-4O` now admits one fixture-scoped write profile:

```powershell
shardloom object-store-write-smoke <local-source-path> <local-object-path> --profile local-emulator [--idempotency-key key] [--allow-overwrite] [--rollback-after-commit] --format json
```

This is not real S3/GCS/ADLS support and not a table/lakehouse commit protocol. The profile treats
a local file path as a local-emulator object target, writes through a staging path, commits by
renaming the object and writing a ShardLoom sidecar commit manifest, and can immediately roll the
object and manifest back for cleanup proof. It does not resolve credentials, list objects, probe a
network provider, or invoke any external query engine.

Successful local-emulator rows emit:

```text
provider_profile=local-emulator
object_store_write_status=committed | rolled_back
write_staging_status=performed_local_emulator
commit_protocol=local_emulator_sidecar_manifest
commit_protocol_status=committed | rolled_back
commit_status=committed_local_emulator_object | committed_then_rolled_back
rollback_status=not_requested | performed_local_emulator_cleanup
cleanup_deleted_count
idempotency_key
idempotency_status=caller_supplied | derived_from_payload_digest
payload_bytes
written_bytes
payload_digest
target_content_digest
commit_manifest_digest
claim_gate_status=fixture_smoke_only
object_store_io=true
object_store_write_io=true
write_io=true
table_commit_allowed=false
fallback_attempted=false
external_engine_invoked=false
```

Remote provider URIs such as `s3://`, `gs://`, `abfs://`, and `abfss://` remain blocked by this
runtime path with `object_store_write_status=blocked_remote_provider`,
`credential_resolution_performed=false`, `network_probe_performed=false`, `provider_probe_performed=false`,
`object_store_io=false`, `write_io=false`, `fallback_attempted=false`, and
`external_engine_invoked=false`.

The local-emulator write smoke does not authorize credential lookup, provider listing, public-object
reads or writes, authenticated object-store writes, table metadata writes, table/lakehouse commits,
catalog interaction, distributed execution, performance claims, production use, or external-engine
fallback. The separate local table append commit rehearsal below writes only a ShardLoom-owned
local-manifest fixture artifact and does not upgrade this object-store smoke into table-format or
cloud-provider support.

## GAR-RUNTIME-IMPL-4O Local Table Append Commit Rehearsal

`GAR-RUNTIME-IMPL-4O` now admits one fixture-scoped table operation profile:

```powershell
shardloom local-table-append-commit-rehearsal-smoke <local-committed-manifest-path> --profile local-manifest [--idempotency-key key] [--allow-overwrite] [--rollback-after-commit] --format json
```

This is not an Iceberg, Delta, Hudi, catalog, object-store, or production lakehouse commit protocol.
The profile declares a ShardLoom-owned local-manifest fixture with a base snapshot and append delta,
writes a staged committed manifest JSON plus sidecar table commit record to local paths, records
idempotency and digest evidence, and can immediately roll those artifacts back for cleanup proof. It
does not resolve credentials, list objects, probe a provider, contact a catalog, or invoke any
external query engine.

Successful local-manifest rows emit:

```text
provider_profile=local-manifest
table_format=shardloom_local_manifest
table_append_commit_status=committed | rolled_back
write_staging_status=performed_local_manifest
commit_protocol=local_manifest_sidecar_commit_record
commit_protocol_status=committed | rolled_back
commit_status=committed_local_manifest | committed_then_rolled_back
table_commit_rehearsal_status=rehearsed_local_manifest_commit | rehearsed_then_rolled_back
rollback_status=not_requested | performed_local_manifest_cleanup
cleanup_deleted_count
idempotency_key
idempotency_status=caller_supplied | derived_from_manifest_digest
base_snapshot_id
append_snapshot_id
committed_snapshot_id
manifest_file_count
manifest_segment_count
base_row_count
append_row_count
effective_row_count
manifest_payload_digest
committed_manifest_digest
commit_record_digest
correctness_digest
claim_gate_status=scoped_local_table_append_commit_rehearsal_only
catalog_io_performed=false
object_store_io=false
table_catalog_commit_performed=false
fallback_attempted=false
external_engine_invoked=false
```

Remote provider URIs such as `s3://`, `gs://`, `abfs://`, and `abfss://` remain blocked with
`table_append_commit_status=blocked_remote_provider`, `credential_resolution_performed=false`,
`network_probe_performed=false`, `provider_probe_performed=false`, `object_store_io=false`,
`write_io=false`, `fallback_attempted=false`, and `external_engine_invoked=false`.

The local table append commit rehearsal does not authorize credential lookup, provider listing,
public/authenticated cloud writes, table-format dependencies, catalog interaction, object-store
table commits, production rollback/recovery, merge/update/delete, distributed execution, performance
claims, production use, or external-engine fallback.

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

Every ladder row keeps live-provider effects disabled:

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

`object_store_uri_parse` is report-only URI vocabulary. `public_no_credential_read` is now admitted
only for the `public-no-credential-fixture` profile, where the URI is parsed and caller-supplied
local fixture bytes are read without credentials, provider probes, network traffic, or cache writes.
Authenticated reads, live-provider byte-range reads, full-file provider reads, local cache writes,
write staging, and commit protocol remain blocked until separate runtime evidence exists. The
ladder does not authorize credential lookup, provider probes, network traffic, live object-store
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

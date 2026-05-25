<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local table append commit rehearsal smoke

## Quick Answer

- **Audience:** user validating the first fixture-scoped table metadata/snapshot append commit rehearsal without catalog or cloud services
- **Status:** `smoke_supported`
- **Execution mode:** `local_table_append_commit_rehearsal_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Local-manifest fixture table append commit rehearsal only; no Iceberg/Delta/Hudi production runtime, catalog service, object-store table commit, merge/update/delete, distributed runtime, production use, performance claim, or Spark-replacement claim.

## Can ShardLoom Do This?

ShardLoom can run an explicit local-manifest table append commit rehearsal over a ShardLoom-owned fixture. It writes a staged committed manifest plus sidecar table commit record to local paths and reports snapshot, idempotency, digest, rollback, Native I/O, and no-fallback evidence.

## Claim Boundary

Local-manifest fixture table append commit rehearsal only; no Iceberg/Delta/Hudi production runtime, catalog service, object-store table commit, merge/update/delete, distributed runtime, production use, performance claim, or Spark-replacement claim.

## How To Try It

```powershell
target\debug\shardloom local-table-append-commit-rehearsal-smoke target\table-commit\metadata-v2.json --profile local-manifest --idempotency-key orders-table-commit-001 --rollback-after-commit --format json
```

## Internal Flow

`local_manifest_fixture, append_delta_fixture -> local_table_append_commit_rehearsal_smoke -> batch -> committed_local_manifest, sidecar_table_commit_record, rollback_cleanup_evidence, native_io_certificate -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=local-manifest`
- `table_append_commit_status`
- `table_format=shardloom_local_manifest`
- `base_snapshot_id`
- `append_snapshot_id`
- `committed_snapshot_id`
- `manifest_file_count`
- `manifest_segment_count`
- `base_row_count`
- `append_row_count`
- `effective_row_count`
- `write_staging_status`
- `commit_protocol_status`
- `commit_status`
- `table_commit_rehearsal_status`
- `rollback_status`
- `cleanup_deleted_count`
- `idempotency_key`
- `manifest_payload_digest`
- `committed_manifest_digest`
- `commit_record_digest`
- `correctness_digest`
- `credential_resolution_performed=false`
- `network_probe_performed=false`
- `catalog_io_performed=false`
- `object_store_io=false`
- `table_catalog_commit_performed=false`
- `native_io_certificate_status`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A fixture-smoke report with base/append/committed snapshot ids, committed manifest and sidecar commit-record digests, idempotency, optional rollback cleanup, local-manifest Native I/O evidence, catalog/object-store/network-disabled fields, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `treating_local_manifest_rehearsal_as_iceberg_commit`
- `expecting_catalog_commit`
- `expecting_s3_table_commit`
- `expecting_merge_update_delete_runtime`
- `treating_rollback_cleanup_as_production_recovery`

## Reference Files

- `docs/architecture/table-intelligence-layer.md` - What this proves: Table maintenance execution matrix posture and local append commit rehearsal evidence boundaries.
- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store and table/lakehouse operation ladders remain separated, with cloud/provider table commits still blocked.
- `docs/architecture/phased-execution-completed-ledger.md` - What this proves: Completed-session provenance for the local table append rehearsal and its claim boundary.
- `python/README.md` - What this proves: Python-facing command wrapper posture and local technical-preview scope.

## Related Use Cases

- `object-store-local-emulator-write-smoke`
- `object-store-local-emulator-read-smoke`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/native-io-certificate.html` - Native I/O certificate (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/object-store-boundary.html` - Object-store boundary (`Platform Boundaries` / `blocked`)
- `website/field-guide/table-lakehouse-boundary.html` - Table/lakehouse boundary (`Platform Boundaries` / `blocked`)

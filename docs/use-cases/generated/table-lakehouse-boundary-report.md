<!-- SPDX-License-Identifier: Apache-2.0 -->

# Table and lakehouse boundary

## Quick Answer

- **Audience:** user asking whether Iceberg metadata, table scans, Delta, Hudi, or catalog commits are supported
- **Status:** `smoke_supported`
- **Execution mode:** `iceberg_metadata_json_manifest_list_and_manifest_file_split_plan_smoke_plus_blocked_runtime`
- **Engine mode:** `batch`
- **Claim boundary:** ShardLoom has scoped local-manifest metadata/read and append-commit rehearsal evidence, a scoped local Iceberg metadata JSON smoke with snapshot selection, an explicitly requested feature-gated local Avro manifest-list summary smoke, and an explicitly requested feature-gated local Avro manifest-file split-plan smoke. That does not promote Iceberg data-file runtime, Delta/Hudi runtime, external catalogs, object-store table runtime, table scans, table writes/commits, production lakehouse support, Foundry support, or performance claims.

## Can ShardLoom Do This?

Table and lakehouse boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

ShardLoom has scoped local-manifest metadata/read and append-commit rehearsal evidence, a scoped local Iceberg metadata JSON smoke with snapshot selection, an explicitly requested feature-gated local Avro manifest-list summary smoke, and an explicitly requested feature-gated local Avro manifest-file split-plan smoke. That does not promote Iceberg data-file runtime, Delta/Hudi runtime, external catalogs, object-store table runtime, table scans, table writes/commits, production lakehouse support, Foundry support, or performance claims.

## How To Try It

```text
target\debug\shardloom iceberg-metadata-read-smoke target\iceberg\metadata.json --format json
```

## Blocker

Table-format runtime still needs data-file scan execution, delete/tombstone semantics, write/commit/rollback, object-store, catalog, and certificate evidence before production support can be claimed.

## Internal Flow

`local_iceberg_metadata_json, optional_local_iceberg_manifest_list_avro, optional_local_iceberg_manifest_avro, iceberg_table, delta_table, hudi_table, catalog_metadata -> iceberg_metadata_json_manifest_list_and_manifest_file_split_plan_smoke_plus_blocked_runtime -> batch -> iceberg_metadata_summary, snapshot_selection_evidence, optional_manifest_list_summary, optional_manifest_file_split_plan, table_compatibility_matrix, deterministic_blocker -> evidence -> claim gate`

## Evidence You Should See

- `iceberg_metadata_read_smoke_status`
- `schema_version=shardloom.iceberg_metadata_read_smoke.v1`
- `metadata_read_status`
- `local_metadata_json_read_performed=true`
- `snapshot_selection_performed`
- `time_travel_selection_performed`
- `manifest_list_requested`
- `manifest_list_reader_feature_enabled`
- `manifest_list_read_performed`
- `manifest_summary_pruning_performed`
- `planned_manifest_split_count`
- `planned_data_file_count`
- `manifest_file_requested`
- `manifest_file_reader_feature_enabled`
- `manifest_file_read_performed`
- `data_file_split_planning_performed`
- `planned_data_file_split_count`
- `planned_data_file_split_bytes`
- `data_file_read_performed=false`
- `delete_file_semantics`
- `table_scan_status`
- `delete_tombstone_status`
- `commit_protocol_status`
- `rollback_status`
- `local_table_append_commit_rehearsal_smoke_present`
- `local_table_manifest_write_request_count`
- `local_table_commit_record_write_request_count`
- `local_table_recovery_read_request_count`
- `table_translation_report_status`
- `table_metadata_loss_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=scoped_iceberg_metadata_json_smoke_only`
- `claim_gate_status=scoped_iceberg_metadata_manifest_list_summary_smoke`
- `claim_gate_status=scoped_iceberg_manifest_file_split_plan_smoke`

## Expected Output Or Evidence

A scoped Iceberg metadata JSON smoke report with table metadata fields, current/explicit/as-of snapshot selection evidence, optional feature-gated manifest-list summary/split-count evidence, optional feature-gated manifest-file split-plan evidence, deterministic blockers for delete files and broader table runtime, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `mistaking_metadata_json_smoke_for_table_runtime`
- `mistaking_manifest_list_summary_for_data_scan_runtime`
- `mistaking_manifest_file_split_plan_for_data_scan_runtime`
- `treating_local_commit_rehearsal_as_iceberg_commit`
- `expecting_catalog_commit`
- `expecting_s3_table_commit`
- `expecting_merge_update_delete_runtime`
- `treating_local_files_as_lakehouse_support`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/table-intelligence-layer.md` - What this proves: Table maintenance execution posture and lakehouse/table claim boundaries.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.

## Related Use Cases

- `local-table-append-commit-rehearsal-smoke`
- `object-store-boundary-report`
- `object-store-local-emulator-write-smoke`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- [Object-store boundary](https://shardloom.io/field-guide/object-store-boundary) (`Platform Boundaries` / `smoke_supported`)
- [Table/lakehouse boundary](https://shardloom.io/field-guide/table-lakehouse-boundary) (`Platform Boundaries` / `smoke_supported`)

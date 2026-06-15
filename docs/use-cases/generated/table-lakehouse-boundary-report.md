<!-- SPDX-License-Identifier: Apache-2.0 -->

# Table and lakehouse boundary

## Quick Answer

- **Audience:** user asking whether Iceberg metadata, table scans, Delta, Hudi, or catalog commits are supported
- **Status:** `smoke_supported`
- **Execution mode:** `iceberg_delta_hudi_metadata_smokes_plus_blocked_runtime`
- **Engine mode:** `batch`
- **Claim boundary:** ShardLoom has scoped local-manifest metadata/read and append-commit rehearsal evidence, a scoped local Iceberg metadata JSON smoke with snapshot selection, explicitly requested feature-gated local Avro Iceberg manifest-list and manifest-file smokes, metadata-level Iceberg schema/partition/delete/deletion-vector admission evidence, a scoped local Delta transaction log metadata smoke, and a scoped local Hudi timeline/metadata summary smoke. That does not promote Iceberg data-file runtime, schema projection execution, partition-filter execution, delete application, Delta checkpoint replay/runtime, Delta deletion-vector application, Hudi base-file scan, Hudi log merge, Hudi table-service execution, external catalogs, object-store table runtime, table scans, table writes/commits, production lakehouse support, Foundry support, or performance claims.

## Can ShardLoom Do This?

Table and lakehouse boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

ShardLoom has scoped local-manifest metadata/read and append-commit rehearsal evidence, a scoped local Iceberg metadata JSON smoke with snapshot selection, explicitly requested feature-gated local Avro Iceberg manifest-list and manifest-file smokes, metadata-level Iceberg schema/partition/delete/deletion-vector admission evidence, a scoped local Delta transaction log metadata smoke, and a scoped local Hudi timeline/metadata summary smoke. That does not promote Iceberg data-file runtime, schema projection execution, partition-filter execution, delete application, Delta checkpoint replay/runtime, Delta deletion-vector application, Hudi base-file scan, Hudi log merge, Hudi table-service execution, external catalogs, object-store table runtime, table scans, table writes/commits, production lakehouse support, Foundry support, or performance claims.

## How To Try It

```text
target\debug\shardloom iceberg-metadata-read-smoke target\iceberg\metadata.json --format json; target\debug\shardloom delta-log-metadata-read-smoke target\delta\00000000000000000000.json --format json; target\debug\shardloom hudi-timeline-metadata-read-smoke target\hudi\.hoodie --metadata-json target\hudi\metadata-summary.json --format json
```

## Blocker

Table-format runtime still needs data-file scan execution, schema projection execution, partition-filter execution, delete application, Delta checkpoint replay/deletion-vector application, Hudi base-file/log-file merge and table-service execution, write/commit/rollback, object-store, catalog, and certificate evidence before production support can be claimed.

## Internal Flow

`local_iceberg_metadata_json, optional_local_iceberg_manifest_list_avro, optional_local_iceberg_manifest_avro, local_delta_log_json, local_hudi_timeline_directory, optional_local_hudi_metadata_summary_json, iceberg_table, delta_table, hudi_table, catalog_metadata -> iceberg_delta_hudi_metadata_smokes_plus_blocked_runtime -> batch -> iceberg_metadata_summary, snapshot_selection_evidence, optional_manifest_list_summary, optional_manifest_file_split_plan, schema_partition_evolution_admission, delete_deletion_vector_admission, delta_log_metadata_summary, hudi_timeline_metadata_summary, table_compatibility_matrix, deterministic_blocker -> evidence -> claim gate`

## Evidence You Should See

- `iceberg_metadata_read_smoke_status`
- `schema_version=shardloom.iceberg_metadata_read_smoke.v1`
- `schema_version=shardloom.delta_log_metadata_read_smoke.v1`
- `schema_version=shardloom.hudi_timeline_metadata_read_smoke.v1`
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
- `schema_evolution_present`
- `schema_id_order`
- `schema_evolution_admission_status`
- `partition_evolution_present`
- `partition_spec_id_order`
- `manifest_partition_spec_id_order`
- `partition_evolution_admission_status`
- `manifest_file_position_delete_file_entry_count`
- `manifest_file_equality_delete_file_entry_count`
- `manifest_file_deletion_vector_entry_count`
- `delete_tombstone_deletion_vector_admission_status`
- `local_delta_log_json_read_performed=true`
- `min_reader_version`
- `min_writer_version`
- `reader_feature_order`
- `writer_feature_order`
- `deletion_vector_action_count`
- `cdc_action_count`
- `local_timeline_directory_read_performed=true`
- `timeline_entry_count`
- `pending_instant_count`
- `delta_commit_action_count`
- `metadata_table_summary_json_read_performed`
- `metadata_partition_order`
- `data_file_read_performed=false`
- `checkpoint_read_performed=false`
- `base_file_read_performed=false`
- `log_file_read_performed=false`
- `metadata_table_storage_read_performed=false`
- `table_service_execution_performed=false`
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
- `claim_gate_status=scoped_delta_transaction_log_metadata_smoke_only`
- `claim_gate_status=scoped_hudi_timeline_metadata_smoke_only`

## Expected Output Or Evidence

Scoped Iceberg/Delta/Hudi metadata smoke reports with table metadata fields, current/explicit/as-of Iceberg snapshot selection evidence, optional feature-gated Iceberg manifest-list and manifest-file evidence, schema/partition/delete/deletion-vector admission fields, Delta protocol/action/table-feature/deletion-vector fields, Hudi timeline/action/metadata-partition fields, deterministic blockers for broader table runtime, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `mistaking_metadata_json_smoke_for_table_runtime`
- `mistaking_manifest_list_summary_for_data_scan_runtime`
- `mistaking_manifest_file_split_plan_for_data_scan_runtime`
- `mistaking_delta_log_metadata_for_delta_runtime`
- `mistaking_hudi_timeline_metadata_for_hudi_runtime`
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

# Table Intelligence Layer

This document defines the CG-9 aggregate surface that keeps table, schema, CDC, layout, and
compaction evidence visible before ShardLoom performs catalog reads, table metadata reads, data
reads, maintenance writes, or table-format runtime behavior.

`docs/architecture/table-protocol-source-review.md` records the current source-checked external
protocol intake for `PROD-READY-1C`; this layer remains the runtime/evidence boundary and does not
promote external protocols by source review alone.

The first implementation is `TableIntelligenceReport`, exposed through:

```powershell
shardloom table-intelligence-plan --format json
```

The catalog/table metadata promotion gate is `CatalogMetadataIntegrationGateReport`, exposed
through:

```powershell
shardloom cg9-catalog-metadata-gate --format json
```

The scoped local metadata smoke is `LocalTableMetadataReadSmokeReport`, exposed through:

```powershell
shardloom local-table-metadata-read-smoke --format json
```

The scoped source-reviewed Iceberg metadata JSON smoke is exposed through:

```powershell
shardloom iceberg-metadata-read-smoke <metadata-json-path> [--snapshot-id id|--as-of-timestamp-ms ms] [--manifest-list local.avro] [--manifest local.avro] --format json
```

The scoped source-reviewed Delta log metadata smoke is exposed through:

```powershell
shardloom delta-log-metadata-read-smoke <delta-log-json-path> --format json
```

The scoped source-reviewed Hudi timeline metadata smoke is exposed through:

```powershell
shardloom hudi-timeline-metadata-read-smoke <timeline-dir> [--metadata-json local.json] --format json
```

The scoped local delete/tombstone smoke is `LocalDeleteTombstoneReadSmokeReport`, exposed through:

```powershell
shardloom local-delete-tombstone-read-smoke --format json
```

The scoped append-only CDC overlay smoke is `LocalAppendOnlyCdcOverlaySmokeReport`, exposed
through:

```powershell
shardloom local-append-only-cdc-overlay-smoke --format json
```

The scoped local table append commit rehearsal is exposed through:

```powershell
shardloom local-table-append-commit-rehearsal-smoke <local-committed-manifest-path> --profile local-manifest [--idempotency-key key] [--allow-overwrite] [--rollback-after-commit] --format json
```

The scoped local table commit recovery replay is exposed through:

```powershell
shardloom local-table-commit-recovery-smoke <local-committed-manifest-path> --profile local-manifest [--idempotency-key key] --format json
```

## Scope

- [x] Aggregate existing schema evolution compatibility evidence.
- [x] Aggregate partition evolution compatibility evidence.
- [x] Aggregate delete/tombstone compatibility evidence.
- [x] Aggregate table compatibility evidence.
- [x] Aggregate CDC incremental planning evidence.
- [x] Aggregate layout health planning evidence.
- [x] Aggregate compaction planning evidence.
- [x] Track snapshot/manifest, catalog compatibility, and commit/recovery surfaces as planned.
- [x] Gate catalog/table metadata integration surfaces through
      `CatalogMetadataIntegrationGateReport` before enabling runtime metadata access.
- [x] Support one in-memory local manifest-backed table metadata read smoke through
      `LocalTableMetadataReadSmokeReport`.
- [x] Support one source-reviewed local Iceberg table metadata JSON read smoke with current,
      explicit snapshot-id, and as-of timestamp snapshot selection while leaving catalog,
      object-store, manifest, data-file, delete-file, write/commit, production, and performance
      paths blocked.
- [x] Support one explicitly requested, feature-gated local Avro Iceberg manifest-list summary read
      with manifest-summary pruning evidence, manifest-level split counts, and delete/unknown
      manifest blockers while leaving manifest-file and data-file runtime blocked.
- [x] Support one explicitly requested, feature-gated local Avro Iceberg manifest-file split-plan
      read with data-file split counts, bytes, records, and deleted/delete/unknown entry blockers
      while leaving data-file scan runtime blocked.
- [x] Support Iceberg metadata-level schema/partition evolution admission fields with field-ID,
      partition field-ID, partition spec-ID, manifest partition-spec, and fail-closed projection
      blockers while leaving data projection and partition-filter execution blocked.
- [x] Support Iceberg delete/tombstone/deletion-vector admission classifiers for position deletes,
      equality deletes, deletion-vector-shaped entries, delete manifests, and deleted data-file
      entries while leaving delete application blocked.
- [x] Support one source-reviewed local Delta transaction log metadata read smoke with protocol,
      table metadata, action, table-feature, deletion-vector, CDC, and no-fallback evidence while
      leaving checkpoint replay, data-file scans, deletion-vector application, writes/commits, and
      Delta runtime blocked.
- [x] Support one source-reviewed local Hudi timeline/metadata summary smoke with
      requested/inflight/completed instant parsing, action-family classification, optional metadata
      table summary JSON evidence, and no-fallback blockers while leaving base-file scans, log
      merge, metadata-table storage reads, table services, writes/commits, and Hudi runtime
      blocked.
- [x] Support one in-memory local manifest-backed delete/tombstone read smoke through
      `LocalDeleteTombstoneReadSmokeReport`.
- [x] Support one in-memory local append-only CDC overlay smoke through
      `LocalAppendOnlyCdcOverlaySmokeReport`.
- [x] Support one local-manifest append commit rehearsal with staged committed-manifest write,
      sidecar commit record, idempotency, and optional rollback cleanup evidence.
- [x] Support one local-manifest commit recovery replay smoke that verifies the committed manifest
      digest, sidecar commit record, correctness digest, and optional idempotency key without
      catalog/object-store effects.
- [x] Expose delete/tombstone, CDC, compaction, and maintenance-write execution posture through
      `TableMaintenanceExecutionMatrixReport`.
Out of scope until promoted GAR slices complete:

- Broader catalog/table metadata reads are carried by later GAR slices after the completed
  `GAR-0020-A` admission gate, `GAR-0020-C` local metadata smoke, and scoped `PROD-READY-1C`
  Iceberg metadata JSON/manifest-list summary smoke. The Iceberg smoke reads one local metadata JSON
  file and, only when `--manifest-list` is supplied with `universal-format-io`, one local Avro
  manifest-list summary. It can also read one local Avro manifest file when `--manifest` is supplied
  with `universal-format-io`. Delta/Hudi now have local metadata-only smoke commands for one local
  Delta log JSON file and one local Hudi timeline directory plus optional local metadata summary
  JSON. These still do not read data files, object stores, catalogs, checkpoints, Hudi base/log
  files, metadata-table storage, or table services.
- Broad delete/tombstone runtime beyond the completed `GAR-0020-D` local fixture smoke, CDC
  execution beyond the completed `GAR-0020-E` append-only overlay smoke, broad compaction writes,
  broad table data I/O, object-store I/O, lakehouse/catalog commits, and table-format runtime
  surfaces remain unsupported. The local table append commit rehearsal and recovery replay operate
  only on a ShardLoom-owned local-manifest fixture artifact plus sidecar commit record; they are not
  Iceberg/Delta/Hudi, catalog, object-store, production commit, or exactly-once recovery support.
  `GAR-0028-A` now supplies the deterministic commit-semantics gate for lakehouse/catalog paths;
  later runtime promotion still requires workload fixtures, commit execution evidence, execution
  certificates, Native I/O certificates, materialization/decode evidence, and no-fallback evidence.

## Default Policy

- `catalog_io_performed=false`
- `table_metadata_io_performed=false`
- `data_io_performed=false`
- `write_io_performed=false`
- `external_table_format_dependency_added=false`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`

For the CG-9 metadata gate:

- `snapshot_manifest_metadata_read_allowed=false`
- `catalog_resolution_allowed=false`
- `table_metadata_read_allowed=false`
- `catalog_io_allowed=false`
- `object_store_io_allowed=false`
- `external_table_format_dependency_allowed=false`
- `credential_resolution_allowed=false`
- `metadata_cache_runtime_allowed=false`
- `metadata_integration_claim_allowed=false`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `support_status=unsupported`
- `claim_gate_status=not_claim_grade`
- `deterministic_unsupported_diagnostics_ready=true`

The aggregate report is evidence and routing context only. It does not certify that Iceberg, Delta,
Hudi-like, catalog, manifest, recovery, or table-maintenance runtime behavior exists.

## GAR-COMPAT-1D Table-Format Boundary Matrix

The universal compatibility scoreboard projects table-format status through
`shardloom.universal_compatibility.table_format_boundary_matrix.v1` so user-facing status,
Python typed accessors, and website/status pages can explain Iceberg, Delta, and Hudi boundaries
without treating local metadata smoke as table-format runtime.

The matrix order is:

```text
table_metadata_read
table_scan
snapshot_time_travel
partition_evolution
delete_tombstone
append
merge_update_delete
commit
rollback
catalog_interaction
object_store_coupling
```

Every matrix row keeps:

```text
catalog_io_allowed=false
object_store_io_allowed=false
table_metadata_read_allowed=false
table_data_read_allowed=false
write_io_allowed=false
commit_allowed=false
rollback_allowed=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_claim_grade
```

`table_metadata_read`, `partition_evolution`, and `delete_tombstone` are report-only because local
manifest metadata and delete/tombstone fixture smokes are related evidence. They are not
Iceberg/Delta/Hudi runtime support. Table scan, snapshot/time-travel, append, merge/update/delete,
commit, rollback, catalog interaction, and object-store-backed table runtime remain blocked until
separate table-format dependency, catalog, object-store, commit, correctness, execution-certificate,
Native I/O, materialization, and no-fallback evidence exists.

## Delete, CDC, And Maintenance Execution Matrix

`GAR-0020-B` adds `shardloom.table_maintenance_execution_matrix.v1` to
`table-intelligence-plan` under `table_maintenance_execution_matrix_*` fields. The matrix is a
side-effect-free readiness and blocker surface for table operation families that were previously too
broad to reason about as one item.

The matrix classifies:

1. `file_level_delete_compatibility`, `cdc_append_only_planning`,
   `cdc_metadata_only_planning`, and `compaction_planning` as report-only evidence backed by the
   existing delete/tombstone, CDC incremental, layout health, and compaction planning reports.
2. `table_metadata_write` and `table_maintenance_commit` as report-only fixture evidence backed by
   the local table append commit rehearsal smoke. They remain non-promotional: broad runtime
   execution, catalog commits, object-store commits, and table-format commit claims stay blocked.
3. `segment_tombstone_execution`, `row_level_delete_execution`, `position_delete_execution`,
   `equality_delete_execution`, `cdc_update_delete_tombstone_execution`,
   and `compaction_execution_write` as unsupported until their required fixtures, commit semantics,
   correctness evidence, execution certificates, Native I/O certificates, materialization/decode
   evidence, and no-fallback evidence exist.

The matrix reports:

- `support_status=report_only_with_unsupported_runtime_paths`
- `claim_gate_status=not_claim_grade`
- `operation_count=12`
- `report_only_operation_count=6`
- `unsupported_operation_count=6`
- `runtime_promotions_blocked=true`
- `deterministic_unsupported_diagnostics_ready=true`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `table_format_execution_claim_allowed=false`

It does not authorize broad delete/tombstone runtime, CDC execution, compaction writes, production
table metadata writes, table-format/catalog commits, object-store I/O, lakehouse/catalog runtime,
external engines, fallback execution, or production table-format claims.

## Catalog Metadata Integration Gate

`GAR-0020-A` adds deterministic admission diagnostics to
`CatalogMetadataIntegrationGateReport`. The gate is exposed directly through
`cg9-catalog-metadata-gate` and embedded in `table-intelligence-plan` under
`catalog_metadata_integration_gate_*` fields.

The gate classifies:

1. `table_intelligence_foundation` and `catalog_ref_skeleton` as existing report-only evidence.
2. `snapshot_manifest_boundary`, `catalog_table_resolution`, `table_metadata_read`,
   `partition_metadata_read`, `delete_tombstone_metadata_read`, and `cdc_metadata_read` as
   unsupported until fixture evidence, snapshot/catalog refs, table metadata schema, credential
   policy, execution certificate, Native I/O certificate, and no-fallback policy evidence exist.
3. `table_format_dependency_admission` as unsupported until dependency/license approval,
   feature-gating, version records, policy admission, and no-fallback evidence exist.
4. `commit_recovery_metadata_binding` and `metadata_cache_invalidation` as unsupported until commit
   protocol, recovery, cache-key, invalidation, credential, execution, and Native I/O evidence
   exists.

The gate is side-effect-free:

- `support_status=unsupported`
- `runtime_promotions_blocked=true`
- `deterministic_unsupported_diagnostics_ready=true`
- `unsupported_diagnostic_count=9`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `claim_gate_status=not_claim_grade`

It does not authorize catalog resolution runtime, metadata reads, data reads, external table-format
dependencies, credentials, object-store I/O, table/catalog writes, lakehouse runtime, external
engines, fallback execution, or production table/catalog claims.

## Local Table Metadata Read Smoke

`GAR-0020-C` adds `shardloom.local_table_metadata_read_smoke.v1` as the first scoped runtime-backed
table metadata surface. It constructs an in-memory local fixture from `CatalogRef`,
`DatasetManifest`, `SnapshotRef`, `SchemaDefinition`, `PartitionSpec`, and native Vortex
file/segment metadata, then emits a typed metadata summary and stable digest.

The smoke reports:

- `support_status=runtime_supported`
- `claim_gate_status=scoped_local_metadata_smoke_only`
- `local_manifest_metadata_read_performed=true`
- `table_metadata_summary_emitted=true`
- `table_metadata_read_performed=true`
- `metadata_summary_digest=fnv1a64:*`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `deterministic_unsupported_diagnostics_ready=true`

The smoke remains deliberately narrow. It does not read filesystem manifest files, read data files,
open object stores, resolve credentials, invoke external table-format dependencies, write table
metadata, execute CDC/delete/tombstone behavior, certify lakehouse/object-store/Foundry runtime, or
support production SQL/DataFrame/table/catalog claims. The CG-9 metadata gate therefore continues to
report `table_metadata_read_allowed=false` for broad runtime promotion while exposing the scoped
smoke command and report refs.

## Iceberg Metadata JSON And Manifest-List Summary Smoke

`PROD-READY-1C` adds `shardloom.iceberg_metadata_read_smoke.v1` as the first source-reviewed
external table-profile implementation. It reads one local Apache Iceberg table metadata JSON file,
selects the current snapshot by default, supports explicit `--snapshot-id`, and supports
`--as-of-timestamp-ms` selection by choosing the latest snapshot at or before the requested
timestamp. When `--manifest-list local.avro` is supplied and `shardloom-cli` is built with
`--features universal-format-io`, it also reads one explicit local Avro Iceberg manifest list for
manifest-summary pruning and manifest-level split-count evidence only. When `--manifest local.avro`
is supplied with the same feature, it reads one explicit local Avro Iceberg manifest file for
data-file split-plan evidence only. The smoke also computes metadata-level schema/partition
evolution admission and delete/deletion-vector admission; these are planning and blocker surfaces,
not data projection, partition-filter, delete-application, or scan execution.

The smoke reports:

- `support_status=runtime_supported` when the metadata JSON uses admitted metadata-only semantics.
- `claim_gate_status=scoped_iceberg_metadata_json_smoke_only` for metadata-only mode.
- `claim_gate_status=scoped_iceberg_metadata_manifest_list_summary_smoke` for the feature-enabled
  manifest-list summary mode.
- `claim_gate_status=scoped_iceberg_manifest_file_split_plan_smoke` for the feature-enabled
  manifest-file split-plan mode.
- `source_protocol=apache_iceberg_table_metadata`.
- `local_metadata_json_read_performed=true`.
- `table_metadata_read_performed=true`.
- `snapshot_selection_performed=true`.
- `time_travel_selection_performed=true|false` depending on selector.
- `manifest_list_ref_count`.
- `manifest_list_requested=true|false`.
- `manifest_list_reader_feature_enabled=true|false`.
- `manifest_list_read_performed=true|false`.
- `manifest_list_entry_count`, `manifest_list_data_manifest_count`,
  `manifest_list_delete_manifest_count`, and `manifest_list_unknown_content_manifest_count`.
- `manifest_summary_pruning_performed=true|false`.
- `planned_manifest_split_count` and `planned_data_file_count`.
- `manifest_file_requested=true|false`.
- `manifest_file_reader_feature_enabled=true|false`.
- `manifest_file_read_performed=true|false`.
- `manifest_file_entry_count`, `manifest_file_added_data_file_count`,
  `manifest_file_existing_data_file_count`, and `manifest_file_deleted_data_file_count`.
- `schema_evolution_present`, `schema_id_order`, schema add/drop/rename/type/requiredness counts,
  and `schema_evolution_admission_status`.
- `partition_evolution_present`, `partition_spec_id_order`, partition add/remove/rename/source/
  transform counts, `manifest_partition_spec_id_order`, and
  `partition_evolution_admission_status`.
- `manifest_file_position_delete_file_entry_count`,
  `manifest_file_equality_delete_file_entry_count`, and
  `manifest_file_deletion_vector_entry_count`.
- `delete_tombstone_deletion_vector_admission_status` and `delete_admission_status`.
- `data_file_split_planning_performed=true|false`.
- `planned_data_file_split_count` and `planned_data_file_split_bytes`.
- `metadata_summary_digest=fnv1a64:*`.
- `fallback_attempted=false`.
- `fallback_execution_allowed=false`.
- `external_engine_invoked=false`.

It also emits deterministic blockers for external catalog resolution, remote object-store metadata
reads, manifest-list reads when the feature is unavailable or absent, manifest-file reads when the
feature is unavailable or absent, data-file scans, delete-file semantics, table write/commit paths,
broad Iceberg runtime, Delta/Hudi runtime, and production lakehouse claims. If the selected snapshot
advertises delete-file summary counts, the command returns an
unsupported envelope with `unsupported_feature_order=delete_files_present` instead of silently
ignoring deletes. If a parsed manifest list contains delete manifests, delete-file counts, or
unknown manifest content, the command returns an unsupported envelope instead of planning those
entries. If a parsed manifest file contains deleted data-file entries, delete-file entries, unknown
content, or unknown entry statuses, the command returns an unsupported envelope instead of silently
planning those entries. It distinguishes position-delete files, equality-delete files, and
deletion-vector-shaped entries. Unsafe schema projection requirements, missing/duplicate field IDs,
partition field/spec integrity issues, unknown partition transforms, and unknown manifest
partition-spec IDs also return deterministic unsupported envelopes.

This smoke does not certify Iceberg table scans, data-file split execution, schema projection
execution, partition-filter execution, delete-file execution, Puffin/deletion-vector reads,
object-store tables, external catalogs, writes/commits, production lakehouse support, or
performance.

## Delta And Hudi Metadata Smokes

`PROD-READY-1C` adds `shardloom.delta_log_metadata_read_smoke.v1` and
`shardloom.hudi_timeline_metadata_read_smoke.v1` as scoped source-reviewed metadata readers. They
are table-protocol admission and diagnostic surfaces only, not Delta/Hudi runtime paths.

The Delta smoke reads one local Delta transaction log JSON file. It reports:

- `claim_gate_status=scoped_delta_transaction_log_metadata_smoke_only`.
- `source_protocol=delta_transaction_log_protocol`.
- `local_delta_log_json_read_performed=true`.
- `delta_log_action_parse_performed=true`.
- `min_reader_version` and `min_writer_version`.
- `reader_feature_order` and `writer_feature_order`.
- `metadata_action_count`, `table_id`, `table_name`, `schema_string_present`,
  `partition_column_order`, and `configuration_key_order`.
- `add_action_count`, `remove_action_count`, `txn_action_count`, `commit_info_action_count`,
  `cdc_action_count`, `unknown_action_count`, `add_stats_action_count`, and
  `deletion_vector_action_count`.
- `checkpoint_read_performed=false`.
- `data_file_read_performed=false`.
- `write_io_performed=false`.
- `fallback_attempted=false`.
- `fallback_execution_allowed=false`.
- `external_engine_invoked=false`.

It returns an unsupported envelope for missing protocol/metadata actions, unsupported Delta reader
or writer versions, reader/writer table features, remove actions, deletion vectors, CDC actions,
or unknown actions. It also emits deterministic blockers for `delta_checkpoint_read`,
`delta_data_file_scan`, `delta_delete_vector_application`, `delta_write_commit`,
`broad_delta_runtime`, and `delta_production_lakehouse_claim`.

The Hudi smoke reads one local Hudi timeline directory and optionally one local metadata-table
summary JSON fixture. It reports:

- `claim_gate_status=scoped_hudi_timeline_metadata_smoke_only`.
- `source_protocol=apache_hudi_timeline_and_metadata_table`.
- `local_timeline_directory_read_performed=true`.
- `timeline_filename_parse_performed=true`.
- `metadata_table_summary_json_read_performed=true|false`.
- `timeline_entry_count`, requested/inflight/completed instant counts, and
  `pending_instant_count`.
- Hudi action-family counts for commit, delta commit, replace commit, clean, compaction,
  log-compaction, clustering, indexing, rollback, savepoint, restore, unknown actions, and unknown
  states.
- Optional `metadata_table_enabled`, `metadata_partition_order`, files/column-stats/record-index
  partition flags, and summary counts.
- `metadata_table_storage_read_performed=false`.
- `base_file_read_performed=false`.
- `log_file_read_performed=false`.
- `table_service_execution_performed=false`.
- `write_io_performed=false`.
- `fallback_attempted=false`.
- `fallback_execution_allowed=false`.
- `external_engine_invoked=false`.

It returns an unsupported envelope for pending instants, delta commits/log-merge requirements,
replace commits, table-service actions, rollback/savepoint/restore semantics, unknown actions or
states, and unknown metadata-table partitions. It also emits deterministic blockers for
`hudi_base_file_scan`, `hudi_log_file_merge`, `hudi_metadata_table_storage_read`,
`hudi_table_service_execution`, `hudi_write_commit`, `broad_hudi_runtime`, and
`hudi_production_lakehouse_claim`.

## Local Delete/Tombstone Read Smoke

`GAR-0020-D` adds `shardloom.local_delete_tombstone_read_smoke.v1` as the first scoped
delete/tombstone read-execution smoke. It constructs an in-memory local manifest fixture with native
Vortex file/segment metadata, applies a ShardLoom-native admission rule for one file-level delete
and one segment tombstone, and emits the effective row ids plus a stable correctness digest.

The smoke reports:

- `support_status=fixture_smoke_only`
- `claim_gate_status=scoped_local_delete_tombstone_smoke_only`
- `admitted_delete_model_order=file_level_delete,segment_level_tombstone`
- `base_row_count=6`
- `file_deleted_row_count=2`
- `segment_tombstoned_row_count=1`
- `effective_row_count=3`
- `effective_row_ids=1001,1002,1003`
- `correctness_digest=fnv1a64:*`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `deterministic_unsupported_diagnostics_ready=true`

The unsupported model diagnostics remain deterministic for `row_level_delete`, `position_delete`,
`equality_delete`, `external_table_metadata`, `cdc_update_delete_tombstone`,
`object_store_delete_manifest`, and `table_format_delete_runtime`.

The smoke remains deliberately narrow. It does not read Vortex files, read object stores, write table
metadata, execute row/position/equality deletes, execute CDC update/delete/tombstone paths, invoke
external table-format dependencies, certify table-format runtime, or create production
table/catalog/lakehouse/performance claims. The `TableMaintenanceExecutionMatrixReport` therefore
continues to block broad runtime promotion while exposing `local_delete_tombstone_smoke_present=true`
and the `gar0020d.local_delete_tombstone_read_smoke` evidence ref.

## Local Append-Only CDC Overlay Smoke

`GAR-0020-E` adds `shardloom.local_append_only_cdc_overlay_smoke.v1` as the first scoped
append-only CDC read/overlay smoke. It constructs an in-memory local fixture with a declared base
snapshot, one append delta snapshot, and a CDC incremental plan. The smoke applies a
ShardLoom-native overlay rule that appends delta rows after base rows and emits the effective row ids
plus a stable correctness digest.

The smoke reports:

- `support_status=fixture_smoke_only`
- `claim_gate_status=scoped_append_only_cdc_overlay_smoke_only`
- `incremental_status=execute_changed_segments_only`
- `overlay_rule=base_snapshot_then_append_delta`
- `base_row_count=3`
- `append_row_count=2`
- `effective_row_count=5`
- `changed_segment_count=1`
- `insert_count=2`
- `update_count=0`
- `delete_count=0`
- `tombstone_count=0`
- `effective_row_ids=1001,1002,1003,4001,4002`
- `correctness_digest=fnv1a64:*`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `manifest_write_performed=false`
- `transaction_execution_performed=false`
- `deterministic_unsupported_diagnostics_ready=true`

The unsupported path diagnostics remain deterministic for `cdc_update`, `cdc_delete`,
`cdc_tombstone`, `manifest_serialization`, `manifest_write`, `transaction_execution`,
`object_store_commit`, `table_catalog_commit`, and `table_format_cdc_runtime`.

The smoke remains deliberately narrow. It does not read Vortex files, read object stores, write
manifests or table metadata, execute transactions or commits, execute update/delete/tombstone CDC
paths, invoke external table-format dependencies, certify table-format CDC runtime, or create
production incremental/lakehouse/performance claims. The `TableMaintenanceExecutionMatrixReport`
therefore continues to block broad runtime promotion while exposing
`local_append_only_cdc_overlay_smoke_present=true` and the
`gar0020e.local_append_only_cdc_overlay_smoke` evidence ref.

## Local Table Append Commit Rehearsal Smoke

`GAR-RUNTIME-IMPL-4O` adds `shardloom.local_table_append_commit_rehearsal_smoke.v1` as the first
fixture-scoped table metadata write and append commit rehearsal. It uses a local-manifest profile,
declares a base snapshot and append snapshot, writes a staged committed manifest JSON to a local
target path, writes a sidecar table commit record, records idempotency evidence, and can immediately
roll back the manifest and sidecar for cleanup proof.

The smoke reports:

- `support_status=fixture_smoke_only`
- `claim_gate_status=scoped_local_table_append_commit_rehearsal_only`
- `provider_profile=local-manifest`
- `table_format=shardloom_local_manifest`
- `base_snapshot_id=gar-runtime-4o-base-snapshot-0001`
- `append_snapshot_id=gar-runtime-4o-append-snapshot-0002`
- `committed_snapshot_id=gar-runtime-4o-committed-snapshot-0002`
- `base_row_count=3`
- `append_row_count=2`
- `effective_row_count=5`
- `manifest_file_count=2`
- `manifest_segment_count=2`
- `commit_protocol=local_manifest_sidecar_commit_record`
- `table_commit_rehearsal_status=rehearsed_local_manifest_commit` or
  `rehearsed_then_rolled_back`
- `rollback_status=not_requested` or `performed_local_manifest_cleanup`
- `idempotency_status=caller_supplied` or `derived_from_manifest_digest`
- `manifest_payload_digest=fnv64:*`
- `committed_manifest_digest=fnv64:*`
- `commit_record_digest=fnv64:*`
- `correctness_digest=fnv64:*`
- `catalog_io_performed=false`
- `object_store_io=false`
- `table_catalog_commit_performed=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`

Remote targets such as `s3://`, `gs://`, `abfs://`, and `abfss://` remain blocked before any write,
credential lookup, network probe, provider probe, or fallback execution. The smoke remains
deliberately narrow. It does not implement Iceberg, Delta, Hudi, catalog resolution, object-store
table commits, merge/update/delete, transaction execution, production rollback/recovery, distributed
runtime, performance claims, or lakehouse production support. The `TableMaintenanceExecutionMatrix`
therefore marks `table_metadata_write` and `table_maintenance_commit` as report-only fixture
evidence while broad runtime promotion remains blocked.

## CDC, Manifest, And Transaction Gate

`GAR-0004-A` adds `shardloom.cdc_manifest_transaction_gate.v1` to the table-intelligence and
`incremental-plan cdc` CLI surfaces.

The gate classifies:

1. `cdc_read_intent` as report-only evidence backed by declared change sets and CDC summaries.
2. `cdc_write_intent` as unsupported until write intent, staged manifest, commit protocol, and
   recovery evidence exist.
3. `manifest_serialization` as unsupported until generalized manifest schema, artifact write
   policy, and Native I/O evidence exist.
4. `manifest_metadata_read` as unsupported until snapshot refs, manifest/catalog locations,
   object-store provider policy, and Native I/O evidence exist.
5. `object_store_commit`, `table_catalog_commit`, and `transaction_execution` as unsupported until
   commit protocol, transaction protocol, recovery, object-store provider, and no-fallback evidence
   are attached.

The gate is side-effect-free:

- `runtime_promotions_blocked=true`
- `deterministic_unsupported_diagnostics_ready=true`
- `fallback_attempted=false`
- `fallback_execution_allowed=false`
- `external_engine_invoked=false`
- `claim_gate_status=not_claim_grade`

It does not authorize metadata reads, data reads, manifest serialization execution, object-store
commits, table/catalog commits, transaction writes, CDC execution, credentials, external engines,
fallback execution, or table/lakehouse production claims.

## Surface Order

1. `schema_evolution`
2. `partition_evolution`
3. `delete_tombstone`
4. `table_compatibility`
5. `cdc_incremental`
6. `layout_health`
7. `compaction`
8. `snapshot_manifest`
9. `catalog_compatibility`
10. `commit_recovery`

## Acceptance Boundaries

- [x] Every CG-9 surface is represented in one deterministic report.
- [x] Report-only surfaces point to their existing command surfaces where available.
- [x] Planned surfaces remain visible without implying runtime support.
- [x] The CLI emits machine-readable JSON fields for counts, surface order, compatibility profiles,
      IO flags, dependency flags, and no-fallback status.
- [x] Contract tests assert the aggregate report is side-effect-free.
- [x] Planned catalog/table metadata integration must update this report before enabling runtime
      behavior.
- [x] Table-format dependency admission is represented by `GAR-0020-A`; runtime dependency approval
      remains gated by dependency/license policy and must not introduce external execution fallback.

# Table Intelligence Layer

This document defines the CG-9 aggregate surface that keeps table, schema, CDC, layout, and
compaction evidence visible before ShardLoom performs catalog reads, table metadata reads, data
reads, maintenance writes, or table-format runtime behavior.

The first implementation is `TableIntelligenceReport`, exposed through:

```powershell
shardloom table-intelligence-plan --format json
```

The catalog/table metadata promotion gate is `CatalogMetadataIntegrationGateReport`, exposed
through:

```powershell
shardloom cg9-catalog-metadata-gate --format json
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
Out of scope until promoted GAR slices complete:

- Local manifest-backed table metadata read smoke and broader catalog/table metadata reads are
  carried by `GAR-0020-C` after the completed `GAR-0020-A` admission gate.
- Delete/tombstone, CDC, compaction, table-maintenance writes, and table-format runtime surfaces are
  carried by `GAR-0020-B` and `GAR-0028-A`.

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

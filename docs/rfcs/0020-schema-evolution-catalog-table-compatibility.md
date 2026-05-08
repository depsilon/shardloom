# RFC 0020: Schema Evolution, Catalog Integration, and Table Compatibility

## Status

Draft

## Summary

This RFC defines ShardLoom's schema evolution, catalog integration, and table compatibility design.

ShardLoom is not a new lakehouse table format, but real adoption requires compatibility with evolving schemas, cataloged datasets, and table-format ecosystems such as Iceberg-compatible and Delta-compatible workflows.

## Context

ShardLoom must read and write datasets that evolve over time while preserving Vortex-native fidelity and explicit diagnostics for compatibility boundaries.

## Goals

- Define schema evolution compatibility levels.
- Define field identity and rename behavior expectations.
- Define catalog-facing schema contracts.
- Define read-time and write-time validation policy.
- Define metadata-loss and coercion diagnostics.
- Preserve Vortex-native output as highest fidelity target.

## Non-goals

- Do not implement a full catalog service.
- Do not redefine Iceberg or Delta table specs.
- Do not add fallback execution engines.

## Core principle

Schema evolution should be explicit, deterministic, and compatibility-scored. Unsupported schema transitions must fail with stable diagnostics rather than silently coercing to lossy behavior.

## Detailed design

### Compatibility levels

- NativeExact: full Vortex-native type/metadata fidelity.
- NativeCompatible: semantically equivalent with explicit metadata mapping.
- CompatibilityLossless: translatable to compatibility outputs without semantic loss.
- CompatibilityLossy: allowed only with explicit opt-in and metadata-loss report.
- Unsupported: deterministic failure.

### Evolution operations

Potential operations:

- Add nullable field.
- Add field with default.
- Drop field.
- Rename field (requires stable field id mapping).
- Widen numeric type.
- Tighten type/nullability (generally unsupported without explicit policy).
- Struct/list nested evolution.

### Schema evolution compatibility report

The initial CG-9 schema-evolution evidence surface is `SchemaEvolutionCompatibilityReport`.
It compares typed schema definitions and emits deterministic compatibility evidence before
catalog access, table metadata IO, reads, writes, or object-store behavior are introduced.

Required fields:

- `compatibility`: the underlying `SchemaCompatibilityReport`.
- `policy`: the applied `SchemaEvolutionPolicy`.
- `safe_change_count`.
- `unsafe_change_count`.
- `field_id_required_count`.
- `missing_field_id_count`.
- `requires_projection`.
- `requires_cast`.
- `requires_default_values`.
- `metadata_loss_reported`.
- `read_supported`.
- `write_supported`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `fallback_execution_allowed=false`.

The evaluator should detect:

- add nullable field.
- add field with default.
- drop field requiring projection.
- rename field with stable field identity.
- possible rename without stable field identity.
- safe widening.
- unsafe narrowing or unknown type change.
- nullability loosening or tightening.
- field identity changes.
- metadata changes and metadata loss.

Safe rename evidence requires stable field IDs. A possible rename without stable field
identity is rejected even when the field shape looks compatible, because accepting it would
make unsafe data movement indistinguishable from a real rename.

### Partition evolution compatibility report

`PartitionEvolutionCompatibilityReport` compares typed partition specs and emits deterministic
compatibility evidence before catalog access, table metadata IO, reads, repartitioning, writes,
or object-store behavior are introduced.

Required fields:

- `from_spec`.
- `to_spec`.
- `level`.
- `changes`.
- `diagnostics`.
- `preserved_field_count`.
- `added_field_count`.
- `dropped_field_count`.
- `transform_change_count`.
- `reorder_count`.
- `unsafe_change_count`.
- `requires_partition_router`.
- `requires_metadata_rewrite`.
- `requires_repartition`.
- `read_supported`.
- `write_supported`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `fallback_execution_allowed=false`.

The evaluator should detect:

- unchanged partition specs.
- added partition fields.
- dropped partition fields.
- transform changes.
- partition field reordering.
- unknown or unsupported transforms.

Known add/drop/transform/reorder changes may be report-compatible only when they explicitly
surface partition routing, metadata rewrite, or repartition requirements. Unknown transforms
are rejected until a native rule can preserve semantics.

### Delete and tombstone compatibility report

`DeleteTombstoneCompatibilityReport` compares declared delete/tombstone models and emits
deterministic compatibility evidence before catalog access, table metadata IO, delete-file
application, tombstone filtering, reads, writes, or object-store behavior are introduced.

Required fields:

- `source_model`.
- `target_model`.
- `level`.
- `diagnostics`.
- `delete_semantics_preserved`.
- `tombstone_semantics_preserved`.
- `requires_explicit_delete_handling`.
- `requires_file_delete_filter`.
- `requires_tombstone_filter`.
- `requires_row_identity`.
- `requires_position_identity`.
- `requires_equality_predicate`.
- `requires_external_table_metadata`.
- `metadata_loss_reported`.
- `unsupported_model_count`.
- `unsafe_change_count`.
- `read_supported`.
- `write_supported`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `fallback_execution_allowed=false`.

Initial support is intentionally narrow:

- `none` is exact.
- `file_level_delete` is initially compatible when the target preserves or adds explicit
  file-level delete semantics.
- Dropping declared file-level delete semantics is rejected until a native metadata-loss
  and rewrite rule exists.
- `segment_level_tombstone` requires a native tombstone-filter rule.
- `row_level_delete` requires a native row-identity rule.
- `position_delete` requires a native position-identity rule.
- `equality_delete` requires a native equality-predicate rule.
- `external_table_metadata` requires explicit external table metadata routing.
- `unknown` is rejected.

The evaluator must not treat external delete files, tombstones, row-level deletes, position
deletes, or equality deletes as fallback execution. They are compatibility signals that must
either route into native ShardLoom handling or fail with deterministic diagnostics.

### Table compatibility aggregation report

`TableCompatibilityReport` aggregates the schema-evolution, partition-evolution, and
delete/tombstone reports for a table path before any catalog adapter or table metadata reader
is allowed to run. The aggregate report is evidence routing, not catalog access.

Required fields:

- `plan`.
- `schema_report`, when an older schema compatibility report is attached.
- `schema_evolution_report`.
- `partition_evolution_report`.
- `delete_tombstone_report`.
- `diagnostics`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `fallback_execution_allowed=false`.

The aggregate report must compute:

- evidence report count.
- aggregate read support.
- aggregate write support.
- aggregate side-effect-free status.
- nested diagnostic propagation.
- nested error blocking.

Aggregation does not make unsupported nested behavior supported. If schema, partition, or
delete/tombstone evidence rejects a path, the table compatibility report must reject the
aggregate path with deterministic diagnostics and no fallback execution.

### CDC incremental compatibility dependency

CDC incremental planning depends on the same compatibility evidence instead of bypassing it.
Append-only or metadata-only changes may be routed from `ChangeSet` evidence into
`CdcIncrementalPlanningReport` without catalog access or data reads, but schema, partition,
delete, tombstone, update, and unknown changes require native compatibility evidence before
future incremental execution can be certified.

Required dependency rules:

- CDC update and delete summaries require a native row-identity rule.
- CDC delete and tombstone summaries require native delete/tombstone handling.
- CDC schema-change summaries require attached schema compatibility evidence.
- CDC partition-change summaries require attached partition compatibility evidence.
- Unknown CDC events or segment changes must be rejected.
- CDC planning must not read catalogs, table metadata, data files, delete files, or object
  stores.
- CDC planning must not run fallback execution.

This dependency keeps incremental lakehouse intelligence conservative: compatibility reports
decide whether a table change is safe, and CDC planning only routes safe declared changes into
future native incremental execution.

### Layout health compatibility dependency

Layout-health planning depends on declared manifest/file/segment evidence. It may flag small
files, small segments, missing statistics, missing byte ranges, mixed formats, mixed encodings,
mixed layouts, and non-native data files before any table maintenance work runs.

Required dependency rules:

- Layout health may recommend compaction but must not execute compaction.
- Layout health may report non-native data files but must not treat adapter conversion as
  fallback execution.
- Layout health may require statistics refresh or byte-range indexes but must not read files
  to create them.
- Layout health must surface mixed-format/layout/encoding evidence before catalog-table
  metadata integration or maintenance planning can treat the path as safe.
- Layout health must not read catalogs, table metadata, data files, delete files, or object
  stores.
- Layout health must not run fallback execution.

This dependency keeps maintenance intelligence evidence-first: table compatibility reports
decide semantic safety, layout health decides physical-maintenance risk, and future compaction
planning decides whether a rewrite is allowed.

### Compaction planning compatibility dependency

Compaction planning depends on layout-health evidence and declared manifest metadata. It may
emit future maintenance recommendations for small files and small segments, but it must not
produce executable compaction tasks, write files, update manifests, apply delete/tombstone
semantics, or commit table changes.

Required dependency rules:

- Compaction planning must consume layout-health evidence before recommending any rewrite.
- Missing statistics or byte ranges must block recommendation emission behind metadata
  refresh/index requirements.
- Mixed formats, mixed encodings, mixed layouts, and non-native data files must block
  recommendations behind layout or adapter-fidelity review.
- Recommendation groups are planning evidence only; they are not write intents, commit
  intents, catalog updates, or object-store operations.
- Compaction planning must not read catalogs, table metadata, data files, delete files, or
  object stores.
- Compaction planning must not run fallback execution.

This dependency keeps compaction conservative until write-side payloads, manifest finalization,
commit protocol, delete/tombstone semantics, adapter fidelity, and object-store execution have
their own native evidence.

### Catalog integration contract

Catalog adapters should expose:

- Stable table identity.
- Schema version and field identity mapping.
- Partition/spec metadata when relevant.
- Snapshot/version reference.
- Capability flags for supported evolution operations.

### Validation and diagnostics

Validation should check:

- Read schema vs plan schema compatibility.
- Write schema vs sink/output contract.
- Coercion policy compliance.
- Metadata preservation/loss reporting.
- No-fallback policy adherence.

Diagnostics should include stable codes, affected field paths, attempted operation, and suggested next step.

## Failure behavior

Unsupported evolution, unsafe coercion, or missing catalog capability must fail explicitly with deterministic diagnostics and no fallback execution.

## Alternatives considered

- Silent coercion for convenience: rejected.
- Compatibility-format-first schema model: rejected.
- Ad hoc per-connector evolution logic without central contract: rejected.

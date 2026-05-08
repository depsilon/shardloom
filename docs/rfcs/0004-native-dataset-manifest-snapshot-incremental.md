# RFC 0004: Native Dataset, Manifest, Snapshot, and Incremental Change Model

## Status

Draft

## Summary

This RFC defines ShardLoom's native dataset, manifest, snapshot, and incremental change model.

ShardLoom's long-term goal is to handle massive object-store and lakehouse workloads without Spark fallback. To do that, ShardLoom needs a native way to reason about datasets as collections of encoded Vortex segments, immutable snapshots, manifests, changed segments, and idempotent commits.

This RFC does not define a new lakehouse table format. Instead, it defines ShardLoom's internal dataset planning model.

## Context

Spark remains difficult to displace for massive workloads because it handles large-scale scans, incremental writes, retries, task distribution, shuffle, and operational recovery.

ShardLoom cannot displace Spark only by being faster on single-node scans. It needs a native model for:

- Dataset snapshots.
- Segment manifests.
- Incremental reads.
- Changed-segment planning.
- Idempotent writes.
- Atomic or explicitly documented commits.
- Object-store-native execution.
- Native Vortex output.
- Compatibility exports.

ShardLoom should avoid table-format overreach early. Iceberg, Delta, and other table formats already solve snapshot and table semantics for many environments. ShardLoom should first define the internal model it needs to plan and execute efficiently.

## Goals

- Define ShardLoom's native dataset model.
- Define immutable dataset snapshots.
- Define manifests as collections of encoded segment descriptors.
- Define incremental change sets.
- Define changed-segment planning.
- Define write intents and commit records.
- Preserve Vortex as the native highest-fidelity persistence target.
- Support compatibility with external lakehouse table formats without becoming one too early.
- Enable object-store-native planning.
- Enable future distributed execution without Spark fallback.

## Non-goals

- Do not define a new public table format.
- Do not replace Iceberg or Delta table semantics.
- Do not implement distributed execution in this RFC.
- Do not implement Rust code in this RFC.
- Do not add Spark fallback.
- Do not add DataFusion fallback.
- Do not define SQL syntax.
- Do not define a transaction manager for all storage systems.
- Do not guarantee full ACID semantics in this RFC.

## Definitions

### NativeDataset

A logical dataset known to ShardLoom.

A NativeDataset may be backed by:

- Vortex files.
- Vortex segment manifests.
- Object-store paths.
- Future compatibility bridges to lakehouse table formats.

A NativeDataset is not necessarily a database table. It is ShardLoom's planning unit for encoded-columnar execution.

### DatasetManifest

A metadata document describing the physical files, segments, statistics, layout hints, schema information, and snapshot identity needed to plan execution.

A manifest should enable ShardLoom to answer these questions before reading data:

- What segments exist?
- What columns exist?
- What DTypes exist?
- What encodings exist?
- What statistics are available?
- What byte ranges may be read?
- What segments changed since a previous snapshot?
- What output commit produced this snapshot?

### DatasetSnapshot

An immutable view of a dataset at a point in logical time.

A snapshot references one or more manifests and has a stable identity.

Snapshots enable:

- Repeatable reads.
- Incremental planning.
- Change detection.
- Reproducible benchmarking.
- Safe task retries.

### SegmentDescriptor

A physical descriptor for an encoded Vortex segment.

A SegmentDescriptor should eventually include:

- Segment id.
- File reference.
- Byte ranges.
- Row count.
- Column set.
- DTypes.
- Encodings.
- Layout information.
- Statistics.
- Nullability information.
- Sort or clustering hints.
- Partition or shard hints.
- Snapshot membership.
- Write provenance.

### ChangeSet

A set of changes between two snapshots.

A ChangeSet may include:

- Added segments.
- Removed segments.
- Replaced segments.
- Metadata-only changes.
- Schema changes.
- Partition or layout changes.
- Tombstones or deletion markers, if supported later.

### WriteIntent

A planned write operation before commit.

A WriteIntent should describe:

- Target dataset.
- Target output format.
- Planned Vortex output files.
- Temporary paths.
- Expected statistics.
- Expected row counts.
- Commit preconditions.
- Idempotency key.

### CommitRecord

A durable record of a successful or attempted write commit.

A CommitRecord should describe:

- Commit id.
- Input snapshot ids.
- Output snapshot id.
- Added segments.
- Removed segments.
- Output files.
- Format targets.
- Statistics emitted.
- Commit time.
- Commit status.
- Failure or rollback notes if applicable.

## Decision

ShardLoom should introduce a native dataset planning model based on immutable snapshots and segment manifests.

The initial design should prioritize:

1. Vortex-native datasets.
2. Vortex-native outputs.
3. Incremental planning from manifests and snapshots.
4. Explicit compatibility export boundaries.
5. Object-store-friendly file and segment descriptors.

ShardLoom should not require full table-format ownership in early versions.

## Detailed design

### Dataset identity

A dataset should have a stable logical identity independent of any one physical file.

Dataset identity should include:

- Dataset name or URI.
- Storage root.
- Current snapshot reference.
- Schema reference.
- Optional catalog metadata.

### Snapshot identity

Snapshots should be immutable.

Snapshot identity may be based on:

- Monotonic version.
- Content hash.
- Commit id.
- External table-format snapshot id.
- Manifest hash.

The exact implementation can evolve, but snapshots should be stable enough for repeatable reads and incremental planning.

### Manifest structure

A manifest should be sufficient for planning without reading data files.

At minimum, a manifest should describe:

- Dataset id.
- Snapshot id.
- Schema.
- Segment descriptors.
- File descriptors.
- Statistics availability.
- Output provenance.
- Compatibility metadata.

A manifest may be physically stored as:

- Vortex metadata.
- JSON/TOML/YAML during early development.
- A future compact binary manifest.
- Sidecar metadata near Vortex files.

This RFC does not require a specific manifest serialization.

### Incremental planning

ShardLoom should support planning work between snapshots.

Given:

- Previous snapshot.
- Current snapshot.
- Query or transformation.
- Manifest metadata.

ShardLoom should identify:

- Unchanged segments.
- Added segments.
- Removed segments.
- Replaced segments.
- Segments requiring recomputation.
- Segments that can be reused.
- Segments that can be skipped.

This is necessary for Spark-displacement workloads because massive data volumes are often incrementally updated rather than fully recomputed.

### Changed-segment planning

A changed-segment planner should prefer:

1. Metadata-only reuse.
2. Segment-level reuse.
3. Changed-segment execution.
4. Partial recomputation.
5. Full recomputation only when necessary.

Changed-segment planning must be conservative. Incorrect reuse is a correctness bug.

### CDC incremental planning evidence

CDC and incremental workload planning should be represented as explicit evidence before
any CDC execution, table metadata reads, data reads, writes, catalog access, object-store
IO, or fallback execution is allowed.

The CG-9 CDC evidence surface is `CdcIncrementalPlanningReport`.

Required fields:

- `change_set`.
- `incremental_plan`.
- `cdc_events`.
- `status`.
- `diagnostics`.
- `insert_count`.
- `update_count`.
- `delete_count`.
- `tombstone_count`.
- `schema_change_count`.
- `partition_change_count`.
- `metadata_only_count`.
- `unknown_event_count`.
- `changed_segment_count`.
- `metadata_only_segment_count`.
- `unknown_segment_change_count`.
- `requires_snapshot_pair`.
- `requires_row_identity`.
- `requires_delete_handling`.
- `requires_schema_compatibility`.
- `requires_partition_compatibility`.
- `can_reuse_unchanged_segments`.
- `can_execute_changed_segments_only`.
- `requires_partial_recompute`.
- `requires_full_recompute`.
- `unsupported_change_count`.
- `data_read=false`.
- `write_io=false`.
- `catalog_io=false`.
- `object_store_io=false`.
- `fallback_execution_allowed=false`.

`CdcEventSummary` should identify at least:

- `insert`.
- `update`.
- `delete`.
- `tombstone`.
- `schema_change`.
- `partition_change`.
- `metadata_only`.
- `unknown`.

`CdcIncrementalPlanningStatus` should identify at least:

- `reuse_unchanged_segments`.
- `execute_changed_segments_only`.
- `partial_recompute_required`.
- `full_recompute_required`.
- `unsupported`.

The initial planner may certify append-only and metadata-only CDC summaries when a snapshot
pair and changed-segment evidence are present. Updates and deletes must remain unsupported
until native row identity and delete/tombstone handling exist. Schema-change and
partition-change CDC summaries must remain unsupported until attached schema and partition
compatibility evidence proves the transition is safe. Unknown CDC events or segment changes
must fail deterministically.

CDC incremental evidence does not apply CDC events, filter deleted rows, inspect external
delete files, read table metadata, transcode files, recompute data, or execute a query. It
only records whether a declared change summary is safe to route into future native
incremental execution.

### Native Vortex output

For ShardLoom-native writes, Vortex is the preferred output.

Native Vortex output should preserve:

- Logical DTypes.
- Physical encodings where applicable.
- Segment statistics.
- Layout hints.
- Nullability and validity information.
- Snapshot and manifest linkage.
- Write provenance.

### Compatibility with Iceberg and Delta

ShardLoom may later support Iceberg-compatible or Delta-compatible output.

Those outputs are compatibility exports.

They should not become:

- ShardLoom's native persistence model.
- Spark execution triggers.
- Required dependencies for native execution.

### Commit model

ShardLoom should eventually support idempotent commit behavior.

A write should be represented as:

1. Plan write.
2. Write temporary files.
3. Validate written files.
4. Emit manifest changes.
5. Commit snapshot pointer.
6. Record commit metadata.
7. Clean up temporary files when possible.

Atomicity depends on the storage system and catalog. If atomicity cannot be guaranteed, ShardLoom must document the limitation clearly.

## Failure behavior

Unsupported dataset behavior must fail explicitly.

Examples:

- Unsupported manifest version.
- Missing snapshot metadata.
- Unsupported schema evolution.
- Unsupported deletion model.
- Unsupported external table format.
- Ambiguous commit state.
- Missing segment statistics required for a requested optimization.

Failures must not trigger Spark, DataFusion, or another engine as fallback.

## Alternatives considered

### Use Iceberg or Delta as the native model immediately

Rejected for early ShardLoom.

Iceberg and Delta are valuable compatibility targets, but using them as the native model too early could tie ShardLoom's internal execution to external table semantics before its Vortex-native segment model is mature.

### Use Vortex files only, with no manifest model

Rejected.

This would limit incremental planning and make large-scale Spark-displacement workloads harder.

### Use Spark for incremental orchestration

Rejected.

This violates the standalone no-fallback policy.

## Risks

- Building too much table-format functionality too early.
- Creating a manifest model that duplicates Iceberg or Delta badly.
- Over-coupling to unstable upstream Vortex internals.
- Incorrect changed-segment reuse.
- Ambiguous object-store commit behavior.
- Insufficient snapshot metadata for repeatable reads.

## Acceptance criteria

This RFC is accepted when the project agrees that:

- ShardLoom needs a native dataset planning model.
- Snapshots are immutable planning units.
- Manifests describe encoded segments and statistics.
- Incremental planning is based on changed segments.
- Vortex is the preferred native output.
- External table formats are compatibility targets, not native execution dependencies.
- Unsupported dataset behavior fails explicitly.
- Spark and DataFusion fallback remain prohibited.

## Verification plan

Future implementation PRs should verify:

- Dataset manifests can be parsed or modeled.
- Snapshot identity is stable.
- Segment descriptors are inspectable.
- Changed segments can be identified.
- Unsupported manifest features fail clearly.
- Vortex output can be connected to a manifest.
- No Spark or DataFusion dependency is introduced.

## Open questions

- What should the first manifest serialization be?
- Should manifests be embedded in Vortex, stored as sidecars, or both?
- How should snapshot ids be generated?
- How much external table-format metadata should ShardLoom understand initially?
- What deletion model should be supported first?
- What commit protocol should be used for object-store writes?

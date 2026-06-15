# Table Protocol Source Review

## Purpose

This file is the source-checked intake for `PROD-READY-1C`. It records what ShardLoom must prove
before any external lakehouse/table runtime claim. It does not authorize Iceberg, Delta, Hudi,
external catalog, object-store table, merge/update/delete, production lakehouse, or performance
support.

Source check date: 2026-06-15.

## Source Inventory

| Surface | Primary source | ShardLoom intake |
| --- | --- | --- |
| Apache Iceberg table spec | `https://iceberg.apache.org/spec/` | Iceberg runtime requires table metadata, snapshots, manifest lists, manifests, data/delete files, partition specs, schemas, sequence numbers, and manifest-summary pruning. ShardLoom should map this metadata-first into split planning before data reads. |
| Apache Iceberg REST catalog | `https://iceberg.apache.org/rest-catalog-spec/` and `https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml` | REST catalog is the first external catalog candidate because it is a language/engine-neutral OpenAPI surface for table metadata, namespace/table operations, snapshot commits, and credential vending. Credential vending remains denied until object-store credential policy is closed. |
| Delta transaction protocol | `https://github.com/delta-io/delta/blob/master/PROTOCOL.md` | Delta support must start from `_delta_log` protocol parsing, protocol/table-feature gates, checkpoint handling, snapshot isolation, optimistic concurrency, schema/partition serialization, deletion-vector semantics where present, and no Spark runtime dependency. |
| Apache Hudi timeline | `https://hudi.apache.org/docs/timeline/` | Hudi support must treat the timeline as the source of truth for table-state changes, action instants, requested/inflight/completed states, rollback, savepoint, compaction, clustering, and log-compaction semantics. |
| Apache Hudi metadata table | `https://hudi.apache.org/docs/metadata/` | Hudi metadata can reduce expensive object-store listings and expose column/file statistics. ShardLoom should model it as a metadata-first planning source, not as permission to list cloud paths or execute Hudi services. |
| Project Nessie REST API | `https://projectnessie.org/develop/rest/` and `https://projectnessie.org/develop/spec/` | Nessie is a catalog candidate with an OpenAPI REST contract and branch/tag/commit semantics. It remains a catalog/control-plane candidate until auth, branch consistency, commit evidence, and no-fallback certificates exist. |
| Apache Polaris catalog | `https://polaris.apache.org/` and `https://raw.githubusercontent.com/apache/polaris/main/spec/polaris-catalog-service.yaml` | Polaris implements Iceberg REST plus Polaris-native API surfaces. It is a candidate only through an explicit Iceberg REST/Polaris profile with OAuth/Bearer auth handling and no secret materialization in table properties. |
| Apache Gravitino REST API | `https://gravitino.apache.org/docs/next/api/rest/gravitino-rest-api/` | Gravitino exposes a REST API with catalog/schema/table resources and auth schemes. It remains a catalog candidate until a version-pinned profile, credential policy, and table-operation subset are chosen. |

## ShardLoom Mapping

- Metadata-first: external table support must plan from snapshots, manifests, logs, timelines, or
  catalog metadata before reading data files.
- Capillary work units: manifests, log segments, timeline instants, commit records, and data-file
  splits should be independent bounded units with request/byte/retry evidence.
- Dynamic admission: every protocol feature needs an admission row before runtime. Delete files,
  deletion vectors, merge/update/delete, schema evolution, partition evolution, time travel,
  credential vending, branch/tag writes, table services, and catalog commits must fail closed until
  their semantics are implemented and tested.
- PulseWeave coordination: apply only to bounded metadata fetch/parse/commit tasks after the
  single-node commit and recovery contract exists. It must not hide object-store, catalog, auth, or
  external-service effects.
- Vortex-native boundary: table scans lower into ShardLoom-native split manifests and Vortex-native
  input/output where admitted. Parquet/Delta/Iceberg/Hudi files are compatibility/table surfaces,
  not fallback engines.

## V1 Candidate Decision

The current v1-supported table runtime path remains `local_manifest_table_runtime_v1_candidate`.
The first source-reviewed external profile now has a scoped local Iceberg metadata JSON smoke through
`iceberg-metadata-read-smoke`. It reads one local Iceberg table metadata JSON file, selects the
current snapshot, an explicit snapshot id, or an as-of timestamp snapshot, and reports schema,
partition, sort-order, snapshot, manifest-list-reference, and no-fallback boundary evidence.
With `--manifest-list` and `universal-format-io`, the same command can read one explicitly supplied
local Avro manifest list, summarize manifest entries, report manifest-summary pruning and
manifest-level split counts, and block delete/unknown manifest content without fallback. With
`--manifest` and `universal-format-io`, it can also read one explicit local Avro manifest file,
parse manifest entries, report data-file split counts/bytes/records, and block deleted, delete-file,
or unknown entries by default without scanning data files. With explicit `--execute-data-file-scan`,
the same command can lower admitted local Parquet data-file splits into a scoped sequential
compatibility-source columnar scan with current-schema projection, row/batch/byte evidence,
execution/Native I/O certificate refs, and no-fallback fields. It now compares metadata schemas by Iceberg field IDs,
partition specs by partition field/spec IDs, manifest partition-spec IDs, and delete entries by
data/position-delete/equality-delete/deletion-vector-shaped content.

Delta and Hudi now have scoped metadata-only smokes, not runtime support.
`delta-log-metadata-read-smoke` reads one local Delta transaction log JSON file, summarizes
protocol/table metadata, add/remove/txn/commitInfo/CDC actions, table feature arrays, deletion
vector evidence, and no-fallback boundary fields. It fails closed for missing protocol/metadata,
unsupported protocol versions, table features, remove actions, CDC, deletion vectors, unknown
actions, checkpoint replay, data-file scans, writes/commits, and production lakehouse claims.
`hudi-timeline-metadata-read-smoke` reads one local Hudi timeline directory and optionally one local
metadata-table summary JSON fixture. It summarizes requested/inflight/completed instants, action
families, metadata-table partitions, and no-fallback boundary fields. It fails closed for pending
instants, delta commits/log-merge requirements, replace commits, table-service actions,
rollback/savepoint/restore semantics, unknown actions/states, metadata-table storage reads,
base/log-file scans, writes/commits, and production lakehouse claims.

These are scoped metadata, split-planning, and local Parquet scan admission surfaces only. External Iceberg data scans, object-store
tables, catalog runtime, writes/commits, partition-filter execution, delete-file execution,
Puffin/deletion-vector reads, Delta runtime, Hudi runtime, Nessie, Polaris, Gravitino, Glue-like,
and Hive-like profiles remain source-reviewed or planned candidates, not production-supported
runtime.

The next Iceberg implementation step should focus on proven write semantics and commit/recovery, or
on separately admitted partition-filter/delete-application semantics where evidence can close. An
approved no-credential REST-catalog fixture remains a candidate after credential/object-store and
effect policy are narrowed.

Glue-like and Hive-like catalog profiles are intentionally not selected for the first external
candidate. They need separate source/profile review before implementation because their credential,
metastore, partition-listing, compatibility, and deployment semantics differ from the OpenAPI-style
catalog profiles reviewed here.

## Required Before Runtime Promotion

- Version-pinned protocol profile and source refs.
- Table metadata parser with deterministic unsupported diagnostics.
- Snapshot/time-travel selection contract.
- Manifest/log/timeline split planner.
- Schema and partition projection/filter execution semantics for admitted scans.
- Delete/tombstone/deletion-vector execution and Puffin/vector application policy for admitted
  scans.
- Object-store credential, byte-range, retry, and bounded streaming evidence when remote files are
  involved.
- Commit/rollback/recovery contract before writes.
- TranslationReport coverage for preserved/lost metadata, statistics, layout, and materialization.
- Native I/O and execution certificates with `fallback_attempted=false` and
  `external_engine_invoked=false`.

## Claim Boundary

May claim: the protocol sources have been reviewed and mapped to ShardLoom admission gates; the
scoped local Iceberg metadata JSON smoke reads one local metadata file and selects snapshots without
fallback or external engines; and the feature-gated manifest-list summary smoke reads one explicit
local Avro manifest list for manifest-summary pruning/split-count evidence only; and the
feature-gated manifest-file smoke reads one explicit local Avro manifest file for data-file
split-plan evidence only; and the same smoke emits metadata-level schema/partition evolution and
delete/deletion-vector admission evidence with deterministic fail-closed blockers; and scoped local
Delta/Hudi metadata smokes read one local transaction log JSON file or one local Hudi timeline
directory plus optional local metadata-table summary JSON fixture with deterministic no-fallback
blockers.

May not claim: Iceberg data-file scan/runtime, Delta checkpoint replay/runtime, Delta deletion-vector
application, Hudi base-file scan, Hudi log merge, Hudi table-service execution, catalog runtime,
object-store table runtime, table scan, schema projection execution, partition-filter execution,
delete application, Puffin/deletion-vector reads, append/overwrite, merge/update/delete, rollback,
production lakehouse support, Spark replacement, performance, or external engine execution.

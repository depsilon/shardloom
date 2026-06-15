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
External Iceberg manifest-list reads, manifest parsing, data scans, object-store tables, catalog
runtime, writes/commits, delete-file execution, Delta, Hudi, Nessie, Polaris, Gravitino, Glue-like,
and Hive-like profiles remain source-reviewed or planned candidates, not production-supported
runtime.

The next Iceberg implementation step should extend the metadata JSON smoke into manifest-list reads,
manifest parsing, manifest-summary pruning, schema/partition evolution semantics, delete-file
admission, and ShardLoom-native split planning. An approved no-credential REST-catalog fixture
remains a candidate after credential/object-store and effect policy are narrowed.

Glue-like and Hive-like catalog profiles are intentionally not selected for the first external
candidate. They need separate source/profile review before implementation because their credential,
metastore, partition-listing, compatibility, and deployment semantics differ from the OpenAPI-style
catalog profiles reviewed here.

## Required Before Runtime Promotion

- Version-pinned protocol profile and source refs.
- Table metadata parser with deterministic unsupported diagnostics.
- Snapshot/time-travel selection contract.
- Manifest/log/timeline split planner.
- Schema and partition evolution semantics.
- Delete/tombstone/deletion-vector admission policy.
- Object-store credential, byte-range, retry, and bounded streaming evidence when remote files are
  involved.
- Commit/rollback/recovery contract before writes.
- TranslationReport coverage for preserved/lost metadata, statistics, layout, and materialization.
- Native I/O and execution certificates with `fallback_attempted=false` and
  `external_engine_invoked=false`.

## Claim Boundary

May claim: the protocol sources have been reviewed and mapped to ShardLoom admission gates, and the
scoped local Iceberg metadata JSON smoke reads one local metadata file and selects snapshots without
fallback or external engines.

May not claim: Iceberg manifest-list/manifest/data-file runtime, Delta/Hudi runtime, catalog
runtime, object-store table runtime, table scan, append/overwrite, merge/update/delete, rollback,
production lakehouse support, Spark replacement, performance, or external engine execution.

# Native local table readers

Status: local acceptance passed November 2; PR #1645 merged November 2 after all
58 hosted checks passed on `78c5e0a9e262739b3ab5b86d261239e9c6f9baf1`. Both
reported review findings were fixed and validated; automated re-review was still
running at merge, so review is not recorded as cleared. Hosted acceptance is
copied to the evidence directory below.

## Decision and ownership

This is the local read-only extension of active item 18 after item 23's adaptive-owner work,
under RFC 0020 and existing CG-9/CG-18/CG-20/CG-21 obligations. It creates no phase or gate.
The expert comparator is a table-format reader maintainer: snapshot visibility, field identity,
retention and resource ownership must be correct before useful-looking rows count as support.

Resolve table state once under the operation's memory/cancellation owner. Keep the selected
metadata, data and delete generations alive through the source and resulting native arrays.
Reuse `LocalColumnarSource`, the existing Parquet/Avro format providers, native batch admission
and native operators. Table metadata parsing and logical mapping live beside the source adapter;
All five public interfaces lower to that shared owner.

The Vortex-first check finds no admitted Vortex provider for Iceberg manifest visibility, Delta
transaction reconciliation or Hudi timelines. These are compatibility-input protocol decisions.
Existing Vortex arrays, native source/session owners and Vortex output remain authoritative after
the input boundary. No external query engine, integration crate, hidden preparation artifact or
whole-table Arrow conversion is introduced.

## Declaration and snapshot contract

- `read(path, input_format="iceberg", snapshot_id=...)` binds a local metadata JSON file.
  Without a selector, use that file's current snapshot. A metadata filename is explicit; do not
  discover the latest file by sorting names or trusting `version-hint.text`.
- `read(path, input_format="delta", version=...)` binds a local table root. Without a version,
  freeze the latest log version observed at admission, or fail if that version cannot be
  reconstructed. An incomplete or unsupported newer checkpoint cannot select older data.
- `read(path, input_format="hudi", instant=...)` binds a local table root. The exact instant
  selector is the requested/begin identifier of one completed write. Resolve it to that write's
  completion time, then admit completed writes through that completion boundary. Without a
  selector, freeze the latest completed write observed at admission. The initial profile requires
  table version 8 or 9, timeline layout 2, a complete active timeline and no archived LSM history.
- SQL source bindings carry the same optional `table_snapshot` object. Exactly the selector
  appropriate to the format is allowed. Existing file bindings and Python native tuple inputs
  remain compatible. Source declarations, explain and capability discovery do not read tables.
- Selected metadata generations must remain unchanged. Later commits cannot enter the held
  snapshot. Missing, replaced or mutated referenced files fail; a reader does not silently skip
  them. Output must not alias selected metadata, delete files, data files or the table root.
- `prepare_once`, remote paths, catalog/auth operations and table writes stay explicitly outside
  the initial table-source route. Ordinary queries and existing native/compatibility result
  writers continue through the shared pipeline.

## Locally accepted reader profiles

The finite profiles below pass the local controls and complete-result matrix recorded later in
this document. Extend a row only with version-specific source evidence and complete
positive/negative tests; the profiles do not establish general table-format certification.

| Format | Read state | Initial file/schema scope | Visibility obligations and exclusions |
| --- | --- | --- | --- |
| Iceberg v1/v2 | Explicit metadata; current or snapshot ID; manifest list and manifests | Parquet data/delete files; field-ID mapping; primitive logical types; rename, nullable missing fields and admitted widening; partition specs retained per file | Ignore deleted manifest entries; inherit sequence numbers correctly; apply v2 position/equality deletes using sequence/spec/partition applicability; reject v3/v4, encryption, unimplemented field types/defaults or missing IDs without a supported name mapping |
| Delta reader v1/v2 and explicitly admitted v3 features | Complete JSON replay from zero or complete classic/multipart Parquet checkpoint, then contiguous commits | Parquet data; none/name/id column mapping; typed partition values; nullable missing fields | Reconcile adds/removes as version transitions; enforce required reader features; reject unsupported deletion vectors, V2 checkpoints, catalog-managed tables, type-widening features or incompatible schema changes before rows |
| Hudi Copy-on-Write | Version-pinned completed timeline; selected base-file versions and replaced file groups | Local Parquet base files with authoritative schema | Apply completed write/replacement visibility; exclude requested/inflight/orphan files; explicitly reject Merge-on-Read/log merging, bootstrap, incomplete archived history and unsupported rollback/restore/cleaned-history states |

Nested field evolution is a separate acceptance dimension. A top-level rename fixture does not
prove nested support. Mixed/unsupported physical data-file formats fail during admission.
An empty valid snapshot retains its logical schema and returns empty results, rather than a
missing-source error. Snapshot row counts are logical counts after visibility.

## User contract and explicit limits

```python
import shardloom as sl

events = sl.read("/local/events", input_format="delta", version=12)
summary = events.group_by("region").agg("COUNT(*) AS total").collect()

orders = sl.read("/local/orders/metadata/v7.metadata.json", input_format="iceberg", snapshot_id=42)
history = sl.read("/local/customers", input_format="hudi", instant="20261101000100")
```

Supply local filesystem paths. Iceberg snapshot IDs fit signed 64-bit integers; Delta versions
fit unsigned 64-bit integers. Hudi's reader requires a 14- or 17-digit requested instant belonging
to a completed write. SQL uses the same declaration, for example
`{"/local/events":{"input_format":"delta","table_snapshot":{"version":12}}}` as
`--source-bindings`, with `SELECT * FROM '/local/events'`. Typed native callers pass
`tables=[(path, format, snapshot_id, version, instant)]` to `Session.sql` or `Session.plan`;
the existing `files=[(path, format, schema)]` form remains available. A source reference has one
snapshot declaration: conflicting selectors for the same reference fail before source access.
Identical declarations support self-joins. Table sources also compose with ordinary file sources.
Standalone plan schema inspection is not admitted for table sources; inspect the completed
result's schema. This refusal occurs before accessing the table.

Default and scoped `capabilities` output expose
`runtime_capability_local_table_input_{iceberg,delta,hudi}_{status,scope}`. These entries describe
the local reader profiles below and are disabled when the required build/platform support is
absent. Discovery performs no filesystem, catalog or network access. Broad table certification,
catalog integration and table-output statuses remain separate from this read-only availability.

The initial schema boundary is top-level primitive data. Common admitted types are Boolean,
signed integers, float/double, UTF-8, binary, date, microsecond timestamps and decimal precision
1–38. Iceberg additionally admits microsecond time. Delta admits byte/short and feature-declared
`timestamp_ntz`; Hudi uses the corresponding Avro primitive/logical schema. UUID/fixed-binary,
nested evolution, Hudi millisecond logical timestamps, non-null defaults and unsupported
conversions fail explicitly. Iceberg admits int→long, float→double and same-scale decimal
precision growth; Delta and Hudi require matching physical types. These are type admissions,
not a claim that every type/operator combination is implemented by the native engine.

Delta partition constants come from the add action even when physical files also contain those
columns. Empty partition strings mean null. Zoned timestamps require an explicit offset because
the legacy wire spelling omits the writer's timezone; `timestamp_ntz` accepts the specified civil
spelling and rejects offsets. Binary partition constants remain outside this profile. Hudi
partition fields must remain physically present; bootstrap and dropped-partition reconstruction
are not admitted. Logical name mapping does not establish support for Hudi internal-schema
renames or nested evolution.

Plain JSON/properties metadata is limited to 64 MiB per file. An Avro/Parquet control file is
limited to one million records; Delta replay is limited to one million contiguous commits.
The shared memory grant can reject a smaller state when its retained metadata, delete keys or
physical decode workspace does not fit. Control readers use the shared sizing policy, retain
Avro schema credits and admit Parquet row-group workspace before decoder construction. This is
reservation and ownership evidence, not an allocator or process-RSS bound.

## Shared source implementation

The snapshot retains logical schema, exact ordered live files, per-file mapping and partition
constants, applicable delete state, and generation identities for every metadata dependency.
Metadata parse capacity is reserved before parsing. Parsed structures and retained delete keys
stay charged; every file inventory growth is admitted. Source page decoding uses the existing
adaptive owner. No allocation/RSS guarantee is inferred from reservations alone.

For table sources, disable physical-only count, statistics, prefix and ranked-read shortcuts
until mapping/visibility proofs are implemented. Keep physical row ordinals for position deletes
across row groups. Apply deletes before visible-row limits and before releasing rows to native
operators. Equality-delete columns remain available even when the user did not project them;
delete keys use the selected field IDs, null equality and supported exact scalar semantics.
Native aggregation, ordering, joins and filters span the complete selected file collection.

Use existing structured diagnostics with deterministic format/state reasons. No unsupported
feature can degrade into a raw Parquet directory scan, omit a file or ignore a required delete.
Report selected format/version/snapshot and physical decoder separately in source evidence.

## Local acceptance and remaining work

The [original portable acceptance record](../benchmarks/evidence/native-local-table-readers-2026-11-02/acceptance.json)
and [review-correction supplement](../benchmarks/evidence/native-local-table-reader-review-2026-11-02/acceptance.json)
bind the corresponding runtime and source identities, every original failed attempt and complete
verification archives. The supplement fixes missing Iceberg identity-partition columns and their
equality-delete keys, Delta checkpoint byte/short values, and primitives-only feature compilation.
Independent fixtures use official wire schemas and complete expected results. Final local
verification passes:

- 24 focused table/schema controls covering mapping, delete applicability, checkpoints,
  Hudi visibility/schema ordering, empty snapshots, malformed state, generation mutation,
  cancellation, insufficient grants and ownership release.
- Default and enabled strict workspace Clippy, formatting, 3,311 default workspace tests and
  6,857 enabled workspace tests. The enabled suite retains 36 existing opt-in ignores.
- 98 Python API/binding/query-builder/dataset regressions, three declaration tests and generated
  public-API reference consistency. Declarations and unsupported schema inspection remain
  side-effect-free.
- 275 complete queries across CLI SQL, embedded workflow, Python SQL, DataFrame and typed native
  execution. The base matrix covers all three formats at 16 MiB and 128 MiB grants, including
  multi-file grouping/join/order, delete-key hiding, rename/name reuse, nullable additions,
  historical selections and mixed table/file joins. Additional cases cover primitive logical
  values, Delta partition constants and Iceberg identity partitions with hidden compound
  equality-delete keys, nulls and empty strings.
- Nine output writes with independent readback, three retained-result lifetimes, six output-alias
  refusals, 15 unsupported-state refusals, nine conflicting-snapshot refusals and nine
  pre-access schema-inspection refusals.
- All 299 installed-wheel integration tests without failures, errors or skips on macOS ARM64,
  with installed CLI/library identity, load and removal proof. PR #1645 merged as
  `9cd723df420399d92b3a2a8981d3823899fe2b24` after all 58 hosted checks passed on
  `78c5e0a9e262739b3ab5b86d261239e9c6f9baf1`; exact hosted evidence is
  [hosted-acceptance.json](../benchmarks/evidence/native-local-table-readers-2026-11-02/hosted-acceptance.json).

Final public and installed tests use the same frozen CLI and native-library bytes. The two
reported review findings were fixed and validated; the final automated re-review remained
running at merge, so this record does not claim that review was cleared. Hosted checks and merge
are complete; no benchmark result or performance claim is part of this implementation.

The useful adjacent-format inventory currently identifies SQLite ordinary reads and spreadsheet
workbooks beyond the existing eight file readers. Select an additional provider only after
table-state ownership is working and its license, type, memory and interface contract is grounded;
fixture import/export or an XLSX suggestion alone does not count as reader support.

## Source evidence

- [Iceberg specification](https://iceberg.apache.org/spec/): v1/v2 manifest entries, field IDs,
  sequence inheritance and delete applicability. Later-version sections do not expand v1/v2.
- [Delta protocol at 719768686369f1d52e6891aec1f7fa1be903975c](https://github.com/delta-io/delta/blob/719768686369f1d52e6891aec1f7fa1be903975c/PROTOCOL.md): replay, checkpoints,
  column mapping, partition values, protocol and reader-feature requirements.
- [Hudi technical specification at 66ecd5c68b1b54f4d5f6b7540519efd330bb97b5](https://github.com/apache/hudi/blob/66ecd5c68b1b54f4d5f6b7540519efd330bb97b5/website/learn/tech-specs.md):
  timeline, completion ordering, file-group replacement, cleaning and version boundaries.
  Layout-v2 commit/replace metadata uses Avro object containers, as shown by the
  [1.0.0 v2 serializer](https://github.com/apache/hudi/blob/1e364d19880af711ca7b622af654660b58801852/hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/CommitMetadataSerDeV2.java)
  and [1.2.0 v2 serializer](https://github.com/apache/hudi/blob/f05c83f2b97732de7a558ff9b26959e1139c05f5/hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/CommitMetadataSerDeV2.java).
  The separate legacy/v1 JSON serde does not establish the layout-v2 wire contract.
  The [1.2.0 active timeline](https://github.com/apache/hudi/blob/f05c83f2b97732de7a558ff9b26959e1139c05f5/hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/ActiveTimelineV2.java)
  orders schema candidates by requested time. Completion time decides snapshot membership;
  an older requested write finishing later must not replace a newer visible schema.
  The [schema-changing operation contract](https://github.com/apache/hudi/blob/f05c83f2b97732de7a558ff9b26959e1139c05f5/hudi-common/src/main/java/org/apache/hudi/common/model/WriteOperationType.java)
  also excludes clustering/indexing metadata from schema updates.

Alternatives rejected: metadata-smoke wrappers (no ordinary-query semantics), raw folder scans
(wrong visibility), and an external engine (violates the execution contract). Reusing the existing
decoder with a table-state owner keeps native execution and avoids a second resource scheduler.

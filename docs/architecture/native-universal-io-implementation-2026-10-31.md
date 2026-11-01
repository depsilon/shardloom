<!-- SPDX-License-Identifier: Apache-2.0 -->

# Universal I/O implementation batch

Status: implementation and acceptance remain in progress. The r6, r7 and r8
runtimes passed their combined correctness checks but failed the original
347-query strict performance retention gate. The November 1 maintainer decision
accepts r8's minor timing tradeoff for completing the broader direct-read and
shared-execution correction, subject to the remaining I/O and evidence gates.
Items 18 and 23 now prioritize engine completeness; performance tuning follows
both. This implements the October 31 batching decision in
[item 18](native-universal-io-performance-coverage-2026-10-24.md). Exact direct
source count retains its [separate semantic contract](native-direct-source-count-2026-10-31.md)
inside this batch. Changed candidates have separately identified validation
cycles; failed earlier checks and rejected measurements remain recorded. Full43
remains deferred.

## Direct reads and shared declarations

The October 31 follow-up requires ordinary queries to use direct source reads
through the shared native source owner. An existing automatically prepared
Vortex sibling must not redirect a query or make a fresh original-source query
depend on that artifact's generation. Metadata-only and explicitly requested
content fingerprints follow the same admission path. A retained reader still
rejects a changed original generation; a fresh binding can admit the replacement.

Complete SQL, typed plans, resident-worker calls, operation summaries and
explicit primitive options share source resolution and the native operator
plan. Primitive options remain authoritative when accompanying advisory SQL;
their predicates, projections, transforms, ranges and tie policies cannot be
dropped by routing only the SQL text. The primitive plan adapter reuses the
existing native aggregate, projection, unary, ordering and window operators.
It is declaration lowering, not a second evaluator or an input materialization
path. Single-source and native collection declarations share that adapter.

Explicit `prepare` and the explicitly selected `prepare_once` policy retain
their durable-source contract. Ordinary queries never retry a source failure
through full preparation. Reuse tests must exercise direct readers, exact
results, source invalidation, zero prepared-input writes and writer alias
protection; explicit preparation has separate lifecycle acceptance.

Direct file intake preserves the existing top-level scalar dictionary contract
through the pinned Vortex Arrow importer and the shared native buffer owner.
It reuses ingest's dictionary-type and strict floating-domain checks, including
unreferenced dictionary values. No dictionary-to-row conversion is introduced.
This file-reader admission does not enable the separate experimental encoded
Arrow boundary for caller-owned inputs or broaden nested dictionary support.

### Ordering demand through plain projections

The initial complete local matrix exposed a shared planning gap: SQL removes
hidden ordering columns through a projection between sort and limit. That
projection prevented bounded retention even though equivalent typed and
DataFrame declarations used the existing prefix owner. Source reads and complete
results matched, but the SQL paths retained the full sorted population.

The native binder now passes finite ordering demand through bound column-only
projections, including compact projection pipelines. Renaming, reordering,
duplication and removal preserve row order and cardinality. Computed expressions,
filters, inner ranges, observations and application functions remain barriers;
source completion and full incoming-payload validation still occur. This extends
ShardLoom plan normalization while reusing its existing Vortex-native sorting,
gather and ownership providers. It adds no evaluator or external fallback.
Acceptance requires the new activation regression, error/ownership controls and
the unchanged format/interface matrix on the corrected runtime.

### Reusing unchanged dispatch schemas

The shared Arrow-to-Vortex duration dispatcher previously rebuilt every field
and the schema metadata for every batch, even when no dispatch annotation was
needed. The current candidate borrows the authoritative schema in that case.
The pinned Vortex Arrow importer already accepts a borrowed schema; this uses
that provider boundary without adding a schema cache or another source owner.
Missing duration markers still use the existing owned translation. Both paths
validate metadata recursively, including fields after a missing marker and
physical dictionary/Null fields admitted during explicit preparation.

This removes a demonstrated allocation; it is not an established explanation
for r6's performance result. The storage-reuse regression failed before the
correction while both metadata/preparation controls passed. The changed runtime
passed its correctness gates but did not retain under the performance rule.

### Reusing validated Arrow array descriptions

The next candidate removes another repeated operation in shared batch intake.
Validation constructs each column's `ArrayData`; the ownership boundary now
consumes that same description instead of constructing it again. The existing
metadata reservation covers its temporary column vector and child descriptors.
All schema, full recursive array, strict floating-domain and decimal checks
still finish before any payload allocation or transfer. The original batch
releases its references before the unchanged unique-allocation proof runs.
Shared buffers, custom owners and unsuitable offsets/alignment still require
credited copies. Nested copying recurses through the existing `ArrayData`
without converting each child to an array and back.

This uses the pinned Arrow representation and existing Vortex Arrow importer;
it introduces no source owner, unchecked construction, validation exemption or
execution provider. Source inspection proves repeated representation work, not
its timing contribution. Ownership/error-order regressions and the complete
candidate gates must pass before any performance or acceptance claim.

## Shared ownership and writer policy

One explicit multi-output call computes one native result. The existing credited
relational replay owner supplies sequential staged writers, including its
explicit spill policy. Primitive providers use the same native replay mechanism
under their execution grant and fail on denied resident retention; they do not
silently enable disk spill. Separate calls still compute separately. Report
copies are reserved before publication, and final source validation precedes
publication. Multiple destination commits remain individually atomic, not a
multi-file transaction.

Conversion batches are independent of Parquet row groups and ORC stripes.
Candidate bulk policy uses Snappy, admitted Parquet dictionaries and chunk
statistics, bounded page/row-group targets, and ORC stripes sized by the
execution grant. The pinned Parquet dictionary interner has a fixed initial
allocation per physical column that does not shrink with its page target.
Dictionary admission reserves that cost and omits dictionaries for schemas
whose fixed state exceeds the declared share of the grant. Wide schemas still
use Snappy and chunk statistics. Footer admission follows resulting file units,
including an existing row group or stripe, rather than counting another unit
for each conversion batch.
Parquet page/dictionary workspaces count physical leaf columns; list/struct
containers remain charged as schema/footer nodes without inventing a second
value writer for each container.
ORC admission follows each node's actual encoder streams, including pending
integer literals that the provider's observed-memory method does not count.
Accumulated codec state and footer metadata require credit before writer work.
Pressure and completion flushes are observed separately from conversion splits.
These policies require the item's write/reopen measurements before acceptance;
no throughput gain is claimed from source inspection.

Streaming conversions split by expanded value width and available credit. A
single large value may exceed the soft batch target with full resource admission.
Real offset limits remain checked; no truncation or unaccounted oversized retry
is permitted. The bounded collection API retains its separate limits.

## Avro temporal fidelity

Avro timestamp output preserves admitted millisecond, microsecond and nanosecond
ticks and nulls. Avro distinguishes local timestamps from instants but does not
store an instant's presentation-zone name. Its pinned reader returns zoned
timestamps with `+00:00`; the shared recursive schema translation and fidelity
report state this change explicitly. Reopen validation accepts the provider's
exact nanosecond-unit annotation only for a nanosecond timestamp. Changed units,
naive-versus-zoned semantics, nullability and unexpected metadata remain errors.
The regression includes empty and nested output plus both signed 64-bit tick
endpoints. Native Vortex output retains its original temporal domain.

## ORC provider decision

The pinned Apache-2.0 `orc-rust` provider already reads Decimal128, List and
Struct and writes Date32 plus four timestamp units. Its writer lacked recursive
schema/stream topology and decimal/list/struct encoders. Extend that existing
codec boundary with original implementations based on the
[Apache ORC v1 specification](https://orc.apache.org/specification/ORCv1/),
without an external query engine or a new dependency.

Schema IDs and stripe encodings follow the same preorder tree. Struct children
encode only rows whose parent is valid. Lists encode nonnull lengths and only
their reachable elements; null parent payloads and sliced-away children must
not leak into child streams. Decimal values use exact signed 128-bit varints,
scale streams and validated precision. Null/present state resets across stripes.
Unsupported provider schemas return errors before writing the file header.

ORC dates use Date32 days. Date64 requires exact whole-day conversion and checked
range. ORC timestamp values preserve their native ticks within the wire range;
timezone presentation and Arrow timestamp-unit metadata are not intrinsic ORC
fields. The writer adds the optional `shardloom.arrow.timestamp.unit` type
attribute; the reader validates it and preserves each column's original unit,
including nested columns. Explicit reader precision overrides it. Unknown or
duplicate values of this reserved attribute fail; stream decoding rejects
precision loss. Independent readers may ignore the attribute and choose their
own precision. Non-UTC presentation zones normalize to UTC with the same instant.
The [ORC-763 pre-epoch fractional interval](https://issues.apache.org/jira/browse/ORC-763)
and ORC epoch-offset overflow remain explicit value errors; there is no lossy
timestamp substitution. Time-of-day and Duration have no dedicated ORC wire type
and remain deterministic unsupported cases unless an explicit reversible mapping
is implemented. No claimed full temporal fidelity follows from Date32 support.

Provider tests must cover recursive schemas, null parents, nonzero list offsets,
null-list child payloads, empty/nested lists, multiple batches/stripes, decimal
extremes and invalid schema/value refusal. ShardLoom tests additionally prove
grant denial, cancellation, staging cleanup, fidelity reporting and independent
complete readback with a separate reader implementation.

## Remaining implementation and acceptance

### Recorded r6 outcome

The r6 native workspace suite passed 6,684 tests, with 36 intentional ignored
tests. Its installed package passed 289 tests without skips. The unchanged local
I/O matrix completed all 9,633 declared cases: 205 smoke, 4,555 correctness,
400 fidelity, 18 lifecycle and 4,455 timing observations. These counts establish
completion and correctness, not performance acceptance.

The two declared confirmation rounds retain all original timing observations
and pool repeated measurements. Interface costs, cooperative deadline checks,
and the DataFrame intermediate-payload validation barrier have separate source
and diagnostic evidence; they do not waive unresolved avoidable gaps.

All 347 original regression cases returned correct complete results, but total
engine time increased from the accepted **11.978234339 seconds** to
**12.058373305 seconds**. Under the maintainer's binary complete-cohort rule,
r6 drops and the accepted baseline does not advance.

The r7 dispatch-schema candidate passed 6,686 native workspace tests, with the
same 36 intentional ignored tests, and 289 installed-package tests without
skips. Its corrected-supervision I/O matrix completed all 9,633 cases. All 347
original regression cases and 12,565 complete result rows matched, but total
engine time was **12.021364065 seconds**, above the same **11.978234339-second**
accepted baseline. It also drops; no case or family diagnostic threshold
overrides that complete-cohort result. No unchanged-runtime retry follows.

The r8 candidate reuses validated Arrow array descriptions. It passed 6,688
native workspace tests with 36 intentional ignored tests, 3,292 default-build
tests and 289 installed-package tests without skips. All 347 original cases and
12,565 complete result rows matched. Its **12.065099498 engine seconds** exceeds
the **11.978234339-second** baseline by **0.73%**; the original
`drop_no_complete_engine_gain` result is preserved. This is not an optimization
gain or a causal attribution to the array-description change.

After that result, the maintainer accepted the minor regression for the larger
direct-source/shared-execution correction and clarified that items 18 and 23
must finish engine integration before performance tuning. Slight differences
between I/O formats and execution surfaces are expected. The r8 I/O matrix now
passes all **9,633 observations**: 205 smoke, 4,555 correctness, 400 rich-value
fidelity, 18 preparation/reopen lifecycle, and 4,455 timing observations. All
five interfaces retain complete result checks. Independent post-run decoding of
the saved Vortex outputs validates 348 readbacks covering 33 distinct
output/reference pairs, without executing more engine queries. Frozen inputs
and all 3,249 build-source files still match the validated runtime.

Portable evidence replay, functional gap disposition, documentation/website
checks and PR acceptance remain open. Existing timing screens remain visible as
diagnostic results; they do not require more tuning cycles for this completion
work. Earlier runtimes' matrix observations do not replace current-r8 coverage.

Before the next candidate measurement, the outer supervisor was corrected to
keep inventories of completed evidence outside query execution. Active output
checks retain the same total storage ceiling by subtracting the pre-run inactive
bytes from the active subtree's allowance. Full inventories still run before
and after; free-space, workload, deadline and cleanup checks remain. The original
inner query runner is unchanged. Code and supervision changes are recorded
separately, and neither is asserted to explain r6's timing difference.

### Local dataset and variant contract

The shared local source owner admits recursive directories, `*`/`?` component
globs (`**` crosses directories), and explicit `.manifest` collections. Matching
files have deterministic lexical path order. A manifest is an object with
`version: 1`, a canonical `format`, and a nonempty `files` array of local file
paths resolved relative to the manifest. Duplicate resolved paths fail. The
manifest generation and every selected file remain held through final drain;
directory/glob membership is rechecked. These are file collections, not table
snapshots. Known table metadata directories must fail with an explicit table-
semantics diagnostic rather than being treated as a collection of live rows.

Existing file and directory schemas remain strict by default. A manifest can
explicitly request `schema: "union_by_name"`: preserve first-file field order,
append later new names, and fill missing fields with typed nulls. Equal names
must have identical logical types and metadata; no implicit numeric, nested,
decimal or timezone cast is permitted. Nullable fields widen conservatively.
An optional `partitioning: "hive"` exposes `key=value` path components as
nullable UTF-8 fields, with `__HIVE_DEFAULT_PARTITION__` as null. All files must
declare the same partition keys; collisions with stored columns fail. Partition
values are metadata-derived columns, not a table or partition-pruning claim.
Hive values use resolved paths within the resolved manifest directory; aliases
and parent components cannot escape that directory. Parquet schema unions and
virtual partition fields currently use conservative provider projection without
the strict-schema reader's statistics pruning or metadata-only count capability.

Text adapters additionally admit `.gz`, `.zst` and `.zstd` wrappers. Decode is
incremental over the same held, I/O-admitted reader; physical bytes count the
compressed source, and complete schema-discovery work is reported separately.
Gzip uses the already pinned flate2 1.1.9 (MIT/Apache-2.0); Zstandard uses the
already pinned zstd 0.14.3 (BSD-3-Clause), with a credit-admitted maximum window.
These optional codecs do not add an execution provider. IPC file and stream
framing share the Arrow 59.3.1 boundary, including its LZ4/Zstandard codecs.
TSV uses CSV's quoted-record framing with a literal TAB delimiter. Unknown
encodings, arbitrary dialects and legacy Feather V1 remain explicit exclusions.

Python's generic `read` accepts `input_format` for paths whose extension does not
identify their format. For example, `read("events.manifest", input_format="jsonl",
schema={"id": "int64"})` preserves JSON Lines framing and the declared field type.
The context and session methods forward the same declaration. Construction is
inert; ordinary terminal calls perform source admission and execution.

Whole-document JSON must parse incrementally and preserve exact late-schema
discovery through explicit replay when a schema is not declared. All admitted
source families must feed shared demand/native operators, reporting their real
read/parse/decode limitations. Dataset membership and format variants reuse that
source contract. The maintainer explicitly deferred the remote workflow on
October 31 when asked to identify the service and disposable test dataset.
Authenticated discovery, selective remote reads, native computation, generation-
safe refresh and independent remote readback remain deferred; provider features
alone are not completion. They do not block this local implementation batch.

Before measurements, freeze the five-interface/format/scenario manifest and
finite benefit/resource decisions required by item 18. Combined workspace,
provider, public-interface and installed-package checks, exact output readback,
source/work counters and adversarial review gate acceptance. No optional format,
connector or version publication is authorized by this implementation note.

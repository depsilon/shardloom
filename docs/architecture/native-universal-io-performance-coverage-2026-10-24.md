<!-- SPDX-License-Identifier: Apache-2.0 -->

# Universal I/O performance and coverage

Status: accepted October 24 maintainer handoff, amended October 28. The current
sequence is item 20, item 18, the 1.1.0 milestone, then item 19; the
[phase plan](phased-execution-plan.md#planned) owns the execution order.

The original handoff followed
[user-surface native execution parity](native-user-surface-execution-parity-2026-10-24.md)
and [controlled provider refresh](native-vortex-provider-refresh-2026-10-24.md),
then [warm source reuse](native-warm-source-reuse-2026-10-24.md),
then [public naming](shardloom-public-naming-2026-10-24.md),
then [full-ingest ownership/preparation work](native-ingest-ownership-preparation-2026-10-24.md),
[consolidated dependency maintenance](dependency-intake-2026-10-25.md),
[complete-workflow work elimination](native-workflow-work-elimination-2026-10-25.md),
[ingestion batch/window scheduling](native-ingest-batch-window-scheduling-2026-10-25.md),
and the [connected programming model](native-connected-programming-model-2026-10-24.md).
The subsequent October 24 ordering decision places I/O immediately after
connected programming and before [rich-data lifecycle support](native-rich-data-lifecycle-2026-10-24.md).
Implement current-type adapter coverage here; the rich-data package extends these
adapters when it admits additional logical types.
Grouped optimization and surface parity are completed foundations. The original
handoff reviews main `06653efb` and #1594 `e1af0d68`; it is a source
review and proposal, not new benchmark evidence or completed implementation.
Recheck current source and pinned providers when this work begins.
The refresh evaluates per-consumer Arrow compaction and the optional
object-store registry, Hugging Face and OpenDAL provider primitives. Reuse its
accepted boundaries here; feature availability does not certify a complete
connector, allocation lifetime or transport policy. Export compaction is
separate from import ownership and must account for retained backing buffers.
The later [AI-data package](native-ai-data-workflows-2026-10-24.md) reuses this
source/publication contract for model batches and search-index generations;
consumer adapters must not reopen original inputs to bypass ShardLoom plans.
The queued [Foundry integration](native-foundry-integration-2026-10-25.md)
consumes these same multi-file, native writer and publication contracts after
everyday analytical completion and before the AI-data package.

**October 28 continuation:** finish active item 20 first, then complete this
item's remaining I/O work with the shared demand-aware defaults and the
[cross-surface and format acceptance gate](#cross-surface-and-format-acceptance-gate)
below. The planned **1.1.0 minor version bump follows item 18 acceptance**,
before item 19. Foundry and AI acceptance move to the later 1.2.0 and 1.3.0
milestones respectively. This updates the earlier ordering without accepting
the preserved `codex/universal-io` branch or claiming new runtime/timing proof.

## Contract and intake

Reuse source-encoding translation accepted by the preceding ingestion and
analytical items, carrying its representation capabilities through the current
shared native consumers. Extend proved type/validity/dictionary/
offset mappings across adapters here; do not assume equal encodings or duplicate
the earlier mechanism. Output adapters continue to report unavoidable
normalization and metadata loss.

Format choice determines input/output adaptation around the same native engine.
Equivalent data, semantics, resources and result requirements receive equally
capable native computation. Minimize required reads, decode, conversion, encoding
and copying; avoid repeated preparation/execution, unnecessary intermediate
files and output reconstruction. Physical text parsing and compressed encoding
remain real work, with explicit costs rather than invented equality.

Source adapters feed native Vortex representations and shared operators; native
results feed destination adapters. Native representation does not always require
durable preparation of the complete source. Admitted native batches may serve
a selective one-off operation, while durable native preparation remains valuable
for repeated broad work. This is an input strategy, not another query engine.
The #1583 provider-selection and scalar-writer repairs are completed foundations.

The October 25 addition originally placed explicit multi-output reuse in item
15, whose implementation has since been dropped. Ground the one-producer
contract against current shared producer/replay owners here; do not assume that
discarded implementation survives or restore it unchanged. Immediate I/O work
starts with bulk writer policy. Rich ORC output, long values, target-aware
conversion and streaming JSON follow. Selective first-use reading, dataset
layouts, variants and enterprise,
table and transport adapters remain later implementation groups; they must not
delay the immediate local fixes. Audit complete families rather than assuming
every listed feature is absent or making a format count a completion criterion.
This explicit queue addition does not make every family a v1 release blocker or
close independent CG-1 through CG-23 evidence gates.

## Shared demand-aware defaults

Carry the accepted [source-demand contract](native-source-demand-2026-10-26.md)
and [Parquet completion](native-parquet-demand-completion-2026-10-26.md) into
ordinary queries over every supported input family. Parquet's implementation
is a completed foundation; audit the remaining adapters against current main
and extend the existing source capabilities, backward demand planning, native
batch ownership and operators. Do not revive the unfinished item-18 branch
unchanged or add a separate engine mode, planner or per-interface fast path.

- Bind schema and derive the complete required-column set, including hidden
  filter, grouping, join, ordering, window and repeated-source dependencies.
  Apply projection, statistics/region pruning and safe prefix/limit demand only
  where the adapter and query semantics prove them valid. Preserve residual
  predicates, source order, global limits and error semantics.
- Declare each adapter's actual abilities. Where a format lacks usable metadata
  or selective decoding, use conservative, bounded native reading and report
  its required parse/decode work. Do not report physical pruning from merely
  discarding decoded columns, or treat unavailable read counters as zero.
- Use the same capability/strategy decision across CLI SQL, embedded workflow,
  Python SQL, DataFrame and typed native declarations. Preserve source identity,
  memory and I/O admission, cancellation, reader lifetime and final drain checks.
  Read, decode, identity or resource failures terminate execution; they do not
  retry through full preparation or an external engine.
- Keep ordinary query validation scoped to metadata and demanded data. Explicit
  full-source validation and durable complete-source Vortex preparation retain
  their complete-input guarantees. A selective query result may be persisted,
  but cannot be advertised or reused as a complete prepared copy of its source.
  Prove these distinctions with demanded/undemanded corruption, late malformed
  records and changed-source tests.

## Later conditional encoding research

The optional `PERF-09.portfolio` owner now includes exact cross-column
transforms inspired by [FastLanes multi-column compression](https://ir.cwi.nl/pub/35881/35881.pdf).
For a proved integer/fixed-scale relation such as `y = a*x + b + residual`,
preserve exact reconstruction, NULLs, overflow and exceptions; floating data
requires the original bit/semantic contract, not a real-number approximation.
First check Vortex's existing encoding/extension surfaces. A useful profile
must justify dependency reads, bounded dependency graphs, encoder metadata,
random access and selected reconstruction against independent columns.
Test narrow reads and uncorrelated data as controls. This remains conditional
research behind the execution and ingestion opportunities, distinct from the
rejected structural-text profile, without a new format or broad release promise.

## First: reuse explicit fanout and improve writer layout

- **One producer for one multi-output request.** The reviewed single-use streaming
  path already records/replays one native result. Ordinary non-streaming
  relational `write_many` and primitive per-destination `operation.write(...)`
  callbacks need reconciliation with that mechanism. Extend it to file-backed
  and retained-memory workflows. Small results use credited native owners;
  larger results use bounded delivery or explicitly admitted native replay/spill.
  Sequential writers are sufficient. Separate user calls do not gain hidden
  answer caching. Prove one producer execution through a public test and complete
  independent readback of every destination. Multi-file staging is not atomic
  publication; preserve each destination's failure and commit contract.
- **Separate execution/conversion batches from file units.** Audit the shared
  sink's reviewed 2,048-row conversion batches, per-batch Parquet flushes,
  uncompressed PLAIN output with dictionaries/statistics disabled, and 1-MiB ORC
  stripe target. Let format-native row groups/stripes accumulate within admitted
  credit, flushing for size, pressure, format requirements and completion.
  Compression, dictionaries and statistics become deliberate measured policies,
  with codec cost, output traffic and later reads considered together. Do not
  equate a small work batch with a row group, stripe or output file.

The pinned Parquet writer's row-group accumulation API and ORC writer behavior
must be verified before design. Use large-output/write–reopen workloads to
choose policy. These settings are not evidence for the earlier approximately
57-second Parquet Full43 workflow difference: that suite returns only 381 rows,
and the recorded follow-up established one preparation per fresh suite.

## Next: useful coverage with resource-aware conversion

- **Long values and names.** The streaming sink already removes cumulative row,
  batch, column and file-size restrictions; do not claim all product writes are
  capped at 65,536 rows. Audit the remaining 64-KiB individual-string, 8-MiB Arrow
  batch and 256-byte field-name limits. Split multi-row conversions by actual
  width and available resources. Give a single large string or nested value an
  admitted representation/strategy. Preserve real format limits, reservations,
  cancellation and malformed-data rejection. Removing a constant or truncating
  data is not an implementation.
- **Target-aware conversion.** Preserve existing compatible contiguous native
  byte-buffer reuse. Each sink should state support for dictionaries, views,
  contiguous buffers, large offsets, logical metadata and nesting. Arrow batches
  are legitimate codec boundaries; avoid native-column → scalar-row → rebuilt-
  column detours. Vortex output retains its native path. Convert once at the
  explicit boundary and report unavoidable decode/reconstruction accurately;
  neither is zero-copy.
- **ORC rich types.** Implement decimal, date/timestamp, List and Struct output
  through the pinned provider boundary with exact mappings and independent
  reader validation; then audit additional type/encoding combinations. Present
  exclusions are provider-integration limitations, not intrinsic format limits.
  A documentation-only restatement of those gaps does not satisfy this request.
- **Whole-document JSON streaming.** `whole_json_typed.rs` currently retains the
  complete source text and typed-column collection before writer batches. Extend
  record-array parsing to feed native typed batches incrementally. With an
  explicit schema, bind parser/builders directly; without one, choose exact
  schema discovery/widening/replay that retains late fields and type changes.
  Measure any additional pass. Bounded writer batches alone are not a streaming
  parser. Cover declared-schema empty input, long/nested/Unicode values, exact
  numbers, malformed suffixes and missing-versus-null semantics. JSONL and CSV
  retain format-correct framing; general JSON and quoted multiline CSV are not
  arbitrary newline-separated records.

## Then: first-use strategies, family coverage and dataset access

Reuse item 20's accepted Parquet demand reader and selected-chunk, conservative
prefix and directory/content-identity work. Existing projected Avro helpers
are building blocks, not proof of equivalent ordinary first-use behavior in
that adapter or other formats. Inspect metadata,
select required columns/eligible regions, decode admitted batches, retain native
owners and invoke existing operators. Verify projection, row-group/row-selection
and filtering support in the pinned Parquet reader and actual indexing support
in the pinned ORC reader. Preserve validation and generation identity. A projected
or predicate-specific prepared artifact cannot masquerade as a complete source.
Choose the input strategy internally; no separate user-facing fast/compatibility
engine modes are requested.

Item 20's shared source-demand contract owns common planning, setup and admission.
This I/O package extends the format-specific read/pushdown and streaming
mechanisms plus their full consumer proof. Develop one shared implementation
instead of creating parallel source resolvers or treating a plan or isolated
helper as completed adapter coverage.

| Family | Coverage to audit and complete with actual read/write/reopen evidence |
| --- | --- |
| Vortex | Native fidelity, output-derived statistics, applicable encodings and independent-producer interoperability. |
| Parquet | Logical/nested types, supported codecs/encodings, useful metadata, scalable layout and exact interoperability. |
| Arrow IPC / Feather | Distinguish file and stream framing, dictionary/delta rules, compression, empty batches, large offsets and metadata; Feather V2 is the file variant, V1 is separate legacy scope. |
| ORC | Rich output types above, then broader type/encoding combinations with provider-specific proof. |
| Avro | Reader/writer schema resolution, nullable unions, defaults, logical types, codecs and block streaming. |
| CSV / TSV / delimited | Dialects, quoting/escaping, multiline framing, encodings/BOM, explicit schema/null rules and long values. |
| JSON / JSONL / NDJSON | Streaming, rich values, exact numbers, schema evolution and explicit temporal/binary representations. |

Every family distinguishes exact logical preservation, explicit reversible
representation and deliberately requested lossy/coerced export. Text formats
cannot always preserve native physical metadata; exact re-import may use an
explicit schema or optional companion. Never hide that requirement behind a
self-describing claim. New adapters do not by themselves establish timezone,
wide-decimal, map/union or arithmetic support throughout the native engine.

Extend directory/glob/manifest and partitioned datasets, many-small-file handling,
schema evolution and compression wrappers through the same source contracts.
Define collection membership, partition values, ordering and generation changes
consistently. XLSX is the next proposed enterprise family, followed by justified
legacy workbook/specialized formats with real implementations. Specify worksheet
selection, formulas versus cached values, dates, precision and format limits;
no silent macro execution or coercion.

Keep table semantics separate from file formats: Iceberg requires table state,
schemas and deletes; Delta also requires applicable deletion-vector and column-
mapping semantics; Hudi requires base/log-file behavior. Reading every Parquet
file in a table directory is not table correctness. Verify current specifications
and pinned implementation support before adding a provider. Keep S3, ADLS, GCS
and HTTP as transport boundaries with authentication, range access, retries and
publication semantics, reusing file adapters and the native engine.

The subsequent [everyday analytical completion](phased-execution-plan.md#everyday-analytical-completion)
request keeps remote/database/table integration in this package. Complete one
selected authenticate → schema discovery → selective read → native computation
→ safe publication workflow, including credentials, range access, retries,
source generations and independent readback. A connector name or manually
staged file is not that acceptance. Safe replacement/refresh of an existing
dataset comes before append/upsert against an explicit table contract, then
broader transactional operations under the table/commit RFCs. Specify destination
generation conflicts, reader visibility, staging, failure recovery and cleanup;
per-file atomic publication does not imply a multi-file transaction. These later
groups retain their place after the immediately actionable local writer fixes.

## Measurement and completion

Reuse existing Full43 and modular workflow tooling. Use small fixtures broadly
and large workloads selectively, not the complete frontend × format × codec ×
type product at 100 million rows. Query headlines remain engine-only. For this
explicit I/O campaign, separately label preparation, conversion, writer
finalization, output size and complete-workflow observations; moving necessary
work outside a timer is not an improvement.

Required workload groups are equivalent prepared-native queries; fresh source →
computation → output; large output → independent reopen → follow-on query;
one computation → multiple independently read destinations; and changed-source,
pressure, cancellation/failure and publication-cleanup cases. Large-output tests
must cover many batches and long documents/values: a 381-row suite cannot assess
bulk file layout or parser memory behavior. Freeze finite correctness/resource/
benefit decisions before each performance candidate and preserve every outcome.

### Cross-surface and format acceptance gate

The October 28 maintainer instruction makes correctness and timing across all
execution surfaces and I/O scenarios a required item-18 acceptance deliverable
before the 1.1.0 version bump. The objective is to eliminate avoidable performance
differences on equivalent work. CSV parsing, compressed decoding and different
output encodings need not take identical time; necessary physical work must be
distinguished from redundant preparation, weaker planning or wrapper-specific
execution. Existing smoke tests and declarative parity reports do not establish
this gate, and the earlier failed surface timing screen stays failed.

Freeze a coverage manifest against the accepted runtime and package, recording
every applicable surface, input, output and scenario combination. Use all five
surfaces: CLI SQL, embedded workflow, Python SQL, DataFrame and typed native
relations. Cover Vortex, Parquet, Arrow IPC/Feather, ORC, Avro, CSV/TSV and
JSON/JSONL/NDJSON; distinguish IPC file/stream framing and supported codecs,
layouts and schema options. Audit aliases instead of multiplying identical
implementation paths. Include admitted directory/multi-file and mixed-source
workflows. Unsupported combinations need their diagnostic and reason in the
manifest; missing measurements stay open, not passed or silently omitted.
An advertised or admitted combination that fails remains a blocker, not an
unsupported exemption.
Later table, remote and enterprise adapters retain their separate implementation
contracts and join the matrix when admitted; this gate does not make every
optional adapter mandatory.

| Scenario | Required matched evidence |
| --- | --- |
| Prepared native computation | The same native input and equivalent query/result semantics across all five surfaces, including narrow/selective and broad scans, aggregation, joins, ordering, windows and composed/retained results. |
| Fresh source and demand | Each admitted source family with no prepared artifact: projection, selective and unprunable controls, safe limit, broad consumption and repeated/mixed-source consumers; record requested versus applied demand. Fresh source does not imply cold filesystem cache. |
| Reuse and complete preparation | Explicit full preparation, repeated queries, separate-process reopening and changed-source rejection, keeping complete-source guarantees separate from query-scoped validation. |
| Output and follow-on reads | Native and compatibility destinations, large multi-batch writes, one computation with multiple outputs, independent complete readback, then a follow-on query; exercise different input/output format pairs as well as same-format round trips. |
| Fidelity and resources | Empty/null/unsigned/decimal/temporal/nested and long-value cases as admitted, small through large/skewed/wide inputs, pressure/spill, cancellation, partial failure and publication cleanup. Rich types added by item 19 extend this same matrix. |

Reuse the 347-case declarations, independent complete-result comparators,
existing native/public-interface tests and modular I/O runners. Add necessary
adapter and surface cases without implementing another benchmark query engine.
Run broad correctness on bounded fixtures; use a declared representative timing
set across every admitted surface and format pair, with larger selective-read and
bulk-output cases where scale changes behavior. Record the mapping and justified
exclusions so a sampled large-scale suite cannot masquerade as an exhaustive
surface × input × output × codec × type test. Full43 remains an isolated,
single-pass regression check and cannot replace this matrix.

Match logical values/schema, query semantics, output fidelity, resource grants,
source generation and workload scale. Compare surface overhead on the same
physical inputs; record physical encoding/layout differences when comparing
formats. Freeze source/build/package identities, cache context and native
strategy/worker evidence. Use the same engine-owned clock for engine comparisons;
retain preparation, conversion, serialization/finalization, complete API/workflow
clocks and read/decode/copy/memory observations separately in diagnostic evidence.
Missing clocks or counters remain unavailable. Routine timing updates remain
engine-only. Record complete values, schemas, nulls, ordering, metadata and native
no-fallback certificates, with independent output readback.

Define relative and absolute material-gap thresholds, controls, comparison order
and a finite investigation/confirmation budget before candidate measurements.
Compare case and family results, not only a suite total. Investigate material
gaps in shared planning/operators first, then justified adapter policy; preserve
every observation and original failed gate. A single sequential observation is
not proof of timing equivalence. No routine repeated Full43, cache manipulation,
artificial pacing or new competitor run follows from this addition; existing
competitor observations retain their original formats and clocks.

Before advancing to the version bump:

- [ ] Freeze and execute the manifest's required correctness and timing coverage
  across all five surfaces and every admitted format/scenario family.
- [ ] Prove default demand decisions, complete-source boundaries, exact output
  fidelity and lifecycle/resource behavior through ordinary public calls.
- [ ] Fix demonstrated avoidable performance gaps and validate neighboring and
  held-out controls. Keep unavoidable format costs and measurement uncertainty
  explicit; unresolved material avoidable gaps block acceptance unless the
  maintainer explicitly accepts a source/evidence-linked exception.
- [ ] Produce a replayable report with per-case engine timings, work evidence,
  complete result/readback checks, coverage gaps and retain/drop decisions.
- [ ] Complete applicable workspace, native, installed-package and regression
  gates, merge the accepted work, and reconcile docs/capabilities and the ledger.

This is a future acceptance requirement, not a claim that the matrix has run or
that performance parity is established. The version bump follows this evidence;
package/channel proof and publication authorization retain their own gates.

ShardLoom technique review: use metadata-first source pruning, retained source
and result ownership, bounded capillary work units, existing PulseWeave producer
contracts where applicable, dynamic shaping under shared grants, explicit timing
boundaries and existing evidence tiers. Introduce no duplicate scheduler,
unaccounted buffers, implicit effects or alternate query engine. Native Vortex
input/output and no-fallback certificates remain mandatory. Complete family
contracts include mappings, unsupported diagnostics, provider/license review,
independent readback, pressure/failure tests, docs and source-linked evidence;
additional table/transports do not bypass their governing RFCs or authorization.

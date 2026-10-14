<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native streaming input composition

Status: local and hosted acceptance complete under PERF-03/06/07/11/12 and
CG-5/19/20/21. [PR #1548](https://github.com/depsilon/shardloom/pull/1548)
merged at `5b5cd6a7` after all 44 checks passed. The
[hosted receipt](../benchmarks/evidence/native-stream-composition-hosted-2026-10-13.json)
records unchanged reviewed/merged source and preview/production verification.
The [phase plan](phased-execution-plan.md) owns sequencing.
The October 13 maintainer direction resumes capability and hardening work,
with the quickest coherent units first and conditional investigations last.
The [fresh full UAT](../benchmarks/native-fresh-uat-2026-10-13.md) establishes
the unchanged-main baseline before these changes.

## Shared preparation lifecycle

The batch transaction must establish its cancellation reader and shared native
session before source resolution or compatibility preparation. Dynamic SQL
resolves its source declarations before registering memory inputs, so starting
the reader in that registration callback is too late. The same token must reach
source expansion, native preparation, compatibility normalization, input intake,
execution and sinks. Initial cancellation must prevent producer demand and
source access; cancellation after a producer returns must drop its new owner.

Compatibility inputs use the existing controlled normalization facade and
Vortex ingest lifecycle. Framing scratch, normalized sources and native state
share one memory grant. The source-generation identity transfers into the
prepared operation and remains subject to final validation. Cancellation must
remove owned staging files, preserve foreign/existing destinations and return
no successful result or output prefix. Once an independent preparation has
already committed successfully, later query cancellation does not delete that
valid prepared artifact. This is cooperative cancellation, not interruption of
an arbitrary blocked filesystem call.

Vortex-first decision: `use_vortex_native_provider`. Retain pinned Vortex 0.85 native
arrays, dtype, file source and writer providers, the existing resident session,
reserved allocator and controlled ingest. This change connects ShardLoom's
transaction ownership to those established providers; no new data representation,
execution provider, dependency or fallback is introduced. The expert comparator
is an encoded-columnar maintainer checking admission order, source identity,
final-owner credit, publication and cancellation together.

## Composition and ownership

Multiple input producers require distinct URI/sequence/completion state under
the same grant, including a streamed build and probe, mixed resident sources,
empty/null inputs and failure in either producer. A second producer must never
receive the first producer's sequence number or completion credit.
The binder retains a reserved collection of schema-only sources. Native scan
nodes identify their source slot; one transaction callback receives the URI and
that source's batch ordinal. Existing single-source Rust and Arrow callbacks
remain valid only for one declaration. Completion requires every admitted source
to reach its own explicit end, including empty sources and zero-result plans.
Per-source execution metadata competes with input, operator state and output
for the existing allocation. Each producer is consumed once; declared repeated
scans use the replay owner below.

Repeated source references must consume a caller producer once per execution.
Any retained replay data needs explicit native ownership, reservation and,
where admitted, query-local native spill with quota and cleanup. Calling the
producer again or silently switching to unbounded resident intake is forbidden.
Different projections on self-join sides must retain the complete required
source schema and exact values.

The replay implementation retains compact native arrays under the existing
query allocation only for sources declared for reuse. The complete static plan
identifies repeated scans before demand. Dynamic frontends must declare reuse
before their first schema-discovery stage, because a callback can resolve several
subtrees before returning its final plan. SQL already retains its complete parsed
declaration and resolved source mapping and makes that declaration automatically,
including distinct SQL aliases that resolve to the same source URI.
An undeclared second use after a single-pass source has started must fail rather
than calling its producer again. Ordinary one-pass pivots retain only completed
pivot state and do not acquire a replay cache.

With explicit spill permission, use the existing stable native ordering/run
store with an empty key list: original ordinals and earlier-run tie precedence
preserve source order. Complete run handles can be retained without keeping an
open reader; each replay opens a bounded reader under the same quota and query
allocation. Preserve unobserved values during internal copying, then apply the
normal semantics when an operator or public result observes them. The original
producer's buffer owners must be released before its next demand. Input counters
count the producer once; replay passes, rows, compact-copy bytes and actual
spooled sources need separate evidence. No replay data survives the execution.
Final completion must release replay owners and finish owned spill cleanup
before certification or publication.

The same-grant write/reopen acceptance exposed a terminal sink issue: an ordered
join can emit one native row at a time, and the flat writer retains one credited
footer entry per submitted array. An 8,193-row replay result therefore exhausted
a 16-MiB grant through fragment metadata even though streamed delivery fit.
The writer batches small produced arrays using Vortex `ChunkedArray` and the
existing credited native payload copier. This is `use_vortex_native_provider`
inside the existing native sink, with no new file representation or executor.
Pending fragments are bounded by 128 rows, 256 KiB and 128 arrays; larger arrays pass
through unchanged. Only the already admitted native payload-copy schemas are
batched. Copy bytes and actual adapter scalar-leaf constructions are reported,
including repeated sizing passes and null/default leaves. Scalar counts exclude
index/offset/validity metadata, raw buffer copies and provider internals; the
sink boundary reports when adapter scalar copying occurs. Full dtype/value/order must survive, and
failure or cancellation must release pending owners without publishing output.
No replay-block resizing or increased grant substitutes for that acceptance.
The same fixture then exposed readback admission: requesting 257 result rows
still read an 8,192-row file range before splitting it. File reads feeding
JSON/Arrow materialization now honor the downstream batch demand before native
payload work. Pinned Vortex `ScanBuilder::with_natural_splits` retains only the
two file endpoints under a fixed metadata reservation; `RepeatedScan::execute`
clamps them to each requested range. One-row delivery therefore does not create
full-file per-row split metadata. Native array retention
and delivery keep the existing 8,192-row source work units: prematurely splitting
them duplicated encoded buffers during cross-session transfer. The complete
compact-transfer regression keeps its original physical-copy limit. Dynamic
schema discovery retains its internal native work units before terminal delivery.

This reuses pinned Vortex arrays, native file serialization and the already
admitted `QueryRunStore`; it adds no row representation, file format, dependency
or external executor. Acceptance must cover different side projections, exact
rich values, null/empty/tie behavior, multiple passes and sources, dynamic-stage
reuse, resident denial versus same-grant spill completion, cancellation, producer
and consumer failures, corrupt runs, quota denial, process death/cleanup/restart,
complete Vortex write/reopen and final-owner credit release. The focused replay
checks and complete public family acceptance pass; final results are below.

Dynamic pivot must use the existing execution-time native schema discovery and
completed pivot state. It does not inherently require source replay. Admission,
source completion, downstream binding, cancellation and final certificates must
cover that first execution as well as subsequent result consumption.

The execution binder must retain the prepared streaming schema slots in their
original order. Before binding a dynamic subtree, check its admitted operator
shape and declare any repeated use before its first demand. A resolved native relation
is an execution-scoped reference whose ownership is already checked by the
existing binder, not a second request to the producer. Completion still checks
every declared streaming source independently. Discovery may consume an input
before a downstream schema-dependent error becomes knowable; that error must
abort the transaction without a final success report or published destination.
Unsupported stateful/subquery families remain explicit refusals.

The focused dynamic implementation now passes 19 discovery/lifecycle tests and
24 native pivot-spill tests, including a streaming pressure fixture with 24,580
input rows and 6,145 fully checked output rows. The no-spill execution denies
at the same 16-MiB grant under which explicit native spill completes twice.
The public Python/SQL cohort passes 61 dynamic cases plus 41 input/preparation
regressions. Those cases include successive pivots, two streamed pivots joined
downstream, empty domains, sum/count/mean/min/max/first-unique cells, late failures and
exact uint64/decimal/timestamp/binary/list Vortex write/reopen values and schemas.
The ten public dynamic pressure/failure cases also pass, including exact complete
streamed results, native write/reopen, real run corruption, quota exhaustion,
late producer failure and early consumer close. Native ordinary/streamed pivot
process-death tests preserve live/unknown owners, clean abandoned owned runs and
restart from fresh input with complete exact results.

Replay coverage includes all seven join kinds; different projections/predicates;
nulls, empty input and draining zero limits; three scans and two repeated sources;
window and dynamic-pivot composition; and fresh executions. The public cohort
passes 62 replay cases plus 86 input/dynamic regressions. Exact admitted integer
widths, binary, decimal, temporal and nested values survive actual replay runs
and complete native write/reopen. Native failure and process-death tests check
owned cleanup, original-producer release and preserved foreign files.

The ten public replay pressure/failure cases pass with 8,193 rows of 2,048-byte
text. Resident replay is denied under the same 16-MiB grant where native spill
delivers and writes/reopens every value in source order. Actual query runs,
corruption, quota exhaustion, producer failure and early close are checked.
Complete acceptance passes 757 public cases across all 19 input families,
3,219 default-workspace tests and 4,145 native-feature tests. The native suite
has 27 intentional exclusions: three non-Unix cases run by Windows CI and 24
manual benchmark, fixture or workload-dependent checks. All 129 Full43 query
executions pass complete result, schema, certificate and process-cleanup checks.
Formatting and default/write/read feature Clippy gates pass. The
[acceptance report](../benchmarks/native-stream-input-composition-2026-10-13.md)
records timing, identities, failed attempts and claim boundaries.
Documentation, generated guide checks and desktop/mobile/search review pass.
The [portable evidence index](../benchmarks/evidence/native-stream-input-composition-2026-10-13.json)
binds the accepted source, executable, complete results and verification records.

Acceptance includes focused ownership/failure regressions, actual Python/SQL
transactions, mixed compatibility inputs, complete native write/reopen results,
memory denial and spill/recovery where applicable, required workspace/native
checks and final Full43 regression. Successful prefixes, placeholder payloads
and synthetic spill reports do not satisfy those gates. Version 0.5.1 and
published packages remain unchanged; whole PERF and CG gates remain open.

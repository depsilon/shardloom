# Performance implementation and retain/drop evidence

Status: retained scoped batch validated. Numeric compression is retained by
explicit maintainer direction; native numeric-consumer corrections are integrated
for its actual encodings. Frozen C7 passes all 129 full43 value checks, with a
9.55% lower best-of-three total and a 7.02% higher geometric mean than baseline.
Final resident, bounded memory, native-output, held-out and numeric-spill checks
pass within the scopes below. Earlier candidate failures and tradeoffs
remain part of the record. This is not an official ClickBench rank, a release,
or completion of all thirteen PERF packets.

The [structured evidence bundle](perf-drop-ship-2026-09-05.json) owns frozen source
and binary hashes, raw-run references, complete matrices and final decisions.
The local source manifests and summaries below are its evidence inputs. The
[implementation record](../architecture/performance-overhaul-implementation-2026-09-05.md)
and [phase plan](../architecture/phased-execution-plan.md) distinguish completed
scopes from remaining family-wide acceptance.

## Comparison boundaries

All large runs use the same official 99,997,497-row Parquet source, guarded
local-only storage and exclusive runner locks. Host: macOS 26.5.1 arm64,
10 logical CPUs, 17,179,869,184 bytes physical memory. Ingest requests two workers;
queries request twelve workers and 24 GB memory. Requested settings do not change
the host's actual capacity. The fixed 4 GiB process-memory target was explicitly
removed; observed RSS is not an allocator admission limit.

Native process wall time includes process creation, required publication and
verification, complete public output and exit. CPU service work is recorded
separately and may overlap wall time. Page-cache residency is uncontrolled;
these runs have no query-answer cache. No builds or other ShardLoom benchmarks
overlap timed runs. Foreground host activity is not a controlled production load.

Full43 comparisons check complete typed outputs and explicit no-fallback fields.
They are regression comparisons, with the separately recorded independent Q20
reference; they do not make every retained result an independent oracle. The
best-of-three sum, hot sum, geometric mean and all raw samples describe different
aspects of performance. A lower sum does not cancel slower short queries.

| Snapshot | Source revision | Role |
| --- | --- | --- |
| Baseline | `62b63b9244f6fb98364a5f66847a2867ebf30098` | Frozen public comparison control. |
| C3 | `b3bb15adf4c0e74dac498000d78b931a9ca80674` | Resident ownership, bounded ingest, native sink and numeric query spill. |
| C4 | `3f82941ee9e1cea8c7134ad5f9202bee4f7a94cd` | C3 plus actual bounded count workers; rejected serial fused count removed. |
| C5 | `f1929a85cd0514f253da15738f67af3225b59bf9` | Numeric storage compression and physical inventory on C4, with corrected writer timing. |
| C6 | `16098c7eb15726f6d5cb4b8e1d5ffe3ec8e2f20c` | Complete-key partition count reduction; targeted Q34/Q35 measured on the original artifact. |
| C7 | `3c7ea538d9c7d240d03be982390b65b9f0c6dc88` | Integrated typed numeric consumers, partition reduction, deferred query inventory and resident/ownership corrections; full43 on the retained C5 numeric artifact. |
| Text experiment | `3d70119a7e0f89e4b14ef50b2dac4e381c9e69ac` | Isolated text-zoning candidate; not integrated or retained without lifecycle evidence. |

Later combined-source fixes must be identified separately in the final bundle;
their validation cannot be attributed retroactively to an earlier binary.
C7's frozen source and binary are recorded in `candidate-7-source.json`; its CLI
SHA256 is `d9faf7b2ae68c6f1b7fea6f573115950c256ba9091b663fbcf4f9dd7ef71a65a`.
The full43 run uses C5's 18,643,482,956-byte artifact, not a newly timed C7 ingest.
The final public and native checks below use this frozen CLI/library. The later
`9350cec1` memory-file example only corrects its explicit output bound; it does
not change the CLI/library measured in C7.

## Durable ingestion and stored representation

| Snapshot | Complete process seconds | Peak RSS bytes | Artifact bytes |
| --- | ---: | ---: | ---: |
| Baseline | 155.098148 | 10,954,833,920 | 38,147,848,068 |
| C2 | 158.326257 | 11,347,263,488 | 38,147,848,068 |
| C3 | 99.175058 | 3,428,319,232 | 37,846,260,172 |
| C5 numeric | 98.344770 | 3,188,228,096 | 18,643,482,956 |

C2 did not demonstrate an ingest improvement and retained the baseline artifact
SHA. C3 changes the physical policy: one admitted source-batch subtree completes
before the next, retaining native-buffer credits through writer use and
coalescing within each source batch. C5 adds numeric compression after the last
canonicalizing repartition. Its smaller artifact and RSS are measured local
outcomes, not proof of a complete global allocator or universal query-speed gain.

Historical ownership caveat: these frozen measurements do not prove retention
of root `LayoutRef` credits through provider footer completion. The strategy-owned
lease could drop before Vortex 0.85 finished the footer. The later correctness
fix moves that ownership onto the returned root's `LayoutChildren` to cover the
longer lifetime; its validation is recorded separately. Native input-buffer
ownership evidence remains a separate proof. No frozen result, source identity
or acceptance status is changed, and no new performance result is claimed.

The remaining independent publication readback/checksum is preserved. No required
validation or first-query preparation is moved outside the ingest clock. Original
reader/input owners, provider scratch and allocations bypassing the native host
allocator remain outside admitted copied-buffer accounting.

Actual persisted-array inspection read every Flat reference through the native
serialized-array reader. Both C3 and C5 have 118,864 layout nodes and 82,908
unique Flat segments. C3 contains 191,196 physical array nodes; C5 contains
261,361. These are actual decoded metadata trees, not schema-derived encoding
predictions. The inspection does not canonicalize their values.

| Data column | C3 referenced segment bytes | C5 referenced segment bytes | C5 observed array encodings |
| --- | ---: | ---: | --- |
| AdvEngineID | 200,039,040 | 2,087,972 | bitpacked, frame-of-reference, constant, primitive, run-end, sparse |
| UserID | 800,023,968 | 376,451,780 | bitpacked, frame-of-reference, primitive, run-end |
| ResolutionWidth | 200,039,040 | 75,808,428 | bitpacked, frame-of-reference, constant, primitive, run-end |
| EventTime | 800,023,968 | 247,164,412 | bitpacked, frame-of-reference, primitive |
| URL | 2,213,253,092 | 2,213,253,092 | Zstd; unchanged text control |
| SearchPhrase | 320,181,624 | 320,181,624 | Zstd; unchanged text control |

Column byte totals are non-additive when segments are shared, and exclude footer,
padding and postscript. Exact provider encoding IDs and auxiliary roles are in
the structured inventories. Inspection ran separately from timed ingestion and
queries with an explicit 64 GiB requested-segment limit. `--summary-only` omits
per-reference detail from emitted JSON while preserving complete inspection,
actual encoding sets, counts, limits and artifact SHA; full detail is reproducible
without that flag. Existing storage/log guards are unchanged.

C3 artifact SHA256: `9c282541ffbdc02f5502189cdf02ac15dd88c3e36d49742fb170effebe67c8e5`.
C5 artifact SHA256: `93acc7b9bbabed1f6e15a91aeacda45637bd5d6c5fed26e9b2052bf9b77e84f2`.
The baseline SHA256 is
`6777eb4deea57cea7d83e772b3af4db2ebd77f003c38c1997ee0aadf02071c97`.

## Query tradeoffs and setup cost

| Full43 comparison | Passed runs | Best-of-three sum, seconds | Hot sum, seconds | Geometric mean, seconds |
| --- | ---: | ---: | ---: | ---: |
| Baseline / baseline layout | 129/129 | 145.598184 | 145.789386 | 1.019390 |
| C4 / C3 layout | 129/129 | 136.469017 | 136.496707 | 1.236274 |
| Initial C5 / numeric layout | 129/129 | 196.321995 | 197.082914 | 1.576350 |
| C7 / retained C5 numeric layout | 129/129 | 131.686635 | 132.309902 | 1.090906 |

C7's best-of-three total is 9.55% lower than baseline, while its geometric mean
is 7.02% higher. All 129 samples total 399.893906 s. This is a workload tradeoff,
not an overall superiority claim; several short queries remain slower than the
original-layout baseline despite recovering much of the C4/C5 setup regression.

C4 improves the sum while worsening the geometric mean. Large exact count
queries Q34/Q35 improve, but several short calls have approximately 0.08–0.10 s
more fixed cost. Selected minimum native process times make that tradeoff clear:

| Query | Baseline seconds | C4/C3 seconds | C7/C5 seconds |
| --- | ---: | ---: | ---: |
| Q2 | 0.026250 | 0.112778 | 0.040698 |
| Q17 | 15.877899 | 15.075720 | 16.155425 |
| Q34 | 13.940867 | 10.900442 | 4.820260 |
| Q35 | 13.892109 | 10.607535 | 4.543462 |
| Q37 | 0.159937 | 0.248509 | 0.184215 |
| Q39 | 0.060681 | 0.152447 | 0.083957 |
| Q41 | 0.046681 | 0.140640 | 0.069254 |
| Q42 | 0.029268 | 0.121639 | 0.056281 |
| Q43 | 0.037236 | 0.135182 | 0.057720 |

The setup explanation is source-grounded but not a fully isolated timing claim.
Q2 run 3 records control-plane time of 8,082 us versus 70,191 us, while scan
splits change only from 856 to 866 and evidence collection from 738 to 997 us.
Footer segment count increases from 34,371 to 82,908. In the measured C4/C5
query path, `local_primitives.rs`'s `read_local_vortex_scan` measured file open,
session/binding and `VortexLocalPrimitiveEmbeddedLayoutReport::from_file` before
iterating the scan. That report recursively visited the complete layout tree
through `collect_vortex_local_layout_encodings`. C3's `BoundedIngestLayout`
wrote a complete child tree per source batch, increasing the work required by
that historical metadata traversal.

The pinned provider compounds that cost: `ViewedLayoutChildren::child` lazily
constructs and caches each requested node, clones read contexts and builds child
metadata. A whole-tree evidence walk therefore forces otherwise unused columns'
layout nodes into memory before a one-column query. No quadratic behavior has
been established. Deferring or reusing inventory must preserve truthful evidence
and source-generation ownership, rather than reporting a partial walk as complete.

This supports expanded-footer/control-plane work as a principal fixed-cost
regression; it does not assign all 62 ms of additional setup to one function.
Q37–43 show similar added latency with unchanged maximum chunk rows in six of
seven cases. The integrated C7 correction records root-only query inventory with
an explicit deferred full-tree scope. Complete traversal remains available for
explicit metadata inspection; it is not reported as completed during ordinary
query setup. The measured C7 calls combine this correction with numeric-consumer
and partition changes; the table does not isolate the inventory correction's cost.

The initial C5 run preserves all 129 complete-value checks and 608.884225 s
across all samples. Q17 takes 45.910274, 44.203076 and 40.051651 s, exposing an
expensive encoded consumer despite the storage gain. Numeric compression remains
retained under the maintainer's explicit instruction. Native typed accessor
corrections and their materialization evidence are integrated. On the same C5
numeric artifact, C7 Q17 takes 16.155425, 16.224298 and 16.338160 s. This recovers
the initial 40–46 s regression but remains above the baseline's 15.877899 s best.
C7 Q34 takes 4.820260, 5.265181 and 5.218992 s; Q35 takes 4.543462, 4.768852 and
4.596438 s. C6's separate original-artifact control below remains the clearer
comparison for partition reduction alone. No earlier raw sample is replaced.

## Accepted partition reduction on the original artifact

C6's targeted Q34/Q35 run uses the original 38,147,848,068-byte Vortex artifact,
with identical source-generation metadata to the baseline and C4 controls. All
six complete outputs pass. This comparison isolates a query-runtime change from
the C3/C5 storage-policy changes; it is not a full43 result or final numeric-layout
acceptance. All three samples are retained below, in run order.

| Query | Baseline seconds | C4 seconds | C6 seconds |
| --- | --- | --- | --- |
| Q34 | 14.218782, 13.940867, 13.978248 | 12.403626, 10.570235, 10.346895 | 5.544236, 4.849633, 4.662216 |
| Q35 | 13.892109, 13.964439, 13.967196 | 10.908500, 10.981726, 10.761406 | 4.868039, 4.653582, 5.063076 |

C6 moves complete-key reconciliation into 64 logical partitions on the shared
workers. In Q34 run 1 it commits all 99,997,497 rows and reconciles 18,342,019
groups before final partition selection. Nine compute workers are active at
peak, within an applied ceiling of ten including the caller. The bounded final
caller reconciliation is 1.333276 ms, versus C4's 10.634861 s caller all-key merge.
No local top-K discards keys before complete partition reconciliation. All six
C6 runs report zero native handoffs and retry jobs.

Q34 run 1 records 42.500414 s summed worker-closure elapsed, including waits and
nested work; it is neither CPU time nor an exclusive wall phase. Its 17.468207 s
partition reconciliation and 8.379639 s partition-lock wait are nonexclusive
service totals, while the caller's 4.683334 s join wait overlaps worker work.
Do not add these fields together or subtract them from the 5.544236 s process
wall. Measured user plus system CPU is 30.898638 s.

The shared reservation peak in that run is 7,027,336,640 bytes, covering admitted
native buffers, partials, partition vectors and growth overlap. It does not cover
every provider owner, output map or process allocation. All measured C6 RSS peaks
are retained separately:

| Query | Run 1 peak RSS bytes | Run 2 peak RSS bytes | Run 3 peak RSS bytes |
| --- | ---: | ---: | ---: |
| Q34 | 5,869,993,984 | 6,205,603,840 | 6,917,718,016 |
| Q35 | 6,976,421,888 | 6,886,309,888 | 6,895,747,072 |

The accepted scope is exact single non-null UTF8 key COUNT(*) and its ordered
limit. Composite, nullable, distinct and floating aggregate families and pressure
handoff costs remain separate work. C7's combined full43 result above does not
broaden the C6 implementation's admitted family.

## Public results, memory and acceptance scope

The paired C3 native-sink run has 16/16 complete reopen records, and its held-out
operator/public run has 640/640 accepted records across requested worker counts
1/2/4/8/12. Both totals include baseline controls and warmups; they are not counts
of fresh C7 checks. The raw runs are `native_output_20260905T204615573568Z` and
`heldout_operators_20260905T204926998804Z`, using C3 binary SHA256
`c82a3e32c9c6f15bf3fe1a0a734ba7d3b26a780b6832e7123c495f6e88243e52`.
These historical checks remain separate from the fresh C7 acceptance:

| Final check | Accepted records and boundary |
| --- | --- |
| Public resident call paths | 744/744 complete outputs: four cases, three timing surfaces, baseline/candidate, 30 samples plus one warmup. |
| Held-out operator matrix | 640/640 at 4,096 rows: 16 cases, requested workers 1/2/4/8/12, baseline/candidate, three samples plus one warmup. Independent Python integer/set/group/order expectations. |
| Native array output | 16/16 complete reopened outputs across two output sizes, baseline/candidate and three samples plus one warmup, from a 40,000-row fixture. |
| Numeric query spill | Baseline and C7 return all seven independently expected exact integers after offset 123,456 over 131,072 rows; C7 performs actual native spill and completes owned cleanup. |

The held-out matrix checks renamed schemas, nullable fields, exact integer
extrema, overflow, skew, distinct, composite grouping, string length, sorting,
projection and empty results. Its small-integer floating sums are exactly
representable; equality checks do not use a tolerance. Requested worker counts
are ceilings, not utilization or scaling evidence. Two larger 131,072-row matrix
attempts stop at storage/log guards, the second after 259 accepted records and one
guard failure. Neither is a completed large matrix. Limits remain unchanged and
lossless archives preserve the partial attempts.

Native-output timing includes write, flush, native validation and publication;
complete independent fixture comparison follows the timed call. A frozen
candidate reader with logical-field lookup verifies both variants, because the
old reader's physical-Struct assumption cannot read every valid output layout.
This is a result/persistence check, not an independent engine implementation.

The spill call admits 4,194,304 operator bytes and 33,554,432 temporary-disk bytes.
It writes and validates 63 native runs, peaks at 3,944,448 admitted memory bytes
and 6,407,512 disk bytes, and reports cleanup completed. That reservation excludes
source-provider and final-output payloads; it is not process RSS. Aggregate,
distinct and join spill remain open. The comparison demonstrates exact completion
under the explicit spill policy, not a speed gain over an in-memory baseline.

### Public call latency

All values are milliseconds, in p50 / p95 / p99 order, using nearest-rank
percentiles. Each cell has 30 timed samples; pairs alternate sequentially between
baseline and C7 on one deterministic 32-row renamed/null/large-integer fixture.
Every output is compared in full. Worker startup is separate; Python import is
excluded. Fresh CLI time includes process creation through complete stdout/exit;
worker time includes request encoding/write and the response line; Python adds
public argument construction and typed-envelope parsing.

| Surface and case | Baseline p50 / p95 / p99 | C7 p50 / p95 / p99 |
| --- | --- | --- |
| Fresh CLI count | 5.978 / 6.215 / 6.269 | 5.723 / 6.097 / 6.456 |
| Fresh CLI projection | 5.912 / 6.084 / 9.141 | 6.069 / 8.225 / 8.248 |
| Fresh CLI filter | 6.030 / 6.444 / 7.992 | 6.068 / 6.396 / 8.081 |
| Fresh CLI empty filter | 5.891 / 6.827 / 8.967 | 5.992 / 6.228 / 6.769 |
| Worker count | 0.913 / 1.025 / 1.028 | 0.370 / 0.397 / 0.765 |
| Worker projection | 0.809 / 0.894 / 0.948 | 0.461 / 0.539 / 0.546 |
| Worker filter | 0.857 / 0.990 / 0.995 | 0.485 / 0.522 / 0.544 |
| Worker empty filter | 0.747 / 0.826 / 0.854 | 0.402 / 0.455 / 0.468 |
| Python count | 1.271 / 1.375 / 1.395 | 0.669 / 0.737 / 0.746 |
| Python projection | 1.111 / 1.206 / 1.208 | 0.774 / 0.853 / 0.878 |
| Python filter | 1.165 / 1.244 / 1.260 | 0.791 / 0.863 / 0.874 |
| Python empty filter | 1.035 / 1.150 / 1.157 | 0.679 / 0.757 / 0.800 |

All 248 C7 worker/Python records, including warmups, report exactly one source
open and completed-execution counts 1 through 31 for each retained request.
Prepared count and collection execute every call; no answer cache supplies
results. Framing, source mutation, changed requests and parse failures have
explicit cleanup tests. The result supports these resident calls, not a broad
fresh-process gain or a native Python binding. OS cache is uncontrolled; process
RSS and copied/decoded bytes were not measured in this public harness.

### Prepared Rust and typed memory latency

The prepared Rust example retains 1,000 raw samples per surface, plus warmups:
5,005 actual executions across three prepared sources. Complete array/JSON
values match each surface's initial native result; this is regression parity,
separate from the independent public fixture checks. Preparation, verification
and returned-result drop are excluded from these operation timings.

| Prepared Rust surface | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Metadata count, including admission/generation checks | 0.004125 | 0.004500 | 0.005125 |
| Projection to owned arrays | 0.018625 | 0.028041 | 0.047042 |
| Projection to complete JSON | 0.034458 | 0.054875 | 0.085334 |
| Filter/projection to owned arrays | 0.022875 | 0.025083 | 0.035958 |
| Filter/projection to complete JSON | 0.030333 | 0.034459 | 0.044250 |

Typed-memory timing includes borrowed-view creation, validation, native intake,
expression binding, admission wait, filter/projection and complete JSON return.
Session construction, caller fixture creation, reference verification and result
drop are excluded. Every foreground scalar is checked against independently
constructed values. Both profiles return eight complete rows.

| Typed-memory profile and load | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| 32 nullable UTF8/bool/f64/exact-i64 rows, isolated | 0.028625 | 0.044333 | 0.052666 |
| Same profile, mixed | 0.391542 | 0.451375 | 0.528250 |
| 4,096 rows, two i64 columns / 64 KiB raw values, isolated | 0.100583 | 0.144000 | 0.166125 |
| Same profile, mixed | 0.469166 | 0.497292 | 0.851583 |

Each row retains 1,000 raw samples. The mixed background filters/projects
16,384 native nullable-int rows into 8,192 owned rows through the same session,
admission gate and memory budget. Timed intervals overlap in 999/1,000 nullable
foreground calls and 1,000/1,000 i64 calls. Overlap means concurrent in-flight
calls, including queue wait, not simultaneous CPU execution. Background values
are independently checked once and row counts on every operation.

Both profiles peak at 395,792 admitted bytes under a 64 MiB limit, with zero
denials and zero owned live bytes after all results/sources drop. Accounting
covers native value/offset/validity buffers and JSON capacity; caller/parser
storage, array metadata, allocator-bypassing scratch and RSS are excluded.
The nullable fixture has 1,089 raw value bytes, 1,440 admission bytes and 1,085
copied numeric/UTF8 payload bytes; these have different documented scopes.
The i64 fixture has 65,536 raw/copied value bytes and 65,562 admission bytes.
These measurements do not establish a general sub-millisecond service guarantee,
a 100M-row mixed-ingest envelope or a general live engine.

### Immutable memory-file generation and publication

The final 16,384-row example at `9350cec11392447d5ed2b7600140bbac3a82d598`
passes complete query and all-row reopened-value checks. Only the example's
output bound changed after C7; `candidate-7-memory-file-generation-source.json`
identifies that source and binary separately. One warmup per variant precedes
100 alternating sequential pairs; all raw samples are retained.

| Query surface | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Immutable native memory-file | 0.324083 | 0.534458 | 0.657417 |
| Ordinary typed-memory source | 0.500625 | 0.853916 | 1.010625 |

Both timings include binding, native execution and complete JSON rendering,
excluding verification/drop. Typed intake takes 2.123125 ms once; native-file
generation takes 0.723042 ms, serializing one array with no dictionary build.
It copies 729,704 bytes during segment assembly. Query-phase counters report
202 memory-segment requests and 147,400,208 returned bytes, including repeated
reads, and zero source file opens. These counters precede publication/reopen.

Durable publication takes 12.614 ms, including independent SHA readback of all
731,240 file bytes and native validation. It serializes one footer, no arrays
and no dictionaries: the queried segment bytes are the published bytes. Complete
reopen/JSON collection takes 6.813 ms. Peak admission is 4,419,216 bytes, with
zero denials and zero live bytes after all owned objects drop. Provider metadata,
scratch and RSS remain outside exhaustive accounting. This bounded one-segment
prototype is not a general performance win or a compressed-ingest replacement.

The first large demo stopped at its 64 KiB JSON bound without publishing output.
The example now requests the same explicit 32 MiB bound as its ordinary-memory
comparison, with a passing maximum-fixture regression and focused clippy. The
failed attempt is preserved; no library or public CLI limit was relaxed.

## Attachment implementation decisions

| Supplied idea | Implemented or retained scope | Remaining or deliberately deferred scope |
| --- | --- | --- |
| Make numeric compression reach storage | C5 compresses after final coalescing; actual per-column physical evidence; C7 typed consumers pass full43 with the recorded total/geometric-mean tradeoff. | Broader codec/resource/lifecycle acceptance. Probe result reuse is unavailable through the native dictionary API. |
| Couple text intake, helpers and encoding | Existing codec preserved; isolated native text-zone prototype and exact nullable/Unicode tests. | Dictionary-preserving source/helper/consumer redesign and lifecycle comparison. Text zones are not silently included. |
| Real in-memory Vortex file generations | Typed native generation/publication and identity checks; 16,384-row complete query/reopen checks and explicit copy/serialize/read/open counters. | Broader generations and lifecycle workloads; no empty template supplies data statistics. |
| Carry ownership through writer retention | Copied native buffers and native allocator outputs share the artifact-local pool; bounded source subtrees. | Original source owners, provider scratch and bypass allocations; no RSS-wide coverage claim. |
| Move actual aggregate work into shared workers | Bounded exact non-null single-key Int64/UInt64/UTF8 COUNT(*) partials; C6 passes original-artifact Q34/Q35 and C7 passes combined full43. | Remaining numeric/composite/distinct families and pressure handoff costs. |
| Correct stage timing | Historical encode/write field now reports measured inclusive writer wall; numeric probe/compress/preserve work remains separate. | More granular directly observed provider spans; older synthetic residuals are not comparable. |
| Separate decode/morsel/zone/frame/write granularity | Existing row/byte limits and explicit within-source-batch writer boundary. | General decoupled costed policy; current boundary has measured footer/setup tradeoffs. |
| Preserve small-call latency during bulk work | Owned workers/results; 1,000-sample isolated and mixed native intake/JSON profiles with queue-inclusive timing and observed operation overlap. | Larger declared bulk-load/pressure envelopes; no unsafe removal of admission locks or generation checks. |
| Preserve checksum guarantee | Independent readback retained inside complete ingest time. | Any alternative writer receipt requires separately admitted equivalent guarantees. |

The text prototype's first pruning assertion confused logical segment request
registration with actual I/O. Pinned Vortex eagerly constructs projection
futures before awaiting pruning, so projected text registers payloads even for
later-pruned zones. The corrected test keeps complete full-row checks and hard
no-request assertions for unprojected filter-only text. Coalescing means neither
logical requests nor future polls alone are physical-byte measurements. The
frozen `3d70119a` candidate contains the corrected test; this packet records no
passing run of that revision's revised pruning assertion. The prototype remains
isolated pending focused validation and actual read-byte/lifecycle evidence.

Earlier count-only worker and serial fused-count experiments without demonstrated
benefit were dropped. The final bundle must retain their source/log identities
without presenting them as current execution. Proposed 15–20% goals in the
attachment are objectives, not promised results or additional mandatory scope.

## Validation and outstanding gates

Frozen C3 source records native-feature clippy, 1,389 native engine tests and
native CLI suites before commit. C4 records native-feature clippy and 2,898
CLI/Vortex tests across 80 suites, with one deliberate fixture-generation ignore.
C5 numeric validation passes 1,394 native tests and two inventory-example tests.
C6 passes native-feature clippy, 1,440 native library tests with one intentional
ignore, and a 131,072-row public SQL/DataFrame numeric-sort spill check. These
checks belong to C6's frozen source and do not substitute for later validation.
Variant switches exposed stale shared-target artifacts; final validation cleans
the affected package artifacts before checking a different source variant.

The final-validation logs record these successful checks:

| Check | Result | Revision attribution |
| --- | --- | --- |
| Default workspace, all targets | 3,403 passed in 102 suites | Final-validation working tree; log has no embedded revision. |
| Native CLI/Vortex, all targets | 2,947 passed in 82 suites; one intentional ignore | `4d95116d`, before the final session-reset refactor. |
| Resident worker after reset refactor | Eight passed | Frozen `3c7ea538`. |
| Default and native all-target clippy | Passed with warnings denied | Frozen `3c7ea538`. |
| Python performance-harness unit tests | 24 passed | Final-validation working tree; log has no embedded revision. |
| User-surface reference validator | Passed, zero blockers | Final-validation working tree; log has no embedded revision. |
| Workspace formatting and contribution governance | Passed | Final retained working tree; exact logs in the structured packet. |
| Memory-file example maximum-fixture regression and focused clippy | One test passed; clippy passed | Example-only bound correction at `9350cec1`; C7 CLI/library unchanged. |

The structured packet preserves exact commands, logs and snapshot caveats. A full
native-suite rerun at `3c7ea538` is not claimed: the broad run preceded the reset
refactor, followed by its focused worker tests and both final clippy checks.
The final read-only user-surface check also passes in
`perf-final-user-surface-check-2.log`.
The final scoped resident, output, held-out, memory and spill runs above pass.
Numeric storage and these checks do not
close global CPU/I/O/codec admission, remaining prepared/operator families,
general exact aggregation under pressure, shared spill, all sinks, fused physical
composition, native Python binding or broader held-out comparative gates.
PERF-13 remains a conditional compilation feasibility item. No extra runtime,
unsafe policy change, fallback engine, answer cache or release is authorized by
this evidence packet.

Raw local inputs live under
`/Users/dylan/LocalData/shardloom/clickbench-100m-uat/logs` and
`/Users/dylan/LocalData/shardloom/perf-drop-ship-20260905`:

- Ingest: `ingest_cli_uat_gated_20260905T193801Z`, `...T195430Z`,
  `...T210332Z`, `...T215037Z` (baseline, C2, C3, C5).
- Full43: `full43_20260905T201730795323Z` (baseline),
  `full43_20260905T212309166479Z` (C4/C3 layout),
  `full43_20260905T215646214043Z` (initial C5/numeric layout),
  `full43_20260905T223708789324Z` (C7/retained C5 numeric layout).
- Targeted Q34/Q35 on the original artifact: `full43_20260905T211956997943Z`
  (C4) and `full43_20260905T221519122149Z` (C6). The directory naming convention
  does not make these full43 runs. C6's binary SHA256 is
  `d722d96ce77279623b6de61595feed3fa61afb41f6ca69e197bca59898ac9524`;
  `candidate-6-source.json` records its frozen source and validation.
- `candidate-3-physical-encodings.json`, `candidate-5-physical-encodings.json`,
  frozen source/binary manifests and exact-target artifact verification/removal
  records. Large stdout is retained as verified gzip with an archive index.
- Final public calls: `resident_call_paths_20260905T224426143459Z`;
  held-out: `heldout_operators_20260905T225033466111Z`; output:
  `native_output_20260905T225311422828Z`; spill:
  `query_spill_20260905T225422574711Z`.
- `candidate-7-resident-latency.json`, `candidate-7-memory-nullable32.json`,
  `candidate-7-memory-int64-64k.json` and
  `candidate-7-memory-file-generation-fixed.json`, with their source manifests.
- `perf-final-format.log` and `perf-final-contribution-governance.log` under
  `/Users/dylan/LocalData/shardloom`.

The durable bundle identifies final additional runs explicitly. Do not replace
this record with a single favorable total or discard the slower-query evidence.

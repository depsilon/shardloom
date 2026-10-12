# Full-domain timestamp provider and native payload acceptance

Status: local source/native and installed-wheel checks, complete public/Full43
and fresh-UAT runs, and independent evidence verification have passed.
Hosted integration remains pending. This is the finite follow-up in the
[timestamp provider contract](../architecture/native-timestamp-provider-2026-10-11.md)
under PERF-07/08/11/12 and CG-19/20/21. Published v0.5.1 packages are unchanged.
The six remaining areas, eight investigations and CG-1 through CG-23 retain
their separate obligations.

## Behavior and provider decision

Timezone-free microsecond timestamps retain their entire signed Int64 storage
domain through native compression, exact available statistics and complete
file reopening. The writer no longer omits file-wide minimum/maximum statistics
merely because a timestamp field is present. Empty or all-null input does not
gain invented extrema. Ordinary fields in the same file retain their own
statistics. Root, flat and nested timestamps use the existing Vortex compressor,
layout, file writer, session and explicit concurrency. This restores statistics
in the ingest/preparation writer that omitted them for timestamp-bearing input.
The separate computed-result writer still does not recompute file statistics;
that output-to-next-input gap belongs to
[`NATIVE-PERSISTED-REUSE`](../architecture/native-persisted-reuse-2026-10-11.md).

Two reviewed Apache-2.0 provider patches retain Vortex 0.85.0 and its format:

- [Timestamp scalar admission and display](../../vendor/vortex-array/README.shardloom.md)
  avoid a narrower calendar constructor for the already admitted storage domain.
  In-calendar formatting is unchanged; values beyond that calendar render as
  exact epoch microseconds.
- [Encoded comparison validity](../../vendor/vortex-datetime-parts/README.shardloom.md)
  preserves timestamp nulls when combining days, seconds and subseconds. The
  existing native component comparisons remain in use.

The provider decision is `use_vortex_native_provider`. No temporal unit or
timezone semantics are added. External readers using unpatched providers are
not certified by this change. Arrow remains an explicit interchange endpoint;
no external execution engine, independent worker pool or compatibility
representation is inserted into native computation.

The first compressed-file measurement exposed repeated dictionary traversal
in an existing result-compaction boundary. That boundary already copies
selected values into allocator-owned output. It now resolves selected native
numeric storage and validity once through the existing `NativeNumericOwner`.
Integer widths, signedness and temporal dtypes remain exact. Decimal validation
and private movement of unobserved floating-point values keep their existing
paths. Selection still precedes conversion, and retained children preserve
their output and schema credits.

## Separate measured and final runtime identities

The measured release is built from
`d536bcd1c6d7dc2f7e3ba719b9ebf2f44d909aae`. Its
`native-release-artifacts-315.json` freezes all 2,108 selected Rust, Python,
Cargo, provider and license assets. They are checked before and after builds,
measurements and source gates. Documentation changes are separate from these
runtime identities.

The paired performance observations below belong to that measured release.
Later report/resource corrections do not inherit those measurements. The final
runtime is built from `20c3960d3386436a1d82288917750347f86b8294`, recorded in
`native-release-artifacts-320.json`. Its CLI and extension are newly built and
tested after the per-plan source-effect and admitted-allocation peak fixes.

`native-release-artifacts-324.json` reuses the exact final runtime artifacts
and records three verification-only source changes: the report reader and its
tests accept both existing session lifetime-scope labels, and the public runner
uses compact JSON for its summaries. All 2,105 other selected assets are byte
identical to the final build's 2,108-asset source set. Exact patches and both
source maps are retained. Native operations, fixtures, expected values, schemas,
resource grants and call sequences are unchanged by these harness corrections.

| Measured release 315 artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Release CLI | 74,968,128 | `be56d8c756c2a781db4c994d0e08d029af44b0a25a50114679dbeef7542ebc17` |
| Release extension | 74,545,984 | `f03cc16c81f3781416a1ebafbefb0ff09089564af48ab4f87441223845d5f425` |
| Installed local wheel | 48,688,297 | `b3f0120115489d472bdd63394e1bb25fd11a4481e393e22590a83c47d3ac40e1` |

The measured `native-wheel-proof-315` and the final isolated
`native-wheel-proof-320` each pass all 75 native integration
tests with no failures, errors or skips. Their tag is
`cp310-abi3-macosx_27_0_arm64`; actual loading is verified on CPython 3.12.14.
The static integrity phase's `load_verified=false` is followed by a separate
successful installed load. The notice bundle covers 384 registry versions and
four vendored packages, with 729 file records and 206 texts. This is local
source/package evidence, not publication or cross-platform certification.

| Final release 320 artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Release CLI | 74,968,192 | `2476d1edbdec5977b1d076d06bf317ffe345bb47a30af7b3c53b9164f62c334f` |
| Release extension | 74,546,096 | `8fe2362a04dfc8b547f063ca69edaea273daeecbb331df2d521e9fe5edc4d993` |
| Installed local wheel | 48,679,751 | `4aec1add9905f39da5e01a62b89816a835f2654c46fa3bb0f15a4fd45795ab61` |

The final public acceptance uses source identity
`a7f1177c46b1db00a4b5e2f8c742b7a146927a4bfd2afe8325e3e5550932e42d`.
This identity hashes the full current source map, including the three recorded
verification changes; it is separate from the original compiled source identity.

## Review correction after complete UAT

PR #1543 review identified an overbroad certificate statement: compression visits
the whole timestamp-bearing top-level nested field, including compatible
non-timestamp siblings. Only other top-level nested fields retain the uncompressed
preservation policy. The correction names those scopes explicitly; it changes no
physical writer algorithm. A new regression writes a mixed nullable struct and
an unrelated struct through all four compositions, inspects the serialized
sibling encodings, and compares every reopened dtype, null and value. It fails
with the previous certificate and passes with the corrected certificate.

The [review receipt](evidence/native-timestamp-certificate-review-2026-10-11.json)
binds the two changed source files to the prior 2,108-file source map. Formatting,
default workspace Clippy, all 3,217 default workspace tests, native workspace
Clippy and all 12 selected full-domain writer tests pass. The
[complete review packet](evidence/native-timestamp-certificate-review-2026-10-11.json.xz)
retains the source map, exact patch, checks, logs and failed regression. The first
regression command selected zero tests and is preserved without acceptance credit.
Its compressed SHA-256 is
`554416d397db888ed3cb24cfb218e0315c8fbd8519addafe3cc89f56f7279307`.
The earlier UAT and performance observations retain their original source and
binary identities; neither was repeated for this certificate-only correction.

## Complete writer and reopen comparison

Accepted cohort: `timestamp-writer-measurement-316`. Each of seven workloads
contains 131,073 rows, an ordered Int64 identifier, timestamp values and a
nullable numeric neighbor. The nested workload adds a nullable struct with
timestamp and nullable/empty/list children. Input generation uses integer
storage directly, including both signed endpoints, without calendar conversion.

Each fresh interpreter executes public native `Session.command` preparation
from Parquet to a new Vortex file, creates and completely collects a native
file plan, then requests and consumes the complete Arrow stream. The timer
includes those three stages and their required report/owner setup. Imports,
session construction, fixture generation, hashes, assertions, evidence writes
and close are outside that clock. Complete Vortex and Arrow outputs, raw
preparation reports and process observations are retained for every sample.

There is one preflight pair per workload followed by five measured pairs,
alternating order by workload and pair. No sample is discarded. An independent
verifier reopens all 84 outputs, checks their exact schema and all 11,010,132
result rows, and separately checks 917,511 oracle rows using integer-domain
expectations. Every report proves the shared 1-GiB grant, two admitted execution
lanes, no resource denial and no fallback execution.

The baseline is the earlier accepted `native-release-artifacts-300` and
installed `native-wheel-proof-300b`. All 1,318 original source files are checked
against their retained evidence packet. That implementation includes the earlier
uncompressed-timestamp policy and predates other native-interface review fixes.
Consequently this is a complete implementation comparison, not isolated
attribution to the timestamp patch.

| Workload | Baseline median workflow, ms | Candidate median workflow, ms | Baseline Vortex bytes | Candidate Vortex bytes |
| --- | ---: | ---: | ---: | ---: |
| Constant minimum timestamp | 10.986 | 11.491 | 1,197,846 | 149,158 |
| Constant maximum timestamp | 11.390 | 9.937 | 1,197,838 | 149,150 |
| Narrow range near minimum | 11.764 | 12.916 | 1,249,228 | 365,308 |
| Narrow range near maximum | 13.175 | 12.730 | 1,249,220 | 365,300 |
| Mixed signed domain | 14.498 | 13.837 | 1,301,518 | 303,598 |
| Ordinary epoch microseconds | 12.300 | 14.621 | 1,248,960 | 534,192 |
| Nested nullable timestamps | 1,297.664 | 182.027 | 5,345,665 | 1,516,537 |

Files are 57–88% smaller in these fixtures. The nested complete workflow is
about 86% shorter. Compression has a cost: the ordinary-timestamp workflow
increases by 2.321 ms, about 19%, while its file is 57% smaller. Its preparation
stage medians are 9.359 and 10.979 ms; requested columnar export medians are
1.130 and 1.856 ms. Stage medians need not sum to the median complete workflow.
Retain compression and restored statistics for the observed storage benefit
and nested-workflow improvement; do not claim every preparation or query is
faster. The small flat-workload timings also show ordinary run-to-run variation.

Native writer reservation peaks remain at roughly 258–260 MiB, with a maximum
of 272,094,040 bytes across the cohort. These are shared-owner reservation
observations, not process memory limits. Provider scratch and caller-owned
arrays retain their documented exclusions. Process lifetime peak RSS after
native work ranges up to 91,488,256 bytes; it includes interpreter/setup work
and is not directly interchangeable with reserved capacity.

## Diagnosis and isolated compaction comparison

The original `timestamp-writer-measurement-314b` passes all 84 complete-value
checks but exposes a nested regression: medians are 1.283132 seconds for the
earlier baseline and 1.477398 seconds for its initial compressed candidate.
Those samples remain intact. The subsequent crossover uses that same candidate
to read frozen old and new files; median native collection is 1.253025 versus
1.437573 seconds. A separate sampling profile identifies the compact-result
builder's repeated dictionary/scalar traversal. Profiling is diagnostic and is
not run alongside timed comparisons.

`timestamp-reopen-comparison-316` isolates the numeric-copy correction. Release
314 and release 315 read the exact same frozen compressed files, with all
committed source blobs checked for both builds. Their runtime source difference
is the selected native numeric-copy implementation and its regression test.
It uses one preflight and five alternating measured pairs for each workload;
every complete result is retained. The independent verifier checks all 24
samples and 3,145,752 result rows against 262,146 independently checked oracle
rows. Shared reservations return to zero after close in every sample.

| Same frozen file | Baseline median collection, ms | Candidate median collection, ms | Baseline median complete collect/export, ms | Candidate median complete collect/export, ms |
| --- | ---: | ---: | ---: | ---: |
| Ordinary epoch timestamps | 2.175 | 2.179 | 4.020 | 4.020 |
| Nested nullable timestamps | 1,448.568 | 151.805 | 1,450.855 | 154.057 |

All five nested pairs are about 89% shorter in native collection and exceed the
predeclared 10% retention threshold. Flat medians are effectively unchanged.
One flat candidate collection takes 32.410 ms against its paired baseline's
2.175 ms; it remains in the raw samples and median calculation. Its cause is
not established, and this cohort does not establish equal tail latency.

## Measurement and correctness boundaries

The host is Apple M5 with 16 GiB physical RAM, macOS 27.0 arm64, CPython 3.12.14
and PyArrow 25.0.1 for explicit interchange validation. Rust release builds use
the recorded Rust 1.99 toolchain and the `release-user-surfaces` feature. The
1-GiB/two-lane measurement policy is recorded separately from physical memory,
RSS and later regression policies.

Builds, tests, profilers and timed workloads run sequentially under the shared
workload guard. Output remains in local-only storage under unchanged disk,
workspace, log and deadline controls. Inputs are prehashed and writer outputs
are newly written before reopening; OS caches and desktop activity are
uncontrolled. No cold-cache, kernel-only, cross-engine superiority or tail-latency
claim is made. CPU, RSS and host-load observations accompany each sample.

Focused regressions cover scalar construction/display/order/metadata; invalid
storage and unsupported temporal contracts; constant/decomposed/compressed
arrays; all six comparisons in both operand orders; empty and null inputs;
full file reopen and conservative pruning; root, flat and recursive fields;
retained streams and writer compositions; cancellation, denial, previous-output
preservation and released credits. Numeric compaction separately checks 4,099
arbitrary selected rows through dictionary parents and temporal children,
signed narrow and maximum unsigned integers, null gather indices and child
ownership after the parent is dropped.

Historical failed harness runs, the rejected first candidate and all original
comparison samples remain retained. The final complete public run has its own
fresh execution and source identity; it does not combine earlier partial runs.

Completed source gates include formatting, strict default workspace lint,
3,217 default workspace tests and 1,145 reduced-writer CLI tests. The complete
native CLI/Vortex profile passes 4,068 tests across 92 executables, with 27
existing manual/platform tests ignored. Seven feature checks pass, including
native strict lint, native without writer, writer-only, lean, all-features and
Rust 1.96 lean/native checks. These counts overlap across profiles.

The final Python source run passes 746 tests, plus seven native-package tool
tests and seven license-tool tests, without skips. License bundle, version
source, package-channel self-test and diff checks pass. An earlier source-test
attempt omitted the optional dependency directory and failed; that environment
failure remains recorded. The dependency-policy check passes Cargo deny and
finds no fallback-engine runtime dependency. This is not a full release audit:
Cargo audit and pip-audit were not requested, and release advisory acceptance
remains in the release gate.

## Public regression report correction

The first broad public attempt passes 13 complete-result checks before its base
runner rejects `resident_source_opens=30` against a per-plan expectation of two.
The saved preceding writer reports 28 opens. Shared session ownership intentionally
preserves cumulative counts across queries, writers and output reopenings; the
merged worker regressions already require that behavior. This is a stale harness
assumption, not evidence that this plan opened its two sources 30 times.

Tracing the same boundary exposes a separate report defect: lifetime opens cause
a later memory-only plan to claim source I/O and an initial footer open. A new
file/memory/source-free worker regression reproduces that false claim. The corrected
report carries `relational_prepared_file_source_count` from its actual held file
handles, identifies `resident_counter_scope=shared_session_lifetime`, and derives
both side-effect flags from this plan. Lifetime resource owners and counters stay
intact. The worker regression and complete source/native gates pass after the
fix; the final public run also exercises the corrected reports.

The public harness now checks the exact per-plan file count, source effects and
lifetime-counter validity. Consecutive repeats must add exactly one completion
and no source opens; lowered-plan reuse assertions remain. Case declarations,
expected values, schemas, resource grants, writer formats and call sequences are
unchanged. The failed partial cohort remains retained and receives no complete
public acceptance credit. Measured release 315 retains its own timing identity;
the reporting correction is present in the separately rebuilt final runtime.

## Allocation observations and harness corrections

The second broad attempt found another scope mismatch: a shared session's
1,076,174,900-byte lifetime peak, established under an earlier 4-GiB declaration,
was compared with a later 1-GiB operation. The existing pool now also records
the peak since the last successful allocation admission, starting with all
retained reservations. Admission of an unchanged limit starts a new observation;
rejected admission preserves both peaks. Fixed pools retain lifetime scope.
Concurrent reservations and snapshots share the allocation's admission lock.
The additional observation changes neither retained ownership nor the limit.

Reports preserve lifetime history and expose the current allocation peak with
its own scope. Tests cover retained and transient owners, smaller and repeated
grants, rejection, concurrent reservations and complete operations. These are
native reservation observations, with provider-bypass allocations and process
RSS still excluded. A later report-reader correction accepts both pre-existing
writer and resident lifetime-scope labels while continuing to enforce
`live <= allocation_peak <= lifetime_peak` and `allocation_peak <= grant`.

The next public run completed all ten families but failed the unchanged
192-MiB log admission by 159,744 accounted bytes. Its original evidence remains
failed. Removing indentation from summary JSON preserved every parsed field
and all 32,497 cases while reducing those summaries from 106,697,827 to
89,042,849 bytes. No raw report, case, expected value or storage ceiling changed.
The fresh complete run then passed under the same limit.

## Final public and retained-file acceptance

The final complete public run validates 32,497 cases and 18,595,284 result rows
across base, unary, nested, pivot, pivot-pressure, typed, memory, frames,
frames-pressure and scalar families. Direct unary acceptance adds 202 checks
and 131,734 rows. The same frozen runtime also passes 87 typed-input, 442 streaming,
20 input-growth, five input-pressure, 48 batch, 19 format, 145 semantic and nine
golden checks. These are distinct acceptance families, not disjoint product
capability counts.

The retained 99,997,497-row Vortex input passes all 43 queries at each of three
repetitions: 129 complete results checked against the retained regression packet.
This observation uses the final CLI, a declared 24-GiB native budget and 12 lanes.
The input identity, complete query file, references, resources and every result
are retained. This is correctness acceptance on an existing artifact. The
separate fresh-ingest workflow below creates and queries a new native file.

The retained-file run sums to 384.398543 seconds of native process time across
all 129 calls. Selecting the fastest valid call per query sums to 119.511092
seconds; selecting the fastest of each query's second and third calls sums to
121.697075 seconds. These are
defined aggregates, not a separately observed end-to-end wall clock. They are
not a paired performance comparison.

The earlier retained-file observation showed Q24 near 3.24–3.30 seconds with
about 27 CPU seconds; the current observation is about 20.52–21.67 seconds with
about 21 CPU seconds. The examined configurable-session/prepared-sort scheduling
code is already present in main. This is evidence for a matched scheduling
investigation under PERF-03, not attribution to the timestamp patch or a claim
of general speedup. The follow-up must use the same input, resources and full
result checks, account for admitted provider/operator workers, and retain all
controls and samples.

The complete public packet contains 5,074,509,355 uncompressed bytes with
SHA-256 `c90698bbddd642b86e3b34033648ec16568734b70cf3bf450e957067c53ea902`.
Its 54,271,968-byte compressed form has SHA-256
`bebb133f7c05d35a1f70e18295fa6ae28bfeaa7f88566d89e75808ddd16ce2d2`.
The frozen finalizer reopens complete values and checks every retained resource
report. A separate streaming inspection initially double-counted authoritative
resource declarations and typed mirrors. The corrected inspector counts the
authoritative declaration once while validating every mirror, including invalid
values and reversed key/value order. Its 1,399 structural/mutation cases and
100 resource-report contract cases pass. The full corrected packet inspection
passes with 66,067 authoritative declarations, 132,134 resource-origin fields
and zero invalid reports, routes or fallback claims. It verifies the complete
decompression hash and every saved pressure row. Original helpers and failure
receipts are preserved; no native execution is rerun for this inspection
correction.

## Fresh ingest, queries and format workflows

The same frozen final CLI completes a separate fresh UAT. It reads the resident
14,779,976,446-byte Parquet source and produces a new 15,713,610,545-byte Vortex
file. Input count, native stream validation and prepared footer each report
99,997,497 rows. All 43 queries then execute three times against that new file;
all 129 complete results match the retained regression packet with the existing
finite-float tolerance. This is not a fresh independent SQL oracle or an
all-cell comparison of the large input and output files.

The source and destination generations remain unchanged through complete
post-run hashing, outside native timing. The source SHA-256 is
`a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`;
the newly written Vortex SHA-256 is
`3b6f97cff0294042530605195ab720905f36cde130f6ae850d146b0b28e6638a`.

| Measurement | Native process seconds |
| --- | ---: |
| Fresh ingest | 52.344952 |
| Query calls grouped by repeat 1 | 93.720268 |
| Query calls grouped by repeat 2 | 90.113564 |
| Query calls grouped by repeat 3 | 90.219601 |
| Sum of the minimum for each query | 88.623710 |
| Sum of the median for each query | 90.935459 |
| Sum of each query's minimum over repeats 2 and 3 | 89.138230 |
| All 129 query calls | 274.053433 |

The harness executes each query three times before advancing to the next query.
Repeat groups are sums, not chronological Full43 passes. Native process timings
include startup, complete output and exit. The guarded ingest and query stages
take 61.178698 and 307.227687 seconds, including their monitoring and validation.
Adding those durations does not produce an observed combined workflow wall clock.
Inputs were prehashed and query input had just been written; OS caches and
desktop activity were uncontrolled. This is an unpaired correctness observation,
with no speedup or regression-attribution claim.

The large-file stages use the declared 24-GiB policy and 12-lane maximum on the
16-GiB host. Maximum observed child RSS is 2,594,537,472 bytes for ingest and
5,788,286,976 bytes across the query calls; these are observations, not enforced
memory bounds. Q24 takes 14.82–15.66 seconds on this fresh file, with roughly
15 user-CPU seconds per call. That observation remains part of the separately
scoped scheduling follow-up, alongside the retained-file evidence above.

All 18 serial format cohorts pass: 1,408 comparison records, including 704
native records, across 22 declared workflows with 1,000 fact rows and 20
dimension rows. The selection covers all eight input formats with collected
results for raw and prepared input, then all eight output formats from CSV for
raw and prepared input. It is not a complete Cartesian input/output matrix.
Each requested output is reopened and compared in full. Independent pandas
values are a testing oracle; ShardLoom records execute through the shared native
engine. The workflow policy is 1 GiB and two lanes. Its guarded 485.043868 seconds
include fixtures, reference work, native execution, readback and validation, so
that duration is not a native query-performance score.

The fresh packet retains all 129 raw query reports and complete values, all 18
workflow reports, 11,311 process logs, source/build identities, executed helpers,
resource declarations and cleanup provenance. Its compressed size is 2,017,468
bytes with SHA-256
`592c6aea3ce920907b0da1d8ac77edec4404b79f2c193676f0db3285236b3e75`;
the 221,862,972-byte uncompressed packet has SHA-256
`1e9b482617399167980a9f1673e777f1f22a8771dbf3bc4c979947c1ba3f3672`.
Independent inspection verifies the complete decompression hash and all expected
query/workflow counters, with no invalid fallback, claim or private-path fields.
Its first attempt flagged ten archived source strings containing the privacy
detector's own literal. The corrected checker exempts only that exact code token,
still rejects real paths beside it, and passes all 74 positive/negative contract
cases. All other full-packet counters are identical to the first inspection.
Original helpers, the failed receipt and the unchanged packet remain preserved;
no native work or packet generation was repeated for this correction.
No package version or publication changed.

## Preserved evidence and replay

The [evidence manifest](evidence/native-timestamp-provider-2026-10-11.json)
links the immutable acceptance receipts, independent inspections and archive
transport. The [complete public packet](evidence/native-timestamp-provider-2026-10-11.json.xz)
and [fresh-UAT packet](evidence/native-timestamp-provider-fresh-uat-2026-10-11.json.xz)
retain their original compressed bytes and hashes stated above.

The source/measurement archive contains 68,056 members and 3,508,677,702 bytes
of original member content. Its seven source snapshots contain 13,965 files.
All four comparison cohorts remain intact, including the rejected first writer
candidate and the diagnostic crossover. Independent readback repeats all 216
complete result comparisons, totaling 28,311,768 rows. Every archived member's
size/hash, frozen source map, build/wheel identity, compaction and fresh-packet
provenance is verified. The separate public packet retains its own complete
stream inspection rather than receiving new execution credit from packaging.

The archive's 147,367,752 compressed bytes have SHA-256
`8e3071180bd9159c049f81885e34db732c5935aa56abdb848658affe7044813f`.
The manifest lists five numbered parts, each at most 32 MiB. Concatenate them in
that exact order and verify the complete archive hash before extraction. Splitting
changes no archive bytes; reconstruction is checked before publication.

The original independent verifier checked every member before reusing a variable
for a seven-field inspection object, which made its final summary report seven
members. A second streaming pass rechecks all 68,056 member sizes and hashes and
corrects that one field. The original report, executed verifier, correction helper,
guard receipts and unchanged complete-value results remain in the evidence manifest.

Replay helpers and complete commands are preserved in the packets and archive.
Bind their recorded local paths to new unsynced output locations and the named
source/build snapshots; do not overwrite completed run identities. Large resident
ClickBench inputs and compiled executables are identified by hash, not included
in this archive. Native reruns require rebuilding the recorded feature/toolchain
profile and supplying the declared resource policy. The archived sample checks
use PyArrow only as a complete-value testing oracle and explicit interchange reader.

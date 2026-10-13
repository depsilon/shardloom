# Encoded native transfers and independent producer intake

Status: local encoded transfer and independent Vortex IPC acceptance pass,
including independent packet inspection and the refreshed local installed-wheel
proof. The complete public regression and retained Full43 correctness evidence
remain accepted as recorded below. All four
[query revisit experiments](native-query-revisit-2026-10-12.md) are now decided;
their final cumulative and installed-package checks pass; hosted CI acceptance
remains pending. This
report does not declare the broader acceptance effort complete.

This finite unit implements the
[encoded transfer contract](../architecture/native-encoded-interchange-2026-10-12.md)
under PERF-02/03/07/11/12 and CG-19/20/21. Version 0.5.1 stays fixed. All six
remaining areas, eight investigations and CG-1 through CG-23 retain their
separate owners.

## Behavior and provider boundary

Native results and sources can enter the existing Arrow-compatible Python
intake without first exporting Arrow. Same-owner sources retain shared native
ownership. A different receiving allocation reconstructs admitted physical
arrays and copies their payload buffers under its own grant. Shared children
and buffers are deduplicated; source statistics and cached execution results
are discarded. The receipt separates encoded copies, native materialization,
metadata and explicit provider workspace. Later queries reuse the source and
prepared handles while still performing actual query execution.

Consecutive selections now use native slices. This removes a dense u64 gather
index from otherwise compact constant, dictionary, run-end and packed results.
For integer/boolean record fields, transfer compares deduplicated physical
buffer sizes with bounded native materialization before copying. Short packed
views and small chunk containers use the smaller representation. Floats,
decimals, extensions and nested values retain their physical strategy, so this
cost choice does not narrow their admitted value semantics.

The Vortex-first decision is `use_vortex_native_provider`. The existing pinned
Vortex 0.85 construction parts, arrays and public message format supply the
representation. A reviewed Apache-2.0 provider patch retains an opaque metadata
owner through clones, children, slices, parts and structural rewrites. Payload
buffers retain their own receiving credit when extracted. The measured common
array allocation grows by 16 bytes on this 64-bit target; the `ArrayRef` handle
size is unchanged. No serialized field or external execution engine is added.

The source-only `sl.from_vortex_ipc`, `ctx.from_vortex_ipc` and native
`Session.from_vortex_ipc` helpers accept complete immutable dtype and array
message bytes under explicit native resources. Combined transport is limited
to 64 MiB. A closed registry, bounded structural verification and coordinate
checks precede accepted construction. Wire statistics and dictionary assertions
are not trusted. Unknown versions, registries, schemas, compression and corrupt
coordinates fail explicitly. The contract lists the exact types and limits.

## Frozen build and methodology

Measured build 359 records 2,216 selected Rust, Cargo, Python, provider and
license assets at parent `249657ede5a3dafc6f59f9922e97457edd12a83f`, including
the uncommitted implementation. The refreshed build-367 release identity keeps
the same CLI and binding SHA-256 values and byte lengths as build 359; the
source overlay contains five corrected source/document files, and the verified
base archive remains unchanged. Documentation is outside the runtime source
map.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Candidate release CLI | 75,186,624 | `0547befa0f91246fdae3154158620d749af626991a5b62f6e9f6ab5a198daad8` |
| Candidate native extension | 74,993,152 | `cc29afabed3506c0059b2e522034046aa76108d7758687b79a075dba1ac9f820` |

The baseline is the retained accepted native extension from persisted-output
build 343. This compares named artifacts, rather than rebuilding current main
and calling it the earlier runtime. The local host is an Apple M5 with 16 GiB
physical RAM, macOS 27.0 arm64 and Python 3.12.14. Each receiver admits 512 MiB
and one execution lane. These are native admission limits, not whole-process
RSS guarantees. Inputs and libraries are prehashed; OS cache and ordinary host
activity are uncontrolled.

The frozen protocol has eight cohorts, five alternating baseline/candidate
pairs per cohort and one fresh interpreter per sample. There are no discarded
samples or warm-ups. Every sample executes a full query, a different limited
nonnull query and the repeated full prepared query. Complete values, schemas,
validity and order are checked against independent literal references. Producer
handles are dropped before receiving queries, intake receipts remain unchanged
across reuse, and native receiving reservations return to zero after final drop.

Transfer timing covers only receiving intake. The lifecycle starts with owner
and source creation and includes transfer, producer release, preparation, three
queries, explicit Python row exports, native reports and native cleanup. It
excludes interpreter/library loading, hashing, reference generation, validation,
compression and evidence persistence. Whole child-process observations remain
separate. Caller-owned Python objects are outside native reservation accounting.

## Paired retention result

All eight frozen retention gates pass. Expanding encoded cohorts must reduce
copied payload bytes by at least 20%; short and canonical inputs cannot increase
their copied bytes. Same-owner and generated-range transfers must copy zero.
Both transfer and lifecycle medians must remain within the greater of 5% or
5 ms of their respective baseline medians. These gates and all five samples
per artifact remain unchanged from the rejected complete attempt.

Times below are milliseconds. Each slash separates baseline and candidate.

| Cohort | Rows | Copied payload bytes | Transfer median | Lifecycle median |
| --- | ---: | ---: | ---: | ---: |
| Constant | 65,539 | 524,312 / 0 | 3.300 / 0.059 | 14.840 / 10.634 |
| Dictionary | 65,539 | 61,448,487 / 18,011 | 33.064 / 0.091 | 154.347 / 81.456 |
| Run-end | 65,539 | 524,312 / 18,432 | 3.640 / 0.111 | 15.899 / 12.050 |
| Packed integer | 65,539 | 262,156 / 41,600 | 3.482 / 0.070 | 15.338 / 10.755 |
| Packed prefix | 17 | 68 / 68 | 0.051 / 0.074 | 1.978 / 1.808 |
| Small primitive | 128 | 1,024 / 1,024 | 0.075 / 0.086 | 1.220 / 1.235 |
| Same owner | 65,539 | 0 / 0 | 0.005 / 0.003 | 13.530 / 12.730 |
| Generated range | 131,073 | 0 / 0 | 0.005 / 0.004 | 19.177 / 17.543 |

These are representation and retention observations on declared fixtures, with
five observations per artifact. They are not confidence intervals, cold-cache
results, general query speedups or comparisons against another engine. Zero
constant payload copies do not mean zero metadata allocation.

## Independent producer and IPC lifecycle

An independent, unpatched crates.io Vortex 0.85 Rust program produces four
cohorts: 4,099 mixed nullable rows, 65,539 encoded rows, 257 nested rows and an
empty typed record. Each of five producer processes writes both public native
files and separate dtype/array messages, then exits before intake. The
receiver drops transport bytes before three different/repeated native queries.
All values, schema and row order, unchanged intake receipts, three actual
executions and final zero receiving reservations pass in all 20 receivers.

| Cohort | Import median, ms | Receiving lifecycle median, ms | Initial IPC copy bytes | Encoded reconstruction copy bytes | Coordinate arrays / values checked |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mixed nullable | 0.246 | 2.511 | 136,703 | 136,471 | 1 / 4,100 |
| Encoded | 1.729 | 80.035 | 184,767 | 184,529 | 3 / 66,220 |
| Nested | 0.068 | 0.905 | 8,477 | 8,297 | 0 / 0 |
| Empty | 0.054 | 0.616 | 148 | 4 | 1 / 1 |

IPC reconstruction reports no native materialized rows in these cohorts.
Coordinate validation can execute encoded children; its counters cover explicit
validator calls and checked coordinates, rather than all provider work or
decoded bytes. Initial message copies and receiving encoded copies are separate
stages, not alternate names for one copy.

Each whole export/import/query/validation/evidence workflow takes approximately
2.22–2.25 seconds on this host. That clock includes the producer and all four
receivers plus hashing, validation, compression and host-boundary observations.
It is not a sum of the table's medians. The producer is a debug fixture utility
and the receiver is the release artifact above. No equivalent baseline IPC API,
IPC-versus-file speedup, producer peak-RSS bound or universal zero-decode claim is
asserted. Upstream Python's released public export surface was source-inspected;
its compiled package was not executed or added as a dependency.

## Validation and preserved failures

Independent measurement inspection reopens 312 compressed reference/result
files containing 10,408,143 rows. It checks all 80 paired samples, 20 IPC
receivers, 60 producer files, 350 execution reports, source/binary/protocol
identities, process completion and the reconstructed timing/copy gates.

Default workspace format, lint and 3,217 tests pass. Full CLI/Vortex native
feature targets pass 4,109 tests with 27 existing ignored tests. Every ignored
declaration and reason is checked against the parent commit: three require the
non-Unix CI branch; the other 24 are explicitly manual experiments or fixture
operations. These suite counts overlap. The focused 75 resident-source and
three selection tests are included in the feature suite; provider ownership
tests are an independent excluded-crate check.

The first transfer attempt completed 50 encoded/prefix samples before the small
primitive baseline rejected a shorthand dtype string. The corrected driver uses
the documented serialized native dtype; workloads and gates did not change.
The complete second attempt retained all 80 samples but failed four copy gates: run-end, packed,
packed-prefix and small-primitive. It exposed the dense selection index and
unhelpful copies of small physical containers. Native slicing and the
pre-admitted integer/boolean representation comparison address those causes.
All original observations remain retained; the accepted run uses the unchanged
complete protocol and thresholds. Focused test and lint failures and their
corrections also remain visible.

An earlier installed-wheel proof (build 359) stopped during license-bundle
verification because the notice bundle did not match the reviewed vendored
dependencies. Refresh 363 had already updated the Vortex-array
`provenance.local_changes` field and generated a new manifest identity. Its
wrapper then failed at the final Python gate: two release-report tests exposed
stale expected method-row count 114 against the current count of 115. Refresh
367 failed before the README wording correction because its new README pinned
the active provider text to “Vortex 0.85”; the version-source report and log
record that blocker. The later expected-count and README corrections are
covered by the passed 367b refresh, corrected version-source report, and fresh
installed-wheel proof. The source suites report 750 Python tests, seven
native-package-tool tests, and seven license-tool tests, all passing; these
separate suites can overlap in behavior and their counts are not additive
capability totals. The fresh wheel integration passes 79 tests with zero
failures, errors, or skips. Its license bundle contains 383 registry packages,
five vendored packages, 729 license-file records, and 206 unique license texts.
The earlier failed aggregate wrappers remain retained; none of these local
proofs constitute hosted CI completion or publication.

## Complete public regression and query timing concern

The frozen runtime passes 32,497 public checks comparing 18,595,284 complete
result rows and 202 direct unary checks comparing 131,734 rows. The same run
passes 87 typed-input, 442 streaming, 20 input-growth, five input-pressure,
48 batch, 19 format, 145 admitted-semantic and nine golden-workflow checks.
These families overlap in supported behavior and are not additive capability
counts. Every selected build asset and artifact remains unchanged.

The retained 99,997,497-row file passes all 129 Full43 calls. Complete native
process time sums to 524.463237 seconds; summing each query's minimum over its
three samples gives 168.525189 seconds. The actual guarded query stage takes
589.623093 seconds including supervision and validation. These are different
clocks. No fresh ingestion was performed for this unit. The unchanged retained
artifact is 15,713,610,513 bytes with complete SHA-256
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.

This run is slower than earlier unpaired observations. The slow run and every
result remain retained.
Neither the old roughly 70-second headline nor the later run is substituted for
a controlled comparison.

The targeted comparison uses the baseline and candidate CLIs on the exact same
retained file, with 24 GiB and maximum parallelism 12. It includes Q1, Q24, Q14,
Q16 and Q23, with five alternating pairs per query. Native wall time, user/system
CPU, child peak RSS and host load observations are separate. All samples and
complete result checks are retained. The frozen per-query median gate permits
at most the greater of 5% or 5 ms regression, with no discarded warm-ups or
automatic sample replacement. All 50 calls pass complete-value checks and all
five median gates pass. Native process times below are seconds; these five-query
results do not supply a Full43 comparative score.

| Query | Baseline median | Candidate median | Candidate limit | Decision |
| --- | ---: | ---: | ---: | --- |
| Q1 | 0.050690 | 0.051194 | 0.055690 | Pass |
| Q24 | 20.178673 | 18.119073 | 21.187606 | Pass |
| Q14 | 3.482324 | 3.524549 | 3.656440 | Pass |
| Q16 | 3.290022 | 3.270274 | 3.454524 | Pass |
| Q23 | 7.695503 | 7.941623 | 8.080278 | Pass |

The actual paired workflow is 346.576116 seconds, including host observations,
validation and log compression but excluding the initial/final complete input
hashes. The enclosing guard is 362.710706 seconds. Cache and ordinary host
activity remain uncontrolled. Q24 samples
range from 16.095 to 21.534 seconds across the two builds, so the table is a
predeclared regression decision rather than proof of a Q24 optimization.
The median per-sample CPU-to-native-wall ratio is 1.055755 for the baseline and
1.056017 for the candidate. Input identity checks complete without change;
all native processes exit and the UAT lock is released.

Q24 retains its PERF-03 scheduling investigation at this encoded-interchange
checkpoint. Both artifacts above contain the same scheduling path; their parity
does not close that investigation. At this checkpoint, configurable sessions
create no persistent provider drivers and prepared sort enters the source
without requesting temporary provider drivers. The subsequent finite query
campaign measures temporary drivers and scan granularity separately against
cumulative accepted baselines. Those later observations belong to that campaign,
not to the encoded-transfer comparison above.

The first independent public readback stopped after reopening the public
results because its supervised Python environment omitted the already installed
PyArrow dependency directory. Attempt 2 supplied the existing format
dependencies and restarted the complete readback; it did not combine partial
query cohorts. It passed and produced a 54,220,808-byte compressed packet
containing 5,073,534,384 uncompressed bytes. A full stream inspection timed out
at 1,800 seconds; a later attempt with a 3,600-second deadline failed. The
subsequent repair changed the inspector to use the frozen metadata paths and
count resource declarations at authoritative envelope fields once, while
continuing to check nested projections. The repaired 688-case inspector
contract and 100 resource-report checks pass. Independent inspection of the
unchanged packet then passed in
1,693.123966 seconds, validating the complete stream, values, claims and
resource reports. Its inspection wrapper first left a running coordinator
receipt and hit `FileExistsError` when it tried to create that receipt again;
the original writer failure was reproduced, and the finalizer wrote one
completion receipt after rechecking the saved identities. Query execution,
finalization and packet inspection were not rerun during that receipt repair.

The [portable evidence index](evidence/native-encoded-query-2026-10-13.json)
links the accepted source and complete public-result archives together with the
later query campaign. The source packet and query overlay preserve the measured
source versions, raw transfer
and IPC values, frozen references and drivers, provider fixtures, original
failures and build/process receipts. Complete public regression has its own
independently inspected portable packet. No executable, installed environment or
large ClickBench dataset is bundled. Local encoded transfer/IPC, packet
inspection and installed-wheel evidence are closed for this acceptance unit;
hosted CI remains pending. The four-candidate query campaign has three retained
candidates and one drop, with passing final cumulative native/Full43/package
acceptance recorded separately in its report. The broader optimization queue
retains its existing owners and is not closed by these finite local receipts.

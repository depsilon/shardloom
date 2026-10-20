<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite native projection-request deferral — October 20, 2026

Status: **rejected before timing**. Only one of four sparse targets passes the
prospective actual-read admission gate. Exactness and ownership checks pass, but
the observed read reduction is insufficient across the required cases. The
experimental provider patch and temporary observation modules are removed.
A copied-executable naming gap also limits concurrency-guard coverage; no
exclusive-workload, speed or positive-admission claim follows from these records.

This is the finite row-16 result under `PERF-03.regions`, not completion of the
broader region owner. RFC 0044, the 58 roadmap substeps and CG-1 through CG-23
retain their separate gates. Region-local reduction, ordered candidates,
preparation design and a newly admitted topology remain unproved by this work.

## Mechanism and frozen scope

The [count intake](native-region-projection-intake-2026-10-20.md) establishes
potentially avoidable projected-payload reads in the existing native provider.
It is followed by the separately frozen
[candidate protocol](native-region-projection-design-2026-10-20.md). Both
documents remain unchanged as prospective records.

The only runtime candidate changes pinned Vortex 0.85's private `FlatReader`
array helper. Filter requests remain eager; projected-payload requests wait until
their array future is polled. The existing split executor awaits its filter
before polling projection. Projection-tree construction, row-range conversion,
validation and the shared native decode path remain in their original locations,
including synchronous projection errors under an all-false filter.

The Vortex-first classification is `use_vortex_native_provider`. The experiment
uses the existing `SegmentSource`, layout reader, native scan, prepared source,
aggregate and resource owners. It adds no region model, scheduler, pool, cache,
runtime option, dependency or external-engine execution fallback.

Eight immutable native files each contain 131,085 rows, including a tail. Their
nullable I64 filter/payload columns have independently specified values and
ordinary, misaligned or skewed chunk geometry. The complete prepared aggregate
returns COUNT(*), COUNT(p), SUM(p), MIN(p) and MAX(p) for `f >= 100` at CPU grants
1 and 4 and a 64-MiB native-memory grant. Sparse zoned/plain files are targets;
all-true, all-false, mixed, nullable, misaligned and skewed files remain controls.

## Completed reads and decision

The intake passes 32 complete calls and independently checks 1,048,680 source
rows, all source values, schemas, geometry, actual read ranges and output values.
All eight target observations meet the prospective opportunity threshold. This
is an upper bound on potentially avoidable payload work, not an achieved saving.

The subsequent comparison contains 64 valid observed calls: two repetitions of
every profile/grant for both roles, with identical measurement source. The gate
compares the candidate's larger observed count with the baseline's smaller count
for each target. All four targets must reduce total operation bytes by at least
10% and 65,536 bytes, and rejected-projection bytes by at least 25%.

| Target | Baseline minimum total bytes | Candidate maximum total bytes | Baseline minimum rejected-p bytes | Candidate maximum rejected-p bytes | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Sparse zoned, P1 | 3,259,376 | 2,568,024 | 1,183,392 | 887,544 | Pass |
| Sparse zoned, P4 | 4,246,440 | 4,443,792 | 1,577,856 | 1,577,856 | Fail |
| Sparse plain, P1 | 3,485,056 | 3,221,872 | 1,282,008 | 1,084,776 | Fail |
| Sparse plain, P4 | 2,104,224 | 7,626,928 | 788,928 | 2,794,120 | Fail |

Counts include completed-read multiplicity. The packet retains both repetitions,
unique coverage, metadata/payload intersections, unattributed gaps and separate
preparation reads for every control as well. Coalescing and repeated-read counts
vary; these observations do not isolate their cause. No individual grant or
favorable observation is promoted over the required four-case gate.

The failed admission stops the experiment. The planned 240 timing workflows and
3,120 complete query executions were never run. Observer clocks are not engine
performance evidence. No threshold changes, sample replacement or unchanged
candidate rerun are used to obtain a different decision.

## Exactness, failure and ownership proof

Both roles pass the 16 ordered-value cases, checking every selected pair and
original natural-split batch boundary: 32 calls and 1,299,124 selected rows in
total. Natural coverage is independent of CPU grant. Additional checks cover
nullable I64/U64/F32/F64 values, integer extremes, signed zero, infinities and
NaN payload bits; direct all/false/empty masks; unpolled drop; eager construction
versus polled request activation; and explicit provider/decode errors.

Controlled layout-reader checks preserve synchronous projection-construction
errors under a zero-match filter, filter/projection failures, empty output,
started-pending drop and later-split progress behind a delayed ordered frontier.
Public checks reject changed source generations and pre-cancellation. These
controlled tickets prove scheduling and owned release, not physical reads or
process memory. Existing real blocking-I/O cancellation and failure-drain tests
remain part of the nine regression selectors: 25 tests per role, 50 executions.

Every completed observed query has exact values and dtype, one source preparation,
one completed execution, native certification, no Arrow conversion and no
external fallback. Managed reserved bytes and denied reservations are zero after
teardown. Native allocation is not an RSS guarantee. All owned process groups
are recorded drained and direct children reaped.

## Guard coverage limit

The existing concurrency guard recognizes `shardloom-`/`shardloom_` test names.
The copied executables were named `region-projection-830-baseline-r1` and
`region-projection-830-candidate-r1`, so another invocation of that guard would
not recognize those names. This was found during final review after the read
decision. The experiment retained serial orchestration, the ordinary storage
limits and supervision/drain of its owned process groups. That does not prove
exclusive workload isolation. No actual overlap is established by this review,
and its absence cannot be reconstructed from the retained owned-process samples.

The limitation is retained with the raw read rejection. It cannot support a
positive admission, a claim that any observed difference is solely caused by the
candidate, or a timing claim. Future qualification must verify each copied
executable's actual basename against the unchanged guard before observations,
using a recognized `shardloom-` prefix. Historical records and the failed gate are
not rewritten or rerun.

## Evidence and replay

The [comparison packet](../benchmarks/evidence/region-projection-packet-831-r1.json.xz),
[manifest](../benchmarks/evidence/region-projection-packet-831-r1.json.manifest.json)
and [standalone verifier](../benchmarks/evidence/package_region_projection_831.py)
preserve the complete intake, prerequisite and read records, frozen protocols,
schedule, sources, provider patch, compiler records and executable identities.
The packet SHA-256 is
`4225868b6c08d0ce9a9b6412d24a4c2c181c61684271b766fed685b560f149b0`.

Replay verifies five full source versions against Git plus archived additions
and patches: three intake revisions with 3,047 files each and both comparison
roles with 3,050 files each. Actual compiled local dependency inputs are
individually bound: 1,545 for each qualified intake build and 1,548 for each
comparison role. Both copied comparison binaries match their qualified optimized
builds and differ from each other. Twenty-six successful guards and four failed
setup/build/lint attempts are preserved; no query result is inferred from a
failed build.

The standalone verifier recomputes complete values, geometry, read intersections,
all prerequisite cases and the frozen rejection without an executable. Pass
`--verify` with the packet, `--scratch-root` with local unsynced storage, and
`--repository` with a checkout containing commit
`603e8ec163a0cbcb24bbf6712a9a4e311e46b187` to include source reconstruction.
Payload hashes and sizes are captured identities during portable replay;
the verifier explicitly reports `physical_payloads_revalidated=false` and never
fabricates retired native files. Reproducing physical reads requires rebuilding
the archived source and regenerating inputs under fresh guarded paths.

## Restoration and fixture correction

The provider and module declarations return byte-for-byte to the accepted source.
All four temporary Rust observation/proof modules are removed. The first fresh
native suite exposes an existing test-fixture collision: 3,040 tests pass, one
fails with `AlreadyExists` before its ordinary execution begins, and 30 existing
manual/opt-in tests are ignored. That failed receipt and freshly compiled
executable identity remain preserved.

The numeric-partition fixture previously used only process ID and wall-clock
nanoseconds as its filename. The retained test-only correction adds a process
sequence number while preserving exclusive creation. A fixed-timestamp test
creates two complete live fixtures, checks independent names and equal expected values, and
proves dropping either does not remove the other. Its module remains under
`cfg(test)`; production provider/runtime/API bytes are unchanged.

The repaired source passes formatting, strict workspace/native Clippy, all 3,235
workspace tests and all 3,042 native tests; 30 existing native manual/opt-in tests
remain ignored. These suites overlap. The fresh native build and all 3,027
validation-source identities are bound to the successful receipts. The existing
public executable and native library remain
identical; all 2,221 other files in their broader source binding match, with the
one test-only fixture difference explicitly recorded. This is a source/artifact
bridge to prior public acceptance, not a fresh installed-package claim.

The [restoration packet](../benchmarks/evidence/region-closeout-831.json.xz),
[manifest](../benchmarks/evidence/region-closeout-831.manifest.json) and
[closeout verifier](../benchmarks/evidence/package_region_closeout_831.py)
preserve the original fixture failure, correction, five successful final checks,
public source bridge and retirement receipt. The packet SHA-256 is
`1f293cb96ade31d6c4c5b4fbcb38983cf59c1fe29038d12fdf516309ed8a0c26`.
Its replay checks 3,038 unique source files against the base commit and applies
the exact fixture-only patch. Supply the comparison packet with `--comparison`
when replaying this packet with `--verify`, `--scratch-root` and `--repository`.

After full identity and closed-handle checks, 477 completed generated files
(242,507,137 logical bytes) are retired, including eight temporary native inputs
and both comparison executables. Complete compact replay passes before and after
retirement. The accepted public executable/library and current source inputs
remain unchanged; retired physical inputs and executables require regeneration.

The finite rejection leaves broader region, local-reduction, ordered-candidate,
preparation and topology gates open. A materially different mechanism requires a
new prospective design and evidence. No Full43, package, remote-service,
production or competitive claim follows from this result.

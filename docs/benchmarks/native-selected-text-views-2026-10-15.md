<!-- SPDX-License-Identifier: Apache-2.0 -->

# Selected native text views: bounded acceptance

The sparse Zstd filter now constructs native text views only for selected rows.
It retains the existing frame decoder and allocation owners. The frozen
70-call comparison passes its predeclared retain/drop gate: Q23's median engine
time falls from 3.253929 to 3.170805 seconds at requested parallelism 12, with
an improvement in all ten pairs. This is a 2.55% improvement for that measured
case. All four other parallel control medians are slower, within their original
limits; this evidence does not establish a general workload speedup.

Status: complete local acceptance passes, including all seven Rust/provider
gates, 80 direct Python tests, 757 public workflow cases and one Full43 pass.
The [portable evidence index](evidence/native-selected-text-views-2026-10-15.json)
records 2,305 verified members. PR #1556 is merged after 44 successful checks;
the [hosted packet](evidence/native-selected-view-hosted-2026-10-15.json) records
the installed-wheel and served-source proof. The production deployment
notification remains pending, as detailed below. This source change follows
the immutable 0.6.0 packages and does not publish a release.

## Shared implementation and exact work avoided

The [finite contract](../architecture/native-selected-text-views-2026-10-15.md)
extends the pinned Vortex 0.85.0 provider. ShardLoom's existing admission checks
still select sparse, all-valid UTF-8/binary filters, prune whole frames and
remap row indices. `ZstdData::decompress_slice` still decompresses the same
retained frames. A checked length-prefix walk constructs only the selected
`BinaryView` entries, followed by the safe native output constructor. Output
views and independently retained data buffers keep the existing shared credit.

The allocation regression selects four rows from two retained frames containing
16 rows. The baseline allocates a 16-view array and a four-view gather array;
the candidate allocates one four-view array. The regression fails on the old
implementation and passes after the change. Decoder payload bytes do not
decrease. This is less view construction and copying, with no zero-decode claim.

Unfiltered/dense decoding, null-bearing admission, codec parameters, serialized
layouts, scheduling and public routes keep their existing contracts. The dropped
October 13 full-view reconstruction experiment remains dropped. No alternate
query engine, query-specific execution path, dependency or new unsafe code is
introduced. All user surfaces reach this provider through the shared runtime.

## Frozen comparison

The baseline is the accepted native source from PR #1555, merged at `2b87cf7f`.
Its frozen CLI hash begins `ce3978515a7a`; the candidate begins `315e4f2708eb`.
The candidate's 2,259-file source manifest is frozen before timing, and the build
log proves that the changed vendored provider and native callers were recompiled.
The only differences from the earlier measured baseline are the selected-view
changes and four already-merged timing-harness files; the latter contain no
native runtime change.

Each sample is a complete public CLI call over the same resident 99,997,497-row
Vortex input, with a 24-GiB query allocation. Fresh native processes alternate
baseline/candidate order. The input is fully hashed before and after the cohort;
ordinary host activity and the shared OS cache remain uncontrolled. There is no
pacing, cold-cache claim, replacement sample or threshold adjustment. The host
is macOS 27.0 on arm64 with ten logical CPUs and 16 GiB physical RAM. Requested
parallelism and the 24-GiB admission policy are separate from physical capacity
and measured process memory.

| Case | Pairs | Baseline engine median (s) | Candidate engine median (s) | Median change | Improved pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q23, parallelism 12, target | 10 | 3.253929 | 3.170805 | 2.55% faster | 10/10 |
| Q23, parallelism 1, control | 5 | 8.008276 | 7.804710 | 2.54% faster | 5/5 |
| Q22, parallelism 12 | 5 | 1.225528 | 1.280279 | 4.47% slower | 2/5 |
| Q19, parallelism 12 | 5 | 5.115366 | 5.150072 | 0.68% slower | 2/5 |
| Q29, parallelism 12 | 5 | 5.808467 | 5.865302 | 0.98% slower | 2/5 |
| Q2, parallelism 12 | 5 | 0.144976 | 0.151266 | 4.34% slower | 0/5 |

The target requires a lower engine median, positive median paired saving and
at least nine improved pairs out of ten. Its process CPU median may rise at
most 5%; each control's engine median may rise at most the greater of 5% or
5 ms. Every case's RSS median may rise at most the greater of 25% or 256 MiB.
All six cases pass those frozen rules. Target process CPU changes from
12.279416 to 12.123697 seconds, and target RSS from 4,546,183,168 to
4,449,861,632 bytes. These measurements do not establish a process-memory cap.

Engine clocks govern the decision. Process, result serialization, remaining
process work, CPU, memory and supervision remain separately reported. An
independent replay reopens all six archives and 280 members, reconstructs the
70 raw engine/process observations and all 35 pairs, and matches every stored
median, paired saving and gate with no discrepancy. Complete values, schemas,
native certificates, grants, input generations and false fallback fields pass.

## Correctness and resource checks

| Check | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace, native-read and release-user-surfaces Clippy | Passed with warnings denied |
| Default workspace tests | 3,219 passed; none ignored |
| Native CLI/Vortex tests | 4,223 passed; 29 unchanged manual/platform exclusions |
| Complete vendored Zstd provider tests | 61 passed; none ignored |
| Focused native Zstd group | 55 passed; one existing manual cost test excluded |
| Direct native Python tests | 80 passed; no failures, errors or skips |
| Public workflows | 757 passed across 19 families |
| Full43 acceptance | 43/43 complete queries; 381 result rows; one call per query |

The six added provider tests cover all selection masks in small fixtures,
complete-value segmentation, nested slice offsets, nullable all-valid data,
UTF-8/binary, dictionary/no-dictionary decoding, invalid indices, unsupported
types/nulls, incorrect counts, malformed prefixes/extents and selected invalid
UTF-8. The native filter suite exercises every allocation refusal, retry,
source-owner survival, independently retained buffers, exact output and native
serialization. The removed gather allocation reduces the tested allocation
sequence from six stages to five.

All native checks use the frozen source and serialized workload/storage guards.
The packet preserves the expected baseline regression, the original stale
allocation-count expectation, the missing test-import compilation failure and
the older manual-inventory reference. Corrected tests and declaration checks
pass; no failing native command is silently replaced or omitted.

Public acceptance independently reopens 724 complete-value files containing
22,002 expected rows, 1,062 native envelopes, 20 pressure files containing
677,251 rows, and 136 native/compatibility artifact references. The complete
case inventory matches the accepted public suite. CLI and binding identities
match the same frozen source. These local direct tests do not establish an
installed-wheel or all-platform claim; the later hosted package proof is
recorded in the integration section below.

The Full43 preflight first stopped before any native query because its temporary
wrapper copied an obsolete log ceiling. The corrected wrapper uses the maintained
runner's existing 1-GiB retained-history and 256-MiB active-run policy, also used
by the accepted baseline. No log or dataset was deleted, no policy was raised,
and that refusal is preserved alongside the single completed query pass.

## Single-pass query acceptance

One sequential pass executes every query once, with requested parallelism 12
and the same input and admission policy. All complete values and schemas match
the retained native regression reference; finite floats use its declared
`1e-12` tolerance. This is regression evidence, not a fresh independent oracle.
The input is not reingested for this decoder-only change. Its complete hash and
generation, runtime sources and executable identities match before and after
the run. All native certificates and resource checks pass, both fallback fields
are false, and owned processes and the workload lock are released.

The 43 engine clocks sum to **58.445307 seconds**. Result serialization sums to
**0.001888 seconds**, remaining process work to **2.772546 seconds**, and complete
native processes to **61.219742 seconds**. Summed supervised calls take
**62.842192 seconds**, including **1.622451 seconds** of supervisor overhead;
these sums are not elapsed ingest-to-query time. Process user/system CPU total
169.980076/32.550341 seconds, and complete-value validation totals 0.032598 seconds.
Engine time includes query-time I/O, decoding, operators, source validation and
provider certification. Remaining process work includes startup, preparation,
reporting and exit. CPU overlaps wall time and parallel workers.

The [complete per-query CSV](evidence/native-selected-text-views-2026-10-15.csv)
also retains CPU, peak RSS, page faults, block operations, supervision, validation
and full query text. The table below displays seconds rounded to six places;
the CSV preserves the runner's numeric values. This single pass neither replaces
the paired retention result nor supports a new general speedup claim.

| Query | Engine (s) | Result serialization (s) | Other process work (s) | Process (s) | CPU, user + system (s) | Peak RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 0.000079 | 0.000101 | 0.059091 | 0.059272 | 0.046839 | 147.453 |
| Q2 | 0.197699 | 0.000010 | 0.049306 | 0.247015 | 0.684443 | 562.016 |
| Q3 | 0.850715 | 0.000091 | 0.050429 | 0.901235 | 1.150154 | 450.156 |
| Q4 | 0.385814 | 0.000024 | 0.049882 | 0.435720 | 0.612370 | 372.156 |
| Q5 | 1.392940 | 0.000019 | 0.050357 | 1.443317 | 1.596429 | 1922.125 |
| Q6 | 1.318294 | 0.000023 | 0.049673 | 1.367989 | 4.028250 | 1271.500 |
| Q7 | 0.000441 | 0.000029 | 0.044011 | 0.044481 | 0.042307 | 150.594 |
| Q8 | 0.148957 | 0.000022 | 0.048075 | 0.197054 | 0.591208 | 565.078 |
| Q9 | 1.220563 | 0.000021 | 0.052730 | 1.273314 | 6.273908 | 4234.438 |
| Q10 | 2.315932 | 0.000049 | 0.077877 | 2.393857 | 4.308173 | 3296.953 |
| Q11 | 1.113877 | 0.000051 | 0.051423 | 1.165351 | 2.893867 | 507.141 |
| Q12 | 1.674191 | 0.000024 | 0.049588 | 1.723803 | 2.827294 | 545.719 |
| Q13 | 0.708966 | 0.000033 | 0.052213 | 0.761212 | 4.782975 | 1508.812 |
| Q14 | 1.571753 | 0.000033 | 0.105232 | 1.677018 | 10.975613 | 3755.531 |
| Q15 | 1.467088 | 0.000038 | 0.059380 | 1.526506 | 10.178423 | 1952.281 |
| Q16 | 2.478683 | 0.000025 | 0.052275 | 2.530983 | 4.186670 | 2374.344 |
| Q17 | 2.534518 | 0.000034 | 0.061466 | 2.596018 | 18.464202 | 3094.922 |
| Q18 | 0.337818 | 0.000027 | 0.056228 | 0.394073 | 0.707042 | 484.594 |
| Q19 | 5.265609 | 0.000041 | 0.063357 | 5.329006 | 9.638242 | 4251.172 |
| Q20 | 0.206592 | 0.000062 | 0.061596 | 0.268250 | 0.627107 | 1930.531 |
| Q21 | 1.076072 | 0.000011 | 0.073238 | 1.149321 | 7.689688 | 2973.594 |
| Q22 | 1.254061 | 0.000044 | 0.077128 | 1.331234 | 8.250356 | 3025.203 |
| Q23 | 3.190853 | 0.000052 | 0.087223 | 3.278128 | 12.554117 | 3351.797 |
| Q24 | 1.237401 | 0.000204 | 0.070868 | 1.308473 | 7.780617 | 2453.000 |
| Q25 | 0.359584 | 0.000039 | 0.064349 | 0.423972 | 1.259629 | 1960.219 |
| Q26 | 2.015717 | 0.000030 | 0.051404 | 2.067151 | 2.593457 | 588.547 |
| Q27 | 2.147774 | 0.000032 | 0.062291 | 2.210097 | 2.956035 | 2361.188 |
| Q28 | 2.252664 | 0.000050 | 0.070681 | 2.323395 | 2.888818 | 2676.297 |
| Q29 | 5.681932 | 0.000104 | 0.056676 | 5.738712 | 11.172017 | 1497.219 |
| Q30 | 0.269341 | 0.000060 | 0.059488 | 0.328889 | 0.514376 | 347.938 |
| Q31 | 1.335218 | 0.000035 | 0.108529 | 1.443782 | 3.693964 | 3017.172 |
| Q32 | 1.524352 | 0.000037 | 0.117044 | 1.641432 | 3.714100 | 3327.812 |
| Q33 | 2.164388 | 0.000053 | 0.057432 | 2.221872 | 7.638046 | 2847.000 |
| Q34 | 2.701355 | 0.000037 | 0.060325 | 2.761717 | 18.129819 | 4812.953 |
| Q35 | 2.621113 | 0.000040 | 0.067945 | 2.689098 | 17.706300 | 5353.062 |
| Q36 | 0.422892 | 0.000031 | 0.057114 | 0.480037 | 2.655969 | 864.625 |
| Q37 | 0.415543 | 0.000031 | 0.067441 | 0.483015 | 0.960786 | 1928.078 |
| Q38 | 0.450588 | 0.000042 | 0.073934 | 0.524563 | 0.910650 | 2955.672 |
| Q39 | 0.241763 | 0.000031 | 0.067961 | 0.309755 | 0.831123 | 1879.078 |
| Q40 | 0.912653 | 0.000062 | 0.074499 | 0.987213 | 1.558139 | 3006.047 |
| Q41 | 0.591146 | 0.000023 | 0.085575 | 0.676745 | 1.204612 | 3093.422 |
| Q42 | 0.196700 | 0.000047 | 0.058244 | 0.254990 | 0.559564 | 633.031 |
| Q43 | 0.191666 | 0.000040 | 0.058967 | 0.250673 | 0.692719 | 1433.266 |

## Portable evidence and reproduction

The [index](evidence/native-selected-text-views-2026-10-15.json),
[compressed manifest](evidence/native-selected-text-views-2026-10-15.manifest.json.xz)
and [archive](evidence/native-selected-text-views-2026-10-15.tar.xz) preserve
2,305 members: 157,539,620 original bytes in a 3,081,936-byte archive. Every
member is reopened and size/hash verified. The packet retains the frozen
design and 2,259-file source manifest, five-file source overlay, build identities,
all three comparison cohorts, raw query clocks/results, complete local gate
logs, public inventories, five diagnostic profiles and original failure history.
Bulk native inputs, binaries and public output payloads are omitted with their
identities and verification receipts retained.

Use the archived `compare_selected_views_467.py` protocol to reproduce the
70-call comparison through the maintained public paired runner. The archived
`run_selected_view_full43_468.py` uses the maintained one-call-per-query runner.
Rebind local paths to resident inputs and fresh output names; do not overwrite
the recorded runs. Keep the declared grants, query/reference hashes, source and
executable checks, storage/process guards and alternating sample order. Rebuild
changed provider packages with fresh compilation evidence. The archived raw-clock
replay and public/full-result verifiers permit inspection without rerunning
native workloads.

Documentation build, type, link, scope/status, static-asset and governance checks
pass across all 18 selected commands. The first status check caught a missing
scope-matrix row; adding that row and recording completed PR #1555 resolves it
without changing runtime sources. Local browser inspection verifies the new
evidence paragraph at desktop and 390-pixel widths, with no horizontal overflow
or browser warnings, and search navigation reaches the updated resource section.
At that local acceptance point, hosted review, package lanes and deployment
still required their separate proof, recorded below.

The first hosted wheel attempt stopped in all three lanes before building: the
bundled Zstd provenance description had not been synchronized with the reviewed
provider record. The same seven-test license check reproduced the mismatch
locally. Updating only that description makes all seven license tests and all
13 native-package tooling tests pass; the complete notice verifier covers 383
registry and five vendored packages. All 2,259 frozen source identities were
rechecked: the notice manifest is the sole difference, with runtime sources and
benchmark artifacts unchanged. The original failures remain in the integration
evidence; the subsequent hosted acceptance is recorded below.

## Hosted integration and production source

[PR #1556](https://github.com/depsilon/shardloom/pull/1556) merges at
`fb1812dec4f9fa724b2c587353c74162935e0b2c` after all 44 pre-merge checks succeed
on `ba7643df4576c1869dae33ed0c2ef4f9a2e335bb`. Reviewed, synthetic-test and merged
trees match. Automated review completes on that head, with no findings in the
final review/thread snapshots. The full native log records 2,807 passing tests
and 26 unchanged manual exclusions; provider groups pass 10/7/7/23/4 tests.
Three isolated installed-wheel lanes—Python 3.10 and 3.13 on Linux and 3.13 on
macOS—each pass all 80 native binding tests without failures, errors or skips.
These are integration wheels; no new package publication is attempted.

The [hosted evidence index](evidence/native-selected-view-hosted-2026-10-15.json)
and [archive](evidence/native-selected-view-hosted-2026-10-15.tar.xz) preserve
67 members, including all successful raw logs, earlier failed attempts, source
identities and the executed verifier. Primary verification checks 67 literal
excerpts, all 2,259 accepted source hashes against immutable merged Git blobs,
and both accepted executable hashes. The notice-manifest description is the
sole source-inventory difference; the measured runtime is preserved.

All seven preview responses match accepted source. All seven production
responses preserve merged source, with only the captured Cloudflare script
insertions in HTML. Cloudflare's production check still reports `in_progress`
and supplies no version ID. The packet records that unresolved status explicitly;
served-source verification does not claim that the notification completed.
The earlier rendered guide/search proof remains separately scoped. Remaining
optimization and breadth work continues under its existing owners.

## Diagnostic basis and remaining work

Five accepted-baseline diagnostic samples cover Q19, Q23, Q29, Q34 and Q35.
Each retains the exact executable/source identities, complete result checks,
raw stack sample, native timing receipt and process cleanup. Sampling runs for
up to three seconds at one-millisecond intervals and may perturb scheduling.
Those durations are excluded from the performance decision; stack counts are
not exclusive CPU percentages.

The Q23 sample identifies full retained-row view construction inside the
already-admitted sparse filter and motivates this finite change. The other
profiles remain inputs to the existing PERF-01/04/05/10 work. This unit closes
neither the six-area optimization contract nor the remaining breadth queue.
Structural metadata and unreviewed providers retain their resource owners;
CG-5/CG-6 and broader competitive claims keep their existing requirements.

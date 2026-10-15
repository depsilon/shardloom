<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fresh native UAT with separated query timings — October 15, 2026

Fresh ingestion passed for all **99,997,497 rows**. All **43 queries** passed in
one sequential execution each, with every one of the **381 returned rows** checked
against the retained native regression reference. All **18 modular cohorts** also
passed: **704 ShardLoom executions** and **704 independent pandas reference records**
across the declared input, output and raw/prepared workflow matrix. Pandas is a
testing reference; it does not execute ShardLoom work.

The primary query optimization clock is **62.560288 s**
of engine-owned execution. This is elapsed wall time including native query I/O,
decode where required, source-generation validation, native operators and
certification after preparation. It excludes separately measured result
serialization and the rest of the public process. CPU counters overlap elapsed
time and include parallel workers; they must not be added to wall time.

This is a local acceptance baseline on current source after the immutable 0.6.0
release. It does not establish a speedup, a cold-cache result, a new package
release, an engine comparison, whole-file every-cell ingest equivalence or the
complete input/output Cartesian product. The original Parquet was prehashed and
queries used newly written Vortex; the OS cache and normal host activity were
uncontrolled. No artificial pacing, power/thermal gate or automatic retry was used.

## Measured boundaries

| Measurement | Seconds | Scope |
| --- | ---: | --- |
| Engine execution, sum of 43 calls | 62.560288 | Primary query clock; includes query I/O and native validation |
| Result serialization, sum of 43 calls | 0.002826587 | Synchronous native values/schema serialization |
| Remaining process work, sum of 43 calls | 2.859121 | Startup, parsing, preparation, control/report work, exit and unattributed process work |
| Complete CLI processes, sum of 43 calls | 65.422236 | Engine + result serialization + remaining process work |
| Per-call supervision overhead | 2.095712 | Outside complete native processes |
| Supervised calls, sum of 43 calls | 67.517948 | Process wall + per-call supervision |
| Complete-result value validation | 0.035065 | External per-call validation; outside process and supervision clocks |
| Complete query stage | 86.609748 | Guard invocation through query runner exit; includes setup, archiving and summary writes |
| Fresh ingest process | 58.449630 | Complete native prepare process |
| Complete ingest stage | 62.492269 | Guard invocation through ingest runner exit |
| Ingest plus query workflow | 150.356156 | Includes stage checks; excludes source prehash, later independent readback, modular checks and final hashes |
| Independent reopening of all query outputs | 0.034172 | Rechecks saved raw envelopes, values, certificates, clocks and hashes |
| Modular matrix workflow | 486.914705 | All 18 native/reference cohorts and output validation |
| Source prehash | 5.785425 | Outside all native operation clocks |
| Final source and Vortex hashes | 11.404744 | Outside all native operation clocks |
| Entire fresh UAT | 656.309557 | Driver start through final verification and host-sampler cleanup |

Rows have explicit nested boundaries and must not all be summed. In particular,
remaining process work is a residual; it does not separately measure startup or
preparation. Whole-query user CPU was 174.097458 s and system
CPU was 39.531980 s. Maximum query process RSS was
5.193314 GiB (5,576,278,016 bytes); ingest peak RSS was
2.439133 GiB
(2,618,998,784 bytes). RSS is an observed process high-water
mark, separate from the declared 24-GiB engine admission and maximum parallelism
12. The host had 16 GiB physical RAM
and 10 logical CPUs. The modular matrix uses its own
1-GiB admission and maximum parallelism 2.

## Every query

All rows passed. Times use the single observed call; no best-of-repeats selection
or hot-run score applies. Serialization is shown in milliseconds to retain its
scale. The CSV retains full SQL, all clocks, CPU, RSS, page faults and block I/O.

| Query | Engine s | Serialization ms | Other process s | Complete process s |
| --- | ---: | ---: | ---: | ---: |
| Q01 | 0.000037 | 0.023167 | 0.045628 | 0.045688 |
| Q02 | 0.185222 | 0.007541 | 0.052296 | 0.237525 |
| Q03 | 0.914559 | 0.089708 | 0.051877 | 0.966526 |
| Q04 | 0.400384 | 0.026501 | 0.050767 | 0.451178 |
| Q05 | 1.565692 | 0.022125 | 0.051476 | 1.617190 |
| Q06 | 1.344374 | 0.025125 | 0.051761 | 1.396160 |
| Q07 | 0.000495 | 0.029250 | 0.046657 | 0.047181 |
| Q08 | 0.150009 | 0.024043 | 0.054996 | 0.205030 |
| Q09 | 2.086297 | 0.858750 | 0.062304 | 2.149460 |
| Q10 | 2.697237 | 0.060625 | 0.099157 | 2.796455 |
| Q11 | 1.111985 | 0.196958 | 0.055730 | 1.167912 |
| Q12 | 1.727665 | 0.024250 | 0.055313 | 1.783002 |
| Q13 | 0.678463 | 0.038709 | 0.052523 | 0.731025 |
| Q14 | 1.695702 | 0.037001 | 0.110600 | 1.806339 |
| Q15 | 1.605072 | 0.034875 | 0.064258 | 1.669365 |
| Q16 | 2.577190 | 0.024709 | 0.055768 | 2.632983 |
| Q17 | 2.564745 | 0.036167 | 0.082049 | 2.646831 |
| Q18 | 0.335977 | 0.029958 | 0.054658 | 0.390665 |
| Q19 | 5.329766 | 0.051291 | 0.061294 | 5.391112 |
| Q20 | 0.284740 | 0.062375 | 0.059782 | 0.344584 |
| Q21 | 1.178933 | 0.012250 | 0.067043 | 1.245989 |
| Q22 | 1.505555 | 0.041416 | 0.076420 | 1.582016 |
| Q23 | 3.671472 | 0.047125 | 0.085519 | 3.757039 |
| Q24 | 1.249961 | 0.197833 | 0.070239 | 1.320398 |
| Q25 | 0.353447 | 0.028792 | 0.064597 | 0.418073 |
| Q26 | 2.102665 | 0.036208 | 0.055038 | 2.157739 |
| Q27 | 2.136006 | 0.034250 | 0.062997 | 2.199037 |
| Q28 | 2.585390 | 0.032667 | 0.074323 | 2.659746 |
| Q29 | 5.717468 | 0.116084 | 0.064746 | 5.782330 |
| Q30 | 0.253908 | 0.060834 | 0.054382 | 0.308351 |
| Q31 | 1.295114 | 0.030667 | 0.109869 | 1.405014 |
| Q32 | 1.632290 | 0.052791 | 0.120728 | 1.753070 |
| Q33 | 2.141841 | 0.059167 | 0.065322 | 2.207221 |
| Q34 | 3.175019 | 0.042291 | 0.059923 | 3.234984 |
| Q35 | 2.843268 | 0.038167 | 0.061966 | 2.905273 |
| Q36 | 0.394471 | 0.030500 | 0.057605 | 0.452106 |
| Q37 | 0.438980 | 0.044209 | 0.066436 | 0.505460 |
| Q38 | 0.479123 | 0.043834 | 0.077050 | 0.556218 |
| Q39 | 0.245568 | 0.055625 | 0.065428 | 0.311052 |
| Q40 | 0.900909 | 0.054416 | 0.072871 | 0.973835 |
| Q41 | 0.594827 | 0.022999 | 0.085181 | 0.680030 |
| Q42 | 0.206990 | 0.020834 | 0.062905 | 0.269916 |
| Q43 | 0.201471 | 0.020500 | 0.059639 | 0.261131 |

The [complete timing CSV](evidence/native-query-timing-uat-2026-10-15.csv) and
[portable evidence index](evidence/native-query-timing-uat-2026-10-15.json) retain
unrounded measurements and exact source/result identities. Both Full43 runners
now require engine clocks by default, record one execution per query by default,
and produce the same timing columns. Explicit historical comparisons can allow
absent engine clocks; those values remain unavailable.

The version 2 runner summaries use engine execution for primary totals, geometric
means and paired medians/deltas. Complete process scores remain separately
available under `secondary_process_wall`. Result coverage (`complete`) is distinct
from clock coverage (`timing_available`); a missing engine sample prevents an
engine comparison without substituting process time. This run's archived version
1 summary preserves its original process-score fields; the engine totals and
table above come from its separately recorded raw engine clocks.

## Profiling priorities

The engine clock accounts for 95.625% of complete process time in this pass.
The five largest observations below total 20.736993 s, or 33.147% of engine
time; the ten largest account for 53.234%. These concentrations identify
investigation targets, not the causes of their costs. Result serialization
accounts for 0.004% of process time for these 381 returned rows; larger-result
and direct Python interchange workflows retain their separate acceptance.

| Query | Engine seconds | Shared work to inspect |
| --- | ---: | --- |
| Q29 | 5.717468 | Regex-derived text keys, grouped average/count/minimum and final selection |
| Q19 | 5.329766 | User, minute-of-hour and phrase compound keys and exact state consolidation |
| Q23 | 3.671472 | Positive/negative text predicates followed by grouped minima, count and exact distinct users |
| Q34 | 3.175019 | Full-URL grouping, reconciliation and ranked count selection |
| Q35 | 2.843268 | The same URL grouping with an additional constant expression |

The October 15 maintainer feedback is accepted into existing PERF-01/04/05/10
work. First distinguish actual scan/decode, transformation, key construction,
state updates, reconciliation and final selection costs on the shared native
path. Existing transformed-dictionary handling and parallel text-key
reconciliation are prior implementations to inspect, not new techniques to
claim. The Q34/Q35 difference in one pass does not prove different planning.

Any retained change must use a frozen matched comparison, complete-result
checks, resource and control-query gates, and composed or held-out workflows.
Keep the broader six-area/eight-investigation completion contract active.
Different-query reuse must account for construction cost, retained bytes,
source generations and validity; process retention alone does not establish
reuse of engine work. This pass does not establish cold- or warm-cache timing.

## Reproduction and evidence

The frozen optimized executable has SHA-256 `ce3978515a7a4423c61511e9cf14e53d8cb2853f9e7f0b596d1bc3bb418ae5dd`.
The build records base commit `e8611a3c00af2a7b3f3f742c8b7128c3a258fbc7` plus all
2,258 exact source-file hashes for the uncommitted
compression and timing changes. The portable evidence preserves the source
manifest, build receipt/log, commands, complete query definitions and reference
packet, complete saved outputs, process cleanup receipts, modular fixtures and
reports, host observations and helper sources.

Every query output was reopened independently, with the native I/O certificate
available and `fallback_attempted=false` / `external_engine_invoked=false`.
The original 14,779,976,446-byte Parquet and new 15,713,610,513-byte Vortex file
were hashed after the run and had unchanged generations. Large dataset payloads
and executable bytes are excluded from the portable evidence archive, with their
identities retained. This evidence supplements the earlier
[compression acceptance](native-compression-resources-2026-10-15.md); it does not
rewrite its historical process-only timings.

## Current-source validation

The same 2,258-file source set passes formatting, workspace/native-feature
Clippy, 3,219 workspace tests and 4,222 native CLI/Vortex tests. The 29 named
manual/platform exclusions retain their original declarations. All 80 direct
binding tests pass with no skips, and all 757 public workflow cases pass across
19 families. The unchanged compressor/provider files retain their 72 vendor
tests and the original frozen lifecycle comparison.

Independent public-evidence readback verifies 724 expected-value files with
22,002 rows, 1,062 envelopes and 20 complete pressure files with 677,251 rows.
These overlapping counts are not additive. The runner separately reopens the
actual format outputs. All 18 documentation/build checks pass, and desktop,
mobile and search checks cover the updated benchmark, runtime and limitation
pages. The [acceptance supplement](evidence/native-query-timing-acceptance-2026-10-15.json)
retains all 2,076 verified members, including exact commands, logs, source
overlays, browser evidence and original failure history.

The first direct test attempt had nine child-process failures because its
isolated interpreter could not find PyArrow. A private test environment using
the retained dependency fixed that setup; the rerun uses identical native
binaries. A stale manual-test inventory hash was also corrected after proving
all exclusion declarations unchanged; the passing Rust commands were preserved.
These are source-package checks, not installed-wheel or all-platform proof.
Subsequent [hosted integration](evidence/native-compression-hosted-2026-10-15.json)
passes all 44 checks and three installed Linux/macOS wheel lanes, each with
80 binding tests, before PR #1555 merges. Exact-merge production deployment and
all seven checked served documents/assets pass. No additional query timing run
was needed for this integration-only proof.

Review subsequently found that the runners recorded engine clocks but still
scored comparisons using process time. The reporting correction changes only
the two Python runners and their tests within the frozen source set. All 46
focused timing/runner tests pass, including reversed engine/process rankings,
missing historical clocks, zero clocks and incomplete/failed result cohorts.
The [scoring follow-up evidence](evidence/native-query-timing-scoring-2026-10-15.json)
retains the original regression failures, successful checks and exact source
differences. Native execution sources and the accepted binaries remain unchanged;
the original UAT and acceptance archives retain their hashes and clock meanings.

<!-- SPDX-License-Identifier: Apache-2.0 -->

# Historical query performance gap and completed fresh UAT

Status: the maintainer-requested [fresh complete UAT](native-fresh-unpaced-uat-2026-10-14.md)
passes on the current correction, with 194.125941 seconds across all 129 query
processes, 54.603468 seconds native ingest and all 18 format/workflow cohorts.
It uses no artificial pacing, battery/thermal admission or automatic retry.
The maintainer resumed PR #1551, one version bump after integration and the
remaining work. Needed capabilities are retained. The engine never received
battery or thermal checks. This report preserves the earlier investigation;
it does not establish the cause of every historical timing difference.

## Question and measurement boundary

The earlier complete observations total 177.372562, 214.675706 and 211.784749
seconds across 129 query processes: all 43 queries, three calls each. They are
sums of complete native process times, not sums of query minima or elapsed
workflow time. Their original evidence remains in the
[composition investigation](native-composition-attribution-2026-10-14.md).
The later [context correction](native-admitted-context-2026-10-14.md) passes its
finite paired rule but records substantially slower absolute times. That does
not resolve the historical difference.

The diagnostic compares the exact retained executable from the 177-second
cohort with the current combined decoder/context correction:

| Role | Executable SHA-256 | Source |
| --- | --- | --- |
| Control | `490f00b2e29842b3a4b066245612a2243c8ce8da67a8decc29097e9bec4d0ff7` | Accepted pre-composition source `ed253d1ec74b724215f71f0b410123656e596696`. |
| Candidate | `fee87124c91b90f5f02ff1f9f00d9eaae7fd87724e152641a98bda2efb8725c9` | Frozen combined correction, subsequently committed as `9dd328f5`. |

Both use the same retained Vortex file: 99,997,497 rows, 15,713,610,513 bytes,
SHA-256 `004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
The host record is macOS 27 arm64, 10 logical CPUs and 16 GiB physical memory.
The public request declares 24 GiB and maximum parallelism 12; the native
resource envelope reports effective parallelism 10. The memory declaration
is neither physical RAM nor an enforced process-RSS limit.

Every call uses the public SQL workflow, includes process startup, complete
output and exit, and checks complete retained reference values and no-fallback
certificates. Reference values are retained native regression evidence, not a
fresh independent oracle. The input is prehashed; OS caches and ordinary
desktop activity remain uncontrolled. Input hashing, host snapshots and result
verification are outside the native process clock. The same observer is used
for both roles. No engine behavior changed during these observations.

## Focused comparison

Q3, Q23, Q29 and Q34 each run three alternating pairs in both starting orders:
48 complete calls. All results pass. Medians below preserve both orders; the
historical column is a separately observed cohort, not a matched estimate.

| Query | Historical median | Forward control | Forward candidate | Reverse control | Reverse candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q3 | 0.956776 | 0.937482 | 0.902804 | 1.031612 | 0.985775 |
| Q23 | 3.307699 | 3.217206 | 3.166795 | 3.394104 | 3.486621 |
| Q29 | 5.471411 | 5.446654 | 5.561070 | 5.711429 | 5.762420 |
| Q34 | 2.604848 | 2.347250 | 2.381942 | 3.129090 | 2.397176 |

Times are seconds. For Q3/Q23/Q29, candidate instruction-count medians differ
from control by less than 1% in both orders. The largest candidate increase in
elapsed median across these eight query/order comparisons is 2.73%; this is a
bounded diagnostic observation, not a general overhead bound. Q34's reverse
control is slower than its forward control and remains in the evidence.

The instrumentation keeps `wait4` CPU time separate from Darwin instruction
and cycle counters. An initial seven-call attempt stopped because the two CPU
accounting surfaces exceeded an incorrectly strict 0.2% equality check. That
failed attempt is retained. The corrected protocol uses `wait4` consistently
for comparative CPU time and preserves both surfaces and their difference.
It does not treat the two accounting APIs as interchangeable.

## Complete-suite attempt and observed host conditions

A fresh all-query paired attempt completed three calls per role for Q1–Q20:

| Scope | Control | Candidate |
| --- | ---: | ---: |
| Complete native calls | 60 | 60 |
| Sum of process times, seconds | 80.161570 | 79.552840 |
| Sum of process CPU time, seconds | 261.121638 | 259.457821 |

Q21 control run 1 also passed. During candidate run 1, macOS changed from
nominal thermal state to fair. The watchdog stopped the native process; its
return code is 130 and its incomplete output is not accepted. The attempt
retains 122 observed calls, 121 successful results, 85 sampled host observations
and all 488 bound raw files. Its supervised driver lasted 185.306339 seconds.
There is no complete Full43 total from this attempt, and its completed prefix
must not replace the declared complete cohort.

The run began on battery at 42% and ended at 39%, with Low Power Mode disabled.
Separately, the preserved OS power history records 3% battery at 09:23 local
and an AC attachment at 2% at 09:25, overlapping the earlier slow comparison.
This establishes a power-state confounder; it does not measure that cohort's
CPU frequency or prove how much of its duration the power state caused.
The new thermal interruption is directly observed. Historical cohorts lack
equivalent per-call power/thermal observations, so their full cause remains open.

## ShardLoom technique review

The candidate's actual outputs report these existing execution paths, which
were checked against their native implementations:

- Q3: direct dictionary/typed scalar aggregate updates.
- Q23: dictionary-code top-k with exact first-pass late measures; the reported
  late-measure second pass is false.
- Q29: transformed-dictionary chunk partials, dense accumulation and capillary
  top-k candidate selection.
- Q34: complete-key partition reconciliation and exact top-k; its report
  identifies second-scan avoidance.

The resource envelope retains 262,144-row capillary targets. Pressure-signal
labels alone do not prove that a scheduler adaptation occurred or would improve
these queries. This pass does not change PulseWeave scheduling, scan granularity,
encoding policy, native output, context ownership or decoder admission. Further
engine optimization must identify a measured missing opportunity below the
shared native boundary and preserve the needed capabilities.

## Recent-merge source review

The maintainer's hypothesis is that streaming work changed execution below the
public surface and bypassed ShardLoom's existing encoded paths. The relevant
first-parent sequence from the 177-second accepted source `ed253d1e` is streaming
composition `5b5cd6a7` (#1548), spill recovery `483cbaa9` (#1549), selection resources
`22b2cde0` (#1550), followed by the currently unmerged context/decoder correction
`9dd328f5` (#1551). This source review was read-only during the earlier stop.

One concrete streaming change couples general relational file scans to JSON
delivery size. `PreparedVortexRelational::collect_jsonl_with_input` now selects
`FileScanBatching::Output`; its 2,048-row `BATCH_ROWS` reaches
`local_primitive_relational_scan::run`, which uses `batch_rows.min(SCAN_ROWS)`
instead of the previous fixed 8,192-row scan range. A full file scan can therefore
make about four times as many range executions. Incremental JSON delivery can
request even smaller batches. This bounds materialization before wide reads,
but couples delivery size to source work and is a performance-review target.
It does not prove four times the I/O, decoding or runtime.

The recorded Full43 calls do not enter that general relational path. The
metadata strategy selector, optimized-source dispatch, prepared count and
prepared aggregate owners are byte-identical between `ed253d1e` and `9dd328f5`.
`public_workflow_route.rs` changes only a native-output evidence description in
that interval. The historical route inventory retains one count-all query,
two count-where queries, 35 aggregate queries, one filtered projection and four
sort queries. Existing full output checks and source tracing support this
bounded statement; a route name alone would not exclude hidden helper work.

The recent Q2/Q8 signals share `AdvEngineID <> 0`. Saved outputs for both roles
select 630,500 rows; Q8 run 1 in attempt 432 reports 1,937 numeric accessor decode
calls in each role. Shared helper/context/decoder cost remains a separate
candidate even when these counts match. These observations neither establish
the cause of the 37.303144-second historical increase nor clear every changed
runtime path. No engine implementation is changed by this source review.

## Retired experimental benchmark gate

During this investigation, both maintained query runners were changed to sample
Mac power source, battery charge, Low Power Mode and reported thermal state
before/after native calls and through the existing watchdog at roughly
two-second intervals. The experiment rejected a changing power source, charge
below 35%, enabled/unknown Low Power Mode, unavailable observations or a
non-nominal thermal state. The 35% floor was an agent-selected benchmark policy,
not an Apple throttling threshold. Reported nominal state does not establish
fixed frequency, exclusive CPU use or a cold cache.

Admission failures latched, stopped/reaped the child through the existing
supervisor, released the owned UAT lock and retained a failure receipt plus raw
paths. Receipt/storage writes did not reapply the failed thermal gate. Other
platforms explicitly reported that power/thermal monitoring was unavailable.
No host power setting or scheduling policy was changed.

Before removal, all 52 harness/reference tests passed. Four negative integration
regressions failed against the original runner bodies and passed with the gate.
The exact CI discovery commands also passed, including the paired runner and
new host guard; the CI gate-matrix validator passed. A real Q1 smoke through
the experimental paired runner completed all six native calls with admitted
Mac host observations, full-value equality and final identity checks. It
verified that gate's integration and supplied no full-suite performance
acceptance. Source comparison verified that the six changed Python files
belonged to the benchmark harness/tests and that the other 2,241 selected
source paths remained identical. The previously
passed Rust/native gates remain attached to the unchanged engine source;
they were not rerun for this experiment.

The maintainer rejected this device-dependent execution policy. The new host
guard and its tests were removed, and both runners, their existing test files
and CI wiring were restored byte-for-byte to `9dd328f5`. No tests or benchmarks
were run during that stop. The later fresh UAT followed a new explicit request.
The experimental
helper sources and their original test results remain in the immutable packet
below; that packet records the experiment and is not the current runner source.

## Subsequent plugged-in attempts

These local attempts compare the same historical executable and current
correction identified above. Each complete query has three calls per executable.
All completed calls passed result checks; none of these attempts completed the
declared all-43-query comparison or its conditional reverse-order checks.

| Attempt | Completed query scope | Historical executable | Current correction | Stop |
| --- | --- | ---: | ---: | --- |
| `performance-ac-full43-431` | Q1-Q13 | 41.157241 s | 40.374706 s | Experimental thermal gate during a cooling pause. |
| `performance-ac-full43-432` | Q1-Q14 | 44.235230 s | 44.287862 s | Experimental thermal gate after the first Q15 pair, during a cooling pause. |
| `performance-ac-full43-433` | Q1-Q2 | 1.038580 s | 1.092666 s | Power-source change during a pause after the first Q3 pair. |

The additional Q15 pair in attempt 432 and Q3 pair in attempt 433 remain in
their records but are excluded from the complete-query totals. Cooling and
setup are outside these native process sums. Driver durations were 295.661610,
547.233342 and 88.161962 seconds, respectively. Raw records remain under the
UAT root's `logs/<attempt>/result.json` and associated output/timing files;
these subsequent attempts are not included in the portable packet linked below.

Q2 and Q8 exceeded the declared forward elapsed/CPU thresholds in attempt 432;
Q2 also exceeded them in attempt 433. Neither did in attempt 431. Reverse-order
confirmation is missing. These query-level signals remain unresolved; aggregate
prefix totals do not dismiss them. The interrupted attempts establish neither
absence of a regression nor a device explanation for the historical difference.

The [portable observations](evidence/native-performance-gap-2026-10-14.tar.xz),
[member manifest](evidence/native-performance-gap-2026-10-14.json.xz) and
[readback verification](evidence/native-performance-gap-2026-10-14.verification.json)
retain the protocols, raw calls, helper snapshots, counter checks, failed
attempts, primary verification and harness regression evidence. Large binaries
and the input dataset are identified by hash and remain outside the packet.

## Current disposition

The new complete run provides current correctness and timing confirmation on
the same executable used in the slower context comparisons. It does not provide
a controlled attribution of the older durations. Preserve the measured
query-level differences and needed capabilities; further optimization requires
a reproducible cost and a concrete mechanism.

The maintainer's later direction lifts the PR/release/breadth pause after this
complete UAT. Future UAT executes each query once by default; repeats require
an explicit request. All samples, complete values, native and workflow clocks,
storage limits and cleanup evidence remain required. Artificial delays and the
retired device gate are excluded from the standard execution policy. Hosted
integration and normal release gates remain required; no broad capability
closeout or performance-superiority claim follows from this report.

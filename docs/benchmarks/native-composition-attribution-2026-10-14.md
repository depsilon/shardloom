<!-- SPDX-License-Identifier: Apache-2.0 -->

# Streaming-composition timing follow-through

The matched comparison does not reproduce the historical suite increase under
its predeclared thresholds. All 516 complete calls pass. No query exceeds the
elapsed-time, CPU or RSS threshold in both starting orders. This result does not
establish the cause of the earlier unpaired measurements or prove zero overhead.
Native execution, complete-result review and
[portable packaging](evidence/native-composition-attribution-2026-10-14.json)
are complete. The packet preserves all 1,725 evidence members, including the
original historical observations and both complete matched cohorts.

## Historical observation preserved

| Historical checkpoint | All 129 complete calls, seconds |
| --- | ---: |
| Fresh input before composition | 177.372562 |
| Streaming-composition acceptance | 214.675706 |
| Selection-resource acceptance | 211.784749 |

Every historical query's three-call sum increases from the first to the second
checkpoint. The total increase is 37.303144 seconds. Q29, Q34, Q35, Q23, Q19, Q33
and Q17 account for 19.277199 seconds, or 51.677% of that difference. These are
literal decompositions of the retained observations, not causal attribution.
The 66.444828-second sum of per-query minima from selection acceptance remains
a different statistic and does not replace any total in this table.

## Matched protocol and complete results

Use the retained executables from accepted pre-composition source `ed253d1e`
and composition source `5b5cd6a7`. Check all 1,749 and 1,757 native source files,
respectively, against those Git trees and their original build records. Freeze
the full protocol, executable hashes, queries, native reference, helpers and
resident input before timing.

Run all 43 queries with three alternating pairs each, followed unconditionally
by a complete cohort with reversed starting order. Both roles read the same
15,713,610,513-byte, 99,997,497-row Vortex file, with 24 GiB declared memory and
maximum parallelism 12. The host has 16 GiB physical RAM and ten logical CPUs;
the declared grant is not a process-RSS limit or an observed worker count.
No ingestion, cache purge, concurrent build or native test is part of this study.

Time each complete native process through startup, full output and exit.
Hashing, result validation, archive work and host sampling are outside that
clock; supervised duration is separate. Prehashing warms input access, and the
shared OS cache and ordinary desktop activity remain uncontrolled. The exact
historical thermal/cache state was not fully recorded and cannot be recreated.

All 516 outputs pass full values, schemas, route and resource checks, totaling
4,572 returned rows. The reviewer reopens all 86 per-query archives and their
2,064 raw output, stderr, timing and PID members. Both orders retain every call.
Native I/O certificates are available and both fallback/external-engine fields
remain false. The value reference is retained native regression evidence, not
a newly executed independent engine oracle.

| Starting order | Pre-composition all 129 calls | Composition all 129 calls | Numerical change |
| --- | ---: | ---: | ---: |
| Forward | 217.941149 s | 216.404682 s | -0.705% |
| Reverse | 220.624497 s | 218.656060 s | -0.892% |

| Order / executable | Sum of query minima | Sum of query medians | Total child CPU | Largest process RSS |
| --- | ---: | ---: | ---: | ---: |
| Forward / pre-composition | 70.053540 s | 72.304128 s | 756.560003 s | 5,762,072,576 bytes |
| Forward / composition | 68.641740 s | 72.365958 s | 751.347732 s | 5,735,809,024 bytes |
| Reverse / pre-composition | 70.776602 s | 73.665556 s | 761.303324 s | 5,772,148,736 bytes |
| Reverse / composition | 69.828466 s | 72.445297 s | 763.527881 s | 5,789,794,304 bytes |

Supervised cohort durations are 537.612013 and 549.214175 seconds. They include
both executables and harness work, so they are not another 129-call timing.
The smaller composition totals are observations; this study makes no speedup
claim and does not establish statistical equivalence.

## Query-level decision and scanner scope

The fixed per-query rule uses each role's median. An elapsed or CPU increase
must exceed the greater of 10% and 5 milliseconds; RSS must exceed the greater
of 25% and 256 MiB. The same query and metric must breach in both orders to
identify a repeated binary-associated cost increase requiring diagnosis.
All 43 queries and all three metrics remain in the result records.

Q35 and Q38 breach elapsed time in the first order only. Q35 changes from
3.432727 to 3.936778 seconds forward, then from 3.733446 to 3.394249 seconds
in reverse. Q38 changes from 0.412975 to 0.538799 seconds forward, then from
0.419272 to 0.373656 seconds in reverse. Neither has a CPU or RSS breach.
No query breaches any threshold in the reverse order. Preserve these first-order
observations as uncertainty rather than dropping them or declaring no overhead.

The historical and matched outputs select the same specialized routes: one
count-all query, two count-where queries, 35 aggregate queries, one filtered
projection and four sort queries. Source tracing follows their own footer,
prepared-scan, aggregate, projection and sort implementations. These calls do
not reach the general relational file scanner whose output-sized batching was
flagged in the review. That scanner therefore is not an established cause of
this Full43 difference. This result neither repairs nor measures its behavior
on other general relational workloads.

The old executable itself now records totals near the composition executable,
rather than its original 177.373-second observation. This weakens a persistent
composition-code explanation under the tested conditions. It does not identify
a historical host cause. Keep the original discrepancy explicit and preserve
the 512-KiB small-memory readback behavior; do not change scan sizing from this
evidence alone.

## Provenance and next correction

The pre-composition executable SHA-256 is
`490f00b2e29842b3a4b066245612a2243c8ce8da67a8decc29097e9bec4d0ff7`;
composition is
`93e6976f618abd276b02b897e5be4c9fda0321b2f4d48b65bbc29252497b4b75`.
Both use Vortex input SHA-256
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
The frozen driver rechecks binaries, input, helpers and the active source map,
and verifies child cleanup and lock release after both orders.

Proceed with the separately grounded
[admitted-context correction](../architecture/native-admitted-context-2026-10-14.md).
It addresses observed helper ownership defects and cannot retrospectively
explain this historical timing difference. No package publication, new provider
or whole PERF/CG closure follows from this study.

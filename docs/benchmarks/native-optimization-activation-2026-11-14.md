<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization activation: accepted bounded coverage unit

Status: **locally accepted**, November 14, 2026. Retain the coherent activation
candidate at `f414d9dd305cc23dfb9e0389a106ba5d7b91d708`. Complete results, native
execution, ownership, resource checks and both prospective engine-time gates
pass. This accepts the bounded implementation described in the
[activation contract](../architecture/native-optimization-activation-2026-11-13.md),
not complete optimization coverage or an independent speedup for each mechanism.

| Independently gated population | Baseline engine seconds | Candidate engine seconds | Reduction |
| --- | ---: | ---: | ---: |
| All 7,498 fresh-session declarations | 1267.114826002 | 1263.682737384 | 3.432088618 |
| All ten calls in five prepared lifecycles | 0.092849121 | 0.090530670 | 0.002318451 |

The fresh-session reduction is 0.2708585%. **5,189 individual cases regress**,
contributing 17.528735789 seconds of increases before gains elsewhere. The
complete 2,543-case COUNT subgroup also regresses, from 282.761846730 to
284.152322404 seconds; 1,845 of its cases are slower. The frozen rule requires
lower complete fresh-session and lifecycle totals, with no minimum percentage
gain or individual-mechanism threshold. It is unchanged after measurement.
COUNT's connection is retained within this combined unit; these observations
do not independently accept a COUNT performance improvement.

## Scope, source and execution protocol

The baseline is the accepted shared Sort prefix runtime at
`788a8925e587316ed6f6c3d9ca114fef29937bd5`, source fingerprint
`e0192f0cdd42382038ef7aec913b3bd3c3d126159252ddd68d390481dd6387b6`.
The candidate fingerprint is
`504910e4fff98357b1933726687692e1d15b72c250b95df3aadfe1d00404c026`.
Exact source bytes, runtime identities, declarations, input generations,
independent complete references and validation receipts are bound by
`activation-protocol-r3.json` in the packet. Later status/evidence documentation
does not change the measured engine. The PR's website compatibility fix has a
separate source-equivalence record; it is not part of this measured runtime.

The fixed cohort contains 6,943 unchanged original declarations, 285 versioned
COUNT controls and 270 mechanism controls. The latter cross 18 shapes, five
public interfaces and three formats. Profiles are 6,298 primary, 646 holdout,
170 smoke, 114 scale and 270 activation cases. The public interfaces are Python
SQL, DataFrame, typed relation plans, embedded workflows and CLI SQL. Inputs
are Vortex, Parquet and Arrow IPC; ShardLoom performs all measured execution.

The baseline reuses 378 exact compatible observations and executes the other
7,120 declarations once. The candidate executes all 7,498 once. Five separately
declared prepared lifecycles each execute two calls on each runtime; their score
includes the first call's construction/retention work. The four isolated stages
run sequentially: baseline fresh, candidate fresh, baseline lifecycle, candidate
lifecycle. There are no warmups, timing-based selections or routine repeats.
This is neither a Full43 nor a new full10k pass, and it runs no external-engine
benchmark.

The host is Apple M5 with 16 GiB physical memory, macOS
`27.0-arm64-arm-64bit-Mach-O` and Python 3.13.13. Frozen query limits are a 2 GiB
grant, two threads, a 2.5 GiB process RSS ceiling and a 120-second query deadline.
Operating-system cache state is uncontrolled. One observation per declaration
does not establish statistical repeatability or whole-engine superiority.

The earlier r2 baseline completed 7,120 fresh executions and 378 reused results,
but task-owned file inspections overlapped query execution. It is **excluded
from performance acceptance**. The isolation incident and prospective replacement
were recorded before inspecting its engine timings; the complete original
ledger, receipts and protocol remain in the packet. Protocol r3 preserves the
same cases, inputs, runtimes, references, resource limits and acceptance rule.
No such task work overlapped any of the four admitted r3 stages.

## Complete results and regressions

All 14,996 fresh-session results and twenty lifecycle operations pass complete
value/schema comparison, certificate checks and source-bound replay. Every
candidate fresh-session case contains executed file-scan evidence. The complete
case-level clocks, work, resource observations and regressions remain available
in the packet and the [acceptance record](native-optimization-activation-2026-11-14.json).
Overlapping groups must not be added together.

| Format | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Vortex | 4,063 | 969.814028496 | 962.993569597 |
| Parquet | 2,275 | 182.927885758 | 184.599575524 |
| Arrow IPC | 1,160 | 114.372911748 | 116.089592263 |

| Public interface | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Python SQL | 1,640 | 202.231099806 | 201.233578469 |
| DataFrame | 1,424 | 380.536270613 | 378.503528675 |
| Typed relation | 1,461 | 179.787623752 | 180.127779115 |
| Embedded workflow | 1,464 | 219.822734657 | 217.928892707 |
| CLI SQL | 1,509 | 284.737097174 | 285.888958418 |

Parquet, Arrow IPC, typed relation and CLI SQL totals regress. Family and
mechanism groups, including losses in multiway joins, windows and the new
COUNT controls, are retained in the machine-readable record. The combined gain
does not identify the marginal performance benefit of any single code change.

## What actually executes

The shared native owners now report executed scan contracts and completed
positional reader work, perform admitted whole-file projection pruning, connect
eligible grouped COUNT partials and file-backed cyclic semi-joins, and implement
bounded join membership filters, heavy-duplicate ranges and cross-call immutable
segment retention. They preserve the ordinary native path when optional
admission declines. No external query-engine fallback is introduced.

| Input format | Cases with scan reports | Runtime filter admitted | Skew ranges admitted | Cyclic execution | COUNT partial admitted |
| --- | ---: | ---: | ---: | ---: | ---: |
| Vortex | 4,063 | 1,353 | 25 | 5 | 86 |
| Parquet | 2,275 | 1,252 | 25 | 5 | 97 |
| Arrow IPC | 1,160 | 639 | 25 | 5 | 73 |
| Total | 7,498 | 3,244 | 75 | 15 | 256 |

These are cases with positive owner observations, not denominators for all
eligible opportunities or independent speedup claims. Join membership filtering
runs after producer/key validation; it does not establish source I/O pruning.
Skew handling retains stable duplicate order through optional credited ranges.
Executed logical column contracts do not measure physical per-column reads,
device reads, unique bytes or decoded bytes. Positional-read counters include
repeated reads and coalescing gaps.

Each candidate prepared lifecycle records zero cross-call hits on the first
call and 128 on the second. Every call returns all 49,733 expected rows. The
first call completes 361,516 reader bytes in each interface; second-call bytes
are 214,600 for Python SQL, 222,888 for typed relation, 218,744 for embedded
workflow, and 222,888 for both DataFrame and CLI SQL. All five two-call engine
totals individually decrease. Credit remains attached to retained buffers and
the bounded directory, and final session release reaches zero. This is reuse
of immutable native segments under one held generation, not result or join-build
caching. Changed generations and independent source owners retain their tested
failure/cold-start behavior.

## Validation and preserved failures

At the exact candidate source, formatting and strict default/native workspace
Clippy pass. The default suite passes 3,344 tests in 108 binaries, with no
ignores. The native suite passes 7,215 tests in 120 binaries, with 39 intentional
ignores; all 7,254 listed names are reconciled. All 449 installed-package tests
pass with no errors or skips. Seven selected public tests cover 255 activation
subcases, five-interface reuse/generation behavior and complete multi-window,
correlated-membership, pivot/melt/seeded-sampling, and vector/tensor/Variant
compositions. These public checks are correctness/activation evidence, not
additional performance observations.

The installed binary-object write/reopen test first exposed premature I/O-scope
closure after producer completion. The corrected terminal owner keeps I/O open
through sink flush and payload copy, then detaches evidence before publication.
The native regression covers small and multi-batch payloads, null/empty/duplicate
objects, retained report credit and reopening after source removal. Original
local and Linux CI failures remain preserved. The completed validation above
uses the corrected source; old passing checks are not substituted for it.

Complete portable replay succeeds after relocation without importing an engine
or executing queries. All fifty corruption/refusal controls reject their
modified evidence, including declaration, result, clock, source, isolation and
multipart-transport failures. The [verification receipt](evidence/native-optimization-activation-2026-11-14.verification.json),
[final review](evidence/native-optimization-activation-2026-11-14.review.md),
[isolation amendment check](evidence/native-optimization-activation-2026-11-14.isolation.json)
and [transport check](evidence/native-optimization-activation-2026-11-14.transport.json)
accompany the accepted evidence.

## Reproduce the evidence check and continue the queue

The [portable packet](evidence/native-optimization-activation-2026-11-14/README.md)
contains nine archive parts, `manifest.json`, `verify.py` and its README. With
Python 3.13 or newer, run `python3 -I -B verify.py` from that directory. Keep
verification output outside the packet directory, whose exact file set is
checked. The verifier checks part order, lengths, hashes and the reassembled
archive, then replays complete saved results/references, validation, resources
and both decisions. It does not rerun queries or reconstruct independent
reference calculations. Large input/runtime payloads are represented by their
verified identities rather than distributed as executable/data copies.

The accepted source becomes the baseline for the
[metadata and encoded-consumer work](../architecture/native-metadata-encoded-consumer-investigations-2026-11-13.md),
starting with individually complete aggregate measures. Per-opportunity coverage,
physical leaf/decode attribution, source runtime-filter pushdown, join-build and
result reuse, item 19, item 20, RFC 0044 and CG-1 through CG-23 remain open.
PR checks and integration retain their own exact-head boundary. No merge,
package publication, release or competitive claim is implied by this acceptance.

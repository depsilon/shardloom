<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native fixed-width decoder resource acceptance

Status: the decoder-only local acceptance and portable evidence are complete.
The [accepted packet](evidence/native-decoder-scratch-2026-10-14.json) and
[rejected history](evidence/native-decoder-rejected-2026-10-14.json) preserve both
candidates. The [contract](../architecture/native-decoder-scratch-2026-10-14.md)
defines this finite extension of PERF-03/06/08/09 and CG-5/19/20/21. Version
0.5.1 remains unchanged. The measurements below precede the subsequent
[helper-context correction](../architecture/native-admitted-context-2026-10-14.md).
The later [combined acceptance](native-admitted-context-2026-10-14.md) passes
its complete direct/public and paired query checks; hosted integration is pending.

## Behavior and limits

Native Chunked append reserves the reviewed child decoder's temporary byte
capacity before invoking its existing append operation. Temporary credit lasts
through success or failure; final output retains its existing independent
ownership. Direct BitPacked/Patched append and unsigned FoR fusion remain intact.
The FSST metadata estimate additionally covers the empty replacement buffer
allocated when a native primitive builder finishes.

The closed-tree contract covers the enumerated primitive/Boolean/decimal leaves
and physical encodings in the design. Offset/padded extents, alignment, native
finish replacements, copy-on-write overlap and specifically reviewed collection
capacities are included. Unsupported shapes retain their existing execution
without acquiring a new scratch-accounting claim. Arbitrary expression trees,
encoded validity, unreviewed nullable dictionary/patch paths, structural metadata,
compression/training, allocator overhead and total process RSS remain outside
this contract. Vortex stays native input/output; no external executor is added.

## Rejected candidate and unchanged cost rule

The first candidate completes 3,888 fresh-process samples with correct values and
released temporary credits, but 12 cells repeat a material cost increase in both
starting orders. That implementation is rejected. Source inspection finds
recursive host checks revisiting physical children and fresh buffer collections
during alignment inspection. The revision checks each node's own buffers during
the existing capacity walk and borrows canonical buffer handles. Device fixtures
cover values, children, packed storage and validity without permitting a copy.

Both candidates use the same predeclared workload and decision rule: 18 shapes,
three row counts, two chunk counts, nine alternating pairs per cell and two
starting orders. Each process warms eight complete calls and then times a batch
of native Chunked execution, shape checks and destruction. Complete scalar-value
verification and fixture/context construction are outside that clock; whole-child
CPU/RSS observations are recorded separately. The rule requires revision when
the same cell exceeds both 10% and 5,000 added nanoseconds per call in both orders.

The revised candidate passes all 3,888 samples over 108 cells per order. There
are 55 relative-only triggers forward and 54 reverse, with zero material cells
in either order. Independent review rehashes every sample/log pair and recomputes
every median and decision. The original failures, source snapshots and rejected
measurements remain retained. This finite screen supports retaining the resource
correction; it does not establish a speedup or negligible overhead in all work.

## Correctness and workflow acceptance

Focused resource tests cover refusal, retry, exact and one-byte-short grants,
late errors, concurrent pressure, final-owner release and the direct-append/fusion
controls. All six broad gates pass: workspace formatting, workspace Clippy,
workspace tests, native read Clippy, release-feature Clippy and native CLI/Vortex
tests. There are 3,219 passing workspace tests with none ignored, and 4,178
passing native tests with 28 individually recorded manual/platform exclusions.

All 80 direct Python binding tests pass without skips using the source package
and freshly built library. This is not a new installed-wheel observation.
The public suite passes 757 cases across 19 families. Independent reopening
checks the complete case set, 724 expected-value files containing 22,002 rows,
1,062 execution envelopes and 20 pressure files containing 677,251 rows. The
runner separately performs format-specific output reopening; referenced output
bytes and retained complete values are checked again by the evidence reader.
Counts across these suites are not additive.

## Complete-query observation

All 43 ClickBench queries run three times against the retained 99,997,497-row
Vortex input. All 129 complete results and schemas pass, including 1,143 returned
rows. No fresh ingest is performed. The explicit configuration is 24 GiB and a
maximum parallelism of 12 on a macOS arm64 host with 16 GiB physical RAM and ten
logical CPUs. The grant is not a process-RSS ceiling or an observed worker count.

| Clock/statistic | Seconds |
| --- | ---: |
| All 129 complete native processes | 229.881861 |
| Sum of per-query medians | 76.788967 |
| Sum of per-query minima | 71.766618 |
| Supervised suite duration | 301.007832 |

Each native process includes startup, complete output and exit. Validation,
compression and supervision add to the suite clock. Input hashing is outside
both clocks. The input is prehashed; shared OS cache and ordinary host activity
are uncontrolled. This is one retained-input regression observation against
previous native results, not a fresh independent oracle or a paired speed result.

The earlier all-129-call observations remain 177.372562 seconds before streaming
composition, 214.675706 after composition and 211.784749 after selection-resource
acceptance. They are unpaired. This new observation and the separate minima sum
do not explain that history. The separately frozen
[matched comparison](native-composition-attribution-2026-10-14.md) of the retained
pre-composition and composition binaries passes all 516 calls without a repeated
threshold breach. It does not establish the historical cause.

## Source and remaining proof

The runtime is an eight-file overlay on
`22b2cde04973ce4462517d73fe67b5f7a1e8177c`, frozen in a complete 2,244-file
source map before release builds and timing. The accepted CLI SHA-256 is
`ae93a9e365ef7ebad0279ee20de3bd9f924d72db7a75d05ee2e0f4d2fca44aa0`.
All native builds, tests and timed cohorts run sequentially through the existing
guard; process groups drain and direct children are reaped. The original source,
scoring thresholds, storage ceilings and failure history remain preserved.

The accepted archive contains 10,351 verified members; rejected history retains
7,874 members. Archive bytes and original manifest bytes are preserved in the
portable copies. Hosted integration awaits the combined helper-context correction.
Broader allocation coverage, streaming compatibility destinations/fanout and
platform proof retain their existing scope and owners.
No package is published and no whole PERF/CG gate closes from this finite unit.

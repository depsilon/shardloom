<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native helper context and decoder acceptance

The combined correction passes local acceptance on one frozen source: 22 focused
context regressions, six broad gates, 80 direct binding tests, 757 public cases
and 516 complete paired query calls. No query repeats a declared elapsed-time,
CPU or RSS cost breach in both starting orders. The
[portable packet](evidence/native-admitted-context-2026-10-14.json) contains
2,240 verified members. PR #1551 completes hosted integration with all 44 checks
and three installed-wheel lanes passing; the
[hosted receipt](evidence/native-decoder-hosted-2026-10-15.json) binds the exact
merged tree and production observation. These measurements retain source version
0.5.1; the authorized 0.6.0 release is a separate work item.

This closes the local proof for the [context continuity contract](../architecture/native-admitted-context-2026-10-14.md)
and the [decoder resource correction](native-decoder-scratch-2026-10-14.md).
The separate [historical composition comparison](native-composition-attribution-2026-10-14.md)
retains its own protocol and results. Neither comparison establishes the cause
of the original 177.373-to-214.676-second difference.

## Corrected behavior

Aggregate accessors, dictionary values/codes, Boolean and encoded validity,
predicate refinement, row exports and nested scalar/key extraction receive the
caller's admitted execution context. Native execution errors propagate through
the helper chain instead of becoming an unsupported-shape result. Shape-only
planning and diagnostic classification perform no hidden decode.

List offsets and sizes are prepared once with that context before upstream row
convenience access; null rows and empty arrays avoid unnecessary preparation.
Struct extraction enumerates logical fields and excludes the parent's validity
slot. Existing selected-row strategies, sparse-frame avoidance, native append
and FoR fusion, output ownership and the small-memory streaming contract remain
covered. The general relational scanner is unchanged.

Seven of the initial eight aggregate regressions fail on the previous helper
implementation. The final 22 regressions prove admission denial, release and
retry, complete values/nulls/order, conjunction refinement, nested children and
diagnostic-only behavior. FSST tests retain the distinction between its admitted
encoded predicate path and explicitly unsupported full metadata materialization.
Original red tests, intermediate fixture/lint failures and source snapshots are
preserved. The final source inventory covers 552 Vortex Rust files; lexical
session occurrences are not a count of public resource bypasses.

## Build and workflow verification

All six gates pass with Rust 1.99.0 and the pinned Vortex 0.85 dependency:

| Gate | Result |
| --- | --- |
| Workspace formatting | Passed |
| Workspace Clippy, warnings denied | Passed |
| Workspace tests | 3,219 passed; none ignored |
| Native read-feature Clippy | Passed |
| Release-user-surfaces Clippy | Passed |
| Native CLI/Vortex tests | 4,200 passed; 28 named manual/platform exclusions |

The packet retains exact commands, complete logs and each exclusion's source
declaration. Native tests use `vortex-local-primitives,vortex-write,universal-format-io`;
the release CLI and direct binding use `release-user-surfaces`. All source and
helper identities are frozen before execution and checked afterward.

All 80 direct binding tests pass without skips. The 757 public cases cover the
same 19 families as the preceding accepted matrix. Independent review checks
724 expected-value files with 22,002 rows, 1,062 envelopes and 20 pressure files
with 677,251 rows. The public runner also reopens format-specific outputs;
the evidence review verifies all 136 referenced artifact identities. These
overlapping counts are not additive. This is source-package/native-library
proof, not a new installed-wheel observation.

## Complete paired query protocol

Compare the correction with the retained executable whose 1,762 native
Rust/Cargo/config/vendor paths and bytes exactly match merged main `22b2cde0`.
The correction is a 44-file source overlay on that base, recorded within a
complete 2,245-file source map. The base commit alone does not identify the
candidate; all changed source bytes and the tracked patch are retained.

Both roles use the same 99,997,497-row, 15,713,610,513-byte Vortex input and all
43 ClickBench queries. Run three alternating pairs per query, then run all
queries again with reversed starting order unconditionally. Each role completes
258 calls across both orders. The correction's 258 results and 2,286 returned
rows also provide its final Full43 acceptance.

The configuration is 24 GiB declared memory and maximum parallelism 12 on
macOS 27.0 arm64 with 16 GiB physical RAM and ten logical CPUs. The grant is not
a process-RSS ceiling or an observed worker count. The existing workload guard,
exclusive UAT lock, 12-GiB disk headroom, 100-GiB workspace and 1,024/256-MiB
retained/active log limits apply. Samples have 120-second deadlines; each
complete cohort has a 2,400-second outer deadline. No native build, test,
profiling run or ingestion overlaps either cohort.

The native clock includes process startup, complete public output and exit.
Input hashing, output validation, archive work and host sampling are outside
that clock. Supervised cohort duration is reported separately. The input is
prehashed; shared OS cache and ordinary desktop activity are uncontrolled.
All complete values and schemas match the retained native reference. Native I/O
certificates, resource fields and specialized route IDs are checked for every
call; fallback and external-engine fields remain false. All 86 per-query
archives and 2,064 raw output/stderr/timing/PID members are reopened.

## Results and decision

| Starting order | Baseline: all 129 calls | Correction: all 129 calls | Numerical change |
| --- | ---: | ---: | ---: |
| Forward | 334.768985 s | 333.392305 s | -0.411% |
| Reverse | 382.966890 s | 389.067850 s | +1.593% |

| Order / executable | Sum of query minima | Sum of query medians | Total child CPU | Largest process RSS |
| --- | ---: | ---: | ---: | ---: |
| Forward / baseline | 105.251177 s | 110.744600 s | 1,009.175410 s | 5,807,816,704 bytes |
| Forward / correction | 105.985535 s | 110.978131 s | 1,003.361943 s | 5,883,887,616 bytes |
| Reverse / baseline | 119.116653 s | 128.752276 s | 1,036.509987 s | 5,838,389,248 bytes |
| Reverse / correction | 122.316216 s | 128.991877 s | 1,047.172753 s | 5,673,762,816 bytes |

Supervised durations are 823.202551 seconds forward and 943.987916 seconds
reverse. These include both executables and harness work. Both binaries have
higher process totals than earlier sessions; the observations do not establish
a host cause or permit comparison of these totals with earlier unpaired runs
as an isolated code effect.

The predeclared query rule compares each role's median. An elapsed or CPU
increase must exceed the greater of 10% and 5 milliseconds; RSS must exceed the
greater of 25% and 256 MiB. A repeated breach for the same query and metric in
both orders blocks acceptance for diagnosis. No such breach occurs.

The first order records elapsed and CPU breaches for Q37. In reverse, Q37's
elapsed medians are 0.427420 and 0.422676 seconds, with no metric breach.
The reverse order records elapsed breaches for Q3, Q20, Q34, Q35 and Q39, with
a CPU breach for Q3; none repeats its metric breach in the first order.
Neither order has an RSS breach. All observations remain in the packet.
Passing this finite rule supports retaining the correction; it does not prove
zero overhead, a speedup or statistical equivalence.

## Provenance, replay and limits

The baseline executable SHA-256 is
`66142c72e4b4e2508ecc38f70e929766a042f1fb75691ceae6228e79cd5623ad`;
the correction is
`fee87124c91b90f5f02ff1f9f00d9eaae7fd87724e152641a98bda2efb8725c9`.
The shared Vortex SHA-256 is
`004f9639fa0aa527b70f326c366c37b1173dceeacb92b50b2d8fd02957155c18`.
The archive SHA-256 is
`74a85db2c1baa886b5b90b7e5f8f6d033e500412dbb9746937242f850f3933dc`.

The archive retains the frozen plan, build records, complete source overlay,
executed drivers and repository paired runner. Reproduction requires rebuilding
the recorded sources, binding the helpers to local resident input and new output
paths, and running both starting orders under the same guards. Historical
output paths are immutable and cannot be reused. Original executables and bulk
input/output payloads are identified rather than embedded in the portable packet.

All six gates, direct/public acceptance and paired queries use unchanged runtime
source. Process groups drain and children are reaped. Every archive member is
checked after packaging; compressed manifest bytes expand to the original
manifest exactly. Decoder-only accepted/rejected evidence and the historical
composition experiment remain separate companion packets.

This finite unit does not establish general allocation coverage, a process-RSS
limit, cross-platform readiness, an independent correctness oracle, performance
superiority or whole PERF/CG completion. Hosted PR checks and the subsequent
single authorized release train remain separate gates. Vortex remains native
input/output and no external executor is added.

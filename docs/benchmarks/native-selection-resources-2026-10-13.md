<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native selection resource acceptance

Status: the repaired candidate passes local correctness, resource, cost and
portable-evidence acceptance on October 14. Hosted integration remains pending.
Both rejected cost cohorts and the failing-before-repair regression are retained. The
[contract](../architecture/native-selection-resources-2026-10-13.md) scopes this
unit to sparse Zstd selection and related decoder collections under
PERF-03/06/08/09 and CG-5/19/20/21. Version 0.5.1 is unchanged.

## Behavior

Sparse UTF-8/binary frame selection now executes through the admitted native
session. Temporary compressed-frame and metadata collections receive credits
before cloning. Selected row indices and output views use the session allocator;
selection reads mask bits without populating shared lazy caches. The existing
policy still permits 2–64 frames, at most 4,096 selected rows, density at most
one eighth and all-valid input. Unsupported shapes retain their existing native
execution route.

Selected frames use the existing decoder and dictionary. Temporary credits
survive decoding and gathering, then release. Escaped output values, views,
clones and slices retain the applicable output credits. Ordinary Zstd slices
borrow their contiguous frame range, compute validity ranks without a vector
and pre-count view segments before allocating the handle collection. No query
operator, data type, encoding or dependency is newly admitted.

This accounts for the reviewed collection capacities. It does not impose a
whole-process RSS ceiling or close general child-decoder scratch, compression,
dictionary training, allocator bookkeeping or unrelated selection providers.
Vortex remains native input/output; no external executor is involved.

Aggregate UTF-8 accessors, native dictionary values, dictionary preparation and
scalar-distinct validity work retain their caller's existing context/session.
Previously, these helpers could replace an admitted query session with a legacy
session, bypassing the new selection kernel. Context-free compatibility
entrypoints remain explicit; other such entrypoints and dictionary interning
metadata are outside this change.

## Local validation

The final candidate's focused suite passes all 37 Zstd tests. It covers selected
and excluded shapes, every returned value, native serialization, source ownership,
refusal before allocation, exact and one-byte-short grants, retry, empty/dense masks,
dictionary preservation and cleanup after each injected allocation failure.
Output data and view handles keep their credits independently after execution.

Three added context regressions prove sparse UTF-8 and native dictionary denial,
retry and output retention, plus actual dictionary-worker preparation under an
exhausted shared query pool. The two accessor regressions fail on the prior
implementation because a one-byte grant incorrectly permits decoding. The
worker test verifies denial, released pressure, a successful retry and complete
result values with no leaked credits. Adjacent dictionary preparation, ownership,
handoff and scalar-distinct groups pass all 10, 5, 4 and 8 tests respectively.

The frozen source has 2,241 selected files. Workspace formatting, workspace
Clippy and workspace tests pass; there are 3,219 passing workspace tests and no
failures or ignored tests. Clippy also passes with both `release-user-surfaces`
and `vortex-local-primitives`. The native CLI/Vortex suite passes 4,157 tests.
Its 27 existing ignored tests are recorded individually with their source
reasons: three require the non-Unix branch and the others are manual benchmark
or fixture helpers. These are declared exclusions, not additional passing tests.

Review identified a missing direct check of allocator-aware view segmentation
at small boundaries. Two tests were added inside the existing vendor test
module for empty/inline/oversized/binary values, segment limits, invalid length
prefixes and buffer-index overflow. All 17 tests in the vendor array group pass,
with 34 unrelated tests filtered and none ignored. That result is reused only
after verifying all 901 vendor and Cargo graph files match the tested version.
The final workspace and native suites run after the context repair. The existing
native CI lane now also runs the vendor array tests.

All 80 direct native Python methods pass with no failures, errors or skips,
using the source package and a newly built release library. The first attempt used
an interpreter without PyArrow; its failures are retained. The corrected run
uses the retained test environment and verifies the source package and explicit
library paths. This is direct binding evidence, not a new installed-wheel proof.

The public regression passes all 757 cases across 19 families, including
streaming composition, replay, dynamic pivot, constrained spill, cancellation
and output reopen. Independent readback checks the exact case set, every logged
case result, source identities, 724 expected-value files containing 22,002 rows,
1,062 execution reports and 20 pressure files containing 677,251 rows. Referenced
output artifacts are hashed. Their format-specific reopen happens in the public
runner; the readback separately checks the recorded values and pressure hashes.

Original compile/test expectations, the ignored-test postprocessor correction,
standalone vendor test setup, documentation checks and public-readback helper
corrections remain in the evidence. A packaging attempt also stops before archive
creation because it expects the original input identity in one object; the
corrected helper reads its separately recorded path, hash and size. No runtime
result changes during that repair. Counts across these suites are not additive.

## Complete queries and admission cost

The retained 99,997,497-row Vortex artifact and frozen complete-result reference
are used for all 43 ClickBench queries, three times each. A separate predeclared
paired screen compares the accepted composition executable with the resource
change: Q23 with parallelism caps of 12 and one, and Q22/Q21/Q20/Q29 with a cap of
12, with five alternating pairs per case. Both declare 24 GiB. The macOS 27 arm64
host has 16 GiB of physical RAM and ten logical CPUs; the grants are not process-RSS
ceilings and the parallelism caps are not observed worker counts. Each sample includes complete
output and process exit; result checking and compression are outside its timing.

The screen allows each case's median elapsed and CPU time to rise by at most
the greater of 10% or 5 ms. Its RSS allowance is the greater of 25% or 256 MiB.
A breach requires a separate frozen confirmation with reversed order. This
measures admission cost; it is not a speedup or superiority claim. Input is
prehashed and OS cache state and ordinary host activity are uncontrolled.

The final candidate's Full43 observation passes all 129 complete results and
schemas, including 1,143 returned rows. Complete native processes total
211.784749 seconds; the supervised workflow takes 275.937888 seconds. The sum of
each query's best observation is 66.444828 seconds and the sum of its medians is
70.660462 seconds. These are retained-input regression observations, not fresh
ingest timing or a comparative speedup result.

The repaired candidate passes all elapsed-time, CPU and RSS gates in its 60-call
screen. Median complete-process times are:

| Query | Parallelism cap | Baseline seconds | Candidate seconds | Change |
| --- | ---: | ---: | ---: | ---: |
| Q23 | 12 | 3.410874 | 3.519913 | +3.20% |
| Q23 | 1 | 8.197476 | 8.091707 | −1.29% |
| Q22 | 12 | 1.284728 | 1.294141 | +0.73% |
| Q21 | 12 | 1.183072 | 1.180530 | −0.21% |
| Q20 | 12 | 0.259833 | 0.261666 | +0.71% |
| Q29 | 12 | 6.173104 | 6.174833 | +0.03% |

The original candidate was rejected after two correct-result cost cohorts.
Reversed-order confirmation still showed elapsed-time increases of 33.0% for
Q23 at cap 12, 17.9% for Q23 at cap one and 23.5% for Q22; Q23 at cap one also
exceeded the CPU gate. Its earlier Full43 observation remains recorded separately
at 189.114938 native seconds and 239.239323 supervised seconds. The context repair
has a new frozen executable; these earlier observations are not reassigned to it.
Independent review reopens all 180 paired outputs across the three cohorts and
recomputes their gates, complete values, schemas, timing and certificate checks.

## Evidence and reproduction

The [portable index](evidence/native-selection-resources-2026-10-13.json)
binds the [6,997,040-byte archive](evidence/native-selection-resources-2026-10-13.tar.xz),
compressed member manifest and independent readback. Every one of its 4,171
members is reopened and checked by size and SHA-256, covering 177,053,498 original
bytes. It contains the exact runtime overlay on `528aa23e`, the complete
2,241-file source map, workflow change, rejected source captures, failing
regressions, successful gates, all final public logs, both Full43 cohorts and all
three paired cost cohorts. Superseded public logs remain local; their summary
and review are preserved. Executables and bulk data are identified but excluded.

The accepted CLI SHA-256 is
`66142c72e4b4e2508ecc38f70e929766a042f1fb75691ceae6228e79cd5623ad`.
Use the archived source map and overlay, not the base commit alone, to reconstruct
this candidate. The archive includes the exact build, guard, public regression,
cost and Full43 drivers. A replay requires new output identities, resident local
inputs and the recorded allocation policy; completed paths refuse overwrite.
The original 192-MiB packet limit, 100-GiB workspace ceiling, 12-GiB free-space
headroom and serialized native execution remain enforced. ClickBench retains
its shared 1,024-MiB historical and 256-MiB active log budgets.

The broader allocation, state, platform and conditional-investigation obligations
retain their existing owners.

<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native dictionary capacity growth: dropped candidate

The proposed capacity change is removed. It prevents a repeat dictionary from
growing two containers unnecessarily, but the frozen complete-query comparison
does not establish the required benefit. Q19 improves in only five of ten target
pairs and has a slightly slower median. The Q22 control also exceeds its original
5% regression limit. All 80 calls return complete correct values.

The accepted selected-view runtime is restored. A comparison of all 2,259 frozen
source assets leaves only the already reviewed Python notice-description update;
no native implementation differs from that accepted baseline. This finite drop
does not close PERF-01/03/04/05/06, the six-area completion contract or its eight
conditional investigations. Streaming destinations and broader capability work
remain ahead of the native transport queue. No new package is published.

## Mechanism and regression proof

The existing dictionary binder reserves the full incoming dictionary cardinality
before checking global string identities. Binding the same 64 strings a second
time grows the identity table and retained-value vector from capacities
`(112, 64)` to `(224, 128)`, despite adding no value. The new regression fails
on the accepted implementation at that exact assertion.

The candidate removes that speculative reservation and performs fallible
single-entry reservation only after the exact borrowed lookup proves a missing
value. Five focused tests pass for unchanged repeat capacity, mixed/duplicate
entries, first-seen IDs and forgotten-ID reinsertion, empty/declined/all-valid
accessors, checked capacity refusal and source-buffer release. The broader
dictionary group passes 102 tests with no failures or ignored cases. Existing
dictionary ownership promotion, cached chunk hashes, exact preunion and
partition workers are unchanged.

The [predeclared contract](../architecture/native-aggregate-dictionary-growth-2026-10-15.md)
identifies incremental rebuilding of mostly-new dictionaries as a risk. This
experiment does not isolate that cost as the cause of any timing difference.
The Q19 diagnostic's reserve/rehash samples motivated the screen; their durations
are not benchmark results. Container capacity is also distinct from live memory
admission, allocator overhead and process RSS.

## Complete-operation comparison

The accepted CLI has SHA-256 prefix `315e4f2708eb`; the candidate has prefix
`dc1bac789fdb`. Both build receipts and complete source manifests are retained.
The candidate build proves fresh compilation of `shardloom-vortex` and the CLI.
Only the interner implementation/test file and dictionary ownership test file
differ from accepted native source. The separate package-notice correction
changes no runtime code.

Each observation is a complete public CLI call over the same resident
99,997,497-row Vortex input with a 24-GiB grant. Baseline/candidate order
alternates within each case. The host is macOS 27.0 on arm64 with ten logical
CPUs and 16 GiB physical RAM. Requested parallelism and the grant are separate
from physical capacity. Input prehashing and the shared OS cache are explicit;
there is no cold-cache claim, pacing, replacement observation or automatic retry.

| Case | Pairs | Baseline engine median (s) | Candidate engine median (s) | Observed median change | Improved pairs | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Q19, parallelism 12, target | 10 | 4.832928 | 4.833546 | 0.013% slower | 5/10 | Fail |
| Q19, parallelism 1 | 5 | 7.491701 | 7.625478 | 1.786% slower | 1/5 | Pass |
| Q17, parallelism 12 | 5 | 2.584905 | 2.587418 | 0.097% slower | 2/5 | Pass |
| Q22, parallelism 12 | 5 | 1.243627 | 1.317386 | 5.931% slower | 1/5 | Fail |
| Q23, parallelism 12 | 5 | 3.209996 | 3.227750 | 0.553% slower | 2/5 | Pass |
| Q29, parallelism 12 | 5 | 5.779295 | 5.741519 | 0.654% faster | 3/5 | Pass |
| Q2, parallelism 12 | 5 | 0.147447 | 0.146636 | 0.550% faster | 3/5 | Pass |

The target requires a lower engine median, positive median paired saving and at
least nine improved pairs out of ten, with no minimum percentage or second
saving. Target process CPU may rise at most 5%; control engine medians may rise
at most the greater of 5% or 5 ms. Every case's RSS may rise at most the greater
of 25% or 256 MiB. All CPU/RSS gates pass; the two engine failures determine
the drop. Target process CPU medians are 9.001940 and 8.977738 seconds, and
target RSS medians are 4,618,551,296 and 4,641,570,816 bytes. These observations
do not establish a general speed or memory improvement.

Every complete value, schema, native certificate, declared resource grant,
input generation and false fallback field passes. Engine time governs the
decision; serialization, remaining process work, complete process wall time,
CPU, RSS and supervision remain separate. The comparison reports 349.041308
seconds of workflow elapsed, with 363.109225 seconds for its outer guarded
invocation, which also includes initial identity validation. Neither includes
a new ingestion. All native children and the workload lock are released.

Broad workspace/native/public acceptance and a new Full43 pass were not run
for this rejected candidate. They are retention gates; the accepted baseline's
existing proof remains intact. The candidate patch, original design, source
overlay, expected baseline failure, passing focused tests and every comparison
sample remain evidence, rather than shipped implementation.

## Portable evidence and independent replay

The [evidence index](evidence/native-aggregate-dictionary-growth-2026-10-15.json)
binds 82 verified archive members, including all seven nested comparison archives
and their 320 original raw members. It preserves the expected baseline failure,
passing focused tests, exact removed patch, both source/build manifests, original
frozen design and complete reference values. Native executables and the large
input remain identified by their full sizes and hashes rather than duplicated.

Independent clock/resource replay and primary complete-value replay agree with
every original gate and all 80 complete calls. Primary review corrected the
independent replay's CPU check to apply only to the target, as the frozen
protocol requires, and verified the entire recorded call order. The earlier
replay is preserved unchanged; the correction changes no result or decision.
Every outer and nested member is reopened and hash-checked before publication
of this evidence. No query measurement is rerun or replaced during that audit.

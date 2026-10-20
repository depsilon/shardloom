<!-- SPDX-License-Identifier: Apache-2.0 -->

# Compact local indices — October 19, 2026

Status: **finite U16 local-index candidate retained after local acceptance**.
Both frozen comparison cohorts meet every memory target and throughput control.
The candidate saves tracked memory and increases engine time in the small
comparison cases; it is not a CPU speed improvement. The full current-host
acceptance matrix passes.

This is the fifth completed finite decision in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md).
`PERF-10.compact_selections`, the other PERF-10 obligations and CG-1 through
CG-23 remain open. The earlier
[contiguous-range candidate](native-compact-selection-2026-10-18.md) stays rejected.

## Production opportunity and provider decision

Relational scan and expression-filter consumers allocated a credited
`ReservedVec<usize>` for every local input row. Noncontiguous selection then
built a second, allocator-owned U64 index array for native take. On this 64-bit
host, an 8,192-row local selection reserved 65,536 bytes for its first vector.
The selected positions feed real projection and count operations.

The retained [local selection implementation](../../shardloom-vortex/src/local_primitive_relational_selection.rs)
chooses U16 storage when the complete local source extent is at most 65,536 rows.
Larger local extents keep the original wide representation. Every position is
checked against that extent before insertion; width never depends on a column
name, query, observed density or benchmark. Arbitrary order and duplicate
positions remain explicit. The existing contiguous native slice recognition is
preserved in both widths; the candidate does not omit ordinal construction.

Vortex-first classification is `use_vortex_native_provider`. Pinned Vortex
0.85.0 supports unsigned take widths and retains the supplied index dtype in
native dictionary views. ShardLoom's existing credited result builder constructs
U16 arrays for noncontiguous take. Capacity is reserved before allocation, and
retained native buffers keep their existing shared owner. Global gathers retain
checked usize/U64 positions, including positions above 2^32. There is no new
dependency, public API, persistence format or external executor.

## Frozen comparison and decision

The [portable packet](../benchmarks/evidence/compact-index-portable-765.json)
contains `compact-local-indices-762-v1`, frozen before the harness, candidate and
measurements. Its 28 cells combine seven selection shapes, native count and
projection, and one/four executions over 16,387 nullable rows. Shapes are
unfiltered, all, none, contiguous band, sparse, dense and mixed-null. Complete
actual values and schemas are checked against an independently reconstructed
oracle.

The two-binary preflight covers those cells at 0, 32 and 16,387 rows: 168 complete
workflows and 420 operations. Both fresh-process measurement cohorts finish
before comparative inspection. Cohort A runs baseline then candidate; B reverses
role and cell order. Three samples per cell/role yield 340 workflows and 844
operations including four retained, excluded warmups. Both binaries share the
same measurement source. Test-only counters establish actual compact-owner and
U16-take use before comparison; their cost remains in candidate observations.

Exactly eight cells are memory targets: unfiltered/all, both consumers and both
call counts. Each must save at least 32,768 bytes **and** 2% of median peak
tracked memory in each cohort. No cell may add more than 8,192 tracked bytes.
The following target results are identical in both cohorts and at both call
counts:

| Shape and consumer | Baseline peak bytes | Candidate peak bytes | Reduction |
| --- | ---: | ---: | ---: |
| Unfiltered projection | 1,121,638 | 1,072,486 | 4.38% |
| Unfiltered count | 367,174 | 318,022 | 13.39% |
| All-selected projection | 1,154,406 | 1,092,966 | 5.32% |
| All-selected count | 399,942 | 338,502 | 15.36% |

Every one of the 28 cells also has an engine-time and complete-workflow control.
A regression fails when it exceeds **both** 5% and 1 ms in either cohort. All
controls pass, but the engine-time cost is real: sums of the 28 per-cell engine
medians increase from 9.518200 to 12.594874 ms in A and from 9.498751 to
12.775028 ms in B. These sums describe the frozen comparison cells, not an
elapsed suite duration. The largest per-cell engine increase is 0.282294 ms.
The complete-workflow control includes source/plan preparation, all operations,
output consumption and release, and owner destruction. Its raw clocks remain in
the packet. No threshold, sample subset or cohort was changed after results.

The memory claim is limited to tracked reservations in the frozen workloads.
It is not an RSS cap, whole-engine speed claim, or promise of the same savings
for other plans and platforms.

## Correctness and integration acceptance

Nine new boundary tests cover both widths; empty and maximum U16 extents;
out-of-range refusal; duplicate/reordered positions; nullable and nested sliced
inputs; native constant slices; wide global gathers; reservation denial/retry;
predicate failure; cancellation; failed consumers; and retained output after
source/session drop. Existing selection, memory, ownership and nested suites
also pass. The exact regression prefix is `compact_local_selection_`; the
ignored comparison selector is `native_compact_local_indices_screen`.

| Acceptance boundary | Result |
| --- | --- |
| Formatting and strict workspace/native lint | Pass. |
| Workspace tests | 3,235 pass across 107 targets. |
| Full native library suite | 3,031 pass; 30 opt-in tests remain ignored. |
| Native CLI suites | 1,087 pass across six targets. |
| Clean installed Python package | 105 binding and 19 context tests pass; no failures, errors or skips; final uninstall verified. |
| Public workflows | All 5,930 cases across 39 families pass; 8,151 no-fallback envelopes and 16,389 compressed log members independently reopened. |
| Full43 | All 43 queries once; all 381 rows and schemas match. Engine time: 50.31151625 s. |
| Modular formats/workflows | All 18 cohorts pass: 704 native workflows match 704 independent pandas oracle workflows. |

Suites overlap. Full43 is one regression observation against the retained,
pre/post-hash-verified Vortex input; no new ingest or paired speed comparison is
claimed. The modular matrix covers its declared raw/prepared inputs and output
cohorts, not every possible input/output Cartesian product. Pandas is solely an
independent test oracle. Native Vortex execution and no-fallback certificates
remain mandatory.

The later [completed-eight checkpoint](native-conditional-checkpoint-2026-10-19.md)
validates the same retained runtime sources with two reviewed dependency patches.
The measurements and acceptance records above retain their original compiled
dependency identities; the checkpoint records its fresh correctness and package
validation separately.

## Source identity, evidence and remaining scope

Acceptance binds source head `89f56e36` plus the exact tracked patch and added
files; that commit alone does not reproduce the candidate. The packet includes
both source states, freshly compiled distinct comparison executable identities,
the final CLI/extension/wheel identities, the 2,217-entry acceptance source map,
all raw comparison streams and witnesses, complete acceptance logs and the
drivers needed to reproduce them. Compiler outputs, the large input and generated
payloads stay outside Git with recorded identities and regeneration references.

The [independent packet check](../benchmarks/evidence/compact-index-packet-verification-766.json)
reopens every one of 17,113 archive members, verifies every original file and
confirms all accepted source contents remain unchanged. The
[closeout source check](../benchmarks/evidence/compact-index-closeout-source-766.json)
binds those same runtime, Python and harness contents after the documentation
and evidence additions. Replays require new local paths and the original
sequential/storage guards.

Failed build/lint observations and two evidence-helper corrections remain
preserved. The CLI artifact-binding correction resolved Cargo metadata with the
already-recorded target environment; it did not rebuild or change the accepted
binary. The finalizer initially looked for installed proof logs in an incorrect
subdirectory. It was corrected against the canonical prover and then verified
all eight original log hashes. Neither correction changed runtime sources,
measurements, thresholds or completed native runs.

This closes only the evaluated local-width mechanism. Broader selection/descriptor
work, other conditional investigations, hosted/platform certification and the
release decision remain separate. Published v0.6.0 and the release hold are
unchanged.

<!-- SPDX-License-Identifier: Apache-2.0 -->

# Final spill-merge order — October 19, 2026

Status: **no eligible material signal in the frozen final-tail scope**. All
observed final frontiers contain one or two runs, so none offers a different
adjacent binary merge order. The original schedule remains unchanged. No
candidate schedule was implemented or timed, and the temporary observer and
test harness were archived and removed.

This is the seventh finite decision in the
[conditional investigation sequence](native-conditional-investigations-2026-10-19.md):
five candidate rejections, one retained mechanism and this no-signal attribution
result, with 14 first decisions remaining. `PERF-06.merge_cost`, the broader
owners and CG-1 through CG-23 remain open. This result does not establish general
merge optimality or reject schedules for other frontiers.

## Native boundary and frozen comparison

The [design](native-merge-tail-design-2026-10-19.md) isolates the existing
relational Ordering's final stack. Ingestion continues to combine equal-level
adjacent runs; final completion continues to merge the rightmost adjacent pair.
Stable left-before-right ties, actual native run bytes, two readers, shared
memory credits and simultaneous input/output disk admission stay unchanged.
The separate numeric query-sort path and ingestion-time scheduling are outside
this investigation.

Vortex-first classification is `use_vortex_native_provider`: pinned Vortex
0.85.0 arrays, `NativeWriter` and `SequentialNativeFlatLayout` remain inside the
existing `shardloom-vortex` boundary. The observer adds no execution provider,
dependency, Arrow substrate, persistence format or external-engine fallback.

The [V4 protocol](../benchmarks/evidence/merge-tail-attribution-protocol-776.json)
was frozen before the corrected harness and measurements. Seven UTF8 payload
profiles at 16,383 and 32,767 rows make 14 spill-permitted cells; two resident
controls use 8,192 and 32,767 rows. Nullable Int64 keys sort ascending NULLS LAST
and descending NULLS FIRST. A source ordinal witnesses stable order without
becoming an explicit sort key. Empty and 17-row cases join the full matrix in
preflight.

Every workflow prepares one immutable resident source and executes the ordinary
public Sort/streamed-JSON consumer. Source preparation, native execution, complete
consumption and owner release remain in their corresponding clocks. Fixture
generation, independent expected results and witness persistence remain outside
the compared clocks. One successful memory-source construction and one prepared
source are required; prepared file sources and file opens must both be zero.

All cells use one lane and a 64-MiB native grant. Spill cases explicitly permit
a 1-MiB buffer and 128-MiB temporary quota; the two readers share 128 KiB of work
credits. JSON batches are capped at 2,048 rows and 2 MiB, with a credited client
limit of 16 MiB and 1,024 batches. Every complete value, schema, native
certificate, reservation and owned spill cleanup is checked.

Both roles use the same freshly compiled release test executable. The observer
records actual run lineage, rows, bytes, source intervals, final frontier,
merge spans, reader overlap and store accounting. Three samples per cell and
role run in two cohorts: A runs off then on; B reverses roles and cell order.
All 40 release preflight workflows precede 192 measured workflows and four
retained, excluded warmups: 236 complete workflows and operations in total.

The independent adjacent-interval dynamic program preserves each observed
frontier's order and weights. Its additive-byte cost is an estimate, not a
measurement of an executed alternative schedule. A same spill cell must qualify
in both cohorts: a distinct tree with at least 10% and 1 MiB estimated savings,
and final-tail merge spans of at least 20% and 2 ms of engine work. Observer
overhead controls also apply to engine and complete-workflow costs. Exact
semantics and ownership gate every record independently of these thresholds.

## Observed decision and limits

All 14 measured spill cells actually create native runs. Across the 84
observer-enabled measured spill workflows, each workflow writes 15–39 runs in
total, including intermediate merge outputs. Maximum simultaneous readers are
two, and maximum observed disk overlap is 19,193,732 bytes.

| Final frontier | Measured observer-enabled spill workflows | Different adjacent order available |
| --- | ---: | --- |
| One run | 48 | No final merge remains. |
| Two runs | 36 | The single left/right merge is fixed. |

Every observed optimal estimated tree equals the actual tree; estimated savings
are zero in all samples. For cells with a final merge, cohort median final-tail
spans occupy 11.14–14.78% of engine time, below the frozen 20% threshold. One
observer comparison, 16,383-row alternating payloads in cohort A, also fails the
overhead eligibility gate. Its original samples remain in the decision; no host
cause or universal observer penalty is inferred.

The [independent verification](../benchmarks/evidence/merge-tail-verification-776.json)
reopens all six release streams and 34 complete content witnesses, validates
every source/result/schema/trace and recomputes both cohorts' decisions. All
236 workflows pass correctness and cleanup; no spill cell qualifies. The
frozen matrix is not enlarged after observing its one/two-run frontiers.
A future final-tail investigation needs a new protocol that establishes at
least three actual remaining runs and material final-tail work before candidate
implementation. It cannot inherit a speedup or general scheduling conclusion
from this attribution.

## Validation and preserved failures

Before release compilation, the same frozen sources pass all 40 debug preflight
workflows and the nine focused spill regressions. Independent debug verification
checks complete values, schemas, traces and cleanup; these debug observations
receive no timing-comparison or decision credit. The release build separately
passes the same nine regressions and all 236 workflows. Eighteen independent
oracle tests cover exact values, lineage, adjacency, resource constants, source
accounting, clock separation and invalid evidence.

Three earlier invalid attempts remain intact:

- V1 fails public spill-buffer admission before any operation executes.
- V2 is stopped before execution after a source audit finds two complete JSON
  outputs exceed the existing collector cap; its interrupted build is drained.
- V3's first empty-source public operation returns successfully, then the
  harness incorrectly expects one file open for a resident source. It emits no
  complete workflow record or timing sample, and its final cleanup assertions
  are not reached. V4 corrects only that accounting contract and uses the
  separately observed memory-source preparation count.

The [V3 diagnosis](../benchmarks/evidence/merge-tail-source-accounting-failure-775.json)
retains the successful partial operation and failed assertion without granting
comparison credit. The earlier
[storage refusal](../benchmarks/evidence/merge-tail-storage-admission-failure-775.json)
occurs before child launch; its continuation uses unchanged limits and no
cleanup. The V4 checker also corrects a pre-measurement disk-quota literal to
the original frozen 128-MiB limit and binds all resource constants to the
protocol. No measured sample, workload or decision threshold changes.

The first archive attempt fails because its checker compares directory names
with trailing slashes to names normalized by the archive reader. The
[packaging diagnosis](../benchmarks/evidence/merge-tail-package-failure-778.json)
and failed archive are preserved in the corrected packet. The correction
normalizes directory names only; exact types, sizes, order and bytes remain
mandatory. No query is rerun for packaging.

## Evidence, restoration and next investigation

The [portable packet](../benchmarks/evidence/merge-tail-portable-778.json) contains
2,247 regular members and both empty directories from failed attempts. It
preserves all debug/release outputs, protocols, sources, helpers, build and
executable identities, failed observations and the failed archive. Compiled
binaries remain outside Git. Replays need new local output paths, the pinned
dependencies and the original sequential/storage guards.

The [independent packet reopening](../benchmarks/evidence/merge-tail-packet-verification-778.json)
checks every member and original byte. From the embedded 1,899-file accepted
Rust/Cargo base plus each patch and two added sources, it reconstructs all
1,901 input files for each of V1–V4 and verifies their 23/28/32/39 helper
identities. This is source reconstruction, not dependency vendoring.

The [restoration receipt](../benchmarks/evidence/merge-tail-restoration-778.json)
proves that the five temporary source changes are removed, all 1,899 Rust/Cargo
inputs match accepted commit `d732d788`, and all 2,217 previously accepted
runtime/Python/test/harness source contents remain identical. Formatting passes.
Prior workspace/native/CLI, installed Python, Public39, Full43 and modular
acceptance therefore remains attached to the same production bytes; those
suites are not represented as new runs. There is no candidate integration,
Full43 improvement, package, release or capability claim from this result.

The next conditional investigation is `PERF-06.rematerialization`, comparing
retention, native spill and bounded regeneration for genuinely reclaimable
derived owners with their complete dependencies and reconstruction costs.

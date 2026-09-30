# Mixed-measure exact DISTINCT workers — R3.b

Status: **prototype; no candidate performance claim**.

The control is retained R3.a runtime
`22f7acd22e8c06c651f835bb817e25edcad5d5f5`, frozen separately from the build
directory. Its final Full43 cohort is `paired43_20260930T015704489072Z`.
Q10's selected-measure pass records 1.518–1.597 seconds in grouped updates and
0.510–0.540 seconds in accessors. These are caller elapsed spans, not exclusive
CPU attribution. It processes 42,451,524 selected rows and 8,602,494 chunk-local
unique pairs. The current timer does not separate partial construction from
global merge.

## Proposed shared contract

Extract the existing mixed chunk preunion builder and merge into a private
reusable helper. Ordinary measures still consume every row. Exact DISTINCT
still consumes every new complete group/value pair. Serial callers continue
using the same helper. The caller prepares native accessors, workers build
independent chunk partials, and
the caller must merge them in original chunk order. Preserve the existing
row loop, chunk boundaries, signedness and floating-state fold order.

Reuse `AggregateChunkJobs` for bounded task admission, ordered completion,
cancellation and error propagation. Reuse `Budgeted::into_parts` for an owned
completion callback that holds the existing lease and window permit through
merge; do not introduce a second queue or ownership protocol.

Initial worker admission requires an active R3.a complete-count proof and at
most 128 retained keys, integer nonnullable measures and no spill/residual
expansion. The source/schema precheck selects CPU ownership only; it cannot
certify winners. Provider drivers must remain available for the COUNT prepass,
then retire before admitting compute workers. If worker admission declines,
restore provider progress on the same native operation. Initial task admission
denial may drain admitted work and resume the same serial consumer. Execution
errors after admission must propagate; no engine fallback or source replay.

Reserve bounded chunk/partial capacity and retain leases through source/accessor
ownership, completion and merge. Check the pinned collection-capacity model;
do not describe upstream provider allocations or process RSS as fully tracked.
An accessor that cannot use the existing integer-pair preunion must preserve
its original update path and ordering rather than widening the physical gate.

Vortex-first classification: **use_vortex_native_provider**, through the
existing Vortex 0.85 numeric accessors and source/scan envelope. This proposal
changes ShardLoom's scheduling and partial transport, not upstream encodings,
public source formats or external execution providers.

## Decision gates

- Independent complete values, duplicates with different ordinary measures,
  signed/unsigned extremes, ties/OFFSET and unchanged serial arithmetic.
- Owned completion lease/window lifetime, bounded queued state, pressure
  admission, cancellation, source replacement and prepared reuse/recovery.
- Complete native Q10 against the frozen R3.a executable, including COUNT,
  accessors, worker transport, ordered merge, result output and process exit.
- Keep smaller useful gains; preserve every sample and resource observation.
  If the candidate fails, remove its runtime prototype and retain the evidence.
- A retained change needs applicable full correctness/Full43, formatting,
  Clippy, documentation checks and a cohesive PR before the next candidate.

R3.a's portable bundle remains historical evidence for its own runtime source.
It is not validation of this worker candidate. CG-1 through CG-23
and the existing no-fallback, Vortex-native and release boundaries are unchanged.

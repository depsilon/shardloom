# Mixed-measure exact DISTINCT workers — R3.b

Status: **retained and merged in PR #1486** at
`4ae717a7d67d1d8ffe7aa384c5c96bc48413be44`, after all 40 final-head checks passed.
The independent follow-up audit verified all 264 outputs, 1,056 archive members,
44 query metric rows, 11 frozen-source entries and 12 portable text hashes.

After merge, three superseded frozen executables were retired with SHA-256,
file-generation, no-open-process and retained-evidence checks. This removed
191,832,064 allocated bytes. Receipts, all archived outputs, both portable bundles,
the final reviewed executable, release control and inputs remain. The local
receipt is `performance-candidates-20260926/r3b-superseded-binary-cleanup.json`.

The control is retained R3.a runtime
`22f7acd22e8c06c651f835bb817e25edcad5d5f5`, frozen separately from the build
directory. Its final Full43 cohort is `paired43_20260930T015704489072Z`.
Q10's selected-measure pass records 1.518–1.597 seconds in grouped updates and
0.510–0.540 seconds in accessors. These are caller elapsed spans, not exclusive
CPU attribution. It processes 42,451,524 selected rows and 8,602,494 chunk-local
unique pairs. That control timer does not separate partial construction from
global merge.

## Shared contract

The implementation extracts the existing mixed chunk preunion builder and merge into a private
reusable helper. Ordinary measures still consume every row. Exact DISTINCT
still consumes every new complete group/value pair. Serial callers continue
using the same helper. The caller prepares native accessors, workers build
independent chunk partials, and the caller merges them in original chunk order. It preserves the existing
row loop, chunk boundaries, signedness and floating-state fold order.

It reuses `AggregateChunkJobs` for bounded task admission, ordered completion,
cancellation and error propagation, and `Budgeted::into_parts` for an owned
completion callback that holds the existing lease and window permit through
merge; do not introduce a second queue or ownership protocol.

PR review found that the initial worker queue's private cancellation token did
not observe the public operation token. The follow-up uses the existing linked
child token and `AggregateChunkJobs::with_cancellation`. Running workers now
observe operation cancellation at their periodic checkpoints; stage retirement
and cleanup cannot cancel the caller's token. A native prepared-call test first
reproduced the missing link, then passed with the fix, including reservation
refund and a fresh successful call. All 14 focused tests pass. Revised-runtime
acceptance is tracked separately from the original observations below.

Initial worker admission requires an active R3.a complete-count proof and at
most 128 retained keys, integer nonnullable measures and no spill/residual
expansion. The source/schema precheck selects CPU ownership only; it cannot
certify winners. Provider drivers must remain available for the COUNT prepass,
then retire before admitting compute workers. If worker admission declines,
restore provider progress on the same native operation. Initial task admission
denial may drain admitted work and resume the same serial consumer. Execution
errors after admission must propagate; no engine fallback or source replay.

The queue admits at most eight outstanding chunks, each at most 262,144 rows
and eight measures. Reservations cover a conservative pinned collection-capacity
model and remain held through completion and merge. Retained partial capacities
are checked against that model. Upstream source/provider allocations and global
aggregate state are excluded; this is not complete allocator or process RSS enforcement.
An accessor that cannot use the existing integer-pair preunion must preserve
its original update path and ordering rather than widening the physical gate.

Vortex-first classification: **use_vortex_native_provider**, through the
existing Vortex 0.85 numeric accessors and source/scan envelope. This implementation
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

## Final reviewed-runtime acceptance

The cancellation-linked runtime is `dff85c33763ac773c51ca1dd5e61a675cef6e20f`,
frozen binary SHA-256
`f2b0fb728093cdee69aa0ad127d8d4d75be564c4c442e64ad8d849ab5c94009c`.
It uses the same R3.a control, input, guarded methodology and complete references
as the original cohort below. The final Full43 cohort is
`paired43_20260930T035437075429Z`; all 258 complete results match, with no
regression flags.

Q10 best improves **2.95%**, from **2.316603 to 2.248188 seconds**. Its medians
are 2.321387 and 2.276794 seconds. All three matched pairs are faster and lower
in peak RSS: candidate 638,992,384–646,184,960 bytes versus control
668,467,200–685,785,088 bytes. The suite total is 55.541869 to 55.276440 seconds
(0.48% lower), retaining a scoped Q10 improvement without a material suite-wide claim.

The preceding focused cohort `paired43_20260930T035127271099Z` passes all six
results: best 2.267940 to 2.228492 seconds (1.74% lower). Its slower first candidate
call, 2.812323 versus 2.294617 seconds, remains in the record. All original and
review-follow-up observations are preserved rather than combined into a new best.

This final source passes formatting, workspace and native Clippy, 3,436 workspace,
2,027 native and 1,520 CLI all-target tests. Counts overlap; 22 pre-existing
manual/regeneration native tests remain ignored. The 14 focused tests include
the new running-worker cancellation red/green, healthy token lifetime, pressure
retirement, refund and prepared recovery. The
[review-follow-up evidence bundle](../benchmarks/evidence/mixed-distinct-workers-review-2026-09-30.json.xz)
contains all 264 new comparisons, source manifest/diff, validation logs and the
original bundle's independent audit. It has SHA-256
`3f97f9df139cac1accc97d7f47c6da38cfb281bf2f4ce3272776644c2a7fa594`.

## Original pre-review measurements

The frozen candidate is `c557814b9a34fbd03a3f58779a8360e0b3662c4b`, binary SHA-256
`d5b4d1d4825f05a547f9cc873237be0a371310bb6d1deb13f5cc108f5d0fcca0`.
The control SHA-256 is
`f4bcaa6c7320553859932dc04e038e9313a68eead62f3055416e326cc629a50d`.

| Cohort | Control Q10 best | Candidate Q10 best | Reduction | Complete values |
| --- | ---: | ---: | ---: | --- |
| Focused Q10 | 2.306091 s | 2.197797 s | 4.70% | 6/6 pass |
| Full43 Q10 | 2.310866 s | 2.209022 s | 4.41% | 258/258 across all queries pass |

The focused Q10 medians are 2.310089 and 2.214497 seconds. Its first candidate
call is slower (2.718013 versus 2.306091 seconds); both other pairs improve.
That observation remains in the evidence without a host-load explanation.

In Full43, all three Q10 pairs improve. Medians are 2.321152 and 2.223983 seconds
(4.19% lower). Candidate peak child RSS is 630,652,928–640,761,856 bytes versus
662,372,352–676,986,880 bytes for the control; every matched pair is lower.
Each candidate processes the same 42,451,524 selected rows through 1,550 completed
jobs, with an eight-chunk peak window and a 21,570,144-byte peak task reservation.
Worker build spans overlap and cannot be summed into a complete-query speedup.

The sum of all 43 per-query bests is **54.969086 to 54.949360 seconds**—effectively
flat (0.04% lower). No query triggers the paired harness's regression flag.
This retains a useful scoped Q10 gain, not a material suite-wide improvement.
All samples and medians are preserved; comparisons to earlier differently timed
cohorts are not attributed to this implementation.

The cohorts are `paired43_20260930T030648682590Z` and
`paired43_20260930T031136289889Z`. They run sequentially on the same retained
99,997,497-row, 112-column Vortex artifact and Apple M5 Mac17,3 with 10 logical
CPUs and 16 GiB RAM. Calls include native process startup, complete CLI output
and exit. The requested 24 GiB budget is not a physical-memory/RSS cap. OS caches
are uncontrolled and unrelated host activity is accepted. No new ingest,
cold-storage, format-pulse, production-fairness or subsecond-suite claim is made.

## Correctness and resource proof

- Formatting, workspace Clippy and native release-surface Clippy pass.
- The frozen runtime passes 3,436 workspace, 2,024 native all-target and 1,520 CLI
  all-target tests. Counts overlap; 22 existing manual/regeneration native tests
  remain ignored.
- Thirteen focused tests pass after a test-only follow-up. They cover complete
  ordinary contributions despite duplicate pairs, order-sensitive integer-input
  SUM folding, typed extremes, ties/OFFSET, the maximum 262,144-row chunk with
  unique pairs, capacity/group bounds, actual worker admission and completion,
  pressure before/after queued work, separate cancellation and invalid-proof
  failures, native source-denial refund/recovery, source replacement, prepared
  reuse and owned-result persistence after source deletion.
- The shared queue's owned-completion fixture checks lease and window lifetime
  through successful and failed merges. The serial extraction was compared
  against its original row/update/merge order; ordinary and prepared worker
  fixtures assert nonzero submitted jobs rather than merely correct serial output.
- Caller-supplied prepared sessions that already own provider threads preserve
  their original consumer instead of adding another CPU pool. Tests cover both
  provider-backed and external-worker session ownership.
- Full43 uses complete retained native regression references. Bounded fixtures
  include independent expected values. There is no new full-size external-engine
  correctness oracle, allocator-wide audit or production fairness certification.

The original acceptance follow-up changed tests only. The later cancellation
repair changes runtime token ownership and requires its own acceptance record;
the final acceptance above covers it. The original timed source and observations
remain immutable.
The [portable evidence bundle](../benchmarks/evidence/mixed-distinct-workers-2026-09-29.json.xz)
contains both complete cohorts, all 264 strict value comparisons, archive/member
identities, runtime/test provenance, build and test logs, source review and the
reproduction harnesses. Original and sanitized-text hashes are distinguished.

Retain R3.b under the existing PERF-INTAKE/PERF-02/03 scope. V1 classification is
`v1_candidate_pending_feasibility`; CG-1 through CG-23 retain their independent
acceptance. Broader compound DISTINCT/spill transitions, memory attribution and
unsupported operator families are not completed by this candidate. R4, R6.c,
R10 and R2.b remain undecided; the completed 0.3.2 train and paused format pulse
stay closed.

The PR's website CI also exposed the newly listed
[fast-uri advisory GHSA-hrr3-gc8f-f4qj](https://github.com/advisories/GHSA-hrr3-gc8f-f4qj).
Its lockfile-only patch updates the existing BSD-3-Clause transitive package
from 3.1.7 to the compatible fixed 3.1.8 release. It changes no Rust dependency
or engine execution path; the website audit/build/type gates remain required.

# Remaining Performance Work

## Authorization and acceptance

The maintainer's September 6 instruction is to work through all remaining
performance suggestions; the September 8 continuation also authorizes merging
the open implementation PRs when ready. This continues RFC 0044 and PERF-01 through PERF-13;
it does not introduce replacement phase IDs or close competitive gates.
PR #1435 records the starting measured runtime `e0264748` and documentation head
`9e43d163`. The next retained runtime is `75fc09a0`; the later measured checkpoint
is `48182c5a`. Numeric compression remains retained throughout. PR readiness and
merge status are separate from runtime measurement and package publication.

The September 8 [execution-aware native artifact topology
item](execution-aware-native-artifact-topology-2026-09-08.md) refines the remaining
sequence. Following the later instruction to avoid further investment without
material benefit, the canonical phased plan now parks topology and its dependent
local-reduction, local-Top-K and execution-aware preparation stages. Both measured
scheduling revisions regress without material selected-query gains; preserve
their code and evidence without automatically continuing implementation. Earlier
open items below remain tracked. PRs #1435 and #1436 are merged with all 40 CI
checks passing on each exact head. Broader performance acceptance remains open.

The subsequent `codex/perf-retained-runtime` shipping branch starts from the
pre-topology checkpoint `15102a6a`. The topology runtime/CLI/harness and the
unbuilt isolated Python prototype stay on the preserved experimental branch.
Historical evidence and rejection decisions remain documented here; they do
not imply those experimental implementations are part of the shipping batch.

PR #1437 merged as `d51429e3702e5201646142a3d7252bbd72485c85` on September 8
at 19:54:42 UTC after all 40 checks passed on exact head
`5d2bf2b1fd7c1aa5c2a90d98c35af8095e51c783`. The accepted native runtime is
`572bd52c`, including the ordinary aggregate source-grant restoration and PGO
lookup fix; `5d2bf2b1` changes only the acceptance harness. Its
[acceptance packet](../benchmarks/retained-runtime-acceptance-2026-09-08.md)
records 129/129 Full43 checks, 380 plus 80 JSONL semantic checks, 80 required
integer-distinct worker checks on a Parquet-prepared fixture, and 1,674 complete
resident public-call checks. The nullable JSONL fixtures do not prove those
worker paths. Full43 best-sum is 91.215296 seconds versus fresh Existing-policy
`9152a92b` at 88.661862 seconds, 2.88% higher in an uncontrolled sequential
comparison; no overall throughput win is established. Earlier extraction and
`e739edeb` measurements retain their original identities. Scoped acceptance and
merge are complete; no additional broader PERF or competitive gate is closed.

The post-merge [four-owner ingest packet](../benchmarks/retained-ingest-owner4-2026-09-08.md)
completed after the September 8 interruption and was recovered September 12.
Candidate `572bd52c` took 95.447305 seconds versus the fresh 99.446032-second
control; both had four confirmed constructed owners. Candidate output is
18.591587 GB, all logical values match, and both generated outputs were verified
and retired. This supersedes the pending matched-owner obligation, not broader
scaling or new-layout query acceptance. The maintainer's September 12 priorities
are now the [ingest implementation/test sequence](ingest-performance-implementation-2026-09-12.md).

Each item below requires source-grounded provider admission, implementation or
a concrete feasibility experiment, correctness tests, and a measured retain/drop
decision. A proposal, extra counter, or passing compilation alone does not close
an item. Independent work can proceed together; Cargo builds and large local
measurements run sequentially with existing storage and process guards.

All code remains safe Rust within the existing native provider boundaries.
Native Vortex is the execution and highest-fidelity persistence target. External
engines do not execute residual work. Temporary query runs require the existing
explicit workspace and owned cleanup. This work does not authorize package
publication or a new release.

## Execution ledger

Status is scoped to the stated source revision. **Measured** means a completed
frozen experiment or public acceptance packet. **Integrated, validation pending**
means source and tests are registered but the row's remaining feature, resource
or performance acceptance is not complete. **Experimental, not promoted** preserves
measured feasibility or rejection without changing production defaults. The
phase plan's broader unchecked PERF items remain open; earlier phase snapshots
must not erase these later scoped results or be mistaken for current test totals.

The September 8 pre-topology correctness checkpoint passes 3,409 default
workspace tests, 3,216 native CLI/Vortex tests (nine manual benchmark or fixture
regeneration tests ignored), and 32 Python harness tests. Formatting and workspace, native-release-
surface and minimal-native Clippy checks pass. This includes the prepared-source,
public spill, empty-result certificate and native compatibility-export fixes;
the isolated Python extension has not been built. The subsequent topology
adapter is integrated and passes a subsequent checkpoint: 3,257 native tests,
3,418 default workspace tests and 50 harness tests, with the same nine native
ignored cases. Formatting and all three Clippy feature configurations pass.
This includes the six actual public integration checks and strict matrix/analyzer
gates. A minimal-build failure exposed writer-only test hooks; their corrected
feature gate passes, and the original failure log is preserved. These are correctness
checks, not new performance results or completion of the acceptance ledger.

The subsequent coalesced-job checkpoint `9152a92b` passes 3,266 native tests,
3,420 default tests, 54 harness tests, formatting and all three Clippy checks.
Its Existing/Auto/Fine Full43 packets each pass 129 complete-result comparisons,
but Auto and Fine regress by 1.34% and 1.43%. Fresh independent held-out matrices
for this revision were not run after the negative performance decision. See
[the parked experiment packet](../benchmarks/native-topology-coalesced-2026-09-08.md).

| Existing queue | Remaining work | Current status and required evidence |
|---|---|---|
| PERF-03/04/05/07/09/10/12 | Execution-aware native artifact topology | **Parked, not promoted:** `a3c62434` grouping and `9152a92b` actual coalesced jobs both regress without material selected-query wins. The latter compares 585 two-original jobs with 1,170 single-original jobs under the same worker/original-output limits; native read and array boundaries remain unchanged. All three `9152a92b` Full43 packets pass 129 complete-result checks, but fresh independent held-out matrices were skipped after rejection. Preserve the frozen experiment and unwired local-scalar/sort-ownership candidates. Further topology, local reduction, local Top-K and execution-aware preparation work is paused under the maintainer's effort constraint |
| PERF-04/05 | Compound numeric/text COUNT | **Measured, retained:** exact complete-key workers and existing in-memory pressure/replay behavior; Q15/Q17 gains and complete public/independent held-out acceptance are recorded for `75fc09a0`. This does not implement worker-to-disk spill handoff. Broader aggregate worker families remain open |
| PERF-04/05/06 | Exact grouped distinct and broader aggregate parallelism | **Measured and merged, scoped:** `48182c5a` records the Q9 gain and RSS tradeoff. Final `572bd52c` passes 80 required-worker checks for exact/repeated integer-distinct TopK at requested 1/2/4/8/12, including matching count-descending/group-ascending admission and provider-driver corrections. Its separate 380 plus 80 JSONL checks establish semantics only. Broader aggregate families, resource envelopes and production worker-to-spill transfer remain open |
| PERF-03/04/05 | Compact aggregate state, string slabs and partition ownership scheduling | **Experimental, not promoted; further work deferred:** 63 compact-state and 84 owner-scheduling pairs complete at `48182c5a`. The 16 KiB high-cardinality reducer comparison improves 18.1%, only about 1.35 ms (7.485 to 6.132 ms), with about 24% less admitted state; repeated values are 27.3% slower with 14.44x state. These exclude complete public-query lifecycle costs. Eight owner lanes beat matching dynamic scheduling but lose to fewer lanes. Universal replacement remains rejected; resume a narrow candidate only after profiling demonstrates a dominant public cost and plausible material lifecycle benefit |
| PERF-03/07/10 | Scan-local compressed segment reuse and consumer fusion | **Measured, retained narrow gate:** completed-read savings for separately addressable fields, with small local latency cost; the full43 Chunked-root artifact does not admit this cache gate. **Measured:** actual prepared filtered-count scans reuse one source without caching answers. Broader filter/project/aggregate fusion and complete allocation/lifecycle acceptance remain |
| PERF-10 | Constant/run/FoR/bit-packed numeric computation | **Measured, retained:** weighted constant/run consumers for admitted encoded inputs; dense fused additive-only RunEnd keeps the faster typed route after measured regressions. **Experimental, not promoted:** bounded native FoR/BitPacked feasibility and 21 release pairs; 8K/32K gains are below 1% on one fixture and 1K regresses. The bounded experiment does not establish a public-query throughput win |
| PERF-08/09 | Column-addressable logical file layout over bounded physical writes | **Experimental:** native writer seam and correctness coverage exist at `48182c5a`. **Integrated, validation pending:** paired complete writer lifecycle and repeated native-consumer benchmark. Ordinary writer/default layout is unchanged. Run bounded lifecycle evidence, then any justified production-size fixed-codec/fixed-CPU ingest-plus-query comparison before promotion |
| PERF-07/11 | Multi-segment memory generations and owned-buffer intake | **Measured, scoped:** native column/row-group generations and owned intake preserve ownership, values and durable reopen. Owned intake avoids its payload-copy step, while explicit segment assembly still copies data; selective leaf requests and direct/generation clocks are retained. Broader ingest-to-result lifecycle, pressure/cancellation and multi-source result acceptance remain |
| PERF-03/08/09 | Ingest worker scaling and bounded cohort overlap | **Matched constructed-owner packet complete:** control `75fc09a0` P2 took 99.446032 seconds; candidate `572bd52c` P4 took 95.447305 seconds, with four confirmed owners each. Values match and both owned outputs were retired. Public requests and prefetch differ; one pair is not a stable or causal speedup claim. September 12 CPU-stage balance and bounded-overlap implementation/testing are queued behind measured bottleneck attribution; see the linked sequence. Earlier unequal-owner observations retain their historical scope |
| PERF-03/09/10 | Joint codec/consumer selection and task-level controllers | **Experimental, not promoted; further work deferred:** 27 files and 10,800 complete native queries on 4,096-row fixtures. Dictionary improves categorical reuse-100 lifecycle 18–28% at 4.23–5.83x artifact bytes; unique Dictionary is slower/larger and FSST has no sustained lifecycle gain. This tiny, high-reuse evidence does not establish a dominant public cost or production-scale benefit. Default Zstd and numeric compression remain. Resume conditional selection only with that evidence; a task-level codec/CPU/memory controller remains unimplemented |
| PERF-02 | Prepared aggregates and retained public execution | **Measured and merged, scoped:** final `572bd52c` passes the expanded 1,674-check resident matrix across nine cases and three public call surfaces, including retained integer COUNT/COUNT DISTINCT/SUM, source reuse, fresh execution state, native certificates and ordinary-route handling. This supersedes the pending nine-case acceptance obligation; the prior log-budget failure is archived and supplies no timing claim. Historical `48182c5a` filtered-count measurements remain separately attributed. Broader prepared families and multi-source results remain open; SUM keeps its existing floating accumulation semantics |
| PERF-02/07 | Native Python binding decision | **Parked, excluded from shipping and not scheduled:** the unbuilt isolated PyO3 prototype and its authored admission/ownership tests remain on the preserved experimental branch. Root Python packaging and worker transport are unchanged. Resume only with new measured justification under the material-benefit constraint; dependency/license, exact-interpreter build, 372-record acceptance and ABI/platform packaging work remain uncompleted |
| PERF-03 | Remaining CPU/I/O/codec and retained-memory admission | **Partially integrated:** existing shared worker/scan grants, parent/child spill reservations and source/result ownership. The held-source grant and provider-driver evidence corrections pass broad and scoped final `572bd52c` acceptance and are merged. Global provider/codec allocation coverage, progress/cancel behavior and coordinated remaining operators remain open; owned-byte accounting is not a process-RSS bound |
| PERF-06 | Native exact-distinct and weighted COUNT spill | **Correctness checkpoint passed:** explicit public exact-distinct and weighted UTF8/optional-integer COUNT routes, separate typed evidence/certificates, same-source/runtime ownership, quota overlap, namespace-specific cleanup, and aggregate row-count/empty-source certificate fixes. Production-size resource/performance acceptance remains. Weighted runs use actual per-run key/block geometry while retaining worst-case admission. Both public routes feed a bounded serial source accumulator; drained worker-prefix/untouched-suffix transfer is test-only. Production pressure handoff and general join spill remain unimplemented |
| PERF-07/10 | Native-array compatibility export and broader physical results | **Measured, retained for the admitted bounded profile at `e739edeb`:** [32 release pairs and four prepared series](../benchmarks/retained-native-export-2026-09-08.md), all 96 complete output checks passing; 65,536-row complete-artifact medians improve 3.528x IPC / 3.523x Parquet. At 4,096 rows IPC improves 11.82% and Parquet regresses 1.45%; raw legacy API is faster there because it omits sync/checksum/reopen. Source-generation guarantees differ; no broader public-scale or RSS win is claimed. Prepared series retain one source open and release owned bytes after drop. Broader materializing/multi-source result composition and production-scale acceptance remain open |
| PERF-11/12 | Bulk-load envelopes and broader held-out acceptance | **Measured, scoped:** final `572bd52c` passes 380 plus 80 JSONL semantic checks, 80 required-worker Parquet checks and 1,674 resident public-call checks. Earlier `48182c5a` 760-check matrices at each row count retain their own timing limits. Broader spill, binding, ingest and resource/latency distributions remain open; these packets establish no broad held-out throughput gain |
| PERF-01/08/12 | Final baseline, timing attribution and lifecycle scorecards | **Final runtime measured and merged:** `572bd52c` passes 129/129 Full43 comparisons with 91.215296-second best-sum versus fresh Existing `9152a92b` at 88.661862 seconds (+2.88%, uncontrolled sequential comparison). No overall throughput win is established. Semantic, required-worker, resident and matched-owner ingest packets are complete. Full43 used the protected reference rather than the new ingest output; new-artifact query and broader lifecycle/resource scorecards remain open |
| PERF-13 | Profile-guided optimization feasibility | **Toolchain feasibility only; further experiments deferred:** guarded orchestration, helper tests and a tiny native exact-result instrument/merge/use smoke pass; the selected-tool lookup correction is tested and merged. No full-workspace instrumented/profile-use build or PGO performance benefit is measured. Require a demonstrated dominant public instruction/dispatch cost before representative training and independent untrained evaluation; build/code-size cost and portable correctness remain open |

The ledger remains open while any implementation, evaluation or declared
acceptance remains. A rejected optimization must retain its evidence and explain
which scope was tested; rejection does not imply that the entire operator family
or phase is complete. No universal sub-millisecond, sub-100-second suite, RSS-bound,
or engine-superiority claim follows from this plan.

Two earlier attachment obligations remain explicit within these open rows:

- **PERF-03/08/09/10:** helper representation work is deferred. The measured
  serial P1 ingest contains 7.441 seconds of helper work within 187.600824 seconds
  total: even deleting that measured stage entirely offers an optimistic 3.97%
  saving, below the >=10% retention gate. The old 110-second helper bottleneck is
  obsolete. This observation does not bound every downstream effect or other
  worker setting, and does not complete the helper/lifecycle phase. Preserve
  complete readback and checksum proof; resume implementation only with new
  evidence of material complete-lifecycle benefit.
- **PERF-02/03/07:** measure file-backed small-query latency during long work in
  the same resident session, then decide whether a bounded concurrency change is
  justified. Existing mixed in-memory intake/JSON measurements include queue
  waits; they do not establish independently executing session lanes. Any change
  must preserve source generations, one shared resource grant and cancellation.

These obligations remain open independently of the parked topology experiment
and do not reopen already rejected universal codec, state or scheduling replacements.

The [first retained checkpoint](../benchmarks/perf-native-continuation-2026-09-06.md)
records runtime `75fc09a0`, the unchanged native artifact, 98.831499-second full43
best sum versus 119.887782 seconds, all 129 complete reference results and all
1,360 independent held-out checks. Eighteen query bests still regress; small
held-out timings are effectively unchanged or slightly higher. Intermediate
arithmetic regressions, raw samples, cache tradeoffs and provider allocation
exclusions remain visible. This frozen comparison remains historical evidence;
later implementation uses the [current control ledger](performance-control-progression-2026-09-12.md).
The score does not close the remaining ledger.

## Next coherent completion points

The canonical phased plan parks further topology investment. The list below
preserves outstanding acceptance obligations from the earlier continuation;
it is not a competing execution queue or a claim that remaining work is complete.

1. Preserve completed September 12 attribution, the rejected allocation and
   retained pipeline tests in the [ingest sequence](ingest-performance-implementation-2026-09-12.md).
   The 118.604707-second candidate failed the then-current 95.447305-second baseline;
   the extra unchanged-control observation of 104.044137 seconds does not replace
   that recorded sample. Advance ingest/query controls as faster retained versions
   complete validation under the [control ledger](performance-control-progression-2026-09-12.md),
   without needless unchanged-control reruns. Writer-batch overlap needs
   measured recoverable subtree-tail work; repeated representation removal needs
   proven duplication and compatible ownership. Neither follows automatically
   from the failed allocation. Preserve source generation, numeric compression
   and the existing guards. Fresh-artifact Full43 now passes all 129 complete
   results on output byte-identical to the fully compared retained candidate;
   metadata identity is established, without an independent statistic oracle or
   paired layout-performance claim. Future changed outputs need their own proof.
   The bounded compatibility-export decision remains retained with its measured
   small-case costs. Wider experiments require a concrete material opportunity.
2. Preserve the completed 1,674-check prepared-aggregate public acceptance and
   keep the isolated native Python experiment parked and excluded from shipping; no build or
   372-record experiment is scheduled without new measured justification.
   Remaining prepared-operator and multi-source result families stay open and
   require scoped native designs and material benefit before implementation.
3. Production worker pressure remains an open requirement, not an automatic next
   experiment: connect it to exact native runs only through a drained
   committed prefix and untouched suffix, with no double counting or stale-source
   mixing; validate memory exhaustion, cancellation and complete global order.
   Existing public serial accumulators and test-only transfer seams do not close
   that task. General join spill needs its own schema, state, probe/merge,
   correctness and recovery contract, beyond the two admitted aggregate families.
4. Defer narrow compact-state and categorical high-reuse codec experiments.
   The bounded compact gain is about 1.35 ms in a reducer comparison, with
   repeated-input regressions; codec benefits come from tiny high-reuse fixtures.
   Preserve the experiment packet and rejected universal replacements. Require
   profiling of a dominant public cost and plausible material complete-lifecycle
   benefit before another experiment or controller implementation.
5. Defer full-workspace PGO experiments under the same value gate. Its successful
   toolchain smoke proves neither workspace feasibility nor speedup. If resumed,
   retain representative training, independent untrained evaluation, and build
   and artifact costs. Preserve the completed ingest disposition and
   document retained, rejected and still unsupported scopes at the next PR
   checkpoint. Do not convert these scoped packets into whole-PERF or CG closure.

## Frozen evidence locations

The local receipt root is
`/Users/dylan/LocalData/shardloom/perf-all-20260906`. Relevant preserved packets:

- `next-full43-analysis.json`: both immutable runtime identities, all three
  samples per query, full-value completion, per-query RSS and preserved
  regressions. Completed candidate summary:
  `clickbench-100m-uat/logs/full43_20260906T140759558798Z/summary.json` under the
  sibling ShardLoom local-data root. The interrupted `T135256432838Z` attempt
  contributes no completed suite score.
- `next-heldout-analysis.json` and the two `heldout-next/logs` summaries: 1,520
  complete independent checks with requested worker grants. These do not prove
  the later integer tie-order worker admission was exercised.
- `resident-next/logs/resident_call_paths_20260906T135143973745Z/summary.json`
  and `next-resident-native-count.json`: frozen filtered-count/public-call-path
  results, distinct from the later prepared-aggregate and native-binding work.
- `release-experiments-48182c5a/analysis-48182c5a.md` and its JSON packet:
  numeric, compact-state, owner-scheduling and codec lifecycle retain/drop
  evidence. The analyzer links raw checksums and records scope asymmetries;
  whole-process peaks are not assigned to individual experimental variants.
  The later bounded-value review defers the analyzer's suggested narrow followups;
  it does not change or promote the frozen experiment.
- `retained-runtime-reviewfix1.json`, `retained-572bd52c-binary.json` and
  `retained-runtime-harnessfix-final-python.json`: final runtime/binary and
  harness identities and serial checks. The linked acceptance document records
  the complete Full43, semantic, required-worker and resident packets and merge.
- `ingest-owner4-final-20260908`: completed guarded control-P2/candidate-P4
  packet, with complete verification and both owned artifacts retired. Its
  [published JSON view](../benchmarks/retained-ingest-owner4-2026-09-08.json)
  preserves the measurements and proof identities.
- `pgo-toolchain-smoke-20260908/report.json`: successful tiny native
  instrument/merge/use compatibility, explicitly no workspace build or speedup.

Raw output compressed to restore guard headroom remains lossless and checksum
verified. A formerly plain raw-output path may now have a `.gz` suffix; compression
does not change the frozen summary or make an interrupted run complete.

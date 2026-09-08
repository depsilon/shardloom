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
sequence. The canonical phased plan now prioritizes independent native regions,
then separate measured local-reduction and local-Top-K stages, and only afterward
execution-aware preparation. Earlier open items below remain tracked, with that
dependency order taking precedence. PRs #1435 and #1436 are merged with all 40 CI
checks passing on each exact head; later integrated changes still need validation.

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
workspace tests, 3,216 native CLI/Vortex tests (nine explicit benchmark tests
ignored), and 32 Python harness tests. Formatting and workspace, native-release-
surface and minimal-native Clippy checks pass. This includes the prepared-source,
public spill, empty-result certificate and native compatibility-export fixes;
the isolated Python extension has not been built. The subsequent topology
adapter remains staged and is outside this checkpoint. These are correctness
checks, not new performance results or completion of the acceptance ledger.

| Existing queue | Remaining work | Current status and required evidence |
|---|---|---|
| PERF-03/04/05/07/09/10/12 | Execution-aware native artifact topology | **Adapter staged, integration and validation pending:** source inventory and independent review are complete. Existing Capillary jobs execute bounded original native split steps; coalesced region identity and region/split/worker limits remain distinct. Native metadata pre-pruning precedes payload task construction. Staged tests cover ordered values, floating consumers, sustained coarse-region concurrency, source generations, drain and pruning. Public wiring, mixed-column/nullable cases and the fixed-artifact matrix remain; later local reductions, exact local Top-K and execution-aware preparation each need their own packet |
| PERF-04/05 | Compound numeric/text COUNT | **Measured, retained:** exact complete-key workers and existing in-memory pressure/replay behavior; Q15/Q17 gains and complete public/independent held-out acceptance are recorded for `75fc09a0`. This does not implement worker-to-disk spill handoff. Broader aggregate worker families remain open |
| PERF-04/05/06 | Exact grouped distinct and broader aggregate parallelism | **Measured:** `48182c5a` passes all 129 full43 results; Q9 best improves 8.412124 to 1.116068 seconds versus `75fc09a0`, with peak process RSS about 1.04 to 4.27 GB. The 19-case matrix passes 1,520 checks, but the two explicit integer-tie-order cases used the older typed route. **Integrated, validation pending:** matching count-descending/group-ascending worker admission and subsequent provider-driver corrections. Repeat actual worker-path held-out and resource acceptance on the final candidate |
| PERF-03/04/05 | Compact aggregate state, string slabs and partition ownership scheduling | **Experimental, not promoted:** 63 compact-state and 84 owner-scheduling pairs complete at `48182c5a`. Compact 16 KiB slabs improve high-cardinality time about 18% and owned state about 24%, but repeated values are slower and use about 14.4x owned state; skew trades speed for memory. Eight owner lanes beat matching dynamic scheduling but lose to fewer lanes. Narrow high-cardinality public-query evaluation remains; universal replacement is rejected by this packet |
| PERF-03/07/10 | Scan-local compressed segment reuse and consumer fusion | **Measured, retained narrow gate:** completed-read savings for separately addressable fields, with small local latency cost; the full43 Chunked-root artifact does not admit this cache gate. **Measured:** actual prepared filtered-count scans reuse one source without caching answers. Broader filter/project/aggregate fusion and complete allocation/lifecycle acceptance remain |
| PERF-10 | Constant/run/FoR/bit-packed numeric computation | **Measured, retained:** weighted constant/run consumers for admitted encoded inputs; dense fused additive-only RunEnd keeps the faster typed route after measured regressions. **Experimental, not promoted:** bounded native FoR/BitPacked feasibility and 21 release pairs; 8K/32K gains are below 1% on one fixture and 1K regresses. The bounded experiment does not establish a public-query throughput win |
| PERF-08/09 | Column-addressable logical file layout over bounded physical writes | **Experimental:** native writer seam and correctness coverage exist at `48182c5a`. **Integrated, validation pending:** paired complete writer lifecycle and repeated native-consumer benchmark. Ordinary writer/default layout is unchanged. Run bounded lifecycle evidence, then any justified production-size fixed-codec/fixed-CPU ingest-plus-query comparison before promotion |
| PERF-07/11 | Multi-segment memory generations and owned-buffer intake | **Measured, scoped:** native column/row-group generations and owned intake preserve ownership, values and durable reopen. Owned intake avoids its payload-copy step, while explicit segment assembly still copies data; selective leaf requests and direct/generation clocks are retained. Broader ingest-to-result lifecycle, pressure/cancellation and multi-source result acceptance remain |
| PERF-03/08/09 | Ingest worker scaling and bounded cohort overlap | **Implemented and tested at `48182c5a`:** shared source/conversion/provider CPU grants and explicit one-worker ceilings. Two of eight initial observations are complete: control requested-one/public-ceiling-two 95.837785 seconds, candidate true-one 187.600824 seconds, with peak RSS 3.05/1.97 GB. These are not equal CPU grants. Candidate bytes differ, but [all 11.2 billion logical values match](../benchmarks/perf-native-artifact-equivalence-2026-09-08.md); only that verified generated artifact was retired. Requested 2/4/8, informative repeats and any justified overlap/controller tuning remain, after topology's current priority |
| PERF-03/09/10 | Joint codec/consumer selection and task-level controllers | **Experimental, not promoted:** 27 real files and 10,800 complete native queries with publication and reuse-1/10/100 clocks. Dictionary improves categorical reuse-100 lifecycle 18–28% at 4.23–5.83x artifact bytes; unique Dictionary is slower/larger and FSST has no sustained lifecycle gain. Default Zstd and numeric compression remain. Conditional categorical selection requires production-scale lifecycle evidence; a task-level codec/CPU/memory controller remains unimplemented |
| PERF-02 | Prepared aggregates and retained public execution | **Measured at `48182c5a`:** 1,116 complete resident public calls; filtered-count worker p50 0.683 to 0.483 ms, native prepared filtered-count p50 38.75 microseconds over 10,000 actual executions. **Integrated, validation pending:** retained integer COUNT/COUNT DISTINCT/SUM lowering and source, fresh per-execution state, native certificates, optional CLI-worker reuse, and a single-open ordinary route when retention admission fails. Aggregate and expanded nine-case public-call-path measurements remain; SUM keeps its existing floating accumulation semantics |
| PERF-02/07 | Native Python binding decision | **Isolated prototype, validation pending:** safe PyO3 adapter for retained metadata count, actual filtered count, bounded native-array projection and explicit JSON conversion; held-provider/file-size admission and ABA race tests are authored. Root Python packaging and worker transport are unchanged. Resolve the isolated dependency/license graph, build/load the exact-interpreter extension and Rust control, run ownership/invalidation tests and 372 complete acceptance records, then make a measured integration decision. ABI/platform packaging is a separate later boundary |
| PERF-03 | Remaining CPU/I/O/codec and retained-memory admission | **Partially integrated:** existing shared worker/scan grants, parent/child spill reservations and source/result ownership. The active provider-driver admission/evidence correction needs batch validation. Global provider/codec allocation coverage, progress/cancel behavior and coordinated remaining operators remain open; owned-byte accounting is not a process-RSS bound |
| PERF-06 | Native exact-distinct and weighted COUNT spill | **Measured/tested foundation:** shared real Vortex query-run store and private exact-distinct runs. **Integrated, validation pending:** explicit public exact-distinct and weighted UTF8/optional-integer COUNT routes, separate typed evidence/certificates, same-source/runtime ownership, quota overlap and namespace-specific cleanup. Earlier DISTINCT SQL/DataFrame full-value acceptance passed, but later aggregate row-count/empty-source certificate fixes and the complete weighted batch are not yet validated. Weighted runs use actual per-run key/block geometry while retaining worst-case admission. Both public routes feed a bounded serial source accumulator; drained worker-prefix/untouched-suffix transfer is test-only. Production pressure handoff and general join spill remain unimplemented |
| PERF-07/10 | Native-array compatibility export and broader physical results | **Integrated, validation pending:** bounded native-array IPC/Parquet export, explicit compatibility/Arrow boundary evidence, source-generation and target-publication protections, and full-value regression coverage for renamed projections with predicates and limits. Lost-predicate and zero-result evidence fixes await the full batch gate. Paired actual legacy/export lifecycle benchmarks are authored but unmeasured; broader materializing/multi-source result composition remains open |
| PERF-11/12 | Bulk-load envelopes and broader held-out acceptance | **Measured:** both 4,096- and 131,072-row `48182c5a` matrices complete with 760 checks each at requested 1/2/4/8/12 workers. Aggregate median sums are roughly 1.0% and 0.8% higher, not a broad held-out throughput gain. Final-candidate exact worker-path, spill, export, prepared/binding, ingest and resource/latency distributions remain required |
| PERF-01/08/12 | Final baseline, timing attribution and lifecycle scorecards | **Measured at `48182c5a`:** full43 best sum 91.662289 seconds versus `75fc09a0` 98.831499 seconds (7.25% lower); geometric mean 4.81% lower, with 21 query bests improving and 22 regressing. All 129 complete retained-reference results pass. The guard-interrupted attempt is preserved and excluded. A clean final integrated candidate still needs same-commit ingest, full43, independent held-out/public-surface and lifecycle evidence with regressions and RSS visible |
| PERF-13 | Profile-guided optimization feasibility | **Integrated orchestration, executable smoke passed:** guarded fresh profile ownership, matched build flags and instrument/train/merge/use stages; September 8 tiny native exact-result smoke confirms the installed compiler/profdata pair can work. The earlier helper-test failure log is retained; rerun the 14 helper tests with a passing receipt. Full matched workspace control/instrumented/profile-use builds, representative training, independent untrained evaluation, build/code-size cost and portable correctness remain. No PGO performance benefit is measured |

The ledger remains open while any implementation, evaluation or declared
acceptance remains. A rejected optimization must retain its evidence and explain
which scope was tested; rejection does not imply that the entire operator family
or phase is complete. No universal sub-millisecond, sub-100-second suite, RSS-bound,
or engine-superiority claim follows from this plan.

The [first retained checkpoint](../benchmarks/perf-native-continuation-2026-09-06.md)
records runtime `75fc09a0`, the unchanged native artifact, 98.831499-second full43
best sum versus 119.887782 seconds, all 129 complete reference results and all
1,360 independent held-out checks. Eighteen query bests still regress; small
held-out timings are effectively unchanged or slightly higher. Intermediate
arithmetic regressions, raw samples, cache tradeoffs and provider allocation
exclusions remain visible. Later implementation continues against this frozen
control; the score does not close the remaining ledger.

## Next coherent completion points

The canonical phased plan's topology stages now precede new local-operator and
writer/controller tuning. The list below preserves the outstanding acceptance
obligations from the earlier continuation; it is not a competing execution queue.

1. Validate the entire integrated source batch: default/native/minimal feature
   builds and tests; exact/weighted spill certificates and cleanup; export
   predicates and values; prepared aggregate/session reuse; source identity and
   provider-driver accounting. Freeze a clean revision before attributing new
   measurements to it. Passing `48182c5a` checks do not validate these later edits.
2. Finish the matched ingest curve, repeat informative worker settings, and use
   actual stage/CPU/memory evidence to select any bounded cohort overlap or
   ordered-writer/controller change. Preserve complete artifact identity and
   storage guards. Run column-layout and compatibility-export lifecycle packets;
   promote a layout only after justified fixed-codec/fixed-CPU production-scale
   ingest-plus-query acceptance.
3. Build and validate the isolated native Python experiment, then run its 372
   complete records and the expanded prepared-aggregate public latency matrix.
   Decide native binding integration from actual native return, explicit sink,
   complete Python return, preparation and ownership evidence. Remaining prepared
   operator and multi-source result families still require scoped native designs.
4. Connect production worker pressure to exact native runs only through a drained
   committed prefix and untouched suffix, with no double counting or stale-source
   mixing; validate memory exhaustion, cancellation and complete global order.
   Existing public serial accumulators and test-only transfer seams do not close
   that task. General join spill needs its own schema, state, probe/merge,
   correctness and recovery contract, beyond the two admitted aggregate families.
5. Evaluate the remaining narrow high-cardinality compact-state and categorical
   high-reuse codec candidates on actual public queries and full lifecycle costs.
   Keep the measured universal replacements rejected. Extend fused physical
   consumers and common resource admission where the evidence identifies useful
   work; avoid adding a controller merely because its counters exist.
6. After structural changes stabilize, run matched full-workspace PGO training
   and independent untrained evaluation, including build and artifact costs.
   Finish the same-commit ingest/full43/held-out/public-surface scorecard and
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
- `pgo-toolchain-smoke-20260908/report.json`: successful tiny native
  instrument/merge/use compatibility, explicitly no workspace build or speedup.

Raw output compressed to restore guard headroom remains lossless and checksum
verified. A formerly plain raw-output path may now have a `.gz` suffix; compression
does not change the frozen summary or make an interrupted run complete.

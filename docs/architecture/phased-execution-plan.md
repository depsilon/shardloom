# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work follows the `Current autonomous
  execution order` list below. Completed implementation rows that only await post-merge ledger
  movement must not block the next implementation dependency.
- Use one top-level unchecked checkbox per active item or promoted child slice. Every top-level
  item must include an `Execution checklist:` with nested checkboxes for the concrete substeps that
  make progress visible. Keep acceptance, evidence, boundaries, and verification as plain bullets.
- Use nested checklist boxes only for verifiable work: implementation, tests, generated evidence,
  docs/site updates, CI/benchmark refreshes, and ledger movement. Do not use vague checklist rows
  such as "continue work" or "investigate more" without a named evidence output.
- Every new ShardLoom runtime, support, release, benchmark, or user-surface item must include a
  `ShardLoom technique review:` bullet. That review must explicitly consider whether PulseWeave,
  capillary work units, dynamic admission/work shaping, metadata-first execution, route timing
  surface separation, or evidence-tier controls apply. If none apply, say why. This prevents net
  new features from being designed in a generic way that later needs avoidable refactoring to use
  ShardLoom's own performance and evidence techniques.
- Prefer a small number of reusable Vortex-normalized execution families over route proliferation.
  Public method names, SQL spellings, and CLI aliases may keep distinct labels for user clarity, but
  implementation should collapse aliases into shared planner/runtime/sink contracts whenever the
  source state, operator semantics, materialization boundary, and evidence fields are the same.
  Because ShardLoom is pre-public-use, do not preserve awkward legacy route splits for compatibility
  alone; preserve only the boundaries that make correctness, diagnostics, or evidence clearer.
- Public Python, SQL, DataFrame-style, and CLI surfaces are wrappers over the same admitted runtime
  families, not separate engines. New plan items must state which shared runtime family they lower
  into, how aliases converge, and which evidence fields prove `fallback_attempted=false` and
  `external_engine_invoked=false`. Do not create parallel capability rows for each front door when a
  shared planner/operator/sink contract is the real behavior.
- Treat the user's surface choice as preference-level syntax after source admission. SQL text,
  Python lazy calls, DataFrame-style method chains, and CLI commands may have small parsing or
  ergonomics differences, but they must converge before execution on the same Vortex-normalized
  physical plan, state budget, sink, and evidence vocabulary wherever semantics match. ClickBench
  UAT optimizations are therefore only acceptable when they benefit that shared runtime path and
  are visible to the other user surfaces through the same route evidence.
- Treat input and output formats as adapter boundaries around a Vortex-normalized middle. CSV,
  JSONL/NDJSON, Parquet, Arrow IPC, Avro, ORC, Vortex, generated rows, ranges, and future sources may
  need source-specific parse/scan/write policy, but they should not receive independent user-surface
  execution stacks unless the semantics, materialization boundary, or safety evidence is genuinely
  different. Future entries must check universal ingest, SourceState/prepared-state reuse, native
  Vortex scan/provider surfaces, and declared sink contracts before adding new route names.
- Smoke-only commands, fixture caps, and test harness shortcuts are not production routes. Keep them
  only as internal/dev safeguards with explicit names and diagnostics. A future item that touches a
  public workflow must either route through the product Vortex-normalized/prepared/native path or
  implement that path; it must not raise a smoke cap, expose a smoke route as product support, or
  count smoke success as runtime readiness.
- Local transport optimizations, including the session-scoped Python worker, are transport layers
  only. They must dispatch the same command handlers, return the same typed envelopes, preserve
  route/evidence fields, and never be recorded as a separate execution provider or benchmark route.
  Plan items involving package, Python, or managed-environment performance must distinguish
  transport overhead from engine/runtime timing.
- Benchmark and UAT entries must separate official engine timing from wrapper ergonomics. ClickBench
  or other external benchmark submissions should time the ShardLoom CLI/runtime path unless a
  separate wrapper-specific entry is intentionally declared; Python UAT proves public API parity,
  no-fallback evidence, and wrapper overhead, not the primary engine ranking by default.
- Heavy local replacement-ingest UAT, full 43-query ClickBench UAT, and full workspace/release
  gates run at the end of a cohesive implementation batch, not after every intermediate
  optimization. While runtime rows are still changing, use focused unit/integration checks and
  targeted probes only when they are needed to ship/drop a specific technique.
- Performance optimization items must be decision-gated, not open-ended. Each target must record
  the current measured timing or cost signal, the dominant cost class, the shared runtime component
  to improve, the proposed fix, the retain/drop threshold, and the exact evidence that decides
  whether the technique ships, is revised, or is removed. Do not retain a slower optimization because
  it is architecturally interesting.
- Performance fixes must improve shared ShardLoom/Vortex-normalized components rather than
  one-off query routes. If a targeted ClickBench lane motivates the work, the implementation still
  belongs in reusable ingest, metadata, dictionary, encoded predicate, aggregate, top-K, writer,
  sink, or evidence components unless a documented semantic boundary proves otherwise.
- Performance fixes must prefer shared/reused components over parallel implementations. A
  source-specific adapter may tune read/decode policy, but once data reaches the Vortex-normalized
  middle it should reuse the same prepared-state, writer, segment-layout, metadata, physical-plan,
  operator, sink, and evidence helpers wherever semantics allow. Do not create CSV/Parquet/JSONL,
  SQL/Python/DataFrame, benchmark/UAT, or ClickBench-only variants for the same runtime behavior.
- The October 2 modularity clarification requires a concrete reuse map for each new shape or
  optimization lane: existing component and callers, remaining semantic gap, shared extension or
  strategy, and verification across existing and composed workloads. Sharing a crate or allocator
  alone is insufficient proof of algorithm reuse. Preserve justified semantic/physical strategies
  inside shared components and apply the measured retain/drop gate to performance changes.
- Focused validation entries must use exact test targets before broad gates. Rust unit filters must
  target the exact crate surface: `cargo test -p <crate> --bin <name> <filter>` for binary crates
  and `cargo test -p <crate> --lib <filter>` for library crates. Rust integration filters must use
  `cargo test -p <crate> --test <target> <filter>`, and Python checks should name the concrete
  unittest module/class/test. Prefer `python3 scripts/run_focused_checks.py` profiles for local
  agent work. Do not use bare package-level Cargo filters as focused proof because Cargo still
  enumerates integration test targets and creates avoidable slow-tail work.
- When a maintainer-provided list, audit, attachment, benchmark finding, or review packet proposes
  new work, review each candidate before adding it here. Classify it as already addressed,
  accepted into a new checklist, merged into an existing checklist, v1 candidate pending
  feasibility, deferred beyond the current product scope, or rejected with a reason. Do not paste
  broad lists verbatim into Planned.
- Production-shift items must state whether they are `required_for_v1`,
  `v1_candidate_pending_feasibility`, `deferred_out_of_v1`, `documentation_only`, or
  `unsupported_boundary`. The October 21 scope decision supersedes feasible-by-default v1
  inclusion: requirements come from concrete comparability with alternative compute engines on
  selected workloads, plus the correctness, resource, safety and support promises for those paths.
  Feasibility, an internal technique, or an incumbent feature list alone does not make work required.
- Bind each required addition to a named comparison task, an independently supported baseline
  equivalent, declared semantics/scale/resources and the gap that prevents useful substitution.
  Existing valid evidence can satisfy a requirement without new implementation. Keep unsupported
  behavior deterministic; do not turn optional feature coverage into mandatory implementation.
- Additional breadth is `deferred_out_of_v1`, stays at the bottom of Planned and may never be
  implemented. It is not an autonomous work queue or a release blocker. Promote a specific item
  only for an evidenced competitive gap or a new explicit maintainer choice, with its scope and
  acceptance recorded first. Preserve correctness of already supported paths when deferring work.
- Leave the top-level item unchecked until every required nested checkbox is checked, validation is
  recorded, unsupported paths remain explicit, and the completed summary has been moved to the
  completed ledger after merge or session completion.
- When a nested checkbox becomes too large for one coherent PR/session, promote it to its own
  top-level Planned item and replace the nested row with a link to that promoted item.
- Move a completed item summary to the completed ledger after merge or session completion. The
  ledger entry must name the closed checklist, evidence commands/artifacts, PR or commit, claim
  boundary, and any residual work that was promoted to a new Planned item.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Planned Item Detail Standard

Every unchecked Planned item must be executable by an autonomous Codex session without guessing.

Each item should name:

- Source: governing RFC, architecture doc, benchmark report, issue, PR, or review finding.
- Current state: what exists today and what is still unsupported, diagnostic-only, or report-only.
- Intake review: for externally supplied lists or audits, which candidate rows were accepted,
  merged with existing work, already addressed, or deferred, and why.
- V1 scope classification: `required_for_v1`, `v1_candidate_pending_feasibility`,
  `deferred_out_of_v1`, `documentation_only`, or `unsupported_boundary` for
  production-shift items.
- ShardLoom technique review: whether PulseWeave, capillary work units, dynamic admission/work
  shaping, metadata-first execution, timing-surface separation, or evidence-tier controls apply; if
  not applicable, the item must explain why.
- Execution checklist: nested checkbox rows for the concrete implementation, test, evidence,
  benchmark, docs/site, and ledger steps needed to close the item.
- Next outcome: the concrete result expected from the next cohesive PR/session.
- User-visible surface: CLI, Python, benchmark, docs, API, capability view, evidence artifact, or
  release gate.
- Implementation scope: files, modules, commands, and generated artifacts expected to change.
- Evidence required: correctness, benchmark, execution-certificate, Native I/O, materialization,
  decode, policy, no-fallback, release, security, or website evidence as applicable.
- Acceptance: observable conditions that make the item done.
- Verification: exact tests, validators, benchmark reruns, snapshots, or build commands expected.
- Non-goals: what must not be implemented in the slice.
- Claim boundary: what can and cannot be claimed after completion.
- Fallback boundary: expected `fallback_attempted=false` and `external_engine_invoked=false`
  behavior.
- Ledger rule: completed detail moves to
  `docs/architecture/phased-execution-completed-ledger.md`.

Do not leave planned work as a bare statement such as "`<thing>` remains incomplete." Convert broad
items into evidence-bearing implementation slices. Split a Planned item only when one coherent
reviewable PR/session would be unsafe, blocked by an external dependency, or too broad to validate.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests or validators exist, evidence refs are attached where claims are made, unsupported
paths remain explicit, no fallback engine was invoked, completed details are moved to the ledger,
and supporting docs are updated without becoming a second active queue.

Section-completion rule:

- Prefer one substantial PR/session that completes an entire runtime section over tiny row, format,
  or operator slivers.
- Split only for concrete safety, dependency, generated-artifact, or verification boundaries.
- For a section-completion PR, derive the full checklist from the owning item, companion runtime
  equivalent, status/capability files, route taxonomy, tests, and user-visible surfaces before
  editing.
- Avoid wording such as "promote one format/operator at a time" unless that format or operator has a
  separate dependency or deterministic blocker.

No item may create or imply a public claim unless it explicitly lists the evidence that supports the
claim. Performance, superiority, Spark-displacement, production, SQL/DataFrame, object-store,
Foundry, REST, live/hybrid, and package-release claims require workload-scoped evidence and release
gates. If evidence is missing, the item must say `claim_gate_status=not_claim_grade` or
`support_status=unsupported|blocked|report_only`.

Status reading order:

1. Planned: next work in logical implementation order.
2. Completed ledger: recently finished sessions first, then historical provenance ledgers in
   `docs/architecture/phased-execution-completed-ledger.md`.
3. Competitive Engine Gate detailed checklists: attribution detail only; promote new actionable work
   into Planned before implementation.

## Architecture Document Ownership

- This file is the mutable source of truth for planned sequence, deferred work, and CG closeout
  ordering.
- `docs/architecture/phased-execution-completed-ledger.md` is the mutable source of truth for
  detailed session history and historical phase ledgers.
- `docs/architecture/global-architecture-review.md` may carry global audit rows, but actionable
  implementation must be promoted here before execution.
- Supporting docs may contain rationale, inventories, traceability, and historical notes, but they
  must not introduce a second current queue.
- Repeated support, claim-boundary, benchmark-interpretation, and runtime-state explanations should
  be owned by one canonical doc or generated data artifact; other pages should link to or render
  that source instead of restating parallel wording.

Reference index:

- Status source: `README.md`, `docs/architecture/phased-execution-completed-ledger.md`,
  `docs/architecture/rfc-phase-traceability.md`, `docs/architecture/global-architecture-review.md`,
  `docs/architecture/compute-engine-flow-reference.md`, and
  `docs/architecture/website-current-state-public-reference.md`.
- Benchmark and route evidence:
  `docs/architecture/performance-attribution-and-execution-structure.md`,
  `docs/architecture/benchmark-suite-catalog.md`,
  `docs/architecture/benchmark-competitive-claim-evidence.md`,
  `docs/architecture/benchmark-persistent-runner-decision.md`, and `docs/benchmarks/*`.
- Runtime optimization references:
  `docs/architecture/clickbench-ingest-optimization-ledger.md`,
  `docs/architecture/runtime-evidence-level-tiering.md`,
  `docs/architecture/evidence-aware-logical-optimizer.md`,
  `docs/architecture/vortex-scan-pushdown-completion.md`,
  `docs/architecture/compressed-encoded-kernel-registry.md`,
  `docs/architecture/fused-operator-pipeline.md`,
  `docs/architecture/in-process-session-runtime.md`,
  `docs/architecture/io-reuse-and-fanout-architecture.md`,
  `docs/architecture/allocation-buffer-pool-optimization.md`,
  `docs/architecture/dynamic-work-shaping.md`,
  `docs/architecture/pulseweave-runtime-control.md`,
  `docs/architecture/cold-ingestion-preparation-research-carryforward.md`,
  `docs/architecture/universal-input-contract.md`,
  `docs/architecture/vortex-adapter-integration-plan.md`, and
  `docs/architecture/vortex-runtime-utilization-audit.md`.
- Claim, release, package, and adoption references:
  `docs/architecture/bayesian-performance-layout-advisor.md`,
  `docs/architecture/best-default-certification-gate.md`,
  `docs/architecture/operational-evidence-policy-hardening.md`,
  `docs/architecture/engine-replacement-claim-inventory.md`,
  `docs/architecture/spark-displacement-benchmark-evidence-matrix.md`,
  `docs/architecture/comparative-rerun-managed-platform-posture-gate.md`,
  `docs/architecture/substrait-report-only-contract.md`,
  `docs/release/per-claim-evidence-attachment-matrix.md`,
  `docs/release/ci-work-shaping.md`,
  `docs/release/release-architecture-tracker-gate.md`,
  `docs/release/final-release-rehearsal.md`, and `docs/release/*`.

Reference-doc rule: these files are evidence, guardrails, or inventories. They do not authorize
runtime behavior, support claims, dependency expansion, package publication, external effects, or
fallback execution unless a matching unchecked item below is completed with evidence and moved to
the ledger.

## Planned

Current autonomous execution order:

**October 28 I/O and version milestone clarification:** item 20 remains active.
Then finish item 18 with shared demand-aware reading as the ordinary default
across admitted formats and all five execution surfaces. Its
[acceptance gate](native-universal-io-performance-coverage-2026-10-24.md#cross-surface-and-format-acceptance-gate)
requires matched correctness, engine timing and I/O scenario coverage, with
material avoidable performance gaps resolved or explicitly accepted before the
**1.1.0 minor version bump immediately after item 18**, with the required
README, field-guide and website refresh, generated-surface alignment, and
documentation/site/example checks completed at item-18 closeout before the
bump. Item 19 follows that milestone, then Foundry/item 21 and 1.2.0, then
AI/item 22 and 1.3.0. These are planned milestones; no version source changes
or new publication occur here.
Necessary format costs, unsupported combinations and unavailable measurements
remain explicit, and CG-1 through CG-23 keep their independent gates.

**October 26 Parquet continuation:** after the first source-demand adapter is
accepted and merged in #1612 at `81ab0a4d`, the maintainer makes the **347-case suite
the primary performance and acceptance target**. Complete the remaining
[Parquet source-demand work and justified shared optimizations](native-parquet-demand-completion-2026-10-26.md)
before expanding to other formats. Retain the earlier focused observations as
history and useful correctness coverage, without routinely repeating their
timing suite. Keep the existing Polars observations fixed and their input/clock
differences explicit. Use one ordinary query model; explicit durable Vortex
creation and full-source validation keep their separate guarantees. Item 20 and
CG-1 through CG-23 remain open. The [October 27 completion](../benchmarks/native-parquet-demand-completion-2026-10-27.md)
is locally accepted: all 347 cases, 12,565 complete rows, required native/workspace
checks and 185 installed-package tests pass. Engine time is 34.960427343 seconds
versus 50.877676708, with zero prospective material-regression flags. Selected
chunk reads, conservative prefixes, directory/content identity and shared typed
grouped aggregation are complete for this bounded unit; single observations do
not establish external-engine superiority.

**October 26 maintainer override:** item 17 is complete and merged in #1610 as
`c287ba45`, following #1609 as `931bf814`; all 54 and 57 respective PR checks
pass. Completed generated payload cleanup is recorded. **Item 20 is next and
has started**, expanded into the [broad query program](native-broad-query-program-2026-10-26.md).
Expand and validate ShardLoom's own surface. A later October 26 instruction permits
one Polars run per selected case to prioritize gaps; other-engine testing remains
deferred. Compare matching complete API boundaries and keep native engine clocks separate.
Preserve strong CLI SQL, embedded workflow, Python SQL, DataFrame and typed native
expression parity through shared modular implementation and test definitions.
Use targeted family tests as the main loop and retain ClickBench as an existing
regression guard. New findings belong in the next PR. Item 18's unfinished source and evidence are preserved on
`codex/universal-io` at `87b5a174`; it is not accepted or a prerequisite for item
20. Items 18 and 19 remain requested work pending resequencing. This override
supersedes the earlier immediate ordering below without renumbering item IDs,
restoring dropped mechanisms or changing no-fallback and CG boundaries.

**October 26 foundation prerequisite complete:** #1611 is merged as `4d25a06e`
after all 57 hosted checks pass at `a9cbcb67`, with an identical merge tree.
The [compression and replay corrections](compression-dependency-intake-2026-10-26.md)
and [integration evidence](../benchmarks/evidence/native-broad-query-review-2026-10-26/pr1611-integration.json)
preserve original failures and benchmark attribution. The first
[universal source-demand work unit](native-source-demand-2026-10-26.md) is now
accepted for Parquet through common planning and native batch owners, and merged
in #1612 at `81ab0a4d`; it is not released. The
[acceptance report](../benchmarks/native-source-demand-2026-10-26.md) records
the corrected 347-case checkpoint and complete validation. This does not
accept item 18, change the Arrow/Vortex major graph, waive an advisory or publish
a package.

**October 25 continuation:** PR #1605 is merged as
`167ac248ba662ae7967a7801995e9b160b17bae5`, including the accepted
[CI performance and coverage-preservation batch](../release/ci-performance-2026-10-25.md#completed-remote-acceptance).
All 57 checks pass; the completed ledger records integration and the complete
CI comparison. Item 15 has since been closed as dropped after its combined
Full43 gate failed; all runtime edits were removed and source restored to
`167ac248ba662ae7967a7801995e9b160b17bae5`. Item 16 closes without a runtime
change after current-main ingestion and source review. The maintainer then
requests proceeding through the **1.0.0 version bump train** if no worthwhile
additional change is established. The
[1.0.0 train is complete](../release/v1.0.0-publication-verification.md) through
the GitHub, TestPyPI, PyPI and Homebrew sequence, with fresh package and channel
proofs, merged documentation, verified public-site content and generated-file
cleanup. Connected programming and universal I/O follow. Optional backlog,
blocked prerequisites and CG-1 through CG-23 retain their existing boundaries.

The October 18 [public documentation and API consolidation](public-documentation-api-acceptance-2026-10-18.md)
is implemented and locally validated under PERF-10/07/11/12 with PERF-03 resource
admission preserved. Its API is included in the published v0.7.0 packages.
All four selected channel proofs pass; the [publication record](../release/v0.7.0-publication-verification.md)
tracks documentation integration and website verification separately.
The completed scope is in the
ledger. The subsequent [compact-selection experiment](native-compact-selection-2026-10-18.md)
is measured and dropped: two projection regressions repeat under the frozen
confirmation gate, and production is restored. Its corrected cost harness and
evidence remain. The subsequent [distinct-set membership acceptance](../benchmarks/native-set-membership-spill-2026-10-18.md)
completes finite `UNION`/`INTERSECT`/`EXCEPT` pressure and cleanup under explicit
spill. The [finite unary membership acceptance](../benchmarks/native-unary-membership-spill-2026-10-18.md)
also completes distinct, first/last/all deduplication and duplicate masks under
their original operator-pressure grants. The [finite sampling population acceptance](../benchmarks/native-sampling-population-spill-2026-10-19.md)
now completes the six admitted weighted/unweighted population variants at their
original 16-MiB pressure grant. The [finite subquery predicate acceptance](../benchmarks/native-subquery-membership-spill-2026-10-19.md)
also completes shared relational `IN`, `EXISTS` and quantified `ANY`/`ALL`
state with original 4-MiB native and 8-MiB public pressure controls. Other
sampling strategies, legacy standalone unary state and broader recovery remain
open. The earlier conditional-first pass has seventeen original decisions,
four missing-prerequisite records and two retained follow-ups. The latest
October 21 maintainer decision supersedes the blanket requirement to complete
all 37 breadth substeps. **Requirements are determined by useful comparability
with alternative compute engines. Additional breadth is optional, belongs at
the very bottom of the queue, and may never be implemented.** The
[conditional sequence](native-conditional-investigations-2026-10-19.md) retains
the original decisions; it is not authority to expand the required scope.

The [format strategy parity correction](native-format-strategy-parity-2026-10-21.md)
is now merged in PR #1583 as `19f1892bea8aac694133c2079179bf1363b9d5ca`, after
all 44 required checks passed. Its shared selection, scalar collect/write parity,
preparation reuse and original Parquet/ORC reproductions are accepted. The
original isolated Full43 remains **48.754238077 engine seconds** for 43 queries
and all 381 rows. The preceding 49.990920425-second observation and every earlier
writer result remain unchanged; no broader performance claim follows.

The October 24 maintainer instruction requires a PR between queue categories.
Finish each coherent category's implementation, checks and evidence, then open
its PR before starting the next category. Stack on an unmerged dependency when
needed. The subsequent October 24 instruction delegates merge/close handling
and requires all PRs to be merged or closed before resuming the queue. Package
publication still requires separate authorization.

The October 25 maintainer instruction explicitly combines items 13 and 14 in
PR #1605. Complete their combined validation before merging, then proceed to
item 15. The maintainer later drops the whole item 15 batch, including all
unstarted branches and packages; item 16 now heads the active queue. The PR
exception does not merge later categories.

1. **Complete:** documentation integration and deployed-website verification for the authorized v0.7.0 train, following the GitHub, TestPyPI, PyPI and Homebrew proofs. The [publication record](../release/v0.7.0-publication-verification.md) binds PR #1585's 44 passing checks and the byte-exact production observation. The maintainer's subsequent README/field-guide audit and homepage materialization, admission and lifecycle polish precede resuming the queue below; they do not add engine scope or a package release. Subsequent trains require their own authorization. Do not convert the format correction into a broader performance claim, a first-use adapter rewrite or optional breadth work.
2. **Complete for the selected review scope and integrated:** the [workload gap matrix](../benchmarks/native-workload-comparisons-2026-10-22.md), [selected guarantee acceptance](../benchmarks/native-workload-guarantees-2026-10-22.md) and [independent Full43 correction](../benchmarks/native-independent-full43-2026-10-22.md) are merged in PR #1589 as `3d07013fb12cf21e6040adb03f9e97e995996875`, after all 44 required checks passed. The [integration record](../benchmarks/evidence/native-current-attribution-2026-10-22/pr1589-integration-958.json) preserves the reviewed head and identical merge tree; no benchmark was rerun for integration. Finite acceptance covers the 22-task matrix, named guarantee reviews, independently checked Full43 values, integer/regex corrections and Q29 dictionary admission. It does not close all fifteen required owner contracts or CG-5/CG-6. Extra types, operators, platforms and implementation techniques do not become mandatory because they are feasible.
3. **Finite prerequisite review decided:** [current attribution](../benchmarks/native-current-attribution-2026-10-22.md) is verified, and the [bounded scalar-region candidate](../benchmarks/native-scalar-regions-2026-10-22.md) is dropped under its original benefit and metadata-control gates. Exact local reduction is not retained. All 1,922 Rust/Cargo source hashes are restored to the accepted baseline; its isolated standard Full43 passes at **48.479745297 engine seconds**. The incomplete held-out experiment remains explicit, with 226 of 267 planned candidate-experiment calls executed and 41 unrun. This finite outcome does not establish the missing region/preparation cost basis or promote an entire worker/state family into a requirement.
4. **Finite PGO investigation closed; broader queue paused:** the [October 23 PGO decision](../benchmarks/native-pgo-2026-10-23.md) records a drop with the frozen ingest gate failed and complete-workflow benefit unavailable. Query, held-out, pressure and package evidence remains; no release default changes. `PERF-10.ordered_candidates`, `PERF-09.design` and `PERF-12.topology` remain blocked after region rejection, not completed experiments. The maintainer pauses the remaining required and conditional queue to focus on polish; this does not close all fifteen owner contracts or any CG gate.
5. **Cleanup and structural compute review recorded:** the [October 23 review](native-compute-structure-review-2026-10-23.md) retires verified generated payloads and identifies finite output demand, aggregate-consumer reuse, prepared-expression reuse and common plan normalization as candidates. It finds no proved-unused production code and makes no measured speedup claim. Full43 covers primitive routes, so composed improvements also need a targeted composed workload. The review recommends output demand first; it does not start an implementation or resume the paused queue. Freeze a finite correctness/benefit decision before a selected implementation, then run one isolated standard Full43 after the cohesive change. Rejected mechanisms remain rejected.
6. **Shared provider and surface correction locally accepted:** the [October 23 acceptance](../benchmarks/native-shared-surfaces-2026-10-23.md) connects equivalent declarations to existing native providers, closes the corresponding surface omissions and expands current-family coverage beyond ClickBench. All five isolated Full43 passes validate 215 complete results, matching schemas and the previously missing provider evidence. The [completed ledger](phased-execution-completed-ledger.md) records the source and proof. This satisfies the maintainer's prerequisite for returning to targeted optimizations; freeze each selected candidate's finite correctness/resource/benefit decision before implementation. Optional breadth, rejected mechanisms and independent CG gates remain unchanged. This local acceptance does not claim hosted integration or package publication.
7. **Shared-operator optimization feedback locally accepted:** the [October 24 acceptance](native-engine-optimization-acceptance-2026-10-23.md#final-coherent-acceptance) retains value-only preparation, adjacent measure reuse, cheaper sort comparisons, finite sort demand, prepared expressions, a shared numeric aggregate consumer and unique-key existence joins. All frozen benefit/control gates, eight final validation stages and the final isolated Full43 pass; the latter records **51.333297036 engine seconds** for 43 queries and all 381 independent rows. The [completed ledger](phased-execution-completed-ledger.md) links compact evidence and verified retirement. H1/H2, the unsafe first sort-copy attempt and the first join retention failure remain recorded under their original gates. Historical cross-surface attribution remains limited by missing strategy/worker/read fields. This closes the authorized local feedback batch, without hosted integration, package publication, optional breadth or independent CG-gate closure.
8. **Grouped preparation campaign complete:** the [October 24 acceptance](native-grouped-preparation-2026-10-24.md#final-coherent-acceptance) retains complete-key worker routing and repeated-count numeric blocks. Their 52-call cohorts improve Q19's paired median from 4.370869207 to 3.580011250 engine seconds and Q28's from 2.315951250 to 2.092562459. G3's 58-call text-reuse cohort improves Q23 but fails Q19/Q28 controls and is removed under the frozen gates. Q29/Q34/Q35 source review selects no further justified mechanism; H1/H2 remain rejected. Final default/native checks pass and one isolated Full43 on the retained G1/G2 runtime validates all 381 independent rows and schemas at **49.503497253 engine seconds**, 43 queries once each. This exceeds the earlier 48.693636581 and G1 48.718933086 observations, so no overall Full43 speedup is claimed. Portable replay preserves every cohort and decision. PR #1594 and parity PR #1596 are merged; grouped-preparation PR #1595 is closed because its complete change is included in #1596. Package publication, optional breadth and CG-1 through CG-23 remain independent.
9. **User-surface correction and correctness acceptance complete; timing limitation retained:** the [October 24 acceptance](../benchmarks/native-surface-parity-2026-10-24.md) completes normalized provider selection, actual scratch ownership, aliased metadata counts, shared projection production, mixed-key/read-column preservation, scoped HAVING reduction reuse and wrapper evidence. All 1,080 paired results pass independent values and all accepted-CLI controls pass. The final frontend timing screen passes 35/36 cells and remains failed for workflow/DataFrame Q34; reversed prior direction and matching work reports do not establish a persistent frontend penalty. Apply the finite stopping rule without relabeling the screen. Five isolated Full43s validate 215 complete results, 1,905 rows and matching schemas at CLI 48.701809680, workflow 49.967501000, Python SQL 50.826739087, DataFrame 51.412729914 and typed native 52.287469919 engine seconds. These sequential observations do not establish a causal ranking or timing equivalence. Targeted/broad checks and portable replay preserve every failed cohort and progression decision. Category PR #1596 was opened before the provider refresh and merged unchanged at head `a22760ec` after all 44 checks passed. Timing variability remains a stated limitation, not permission for perpetual benchmarking.
10. **Controlled Vortex provider refresh and portable evidence accepted and merged:** the [October 24 intake](native-vortex-provider-refresh-2026-10-24.md) resolves released Vortex 0.90.0 with a single Arrow/Parquet 59.3.1 graph and ORC 0.9.0. Eight retained patches preserve semantic and allocation obligations, including the `vortex-buffer` empty-owner bridge: explicit empty Bytes/Arrow owners survive clones, typed conversions and Arrow import/export, while ordinary empty slices still detach. The upstream-complete DateTimeParts patch is retired and upstream Arrow/Python replaces its separate path patch. Referenced-byte costing, bounded immediate Parquet view export, sparse FSST gathers and frozen writer-edition checks have targeted proof. The final matched build passes native workspace tests (6,072 passed; 35 existing opt-in ignored), default workspace tests (3,246 passed), strict lint and format, all-feature/minimal checks, and Rust 1.96.0 checks. Its installed Python wheel passes all 140 native integration tests without skips, including malformed capsules and retained owners; the unused Arrow/Python vendor source is retired. Public r2 passes 5,930 cases across 39 families; saved semantic replay validates 5,639 actual/expected files and 56 nested plus 81 case JSONL archives. The recovered modular matrix accepts 1,408 records (704 native and 704 independent-oracle) across 18 cohorts. Preserve the earlier duplicate-report failure and final-cohort storage interruption; all 17 completed cohorts were reused and only the final 44-record cohort rerun after verified lossless log compaction under the unchanged 192 MiB ceiling. Fresh ingest r2 produces 99,997,497 rows; r1's system-Python import failure occurred before engine execution and the accepted retry changed PATH only. One isolated Full43 r1 executes each of 43 queries once and validates 381 complete rows with exact schema continuity. Engine time is 49.625514041 seconds versus the prior CLI observation of 48.701809680; these single observations support no causal speedup or regression claim. Acceptance records are `provider-validation-primary-r5.json`, `fresh-ingest-acceptance-r2.json`, `public-transcript-verification-r3.json`, `modular-r1/provider-native-matrix-2-recovered-report.json` and `full43-acceptance-r1.json` in the provider-refresh evidence directory. Runtime/UAT acceptance is complete and committed locally as `4117846158edc7b1ab692ef90dd99ba686c5360c`; portable replay passes with eight corruption controls. PR #1601 merges at `3c3e2f14d8fe1b517fa53472a544d76452f6395a` after all 44 checks pass and review is checked at head `82d9732e`. The maintainer removes historical-file compatibility as a requirement. V1 reader-cache reuse remains admitted; V2 list-layout and retained-state lifecycle gaps feed the warm package. Rich-type and remote-provider admission remain in the existing rich-data/I/O packages, and optional encoding/Bloom/index experiments stay conditional. This is not a measured ShardLoom speedup.
    - Dependabot consolidation, October 24: [#1568](https://github.com/depsilon/shardloom/pull/1568)'s `pyo3-build-config` 0.30.1 and [#1582](https://github.com/depsilon/shardloom/pull/1582)'s Parquet 59.3.1 / ORC 0.9.0 are included in this active acceptance work. PyO3 runtime remains 0.29.3 because Arrow/Python 59.3.1 requires that runtime; the independent build helper only supplies extension-module link arguments.
    - Arrow 60.0.0's partial update in #1582 would introduce incompatible Arrow 58/59/60 boundary types. It is not needed for this intake. Keep it blocked until a coherent Vortex/Arrow/Parquet/Python graph and consumer proof are available; do not add bridges solely to satisfy the bot PR.
    - [x] Pass native/default workspace tests, strict lint, provider rechecks, all-feature/minimal builds and Rust 1.96.0 checks after the ownership repairs.
    - [x] Pass installed Python/CLI identity, 140 native integration tests without skips, and removal checks; retire the unused Arrow/Python patch after capsule and ownership proof.
    - [x] Finish fresh ingestion and one isolated Full43 acceptance on the final matched build; retain the documented minimal-ingest digest path and do not claim an independent full 100-million-row readback.
    - [x] Close superseded Dependabot PRs #1568 and #1582 on October 24 at the maintainer's request; their required changes remain in this active provider-refresh package. The separate grouped-preparation PR #1595 is also closed because its complete change is contained in parity PR #1596.
    - [x] Merge parity PR #1596 without further changes after all 44 checks pass. The October 24 closeout verifies no open PRs remain; merge commit is `d593da49e275dec4f554218e96e99e2b2fbbbde1`.
    - [x] Complete portable replay of both public runs, the recovered modular matrix, fresh-ingest linkage, all 43 query results, installed-wheel proof and 138 validation receipts; reject all eight corruption controls.
    - [x] Merge provider PR #1601 after all 44 checks pass; exact-head review and merge receipts are preserved. Dependabot requirements are incorporated or explicitly blocked above. Check for newly opened PRs at each category closeout.
    - [x] Complete the [October 25 consolidated dependency follow-up](dependency-intake-2026-10-25.md) with item 13 in merged PR #1605: Tokio 1.54.1, prost/prost-derive 0.14.4, and reviewed workflow-action pins. The earlier bot PRs are closed; partial Arrow 60 remains blocked on a coherent provider/format/Python graph.
11. **Warm-source implementation investigated and rejected under the frozen gates:** the [October 25 decision](native-warm-source-reuse-2026-10-24.md#final-finite-decision) records a complete implementation and two 93-call cohorts with correct independent values and exact schemas. Actual source opens fall from 75 to 49, but only one of four physical families passes the benefit gate and Q28 fails a first-use control; measured dictionary insertions and hits are zero. The final candidate passes focused ownership/lifecycle checks, strict native lint and 142 installed-wheel integration tests without skips. Preserve earlier broad-test failures and their focused repairs; final broad candidate acceptance is not claimed. The exact patch is preserved and all experimental runtime/provider/report changes are removed. PR #1603 merged at `b9b842ef` after all 44 checks passed. This completes the finite pre-ingestion investigation without an accepted cache. Variable-width retention remains dependent on ingestion allocation provenance. The earlier separately requested 129-call accepted-runtime characterization remains distinct from this candidate and from ordinary single-pass Full43. The naming pass follows below; CG-1 through CG-23 remain independent.
12. **Public naming and terminology accepted and merged:** the [October 25 acceptance](shardloom-public-naming-2026-10-24.md#october-25-local-acceptance) applies ShardLoom's unified-engine identity across README, package descriptions, website, field guide, interactive diagrams, API/help copy, human reports and benchmark display names. Actual format/fidelity, lifecycle and measurement distinctions remain explicit. Reports distinguish planned work from observed preparation/reuse, execution and completed output; errors lead with the problem and recovery step. Stable callable/command identifiers, machine schemas and historical evidence remain intact. Required default Rust checks pass with 3,256 tests; selected native suites pass 1,117; Python discovery passes 790 with 26 existing opt-in/environment skips. API/export and website checks, desktop/mobile review and actual CLI observations pass. The unchanged quickstart passes against the retained accepted wheel, not a newly rebuilt local naming wheel. PR #1604 merges at `3239ade0` after all 44 checks pass and review is clear; no repository PR remains open at that closeout. This closes the terminology pass, not broader support, connected-programming work, CG gates or publication.
13. **Full-ingest ownership transfer accepted and merged in PR #1605:** the [October 25 decision](native-ingest-ownership-preparation-2026-10-24.md#october-25-retention-and-consumer-decision) retains 61.2% fewer copied bytes and independently verified byte-identical output by explicit maintainer choice. The frozen 3% ingest-speed gate remains failed. The combined runtime passes installed-package and broad native/default checks, portable replay with 21 corruption controls, and one isolated Full43 at 46.973255495 engine seconds. The completed ledger preserves all earlier observations. PR #1605 merges as `167ac248ba662ae7967a7801995e9b160b17bae5` after the CI revision passes all 57 checks; no repeatable ingest-speedup, package publication or CG closure is claimed.
14. **Consolidated dependency follow-up accepted in the same merge:** the [October 25 intake](dependency-intake-2026-10-25.md) records the source/license-reviewed Tokio/prost and workflow-pin changes, native/default/MSRV/installed-package proof and original audit warnings. Partial Arrow 60 remains blocked until a coherent provider/format/Python graph is available. The completed ledger records acceptance; item 15 began next and is now closed as dropped.
15. **Complete-workflow work elimination — closed, whole batch dropped:** after the combined candidate Full43 failed the frozen regression gate, the maintainer dropped all item 15 runtime changes and every unstarted item 15 branch/package, including shared branches, guarded decorrelation, keep-data-moving and setup work. The runtime is restored to main `167ac248ba662ae7967a7801995e9b160b17bae5`. Earlier counted-join and fused-measure mechanism passes are historical evidence only, not retained-runtime claims. The [October 25 drop report](../benchmarks/native-workflow-drop-2026-10-25.md) records the candidate, restored-binary and fresh-main observations; the cause was not isolated. Item 16 and the subsequent 1.0.0 train are also complete. No item 15 work may be restarted unchanged without a new maintainer decision.
16. **Ingestion representation reuse and batch/window scheduling — closed without a runtime change:** the [October 25 decision](../benchmarks/native-ingest-window-decision-2026-10-25.md) records one isolated latest-main ingestion at 49.303 engine seconds, exact byte identity with the independently validated accepted output, and the unchanged runtime's Full43 at 47.889862794 engine seconds. Source review confirms existing dictionary/schema hints, large-source string views and shared bounded scheduling; no new candidate has established complete-ingest benefit. The maintainer requests advancing the version bump train if nothing worthwhile is established. This closes the investigation before new implementation or a candidate comparison; it does not prove optimal scheduling or reject all future mechanisms. **The subsequent 1.0.0 train is complete**, with source/package validation, GitHub → TestPyPI → PyPI → Homebrew proofs, merged documentation, public-site checks and generated-file cleanup in the [publication record](../release/v1.0.0-publication-verification.md). The dropped item 15 stays out; connected programming and universal I/O retain their subsequent order.
17. **Connected programming model complete and merged:** [PR #1610](https://github.com/depsilon/shardloom/pull/1610) merges as `c287ba45` after both review corrections and all 54 hosted checks pass. The [acceptance](../benchmarks/native-connected-programming-2026-10-26.md) and [completed ledger](phased-execution-completed-ledger.md) retain the local/native/package/Full43 evidence, original failures and cleanup. The preceding documentation PR #1609 merges as `931bf814` with all 57 checks passing. Item 20 inherits this shared lazy/native foundation; no new package publication or CG closure is claimed.
18. **Universal I/O performance and coverage follows active item 20:** the [October 24 handoff with October 28 additions](native-universal-io-performance-coverage-2026-10-24.md) requires a newly grounded one-execution multi-output contract and separates bulk Parquet/ORC layout policy from small conversion batches. The dropped item 15 implementation is not a dependency and may not be restored unchanged. Extend accepted shared demand planning and Parquet source-demand foundations to ordinary queries over the remaining supported formats by default, preserving complete-source validation and durable-preparation guarantees. Rich ORC output, long-value/resource-aware conversion and genuine whole-document JSON streaming follow; selective first-use readers, dataset layouts, IPC/codec variants and enterprise/table/transport adapters retain their family contracts. Preserve completed #1583 foundations, native owners, one shared compute engine and independent readback. Audit real family gaps and implement them; the supplied source review is not benchmark or completion evidence. The [cross-surface and format acceptance gate](native-universal-io-performance-coverage-2026-10-24.md#cross-surface-and-format-acceptance-gate) requires matched correctness and timing across CLI SQL, embedded workflow, Python SQL, DataFrame and typed native execution, covering admitted input/output families, fresh/demanded and prepared/reused sources, large writes, multi-output and readback. Resolve demonstrated avoidable performance gaps before the version bump; distinguish necessary format costs, unknown measurements and explicitly accepted exceptions. Large-output and fresh-source workflows, not Full43's 381 result rows alone, determine adapter acceptance. This does not make every proposed optional adapter a release blocker.
    - **Version milestone: 1.1.0 minor bump after item 18 acceptance and before item 19.** Package the completed item-20 analytical and item-18 I/O/default-demand work only after their correctness, cross-surface/format timing, regression and installed-package evidence passes. Before the bump, item-18 closeout must refresh the README, field guide and website to reflect accepted capabilities/default-demand behavior, ordinary examples for supported surfaces, format/fidelity/support boundaries, current performance evidence and remaining gaps; align generated documentation/site surfaces and pass documentation, website and example checks. Carry compatibility notes, explicit remaining gaps and channel proof into the release train; publication retains its separate authorization.
19. **Rich-data lifecycle support follows universal I/O:** the [October 24 addition](native-rich-data-lifecycle-2026-10-24.md) makes vectors/embeddings, tensors, nested values and actual binary payloads part of ordinary native data workflows. The maintainer's clarification treats baseline rich columns as competitive completeness against established Polars/Spark/DuckDB capabilities. Deliver dense-vector, inline-binary and nested-value fidelity first; accept managed/external binary-object ownership and lazy access separately, then tensor metadata, sparse/multiple/quantized/lower-precision representations, and shared exact vector expressions. Verify a mixed import → retain → select/filter/join → stream/spill → write → separate-process reopen → export workload, including complete bytes, metadata, resource pressure and shared-owner cleanup. Measure avoidable payload reads, copies, decoding and materialization before making efficiency claims. Extend the preceding I/O package with mappings for newly admitted rich types. Model generation, OCR/media interpretation and ANN indexes remain separate optional capabilities; existing report-only contracts do not count as implementation.
20. **Broad analytical surface and performance program is active:** the [October 26 contract](native-broad-query-program-2026-10-26.md) expands [everyday analytical completion](#everyday-analytical-completion) around a 1M-row fact-table family, typed generated workloads, strict correctness, actual execution diagnostics, neighboring queries and held-out validation. Extend the existing shared native runtime and preserve all five public interfaces; keep adapters thin and workload definitions reusable. The initial 347-case native and single-observation Polars baseline is complete and its foundation merged in PR #1611 as `4d25a06e`. The [first universal source-demand work unit](native-source-demand-2026-10-26.md), starting with Parquet, is accepted and merged in #1612 at `81ab0a4d`, with frozen first-use gates, all 347 corrected Parquet cases, required runtime/package checks and portable evidence. The [October 27 completion](../benchmarks/native-parquet-demand-completion-2026-10-27.md) is locally accepted with complete results, required gates and 185 installed-package tests; the observed native engine total improves from 50.877676708 to 34.960427343 seconds. Neither unit is released, and original competitor observations remain unchanged. Statistics/richer aggregation, temporal fidelity, numeric semantics, recursive composition and explicit batch functions retain their earlier priority and six real-workflow obligations. Existing joins/windows/pivots/sets/subqueries/nested data are foundations, not presumed gaps. Other-engine testing and repeated comparisons remain deferred until the expanded ShardLoom surface is tested. Targeted family tests drive this work; ClickBench protects established behavior. Connectors, rich I/O and larger transactional/live/distributed milestones retain their existing owners.
21. **Real Foundry integration follows everyday analytical completion and precedes AI workflows:** the [October 25 handoff](native-foundry-integration-2026-10-25.md) attaches three working deliverables to existing RFC 0036 and packaging owners: a pinned installed native package with real transform/output readback, a scalable multi-file dataset and native writer adapter, then incremental and Foundry-managed Iceberg integration. The compact-structure addition attaches logical delta maintenance to that third deliverable and existing CG-17/CG-22 state owners: begin with explicit changes and a narrow exact aggregate/join family; prove source identity, retained state, retractions, recovery and consistent state/output publication against full recomputation. This is distinct from warm caches and the rejected whole-file refresh adapter, and platform snapshot/append support does not imply a changelog. Verify the actual Linux ABI and replace the stale Conda CLI-wrapper delivery. Foundry hosts the job; ShardLoom computes through ordinary SQL/DataFrame APIs. Preserve snapshot/read-mode identity, global multi-file semantics, explicit resources/spill, schema fidelity and failure-safe output transactions. A small Arrow bridge starts deployment proof; realistic multi-file join/aggregation, independent readback and deliberate failed publication establish adapter acceptance. Whole-table Arrow materialization is not the bulk-data design. Managed Iceberg's documented single-node Arrow interface does not require a standalone Iceberg scanner first and does not imply support for every external virtual table. Keep real-enrollment proof explicit, Mac timing separate, publication separately authorized, and SDK/Compute Module/S3 extensions behind the three deliverables. Foundry remains an optional package, with its implementation now requested in this queue; CG-18/20/21/23 remain independent acceptance gates.
    - **Version milestone: 1.2.0 minor bump after Foundry integration acceptance.** Package the intervening rich-data and Foundry changes with their compatibility notes and applicable acceptance evidence before starting AI data workflows.
22. **AI data workflows follow Foundry integration:** the [October 24 AI-data package](native-ai-data-workflows-2026-10-24.md) connects the queued rich types and shared execution model to typed model-ready batches, streaming then indexed loaders, persisted fitted feature state, exact filtered vector retrieval, then lexical/hybrid retrieval and an optional compact-vector investigation. Reuse native results in PyTorch/Hugging Face adapters; preserve arbitrary sample order and duplicates, epoch generations, worker partitioning, ownership and resource limits. Point-in-time features must respect effective and availability times. Exact eligible-population search precedes optional lossy candidate representations; rescoring does not establish global exactness. Agent inspection/execution reuses existing APIs and bounded result handles. Batch inference remains a later explicit model boundary, while GPU implementation is optional. Validate complete mixed workflows and feature-specific throughput, memory, fidelity and retrieval quality; Full43 is only a regression check. This requested package extends earlier owners without a separate AI engine or a claim that current upstream features are already available through the pinned provider.
    - **Version milestone: 1.3.0 minor bump after AI data workflow acceptance.** Prepare the version and release evidence for that completed scope; optional breadth stays outside the active queue.
23. Keep additional breadth and extensions of completed conditional investigations in the [optional backlog](#optional-breadth-backlog). Do not automatically start it when the earlier stages finish. There is no delivery commitment, and optional work does not block required-area acceptance or a separately authorized release.

Preserve the seventeen finite conditional decisions, both accepted follow-ups,
original failed samples, rejected mechanisms and unchanged prospective gates.
Do not rerun rejected mechanisms unchanged. Fixed-count and known-bound
fractional sampling extensions, first-use summaries, deeper traversal, wider
prepared families and remote clients are optional unless a named comparison gap
justifies them. Existing exact sampling semantics and payload-observation rules
remain protected by their [accepted contract](native-sampling-population-spill-2026-10-19.md).
The completed nullable-layout/cleanup/UAT and public/API/spill foundations remain
in their existing ledgers and PRs. The October 21 authorization covered the
v0.7.0 train after the merged format correction. The October 25 instruction
now authorizes the 1.0.0 train after item 16; later trains still require their
own maintainer authorization.

**October 30 maintainer queue update:** finish item 20's active performance work,
including explicit existing-Top-K execution costs, predicate attribution and
multiway/composed intermediate-movement checks in the
[performance continuation](native-analytical-completion-2026-10-27.md#performance-candidates-after-composition-acceptance).
Item 18 must carry the retained shared performance capabilities across all
supported format inputs, outputs, mixed-source and source-to-sink combinations,
with actual adapter admission and complete readback. Its own acceptance remains
before the 1.1.0 milestone. After item 19, execute the separately versioned
[expanded analytical testing and benchmarking campaign](native-broad-query-program-2026-10-26.md#october-30-coverage-expansion-after-item-19)
using the existing harness, before item 21 Foundry work. The resulting order is
**20 → 18 → 1.1.0 milestone → 19 → expanded coverage/benchmark acceptance → 21 →
1.2.0 milestone → 22 → 1.3.0 milestone**. The frozen 347 cases and their timing
history stay unchanged. Targeted correctness and acceptance for current work are
not deferred; publication still requires separate authorization.

**October 30 Full43 deferral:** do not execute another Full43 pass until item 18's
universal I/O work and format/interface acceptance are complete. This replaces
earlier prospective Full43 requirements at item-20 integration boundaries.
Continue the required targeted, workspace, public, installed-package and frozen
347-case candidate checks. Preserve all completed Full43 observations, including
the October 30 direct-Parquet result, and verify saved evidence without rerunning
queries. Full43 remains a later regression check; it cannot certify item 18's
missing format, mixed-source, output and large-write coverage.

The current implementation remains the RFC 0044 native execution program. Prior R3 cleanup is recorded in the completed ledger; this reconciliation adds no phase IDs and does not bypass an active cleanup dependency. CG-1 through CG-23 remain independent evidence gates.

The latest maintainer instruction after the direct-Parquet Full43 follow-up
authorizes merging the validated shared-execution checkpoint in PR #1619 and
continuing item 20 in subsequent cohesive PRs as needed. It supersedes the earlier
October 28 hold-open instruction; it does not mark the remaining checklist or
whole-program combined acceptance complete. Preserve logical commits and
prospective validation checkpoints. Item 18 and the subsequent 1.1.0 release
retain their separate scope and sequence.

The October 22 maintainer instruction was to finish the required work and gated
investigations above, with one isolated standard Full43 pass after each coherent
implementation; the October 30 hold above now supersedes that execution cadence.
Optional items stay deferred. The subsequently frozen
independent Full43 reference exposes two differences in prior native results:
large-integer average accumulation and regex-domain grouping. Their
[required correctness correction](native-independent-full43-correctness-2026-10-22.md)
precedes new performance mechanisms. Independent native/reference Full43 parity
now passes all 381 rows in the [retained acceptance](../benchmarks/native-independent-full43-2026-10-22.md).
The Q29 rewrite-admission correction restores the native dictionary path; its
single isolated pass records **49.953164785 engine seconds**. Preserve the
stopped and slower observations. The corrected-runtime 22-task refresh now passes
all 704 comparisons, with its post-cohort interruption and five-cohort continuation
retained. PR #1589 integration and the subsequent finite region prerequisite
decision are recorded above; the finite PGO drop and subsequent polish direction
are recorded above and in the completed ledger.

The [reconciliation](native-roadmap-reconciliation-2026-10-18.md) and
[CPU intake](native-cpu-investigations-2026-10-18.md) retain their historical
37-required/21-conditional classification. All 58 child IDs and 14 canonical
owners remain traceable. Current placement is **15 scoped required-contract
rows, five scheduled prerequisite/investigation rows, and 38 optional rows**.
The optional rows comprise 22 former breadth requirements and 16 extensions of
finite conditional decisions. These counts describe scope placement, not new
implementation projects, completed capabilities or a PR forecast.

### Version 1.0.0 train

- [x] `RELEASE-1.0.0` package the accepted runtime through #1605 and close the
  maintainer-requested selected-channel train before item 17.
  - V1 scope classification: `required_for_v1` for this explicitly requested
    release train; this queue identifier adds no engine or capability requirement.
  - Source: RFC 0024, the selected-channel contract and October 25 authorization.
  - ShardLoom technique review: preserve dynamic admission, capillary work units,
    PulseWeave and metadata-first runtime behavior; use timing-surface separation
    and evidence-tier controls to keep source measurements distinct from package
    and publication proofs. No new execution mechanism is part of the bump.
  - [x] Close items 15 and 16 with their original observations and exact runtime
    restoration; no dropped candidate enters the release.
  - [x] Synchronize Cargo, Python and website package versions; regenerate API
    source identities and write scoped [release/migration notes](../release/v1.0.0-release-notes.md).
  - [x] Validate the exact source, native/default checks, installed package,
    no-fallback boundaries, checksums, SBOM and provenance.
  - [x] Complete hosted source checks and review, then publish and independently
    prove GitHub, TestPyPI, PyPI and Homebrew in that order.
  - [x] Advance published-version contracts and current documentation only after
    the corresponding channel proofs exist; verify the public site and cleanup.
  - The [completed publication record](../release/v1.0.0-publication-verification.md)
    binds the release to source `99d62ae3d10117720206463a27527a936623d182`.
    Documentation PR #1607 passes all 57 checks; its merged source passes all 54
    checks. The deployed site matches 18 HTML pages and 37 resources exactly;
    desktop/mobile navigation and search pass. Verified generated-file cleanup
    retains the tested 1.0.0 runtime, current data and all compact results.
  - The October 25 request authorizes this established version train. It does
    not close unfinished capability/production/CG gates or authorize additional
    channels. Pre-bump engine observations retain their source and executable
    identities; package publication alone is not a new benchmark.

### Everyday analytical completion

The October 26 maintainer request expands the October 24 targeted review of
#1595 at `16ff752d` into the [broad analytical program](native-broad-query-program-2026-10-26.md).
The earlier source review is not exhaustive execution proof. Use the existing
RFC 0044 native engine, RFC 0021 expressions/reducers, RFC 0033 workflows and the
relevant type, memory, extension and commit contracts. Keep CG-1 through CG-23
visible and independent; this implementation does not automatically close them.

- [ ] **Item 20: shared analytical breadth, semantics and performance**
  - Source: the October 24 completion requirements below and the October 26
    [program contract](native-broad-query-program-2026-10-26.md), including the
    maintainer's five-interface parity and shared/modular implementation requirements.
  - Current state: item 17 and the existing shared-provider families are accepted.
    The [strict corpus foundation and initial 347-case 1M observation](../benchmarks/native-broad-query-2026-10-26.md)
    are implemented with local native/package and portable evidence acceptance;
    hosted acceptance is complete in #1611 at `4d25a06e`. New
    analytical capabilities and the broader item-20 completion remain unfinished.
    This is requested post-1.0 implementation, not a retroactive release blocker.
  - Intake review: existing parity, ordinary CTEs/parameters and harness guards
    are reused; new breadth and strict semantics are accepted; connectors retain
    their owners; one Polars comparison per selected case is now authorized for
    prioritization, with other engines deferred; rejected mechanisms stay dropped.
  - ShardLoom technique review: reuse metadata-first providers, PulseWeave/capillary
    scheduling, dynamic resource admission and evidence tiers. Measure their actual
    work through existing owners before adding a new strategy. Keep timing surfaces
    separate and Vortex-native input/output explicit.
  - **Two unfinished tracks remain inside item 20:** capability/workflow completion
    and the performance checklist immediately below. Capability PRs do not close
    the performance track. The latest October 29 maintainer ordering is to finish
    the active recursive/materialized composition unit and its acceptance, then
    run the seven performance candidates before explicit batch-function work.
    Batch functions and the later connector/refresh package remain pending.
    This supersedes the earlier capabilities-first ordering. Status reports must show both tracks;
    none of this performance work moves into items 18/19 or the optional queue.
  - **Remaining performance checklist:** the
    [supplied review and admission contracts](native-analytical-completion-2026-10-27.md#performance-candidates-after-composition-acceptance)
    cover seven candidates. The first six experiments have local dispositions
    in the [shared-performance record](native-shared-performance-2026-10-29.md);
    the combined 347-query checkpoint, one direct-Parquet Full43 observation and
    portable/source-only replay are complete. Hosted acceptance remains pending.
    - [x] Evaluate Top-K propagation through direct-column projections: dropped
      after a higher complete engine total; the mechanism stays dropped.
    - [x] Propagate downstream column demand through joins: retained locally,
      preserving keys, predicate dependencies and exact match multiplicity.
    - [x] Extend controlled reader-to-native transfer at the shared intake owner:
      retained locally with copied/transferred-byte and retained-capacity proof.
    - [x] Rank at the generation-bound source before wide/nested payload reads:
      retained locally with actual selected row-group chunk reads and explicit
      two-phase validation scope.
    - [x] Evaluate inline singleton entries in the shared membership index:
      retained locally at **13.433815339 engine seconds** for all 347 cases.
      Duplicate/NULL/sparse behavior stays exact; dense directories were an
      unselected alternative, not an additional required experiment.
    - [x] Prepare string and temporal kernels once per batch, compute requested
      temporal components, and keep numeric literal operands scalar: retained
      locally at **13.274446547 engine seconds** for all 347 cases, with both
      flagged grouped-arithmetic regressions preserved in the acceptance record.
    - [ ] Evaluate measure-specific mixed aggregate state and eligible floating
      extrema traversal sharing with exact completed-domain validation proof.
    Mixed aggregate work is next, then the explicitly
    retained follow-through below. Preserve
    the unchanged 347-query cohort and the under-10-engine-second stretch target.
    After correctness, ownership, resources and no-fallback checks pass, retain a
    lower complete-cohort engine total and drop an unchanged or higher total.
    Keep per-case regressions and original rejected mechanisms visible. Further
    Full43 execution is held until item 18 is complete under the October 30
    decision above; no new benchmark is implied by this queue clarification.
    **Shared-engine boundary:** all admitted formats feed the same ShardLoom-native
    operator, semantics, memory, cancellation and result contracts. Top-K, join
    demand/indexing, expressions and aggregate improvements belong in those shared
    owners. Source-demand and deferred-fetch planning use existing common source
    contracts; each adapter proves the metadata, row identity, read and ownership
    capabilities it supplies. Parquet is the first measured adapter for buffer
    transfer, not a separate compute engine or a requirement that future readers
    use its decoder. Preserve native Vortex encodings rather than forcing inputs
    through Arrow. An adapter lacking an optimization keeps its admitted ordinary
    native path and reports that limitation. Format expansion must pass common
    semantic/ownership/resource conformance plus adapter-specific fidelity checks;
    identical performance or universal current format coverage is not claimed.
  - Execution checklist:
    - [x] Close item 17, merge its PR and documentation prerequisite, and record cleanup.
    - [x] Reconcile the supplied program with existing owners, parity and retain/drop decisions.
    - [x] Extend shared harness modules with typed case contracts, strict result/error
      validation, complete public-interface adapters and corruption controls.
    - [x] Build independently seeded fact/dimension/relationship/edge fixtures and
      verified layout variants, recording observed distributions and logical identities.
      The first 1M observation covers the sorted layout; other layouts retain
      small-fixture identity/readback proof and need larger acceptance later.
    - [x] Review the initial archetypes and generate constrained/boundary/neighboring
      cases, with measured coverage, explicit unsupported targets and held-out partitions.
      All 347 existing executable cases are now discovery data; future held-out
      acceptance requires fresh seeds/layouts/combinations.
    - [x] Run tiny semantics, representative five-interface checks and the initial
      1M ShardLoom baseline; retain family-balanced results, plans, work and failures.
      Native reports preserve available strategies and explicitly unknown counters;
      complete physical-plan and byte instrumentation remain unfinished.
    - [ ] Correct demonstrated shared correctness, reachability, composition,
      repeated-work or algorithm causes using prospective retain/drop gates.
      Apply the October 28 maintainer tradeoff: substantial overall gains can
      outweigh small isolated timing regressions. Preserve their observations
      and assess proportionate follow-up without automatically blocking the
      implementation; correctness and resource/ownership contracts still apply.
    - [ ] Complete the analytical requirements below through shared runtime families,
      preserving the six end-to-end workflows and separate connector ownership.
    - [ ] Validate unchanged reproducers, neighboring/held-out variations, public
      interfaces, unrelated controls and applicable scaling/resource cases.
    - [ ] Retain a compact regression suite and bounded exploration selection;
      update capability/reference docs and require coverage with each new capability.
    - [ ] Complete required workspace/native/package checks, justified ClickBench
      protection, portable evidence review, PR acceptance and ledger movement.
  - Next outcome: a working strict corpus foundation and measured native family
    baseline, followed by shared changes selected from its demonstrated gaps.
    Do not implement per-query or per-interface execution shortcuts.
    The foundation/baseline PR is merged. The maintainer's first
    [universal source-demand work unit, starting with Parquet](native-source-demand-2026-10-26.md)
    is merged in #1612, and Parquet completion is merged in #1613 (`04e7d92a`).
    The October 27 [completion continuation](native-analytical-completion-2026-10-27.md)
    incorporates every remaining analytical, shared-performance, diagnostic and
    acceptance obligation into this active item, followed by the remaining
    file-format and interface-parity work. Preserve shared planning, adapter
    capability contracts and item-18 source/refresh ownership.
    The [aggregate/grouping work unit](../benchmarks/native-analytical-aggregates-2026-10-27.md)
    passes local native, five-interface, workspace, installed-wheel and portable
    checks. Its complete 347-case checkpoint is 31.962394957 engine seconds versus
    34.960427343 previously, with no prospective material-regression flags.
    PR #1614 merged at `690a671e` after all 57 hosted checks passed. This does
    not close the remaining Polars-performance gaps or item 20. The shared
    [numeric and bounded-ordering continuation](../benchmarks/native-shared-analytical-performance-2026-10-27.md)
    is locally accepted with 194 installed-package tests, complete portable replay
    and 18 corruption controls. Its 347-case observation is 28.017558877 engine
    seconds; original individual regression flags and bounded diagnostics remain
    recorded. The borrowed-output candidate is dropped under its frozen gate.
    PR #1615 merged at `5a0b8c0f` after all 57 hosted checks passed. The
    [temporal-fidelity work unit](../benchmarks/native-temporal-fidelity-2026-10-27.md)
    is merged in #1616 at `51cf54fd` after all 57 hosted checks pass, with 6,301
    native tests, 22 public-matrix tests, 199 installed-package tests and portable
    acceptance. The [rich bounded-retention continuation](../benchmarks/native-rich-bounded-retention-2026-10-28.md)
    passes local gates at 27.374316659 engine seconds for 347 cases, compared
    with the fresh temporal baseline's 28.118399295 seconds. The declared
    projection family improves 10.36% with no candidate material flags; 176
    fresh held-out cases, 200 installed-package tests and portable replay pass.
    PR #1617 merged at `328d3bf0` after all 57 hosted checks passed and automated
    review completed without findings. The
    [shared key-lookup continuation](../benchmarks/native-shared-key-lookup-2026-10-28.md)
    passes local gates at 25.508976866 engine seconds for all 347 cases versus
    the reused 27.374316659-second checkpoint. Grouped aggregation improves
    12.44% and DISTINCT 17.39%, with no material regression flags. Complete
    160-case fresh correctness, 201 installed-wheel tests, 23 corruption controls
    and isolated replay pass; PR #1618 merged at `685f16c7` with all 57 checks
    passing and no review findings. The subsequent
    [numeric acceptance](../benchmarks/native-shared-numeric-values-2026-10-28.md)
    retains typed MIN/MAX and independently owned numeric gathering. All 347
    cases and 12,565 rows validate at 23.079365459 engine seconds, with a 9.52%
    whole-cohort reduction and the declared family gate passing in global/grouped
    aggregation and joins. One original case flag remains; its bounded diagnostic
    does not reproduce it. Complete fresh correctness, 202 installed tests,
    30 corruption controls and isolated portable replay pass. Numeric hosted
    acceptance remains separate. Continue the consolidated item-20 completion
    PR, with the expression construction/reuse candidate explicitly dropped
    under its frozen benefit gate. Its unchanged-cohort comparison completed
    347 cases and 12,565 rows at 22.941843204 engine seconds versus the accepted
    numeric baseline of 23.079365459 seconds (0.596% reduction); no declared
    target family reached 10%, and there were no material regression flags.
    Held-out replay passed 160 observations and 3,280,220 result rows. The
    candidate runtime and test changes were removed, restoring the accepted
    numeric implementation, as
    recorded by the expression removal patch and source inventory. The requested
    <=1 engine second for each of the grouped-aggregation, projection and
    combined-join family totals remains a target, not an achieved result; the
    347-case candidate aggregate timing does not establish those family targets.
    The exact short/composite group-key cache is retained for combined acceptance
    at 19.754020577 engine seconds, with grouped aggregation at 6.837246991
    seconds. The subsequent
    [typed complete-payload validation checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/payload-validation-disposition.json)
    passes all 347 cases and 12,565 rows at 18.304092867 engine seconds, a 7.34%
    further reduction. Projection improves 13.60% and ordering 22.92%; no
    material case/family flags occur. Focused native checks, strict native lint,
    two five-interface regressions and 160 fresh correctness observations pass.
    Preserve four initial DataFrame declaration failures and their four corrected
    retries; the 156 original successful observations are reused. Full combined
    native/workspace/package, portable and hosted acceptance remain pending.
    The subsequent join-probe/gather-coordinate candidate is dropped under its
    prospective gate: 18.527516365 engine seconds, with only 0.26349% ordinary-join
    and 0.02803% multiway-join improvement. Complete 347-case results and 160 fresh
    correctness observations pass; preserve the original failures, streaming
    clock unavailability and corrected continuations in its
    [disposition](../benchmarks/evidence/native-item20-completion-2026-10-28/join-reuse-disposition.json).
    Both runtime mechanisms are removed; post-removal workspace lint and 3,274
    workspace tests pass. The retained time at that removal stays 18.304092867
    seconds without a new performance observation. The consumer audit finds no count-only grouped
    query in this cohort, so merely extending grouped CountWorkers to batch inputs
    would miss its mixed-aggregate cost. The refreshed seven-case diagnostic
    profile identifies scalar validation of eligible leaves when a rich sibling
    selects the recursive owner. The
    [recursive typed-leaf checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/recursive-payload-validation-disposition.json)
    reuses the retained validator after the existing parent masks and selected
    domains are resolved. All 347 cases and 12,565 rows pass at 17.729495249 engine
    seconds: 3.14% complete-cohort reduction and 18.07% projection-family benefit,
    with no material regression flags. Twenty-six native payload checks, strict
    lint, four public regressions and complete replay of 160 fresh observations
    (33,260 result rows) pass. The subsequent
    [validation supplement](../benchmarks/evidence/native-item20-completion-2026-10-28/recursive-payload-validation-followup.json)
    records 5,011 native tests passing (35 existing opt-in tests ignored), strict
    workspace lint and 3,274 passing workspace tests on the same runtime source.
    Combined implementation, installed-package, portable and hosted acceptance
    remain open.
    The next retained [demand-driven hashing checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/group-hash-demand-disposition.json)
    validates all 347 cases and 12,565 rows at 17.42942424 engine seconds, 1.69%
    lower overall than 17.729495249. DISTINCT improves 10.80%, passing its frozen
    family-benefit gate with positive overall balance; grouping improves 3.31%.
    No material regression flags occur. Null-equal membership reuses the existing
    exact cache before requesting flat text/binary hashes, while full domain
    validation and all established non-admitted paths remain intact. The final
    189 focused relational tests, strict native lint, four public regressions and
    160 fresh five-interface observations pass (303,260 complete result rows).
    Preserve the corrected lint and missing-Arrow test-launch failures. This is
    a modest single-observation gain, not statistical or causal attribution.
    Full native validation passes 5,015 tests with 35 existing opt-in ignores;
    formatting, strict workspace lint and all 3,274 default workspace tests pass.
    Combined installed-package, portable and hosted acceptance remain open.
    The subsequent [selected-child/direct-decimal validation candidate](../benchmarks/evidence/native-item20-completion-2026-10-28/selected-payload-validation-disposition.json)
    is dropped: 17.670435985 engine seconds for all 347 complete queries versus
    17.42942424, without the frozen family benefit or any material regression
    flags. Four public regressions and 160 fresh five-interface observations
    pass, including 33,260 complete result rows. Preserve its source, original
    observations and failures; remove both optimization mechanisms. Retain the
    independently required malformed-text correction and regression tests.
    Its cost is not isolated by the combined candidate timing; the accepted
    optimization checkpoint at removal is 17.42942424 seconds. The
    [post-removal source validation](../benchmarks/evidence/native-item20-completion-2026-10-28/retained-text-validation.json)
    passes formatting, strict native/workspace lint, 5,016 native tests (35
    existing opt-in ignores) and 3,274 workspace tests. The unchanged provider
    source has 146 passing tests, one existing ignore and strict lint. The
    combined candidate's public/fresh-cohort results do not establish new
    retained-source installed-package acceptance; that combined gate stays open.
    The next [typed numeric expression checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/typed-numeric-expression-disposition.json)
    retains same-storage I64/U64/F64 arithmetic and column/literal COALESCE
    construction at **16.249797458 engine seconds** for all 347 queries and
    12,565 complete rows, a 6.77% reduction. Grouped aggregation improves 13.75%
    and global aggregation 21.31%, passing the frozen family-benefit gate.
    One arithmetic-group query adds 0.021396124 seconds while the cohort saves
    1.179626782 seconds; preserve that flag under the proportionate-regression
    policy. No family regression threshold is crossed. The comparison includes
    the previously untimed UTF-8 correctness repair and establishes no cause or
    statistical confidence. Nine focused and 198 relational tests, four public
    regressions, 160 fresh five-interface observations (410,420 result rows),
    formatting, strict native/workspace lint, 5,025 native tests (35 existing
    opt-in ignores) and 3,274 workspace tests pass. The source-linked record
    preserves all failed attempts and the exact candidate patch. Combined
    installed-package, portable and hosted acceptance stay open.
    The [integer-extrema checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/integer-extrema-fusion-disposition.json)
    retains shared grouped integer MIN/MAX and eligible COUNT traversal at
    **15.992156898 engine seconds** for all 347 queries and 12,565 rows, saving
    **0.257640560 seconds** with no material regression flags. Ten focused and
    207 relational tests, four public regressions, 160 fresh five-interface
    observations (6,860 result rows), formatting, strict native/workspace lint,
    5,035 native tests (35 existing opt-in ignores) and 3,274 workspace tests
    pass on the measured source. Six original failed attempts remain recorded.
    The [maintainer's gain/loss rule](../benchmarks/evidence/native-item20-completion-2026-10-28/complete-cohort-gain-policy.json)
    supersedes percentage floors: lower complete-cohort engine time ships;
    higher or unchanged drops after correctness/resource checks. This explicitly
    reverses the candidate's original cutoff-only drop without changing its
    frozen protocol or rerunning queries. Advance the retained baseline as gains
    compound. The subsequent
    [current-source recovery](../benchmarks/evidence/native-item20-completion-2026-10-28/positive-gain-recovery-disposition.json)
    retains applicable borrowed variable output and exact expression reuse,
    preserving later numeric and validation improvements. All 347 queries and
    12,565 rows pass at **15.400097157 engine seconds**, saving **0.592059741
    seconds**. Five public regressions, 160 fresh five-interface observations
    (412,220 result rows), 5,045 native tests (35 existing opt-in ignores),
    3,274 workspace tests and strict lint pass. One case adds 0.013006291 engine
    seconds with unchanged physical demands; preserve that flag and the absence
    of family flags. Preserve two zero-test temporal launches and the rejected
    acceptance attempt; the exact calendar-feature test passes. Historical gains
    are not added to this combined measurement. The requested Full43 follow-up
    runs directly on resident original Parquet through the shared source-demand
    path, without ingest or preparation. Its input boundary is distinct from
    historical Vortex-input Full43. Combined package/portable/hosted acceptance
    and the rest of item 20 remain open.
    Direct-Parquet Full43 preserves the first naming-contract interruption and
    a second attempt with Q1–29 validated before Q30 reaches its query deadline;
    neither is a complete timing result. The subsequent
    [mixed-width expression checkpoint](../benchmarks/evidence/native-item20-completion-2026-10-28/mixed-integer-expression-disposition.json)
    retains borrowed typed integer evaluation at **15.180311963 engine seconds**
    for all 347 queries and 12,565 rows, saving **0.219785194 seconds** without
    case or family regression flags. Twelve focused tests, six public regressions,
    160 fresh five-interface checks (248,320 result rows), 5,048 native tests
    (35 existing opt-in ignores), 3,274 workspace tests and strict lint pass.
    The original public-fixture domain refusal and unexecuted held-out binding
    remain recorded; explicit casts and refusal checks pass on unchanged binaries.
    A small 90-SUM scale check passes. Fresh isolated Full43 and combined item-20
    acceptance remain required.
    The third direct-Parquet pass validates Q1–32 before Q33 refuses aggregate
    storage growth under the unchanged memory limit. Its
    [incomplete record](../benchmarks/evidence/native-item20-completion-2026-10-28/full43-direct-parquet-incomplete-r3.json)
    preserves the 271 completed rows and has no Full43 total. The subsequent
    [compact grouped-state correction](../benchmarks/evidence/native-item20-completion-2026-10-28/compact-group-disposition.json)
    reuses leased pages for large grouped COUNT/SUM/AVG state, retaining existing
    arithmetic, key ordinals and resource limits. The original Q33 now passes at
    **66.174310876 engine seconds**. All nine focused tests, seven public regressions,
    160 fresh five-interface observations (1,584,120 complete rows), 5,057 native
    tests (35 existing ignores), 3,274 workspace tests, formatting and strict lint
    pass. The unchanged 347-case result is **15.304317834 engine seconds**, a
    **0.124005871-second loss** against the preceding checkpoint; its optimization
    benefit gate fails. Retain this required memory correction without a speedup
    claim, preserving both offline validation-helper corrections and every saved
    observation. The subsequent
    [current-runtime UAT](../benchmarks/native-analytical-checkpoint-2026-10-28.md)
    passes all 43 direct-Parquet queries and 381 complete rows at
    **651.558161209 engine seconds**, one execution each, with no ingest or
    prepared-input writes. Independent saved-result/clock replay, original input
    identity and process cleanup pass. The identical current runtime passes 207
    installed-wheel integration tests without skips and verifies uninstall.
    Historical prepared-Vortex Full43 observations retain their different input
    boundary; this is no matched speedup/regression ratio. The source trace shows
    demanded Parquet batches use shared relational execution while eligible
    prepared Vortex plans can select additional encoded file strategies. Carry
    source-aware provider reachability, format decoding, metadata pushdown and
    preparation/reuse attribution into the next measured work unit; do not assume
    the entire gap is I/O or require ingest for correct direct execution.
    The October 28 maintainer follow-up prioritizes measured shared grouping,
    projection and join costs, then shared-intermediate reuse and plan traversal
    where source and workload evidence warrants them. Recursive CTEs and structural
    limit removal retain their capability/scalability scope; do not assume they
    accelerate the current cohort. This priority preserves every remaining
    capability, workflow and combined-acceptance requirement.
    The October 29 maintainer instruction now completes the remaining capability
    areas and their acceptance before returning to those performance experiments.
    The [time-series continuation](native-time-series-completion-2026-10-29.md)
    begins with as-of joins over the shared native join, ordering and spill owners.
    The [calendar-window continuation](../benchmarks/native-calendar-windows-2026-10-29.md)
    now passes local conformance, resource and ownership acceptance across all
    five interfaces. The [temporal-grid continuation](../benchmarks/native-time-grid-2026-10-29.md)
    now passes local resampling, gap-fill and interpolation acceptance, including
    280 five-interface executions, 8,520 complete rows, real native spill and
    streamed-source failure/ownership checks. Numeric, composition, batch-function,
    diagnostics, installed-package and combined item-20 acceptance remain open.
    The [explicit-decimal continuation](../benchmarks/native-decimal-output-2026-10-29.md)
    now passes six public tests with 250 five-interface executions and 2,695
    complete rows, real native-spill/failure acceptance, and 223 installed-wheel
    integration tests. It adds per-operation precision/scale/rounding and proves
    explicit widened-integer SUM without changing ordinary arithmetic defaults.
    The [floating-domain continuation](../benchmarks/native-float-policy-2026-10-29.md)
    passes 22 public tests with 530 five-interface executions, 4,155 complete
    five-interface rows and actual native spill for grouped/window/pivot and
    duplicate-selection results. Native/default workspace checks and all 245
    installed-package tests pass. Strict remains the default; extended policy
    distinguishes NULL from NaN and preserves carried F32/F64 bits. The review
    correction covers assignment-policy propagation and composable classifiers;
    the original acceptance snapshot and later correction are retained separately. Composition,
    batch-function, diagnostics, the seven performance candidates above and
    combined item-20 acceptance remain open.
    The existing 347-query checkpoint is unchanged during this capability work.
    The latest maintainer instruction closes PR #1619 at this validated checkpoint
    and continues the remaining item-20 work in subsequent cohesive PRs. Retain the dropped
    counted-join, fused-measure and retained-output decisions. The later
    maintainer gain/loss decision reopens cutoff-only borrowed-output and
    expression drops for current-source integration and acceptance.
    This sequence does not replace the wider temporal/time-series,
    rich-aggregate, numeric, composition, batch-function, diagnostics, or
    format/interface obligations.
    Incremental persistence remains deferred; this does not close the broader
    capability and operator-performance obligations above.
  - Acceptance: complete typed results/errors and native certificates at applicable
    interfaces; measured coverage and resources; visible unsupported/failed cases;
    preserved original observations and existing behavior. Case counts alone do not pass.
  - Verification: focused Python/Rust tests for affected owners, real five-surface
    requests, dataset/readback and corruption controls, then `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets`
    and applicable native/installed-package gates at cohesive boundaries.
  - Boundary: the explicit single-run Polars baseline is external comparison only;
    no hidden fallback, new release, broad superiority claim or implicit CG closure.
    Every ShardLoom execution preserves `fallback_attempted=false` and
    `external_engine_invoked=false`. Other-engine comparisons require the later
    completed-surface stage and are not a prerequisite to developing this corpus.

The priority order below applies to the added completion requirements. Items
already owned by connected programming or universal I/O stay with those packages.
The latest October 29 override inserts item 20's seven performance candidates
after acceptance of the active recursive/materialized composition unit and
before priority 5, explicit batch-function integration. The numeric and temporal
units already delivered keep their acceptance obligations; no open package is
marked complete by this resequencing.

| Priority | Completion package | Implementation and acceptance contract |
| --- | --- | --- |
| 1 | Statistics and richer aggregation | Extend shared update/merge/finalize, memory, native spill and window integration with sample/population variance and standard deviation, covariance, correlation, median and exact quantiles. Specify null pairs, small samples, numerical stability, merge behavior and exact quantile interpolation. Exact quantiles require separately admitted state, not variance's fixed-size state. Then add exact SUM/AVG DISTINCT, ordered first/last, values at minimum/maximum keys, list and string aggregates, reusing existing distinct/window/pivot machinery where sound. Approximate quantiles/sketches remain explicitly approximate later functions. |
| 2 | Temporal fidelity and time-series workflows | Preserve timestamp units and timezone identity, time-of-day, durations and calendar intervals from ingestion through comparisons, grouping, casts, serialization and reopen. Complete parsing, formatting, truncation, conversion and arithmetic in shared SQL/DataFrame semantics. Distinguish instant conversion from wall-clock reinterpretation and specify ambiguous/nonexistent local times. Verify dedicated as-of, calendar/time-based rolling, resampling, gap filling and interpolation first, then implement missing behavior through existing joins/windows. Specify ordering, ties, tolerance, nulls, group boundaries and timezone effects; row-count windows do not establish calendar-window support. |
| 3 | SQL/DataFrame composition and parameters | Preserve the [connected programming package](native-connected-programming-model-2026-10-24.md), scoped named plans, nested ordinary WITH scopes, typed parameters and column-name lists. The [recursive/materialized continuation](native-recursive-composition-2026-10-29.md) now passes local complete-value, five-interface, pressure/failure, common-conformance and installed-wheel acceptance; hosted PR acceptance remains pending. It adds action-owned materialization and bounded native frontier iteration, including finite-cycle UNION semantics. GROUPING SETS, ROLLUP/CUBE, aggregate FILTER and ordered aggregates use existing shared providers. Broader combined item-20 acceptance remains open. |
| 4 | Numeric-domain and rich-type fidelity | Define NaN, positive/negative infinity and signed-zero behavior across observation, equality, sorting, grouping, reductions, serialization and validation, preserving a strict caller-selectable policy. Keep NaN distinct from null. Add explicit decimal precision/scale/rounding choices alongside exact behavior, and an exact widened-integer SUM contract separately from floating AVG. Coordinate with [rich-data lifecycle](native-rich-data-lifecycle-2026-10-24.md); carrying a value without observing it is distinct from implementing its arithmetic. |
| 5 | Explicit batch-function integration | Prefer reusable native expression/plan composition. Add declared application batch functions with input/output schema, null and determinism policy, errors, cancellation, ownership, memory and effects accounted for under RFCs 0011/0023. The surrounding query remains ShardLoom execution; expose the application-code/materialization boundary and do not promise built-in-function performance. Existing typed fixtures are a foundation, not completion. |
| 6 | Complete remote/table connectors and safe refresh | Extend the [universal-I/O package](native-universal-io-performance-coverage-2026-10-24.md), retaining immediate local writer work first. A selected workflow must authenticate → discover schema → read selected data → execute natively → publish safely, with credentials, ranges, retries and generation checks. Implement safe replacement/refresh before append/upsert against a defined table contract; broader transactions follow their separate persistence/recovery contract. |

The merged foundation contains the six-function reducer family in
[`native_relational_aggregate.rs`](../../shardloom-vortex/src/local_primitives/native_relational_aggregate.rs),
the COUNT-only DISTINCT admission in
[`sql_native_relational.rs`](../../shardloom-cli/src/sql_native_relational.rs),
and date-days/timezone-free microsecond recognition in
[`native_temporal.rs`](../../shardloom-vortex/src/native_temporal.rs).
The active [analytical implementation](native-analytical-completion-2026-10-27.md)
now extends reductions and windows with moments, paired statistics, exact
quantiles, additive DISTINCT, ordered selectors, collections and aggregate
FILTER. Their focused native and five-interface evidence is recorded there;
the wider item-20 acceptance remains open.
The merged [native declaration binder](../../shardloom-cli/src/sql_native_relation_declaration.rs)
implements scoped ordinary CTEs, column lists and typed parameters using shared
plan references and resource admission. The locally accepted
[composition continuation](../benchmarks/native-recursive-composition-2026-10-29.md)
adds recursive frontier execution and explicit action-owned `MATERIALIZED`
relations through those same native plans; `NOT MATERIALIZED` uses ordinary
bound references. Nonlinear, mutual and correlated retained definitions remain
explicitly unsupported. Shared nondecimal
SUM/AVG finalize as floating values; under the default strict policy the reducer
rejects non-finite sums, and
[`native_decimal_reduce.rs`](../../shardloom-vortex/src/local_primitives/native_decimal_reduce.rs)
rejects averages that would discard fractional digits. This does not imply all
opaque payload movement observes or rejects every non-finite value. The
[as-of implementation](native-time-series-completion-2026-10-29.md) adds exact
direction, tolerance, duplicate/NULL and resource semantics across the public
interfaces. The [calendar-window implementation](../benchmarks/native-calendar-windows-2026-10-29.md)
adds exact elapsed/calendar RANGE semantics and retreating-boundary spill.
The [temporal-grid implementation](../benchmarks/native-time-grid-2026-10-29.md)
adds anchored fixed/calendar resampling, bounded per-partition gap filling and
ordered forward/backward/linear filling with shared resident/spilled owners.
The [explicit-decimal implementation](native-decimal-output-2026-10-29.md) adds
caller-selected precision, scale and six rounding modes to native scalar
arithmetic and Decimal128 additive/continuous-quantile reducers and windows.
Its result rounding occurs once after exact accumulation or interpolation;
ordinary declarations retain their existing domains. The
[floating-domain implementation](native-float-policy-2026-10-29.md) adds immutable
strict/extended session policy, bit-preserving F32/F64 carriage, normalized
analytical identity, classification and nonfinite-aware reducers/windows through
the existing native owners. Its [acceptance record](../benchmarks/native-float-policy-2026-10-29.md)
covers public interfaces, actual spill, corrected NaN-payload regressions,
native/default checks and installed-package acceptance.
The broader SQL aggregate combinations, remaining composition and combined acceptance
above retain their remaining implementation and conformance obligations.

Use **six small end-to-end workloads**, extending existing conformance tooling:
a per-fund statistical summary; an entity/currency as-of valuation join;
a timezone-aware daily report across daylight-saving transitions;
a nested multi-CTE parameterized query; a custom batch transformation;
and safe refresh of an existing dataset. Verify complete values/types and
independent readback, plus applicable pressure/spill, cancellation, changed-source
and publication failures. Exercise equivalent SQL/DataFrame declarations and
mixed rich payloads where relevant. Scale only the selected resource or benefit
case; do not create another exhaustive status matrix or substitute ClickBench
for feature-specific acceptance.

Remove structural ceilings by implementing scalable mechanisms: iterative
traversal, shared plan references, checked sizing and resource-based admission.
The current [native plan](../../shardloom-cli/src/native_runtime_plan.rs) depth
24, [nested schema](../../shardloom-vortex/src/native_payload_schema.rs)
depth 24/node 4096 and CTE name/text bounds are concrete test boundaries, not
permanent exclusions of legitimate deep or wide workflows. Cover binding,
execution, accounting and cleanup beyond each replaced boundary.

Persistent transactional tables, durable continuous processing and distributed
execution remain separate larger milestones under their existing table/commit,
CG-10 and CG-22 contracts. Temporary views do not establish persistent tables;
finite batch streaming does not establish durable unbounded streams; local
parallel workers do not establish multiple-machine execution. Keep these goals
visible without making them prerequisites for completing the local engine.

Official comparison references checked for this addition include
[DuckDB statistical/ordered aggregates](https://duckdb.org/docs/lts/sql/functions/aggregates),
[Polars timezone operations](https://docs.pola.rs/user-guide/transformations/time-series/timezones/),
[Polars calendar grouping](https://docs.pola.rs/user-guide/transformations/time-series/rolling/),
[DuckDB recursive CTEs](https://duckdb.org/docs/lts/sql/query_syntax/with) and
[DuckDB explicit Python/Arrow function boundaries](https://www.duckdb.org/docs/current/clients/python/function).
These establish comparison targets, not ShardLoom support or permission for an
external engine fallback. Recheck pinned providers and exact semantics before
implementation and freeze finite correctness/resource/benefit gates.


### How a competitive requirement is decided

Use a complete task that a user would otherwise run on an alternative compute
engine. Record the selected baseline and supported equivalent, exact input and
result semantics, scale, resource policy, and the ShardLoom gap. Start from the
current [Full43 procedure](../skills/benchmarking.md#local-end-to-end-observations),
the [traditional analytics declarations](../../benchmarks/traditional_analytics/workloads.py)
and supported public workflows. These sources define comparison tasks; their
existence does not prove parity with every engine or feature.

The required rows below protect correct complete results, native Vortex
fidelity, no fallback, safe existing writes, ownership, declared resource/support
limits and credible comparisons for that selected scope. Existing source-bound
evidence may satisfy them. Audit an actual missing guarantee before adding code;
do not widen a scoped guarantee into a whole-process memory promise or an
unbounded cross-product of types, operators and destinations.

Internal techniques such as more worker families, metadata summaries, constant
key pruning or another cache are replaceable means. They become required work
only when evidence shows a selected comparative task cannot meet its accepted
contract without a change. Capability addition is not required to be a speedup,
but correctness alone does not excuse a material regression. Any newly proposed
scope must meet this same decision rule; unknown parity remains unproven.

### Competitive prerequisites and four investigations

These are the already requested prerequisite and investigation stages, after
required comparative gaps and before optional breadth. A narrow dependency on
local reduction does not require every optional `PERF-04` state or worker
extension. A new region mechanism remains subject to a measured opportunity,
provider review, correctness/resource proof and its prospective retain/drop gate.
A missing or no-signal prerequisite leaves the dependent experiment blocked;
it does not force a broader implementation campaign.

| Prerequisite work, after required comparative gaps | Existing owners | Acceptance before dependent investigation |
| --- | --- | --- |
| Independent natural-region execution | `PERF-03.regions`, with `PERF-03.providers` and `PERF-03.mixed_load` | Review the pinned Vortex provider and a materially different design; prove immutable generation identity, complete coverage, original batch boundaries, bounded admission under one CPU/memory grant, skew progress, ordered handoff, cancellation and drain. Preserve earlier rejections and pass a frozen complete-operation/control gate before retention. |
| Exact region-local reduction | Region extensions of `PERF-04.workers`, `PERF-04.states` and `PERF-04.semantics` | Integrate admitted states with the retained region mechanism; prove complete keys, NULLs, integer overflow, floating evaluation policy, source identity and deterministic merge under skew and the declared worker counts. No lossy local Top-K or hidden second worker pool. |
| Region and preparation cost evidence | `PERF-01.attribution`, prerequisite portion of `PERF-09.design`, `PERF-12.comparisons` | Bind region geometry to actual pruning, encoded computation, materialization and merge work, with separate preparation/ingest/storage/query costs. Cover small, wide-text, large-numeric, dictionary-heavy, unknown-row-count and constrained-memory cases. Collect the runtime basis before selecting a preparation policy. |
| Current instruction/dispatch attribution | `PERF-01.attribution`, prerequisite portion of `PERF-13.pgo` | Attribute material current-source costs to native callers/operators/types with valid counter scope, negative controls and complete raw records. Historical whole-process counts do not select a training corpus. A no-signal finding is a recorded prerequisite outcome, not a completed PGO trial. |

The [October 22 baseline attribution](../benchmarks/native-current-attribution-2026-10-22.md)
supplies the current-source diagnostic. The [scalar-region decision](../benchmarks/native-scalar-regions-2026-10-22.md)
rejects the finite mechanism and restores that exact baseline, preserving the
attribution's source binding. The region-dependent investigations below remain
blocked; the [separate PGO investigation](../benchmarks/native-pgo-2026-10-23.md)
is closed with a drop. Refresh selected-path attribution
when an accepted change affects it. The finite evidence is recorded in the ledger.

The four investigation contracts and current dependency outcomes are:

1. `PERF-10.ordered_candidates` — blocked after region rejection: exact local candidates and bounded global merge
   before payload access, including full multi-keys, NULL order, ties, offset/limit
   and absolute source identity.
2. `PERF-09.design` — blocked after region rejection: compare joint preparation/layout choices using the accepted
   runtime cost evidence and all varied input/control families.
3. `PERF-12.topology` — blocked after region rejection: compare the newly admitted mechanism across safe
   current/coarse/small/medium/large modes on the same immutable 100M-row input,
   preserving the original 129-call coverage and independent held-out controls.
4. `PERF-13.pgo` — finite drop recorded October 23: fresh matched builds,
   representative training, complete query and held-out evaluation, pressure and
   installed-package checks pass. The control ingest fails the unchanged byte
   gate against a reference prepared at different parallelism; three ingests are
   unrun, and complete-workflow benefit and break-even are unavailable. ThinLTO
   remains the default. The completed ledger records this finite decision.

The [region prerequisite review](native-region-dependent-prerequisites-2026-10-20.md)
and [PGO prerequisite review](native-pgo-prerequisite-decision-2026-10-20.md)
remain historical evidence. The October 22 region result updates the first
three dependencies and settles the region decision before PGO. Sequencing the work
does not satisfy those gates, reopen rejected mechanisms unchanged, promote PGO,
or authorize a release. Keep all CG-1 through CG-23 and the no-fallback,
Vortex-native, explicit-effects contracts intact.

### Common implementation and acceptance contract

- Source and scope: use each owner’s governing RFC/provider review and the current code, tests and completed ledger. Check Vortex-native providers before inventing an abstraction; public front doors converge on shared native plan/state/sink contracts.
- ShardLoom technique review: evaluate PulseWeave, capillary work units, dynamic admission/work shaping, metadata-first work avoidance, late materialization, timing-surface separation and evidence tiers for each concrete family. Record actual activation; descriptor fields alone are not execution proof.
- Evidence and acceptance: exact complete values, logical types, NULL/bag/order semantics, source identity, resource credits, cancellation/failure/cleanup and actual work/copy/decode counters. Preserve `fallback_attempted=false` and `external_engine_invoked=false`; Vortex stays the native persistence target and compatibility formats remain explicit translation boundaries.
- Performance: freeze current workload, mechanism, samples, controls and retain/drop threshold before timing. The latest prospective speed target is at least 2% of complete-workload engine time, subject to correctness, repeatability and resource/control gates. Do not relax historical gates, combine unrelated gains or infer a whole-engine gain from a focused cohort. Routine Full43 timing is the sum of `engine_execution_seconds` for one call to each query.
- Verification: run the [owner-specific commands and named evidence outputs](native-roadmap-verification-contracts-2026-10-18.md), add exact regression selectors for new behavior, and reject zero-test results. Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace --all-targets` for nontrivial implementation, plus applicable native/provider/public matrices. Complete Full43/format acceptance at coherent batch boundaries under local storage/process guards.
- User-visible surfaces: shared CLI/Python/SQL/DataFrame behavior, native/compatibility outputs, diagnostics/certificates, documented support and workload evidence. Align affected docs/site/capabilities and hosted installed-wheel checks before claiming integration.
- Non-goals and claims: no fallback engines, unapproved effects/dependencies, package publication, global-RSS promises, or unsupported superiority/production claims. `claim_gate_status=not_claim_grade` until the corresponding workload and release gates pass.
- Ledger rule: close a finite substep only with implementation/diagnostics, tests and linked acceptance. Move completed narrative to the completed ledger; leave broader owners open. Scheduled conditional work remains pending its evidence. Optional backlog rows are deferred choices, not required obligations or completed capabilities.

### Required contracts and scheduled investigation owners

Owner descriptions retain historical context. Only their explicitly scoped required
rows and the scheduled prerequisite/investigation rows authorize current work.
Optional extensions do not expand those requirements.

- [ ] `PERF-01` attribute observed regressions and substantiate comparison evidence.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; performance-attribution-and-execution-structure.md; native-drop-review-2026-10-18.md.
  - Current state: Fresh-source ingestion, Full43, separate engine/result/transport clocks and several measured optimization campaigns already exist. Internal overlapping work and the legacy numeric bridge still need scoped attribution.
  - Dependencies: Cross-cutting: freeze the opportunity and gate before each performance candidate; accompany every implementation family below.
  - Implementation scope: Shared timing/certificate helpers, source_backed_encoded_execution.rs, local_primitives.rs, benchmark reports and public evidence.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-01.attribution` **Required for the selected workload contract.** Diagnose measured regressions and support comparison claims with current-source engine/operator attribution and controls. Preserve all samples and distinguish overlapping work from engine elapsed time. An attribution project needs a named performance question; it is not an obligation to instrument every operator.

  - Optional extensions: `PERF-01.numeric_bridge` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-01 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-01); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-03` verify resource ownership and scheduling for declared workloads.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0014/0017/0044; native-local-completion-scope-2026-10-07.md; native-query-revisit-2026-10-12.md; native-file-metadata-buffers-2026-10-16.md.
  - Current state: Explicit operation grants, reviewed payload/selection/codec/metadata owners, worker cleanup and the Q24 provider-driver correction are integrated. The source-split inventory is complete. These scoped owners do not establish a process-RSS ceiling or cover all provider allocations.
  - Dependencies: First runtime dependency. Finish concrete ownership gaps before dependent state, topology or writer experiments.
  - Implementation scope: Resource grants, native session/scan/reader/codec owners, ComputePool and AggregateChunkJobs; preparation and writer ownership.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-03.providers` **Required for the selected workload contract.** Close resource-ownership gaps that violate the declared grants of the selected comparison workloads or existing public paths. Preserve actual driver-free sort/filter/projection progress, completed Q24 and filtered-count corrections, allocation-denial cleanup and explicit excluded owners. Additional allocation classes are optional unless a concrete supported-workload gap requires them; native credits do not imply a whole-process RSS ceiling.
    - [ ] `PERF-03.regions` **Conditional.** For a newly justified topology candidate, discover generation-bound natural regions independently of worker count and run them through bounded Capillary admission. Prove complete coverage, original batch boundaries, skew progress, ordered handoff, cancellation and drain before measuring reductions or ordered candidates. The [finite projection-request deferral](native-region-projection-decision-2026-10-20.md) passes exactness and ownership checks but fails three of four actual-read targets; no timing cohort runs. Its copied-binary guard limitation is preserved, experimental code is removed and fresh workspace/native checks pass after a test-only fixture repair. This does not admit a new topology or local reduction; a materially different mechanism requires a new protocol.
      The later [scalar-region consumer candidate](../benchmarks/native-scalar-regions-2026-10-22.md) is also removed: both complete Full43 cohorts miss the frozen benefit gate and the metadata control starts excluded work. Its exact local reduction is not retained. The incomplete held-out cohort and exact baseline restoration remain explicit; no replacement mechanism is admitted.
    - [ ] `PERF-03.mixed_load` **Required for the selected workload contract.** Prove queue admission, state-growth denial, worker reuse and cancellation for the concurrent operations and retained results already supported by the selected public workflows. Keep caller/Python/library retention distinct from native credits. A general multiuser serving platform is outside this requirement.

  - Optional extensions: `PERF-03.codec_preparation` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-03 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-03); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-06` verify supported native pressure and recovery contracts.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0017/0044; native-local-completion-scope-2026-10-07.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: General aggregate, join, analytic-window and pivot pressure, input replay and finite operator cleanup are accepted. [Finite distinct-set membership](../benchmarks/native-set-membership-spill-2026-10-18.md), [seven relational unary membership/mask variants](../benchmarks/native-unary-membership-spill-2026-10-18.md), [six sampling population variants](../benchmarks/native-sampling-population-spill-2026-10-19.md) and [shared relational subquery predicates](../benchmarks/native-subquery-membership-spill-2026-10-19.md) are locally accepted under explicit spill. Fixed-count and known-bound fractional nonreplacement, legacy standalone unary state and broader recovery remain; replay spill alone does not spill consuming state.
  - Dependencies: After the relevant PERF-03 owner; before broader streaming/type compositions and output recovery depend on it.
  - Implementation scope: Native membership/population consumers, query-run store, operator pressure strategies and owned cleanup/recovery.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:

    - [ ] `PERF-06.recovery` **Required for the selected workload contract.** Prove cancellation, quota exhaustion, corrupt/truncated native runs, source-generation checks and owned cleanup for admitted comparison workloads. Preserve the actual restart or resume guarantee and deterministic failure at unsupported boundaries. New resumable execution and additional spill families require their own comparison need or optional promotion.

  - Optional extensions: `PERF-06.membership`, `PERF-06.strategy`, `PERF-06.rematerialization`, `PERF-06.merge_cost` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-06 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-06); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-07` protect existing result and publication guarantees.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0017/0044; universal-workflow-completion-2026-10-01.md; native-stream-destinations-2026-10-15.md; native-encoded-interchange-2026-10-12.md.
  - Current state: Owned native arrays, bounded flat aggregate/ordered streams, persisted output preparation, encoded receiving copies, independent native-file producers, all eight local streaming destinations and single-execution fanout are integrated. Remaining family-specific payload avoidance, transfer lifetime and publication recovery need separate proof.
  - Dependencies: After relevant PERF-03/06 owners. Generation-conditional publication follows the current correctness work; transport remains later.
  - Implementation scope: OwnedVortexResultBatch, result streams, native/compatibility writers, retained selections and output transaction owners.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:

    - [ ] `PERF-07.publication` **Required for the selected workload contract.** Make existing local writer and fanout workflows safe under source changes, replacement, cancellation and partial commits. Validate complete input before publication; preserve one execution and retained/spooled ownership. Prove the declared single-file atomicity, reader lifetime and retry/cleanup guarantees; state multi-destination limits explicitly. Safe existing writes remain required when reuse or richer transactions are deferred.

  - Optional extensions: `PERF-07.remaining_arrays`, `PERF-07.transfer_lifetime`, `PERF-07.output_windows` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-07 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-07); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-10` preserve shared plan semantics and evaluate admitted ordered candidates.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0016/0021/0022/0044; universal-workflow-completion-2026-10-01.md; performance-plan-exhaustion-2026-09-12.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: Typed native operators, owned results, exact reductions and several fused/encoded paths exist. Typed metadata and conservative pruning foundations are real; richer summaries and consumer-wide proof remain. Total-node admission is implemented, while recursive traversal depth remains bounded. The contiguous-range selection candidate was measured and dropped after repeated projection regressions; the corrected cost harness remains, and production selection behavior is restored.
  - Dependencies: PERF-03 for regions/resources; PERF-04 for local reduction before region-local ordered candidates. Semantics and ownership precede cost-based selection.
  - Implementation scope: Shared native plan/binder, metadata pruning, typed kernel selection and aggregate/order/join/window strategies.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-10.contracts` **Required for the selected workload contract.** Preserve equivalent values, logical types, NULL/bag/order semantics, encoding/selection ownership and declared memory behavior across the current comparison workloads and public SQL/Python/DataFrame/CLI surfaces. Use one admitted native lowering where semantics match. Preserve the [accepted public context/result/discovery contract](public-documentation-api-acceptance-2026-10-18.md); new types, deeper plans and additional delivery mechanisms are optional until a named comparison gap requires them.

    - [ ] `PERF-10.ordered_candidates` **Conditional.** After region and local-reduction gates, compare exact region-local ordered candidates with bounded global merge before payload access; cover offset/limit, complete multi-keys, NULL order, ties and absolute source identity. The [dependency review](native-region-dependent-prerequisites-2026-10-20.md) and [October 22 scalar-region rejection](../benchmarks/native-scalar-regions-2026-10-22.md) leave both prerequisites missing. Current array-fed workers do not establish them. No new local-candidate comparison is admitted or credited as completed.

  - Optional extensions: `PERF-10.summaries`, `PERF-10.metadata_consumers`, `PERF-10.fusion_registry`, `PERF-10.constant_keys`, `PERF-10.costs`, `PERF-10.traversal`, `PERF-10.multiway_joins`, `PERF-10.learned_indexes`, `PERF-10.compact_selections`, `PERF-10.predicate_exclusion` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-10 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-10); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-11` certify declared ingest-to-result envelopes.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0014/0033/0044; universal-workflow-completion-2026-10-01.md; native-stream-input-composition-2026-10-13.md.
  - Current state: Finite single-use/multiple/repeated producers, native replay, cancellable compatibility preparation, generated ranges and rich typed batch intake are accepted. Remaining type/source/operator/destination combinations and declared envelope tails remain.
  - Dependencies: PERF-03/06 owners and PERF-07 publication before broadening dependent compositions; PERF-10 for deeper plans.
  - Implementation scope: Shared input transaction, bounded result streams, source discovery, cancellation and latency harnesses.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-11.envelope` **Required for the selected workload contract.** Validate the declared input/output/memory/queue envelopes of selected public comparison workflows under pressure, cancellation, late input failure and slow consumers. Keep original caller owners and finite completion explicit. Preserve the current supported platform routes, including POSIX batch pipes and Windows bounded collection/writers; new platform or operator combinations are optional.

  - Optional extensions: `PERF-11.latency` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-11 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-11); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-04` verify admitted aggregate and reduction semantics.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0016/0044; performance-plan-exhaustion-2026-09-12.md; native-local-completion-scope-2026-10-07.md.
  - Current state: Shared jobs/partitions, worker grants and exact non-null UTF8 COUNT partitions exist, including removal of dual histogram/sketch maintenance. Other concrete states and actual operator scheduling are not certified by descriptor-only scheduler tests.
  - Dependencies: PERF-03 ownership first; region-local reduction additionally needs retained topology. Extend concrete states through existing jobs without waiting for a new topology. PERF-06 owns pressure transitions and PERF-05 exact keys.
  - Implementation scope: AggregateChunkJobs, native worker states, deterministic reducers and scheduling observers.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:

    - [ ] `PERF-04.semantics` **Required for the selected workload contract.** Prove stable merge order, complete keys, source identity, NULL/tie/offset/limit behavior, integer overflow and floating evaluation policy for currently admitted states and worker configurations used by comparison workloads. Existing valid evidence can satisfy this requirement. New worker/state families are optional except the exact local-reduction scope needed by an explicitly scheduled investigation.

  - Optional extensions: `PERF-04.workers`, `PERF-04.states`, `PERF-04.utilization` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-04 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-04); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-05` verify exact key identity and complete results.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0021/0044; native-shared-execution-2026-10-17.md; native-text-aggregation-screen-2026-10-17.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: Several reclaimable string, compound-key, weighted-domain and exact-distinct mechanisms are retained. The generic/direct key transition audit and broader nullable/composite pressure contracts remain.
  - Dependencies: Coordinate with PERF-04 states and PERF-06 pressure; do not revive rejected histogram/sketch or bitmap variants unchanged.
  - Implementation scope: Dictionary domains, owned/interned keys, candidate arenas and compact/general aggregate states.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-05.identity` **Required for the selected workload contract.** Verify exact dictionary-domain equality and owned/interned key identity across generic/direct transitions exercised by the selected workloads. Preserve retained arena and weighted-domain lifetimes and source identity. Repair demonstrated correctness or ownership gaps without requiring a new key representation.

    - [ ] `PERF-05.validation` **Required for the selected workload contract.** Verify complete results for the currently supported key families on NULL, all-unique, duplicate-heavy, skewed and non-URL inputs against independent references, including retained Q17/Q29/Q33/Q35-style kernels. Document deterministic unsupported layouts. Additional nested/composite family support is optional.

  - Optional extensions: `PERF-05.pressure`, `PERF-05.nested_identity` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-05 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-05); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-09` validate retained writers and evaluate justified layout costs.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; ingest-performance-implementation-2026-09-12.md; native-ingest-text-size-2026-10-18.md; native-persisted-reuse-2026-10-11.md.
  - Current state: Physical ingest plans, planner-selected large-source settings and the writer retain/drop gate exist. September tuning cleanup and the October six-profile text-size screen are complete; all five new profiles were dropped. Persisted output properties are accepted.
  - Dependencies: PERF-08 attribution and PERF-03 resource ownership; independent-region evidence before topology-aware preparation. Keep the retained writer until lifecycle proof passes.
  - Implementation scope: PhysicalIngestPlan/WriterPhysicalDesignPlan, native codec/layout providers, ordered writer queues and atomic commit.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:

    - [ ] `PERF-09.design` **Conditional.** After region evidence, cost preparation jointly for storage, ingest, schedulable topology, pruning, encoded work and merge. Cover small, wide-text, large-numeric, dictionary-heavy, unknown-row-count and constrained-memory planner inputs. The [October 20 dependency review](native-region-dependent-prerequisites-2026-10-20.md) records the missing accepted runtime region/cost basis; no preparation policy is admitted from the rejected request-deferral experiment.
      The [October 22 scalar-region rejection](../benchmarks/native-scalar-regions-2026-10-22.md) leaves this dependency missing; its partial held-out observations do not establish joint preparation costs or a completed design comparison.
    - [ ] `PERF-09.lifecycle` **Required for the selected workload contract.** Validate same-source durable ingestion, native single-file integrity/atomicity and first/repeated downstream use for retained writer changes in the comparison workload set. Preserve source validation, statistics, the existing size/ingest/query gate and no-fallback evidence. This requirement does not mandate a new writer or codec profile.

  - Optional extensions: `PERF-09.overlap`, `PERF-09.portfolio`, `PERF-09.structural_payload` are in the bottom backlog and do not block this required scope.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-09 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-09); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-12` verify selected workloads, supported platforms and comparisons.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0015/0024/0033/0044; universal-workflow-completion-2026-10-01.md; native-local-completion-scope-2026-10-07.md; production-certification-gate.md.
  - Current state: Extensive public, direct, format and Full43 packets certify finite scopes. They do not complete the broad workflow matrix, all OS/resource envelopes, production certification or comparative superiority.
  - Dependencies: Cross-cutting: focused tests during implementation, then complete acceptance at each coherent boundary. The October 18 cleanup, ship/drop audit and fresh UAT are recorded in the completed ledger.
  - Implementation scope: Complete public/native workflow matrices, UAT runners, platform/runtime evidence and release support/claim surfaces.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-12.workflows` **Required for the selected workload contract.** Certify the named comparison workloads and existing supported public read/transform/retain-or-spill/reconsume/write/reopen paths with complete values, types and lifecycle evidence. Cover relevant renamed schemas, distributions and failure variants. Record the selected source/type/destination matrix explicitly; an unbounded Cartesian expansion and all future wide/deep plans are optional.
    - [ ] `PERF-12.cohort` **Required for the selected workload contract.** Pin eligible ClickBench scoring and comparison cohorts separately from cached product sessions. Preserve standard Full43 acceptance as one execution per query and run repeated comparative cohorts only under their declared protocol. Bind baseline versions, workload semantics, data, build, cache context and resources before claiming parity.
    - [ ] `PERF-12.topology` **Conditional.** After a newly admitted topology mechanism, compare current/coarse and safe small/medium/large topology on identical immutable 100M-row input, build and resources. Preserve the original 129-call comparative protocol and classify scan/reduction/filter/grouped/ordered/global-state response with realized/pruned/materialized work; do not restart rejected coalescing without a new mechanism. The [dependency review](native-region-dependent-prerequisites-2026-10-20.md) and [October 22 scalar-region rejection](../benchmarks/native-scalar-regions-2026-10-22.md) leave no admitted replacement. No unchanged large comparison is run or credited as completed.
    - [ ] `PERF-12.support` **Required for the selected workload contract.** Prove the OS/architecture and resource envelopes actually selected for the supported comparison workflows, including installed-package execution, failures, cancellation and cleanup. Keep compatibility, known limits, upgrade/rollback and the published/development distinction explicit. Preserve required hosted Linux/Windows package checks when integration is authorized; adding platforms or services is optional. Publication beyond the authorized v0.7.0 train requires its own approval.
    - [ ] `PERF-12.comparisons` **Required for the selected workload contract.** Record a workload gap matrix in perf-12-acceptance.json: named task, independently supported baseline equivalent and version/settings, exact semantics, scale/resources, current ShardLoom outcome, material gap and required action or optional disposition. Use that evidence to decide additions before implementation. Compare complete outputs and paired uncertainty, preserve non-ClickBench controls and every failed sample, and keep CG-5/6 claim gates. A finite local pass is not broad engine parity or superiority.
  - Accepted finite evidence: the [October 22 matrix](../benchmarks/native-workload-comparisons-2026-10-22.md) and its [corrected-runtime refresh](../benchmarks/native-independent-full43-2026-10-22.md#corrected-runtime-workload-matrix) each accept all 22 declarations and 704 result pairs against pandas 3.0.3, with 368 logical-schema and 48 terminal-order checks. The former retains its failed cohort and complete replacement; the latter retains the log-retention interruption after 13 complete cohorts and executes only the five remaining cohorts. Neither observation is relabelled or repeated. Selected NULL/empty, precision, pressure, lifecycle and platform guarantees retain their separate evidence reviews. Historical pandas 2.2.3 and native Full43 records remain unchanged; no entire required child or competitive gate closes.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-12 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-12); preserve the family/protocol controls above and original experiment gates in the reconciliation.

The finite `PERF-13.pgo` investigation is closed with a drop and moved to the
[completed ledger](phased-execution-completed-ledger.md). Its historical owner
contract and [verification requirements](native-roadmap-verification-contracts-2026-10-18.md#perf-13)
remain references, not an automatic new experiment or PGO promotion.

### Scope and later roadmap references

These areas describe the broader design space, not additional requirements. Apply the competitive requirement rule above; unselected breadth remains optional. They overlap the canonical owners and add no parent checklist:

| Area | Canonical owners | Potential broader scope |
| --- | --- | --- |
| Stateful pressure and recovery | PERF-03/06/10/12 | Remaining membership/population and other unadmitted state, automatic policy selection, per-family corruption/cancellation/cleanup and honest restart/resume guarantees. |
| Remaining allocation coverage | PERF-03/06/08/09 | Unreviewed provider/structural metadata/preparation/writer owners; preserve completed decoder, compression, selection and file-buffer scopes. |
| Broader streaming | PERF-03/06/07/11/12 | Remaining stateful/type combinations and output recovery; accepted finite producers, sets, unary/subqueries and eight local destinations are foundations. |
| Adapter and composition coverage | PERF-02/07/10/11/12; CG-19/20/21 | Complete typed/nested source → transform → retain/spill → reconsume → write → reopen workflows, partition/schema evolution and deeper plans. |
| Defined local support and release | PERF-12; existing release gates | Declared OS/workload/resource envelopes, actual platform runtime, failures/recovery, compatibility, install and upgrade/rollback evidence. |

The twelve named investigations are explicit above: the original rematerialization, multiway joins, nested identities, spill-merge costs, learned indexes, physical-state cache admission, byte-credit output windows and changed-source reuse, plus compact selections/descriptors, reusable codec preparation, reversible structural payloads and exact predicate-exclusion reuse. Their prerequisites and original experiment contracts remain intact. The six CPU packages remain historical intake. Extra first-use summaries and external-memory families are optional unless a selected comparison gap requires them; safety of existing publication remains required. They add no duplicate parent projects or automatic timing runs.

| Later outcome | Existing roadmap owner and dependency |
| --- | --- |
| Independent jobs/users and analytical serving | PERF-03/11/12 and CG-11/20/21/23: deployment-driven concurrent admission/isolation/priorities, protocols, identity, cancellation and publication. Distinct from distributing one query. |
| Incremental/continuous computation | CG-9/17/22/23 and RFCs 0017/0034/0035: change identity, event time, durable progress/checkpoints and coordinated source/sink recovery. |
| Distributed individual computations | CG-10 and existing distributed/runtime owners: partition/stage ownership, exchange, skew, remote data access, recovery and coordinated publication after local contracts. |
| Federation, remote APIs and optional Foundry integration | CG-18/20/21/23 and RFC 0036: governed connectors/identity and explicit control/data planes with platform proof. The [requested Foundry package](native-foundry-integration-2026-10-25.md) is scheduled in item 20 above while remaining optional to engine users; external engines are baselines or declared sources/sinks, never runtime fallback. |

### Competitive Engine Track

These are evidence gates, not replacement phase IDs or closure claims. The [detailed ledger](phased-execution-completed-ledger.md#reference-competitive-engine-gate-detailed-checklist-ledger) and [RFC mapping](rfc-phase-traceability.md) retain their scoped status. Real Vortex output payload acceptance is distinct from CG-3 placeholder artifact-path status; superiority requires CG-5 and CG-6.

| Gate | Scope |
| --- | --- |
| CG-1 | encoded read boundary and real encoded reads |
| CG-2 | real query primitives over Vortex data |
| CG-3 | real output payload write path |
| CG-4 | commit protocol execution |
| CG-5 | correctness/differential harness |
| CG-6 | benchmark harness |
| CG-7 | physical operators/kernels |
| CG-8 | streaming/parallel/adaptive execution |
| CG-9 | lakehouse/table intelligence |
| CG-10 | object-store/distributed execution |
| CG-11 | Python/API foundation |
| CG-12 | plan portability and semantic IR |
| CG-13 | encoded-native compressed execution |
| CG-14 | runtime-adaptive optimizer and execution memory |
| CG-15 | CPU operator specialization |
| CG-16 | evidence-first execution certificates |
| CG-17 | stateful result reuse and incremental execution |
| CG-18 | universal import/deployment/baseline harness |
| CG-19 | universal native I/O envelope |
| CG-20 | SQL/operator/function/adapter/user capability certification |
| CG-21 | user data workflow and ETL |
| CG-22 | native batch/live/hybrid certified execution fabric |
| CG-23 | REST/event/remote API and explicit data planes |

`GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` remains the global-review traceability owner. Its GAR family references stay in the completed ledger and runtime-gap mapping; this reorganization does not mark global runtime gaps or release claims complete.

### Optional breadth backlog

This is the final queue section. Every item here is `deferred_out_of_v1`, has no
delivery commitment, and **may never be implemented**. Do not start these items
automatically after required work or the queued investigations. Promote only a
specific evidenced competitive gap or new explicit maintainer choice, with
scope, dependencies and acceptance recorded first. Preserve the original
finite decisions and correctness of already supported behavior. Deferral is
not completion; none of these rows blocks required-contract acceptance or an
otherwise authorized release.

Broader clauses removed from required rows are optional too: unselected provider
allocation classes, a general multiuser serving system, new restart/resume
guarantees, richer transactions, extra nested/type/plan combinations and
additional platforms. The required rows retain only the selected workload and
existing public promises. The wider design-space references above do not
silently promote these extensions.

- Optional extensions of `PERF-01` (same canonical owner):
    - [ ] `PERF-01.numeric_bridge` **Optional.** Resolve the legacy reader-generated BitPackedUnsigned contract: distinguish unpacked test/descriptive values from executable packed-provider buffers, preserve callable behavior and report actual decode/ownership bytes. The old expansion helpers are currently test-only; do not claim a production hot-path defect without a caller.

- Optional extensions of `PERF-03` (same canonical owner):
    - [ ] `PERF-03.codec_preparation` **Optional.** **Conditional.** Conditional investigation: measure repeated codec setup, then reuse immutable preparation or exclusively leased mutable workspaces through the existing shared owner. Separate recipe identity from data-derived generation identity; include reset, concurrency, reclaim, first-use/later-use costs and portable fresh-process decoding. Follow [CPU package B](native-cpu-investigations-2026-10-18.md#b--reusable-codec-preparation); cross-operation admission belongs with PERF-02.physical_cache. The [one idle prepared-dictionary experiment](native-codec-preparation-2026-10-19.md) passed focused correctness/resource/portability checks but failed frozen complete-workload and control gates; all experimental code was removed. Further retention requires a materially different mechanism and new protocol. This conditional substep remains open.

- Optional extensions of `PERF-06` (same canonical owner):
    - [ ] `PERF-06.membership` **Optional.** Implement quota-accounted native external-memory transitions for remaining resident membership/population consumers and other explicitly unadmitted state families. Preserve exact key, bag, NULL and order semantics; inventory the concrete family before extending the existing run store. Apply [CPU package E](native-cpu-investigations-2026-10-18.md#e--partition-major-external-memory-state): partition-major finalization/release, full ordinals and multiplicity, original-position masks, exact population/sampling semantics and skew progress through native runs. Capability completion is independent of a Full43 speed threshold.
    - [ ] `PERF-06.strategy` **Optional.** Select resident or native external-memory strategies automatically inside authorized memory, temporary-storage and effect policy; prove complete results at pressure transitions on supported large workloads without an arbitrary fixed 4-GiB target.
    - [ ] `PERF-06.rematerialization` **Optional.** **Conditional.** Conditional investigation: compare retain/spill/regenerate for the already observed reclaimable derived owners. First pin whole-dependency reconstruction cost, lifetime/revocation, headroom and bounded regeneration; preserve zero-reclaim alias/key/constant controls. The [private projection regeneration candidate](native-rematerialization-2026-10-19.md) passes all four memory targets and 357 release workflows, but fails wide-dependency/all-null throughput controls in both frozen cohorts. Its code is archived and removed; a materially different mechanism needs a new protocol. This broader conditional substep remains open.
    - [ ] `PERF-06.merge_cost` **Optional.** **Conditional.** Conditional investigation: capture actual spill-run sizes and stable lineage, then compare adjacent cost-aware merge schedules within reader, memory and disk-overlap grants. Preserve stable adjacency and all complete outputs; an ordering-spill connection is not this experiment. The [finite final-tail attribution](native-merge-tail-2026-10-19.md) passes all 236 release workflows but leaves only one/two-run frontiers and zero estimated adjacent-order savings. Its observer is removed; any new final-tail protocol must establish an actual choice and material tail cost. This broader conditional substep remains open.

- Optional extensions of `PERF-07` (same canonical owner):
    - [ ] `PERF-07.remaining_arrays` **Optional.** Complete the remaining physical-result and sink-family matrix using executable arrays, selections, validity and retained owners. Audit selected-row aggregate handoff, exact DISTINCT/compound and three-key Top-K/transformed grouping/pair-measure outputs; extend only missing adapters and prove late payload access, dtype/order/NULL and copied/decoded bytes at close/cancel/slice.
    - [ ] `PERF-07.transfer_lifetime` **Optional.** Preserve the completed reuse of accepted native transfers/prepared sources across later operations. Admit immutable cross-owner sharing only after explicit receiving charges, generation identity, lifetime/revocation and isolation proof; retain the accepted isolated-copy route.
    - [ ] `PERF-07.output_windows` **Optional.** **Conditional.** Conditional investigation: compare byte-credited producer/consumer overlap with the current native/transport acknowledgement path. Establish a current complete-workflow queue bottleneck first; include slow/failing consumers, bounded queues, cancellation and provisional completion. The [finite two-batch native output window](native-output-window-integration-2026-10-19.md) passes both frozen comparison cohorts and complete local acceptance. The explicit option retains the original 4-MiB grant and actual-plan restrictions; engine medians do not improve and no CLI transport window is added. Broader shape, transport and publication obligations remain open.

- Optional extensions of `PERF-10` (same canonical owner):
    - [ ] `PERF-10.summaries` **Optional.** Extend native metadata with false-negative-safe string/domain/dictionary absence, stable transform/frequency summaries and duplicate-heavy candidate directories only where retained physical representation supports them. Preserve exact/conservative/advice-only distinctions and single-artifact provenance. Apply [CPU package D](native-cpu-investigations-2026-10-18.md#d--first-use-summaries-and-exact-predicate-exclusion-reuse): earn creation/storage/read cost on first use and describe the actual transformed output; recurring exclusion facts have a separate conditional step.
    - [ ] `PERF-10.metadata_consumers` **Optional.** Connect admitted metadata to actual predicate pruning, transformed grouping, heavy-hitter candidates, exact distinct, row-ref Top-K and layout advice. Prove missing/empty/all-null/mixed-null safety with independent false-negative tests and report actual segments/rows/bytes avoided, metadata reads and exactness.
    - [ ] `PERF-10.fusion_registry` **Optional.** Extend shared filter/project/aggregate fusion and executable typed registry selection to remaining admitted specialized families. Preserve provider preconditions, candidate/blocked reasons and decoded-reference parity; remove specializations that fail correctness or their frozen complete-operation gate.
    - [ ] `PERF-10.constant_keys` **Optional.** Generalize retained literal-integer constant-key pruning to proven string/NULL constants with exact output reconstruction; do not count ConstantInt-only support as closure.
    - [ ] `PERF-10.costs` **Optional.** Cost native canonicalization, aggregate, distinct, joins, sorting and windows against actual materialization/state work; select only semantics-equivalent, resource-admitted strategies.
    - [ ] `PERF-10.traversal` **Optional.** Replace fixed recursive plan/expression and nested-schema traversal ceilings with scalable credited traversal/lowering. Prove wide/deep compositions, cancellation and deterministic resource denial without stack exhaustion; total-node admission alone does not close depth.
    - [ ] `PERF-10.multiway_joins` **Optional.** **Conditional.** Conditional investigation: compare a native compatible-domain multiway strategy with existing binary joins, including first-use preparation and exact multiplicity/NULL/order semantics. Use an independently specified workload and ordinary-join controls before production selection. The [finite three-input constraint probe](native-multiway-join-2026-10-20.md) is not retained: final public-projection integration misses a control in cohort A while B passes, so the required two-cohort retention gate is unmet without establishing a repeated regression. All complete results pass; experimental code is removed and ordinary native/public/installed-Python regression checks pass. The distinct [cyclic existence follow-up is retained](native-cyclic-existence-intake-2026-10-21.md): all four intermediate-row opportunities, 520 scored workflows across both cohorts, every control, actual-work/ownership checks and final native/workspace/CLI/public/installed-Python/Full43/format acceptance pass. Automatic admission remains limited to the exact three-memory-source Int64 semi-join shape. Its compact packet preserves failed setup attempts and replays all evidence; no publication or broader multiway completion is claimed.
    - [ ] `PERF-10.learned_indexes` **Optional.** **Conditional.** Conditional investigation: identify a large ordered structure with repeated search cost, then certify bounded-error prediction plus exact correction against ordinary search. Include construction, memory, distribution shifts and nonbenefiting controls. The [finite weighted-search predictor](native-learned-index-decision-2026-10-20.md) is not retained: both targets pass both cohorts, but two controls miss the engine bound in A while B passes. Exact results and resources pass; experimental code is removed and fresh workspace/native checks pass. This does not establish a repeated regression or close the broader owner. The separately frozen [prepared-sampling follow-up is retained](native-prepared-sampling-decision-2026-10-21.md) after both complete comparisons and final local acceptance. Its credited scalar-model owner has exact immutable identity, pressure reclaim and teardown; population state remains execution-local. Broader learned-index scope stays open.
    - [ ] `PERF-10.compact_selections` **Optional.** **Conditional.** Conditional investigation: find a production selection/descriptor expansion cost, then preserve compact membership or ordered-gather representations through real count and projection consumers. Preserve duplicate/order semantics, checked wide global positions, sliced bases, validity and retained owners; charge construction/conversion/iteration and complete-operation cost. Follow [CPU package A](native-cpu-investigations-2026-10-18.md#a--compact-selections-and-execution-descriptors). The [bounded U16 local-index mechanism](native-compact-local-indices-2026-10-19.md) is retained after both frozen memory/control cohorts and full local acceptance; its small-case engine-time cost is explicit and global gathers remain wide. The earlier [contiguous-range experiment](native-compact-selection-2026-10-18.md) remains rejected. This finite acceptance does not close broader selection/descriptor obligations.
    - [ ] `PERF-10.predicate_exclusion` **Optional.** **Conditional.** Conditional investigation: reuse exact whole-region evidence that a fully identified predicate has no TRUE result across related operations. Bind complete coverage, immutable region/source, expression/type/NULL/collation/timezone/deletion/access semantics; reject partial/cancelled/failed observations and unsafe negation. Measure construction, use and invalidation separately from first-use summaries under [CPU package D](native-cpu-investigations-2026-10-18.md#d--first-use-summaries-and-exact-predicate-exclusion-reuse). The [one-fact-per-source candidate](native-predicate-exclusion-2026-10-19.md) passes all repeated-use targets but fails two frozen single-call controls; its experimental code is archived and removed. A different mechanism requires a new protocol, and this broader conditional substep remains open.

- Optional extensions of `PERF-11` (same canonical owner):
    - [ ] `PERF-11.latency` **Optional.** Extend separately measured p50/p95/p99 to newly admitted families and the remaining bulk-load envelope, including validation/intake/queue/result stages and native, worker, Python and fresh-process surfaces. Preserve actual execution/invalidation and report engine time for routine query comparisons.

- Optional extensions of `PERF-04` (same canonical owner):
    - [ ] `PERF-04.workers` **Optional.** Move remaining Q10/Q14/Q17/Q19/Q33/Q34/Q35-style production families onto actual shared workers. After topology retention, push admitted mergeable states into regions and compare compact-state reduction with unchanged consumers.
    - [ ] `PERF-04.states` **Optional.** Extend mergeable count/sum/avg/grouped/exact-distinct/predicate and numeric/string/row-ref Top-K states beyond non-null UTF8 COUNT: include additional measures, numeric-plus-UTF8, three-key grouping and packed-pair/grouped-distinct pressure without lossy local Top-K.
    - [ ] `PERF-04.utilization` **Optional.** Measure actual kernel/merge work and worker idle/starved/backpressure time through the shared observer; remove superseded scheduling only after the retained candidate passes its complete-operation and control gates.

- Optional extensions of `PERF-05` (same canonical owner):
    - [ ] `PERF-05.pressure` **Optional.** Partition compact owned state, defer final strings and establish exact overflow transitions for remaining composite/pair/grouped-distinct families. Preserve PR #1428’s completed removal of dual histogram/sketch work.
    - [ ] `PERF-05.nested_identity` **Optional.** **Conditional.** Conditional investigation: measure repeated recursive nested-key work, then compare shared identities after charging canonicalization, collision resolution, representatives and row mapping. Preserve exact nested equality and nonrepeating controls. The [October 19 attribution and candidate](native-nested-key-identity-2026-10-19.md) found material recursive-key work but rejected the direct dictionary identity owner because four required large target cases did not activate. Production is restored; no candidate timing comparison ran. A new mechanism needs a frozen provider/wrapper admission design and complete target activation before benefit measurement.

- Optional extensions of `PERF-09` (same canonical owner):
    - [ ] `PERF-09.overlap` **Optional.** **Conditional.** Only after measured recoverable idle time, extend bounded staged pre-writer overlap across read/derived/convert/compress/ordered feed/digest/commit. Bound completed blocks and head-of-line waits; prove EOF, cancellation, transient ownership, publication races and newly written-artifact queries. The [revised October 20 attribution](native-ingest-overlap-observer-decision-2026-10-20.md) passes all fourteen overhead cells and all 98 preflight/measured workflows. Only one target clears the opportunity gate in both cohorts, so the finite result is no repeatable material signal. Instrumentation is removed and no scheduling candidate is admitted. Broader overlap remains open; the earlier inconclusive observers and unchanged gates are preserved.
    - [ ] `PERF-09.portfolio` **Optional.** **Conditional.** For genuinely different admitted Vortex profiles, measure per-column physical bytes and encode/decode CPU and decide costed portfolio admission. The [finite reduced numeric profile](native-numeric-portfolio-2026-10-20.md) passes all exact and aggregate gates but fails one individual write control in both cohorts; it is rejected and experimental sources are restored. Broader portfolio admission remains open. Do not rerun this profile or the rejected five text profiles unchanged, select only favorable columns after observation, or infer whole-file savings from per-column samples. The October 25 addition attaches exact cross-column transforms here as later conditional research: check the pinned Vortex provider, then consider integer/fixed-scale relationships and lossless residuals only after stronger execution/ingestion opportunities. Preserve overflow, exceptions, validity and required floating bits; charge dependency reads, metadata, random access, reconstruction and encoder cost. Include narrow single-column queries and uncorrelated controls, not just full-scan compression ratio. Prefer a shallow bounded dependency graph and explicit consumer capability. This is distinct from rejected single-column structural-text profiles, adds no format/dependency, and is not a delivery or release prerequisite.
    - [ ] `PERF-09.structural_payload` **Optional.** **Conditional.** Conditional investigation: test a small deterministic reversible representation for original payload structure, with literal residuals/exceptions and exact validity. Preserve logical types and bytes, useful native consumers and producer-close/move/fresh-process-reopen/write/reopen behavior; include complete artifact and workflow costs. The [finite OnPair original-text profile](native-structural-payload-2026-10-20.md) passes all exactness, activation and lifecycle checks but fails storage and write-cost gates in both cohorts; it is rejected and experimental sources are restored. A materially different mechanism needs a new design/protocol, production memory/admission proof and full-artifact lifecycle acceptance. Follow [CPU package C](native-cpu-investigations-2026-10-18.md#c--reversible-structural-payload-encoding), distinct from derived columns and rejected codec-profile sweeps.

- [ ] `PERF-02` finish prepared execution and measured reuse.
  - V1 scope classification: `deferred_out_of_v1` (optional; no delivery commitment).
  - Source: RFC 0044; native-python-surface-2026-10-10.md; native-persisted-reuse-2026-10-11.md; native-conditional-work-campaign-2026-10-07.md.
  - Current state: The in-process native Python/resource boundary and persisted native-output reuse are accepted. Remaining family lowering and high-level builder costs are separate from answer caching.
  - Dependencies: Use PERF-10 native plan/type contracts and PERF-03 ownership; preserve generation invalidation before reuse.
  - Implementation scope: Prepared native plans, resident session/source owners and Python builder lowering.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-02.families` **Optional.** Extend typed prepared execution to remaining operator/type families while preserving actual execution, invalidation and native/public parity; reuse accepted lowering instead of adding a parallel route.
    - [ ] `PERF-02.builder` **Optional.** **Conditional.** Measure high-level SQL generation/parsing/lowering against typed prepared construction on complete workflows, then remove material duplicate work through the shared plan boundary. The [existing-frontend comparison](native-builder-frontends-2026-10-19.md) passed exact semantics and control gates but missed the frozen aggregate benefit gate in both cohorts; public routing is unchanged. A materially different bridge/lowering mechanism requires its own design and protocol; this substep remains open.
    - [ ] `PERF-02.physical_cache` **Optional.** **Conditional.** Conditional investigation: show repeated reconstruction of physical state rather than repeated references to a live owner. Compare admission/reuse/eviction with exact generation identity, retained bytes and reconstruction cost. The [physical-array retention experiment](native-physical-retention-2026-10-19.md) found material repeated reconstruction but rejected both bounded retention policies: sixteen entries failed the original controls, and the initially selected one-entry policy failed controls after integration corrections. All candidate changes were archived and removed. The [retained prepared-sampling follow-up](native-prepared-sampling-decision-2026-10-21.md) satisfies finite identity/retained-credit/reclaim obligations for scalar model metadata within one immutable prepared object; it caches no population, array or result. Broader physical-state reuse needs a materially different mechanism and new protocol; this substep remains open.
    - [ ] `PERF-02.changed_source` **Optional.** **Conditional.** Conditional investigation: identify verifiably reusable regions across changed generations with exact layout/encoding identity and source validation, then compare complete preparation/use. Preserve hard generation rejection until the changed-region proof exists. Apply [CPU package F](native-cpu-investigations-2026-10-18.md#f--changed-generation-reuse-and-safe-publication): trustworthy immutable file/region identity, schema/transform/recipe/dictionary/deletion compatibility and complete refresh-versus-rebuild validation/copy/metadata costs; path/length/mtime alone are insufficient. The [unconditional whole-file adapter](native-changed-source-reuse-2026-10-20.md) passes its identity/lifecycle prerequisites and all 84 comparison workflows, but repeats initial-preparation, complete-workflow and downstream-engine gate failures. Experimental code is removed; result limitations and corrected future guards are explicit. A different mechanism requires a new protocol; general partial regions and safe replacement remain open.
  - Next outcome: Remaining admitted families use the same typed prepared boundary; a builder change requires material complete-workflow overhead.
  - Acceptance/verification: the common contract and [PERF-02 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-02); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-08` remove measured ingestion representation duplication.
  - V1 scope classification: `deferred_out_of_v1` (optional; no delivery commitment).
  - Source: RFC 0044; ingest-performance-implementation-2026-09-12.md; performance-plan-exhaustion-2026-09-12.md.
  - Current state: CPU-stage attribution and the rejected matched-owner allocation screen are complete. Dictionary-derived paths exist; forced dictionaries/plain replay caches and rejected conversion ownership must not be recreated.
  - Dependencies: PERF-03 ownership; measure actual duplication before changing representation. Feed accepted costs into PERF-09.
  - Implementation scope: Universal ingest read/normalize/derived/convert/codec/write/finalize stages and source preflight.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-08.duplication` **Optional.** Inventory repeated representations and traversal/conversion work, then remove only measured duplication. For dictionary-lifted derived columns, require a real dictionary/native metadata seam and prove unique transforms/code replay plus NULL/Unicode/temporal parity; preserve source preflight and retained layout.
    - [ ] `PERF-08.stages` **Optional.** Attribute real normalization, derived build, metadata, codec, writer-feed, write and commit CPU/wait spans; report actual queue/backpressure, stage throughput, bytes/segments/compression and no-fallback evidence without double-counting overlapping work.
  - Next outcome: A measured same-generation duplicate representation or transform removed without weakening validation.
  - Acceptance/verification: the common contract and [PERF-08 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-08); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `NATIVE-ENCODED-EXCHANGE` extend native execution across process and transport boundaries.
  - V1 scope classification: `deferred_out_of_v1` (optional; no delivery commitment).
  - Source: RFCs 0013/0035/0044; native-encoded-interchange-2026-10-12.md; then-pinned Vortex serialization/IPC and current official Flight SQL/RPC contracts.
  - Current state: Local receiving copies and independent native-file producer intake are accepted. The [October 20 protocol/provider review](native-encoded-exchange-protocol.md), [opt-in Arrow/control prerequisite](native-exchange-arrow-control-2026-10-20.md) and [frozen experiment plan](native-exchange-experiment-plan-2026-10-20.md) support the [completed finite transport rejection](native-exchange-decision-2026-10-21.md). All 650 scored workflows pass correctness, encoding, native execution and lifecycle checks; both cohorts fail aggregate benefit and wide-text compressed-Arrow controls. The private runtime is removed, while accepted local/control prerequisites remain. No public negotiated remote service is admitted.
  - Dependencies: The October 19 conditional-first investigation now has its finite decision. Any materially different candidate needs a new prospective protocol and applicable design/provider/security/ownership review. General client/remote-service completion retains the broader optimization/breadth dependencies. CG-23H is attribution, not a replacement phase.
  - Implementation scope: Native producer/transport/receiver boundary, negotiation, authorization, dictionaries/credits and client interoperability.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `NATIVE-ENCODED-EXCHANGE.protocol` **Optional.** Write the pinned-provider RFC/review: native wire/encoding/schema versions, registry/alignment/message duplication, query/partition/snapshot identity, completion/truncation, cancel/reconnect/replay and deterministic strict-native incompatibility. Define authentication, per-request authorization and transport security using established infrastructure.
    - [ ] `NATIVE-ENCODED-EXCHANGE.ownership` **Optional.** Define queue/receive/dictionary grants and exact identity, release/eviction, lifetime and isolation rules. Evaluate buffer/dictionary reuse and same-host mapped handles only with explicit rights/cleanup; version aggregate state separately and keep in-process handles distinct from serialization.
    - [ ] `NATIVE-ENCODED-EXCHANGE.experiment` **Optional.** **Conditional.** The [finite fragment mechanism is rejected](native-exchange-decision-2026-10-21.md) after both complete matched transport/consumer cohorts; no unchanged rerun or favorable-subset promotion. Broader experiment admission requires a materially different bounded producer → transport → receiver → real native-consumer mechanism with prospective semantics/security/resources, failure/cleanup and cost gates against all matched Arrow/Flight and ordinary Vortex IPC controls.
    - [ ] `NATIVE-ENCODED-EXCHANGE.clients` **Optional.** Prove ordinary Flight SQL client interoperability and explicit native client selection, then align README, field guide, capabilities, execution evidence and hosted checks before closing only the accepted scope.
  - Next outcome: Preserve the finite rejection and its evidence. Further exchange work requires a materially different justified mechanism; general protocol/ownership/client completion keeps its own gates.
  - Acceptance/verification: the common contract and [NATIVE-ENCODED-EXCHANGE commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#native-encoded-exchange); preserve the family/protocol controls above and original experiment gates in the reconciliation.

## Completed

Completed finite units and historical decisions live in [the completed ledger](phased-execution-completed-ledger.md). The [reconciliation record](native-roadmap-reconciliation-2026-10-18.md) retains the exact prior-plan identity and every old checkbox disposition.

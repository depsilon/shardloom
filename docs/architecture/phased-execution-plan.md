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
  `unsupported_boundary`. The v1 default is inclusion for anything feasible to complete with
  real runtime behavior, deterministic unsupported diagnostics, safety evidence, and release proof.
  Defer beyond v1 only when the item records a concrete reason such as unavailable external
  platform proof, unresolved safety/security design, missing protocol approval, or scope that would
  make v1 unverifiable.
- Feasible runtime/user-surface rule: do not end a phase-plan item by preserving a blocker for any
  route, operation, input, sink, or user workflow that can be implemented inside this repository
  without external platform approval or unavailable infrastructure. Convert those rows into
  implementation checklist items and create the shared runtime family, even if that requires
  redesigning the route structure. `unsupported_boundary` is reserved for external dependencies,
  effectful/platform-gated environments, explicitly rejected unsafe semantics, or work that has a
  recorded feasibility reason and a replacement design path.
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

The October 18 [public documentation and API consolidation](public-documentation-api-acceptance-2026-10-18.md)
is implemented and locally validated under PERF-10/07/11/12 with PERF-03 resource
admission preserved. Its development API remains unpublished; hosted/platform
acceptance and the release hold remain explicit. The completed scope is in the
ledger. The subsequent [compact-selection experiment](native-compact-selection-2026-10-18.md)
is measured and dropped: two projection regressions repeat under the frozen
confirmation gate, and production is restored. Its corrected cost harness and
evidence remain. The subsequent [distinct-set membership acceptance](../benchmarks/native-set-membership-spill-2026-10-18.md)
completes finite `UNION`/`INTERSECT`/`EXCEPT` pressure and cleanup under explicit
spill. The [finite unary membership acceptance](../benchmarks/native-unary-membership-spill-2026-10-18.md)
also completes distinct, first/last/all deduplication and duplicate masks under
their original operator-pressure grants. Sampling populations, subquery
indexes and broader recovery remain open. Resume the optimization/breadth
sequence below without closing any broader owner or CPU investigation.

The next active finite unit is the [sampling population pressure contract](native-sampling-population-spill-2026-10-19.md):
unknown-bound fractional samples without replacement and fixed/fractional samples
with replacement, weighted or unweighted, through the shared relational route.
Implementation and acceptance are in progress; this does not yet close or
advertise the scoped population capability.

1. The nullable-layout integration, cleanup, ship/drop readiness audit and fresh UAT are complete in the [October 18 packet](../benchmarks/native-roadmap-fresh-uat-2026-10-18.md). PR #1575 integrates the reconciled queue. Keep the next version bump held for a separate maintainer decision.
2. Continue the [six-package CPU intake](native-cpu-investigations-2026-10-18.md) from its recorded evidence. A production ordinal-allocation signal selected the contiguous-range A candidate, but its [corrected complete-operation comparison](native-compact-selection-2026-10-18.md) failed the frozen throughput gate and the runtime change was removed. A remains open for a materially different mechanism; do not rerun the unchanged candidate. B already reuses codec contexts within an operation; measure actual repeated preparation across the proposed ownership boundary before admitting another candidate. No benefit is assumed by queue intake.
3. Continue required breadth through the existing owners: remaining PERF-03 resources, PERF-06 membership/population pressure and recovery, PERF-07 generation-conditional publication, and PERF-02/10/11 source/type/operator compositions. E and F extend these owners; required capabilities do not depend on a Full43 speed gain. PERF-01 attribution and PERF-12 acceptance accompany each coherent unit.
4. Treat original-payload structural encoding (C) and first-use summaries/exact exclusion (D) as independent scoped work with their actual provider/resource prerequisites. Extend PERF-04 reductions within accepted jobs when justified; new region/topology work first passes PERF-03 admission. None of A–F requires all the others to finish first.
5. Preserve recorded drops and paused large format/text experiments. The twelve named investigations retain distinct prerequisites and frozen gates; no unchanged sweep or unimplemented candidate is waiting only for a final timing run. C investigates a different original-payload mechanism rather than repeating rejected codec profiles.
6. Queue native encoded transport after the current optimization/breadth contract. Continuous, distributed and shared-service outcomes retain the roadmap dependencies below.

The current implementation remains the RFC 0044 native execution program. Prior R3 cleanup is recorded in the completed ledger; this reconciliation adds no phase IDs and does not bypass an active cleanup dependency. CG-1 through CG-23 remain independent evidence gates.

The [reconciliation](native-roadmap-reconciliation-2026-10-18.md) maps all 19 previous parents and all 149 children (121 unchecked, 28 already checked). Five overlapping production epics become aliases of the 14 canonical owners below. Ten stale unchecked rows are completed. That immutable reconciliation contains 54 substeps; the subsequent [CPU intake](native-cpu-investigations-2026-10-18.md) adds four conditional steps and extends existing scope. The current queue has 58 substeps under the same 14 owners: 37 required and 21 conditional, including twelve named investigations. The completed pre-bump packet is in the ledger. These are obligations, not a forecast of PR count.

### Common implementation and acceptance contract

- Source and scope: use each owner’s governing RFC/provider review and the current code, tests and completed ledger. Check Vortex-native providers before inventing an abstraction; public front doors converge on shared native plan/state/sink contracts.
- ShardLoom technique review: evaluate PulseWeave, capillary work units, dynamic admission/work shaping, metadata-first work avoidance, late materialization, timing-surface separation and evidence tiers for each concrete family. Record actual activation; descriptor fields alone are not execution proof.
- Evidence and acceptance: exact complete values, logical types, NULL/bag/order semantics, source identity, resource credits, cancellation/failure/cleanup and actual work/copy/decode counters. Preserve `fallback_attempted=false` and `external_engine_invoked=false`; Vortex stays the native persistence target and compatibility formats remain explicit translation boundaries.
- Performance: freeze current workload, mechanism, samples, controls and retain/drop threshold before timing. The latest prospective speed target is at least 2% of complete-workload engine time, subject to correctness, repeatability and resource/control gates. Do not relax historical gates, combine unrelated gains or infer a whole-engine gain from a focused cohort. Routine Full43 timing is the sum of `engine_execution_seconds` for one call to each query.
- Verification: run the [owner-specific commands and named evidence outputs](native-roadmap-verification-contracts-2026-10-18.md), add exact regression selectors for new behavior, and reject zero-test results. Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace --all-targets` for nontrivial implementation, plus applicable native/provider/public matrices. Complete Full43/format acceptance at coherent batch boundaries under local storage/process guards.
- User-visible surfaces: shared CLI/Python/SQL/DataFrame behavior, native/compatibility outputs, diagnostics/certificates, documented support and workload evidence. Align affected docs/site/capabilities and hosted installed-wheel checks before claiming integration.
- Non-goals and claims: no fallback engines, unapproved effects/dependencies, package publication, global-RSS promises, or unsupported superiority/production claims. `claim_gate_status=not_claim_grade` until the corresponding workload and release gates pass.
- Ledger rule: close a finite substep only with implementation/diagnostics, tests and linked acceptance. Move completed narrative to the completed ledger; leave broader owners open. A conditional item is pending evidence, not a dropped or completed obligation.

### Canonical owners and substeps

- [ ] `PERF-01` attribute remaining work and keep execution evidence accurate.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; performance-attribution-and-execution-structure.md; native-drop-review-2026-10-18.md.
  - Current state: Fresh-source ingestion, Full43, separate engine/result/transport clocks and several measured optimization campaigns already exist. Internal overlapping work and the legacy numeric bridge still need scoped attribution.
  - Dependencies: Cross-cutting: freeze the opportunity and gate before each performance candidate; accompany every implementation family below.
  - Implementation scope: Shared timing/certificate helpers, source_backed_encoded_execution.rs, local_primitives.rs, benchmark reports and public evidence.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-01.attribution` Attribute exclusive engine/operator spans and overlapping worker work for measured hotspots; retain raw cost and negative-control evidence without treating summed work as elapsed time.
    - [ ] `PERF-01.numeric_bridge` Resolve the legacy reader-generated BitPackedUnsigned contract: distinguish unpacked test/descriptive values from executable packed-provider buffers, preserve callable behavior and report actual decode/ownership bytes. The old expansion helpers are currently test-only; do not claim a production hot-path defect without a caller.
  - Next outcome: A named current-source cost signal and a truthful boundary for each selected candidate.
  - Acceptance/verification: the common contract and [PERF-01 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-01); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-03` complete shared resource ownership and bounded scheduling.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0014/0017/0044; native-local-completion-scope-2026-10-07.md; native-query-revisit-2026-10-12.md; native-file-metadata-buffers-2026-10-16.md.
  - Current state: Explicit operation grants, reviewed payload/selection/codec/metadata owners, worker cleanup and the Q24 provider-driver correction are integrated. The source-split inventory is complete. These scoped owners do not establish a process-RSS ceiling or cover all provider allocations.
  - Dependencies: First runtime dependency. Finish concrete ownership gaps before dependent state, topology or writer experiments.
  - Implementation scope: Resource grants, native session/scan/reader/codec owners, ComputePool and AggregateChunkJobs; preparation and writer ownership.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-03.providers` Extend one CPU/memory owner to remaining I/O, codec, structural metadata, preparation and writer allocations. Prove actual driver-free sort/filter/projection progress within the grant; preserve the completed Q24 and filtered-count corrections and identify excluded owners.
    - [ ] `PERF-03.regions` **Conditional.** For a newly justified topology candidate, discover generation-bound natural regions independently of worker count and run them through bounded Capillary admission. Prove complete coverage, original batch boundaries, skew progress, ordered handoff, cancellation and drain before measuring reductions or ordered candidates.
    - [ ] `PERF-03.mixed_load` Exercise queue bytes, state growth denial, worker reuse and cancellation under mixed jobs and retained-result pressure; distinguish native credits from caller/Python/library retention and whole-process memory.
    - [ ] `PERF-03.codec_preparation` **Conditional.** Conditional investigation: measure repeated codec setup, then reuse immutable preparation or exclusively leased mutable workspaces through the existing shared owner. Separate recipe identity from data-derived generation identity; include reset, concurrency, reclaim, first-use/later-use costs and portable fresh-process decoding. Follow [CPU package B](native-cpu-investigations-2026-10-18.md#b--reusable-codec-preparation); cross-operation admission belongs with PERF-02.physical_cache.
  - Next outcome: A bounded owner or deterministic denial for each newly admitted provider allocation, with actual scheduling and drain proof.
  - Acceptance/verification: the common contract and [PERF-03 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-03); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-06` finish native pressure transitions and recovery.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0017/0044; native-local-completion-scope-2026-10-07.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: General aggregate, join, analytic-window and pivot pressure, input replay and finite operator cleanup are accepted. [Finite distinct-set membership](../benchmarks/native-set-membership-spill-2026-10-18.md) for `UNION`, `INTERSECT` and `EXCEPT`, and [seven relational unary membership/mask variants](../benchmarks/native-unary-membership-spill-2026-10-18.md), are locally accepted under explicit spill. Sampling populations, subquery indexes, legacy standalone unary state and broader recovery remain; replay spill alone does not spill consuming state.
  - Dependencies: After the relevant PERF-03 owner; before broader streaming/type compositions and output recovery depend on it.
  - Implementation scope: Native membership/population consumers, query-run store, operator pressure strategies and owned cleanup/recovery.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-06.membership` Implement quota-accounted native external-memory transitions for remaining resident membership/population consumers and other explicitly unadmitted state families. Preserve exact key, bag, NULL and order semantics; inventory the concrete family before extending the existing run store. Apply [CPU package E](native-cpu-investigations-2026-10-18.md#e--partition-major-external-memory-state): partition-major finalization/release, full ordinals and multiplicity, original-position masks, exact population/sampling semantics and skew progress through native runs. Capability completion is independent of a Full43 speed threshold.
    - [ ] `PERF-06.recovery` Complete per-family cancellation, crash/restart, quota exhaustion, corrupt/truncated run, source-generation and owned-cleanup acceptance. State separately whether a route restarts or actually resumes execution.
    - [ ] `PERF-06.strategy` Select resident or native external-memory strategies automatically inside authorized memory, temporary-storage and effect policy; prove complete results at pressure transitions on supported large workloads without an arbitrary fixed 4-GiB target.
    - [ ] `PERF-06.rematerialization` **Conditional.** Conditional investigation: compare retain/spill/regenerate for the already observed reclaimable derived owners. First pin whole-dependency reconstruction cost, lifetime/revocation, headroom and bounded regeneration; preserve zero-reclaim alias/key/constant controls.
    - [ ] `PERF-06.merge_cost` **Conditional.** Conditional investigation: capture actual spill-run sizes and stable lineage, then compare adjacent cost-aware merge schedules within reader, memory and disk-overlap grants. Preserve stable adjacency and all complete outputs; an ordering-spill connection is not this experiment.
  - Next outcome: Complete exact supported workloads under the caller’s memory/storage policy, beginning with remaining membership/population families.
  - Acceptance/verification: the common contract and [PERF-06 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-06); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-07` complete owned result and publication contracts.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0017/0044; universal-workflow-completion-2026-10-01.md; native-stream-destinations-2026-10-15.md; native-encoded-interchange-2026-10-12.md.
  - Current state: Owned native arrays, bounded flat aggregate/ordered streams, persisted output preparation, encoded receiving copies, independent native-file producers, all eight local streaming destinations and single-execution fanout are integrated. Remaining family-specific payload avoidance, transfer lifetime and publication recovery need separate proof.
  - Dependencies: After relevant PERF-03/06 owners. Generation-conditional publication follows the current correctness work; transport remains later.
  - Implementation scope: OwnedVortexResultBatch, result streams, native/compatibility writers, retained selections and output transaction owners.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-07.remaining_arrays` Complete the remaining physical-result and sink-family matrix using executable arrays, selections, validity and retained owners. Audit selected-row aggregate handoff, exact DISTINCT/compound and three-key Top-K/transformed grouping/pair-measure outputs; extend only missing adapters and prove late payload access, dtype/order/NULL and copied/decoded bytes at close/cancel/slice.
    - [ ] `PERF-07.publication` Finish generation-conditional replacement/publication and partial-commit recovery for the existing local writers/fanout. Validate complete input before publication, preserve one execution and owned retention/spooling, and prove single-file atomicity separately from multi-destination recovery/atomicity. Apply [CPU package F](native-cpu-investigations-2026-10-18.md#f--changed-generation-reuse-and-safe-publication): real writer coordination and one authoritative managed-generation transition, stable readers, ambiguous-commit retry, separate visibility/durability and reader-safe cleanup. Safe replacement remains required if reuse is dropped.
    - [ ] `PERF-07.transfer_lifetime` Preserve the completed reuse of accepted native transfers/prepared sources across later operations. Admit immutable cross-owner sharing only after explicit receiving charges, generation identity, lifetime/revocation and isolation proof; retain the accepted isolated-copy route.
    - [ ] `PERF-07.output_windows` **Conditional.** Conditional investigation: compare byte-credited producer/consumer overlap with the current native/transport acknowledgement path. Establish a current complete-workflow queue bottleneck first; include slow/failing consumers, bounded queues, cancellation and provisional completion.
  - Next outcome: Generation-checked output replacement and recoverable publication, with explicit single-file versus multi-destination guarantees.
  - Acceptance/verification: the common contract and [PERF-07 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-07); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-10` extend shared physical planning and avoided work.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0016/0021/0022/0044; universal-workflow-completion-2026-10-01.md; performance-plan-exhaustion-2026-09-12.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: Typed native operators, owned results, exact reductions and several fused/encoded paths exist. Typed metadata and conservative pruning foundations are real; richer summaries and consumer-wide proof remain. Total-node admission is implemented, while recursive traversal depth remains bounded. The contiguous-range selection candidate was measured and dropped after repeated projection regressions; the corrected cost harness remains, and production selection behavior is restored.
  - Dependencies: PERF-03 for regions/resources; PERF-04 for local reduction before region-local ordered candidates. Semantics and ownership precede cost-based selection.
  - Implementation scope: Shared native plan/binder, metadata pruning, typed kernel selection and aggregate/order/join/window strategies.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-10.contracts` Carry physical encoding, ordering, selection, ownership and memory contracts through remaining plan/result families; preserve one native lowering across SQL, Python, DataFrame and CLI aliases. The [public context/result/discovery migration](public-documentation-api-acceptance-2026-10-18.md) is locally complete; preserve its compatibility, direct retained-result admission and selected-backend lifecycle in subsequent families. Cross-platform process batch delivery remains shared work with PERF-11.envelope.
    - [ ] `PERF-10.summaries` Extend native metadata with false-negative-safe string/domain/dictionary absence, stable transform/frequency summaries and duplicate-heavy candidate directories only where retained physical representation supports them. Preserve exact/conservative/advice-only distinctions and single-artifact provenance. Apply [CPU package D](native-cpu-investigations-2026-10-18.md#d--first-use-summaries-and-exact-predicate-exclusion-reuse): earn creation/storage/read cost on first use and describe the actual transformed output; recurring exclusion facts have a separate conditional step.
    - [ ] `PERF-10.metadata_consumers` Connect admitted metadata to actual predicate pruning, transformed grouping, heavy-hitter candidates, exact distinct, row-ref Top-K and layout advice. Prove missing/empty/all-null/mixed-null safety with independent false-negative tests and report actual segments/rows/bytes avoided, metadata reads and exactness.
    - [ ] `PERF-10.ordered_candidates` **Conditional.** After region and local-reduction gates, compare exact region-local ordered candidates with bounded global merge before payload access; cover offset/limit, complete multi-keys, NULL order, ties and absolute source identity.
    - [ ] `PERF-10.fusion_registry` Extend shared filter/project/aggregate fusion and executable typed registry selection to remaining admitted specialized families. Preserve provider preconditions, candidate/blocked reasons and decoded-reference parity; remove specializations that fail correctness or their frozen complete-operation gate.
    - [ ] `PERF-10.constant_keys` Generalize retained literal-integer constant-key pruning to proven string/NULL constants with exact output reconstruction; do not count ConstantInt-only support as closure.
    - [ ] `PERF-10.costs` Cost native canonicalization, aggregate, distinct, joins, sorting and windows against actual materialization/state work; select only semantics-equivalent, resource-admitted strategies.
    - [ ] `PERF-10.traversal` Replace fixed recursive plan/expression and nested-schema traversal ceilings with scalable credited traversal/lowering. Prove wide/deep compositions, cancellation and deterministic resource denial without stack exhaustion; total-node admission alone does not close depth.
    - [ ] `PERF-10.multiway_joins` **Conditional.** Conditional investigation: compare a native compatible-domain multiway strategy with existing binary joins, including first-use preparation and exact multiplicity/NULL/order semantics. Use an independently specified workload and ordinary-join controls before production selection.
    - [ ] `PERF-10.learned_indexes` **Conditional.** Conditional investigation: identify a large ordered structure with repeated search cost, then certify bounded-error prediction plus exact correction against ordinary search. Include construction, memory, distribution shifts and nonbenefiting controls.
    - [ ] `PERF-10.compact_selections` **Conditional.** Conditional investigation: find a production selection/descriptor expansion cost, then preserve compact membership or ordered-gather representations through real count and projection consumers. Preserve duplicate/order semantics, checked wide global positions, sliced bases, validity and retained owners; charge construction/conversion/iteration and complete-operation cost. Follow [CPU package A](native-cpu-investigations-2026-10-18.md#a--compact-selections-and-execution-descriptors). The [contiguous-range experiment](native-compact-selection-2026-10-18.md) passed memory targets but failed repeated projection throughput controls and was removed. A materially different mechanism and new frozen protocol are required; this conditional substep remains open.
    - [ ] `PERF-10.predicate_exclusion` **Conditional.** Conditional investigation: reuse exact whole-region evidence that a fully identified predicate has no TRUE result across related operations. Bind complete coverage, immutable region/source, expression/type/NULL/collation/timezone/deletion/access semantics; reject partial/cancelled/failed observations and unsafe negation. Measure construction, use and invalidation separately from first-use summaries under [CPU package D](native-cpu-investigations-2026-10-18.md#d--first-use-summaries-and-exact-predicate-exclusion-reuse).
  - Next outcome: Concrete missing operator/type contracts and exact work avoidance through existing native providers.
  - Acceptance/verification: the common contract and [PERF-10 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-10); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-02` finish prepared execution and measured reuse.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; native-python-surface-2026-10-10.md; native-persisted-reuse-2026-10-11.md; native-conditional-work-campaign-2026-10-07.md.
  - Current state: The in-process native Python/resource boundary and persisted native-output reuse are accepted. Remaining family lowering and high-level builder costs are separate from answer caching.
  - Dependencies: Use PERF-10 native plan/type contracts and PERF-03 ownership; preserve generation invalidation before reuse.
  - Implementation scope: Prepared native plans, resident session/source owners and Python builder lowering.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-02.families` Extend typed prepared execution to remaining operator/type families while preserving actual execution, invalidation and native/public parity; reuse accepted lowering instead of adding a parallel route.
    - [ ] `PERF-02.builder` **Conditional.** Measure high-level SQL generation/parsing/lowering against typed prepared construction on complete workflows, then remove material duplicate work through the shared plan boundary.
    - [ ] `PERF-02.physical_cache` **Conditional.** Conditional investigation: show repeated reconstruction of physical state rather than repeated references to a live owner. Compare admission/reuse/eviction with exact generation identity, retained bytes and reconstruction cost.
    - [ ] `PERF-02.changed_source` **Conditional.** Conditional investigation: identify verifiably reusable regions across changed generations with exact layout/encoding identity and source validation, then compare complete preparation/use. Preserve hard generation rejection until the changed-region proof exists. Apply [CPU package F](native-cpu-investigations-2026-10-18.md#f--changed-generation-reuse-and-safe-publication): trustworthy immutable file/region identity, schema/transform/recipe/dictionary/deletion compatibility and complete refresh-versus-rebuild validation/copy/metadata costs; path/length/mtime alone are insufficient.
  - Next outcome: Remaining admitted families use the same typed prepared boundary; a builder change requires material complete-workflow overhead.
  - Acceptance/verification: the common contract and [PERF-02 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-02); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-11` certify bounded ingest-to-result composition.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0013/0014/0033/0044; universal-workflow-completion-2026-10-01.md; native-stream-input-composition-2026-10-13.md.
  - Current state: Finite single-use/multiple/repeated producers, native replay, cancellable compatibility preparation, generated ranges and rich typed batch intake are accepted. Remaining type/source/operator/destination combinations and declared envelope tails remain.
  - Dependencies: PERF-03/06 owners and PERF-07 publication before broadening dependent compositions; PERF-10 for deeper plans.
  - Implementation scope: Shared input transaction, bounded result streams, source discovery, cancellation and latency harnesses.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-11.envelope` Validate declared input/output/memory/queue envelopes under pressure, cancellation, late input failure and slow consumers, including remaining operator/type compositions. Keep original caller owners, finite-completion semantics and unobserved providers explicit. Cross-platform Python batch pipes remain shared work with PERF-10.contracts; the current process batch protocol requires POSIX, while Windows uses bounded collection and writers.
    - [ ] `PERF-11.latency` Extend separately measured p50/p95/p99 to newly admitted families and the remaining bulk-load envelope, including validation/intake/queue/result stages and native, worker, Python and fresh-process surfaces. Preserve actual execution/invalidation and report engine time for routine query comparisons.
  - Next outcome: An explicit supported composition envelope with complete results and resource/failure evidence.
  - Acceptance/verification: the common contract and [PERF-11 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-11); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-04` complete shared aggregate workers and deterministic reduction.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0016/0044; performance-plan-exhaustion-2026-09-12.md; native-local-completion-scope-2026-10-07.md.
  - Current state: Shared jobs/partitions, worker grants and exact non-null UTF8 COUNT partitions exist, including removal of dual histogram/sketch maintenance. Other concrete states and actual operator scheduling are not certified by descriptor-only scheduler tests.
  - Dependencies: PERF-03 ownership first; region-local reduction additionally needs retained topology. Extend concrete states through existing jobs without waiting for a new topology. PERF-06 owns pressure transitions and PERF-05 exact keys.
  - Implementation scope: AggregateChunkJobs, native worker states, deterministic reducers and scheduling observers.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-04.workers` Move remaining Q10/Q14/Q17/Q19/Q33/Q34/Q35-style production families onto actual shared workers. After topology retention, push admitted mergeable states into regions and compare compact-state reduction with unchanged consumers.
    - [ ] `PERF-04.states` Extend mergeable count/sum/avg/grouped/exact-distinct/predicate and numeric/string/row-ref Top-K states beyond non-null UTF8 COUNT: include additional measures, numeric-plus-UTF8, three-key grouping and packed-pair/grouped-distinct pressure without lossy local Top-K.
    - [ ] `PERF-04.semantics` Prove every admitted state’s stable merge order, complete keys, source identity, NULL/tie/offset/limit, integer overflow and floating evaluation policy under skew at 1/2/4/8/12 workers; generic row-count scheduling is insufficient.
    - [ ] `PERF-04.utilization` Measure actual kernel/merge work and worker idle/starved/backpressure time through the shared observer; remove superseded scheduling only after the retained candidate passes its complete-operation and control gates.
  - Next outcome: Actual shared-worker execution for a coherent remaining state family with complete semantic controls.
  - Acceptance/verification: the common contract and [PERF-04 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-04); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-05` complete exact key ownership and pressure behavior.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0014/0021/0044; native-shared-execution-2026-10-17.md; native-text-aggregation-screen-2026-10-17.md; native-state-structure-campaign-2026-10-07.md.
  - Current state: Several reclaimable string, compound-key, weighted-domain and exact-distinct mechanisms are retained. The generic/direct key transition audit and broader nullable/composite pressure contracts remain.
  - Dependencies: Coordinate with PERF-04 states and PERF-06 pressure; do not revive rejected histogram/sketch or bitmap variants unchanged.
  - Implementation scope: Dictionary domains, owned/interned keys, candidate arenas and compact/general aggregate states.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-05.identity` Bind referenced dictionary entries by exact domain-safe equality and normalize owned/interned keys across generic/direct transitions before and after source-order admission. Audit reclaimable identity/arena and weighted-domain consumers through the same ownership contract.
    - [ ] `PERF-05.pressure` Partition compact owned state, defer final strings and establish exact overflow transitions for remaining composite/pair/grouped-distinct families. Preserve PR #1428’s completed removal of dual histogram/sketch work.
    - [ ] `PERF-05.validation` Prove nulls, all-unique and duplicate-heavy keys, skew, composites and non-URL workloads against decoded references, including already retained Q17/Q29/Q33/Q35-style kernels and unsupported layouts.
    - [ ] `PERF-05.nested_identity` **Conditional.** Conditional investigation: measure repeated recursive nested-key work, then compare shared identities after charging canonicalization, collision resolution, representatives and row mapping. Preserve exact nested equality and nonrepeating controls.
  - Next outcome: Domain-safe exact keys across all admitted transitions with delayed string materialization and bounded retained state.
  - Acceptance/verification: the common contract and [PERF-05 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-05); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-08` remove measured ingestion representation duplication.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; ingest-performance-implementation-2026-09-12.md; performance-plan-exhaustion-2026-09-12.md.
  - Current state: CPU-stage attribution and the rejected matched-owner allocation screen are complete. Dictionary-derived paths exist; forced dictionaries/plain replay caches and rejected conversion ownership must not be recreated.
  - Dependencies: PERF-03 ownership; measure actual duplication before changing representation. Feed accepted costs into PERF-09.
  - Implementation scope: Universal ingest read/normalize/derived/convert/codec/write/finalize stages and source preflight.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-08.duplication` Inventory repeated representations and traversal/conversion work, then remove only measured duplication. For dictionary-lifted derived columns, require a real dictionary/native metadata seam and prove unique transforms/code replay plus NULL/Unicode/temporal parity; preserve source preflight and retained layout.
    - [ ] `PERF-08.stages` Attribute real normalization, derived build, metadata, codec, writer-feed, write and commit CPU/wait spans; report actual queue/backpressure, stage throughput, bytes/segments/compression and no-fallback evidence without double-counting overlapping work.
  - Next outcome: A measured same-generation duplicate representation or transform removed without weakening validation.
  - Acceptance/verification: the common contract and [PERF-08 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-08); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-09` cost physical layout and bounded ordered writing.
  - V1 scope classification: `required_for_v1`.
  - Source: RFC 0044; ingest-performance-implementation-2026-09-12.md; native-ingest-text-size-2026-10-18.md; native-persisted-reuse-2026-10-11.md.
  - Current state: Physical ingest plans, planner-selected large-source settings and the writer retain/drop gate exist. September tuning cleanup and the October six-profile text-size screen are complete; all five new profiles were dropped. Persisted output properties are accepted.
  - Dependencies: PERF-08 attribution and PERF-03 resource ownership; independent-region evidence before topology-aware preparation. Keep the retained writer until lifecycle proof passes.
  - Implementation scope: PhysicalIngestPlan/WriterPhysicalDesignPlan, native codec/layout providers, ordered writer queues and atomic commit.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-09.overlap` **Conditional.** Only after measured recoverable idle time, extend bounded staged pre-writer overlap across read/derived/convert/compress/ordered feed/digest/commit. Bound completed blocks and head-of-line waits; prove EOF, cancellation, transient ownership, publication races and newly written-artifact queries.
    - [ ] `PERF-09.portfolio` **Conditional.** For genuinely different admitted Vortex profiles, measure per-column physical bytes and encode/decode CPU and decide costed portfolio admission. Do not rerun the rejected five text profiles unchanged or infer whole-file savings from sampled text.
    - [ ] `PERF-09.design` **Conditional.** After region evidence, cost preparation jointly for storage, ingest, schedulable topology, pruning, encoded work and merge. Cover small, wide-text, large-numeric, dictionary-heavy, unknown-row-count and constrained-memory planner inputs.
    - [ ] `PERF-09.lifecycle` Validate same-source durable ingestion, single-file integrity/atomicity and first/repeated downstream use for every retained writer change. Apply the existing size/ingest/query gate and preserve source validation, statistics and no-fallback evidence.
    - [ ] `PERF-09.structural_payload` **Conditional.** Conditional investigation: test a small deterministic reversible representation for original payload structure, with literal residuals/exceptions and exact validity. Preserve logical types and bytes, useful native consumers and producer-close/move/fresh-process-reopen/write/reopen behavior; include complete artifact and workflow costs. Follow [CPU package C](native-cpu-investigations-2026-10-18.md#c--reversible-structural-payload-encoding), distinct from derived columns and rejected codec-profile sweeps.
  - Next outcome: A justified layout or pipeline change with complete durable-ingest and downstream-query economics, or an evidenced drop.
  - Acceptance/verification: the common contract and [PERF-09 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-09); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-12` complete workload, platform and comparative acceptance.
  - V1 scope classification: `required_for_v1`.
  - Source: RFCs 0015/0024/0033/0044; universal-workflow-completion-2026-10-01.md; native-local-completion-scope-2026-10-07.md; production-certification-gate.md.
  - Current state: Extensive public, direct, format and Full43 packets certify finite scopes. They do not complete the broad workflow matrix, all OS/resource envelopes, production certification or comparative superiority.
  - Dependencies: Cross-cutting: focused tests during implementation, then complete acceptance at each coherent boundary. The October 18 cleanup, ship/drop audit and fresh UAT are recorded in the completed ledger.
  - Implementation scope: Complete public/native workflow matrices, UAT runners, platform/runtime evidence and release support/claim surfaces.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-12.workflows` Certify read/transform/retain-or-spill/reconsume/write/reopen across formats, renamed schemas, distributions and remaining types/operators. Include held-out ingest/aggregate/relational/ownership/serving cases, source/type/destination combinations and wide/deep plans after their mechanisms; every exposed composition is tested or an explicit implementation gap.
    - [ ] `PERF-12.cohort` Pin eligible ClickBench scoring/cohorts separately from cached product sessions; preserve one-call Full43 acceptance versus explicitly authorized repeated comparative cohorts.
    - [ ] `PERF-12.topology` **Conditional.** After a newly admitted topology mechanism, compare current/coarse and safe small/medium/large topology on identical immutable 100M-row input, build and resources. Preserve the original 129-call comparative protocol and classify scan/reduction/filter/grouped/ordered/global-state response with realized/pruned/materialized work; do not restart rejected coalescing without a new mechanism.
    - [ ] `PERF-12.support` Prove ordinary and benchmark resource envelopes, late failure, slow consumers, cancellation, cleanup/recovery and actual supported OS/architecture runtimes. Record observed resources/RSS and scoped support, known issues, compatibility, upgrade/rollback and install evidence; builds alone are not platform-runtime acceptance. The [October 18 public-guide and API acceptance](public-documentation-api-acceptance-2026-10-18.md) records clean macOS installed examples, stable generation and links/search/desktop/mobile checks. Validate the changed package on hosted Linux and Windows lanes and the deployed site when integration is authorized; retain the published/development distinction and existing release hold.
    - [ ] `PERF-12.comparisons` Record complete outputs, paired uncertainty and non-ClickBench benefit for retained performance changes; separate engine clocks from transport/ingest/storage and preserve all failures and samples. CG-5/6 and claim gates remain mandatory.
  - Next outcome: Complete the remaining source/type/operator workflows and declared platform/resource envelopes; preserve independent correctness and matched comparative evidence for every new claim.
  - Acceptance/verification: the common contract and [PERF-12 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-12); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-13` evaluate profile-guided compilation only when justified.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - Source: RFC 0044 and the current native performance/control reports.
  - Current state: No PGO promotion is established. Earlier optimized-build screens retain their recorded decisions.
  - Dependencies: After measured PERF-10 instruction/dispatch costs; not an automatic sweep or another runtime route.
  - Implementation scope: Build configuration, representative training/held-out workloads and portable-package checks.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-13.pgo` **Conditional.** Establish material remaining instruction/dispatch cost, then freeze representative/held-out training, compilation/transfer break-even and portable-package correctness gates. Retain PGO only on measured complete-workflow benefit; otherwise record the drop.
  - Next outcome: A reproducible retained benefit or a recorded reasoned drop with unchanged runtime semantics.
  - Acceptance/verification: the common contract and [PERF-13 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-13); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `NATIVE-ENCODED-EXCHANGE` extend native execution across process and transport boundaries.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - Source: RFCs 0013/0035/0044; native-encoded-interchange-2026-10-12.md; then-pinned Vortex serialization/IPC and current official Flight SQL/RPC contracts.
  - Current state: Local receiving copies and independent native-file producer intake are accepted. Negotiated remote native exchange, authorization and transport runtime are not implemented by those local paths.
  - Dependencies: After the current optimization/breadth completion contract; design/provider/security gates precede implementation. CG-23H is attribution, not a replacement phase.
  - Implementation scope: Native producer/transport/receiver boundary, negotiation, authorization, dictionaries/credits and client interoperability.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `NATIVE-ENCODED-EXCHANGE.protocol` Write the pinned-provider RFC/review: native wire/encoding/schema versions, registry/alignment/message duplication, query/partition/snapshot identity, completion/truncation, cancel/reconnect/replay and deterministic strict-native incompatibility. Define authentication, per-request authorization and transport security using established infrastructure.
    - [ ] `NATIVE-ENCODED-EXCHANGE.ownership` Define queue/receive/dictionary grants and exact identity, release/eviction, lifetime and isolation rules. Evaluate buffer/dictionary reuse and same-host mapped handles only with explicit rights/cleanup; version aggregate state separately and keep in-process handles distinct from serialization.
    - [ ] `NATIVE-ENCODED-EXCHANGE.experiment` **Conditional.** Implement a bounded producer → transport → receiver → real downstream native operation with equivalent semantics/security/resources and complete error/cleanup behavior. Freeze and measure wire bytes, sender/receiver CPU, copies/materialization, memory and complete downstream cost against optimized Arrow/Flight and ordinary Vortex IPC.
    - [ ] `NATIVE-ENCODED-EXCHANGE.clients` Prove ordinary Flight SQL client interoperability and explicit native client selection, then align README, field guide, capabilities, execution evidence and hosted checks before closing only the accepted scope.
  - Next outcome: A reviewed protocol and one bounded producer-to-real-native-consumer experiment with matched Arrow/Flight and ordinary Vortex IPC controls.
  - Acceptance/verification: the common contract and [NATIVE-ENCODED-EXCHANGE commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#native-encoded-exchange); preserve the family/protocol controls above and original experiment gates in the reconciliation.

### Breadth and later roadmap coverage

Five breadth/operational areas remain. They overlap the owners above and are not five additional parent checklists:

| Area | Canonical owners | Remaining completion boundary |
| --- | --- | --- |
| Stateful pressure and recovery | PERF-03/06/10/12 | Remaining membership/population and other unadmitted state, automatic policy selection, per-family corruption/cancellation/cleanup and honest restart/resume guarantees. |
| Remaining allocation coverage | PERF-03/06/08/09 | Unreviewed provider/structural metadata/preparation/writer owners; preserve completed decoder, compression, selection and file-buffer scopes. |
| Broader streaming | PERF-03/06/07/11/12 | Remaining stateful/type combinations and output recovery; accepted finite producers, sets, unary/subqueries and eight local destinations are foundations. |
| Adapter and composition coverage | PERF-02/07/10/11/12; CG-19/20/21 | Complete typed/nested source → transform → retain/spill → reconsume → write → reopen workflows, partition/schema evolution and deeper plans. |
| Defined local support and release | PERF-12; existing release gates | Declared OS/workload/resource envelopes, actual platform runtime, failures/recovery, compatibility, install and upgrade/rollback evidence. |

The twelve named investigations are explicit above: the original rematerialization, multiway joins, nested identities, spill-merge costs, learned indexes, physical-state cache admission, byte-credit output windows and changed-source reuse, plus compact selections/descriptors, reusable codec preparation, reversible structural payloads and exact predicate-exclusion reuse. Their prerequisites and original experiment contracts remain intact. The six CPU packages also extend required first-use summaries, external-memory membership and safe publication; they do not add duplicate parent projects or imply twelve unfinished timing runs.

| Later outcome | Existing roadmap owner and dependency |
| --- | --- |
| Independent jobs/users and analytical serving | PERF-03/11/12 and CG-11/20/21/23: deployment-driven concurrent admission/isolation/priorities, protocols, identity, cancellation and publication. Distinct from distributing one query. |
| Incremental/continuous computation | CG-9/17/22/23 and RFCs 0017/0034/0035: change identity, event time, durable progress/checkpoints and coordinated source/sink recovery. |
| Distributed individual computations | CG-10 and existing distributed/runtime owners: partition/stage ownership, exchange, skew, remote data access, recovery and coordinated publication after local contracts. |
| Federation, remote APIs and optional Foundry integration | CG-18/20/21/23 and RFC 0036: governed connectors/identity and explicit control/data planes with platform proof. Foundry is optional; external engines are baselines or declared sources/sinks, never runtime fallback. |

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

## Completed

Completed finite units and historical decisions live in [the completed ledger](phased-execution-completed-ledger.md). The [reconciliation record](native-roadmap-reconciliation-2026-10-18.md) retains the exact prior-plan identity and every old checkbox disposition.

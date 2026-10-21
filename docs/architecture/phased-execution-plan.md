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

1. Finish the current six-class provider-ownership PR using its completed acceptance. The final isolated Full43 measured 49.990920425 engine seconds on the unchanged executable and input; the maintainer accepted this observation and directed continued work. Preserve the earlier 112.675662535-second observation and subsequent diagnosis. Use the [isolated Full43 procedure](../skills/benchmarking.md#isolated-full43-procedure) for future runs. This closes the immediate slowdown concern without establishing its cause or a speed gain; passing correctness alone still does not accept a material regression.
2. Use a concrete workload gap matrix under `PERF-12.comparisons` to identify requirements, then complete only the demonstrated gaps and existing public guarantees described below. Scope each gap before implementation and reuse valid accepted evidence. PERF-03 is the current work area, but completing every possible provider allocation is not a prerequisite to moving on once the required workload contract is met. Extra types, operators, platforms and implementation techniques do not become mandatory because they are feasible.
3. Complete the already requested shared prerequisites under existing IDs: a justified independent-region mechanism, exact local reduction needed by its consumers, region/preparation cost evidence and current instruction/dispatch attribution. Preserve natural batch boundaries and the single CPU/memory owner. This stage precedes the optional backlog and does not promote an entire worker/state family into a requirement.
4. When their prerequisites pass, investigate `PERF-10.ordered_candidates`, `PERF-09.design`, `PERF-12.topology`, then `PERF-13.pgo`. Freeze each comparison before collecting candidate results. Retain a verified benefit, record no signal, or drop the candidate under its original gates. A failed prerequisite remains explicit; it is not a completed experiment or permission to invent a replacement benefit.
5. Keep additional breadth and extensions of completed conditional investigations in the [optional backlog](#optional-breadth-backlog), after all required work and the queued investigations. Do not automatically start it when the earlier stages finish. There is no delivery commitment, and optional work does not block required-area acceptance or a separately authorized release.

Preserve the seventeen finite conditional decisions, both accepted follow-ups,
original failed samples, rejected mechanisms and unchanged prospective gates.
Do not rerun rejected mechanisms unchanged. Fixed-count and known-bound
fractional sampling extensions, first-use summaries, deeper traversal, wider
prepared families and remote clients are optional unless a named comparison gap
justifies them. Existing exact sampling semantics and payload-observation rules
remain protected by their [accepted contract](native-sampling-population-spill-2026-10-19.md).
The completed nullable-layout/cleanup/UAT and public/API/spill foundations remain
in their existing ledgers and PRs. Keep the version bump and publication held
for a separate maintainer decision.

The current implementation remains the RFC 0044 native execution program. Prior R3 cleanup is recorded in the completed ledger; this reconciliation adds no phase IDs and does not bypass an active cleanup dependency. CG-1 through CG-23 remain independent evidence gates.

The [reconciliation](native-roadmap-reconciliation-2026-10-18.md) and
[CPU intake](native-cpu-investigations-2026-10-18.md) retain their historical
37-required/21-conditional classification. All 58 child IDs and 14 canonical
owners remain traceable. Current placement is **15 scoped required-contract
rows, five scheduled prerequisite/investigation rows, and 38 optional rows**.
The optional rows comprise 22 former breadth requirements and 16 extensions of
finite conditional decisions. These counts describe scope placement, not new
implementation projects, completed capabilities or a PR forecast.

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

Once those prerequisites are accepted, the four pending investigations are:

1. `PERF-10.ordered_candidates`: exact local candidates and bounded global merge
   before payload access, including full multi-keys, NULL order, ties, offset/limit
   and absolute source identity.
2. `PERF-09.design`: compare joint preparation/layout choices using the accepted
   runtime cost evidence and all varied input/control families.
3. `PERF-12.topology`: compare the newly admitted mechanism across safe
   current/coarse/small/medium/large modes on the same immutable 100M-row input,
   preserving the original 129-call coverage and independent held-out controls.
4. `PERF-13.pgo`: freeze representative training and held-out evaluation, fresh
   control/instrumented/profile-use builds, complete query/ingest/pressure and
   portable-package checks, and compilation/training/transfer break-even.

The [region prerequisite review](native-region-dependent-prerequisites-2026-10-20.md)
and [PGO prerequisite review](native-pgo-prerequisite-decision-2026-10-20.md)
remain historical evidence for why these four are pending. Sequencing the work
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

    - [ ] `PERF-10.ordered_candidates` **Conditional.** After region and local-reduction gates, compare exact region-local ordered candidates with bounded global merge before payload access; cover offset/limit, complete multi-keys, NULL order, ties and absolute source identity. The [October 20 dependency review](native-region-dependent-prerequisites-2026-10-20.md) confirms both prerequisites remain after the finite projection-deferral rejection; current array-fed workers do not establish them. No new local-candidate comparison is admitted.

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
    - [ ] `PERF-12.topology` **Conditional.** After a newly admitted topology mechanism, compare current/coarse and safe small/medium/large topology on identical immutable 100M-row input, build and resources. Preserve the original 129-call comparative protocol and classify scan/reduction/filter/grouped/ordered/global-state response with realized/pruned/materialized work; do not restart rejected coalescing without a new mechanism. The [October 20 dependency review](native-region-dependent-prerequisites-2026-10-20.md) finds no newly admitted replacement; no unchanged large comparison is run.
    - [ ] `PERF-12.support` **Required for the selected workload contract.** Prove the OS/architecture and resource envelopes actually selected for the supported comparison workflows, including installed-package execution, failures, cancellation and cleanup. Keep compatibility, known limits, upgrade/rollback and the published/development distinction explicit. Preserve required hosted Linux/Windows package checks when integration is authorized; adding platforms or services is optional and the release hold remains.
    - [ ] `PERF-12.comparisons` **Required for the selected workload contract.** Record a workload gap matrix in perf-12-acceptance.json: named task, independently supported baseline equivalent and version/settings, exact semantics, scale/resources, current ShardLoom outcome, material gap and required action or optional disposition. Use that evidence to decide additions before implementation. Compare complete outputs and paired uncertainty, preserve non-ClickBench controls and every failed sample, and keep CG-5/6 claim gates. A finite local pass is not broad engine parity or superiority.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-12 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-12); preserve the family/protocol controls above and original experiment gates in the reconciliation.

- [ ] `PERF-13` evaluate profile-guided compilation only when justified.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - Source: RFC 0044 and the current native performance/control reports.
  - Current state: No PGO promotion is established. Earlier optimized-build screens retain their recorded decisions.
  - Dependencies: After measured PERF-10 instruction/dispatch costs; not an automatic sweep or another runtime route.
  - Implementation scope: Build configuration, representative training/held-out workloads and portable-package checks.
  - ShardLoom technique review: apply the common contract above to these shared owners; retain actual activation, resource and avoided-work evidence.
  - Execution checklist:
    - [ ] `PERF-13.pgo` **Conditional.** Establish material remaining instruction/dispatch cost, then freeze representative/held-out training, compilation/transfer break-even and portable-package correctness gates. Retain PGO only on measured complete-workflow benefit; otherwise record the drop. The [October 20 prerequisite decision](native-pgo-prerequisite-decision-2026-10-20.md) verifies absolute counts in 36 historical calls but finds no current dispatch attribution. Required orchestration regressions pass; no new training or PGO acceptance is claimed. Preserve the prior tested-configuration rejection and portable ThinLTO default.
  - Next outcome: Resolve or verify a concrete gap within the required contract above; preserve the separate gates for any scheduled investigation. Optional extensions do not block acceptance.
  - Acceptance/verification: the common contract and [PERF-13 commands, acceptance records and evidence filenames](native-roadmap-verification-contracts-2026-10-18.md#perf-13); preserve the family/protocol controls above and original experiment gates in the reconciliation.

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
    - [ ] `PERF-09.portfolio` **Optional.** **Conditional.** For genuinely different admitted Vortex profiles, measure per-column physical bytes and encode/decode CPU and decide costed portfolio admission. The [finite reduced numeric profile](native-numeric-portfolio-2026-10-20.md) passes all exact and aggregate gates but fails one individual write control in both cohorts; it is rejected and experimental sources are restored. Broader portfolio admission remains open. Do not rerun this profile or the rejected five text profiles unchanged, select only favorable columns after observation, or infer whole-file savings from per-column samples.
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

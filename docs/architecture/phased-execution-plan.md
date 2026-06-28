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

Use this section for the next implementation sequence. Keep it ordered by dependency and user value.
When checkbox order and workflow order differ because a completed row is waiting only for
post-merge ledger movement, follow `Current autonomous execution order`.

Current autonomous execution order:

1. Keep `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active as the standing owner for unchecked global
   architecture runtime-gap families until those rows are closed or promoted into concrete runtime
   work.
2. Create/merge the cohesive PR for `CLICKBENCH-AGGREGATE-PREDICATE-PUSHDOWN-19` when required
   checks are green.
3. Move the completed item details to the ledger after merge/session closeout.

- [ ] `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active owner for unchecked global architecture runtime
  gaps.
  - V1 scope classification: `required_for_v1`.
  - Source: `scripts/check_runtime_gap_family_burn_down.py`,
    `docs/architecture/global-architecture-review.md`, and the release/readiness validators that
    require active ownership for unchecked global review rows.
  - Current state: this is a governance owner, not a separate implementation surface. Runtime
    gap-family mappings preserve provenance back to completed GAR items, but unchecked global
    architecture review rows still need a current active owner while concrete runtime work remains
    open or external-gated.
  - ShardLoom technique review: evidence-tier controls and no-fallback discipline apply. Concrete
    implementation still belongs in shared Vortex-normalized runtime, ingest, operator, sink, or
    evidence components, not one-off route splits.
  - Execution checklist:
    - [ ] Keep this active owner present while any mapped global architecture review runtime gap
      remains unchecked.
    - [ ] For each mapped gap family, either close the global review row with runtime evidence or
      promote the next concrete shared-runtime implementation item before removing this owner.
    - [ ] Run `python3 scripts/check_runtime_gap_family_burn_down.py` whenever this owner,
      global-review rows, or runtime gap-family mappings change.
    - [ ] Move this item to the completed ledger only after all mapped unchecked global review rows
      are closed or replaced by more specific active phase-plan owners.
  - Acceptance: runtime gap-family reports always show both historical provenance and at least one
    active phase-plan owner for unchecked global architecture review rows.
  - Claim boundary: governance traceability only; no runtime, performance, production, or
    superiority claim.
  - Fallback boundary: this owner does not execute runtime work and preserves
    `fallback_attempted=false` / `external_engine_invoked=false` in its validators.

- [ ] `CLICKBENCH-AGGREGATE-PREDICATE-PUSHDOWN-19` Restore native aggregate scalar predicate
  pushdown and metadata pruning.
  - V1 scope classification: `required_for_v1`.
  - Source: corrected full local UAT
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_current_branch_phaseplan_cleanup_20260628T055026Z/rerun_q01_q13_after_projection_fix/summary.corrected.json`
    versus prior best full local UAT
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_resource_envelope_20260627T195812Z/summary.json`.
  - Current state: implementation is complete; ledger movement is pending merge/session closeout.
    Route labels stayed native Vortex aggregate, but key lanes regressed because aggregate
    predicates no longer entered Vortex filter pushdown. Evidence examples: `Q37` arrays read
    changed `7 -> 800`, `Q38` `7 -> 611`, `Q39` `7 -> 611`, `Q40` `7 -> 611`; each also changed
    `local_primitive_filter_pushdown_applied=true -> false` and
    `local_primitive_metadata_first_pruning_consulted=true -> false`. Follow-up UAT also exposed a
    Q29 transformed-dictionary regression where the general-measure route interned/decoded
    `1,798,248` strings; the fix keeps that route on uncached typed transform keys while preserving
    the count-star transform cache.
  - ShardLoom technique review: this is a metadata-first regression. Exact scalar predicates
    (`Compare`, `InList`, `IsNull`, `IsNotNull`, and conjunctions of them) must remain eligible
    for Vortex pushdown in aggregate routes. Grouped `StringContains` remains conservative until
    separate evidence proves it is faster and exact.
  - Execution checklist:
    - [x] Identify the regression boundary from UAT evidence: scalar aggregate pushdown/pruning was
      disabled, causing many more arrays to be scanned.
    - [x] Restore aggregate pushdown for exact scalar predicates while keeping grouped
      `StringContains` residual until separately proven.
    - [x] Add a focused classifier regression test proving aggregate `Compare` stays pushdownable
      while aggregate `StringContains` stays residual.
    - [x] Preserve residual-predicate columns during heavy-hitter/exact recount passes so grouped
      residual filters keep their required source columns.
    - [x] Restore transformed-dictionary general-measure updates to uncached typed keys so Q29 avoids
      broad string interning/decoding while count-star transformed dictionary cache behavior stays
      covered.
    - [x] Suppress transformed-dictionary cache evidence when no cache entries exist so general
      transformed-measure routes do not report an empty cache as a populated transform map.
    - [x] Run focused Rust tests covering route-aware predicate splitting and grouped aggregate
      mixed pushdown/residual behavior.
      - Evidence: `cargo test -p shardloom-vortex --features vortex-local-primitives --lib
        string_contains_pushdown_is_route_aware_for_aggregate_regression_guard -- --nocapture`,
        `grouped_aggregate_mixed_predicate_preserves_pushdown_and_residual_boundary`,
        `grouped_aggregate_recount_projection_preserves_residual_columns`, and
        `grouped_count_star_transformed_dictionary_reuses_group_transform_per_dictionary_value`.
        The Q29-family guard
        `grouped_general_measures_transformed_dictionary_reuses_selected_dictionary_counts` now
        asserts `group_key_storage=typed_single_key`, `decoded_string_count=0`, and no transform
        cache entries.
    - [x] Rebuild release CLI and rerun targeted UAT for `Q14`, `Q22`, `Q23`, `Q29`, `Q37`-`Q40`,
      and `Q13`.
      - Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_aggregate_pushdown_fix_20260628T061610Z/summary.json`
        and
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_all_slowed_lanes_after_pushdown_20260628T062054Z/summary.json`.
    - [x] Keep the patch only if affected lanes restore pushdown/pruning and no targeted lane
      regresses beyond local run variance.
      - Evidence: targeted slowed-lane rerun completed `18/18` successful, total `62.426s`,
        geomean `0.681s`; only Q05 appeared slower on one run, then repeated at `1.713s`,
        `1.493s`, and `1.594s`, matching normal local variance.
    - [x] Run replacement-ingest plus full 43-query UAT after the route returned to first-class
      timing.
      - Evidence: replacement ingest
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260628T062739Z/prepare_summary.json`
        completed in `421s`, produced one `.vortex` artifact at `34,933,241,344` bytes, and kept
        `fallback_attempted=false` / `external_engine_invoked=false`. Patched full 43-query UAT
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_q29_uncached_faststate_20260628T065223Z/summary.json`
        completed `43/43`, total `204.850s`, geomean `1.388s`.
    - [x] Rebuild the public `release-user-surfaces` CLI after the evidence-reporting tightening
      and rerun the Q29 contract probe.
      - Evidence:
        `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/q29_after_cache_evidence_fix_20260628T070117Z/summary.json`
        completed successfully with `local_primitive_group_key_storage=typed_single_key`,
        `local_primitive_decoded_string_count=0`, no transform-cache entries,
        `fallback_attempted=false`, and `external_engine_invoked=false`.
  - User-visible surface: shared SQL/Python/DataFrame/CLI native Vortex runtime evidence; no new
    public route family.
  - Non-goals: no new provider family, no generic membership metadata, no query-answer sidecar, no
    approximate predicates, and no external engine fallback in this patch.
  - Claim boundary: local UAT optimization evidence only; no official ClickBench or superiority
    claim.
  - Fallback boundary: every successful or blocked route must keep `fallback_attempted=false` and
    `external_engine_invoked=false`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

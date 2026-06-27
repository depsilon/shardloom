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

1. Resolve `PR-CODEX-COMMENT-RESOLUTION-1361-1363` before returning to the performance queue.
2. Keep `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active as the standing owner for unchecked global
   architecture runtime-gap families until those rows are closed or promoted into concrete runtime
   work.
3. Implement `CLICKBENCH-100M-PHYSICAL-POLICY-PLANNER-6` first so shared resource/runtime
   settings are route-aware and cannot improve one lane while regressing another.
4. Implement `CLICKBENCH-100M-SINGLE-ARTIFACT-LAYOUT-ADVISOR-9` next so ingest layout,
   compression, segment stats, and storage-size choices improve query locality inside the single
   `.vortex` artifact.
5. Implement `CLICKBENCH-100M-HIGH-CARDINALITY-AGGREGATE-7` for the remaining exact
   high-cardinality grouped lanes.
6. Implement `CLICKBENCH-100M-STRING-DOMAIN-PREDICATE-8` so URL/domain/length/search phrase
   predicates and grouping consume embedded dictionary/domain metadata before row-string scans.
7. Implement `CLICKBENCH-100M-INGEST-WRITER-COALESCING-10` after layout choices settle, then run
   one replacement ingest UAT that records load time, artifact size, segment shape, and query
   impact together.
8. Update docs, generated status surfaces, and focused validators from the implemented evidence.
9. Run focused PR validation only while implementation rows are still changing; do not run the full
   workspace suite or full ClickBench UAT until the cohesive batch is ready.
10. Create/merge the cohesive PR when required checks are green.
11. After the current optimization batch is complete, run the heavy local Desktop UAT once on the
   merged build/artifact, replacing the existing prepared `.vortex` file rather than creating
   duplicate massive artifacts.
12. Start any version/release train only after that end-of-batch UAT result is acceptable.

- [ ] `PR-CODEX-COMMENT-RESOLUTION-1361-1363` Resolve recent Codex review findings before
  returning to the ClickBench optimization queue.
  - V1 scope classification: `required_for_v1`.
  - Source: thread-aware review sweep for PRs `#1354` through `#1363`, with unresolved Codex
    threads found on `#1361`, `#1362`, and `#1363`.
  - Current state: the `DISTINCT ... ORDER BY ... LIMIT` sort lowering and direct primitive
    memory-envelope comments are already addressed in merged `main`; the worker-count bound,
    active phase-owner validation, and targeted-UAT evidence wording still need follow-up.
  - ShardLoom technique review: ResourceEnvelope/PulseWeave worker admission applies to the Vortex
    runtime-driver fix; timing-surface discipline applies to the UAT evidence wording; active
    phase ownership applies to evidence-tier controls. No new route family or external execution is
    allowed.
  - Execution checklist:
    - [x] Confirm `#1361` `DISTINCT ... ORDER BY ... LIMIT` no longer lowers into `sort_rows`.
    - [x] Confirm `#1362` direct primitive execution evidence honors requested `memory_gb`.
    - [x] Bound local Vortex current-thread worker count by local CPU availability before spawning
      workers.
    - [x] Require an active phase-plan owner for unchecked global architecture runtime-gap
      families instead of accepting completed-ledger history as active ownership.
    - [x] Clarify targeted runtime-driver UAT evidence so count/filter control rows are not
      claimed as worker-pool validation.
    - [x] Run focused validation for the Vortex worker-count unit, runtime gap-family validator,
      JSON report syntax, and formatter.
    - [ ] Open and merge the follow-up PR when required checks are green, then move this
      completed review-fix item to the completed ledger.
  - Acceptance: all still-actionable Codex review findings from the recent PR train are addressed
    in source, docs/evidence, and focused validation without creating route proliferation or
    weakening no-fallback evidence.
  - Status: implementation validated; follow-up PR/merge and ledger movement pending.

- [ ] `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` active owner for unchecked global architecture runtime
  gaps.
  - V1 scope classification: `required_for_v1`.
  - Source: `scripts/check_runtime_gap_family_burn_down.py`, `docs/architecture/global-architecture-review.md`,
    and the `#1363` review finding that completed-ledger items must not count as active owners for
    unchecked global review rows.
  - Current state: runtime gap-family mappings preserve provenance back to completed GAR items, but
    unchecked global architecture review rows need a current active owner while concrete runtime
    work remains open.
  - ShardLoom technique review: evidence-tier controls and no-fallback discipline apply. This row
    is ownership/governance only; concrete implementation still belongs in shared Vortex-normalized
    runtime, ingest, operator, sink, or evidence components, not one-off route splits.
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
  - Status: active carry-forward owner.

- [ ] `CLICKBENCH-100M-PHYSICAL-POLICY-PLANNER-6` Add a route-aware native Vortex physical policy
  planner so shared ResourceEnvelope/PulseWeave settings improve the right lanes without slowing
  state-sensitive routes.
  - V1 scope classification: `required_for_v1`.
  - Source: local 100M UAT rank review and targeted slow-lane probe
    `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/targeted_slow_lanes_resource_envelope_20260627T213204Z/summary.json`.
    The memory-aware policy fix improved `CB-Q17`, `CB-Q19`, `CB-Q21`, `CB-Q23`, `CB-Q24`,
    `CB-Q29`, `CB-Q34`, and `CB-Q35`, but `CB-Q33` regressed when the same shared runtime/capacity
    shape was applied to near-input-cardinality numeric-pair state. A single-query
    `CB-Q33` probe with the state-boundary-friendly policy returned to the prior fast range.
  - Materiality review: accepted as the first option to decide because it prevents broad knobs from
    creating hidden tail regressions. It is not front-door work; it belongs after Vortex
    normalization and must feed SQL/Python/DataFrame/CLI equally.
  - ShardLoom technique review: PulseWeave policy, ResourceEnvelope, ProofBound route evidence,
    state-pressure classes, capillary work-unit sizing, and timing-surface discipline are all
    applicable. Do not add query-specific route forks or hidden caches.
  - Decision-gated checklist:
    - [ ] Define a small physical-policy classifier for local native Vortex route families:
      stateless scan/count, string predicate, row-ref sort/top-K, string heavy-hitter top-K,
      numeric-UTF8 heavy-hitter top-K, transformed dictionary aggregate, and near-input-cardinality
      numeric-pair aggregate.
    - [ ] Route `max_parallelism`, scan runtime driver, heavy-hitter capacity, group-state budget,
      row-ref retention, and writer coalescing through the classifier instead of applying every
      knob globally.
    - [ ] Emit evidence fields for selected policy, rejected alternatives, and state-pressure
      reason so regressions can be diagnosed without rerunning the full suite.
    - [ ] Prove with focused tests that default public routes still report
      `fallback_attempted=false` and `external_engine_invoked=false`.
    - [ ] Run targeted local 100M probes for `CB-Q21`, `CB-Q24`, `CB-Q33`, `CB-Q34`, and `CB-Q35`
      before retaining the change.
  - Acceptance: the planner improves or preserves each targeted lane relative to the latest
    comparable evidence, especially restoring `CB-Q33` while keeping the `CB-Q21`/`CB-Q24`/
    `CB-Q34`/`CB-Q35` gains; no public route or user surface forks away from the shared native
    Vortex physical runtime.
  - Non-goals: no official ClickBench claim, no sidecars, no query-answer caches, no DataFusion/
    DuckDB/Spark/Polars execution, and no one-off ClickBench query special cases.
  - Status: top-priority open implementation item.

- [ ] `CLICKBENCH-100M-SINGLE-ARTIFACT-LAYOUT-ADVISOR-9` Add a single-`.vortex` layout and
  encoding advisor that improves query locality and storage size together without sidecars.
  - V1 scope classification: `required_for_v1`.
  - Source: the current 100M artifact is about `34.933 GB` from a `14.780 GB` Parquet source.
    ClickBench combined scoring includes storage size, so query-only gains can be offset by a large
    artifact if layout/encoding policy is too loose.
  - Materiality review: accepted as the main end-to-end lever after route policy and slow-lane
    kernels. It should make later scans cheaper and reduce storage score debt; it must not become
    query-specific pre-aggregation or a multi-file sidecar model.
  - ShardLoom technique review: Universal Ingest, embedded Vortex metadata, source-native
    adapters, layout advisor output, domain dictionaries, segment stats, row-position locality,
    dictionary/code preservation, and timing-surface separation are applicable.
  - Decision-gated checklist:
    - [ ] Define a reusable advisor contract for column roles: date/counter filter columns,
      high-cardinality keys, URL/domain/text columns, derived compact metadata, row-position
      locality, and wide payload columns.
    - [ ] Persist generic metadata/layout decisions inside the `.vortex` artifact only; remove or
      reject any sidecar/manifest/query-answer-cache dependency.
    - [ ] Add column-specific encoding/compression decisions that balance load time, storage size,
      and downstream query locality.
    - [ ] Expose storage-size, segment-count, row-block, compression, dictionary, and metadata
      evidence in the prepare report.
    - [ ] Run replacement ingest UAT only after code settles, replacing the existing Desktop
      artifact and recording size/load/query effects together.
  - Acceptance: artifact size or query locality materially improves without query-specific
    summaries, hidden external execution, duplicate artifacts, or unacceptable load-time growth.
  - Status: top-priority open implementation item.

- [ ] `CLICKBENCH-100M-HIGH-CARDINALITY-AGGREGATE-7` Redesign exact high-cardinality aggregate
  state for `CB-Q17`/`CB-Q19`/`CB-Q33`/related grouped lanes through reusable packed-key,
  segment-local, and spill-ready state machinery.
  - V1 scope classification: `required_for_v1`.
  - Source: local UAT tail review shows the ranking problem is dominated by a small number of
    high-cardinality/group-state lanes, not by median query latency or SQL/Python/DataFrame
    front-door overhead.
  - Materiality review: accepted as a major query-runtime lever because state construction and
    exact merge dominate these rows. Marginal hash-map tweaks should be dropped unless targeted
    UAT proves a material gain.
  - ShardLoom technique review: capillary segment-local partials, packed/dictionary composite keys,
    dynamic work shaping, memory-budgeted exact merge, late string materialization, and native
    spill contracts are applicable. External engines and approximate answers are not acceptable.
  - Decision-gated checklist:
    - [ ] Add a reusable packed-key state boundary for numeric, numeric/string, string, and
      transformed dictionary group keys without materializing visible strings until output.
    - [ ] Add segment-local capillary partial aggregation and exact merge evidence that can be
      reused by SQL/Python/DataFrame/CLI routes.
    - [ ] Add memory-budget and spill-readiness diagnostics that fail closed if exact native spill
      is required before it is certified.
    - [ ] Keep exact semantics for count, sum, avg, min, max, count-distinct, HAVING, ORDER BY,
      LIMIT, and OFFSET.
    - [ ] Run targeted local 100M probes for `CB-Q17`, `CB-Q19`, and `CB-Q33`; retain only changes
      with material improvement or materially better memory pressure at neutral runtime.
  - Acceptance: exact results remain stable, candidate/string materialization decreases, and the
    route evidence shows reusable packed/partial/merge behavior rather than a facade-specific
    implementation.
  - Status: top-priority open implementation item.

- [ ] `CLICKBENCH-100M-STRING-DOMAIN-PREDICATE-8` Move URL/domain/length/search phrase predicate
  and grouping work toward embedded dictionary/domain metadata and segment membership before raw
  row-string scans.
  - V1 scope classification: `required_for_v1`.
  - Source: `CB-Q21`, `CB-Q23`, `CB-Q29`, `CB-Q34`, and `CB-Q35` remain among the highest timing
    lanes, while the current single `.vortex` artifact already carries embedded layout/statistics
    and derived metadata that can be consumed more deeply.
  - Materiality review: accepted because string/domain work affects both count/filter and grouped
    aggregate lanes. A change is only material if it reduces scan/string/decode work or turns
    repeated transforms into dictionary/code work.
  - ShardLoom technique review: metadata-first planning, embedded single-artifact dictionaries,
    dictionary-derived metadata, segment membership sketches, capillary predicate units, and
    encoded kernels are applicable. Query-answer sidecars and private benchmark summaries are not.
  - Decision-gated checklist:
    - [ ] Add reusable dictionary/domain predicate planning for contains, LIKE/NOT LIKE, URL domain,
      string length, and SearchPhrase non-empty predicates where exact metadata proves admission.
    - [ ] Prefer dictionary-value evaluation once per dictionary entry over per-row string work,
      while preserving SQL null/string semantics.
    - [ ] Connect derived URL/domain/length metadata to grouped aggregate and sort/top-K routes
      through the same physical planner.
    - [ ] Emit evidence for dictionary metadata used, segment membership consulted, raw string scan
      fallback avoided, and exactness proof.
    - [ ] Run targeted local 100M probes for `CB-Q21`, `CB-Q23`, `CB-Q29`, `CB-Q34`, and `CB-Q35`.
  - Acceptance: at least one target string/domain lane materially improves without slowing the
    others; all successful routes stay native Vortex and no-fallback.
  - Status: top-priority open implementation item.

- [ ] `CLICKBENCH-100M-INGEST-WRITER-COALESCING-10` Reduce load-time score debt by redesigning the
  Universal Ingest -> Vortex writer path around bounded capillary stages, coalesced batch handoff,
  adaptive row-block sizing, and single-pass derive/write/digest accounting.
  - V1 scope classification: `required_for_v1`.
  - Source: replacement ingest evidence shows `380.428s` prepare wall in the latest full replacement
    run, with the dominant cost in Vortex encode/write and segment write. Earlier evidence also
    recorded `ShardLoom prepare-once=477.811s`, `Vortex write/encode=455.636s`, and
    `segment_write=454.059s`.
  - Materiality review: accepted because ClickBench combined scoring includes load time, and ingest
    work also determines the downstream single-artifact layout. Small writer knob changes are
    insufficient unless they improve wall time, segment shape, or artifact size without query
    regressions.
  - ShardLoom technique review: Universal Ingest, capillary source/read/build/encode/write units,
    PulseWeave writer pressure, dynamic coalesce/split, source-native adapters, dictionary-derived
    metadata, and timing-surface attribution are applicable.
  - Decision-gated checklist:
    - [ ] Split source read, typed builder, derived metadata, encode/layout, segment write,
      footer/register, digest, and reopen verification timing so the long pole is explicit.
    - [ ] Add bounded capillary queues and coalesced typed-batch handoff into the single Vortex
      writer while preserving ordered commit and atomic replacement.
    - [ ] Use source-native adapters: preserve Parquet/Arrow dictionaries and batches where safe;
      keep CSV/JSONL projection/schema-aware typed builders out of row materialization.
    - [ ] Compute derived metadata and digest in one pass where possible instead of rescanning
      strings or the output file.
    - [ ] Run replacement ingest UAT after route/layout changes settle; record load time,
      artifact size, segment count, and full query effect together.
  - Acceptance: load time or writer/segment timing materially improves, or artifact shape improves
    enough to justify neutral load time; no sidecars, duplicate massive files, external engines, or
    query-specific cache artifacts are introduced.
  - Status: top-priority open implementation item.


## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

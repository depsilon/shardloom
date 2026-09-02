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
When a work item is implemented, move the detailed completed record to
`docs/architecture/phased-execution-completed-ledger.md` and leave this section focused on open
checkbox work only.

Global-runtime rule: performance entries in this queue are shared ShardLoom engine/runtime
enhancements, not ClickBench-only fixes. Implement them below the SQL/Python/DataFrame/CLI front
doors after Vortex normalization so every admitted public surface benefits from the same prepared
Vortex layout, metadata-first planning, capillary work shaping, resource envelope, writer, and
evidence contracts. Retain an optimization only when focused or UAT evidence shows a material
improvement without correctness, no-fallback, or single-artifact regressions; otherwise drop or
revise it before moving on.

- [ ] `CLICKBENCH-DOMAIN-TRANSFER-1` - ClickBench leaderboard domain-transfer runtime batch

  Source: user-requested ClickBench leaderboard research on September 2, 2026, the official
  ClickBench methodology and generated leaderboard data, current local
  `docs/benchmarks/clickbench-100m-current-branch-uat.json`, and
  `docs/benchmarks/clickbench-100m-uat-burndown.json`.

  Current state: the current local 100M-row UAT evidence is not official ClickBench evidence and is
  not claim-grade. It reports a single prepared `.vortex` artifact of 38,147,848,068 bytes
  (35.53 GiB), source bytes of 14,779,976,446, ingest wall time of 301s, query total of 215.637s,
  local geomean of 1.2845s, derived hot total of 216.334s, and derived cold total of 223.587s.
  Slow rows are concentrated in high-cardinality/string top-K, transformed URL/domain grouping,
  bounded wide-row top-K, string predicate counts, and exact distinct. Official ClickBench ranks
  still require an official-compatible runner, hardware normalization, full 43-query reproducibility,
  and CG-5/CG-6 claim evidence.

  Intake review: accepted the five legally appropriate transfer candidates as implementation
  slices, not copied algorithms or competitor-derived code. Segment/granule metadata, morsel-style
  scheduling, specialized kernels, ingest-as-query-serving-layout, and columnar result discipline
  are accepted as general systems patterns to implement with original ShardLoom code. External
  engines remain baselines only; no leaderboard implementation, GPL/AGPL/SSPL/BUSL/proprietary
  code, source-available code, or query-engine integration may be copied or invoked.

  V1 scope classification: `v1_candidate_pending_feasibility` for the runtime optimizations, with
  `required_for_v1` evidence obligations for explicit no-fallback reporting, Native I/O
  certificates, correctness checks, and claim-gate wording on any benchmark output this item
  touches.

  ShardLoom technique review: metadata-first execution applies through per-segment/granule
  summaries and prepared source-state admission before row work; capillary work units apply through
  bounded scan morsels with thread-local state and deterministic merge; dynamic admission/work
  shaping applies to retaining only measured improvements and dropping slower profiles; route
  timing surface separation applies because ingest, source-state build, scan, operator, sink, and
  wrapper timing must stay separate; evidence-tier controls apply because local UAT, focused
  microbenchmarks, and official leaderboard claims require different proof levels. PulseWeave is
  relevant only as runtime-control policy after these CPU/local path changes produce stable
  telemetry, so this item must not introduce autonomous adaptive policy without measured gates.

  Vortex-first provider check: checked Vortex concepts, encodings/layouts/statistics, file I/O,
  scan/source/sink, and versioning guardrails. Use admitted upstream Vortex array/scan/file/sink
  APIs only inside `shardloom-vortex` provider boundaries, wrap Vortex-native metadata where it is
  already present, implement ShardLoom-native metadata/index state where the provider surface does
  not expose the needed ClickBench-style summaries, and reject query-engine integrations
  (`vortex-datafusion`, DuckDB, Spark, Polars, Velox, Trino, Dask, Ray) as runtime helpers.

  Execution checklist:
  - [x] Add this detailed phase-plan item with accepted transfer candidates, source/current-state
    evidence, Vortex-first decisions, no-fallback boundaries, and validation gates.
  - [x] Seed prepared/native source-state for single high-cost category, grouped-category,
    ranked/top-K, and selective predicate lanes without counting that as multi-query reuse.
  - [ ] Implement segment/granule metadata as an execution primitive: embed or derive
    min/max/null/cardinality/string-absence/top-K candidate summaries from the single Vortex
    artifact; surface deterministic metadata coverage fields; use metadata-only answers or segment
    pruning before scans; verify no sidecars and `fallback_attempted=false`.
  - [ ] Implement capillary/morsel scheduling for the prepared Vortex scan path: partition reader
    chunks into bounded work units, keep thread-local aggregate/top-K/distinct state, merge
    deterministically, report queue/parallelism/memory evidence, and keep single-thread fallback
    impossible as a silent external-engine delegation.
  - [ ] Implement specialized native kernel dispatch for the accepted hot lanes:
    string predicate count, exact distinct, high-cardinality/grouped top-K, transformed URL/domain
    grouping, and bounded wide-row top-K. Reuse `shardloom-core` specialization identifiers and
    `shardloom-vortex` runtime evidence; compare against decoded reference behavior in tests.
  - [ ] Implement ingest-as-query-serving-layout policy: update the layout/write advisor from
    report-only guidance toward measured, gated writer choices for chunk sizing, dictionary/string
    handling, clustering hints, and statistics preservation; retain only changes that improve
    load/size or query timing in focused evidence.
  - [ ] Strengthen columnar result/data-plane discipline: keep row references and compact columnar
    result batches until the declared sink boundary, avoid wide row JSON/string assembly on hot
    paths, and keep result-sink replay certificates for written Vortex outputs.
  - [ ] Run focused unit/integration checks for each shipped slice, then the required workspace
    gates: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    and `cargo test --workspace --all-targets`.
  - [ ] Run targeted 100M ClickBench UAT for changed hot lanes and the full 43-query local UAT only
    after the runtime batch stabilizes; record retain/drop decisions and update the burndown.
  - [ ] Move the completed implementation details to
    `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion.

  Completed slice evidence:
  - Focused source-state tests:
    `cargo test -p shardloom-vortex --lib --features vortex-traditional-analytics-benchmark source_state`.
  - Required gates: `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`, and
    `cargo test --workspace --all-targets`.

  Next outcome: a cohesive PR that implements the segment/granule metadata execution primitive,
  surfaces deterministic metadata coverage evidence, and uses metadata-only answers or segment
  pruning before scans where the current Vortex provider boundary admits it.

  User-visible surface: benchmark reports, CLI prepared/native Vortex batch evidence fields,
  diagnostics/capability evidence, and phase-plan documentation.

  Implementation scope: `docs/architecture/phased-execution-plan.md`,
  `shardloom-vortex/src/traditional_analytics.rs`, follow-on shared runtime helpers in
  `shardloom-core` and `shardloom-cli` only when a slice needs public route binding, and benchmark
  artifacts under `docs/benchmarks/` when UAT is rerun.

  Evidence required: exact output correctness for every optimized lane, decoded reference or
  fixture oracle checks where appropriate, Native I/O certificate evidence, materialization/decode
  boundary evidence, per-stage timing, source-state/metadata coverage, result-sink replay evidence
  for written outputs, and no-fallback/no-external-engine fields.

  Acceptance: optimized lanes produce the same results as the current decoded/reference behavior;
  performance rows either show material improvement and are retained or are removed/revised;
  prepared source-state and metadata fields classify every requested scenario; single `.vortex`
  artifact discipline is preserved; unsupported work fails deterministically; and no public
  superiority or Spark-displacement claim is emitted.

  Verification: focused `cargo test -p shardloom-vortex --lib --features
  vortex-traditional-analytics-benchmark <test-filter>` checks for each runtime slice, targeted CLI
  route tests when public SQL/Python/CLI surfaces change, required workspace validation commands,
  and local ClickBench UAT artifacts only after a stable performance batch.

  Non-goals: no copied implementation code from ClickBench competitors, no new external execution
  fallback, no Spark/DataFusion/DuckDB/Polars/Velox runtime delegation, no official ClickBench
  leaderboard claim, no object-store/distributed/spill expansion, no package release, and no
  one-off ClickBench-only route that bypasses shared Vortex-normalized runtime families.

  Claim boundary: until CG-5 correctness and CG-6 benchmark evidence are satisfied, this item may
  claim only local implementation/evidence improvements and measured local UAT changes. It may not
  claim ClickBench rank, production readiness, broad SQL/DataFrame coverage, or Spark displacement.

  Fallback boundary: all affected fields must preserve `fallback_attempted=false`,
  `external_engine_invoked=false`, `fallback_execution_allowed=false`, and deterministic
  unsupported diagnostics for unimplemented behavior.

  Ledger rule: completed detail moves to
  `docs/architecture/phased-execution-completed-ledger.md`.

Current autonomous execution order:

1. Implement the segment/granule metadata execution-primitive slice for
   `CLICKBENCH-DOMAIN-TRANSFER-1`.
2. Validate the focused Vortex metadata/pruning evidence and update this checklist.
3. Continue through capillary/morsel scheduling, specialized kernels, ingest-layout policy, and
   columnar result-boundary slices only while each can be retained by measured
   correctness and performance evidence.

Validator ownership note: `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1` remains named here as the active
global-review runtime-gap owner required by `scripts/check_runtime_gap_family_burn_down.py`. It is a
traceability owner, not an open implementation item; concrete completed work lives in the completed
ledger.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

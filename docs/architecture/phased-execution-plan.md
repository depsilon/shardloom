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

The 2026-09-02 completion-claim audit reopened the ClickBench domain-transfer optimization work as
real production implementation items. The prior ledgered work added useful policy, evidence,
front-door, and partial runtime improvements, but the fresh replacement-ingest UAT evidence proves
the writer/ingest overhaul was not complete:

- Retained pre-audit ingest baseline: `301s` local Desktop replacement-ingest wall time.
- Fresh current-branch rerun before the rejected tuning patch:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260902T201343Z`,
  `331s`.
- Rejected experimental tuning patch rerun:
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260902T234603Z`,
  `360s`, with `vortex_write_millis=311716`,
  `universal_ingest_derived_metadata_build_millis=121578`,
  `vortex_compression_millis=102253`, and one `37,918,138,164` byte `.vortex` artifact.
- Current clean implementation UAT after the 2026-09-03 production implementation slice:
  replacement ingest
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/ingest_cli_uat_gated_20260903T112348Z`
  completed in `271s` and produced one `38,147,848,068` byte `.vortex` artifact; full 43-query
  UAT
  `/Users/dylan/Desktop/shardloom-clickbench-100m-uat/logs/full43_after_clean_ingest_current_impl_20260903T112902Z/summary.json`
  completed `129/129` CLI runs with best-of-3 query total `189.197s`, geomean `1.142898s`,
  `fallback_attempted=false`, and `external_engine_invoked=false`.

These entries use the official ClickBench generated leaderboard data and public system docs as
directional strategy input only. ShardLoom local Desktop UAT is not official ClickBench hardware and
must not be reported as a leaderboard or superiority claim.

- [ ] `CLICKBENCH-PRODUCTION-WRITER-PHYSICAL-DESIGN-1` build a real production Universal Ingest
      writer physical-design pipeline instead of writer-policy constants.
  - Source: maintainer correction on 2026-09-02 that the domain-transfer writer overhaul was marked
    complete without a real writer architecture change; official ClickBench generated data showing
    load/size leaders benefit from load-time physical design; local UAT evidence above showing the
    current writer path is still the dominant long pole.
  - Current state: `vortex_ingest` has writer profile selection, row-block/coalescing constants,
    broad source-text Zstd overrides, worker-pool evidence, compression timing, and a streaming
    `ArrayIterator`. It still sends one ordered stream through one final
    `VortexSession::write_options().write(ArrayStream)` boundary, and the rejected 2026-09-02
    tuning patch worsened replacement ingest from the retained `301s` baseline to `360s`.
  - Intake review:
    - Accepted from the five technique review: treat ingest as physical design, not serialization.
    - Merged with prior writer/resource evidence rows: `UNIVERSAL-INGEST-WRITER-RESOURCE-ARTIFACT-1`,
      `GLOBAL-WRITER-ENCODE-COALESCING-1`, `CLICKBENCH-100M-INGEST-WRITER-COALESCING-10`, and
      `CLICKBENCH-INGEST-WRITER-SEGMENT-ECONOMICS-15`.
    - Corrected audit disposition: those rows remain historical evidence/policy work and cannot be
      cited as production writer-overhaul completion.
  - V1 scope classification: `required_for_v1` for credible local production ingest posture and
    `v1_candidate_pending_feasibility` for any provider feature that requires upstream Vortex
    writer API changes.
  - ShardLoom technique review: apply dynamic work shaping to choose row-group tasks, row-block
    size, bytes-per-block, compression, and derived metadata budget; use capillary work units for
    source read, derived build, Arrow-to-Vortex conversion, compression, and final sink feed; use
    PulseWeave only at stage boundaries for backpressure/starvation evidence; keep metadata-first
    layout advice from Parquet/Vortex footers; keep evidence-tier controls so rejected tuning
    patches are automatically marked dropped.
  - Execution checklist:
    - [x] Add a `PhysicalIngestPlan`/`WriterPhysicalDesignPlan` data model with source footer facts,
      schema classes, source row-group/task topology, row-count/byte estimates, desired Vortex
      layout shape, compression plan, derived metadata plan, resource-envelope budget, and
      retain/drop thresholds.
    - [x] Replace hardcoded large-source writer constants with planner decisions that are stable,
      digestible, and explainable in public prepare evidence.
    - [ ] Build a staged pipeline for source read, embedded-derived construction, Arrow-to-Vortex
      conversion, compression/layout preparation, ordered final writer feed, digest, and atomic
      commit, with bounded queues and explicit single-artifact cleanup.
    - [ ] Parallelize CPU-heavy pre-writer work under ShardLoom control before the final Vortex sink
      boundary, and report how much time is source wait, derived-build CPU, compression CPU,
      writer-feed wait, final Vortex write, and commit.
    - [ ] Add a retain/drop gate that rejects writer profiles slower than the retained replacement
      ingest baseline or that regress artifact size/query UAT without a documented reason.
    - [ ] Remove or revert the 2026-09-02 experimental tuning patch if its only evidence remains the
      `360s` rejected UAT run.
    - [ ] Add unit tests for planner decisions across small sources, large numeric sources,
      ClickBench-like wide text sources, source-dictionary-heavy sources, missing row-count sources,
      and memory-constrained profiles.
    - [ ] Add integration/UAT harness fields for stage CPU utilization, queue depth/backpressure,
      rows/sec by stage, artifact bytes, segment count, compression bytes/time, and
      `fallback_attempted=false` / `external_engine_invoked=false`.
    - [x] Run replacement-ingest UAT and full 43-query UAT after implementation before moving the
      item to the completed ledger; current clean implementation evidence is
      `ingest_cli_uat_gated_20260903T112348Z` at `271s` and
      `full43_after_clean_ingest_current_impl_20260903T112902Z` at `189.197s` best-of-3 query
      total.
  - Next outcome: ShardLoom has a production writer physical-design implementation that can be
    retained only if it beats the `301s` local replacement-ingest baseline or provides an explicit
    correctness/single-artifact tradeoff accepted by the maintainer.
  - User-visible surface: `prepare dataframe`, SQL/Python/DataFrame prepare-once flows, ClickBench
    ingest UAT, public workflow preparation evidence, and benchmark docs.
  - Implementation scope: `shardloom-vortex/src/vortex_ingest.rs`,
    `shardloom-vortex/src/universal_format_io.rs`, public prepare evidence lifting in
    `shardloom-cli`, ingest UAT scripts, benchmark JSON artifacts, and this ledger/plan pair.
  - Evidence required: focused planner tests, writer integration tests, replacement-ingest UAT,
    full 43-query UAT, artifact-size comparison, segment-count comparison, CPU/stage attribution,
    no-fallback/no-external-engine assertions, and clear retain/drop decision.
  - Acceptance: a single `.vortex` artifact is produced; unsupported writer shapes fail explicitly;
    no query-answer sidecars or external engines are introduced; current retained correctness holds;
    replacement-ingest wall time improves versus `301s` or the optimization is dropped and the plan
    records why.
  - Verification: `cargo fmt --all -- --check`, focused `shardloom-vortex` writer/ingest tests,
    focused `shardloom-cli` public prepare evidence tests,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, replacement-ingest UAT with
    `scripts/run_clickbench_ingest_uat.sh --replace-existing`, and full 43-query local UAT.
  - Non-goals: no Spark/DataFusion/DuckDB/Polars/Velox runtime fallback; no `vortex-datafusion`;
    no temporary multi-file public artifact; no query-answer cache; no copied competitor code; no
    official ClickBench claim from local Desktop evidence.
  - Claim boundary: local implementation and UAT evidence only until official benchmark hardware,
    reproducible scripts, and release claim gates exist.
  - Fallback boundary: every success or rejection must record `fallback_attempted=false`,
    `external_engine_invoked=false`, and `fallback_execution_allowed=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with the before/after ingest timing and
    retain/drop decision.

- [ ] `CLICKBENCH-PRODUCTION-SEGMENT-METADATA-PRIMITIVE-1` promote segment/granule metadata from
      evidence strings into a reusable execution primitive.
  - Source: official/public ClickHouse sparse primary-index and skip-index documentation, Firebolt
    primary-index documentation, ClickBench size/cold leaders, ShardLoom Vortex-first metadata and
    pruning guardrails, and the 2026-09-02 ledger audit finding that several completed rows reported
    metadata availability without a reusable production primitive.
  - Current state: ShardLoom records row-group extents, footer segment counts, basic statistics
    posture, layout inventories, some derived-column metadata, and query-lane evidence. It does not
    yet have a single production `SegmentMetadata`/`GranuleMetadata` contract consumed uniformly by
    pruning, grouping, top-K, writer layout advice, and public certificates.
  - Intake review:
    - Accepted from the five technique review: make segment/granule metadata an execution
      primitive.
    - Merged with prior evidence rows:
      `PARQUET-ROWGROUP-EXTENT-COMPILER-1`, `GLOBAL-METADATA-FIRST-FAST-LANES-1`,
      `GLOBAL-DICTIONARY-DERIVED-METADATA-1`, `CLICKBENCH-100M-SINGLE-ARTIFACT-LAYOUT-ADVISOR-9`,
      and `CLICKBENCH-100M-STRING-DOMAIN-PREDICATE-8`.
    - Corrected audit disposition: earlier rows provided useful evidence and partial query use, but
      not a shared production metadata primitive with false-negative-safe semantics.
  - V1 scope classification: `required_for_v1` for local prepared Vortex execution; remote/object
    store/table metadata remains `unsupported_boundary` until separately admitted.
  - ShardLoom technique review: use metadata-first execution before source reads or decoded scans;
    attach metadata to capillary segment units; apply dynamic work shaping from segment selectivity
    and byte estimates; use evidence-tier controls to separate exact metadata, conservative
    summaries, approximations, and diagnostic-only rows; PulseWeave applies only to planning and
    pruning evidence.
  - Execution checklist:
    - [x] Define a typed `SegmentMetadata` model with row range, physical byte range, row count,
      null count/posture, min/max where ordered, byte-length bounds, dictionary membership summary,
      string/domain absence certificates, cardinality sketch status, and source provenance.
    - [x] Define exactness levels: exact, conservative-may-read, approximate-for-advice-only, and
      diagnostic-only, with explicit fail-closed behavior when a query would need exact metadata.
    - [x] Persist or reconstruct the metadata through the single `.vortex` artifact contract; any
      temporary ingest-only structure must either be embedded, derivable from Vortex footer/layout
      facts, or marked non-production.
    - [ ] Feed the metadata primitive into predicate pruning, URL/domain grouping, heavy-hitter
      candidate generation, exact distinct, row-ref top-K, writer layout advice, and estimate/explain
      evidence.
    - [ ] Add tests proving no false negatives for min/max, null, byte-length, dictionary membership,
      domain absence, empty/all-null segments, mixed-null segments, and missing-metadata blockers.
    - [x] Add public primitive evidence fields for row count, segment count, exactness/provenance,
      metadata read plan, per-family admission, sidecar/fallback status, and deterministic digest.
    - [ ] Add public query-consumption evidence fields for `segments_total`, `segments_pruned`, `segments_read`,
      `rows_pruned`, `bytes_pruned`, metadata exactness, metadata source, and fallback status.
    - [ ] Run targeted ClickBench lanes that should benefit from metadata before full UAT: Q21-Q24
      predicate/top-K, Q29 transformed grouping, Q33/Q34/Q35 heavy-hitter/domain rows, and exact
      distinct rows.
  - Next outcome: query planning and execution can make conservative segment decisions from one
    reusable metadata primitive instead of ad hoc per-route evidence strings.
  - User-visible surface: explain/estimate/capability reports, public workflow evidence, query UAT
    rows, and prepared-state certificates.
  - Implementation scope: Vortex prepared-state metadata types, source adapter extent metadata,
    local primitive planners, predicate/group/top-K execution, CLI evidence lifting, benchmark
    artifacts, and docs.
  - Evidence required: unit tests for exactness and blockers, decoded-reference parity tests,
    targeted query UAT, full 43-query UAT if retained, no-fallback evidence, and artifact-size
    impact.
  - Acceptance: metadata-backed pruning never drops matching rows; metadata absence fails closed or
    reads conservatively; query evidence shows actual segment/row/byte reduction where expected;
    no sidecar is required for the product artifact.
  - Verification: focused metadata primitive tests, focused local primitive pruning tests, CLI
    public evidence tests, `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full UAT after the retained
    batch.
  - Non-goals: no Bloom/filter implementation with unproven false-negative behavior; no query
    answer caches; no external search/index engine; no object-store/table metadata claim.
  - Claim boundary: local prepared Vortex metadata execution only, not official ClickBench or
    broad production/table support.
  - Fallback boundary: all metadata decisions must preserve `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with exactness tests and UAT evidence.

- [ ] `CLICKBENCH-PRODUCTION-MORSEL-SCHEDULER-THREADLOCAL-MERGE-1` replace route-local partial
      parallelism with a reusable morsel scheduler, thread-local state, and deterministic merge
      contract.
  - Source: public CedarDB/HyPer/Umbra lineage around morsel-driven parallelism and data-centric
    execution, ClickHouse processing-lane documentation, and local evidence that `max_parallelism=12`
    does not currently imply all hot stages use all cores.
  - Current state: ShardLoom has capillary work-unit vocabulary, Parquet row-group source workers,
    some aggregate partial-state routes, and Vortex writer worker-pool evidence. It does not have a
    common scheduler that makes every segment/morsel operator pull bounded work, keep thread-local
    state, report utilization, and merge deterministically.
  - Intake review:
    - Accepted from the five technique review: morsel-style scheduling with thread-local state and
      deterministic merge.
    - Merged with prior rows:
      `AGGREGATE-PARTIAL-PARALLELISM-1`, `CLICKBENCH-100M-HIGH-CARDINALITY-AGGREGATE-7`,
      `CLICKBENCH-100M-TAIL-LANE-OPTIMIZATION-20`, `GLOBAL-HIGH-CARDINALITY-AGGREGATION-1`, and
      `GLOBAL-EXACT-DISTINCT-GROUPED-1`.
    - Corrected audit disposition: earlier partial implementations remain valid per-route
      improvements but do not close a production shared scheduling substrate.
  - V1 scope classification: `required_for_v1` for local analytic runtime families; distributed
    scheduling remains `unsupported_boundary`.
  - ShardLoom technique review: use capillary morsels as the unit of parallel work; PulseWeave
    should record starvation, skew, and backpressure at stage boundaries; dynamic work shaping should
    adapt morsel size from row/byte/selectivity evidence; metadata-first pruning should reduce the
    morsel queue before execution; evidence tiers must distinguish single-thread, source-overlap,
    operator-parallel, and final-merge work.
  - Execution checklist:
    - [x] Define a reusable `MorselScheduler` abstraction over prepared Vortex segments, Parquet
      row-group tasks, generated sources, and future local compatibility sources, with bounded
      queueing and stable work IDs.
    - [ ] Define thread-local state traits for count, sum/avg, grouped aggregate, exact distinct,
      string heavy-hitter, numeric/string top-K, row-ref top-K, and predicate count families.
    - [x] Implement deterministic merge ordering for the shared scheduler state contract, including
      stable worker-index merge ordering and test coverage across requested worker counts. Every
      concrete top-K/distinct/aggregate family still needs its own tie/null/floating-point policy
      before this item can close.
    - [ ] Implement deterministic merge ordering for every state family, including ties, null
      ordering, floating-point accumulation policy, source-order preservation, and limit/offset.
    - [x] Add memory-envelope admission per worker and per merge stage, with spill/fail-closed
      diagnostics when exact state exceeds budget.
    - [x] Add utilization evidence: requested/applied workers, runnable morsels, completed morsels,
      mean/max stage time, skew ratio, merge time, worker summaries, and rows/sec.
    - [ ] Add worker idle/starved/backpressure time once route operators run directly through the
      shared scheduler observer rather than only validating the consumed morsel contract.
    - [ ] Convert at least the Q10/Q14/Q17/Q19/Q33/Q34/Q35-style families to the shared scheduler
      before claiming this item complete.
    - [x] Test deterministic row-count scheduling at parallelism `1`, `2`, `3`, and `12`, plus
      memory-budget blockers, metadata-pruned no-work behavior, custom observer state execution,
      and no-fallback evidence fields.
    - [ ] Test intentionally skewed segment distributions and deterministic tie cases for each
      concrete top-K/distinct/aggregate state family after those states are routed through the
      shared scheduler.
  - Next outcome: hot query families use one production parallel execution contract rather than
    isolated per-route worker logic or policy evidence.
  - User-visible surface: query runtime, public workflow execution evidence, explain/estimate
    operator plans, benchmark/UAT artifacts, and resource diagnostics.
  - Implementation scope: local primitive execution, aggregate/top-K state types, scheduler/resource
    helpers, CLI evidence lifts, test fixtures, and UAT artifacts.
  - Evidence required: decoded-reference parity, determinism tests across worker counts, resource
    admission tests, targeted slow-lane UAT, full UAT if retained, and no-fallback evidence.
  - Acceptance: changed query families produce byte-for-byte stable outputs across parallelism
    settings; evidence proves operator-stage parallel work, not only source prefetch or writer pool
    configuration; slow lanes improve or the conversion is dropped/revised.
  - Verification: focused scheduler/state tests, local primitive family tests, CLI evidence tests,
    `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no distributed runtime, no approximate answers for exact SQL semantics, no external
    execution engine, no hidden row-materialized fallback.
  - Claim boundary: local single-node scheduler/runtime evidence only; no official ClickBench,
    production cluster, object-store, or Spark-displacement claim.
  - Fallback boundary: unsupported scheduling paths must fail or use existing ShardLoom-native
    serial execution with explicit evidence, never external fallback.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with worker-utilization and
    determinism evidence.

- [ ] `CLICKBENCH-PRODUCTION-SPECIALIZED-KERNEL-REGISTRY-1` turn hot-lane policy dispatch into a
      production native kernel registry with encoded preconditions and correctness contracts.
  - Source: public HyPer/CedarDB code-generation and specialized execution references, DuckDB vector
    format documentation, Vortex encoded-array/provider capabilities, and the 2026-09-02 audit
    finding that some completed rows described native dispatch evidence without a centralized
    production kernel contract.
  - Current state: ShardLoom has many specialized helpers and route-family labels for dictionary
    predicates, heavy-hitter recount, exact distinct, transformed grouping, packed keys, and
    row-ref top-K. Preconditions, null behavior, encoding requirements, materialization boundaries,
    and retain/drop evidence are spread across route-specific code and evidence strings.
  - Intake review:
    - Accepted from the five technique review: specialized kernels without adopting an external
      query engine.
    - Merged with prior rows: `CODE-SPACE-OLAP-RUNTIME-1`,
      `RUNTIME-GAP-NATIVE-VORTEX-OPERATOR-COVERAGE-1`,
      `GLOBAL-STRING-DOMAIN-EXECUTION-1`, `GLOBAL-HIGH-CARDINALITY-AGGREGATION-1`,
      `GLOBAL-EXACT-DISTINCT-GROUPED-1`, and `CLICKBENCH-100M-TAIL-LANE-OPTIMIZATION-20`.
    - Corrected audit disposition: previous rows remain useful shipped helpers/evidence, but
      production completion requires one registry with admitted/executed/blocked semantics.
  - V1 scope classification: `required_for_v1` for local prepared/native kernels; LLVM/Cranelift or
    generated machine code is `deferred_out_of_v1` unless separately approved by RFC and dependency
    review.
  - ShardLoom technique review: use encoded execution and metadata-first admission before kernel
    selection; capillary scheduling should feed kernels segment/morsel batches; dynamic work shaping
    should choose between dictionary, dense, sparse, direct-primitive, and row-ref kernels; evidence
    tiers must distinguish registry admission, execution, decoded-reference checks, and performance
    retention.
  - Execution checklist:
    - [x] Define a kernel registry schema with operator family, input dtype, encoding/layout
      preconditions, null semantics, determinism, memory envelope, materialization level, Vortex
      provider surface, fallback prohibition, and unsupported diagnostic.
    - [ ] Register first production kernels for string heavy-hitter top-K, transformed
      dictionary URL/domain grouping, numeric-pair aggregate, numeric+UTF8 grouped top-K, dense
      exact distinct, row-ref top-K, exact predicate count, and direct primitive aggregate.
    - [x] Replace route-local policy labels with registry selection where semantics match, while
      keeping public route names stable.
    - [ ] Add decoded-reference parity tests for every admitted kernel, including empty, all-null,
      mixed-null, high-cardinality, low-cardinality, dictionary, direct primitive, and unsupported
      layout cases.
    - [x] Add explain/evidence fields for kernel candidate set, selected kernel, admission reason,
      execution level, decoded/materialized rows, and blocked alternatives.
    - [ ] Add retain/drop gates so kernel specialization that fails correctness, degrades target UAT,
      or only changes evidence cannot be marked complete.
  - Next outcome: ShardLoom can make defensible kernel-selection decisions from one native registry
    without hand-waving route labels as implementation.
  - User-visible surface: explain/capabilities, public workflow evidence, query runtime, benchmark
    artifacts, and diagnostics.
  - Implementation scope: kernel registry types, local primitive planner/executor integration,
    existing specialized aggregate/predicate/top-K helpers, CLI evidence lifting, tests, and docs.
  - Evidence required: per-kernel correctness tests, unsupported diagnostics, targeted slow-lane UAT,
    full UAT if retained, no-fallback evidence, and materialization/decode counters.
  - Acceptance: every admitted specialized kernel has explicit preconditions and decoded-reference
    proof; every blocked shape has a stable diagnostic; public evidence proves real kernel execution
    rather than label-only dispatch.
  - Verification: focused kernel registry tests, local primitive execution tests, CLI evidence tests,
    `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no DataFusion/Spark/DuckDB/Polars/Velox execution; no code copied from ClickHouse,
    DuckDB, HyPer, Umbra, CedarDB, Firebolt, Doris, Arc, or another system; no JIT/compiler
    dependency until an RFC approves it.
  - Claim boundary: local native-kernel execution evidence only, not broad SQL/operator parity or
    official benchmark superiority.
  - Fallback boundary: registry miss must produce deterministic unsupported or ShardLoom-native
    lower-tier execution evidence, never external fallback.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with kernel-by-kernel proof.

- [ ] `CLICKBENCH-PRODUCTION-COLUMNAR-RESULT-DATAPLANE-1` keep result and retained-row data columnar
      until the declared sink boundary.
  - Source: public ClickHouse lazy materialization documentation, DuckDB vector format documentation,
    Arrow Flight SQL/result-plane design signal, ShardLoom late-materialization guardrails, and the
    2026-09-02 audit finding that prior columnar data-plane work was mostly evidence without a
    complete result/sink contract.
  - Current state: ShardLoom has row-ref top-K paths, selected-row materialization evidence, compact
    result fields, and JSON/CSV/Vortex sink evidence. Several public routes still materialize row
    values earlier than necessary or report columnar posture without a shared internal
    `ColumnarResultBatch` contract through top-K, sort, aggregate, and sink handoff.
  - Intake review:
    - Accepted from the five technique review: columnar result/data-plane discipline.
    - Merged with prior rows: `GLOBAL-ROWREF-TOPK-PRUNING-1`,
      `GLOBAL-RUNTIME-GAP-CARRY-FORWARD-1`, `RUNTIME-GAP-OUTPUT-SINK-FANOUT-1`,
      `CLICKBENCH-100M-PHYSICAL-POLICY-PLANNER-6`, and
      `CLICKBENCH-DOMAIN-TRANSFER-1` columnar result evidence.
    - Corrected audit disposition: prior rows remain valid route-specific improvements, but the
      production result data-plane must be implemented as a shared sink-facing contract.
  - V1 scope classification: `required_for_v1` for local CLI/Python/DataFrame/SQL result correctness
    and performance posture; remote result delivery remains `unsupported_boundary`.
  - ShardLoom technique review: combine row-ref top-K, selection vectors, metadata-first column
    pruning, capillary batch execution, and evidence-tier materialization certificates; dynamic
    work shaping should choose retained-row batch sizes; PulseWeave should report materialization
    pressure at sink boundaries only.
  - Execution checklist:
    - [x] Define a shared `ColumnarResultBatch`/`RetainedRowSet` contract that carries selected
      columns, row refs/source ordinals, validity/null semantics, order/tie metadata, and sink
      materialization requirements.
    - [ ] Route filter/project/limit, sort/top-K, grouped aggregate, distinct, and row export
      through columnar result batches until JSON/CSV/user-row rendering requires row materialization.
    - [x] Add sink adapters that can consume columnar batches for Vortex/Arrow-compatible local
      outputs and explicitly materialize only at JSONL/CSV/CLI text boundaries.
    - [x] Add materialization certificates with rows considered, rows retained, rows materialized,
      columns decoded, payload bytes decoded, sink boundary, and no-fallback status.
    - [ ] Add tests for order stability, null rendering, selected-column projection, wide payload
      top-K, grouped output, distinct output, JSON/CSV parity, and unsupported remote delivery.
    - [ ] Run targeted UAT on Q23/Q24/Q25 and broad top-K/grouped rows, then full UAT for retained
      changes.
  - Next outcome: ShardLoom result flow is columnar and sink-aware internally, with row materializing
    only at the declared user-visible boundary.
  - User-visible surface: CLI JSON/text output, Python collection, DataFrame-style collection,
    local sinks, benchmark timing evidence, and materialization diagnostics.
  - Implementation scope: result model types, local primitive executors, sink/export helpers, CLI
    renderer/evidence lifting, Python wrapper expectations, tests, benchmark artifacts, and docs.
  - Evidence required: row/column materialization counters, sink-boundary certificates,
    decoded-reference parity, targeted query UAT, full UAT if retained, no-fallback evidence, and
    artifact/output replay checks.
  - Acceptance: wide payload/top-K routes avoid full wide-row materialization; JSON/CSV/Python
    outputs remain identical; Vortex/Arrow-compatible sinks consume columnar batches where admitted;
    evidence names the exact materialization boundary.
  - Verification: focused result/sink tests, public route evidence tests, Python parity tests where
    wrappers are touched, `cargo fmt --all -- --check`,
    `cargo clippy --workspace --all-targets -- -D warnings`,
    `cargo test --workspace --all-targets`, targeted query UAT, and full 43-query UAT for retained
    changes.
  - Non-goals: no remote Flight service, no hidden pandas/Polars materialization, no broad Arrow
    execution substrate claim, no query-answer sidecar.
  - Claim boundary: local result-path implementation evidence only, not broad public API,
    cloud/result-service, or official benchmark claim.
  - Fallback boundary: every result path must keep `fallback_attempted=false` and
    `external_engine_invoked=false`; unsupported sinks fail explicitly.
  - Ledger rule: completed detail moves to
    `docs/architecture/phased-execution-completed-ledger.md` with materialization counters and
    sink-parity evidence.

Current autonomous execution order:

1. Do not start implementation from the rejected 2026-09-02 writer-tuning diff. If implementation is
   resumed, first retain/drop or remove that experimental patch using the `360s` UAT evidence.
2. Start with `CLICKBENCH-PRODUCTION-WRITER-PHYSICAL-DESIGN-1` unless the maintainer explicitly
   chooses another item; the writer/ingest path is the currently measured load long pole.
3. Then proceed through segment metadata, morsel scheduling, specialized kernels, and columnar
   result data-plane work in that order unless fresh evidence changes the dependency order.
4. Keep all runtime work attached to shared ShardLoom/Vortex-native execution surfaces, with
   external engines restricted to baseline/oracle evidence only.

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

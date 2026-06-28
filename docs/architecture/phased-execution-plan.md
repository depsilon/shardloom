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
2. Work `RUNTIME-GAP-NATIVE-VORTEX-OPERATOR-COVERAGE-1` first because source/sink/operator
   coverage is the shared dependency for SQL, Python, DataFrame, CLI, and ClickBench lanes.
3. Work `RUNTIME-GAP-FRONT-DOOR-SHARED-RUNTIME-PARITY-1` after native coverage changes so public
   surface aliases converge onto the same Vortex-normalized runtime families.
4. Work `RUNTIME-GAP-OUTPUT-SINK-FANOUT-1` after shared runtime parity so writes/fanout use the
   same source/prepared/output evidence boundary.
5. Work `RUNTIME-GAP-MEMORY-SPILL-FAULT-TOLERANCE-1` after the operator and sink boundaries are
   stable enough to attach memory, spill, cancellation, and cleanup evidence.

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

- [ ] `RUNTIME-GAP-NATIVE-VORTEX-OPERATOR-COVERAGE-1` Burn down native Vortex
  source/sink/operator and encoded execution coverage gaps through shared runtime families.
  - Source: `target/runtime-gap-family-burn-down-current.json` rows in the
    `native_vortex_operator_runtime` family, including global review lines 95, 116, 162, 183, and
    956.
  - Current state: the latest local 100M UAT proves strong shared native Vortex paths for many
    ClickBench lanes, but the global review still records non-universal source/sink/operator,
    segment-extraction, predicate, DType, nested/null, and compressed encoded execution coverage.
    The next work should convert concrete blockers into native runtime evidence or deterministic
    admission denial, not add route labels.
  - V1 scope classification: `required_for_v1` for locally feasible source/sink/operator coverage;
    external object-store or production platform proof remains outside this item.
  - ShardLoom technique review: use metadata-first planning, embedded `.vortex` statistics/layout
    metadata, dictionary/encoded kernels, selection vectors, row-ref materialization, capillary
    operator units, PulseWeave state budgets, and evidence-tier controls where they reduce real
    work. Check upstream Vortex providers before adding ShardLoom-specific primitives, and never use
    Vortex query-engine integrations as fallback.
  - Execution checklist:
    - [x] Generate and inspect the current native operator/source/sink blocker matrix from existing
      capability reports, Vortex runtime reports, and UAT evidence.
    - [x] Classify blockers into `already_runtime`, `feasible_now`, `external_gated`, or
      `unsafe_semantics`, with exact files and diagnostics for each row.
      Evidence: `native_unsupported_coverage` now keeps object-store, table/catalog, streaming,
      unstructured media, external effects, live/distributed/remote, best-default-claim, and broad
      operator/SQL/DataFrame claims explicitly unsupported, while local Vortex artifact writes and
      Vortex-derived compatibility exports are no longer duplicated as unsupported sink rows.
    - [x] Reclassify stale local Vortex Source/Split admission evidence from
      fixture-smoke/generalized-blocked to local runtime admitted, while keeping object-store,
      table/catalog, remote split serialization, and remote Native I/O proof external-gated.
    - [x] Align Vortex runtime-utilization audit rows so local scan/source/split usage is
      `partial_runtime_evidence` rather than `planned_runtime_provider`, while dynamic predicate
      ordering and remote/object-store evidence remain gated.
    - [x] Promote the selected local Vortex I/O reader lane from fixture-smoke posture to
      feature-gated runtime evidence with no generalized-reader blocker, while keeping broad
      schema/remote reader claims gated.
    - [x] Promote local non-null Vortex sparse patch/fill reader chunks into reader-generated
      ShardLoom run-length encoded kernel inputs with no fallback, update the segment-extraction
      admission report to `scoped_feature_gated_runtime`, and keep nullable/generalized sparse
      layout extraction gated.
    - [x] Promote Vortex dictionary reader chunks with nullable codes/values and run-end chunks
      with nullable values into the shared encoded kernel-input path, preserving encoded nulls in
      `EncodedValueBatch` instead of blocking the reader chunks.
    - [x] Add focused runtime evidence that FSST string-contains lanes use Vortex's native
      `LikeKernel` through shared local primitive helpers without row materialization or fallback;
      keep broad dictionary/FSST provider-disposition rows gated until grouped/top-K parity and
      benchmark evidence prove wider admission.
    - [x] Promote Vortex 0.75 mask `AllTrue`/`AllFalse`/`true_count` semantics into shared
      masked dictionary/FSST/direct UTF-8 contains fast paths so empty/full filter masks avoid
      per-row string work, while keeping broader nullable aggregate/runtime claims gated until
      lane-specific null-semantics and benchmark evidence exist.
    - [x] Evaluate Vortex 0.75 `byte_length()` for length/string-transform lanes and record a
      current-runtime drop decision rather than a provider gate for current routes: ShardLoom's
      existing dictionary-derived length, transformed-dictionary general measure, and compact
      code-pair partial paths compute once per dictionary value and avoid row string materialization
      for the relevant SQL/Python/DataFrame aggregate shapes. Revisit only if a non-dictionary
      UTF-8 lane has UAT evidence showing Vortex `byte_length()` removes materialization without
      slowing the dictionary-heavy lanes.
    - [x] Evaluate Vortex 0.75 grouped `count`/primitive `sum` kernels and keep the current
      ShardLoom capillary/hash grouped-aggregate state selected for flat-column SQL/Python/
      DataFrame routes: upstream grouped kernels operate on already-grouped list/fixed-size-list
      values, so wrapping them before ShardLoom builds group state would add a list-construction
      phase for the current ClickBench-style lanes. Record the row as a current-runtime drop
      decision and revisit only for routes that already carry pre-grouped Vortex list arrays or can
      prove grouped-list construction is cheaper than the existing packed-key/dictionary-code/
      capillary state.
    - [x] Promote Vortex 0.75 layout-reader/child-cache provider disposition from candidate-only to
      shared runtime evidence recorded: shared local Vortex open paths use
      `with_layout_reader_cache()`, embedded layout reports emit `layout_reader_cache_status`, and
      focused local primitive tests prove no decode/materialization/fallback for metadata-pruned
      routes. Keep broad layout-cache performance claims gated until UAT/benchmark evidence proves
      a concrete lane improvement.
    - [x] Implement the highest-value `feasible_now` native Vortex provider/operator/sink coverage
      in shared `shardloom-vortex`/CLI runtime helpers, not benchmark-only paths.
      Evidence: the shared Native I/O report now marks `local_vortex_artifact_sink` as
      `runtime_supported` and `compatibility_export_sink` as
      `local_compatibility_export_admitted`, with local-output/translation certificate refs and no
      unsupported diagnostic, matching the existing public write/fanout runtime contract.
    - [x] Add focused Rust tests proving no fallback, native Vortex input/output, correct decoded
      references where applicable, and deterministic blockers for any remaining unsupported shape.
      Evidence: `shardloom-core` Native I/O report tests, `native_io_envelope_plan_snapshots`,
      `compute_capability_matrix_snapshots`, `typed_envelope_compatibility_lock`, and
      `typed_envelope_contract_snapshots` cover the changed row counts/status fields.
    - [x] Update capability/status/release evidence so supported rows are not reported as
      diagnostic-only, and real external-gated rows keep stable blocker IDs.
      Evidence: `compute-capability-matrix` now reports 21 unsupported rows with sink count 3,
      removes the duplicate compatibility-export sink blocker, and labels remaining scalar and
      SQL/DataFrame rows as broad unsupported claims rather than the admitted local primitive/front
      door subset.
    - [x] Run focused native Vortex validators and any targeted UAT needed to prove the changed
      operator family is faster or at least not slower.
      Evidence: focused Rust report/snapshot tests, `python3
      scripts/check_compatibility_output_translation_reports.py`, `python3
      scripts/check_v1_local_output_sink_scope.py`, and `python3
      scripts/check_runtime_gap_family_burn_down.py` pass locally. `python3
      scripts/check_v1_local_resource_safety.py --skip-build --binary
      target/release-user-surfaces-validation/debug/shardloom` also passes after building the
      feature-gated CLI with rustup stable in an isolated target directory.
    - [ ] Move the completed summary to the ledger after merge.
  - Next outcome: one cohesive PR that closes at least one concrete native Vortex operator/source or
    sink coverage family with runtime evidence and updates the blocker matrix.
  - User-visible surface: Vortex-native CLI, SQL/Python/DataFrame routes after normalization,
    capability reports, benchmark/UAT evidence.
  - Implementation scope: `shardloom-vortex/src`, `shardloom-cli/src`, capability/report scripts,
    focused Rust tests, and release/status docs generated from those reports.
  - Evidence required: Native I/O certificates, operator blocker matrix, encoded/residual/
    materialized mode evidence, decoded-reference checks, and no-fallback evidence.
  - Acceptance: locally feasible native Vortex blockers are either implemented with shared runtime
    evidence or reclassified with concrete external/unsafe reasons; no successful route reports
    external engine invocation or hidden fallback.
  - Verification: `cargo test -p shardloom-vortex ...`, `cargo test -p shardloom-cli vortex_...`,
    `python3 scripts/check_runtime_gap_family_burn_down.py`,
    `python3 scripts/check_user_route_capability_report.py`, plus targeted UAT only for touched
    query/operator lanes.
  - Non-goals: no broad official ClickBench claim, no production object-store claim, no
    DataFusion/DuckDB/Polars/pandas/Spark fallback, no query-answer sidecars.
  - Claim boundary: scoped native Vortex runtime evidence only.
  - Fallback boundary: successful and blocked routes must preserve `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: move completed detail to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [ ] `RUNTIME-GAP-FRONT-DOOR-SHARED-RUNTIME-PARITY-1` Close SQL/Python/DataFrame/CLI
  front-door breadth gaps by converging aliases onto shared Vortex-normalized runtime families.
  - Source: `target/runtime-gap-family-burn-down-current.json` rows in the
    `language_front_door_runtime` family, including global review lines 66, 1278, 1342, and 1480.
  - Current state: `scripts/check_sql_python_dataframe_parity.py` and route-capability reports pass
    for the current scoped rows, and the Python user-surface gate passes once CI-style release
    dry-run evidence exists. Remaining global review rows still describe broad language/runtime
    breadth rather than a completed public semantic matrix.
  - V1 scope classification: `required_for_v1` for feasible local SQL/Python/DataFrame/CLI
    semantics that lower into existing shared runtime families; arbitrary UDFs, notebooks, and
    platform-dependent surfaces stay out of this item.
  - ShardLoom technique review: front doors should be syntax/ergonomics wrappers. SQL text,
    Python lazy calls, DataFrame-style method chains, and CLI commands must converge after source
    admission on the same Vortex-prepared plan, physical policy, sink, materialization boundary,
    and evidence vocabulary.
  - Execution checklist:
    - [x] Inspect parity and Python user-surface reports for any public alias, method, SQL shape, or
      CLI spelling that still maps to a facade-only path.
    - [x] Promote feasible aliases into shared runtime lowering or remove/rename obsolete facade
      rows that imply a separate execution path.
    - [x] Add or update parity tests for equivalent SQL, Python, DataFrame-style, and CLI shapes
      over the same prepared Vortex source.
    - [x] Ensure deterministic unsupported diagnostics remain only for unsafe/external semantics,
      with stable blocker IDs and concrete next actions.
    - [x] Update README/docs/user-surface references if public examples or method availability
      changes.
    - [x] Re-run parity, Python user-surface, and route-capability validators.
    - [ ] Move the completed summary to the ledger after merge.
  - Next outcome: public surface choice is demonstrably preference-level syntax for the next
    admitted runtime family, not an alternate execution stack.
  - User-visible surface: README examples, Python API, SQL CLI, DataFrame-style helpers, route and
    capability reports.
  - Implementation scope: `python/src/shardloom/query.py`, `python/src/shardloom/context.py`,
    `python/src/shardloom/session.py`, `shardloom-cli/src/sql_local_source_runtime.rs`, parity
    validators, and focused tests.
  - Evidence required: positive parity fixtures, decoded-reference expectations, deterministic
    blockers, no-fallback evidence, and release dry-run transcript when Python package gates run.
  - Acceptance: all admitted equivalent front-door shapes lower to the same runtime family and
    report matching fallback/external-engine status.
  - Verification: `python3 scripts/check_sql_python_dataframe_parity.py`,
    `python3 scripts/check_python_user_surface_completion.py`,
    `python3 scripts/check_user_route_capability_report.py`, and focused Python/Rust tests for the
    touched method or SQL family.
  - Non-goals: no broad pandas/Polars compatibility claim, no broad ANSI SQL claim, no notebook or
    arbitrary callable/UDF execution.
  - Claim boundary: scoped public front-door runtime parity only.
  - Fallback boundary: front-door wrappers must not invoke pandas, Polars, DuckDB, Spark,
    DataFusion, Velox, or another external execution engine.
  - Ledger rule: move completed detail to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [ ] `RUNTIME-GAP-OUTPUT-SINK-FANOUT-1` Promote production output, fanout, and user-facing write
  runtime through shared Vortex-normalized sink contracts.
  - Source: `target/runtime-gap-family-burn-down-current.json` rows in the `output_sink_runtime`
    and `io_reuse_fanout_followthrough` families, including global review lines 205 and 1876.
  - Current state: local Vortex output, JSONL/CSV-style compatibility sinks, and selected fanout
    evidence exist, but broad production output sink APIs, object-store output, replay/fidelity
    proof, and cross-format fanout follow-through are still claim-gated.
  - V1 scope classification: `required_for_v1` for local/native/compatibility sinks feasible in
    this repo; production object-store credentials and platform writes remain external-gated unless
    a local isolated provider fixture proves them.
  - ShardLoom technique review: use capillary sink units, metadata-first output planning,
    PulseWeave sink pressure, route timing surface separation, and explicit metadata-loss evidence.
    Compatibility export is translation, not fallback execution.
  - Execution checklist:
    - [x] Inventory current `write`, `write_jsonl`, `write_csv`, native Vortex output, and fanout
      routes across SQL/Python/DataFrame/CLI surfaces.
    - [x] Collapse equivalent sinks onto shared output planning and sink artifact helpers.
      Evidence: local write/fanout method rows, public workflow route rows, and the
      compatibility-output writer matrix now distinguish admitted Vortex-derived local
      compatibility exports from blocked table/object-store sink work instead of preserving stale
      fixture-smoke wording.
    - [x] Add replay/fidelity and metadata preservation/loss evidence for each admitted sink
      family.
    - [x] Add focused tests for native Vortex output, compatibility output, fanout partial-failure
      handling, and deterministic blockers for external/platform writes.
    - [x] Update capability/release docs so local sinks are not underclaimed and external sinks are
      not overclaimed.
    - [x] Run output/fanout validators and focused sink tests.
    - [ ] Move the completed summary to the ledger after merge.
  - Next outcome: local/user-facing writes and fanout share one output contract with explicit
    artifact, replay, metadata, and no-fallback evidence.
  - User-visible surface: Python `write*` helpers, CLI output commands, native Vortex output,
    compatibility exports, fanout reports.
  - Implementation scope: `shardloom-cli/src/sql_local_source_runtime.rs`, `shardloom-vortex/src`,
    `python/src/shardloom/session.py`, output/fanout reports, docs/release status files, and tests.
  - Evidence required: OutputPlan, sink artifact proof, replay/fidelity checks, metadata-loss
    report, and no-fallback evidence.
  - Acceptance: admitted local sinks are runtime-backed, external/platform writes fail closed with
    deterministic diagnostics, and fanout does not hide partial writes or fallback execution.
  - Verification: focused output/fanout Rust and Python tests,
    `python3 scripts/check_user_route_capability_report.py`, and release readiness validators
    touched by sink status.
  - Non-goals: no production cloud/object-store write claim without isolated provider evidence, no
    compatibility export as execution fallback.
  - Claim boundary: scoped local sink/fanout runtime evidence only.
  - Fallback boundary: output translation must keep `fallback_attempted=false` and
    `external_engine_invoked=false`.
  - Ledger rule: move completed detail to
    `docs/architecture/phased-execution-completed-ledger.md`.

- [ ] `RUNTIME-GAP-MEMORY-SPILL-FAULT-TOLERANCE-1` Attach real bounded memory, spill, adaptive,
  retry, cancellation, commit, and cleanup evidence to shared runtime operators.
  - Source: `target/runtime-gap-family-burn-down-current.json` rows in the
    `spill_fault_tolerance_runtime` family, including global review lines 376, 416, and 432.
  - Current state: memory/resource evidence and local safety reports exist, but broad runtime
    spill/OOM enforcement, adaptive execution, runtime filters, skew handling, compaction writes,
    retry, cancellation, and commit execution are still incomplete.
  - V1 scope classification: `required_for_v1` for locally feasible bounded-memory and cleanup
    guarantees on shared operators; distributed shuffle/cluster runtime remains external-gated.
  - ShardLoom technique review: use PulseWeave resource envelopes, ScarcityLedger-style pressure
    evidence, capillary work-unit retry/idempotency state, ProofBound-safe adaptation, spill-backed
    native state where needed, and deterministic pre-OOM blockers.
  - Execution checklist:
    - [x] Inventory current resource-envelope, memory, spill, cancellation, retry, and cleanup
      evidence across native Vortex operators and sinks.
      Evidence: `shardloom-exec` memory/recovery/PulseWeave models, public native Vortex
      primitive resource-envelope/state-budget fields, pre-OOM fixture, retry/cancellation gates,
      and local output/prepared-state cleanup reports are now referenced by the v1 local
      resource-safety gate.
    - [x] Choose one shared high-risk operator or sink family and add real memory reservation,
      pressure, pre-OOM, cancellation, and cleanup evidence.
      Evidence: the v1 local resource-safety gate now executes a public native Vortex aggregate
      route through `run cli` and validates its shared resource envelope, memory-admission
      decision, reservation-release proof, scalar aggregate state-budget, spill fail-closed policy,
      native I/O certificate, and no-fallback evidence alongside the deterministic pre-OOM, retry,
      cancellation, and cleanup reports.
    - [x] Implement spill-backed exact state only where a local workload proves in-memory state is
      unsafe or materially slower than bounded spill.
      Evidence: no admitted local aggregate/sink route currently proves in-memory state unsafe or
      slower than bounded spill; existing `shardloom-exec` spill payload primitives remain
      synthetic/planning support, so the runtime contract stays bounded-memory admission plus
      deterministic pre-OOM/spill-required fail-closed diagnostics rather than adding slower or
      unproven spill I/O to the hot path.
    - [x] Add tests for deterministic pre-OOM denial, cleanup after cancellation/error, and no
      external fallback.
      Evidence: existing Rust pre-OOM/retry/cancellation gate tests plus
      `scripts/check_v1_local_resource_safety.py` now validate the admitted public native Vortex
      aggregate resource route and fail closed if fallback/external-engine, spill I/O, missing
      memory-admission, or missing reservation-release evidence appears.
    - [x] Update status/release docs so synthetic-only safety rows are not mistaken for production
      runtime proof.
      Evidence: `docs/architecture/v1-local-resource-safety.md`,
      `docs/release/hard-release-readiness-gate.md`, `scripts/check_v1_local_resource_safety.py`,
      and `scripts/check_release_readiness.py` now require the admitted public native Vortex
      aggregate resource route in addition to planning-only gates.
    - [x] Run focused memory/fault-tolerance tests and targeted UAT for the changed route if it
      affects query timing.
      Evidence: `python3 scripts/check_v1_local_output_sink_scope.py` passes, and the focused Rust
      pre-OOM/resource route tests pass. `python3
      scripts/check_v1_local_resource_safety.py --skip-build --binary
      target/release-user-surfaces-validation/debug/shardloom` passes against a feature-built CLI
      produced with `CARGO_TARGET_DIR=target/release-user-surfaces-validation CARGO_INCREMENTAL=0
      CARGO_BUILD_JOBS=1
      ~/.cargo/bin/cargo +stable build -q -p shardloom-cli --features
      "release-user-surfaces vortex-traditional-analytics-benchmark"`.
    - [ ] Move the completed summary to the ledger after merge.
  - Next outcome: the first shared operator/sink family has runtime memory/fault-tolerance proof
    instead of report-only posture.
  - User-visible surface: memory/spill diagnostics, runtime reports, benchmark safety rows, CLI and
    Python evidence envelopes.
  - Implementation scope: `shardloom-cli/src/cg14_memory_runtime_hardening.rs`,
    `shardloom-cli/src/fault_tolerance_promotion_gate.rs`, `shardloom-exec/src`, native Vortex
    operator/sink helpers, and tests.
  - Evidence required: memory reservation proof, pre-OOM deterministic blocker, spill cleanup
    proof, cancellation/retry state evidence, and no-fallback evidence.
  - Acceptance: at least one shared runtime family has concrete bounded-memory/fault-tolerance
    execution evidence, and remaining families are classified with exact blockers or promoted
    checklist rows.
  - Verification: focused memory/fault-tolerance Rust tests, `cargo test -p shardloom-exec` where
    touched, and `python3 scripts/check_runtime_gap_family_burn_down.py`.
  - Non-goals: no distributed runtime, no external shuffle/storage fallback, no broad production
    HA claim.
  - Claim boundary: scoped local memory/fault-tolerance runtime evidence only.
  - Fallback boundary: memory pressure, retry, and commit failures must fail deterministically
    before fallback, external delegation, or process OOM where the route is admitted.
  - Ledger rule: move completed detail to
    `docs/architecture/phased-execution-completed-ledger.md`.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

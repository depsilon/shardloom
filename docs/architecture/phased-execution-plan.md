# ShardLoom Phased Execution Plan

## How to maintain this file
- Keep actionable working items in Planned.
- Keep Completed as a pointer to `docs/architecture/phased-execution-completed-ledger.md`; do not
  place detailed completed session blocks in this file.
- Keep Planned in logical implementation order even when CG or phase numbers are out of order.
- Do not keep a separate Active section; the next autonomous work should be the next unchecked
  Planned checklist item.
- Move completed session blocks to the top of
  `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion; do not
  reshuffle older completed history unless the content is incorrect.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

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
  detailed completed session and historical phase ledgers.
- Supporting architecture docs may contain rationale, inventories, traceability, and historical
  ledgers, but they must not introduce a second "current" queue.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- If a supporting doc records completed history, keep it clearly labeled as a completed ledger or
  historical note, and do not let it become a current queue.

Supporting docs:
- `README.md`
  - Role: project entry point, stable orientation, and compact core-concepts doorway.
  - Status rule: points to this phase plan and the completed ledger for current planned/completed
    state; must not duplicate working checklists or become the full glossary.
- `docs/architecture/phased-execution-completed-ledger.md`
  - Role: detailed completed session ledger and historical phase provenance split out of this phase
    plan.
  - Status rule: may record completed work only; it must not introduce planned work or a second
    current queue.
- `docs/architecture/rfc-phase-traceability.md`
  - Role: maps phases and CG work to governing RFCs.
  - Status rule: may record traceability history, but this file owns current work state.
- `docs/architecture/capability-certification-sequencing.md`
  - Role: CG-20 sequencing ledger and implementation-order reference.
  - Status rule: phase-plan checklist owns planned CG-20 work items.
- `docs/architecture/vortex-public-api-inventory.md`
  - Role: Vortex public API evidence and adapter-boundary inventory.
  - Status rule: API findings inform CG-1/CG-2/CG-3 queue items here.
- `docs/architecture/vortex-runtime-utilization-audit.md`
  - Role: Vortex-first runtime utilization audit for arrays, execution layers, Scan
    Source/Sink/Split, layouts, I/O, sessions/registries, device posture, extension types, and
    benchmark discipline.
  - Status rule: report/code surfaces here do not authorize runtime provider promotion; actionable
    provider or benchmark work must remain represented in this phase plan.
- `docs/architecture/vortex-adapter-integration-plan.md`
  - Role: Vortex adapter rationale, boundaries, and historical integration notes.
  - Status rule: adapter work is actionable only after represented in this phase plan.
- `docs/architecture/repo-cleanup-backlog.md`, `diagnostics-normalization-backlog.md`,
  `terminology-consolidation-backlog.md`, and `feature-footprint-doctor-plan.md`
  - Role: cleanup inventories and completed cleanup ledgers.
  - Status rule: future cleanup must be promoted into this file as a concrete checklist item.
- `docs/architecture/canonical-terminology.md`
  - Role: authoritative glossary and concept index for ShardLoom vocabulary.
  - Status rule: defines terms and links to governing RFCs, but does not mark current phase or CG
    completion.
- `docs/architecture/systems-learning-map.md`
  - Role: technique-transfer map from external systems and design references into ShardLoom-native
    contracts.
  - Status rule: records lessons and guardrails only; it does not authorize dependencies, runtime
    behavior, or CG completion.
- `docs/architecture/benchmark-suite-catalog.md`
  - Role: CG-6.25 benchmark-suite catalog and Priority 2.7 source-backed correctness/benchmark
    matrix orientation.
  - Status rule: records matrix/catalog report surfaces, the executable local taxonomy runner
    status, and claim blockers; full comparative benchmark reruns and performance claims remain
    separate planned/release-readiness actions.
- `docs/architecture/crate-posture-public-exports.md`
  - Role: Priority 2.8 crate posture and public export grouping reference.
  - Status rule: documents current executable/report-only/blocked/future/prohibited-fallback export
    posture only; it does not authorize runtime or dependency expansion.
- `docs/architecture/workspace-feature-build-matrix.md`
  - Role: Priority 3.5 workspace feature/build validation matrix reference.
  - Status rule: records required validation rows and release blockers; it does not authorize
    package publication, dependency expansion, runtime expansion, or fallback execution.
- `docs/architecture/universal-import-deployment-baseline-harness.md`
  - Role: Priority 3.5 / CG-18 universal import, deployment, and baseline harness maturity
    reference.
  - Status rule: records required local/CI/container/optional Foundry/optional benchmark harness
    rows and comparison-only baseline environment boundaries; it does not authorize harness
    execution, package publication, external engine invocation, or fallback execution.
- `docs/architecture/rfc-coverage-followthrough.md`
  - Role: Priority 3.6 RFC coverage follow-through reference for RFC 0010, RFC 0011, RFC 0020,
    RFC 0022, and RFC 0023 before broader user/runtime expansion.
  - Status rule: records report-only coverage gates for developer/agent usability, modular
    extensibility, table/catalog compatibility, plan interop, and extension sandboxing; it does not
    authorize parser expansion, dependency expansion, imported-plan execution, extension execution,
    external effects, external engine invocation, or fallback execution.
- `docs/architecture/typed-command-result-envelope.md`
  - Role: Priority 3.9 typed command/result envelope reference for the `shardloom.output.v2`
    protocol slice and remaining command-family migration work.
  - Status rule: records the typed envelope slots and temporary legacy field mirror; it does not
    authorize runtime expansion, command effects, external engine invocation, REST/server behavior,
    or fallback execution.
- `docs/architecture/incumbent-gap-opportunity-map.md`, `lakehouse-value-prop-compatibility.md`,
  `universal-input-contract.md`, and `spill-reservation-lifecycle-integration.md`
  - Role: reference maps and constraints.
  - Status rule: they guide design decisions but do not mark CG completion.
- `docs/architecture/operational-evidence-policy-hardening.md`
  - Role: shared evidence, policy, workload, lifecycle, protocol-parity, benchmark-constitution, and
    artifact-safety contracts for CG-20 through CG-23.
  - Status rule: contract reference only; actionable implementation work must be represented in the
    Planned queue.
- `docs/architecture/vortex-upstream-alignment-hardening.md`
  - Role: Vortex compatibility, Scan API, compute-provider, residual-boundary, device,
    extension-type, object-store telemetry, integration-boundary, and benchmark-interoperability
    contract reference.
  - Status rule: contract reference only; it does not authorize new Vortex APIs, dependencies,
    runtime behavior, claims, or fallback execution.
- `docs/skills/vortex/vortex-first-provider-check.md`
  - Role: Vortex-adjacent implementation guard requiring agents to check upstream Vortex concepts
    and classify decisions before inventing new ShardLoom abstractions.
  - Status rule: process guard only; it does not authorize new Vortex API use, dependency changes,
    runtime behavior, support claims, external engine invocation, or fallback execution.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

### Top Findings Intake From Subagent Audits

These items came from the May 13, 2026 subagent audits and sit ahead of the normal P7/P8/P9 queue
until each finding is fixed, proven already fixed, or folded into a named child slice below. Review
threads must be re-read with `gh` before implementation because later merged work may already have
made some comments stale even when GitHub still shows them unresolved.

Thread-state rule: code fixes and doc reconciliation do not automatically close GitHub review
threads. Before release readiness, re-query PR #360 and up, classify each still-unresolved thread
as fixed, stale, intentionally deferred, or still actionable, and resolve/comment only with evidence
from the merged code and tests.

- [x] Audit-F1 schema-breaking REST/OpenAPI status-field repair bundle.
  - Source findings: unresolved P1 review comments on PR #520 and PR #521; related REST API schema
    review should also cover PR #517, PR #518, and PR #519.
  - Required fixes:
    - Rename event-stream-specific response state out of inherited `OutputEnvelope.status`; use
      `event_stream_status` in `EventStreamResponse` so OpenAPI `allOf` does not intersect command
      envelope statuses with event-stream enum values.
    - Move security/governance-specific state out of inherited `OutputEnvelope.status`; use
      `governance_status` in `SecurityGovernanceResponse`.
    - Audit the other REST response schemas for the same unsatisfiable `allOf` pattern, especially
      local lifecycle, plan preview, and data-plane response schemas, and keep command-level
      `status` reserved for `success|warning|error|unsupported`.
    - Add checked-in OpenAPI/schema assertions or snapshots that fail on future domain-status
      collisions.
  - Verification: OpenAPI contract checks, REST API protocol snapshots, Python typed REST helpers,
    and typed-envelope compatibility locks.
  - Completed: OpenAPI `LocalLifecycleResponse`, `EventStreamResponse`,
    `SecurityGovernanceResponse`, and `DataPlaneResponse` now use their existing domain status
    fields instead of redefining inherited `OutputEnvelope.status`; the checked-in OpenAPI contract
    test asserts this invariant.
- [x] Audit-F2 unresolved P1 Codex review-thread correctness and certification repair bundle.
  - Source findings: GitHub reports unresolved, non-outdated review threads on merged PRs #360 and
    up. The following P1 threads must be verified against current code and fixed if still valid:
    - PR #362 `agent_contract.rs`: remove effectful benchmark commands from the agent-safe/
      side-effect-free surface.
    - PR #375 `local_primitives.rs`: validate fixture identity before certifying primitive
      evidence.
    - PR #395 `encoded_predicate_evaluation.rs`: reject segment/value encoding mismatches before
      emitting filter evidence.
    - PR #397 `encoded_projection_execution.rs`: require per-segment projection coverage before
      preserving filter evidence.
    - PR #407 `generalized_encoded_filter_execution.rs` and
      `generalized_encoded_projection_execution.rs`: tie prepared filter/projection certification to
      real fixture provenance, not coarse counts.
    - PR #412 `correctness.rs`: keep property/fuzz gate blocked until execution evidence exists.
    - PR #418 `correctness.rs`: do not permanently block deferred-artifact completion when no
      deferred families remain.
    - PR #428 `release.rs`: separate provenance attestation from signature evidence.
    - PR #431 `memory.rs`: enforce spill-required operator classes in certified declarations.
    - PR #432 `observability.rs`: validate required observability areas before marking coverage
      complete.
    - PR #433 `security.rs`: reject security reports missing mandatory evidence areas.
    - PR #434 `recovery.rs`: decouple fault-tolerance certification evidence from side-effect
      checks.
    - PR #449 `top_level_facade.rs`: preserve reader-backed provider evidence during split
      conversion.
    - PR #454 `release.rs`: keep release claims blocked until all required build-matrix rows pass.
    - PR #455 `universal_harness.rs`: update/reconcile CG-18 status snapshots so tests pass.
    - PR #515 `engine_modes.rs`: restrict live fixture selection to output modes it can emit.
    - PR #516 `hybrid_engine.rs`: reject invalid hybrid predicates instead of returning empty
      success.
  - Verification: targeted tests for each corrected invariant, then broad workspace fmt/clippy/test
    when touched surfaces cross crates or affect certification claims.
  - Completed: current main already had the PR #362, PR #375, PR #395, PR #397, and PR #455
    concerns fixed or stale. This bundle now fixes the remaining P1s by tying generalized encoded
    filter/projection certificates to real fixture provenance, preserving reader-backed provider
    evidence during split conversion, keeping property/fuzz correctness blocked until execution
    evidence exists, allowing deferred fixture-family artifact closure when no deferred families
    remain, separating release provenance attestations from signatures, keeping release claims
    blocked until all required build-matrix rows pass, enforcing spill-required operator
    certification, requiring mandatory observability/security evidence areas, decoupling
    fault-tolerance certification evidence from side-effect checks, restricting live fixture
    output modes to emitted modes, and rejecting unsupported/malformed hybrid predicates without
    fallback.
- [ ] Audit-F3 unresolved P2 Codex review-thread triage and repair bundle.
  - Source findings: unresolved P2 review threads remain on PRs #362, #366, #376, #380, #381,
    #384, #385, #386, #391, #393, #396, #420, #424, #426, #428, #433, #436, #437, #438, #439,
    #445, #446, #448, #449, #451, #452, #457, #459, #461, #473, #482, #506, #507, #515, #517,
    #518, #519, and #520.
  - Required core/report gate fixes:
    - PR #362 `agent_contract.rs`: verify the agent-safe capability surface points at
      `capabilities certification`; current code may already make this thread stale.
    - PR #366 `benchmark.rs`: block `external_comparison_results` when external rows exist but
      required metrics are missing, not only when entire external result rows are absent.
    - PR #428 `release.rs`: aggregate artifact status across all matching artifacts instead of
      order-dependent `.find(...)`.
    - PR #433 `security.rs`: enforce `redaction_required` and `audit_required` in validation.
    - PR #436 `cpu_specialization.rs`: require an unblocked dispatch class before opening
      specialization admission, and keep side-effect checks independent from admission-policy
      flags.
    - PR #437 `release.rs`: make `release_gate_blocking_count()` count every condition that
      `release_gated()` treats as blocking.
    - PR #438 `correctness.rs`: require external-oracle execution when oracle evidence is missing
      even before artifacts exist, and require deferred fixture-family artifact population when
      deferred families exist even if artifact count is zero.
    - PR #439 `benchmark.rs`: decouple benchmark closeout eligibility from unrelated claim-gate
      state, or list the extra blockers explicitly in the closeout blocker set.
    - PR #445 `approx_sketch.rs`: make runtime-promotion blocking include
      `materialization_without_report_allowed`.
    - PR #452 `benchmark_suite.rs`: verify canonical benchmark taxonomy members, not only vector
      lengths.
  - Required CLI/Python/protocol fixes:
    - PR #381 `main.rs`: when local primitive execution has no certificate, emit the full
      uncertified execution-certificate field set, including fixture ID and side-effect/
      no-fallback fields.
    - PR #384 `main.rs`: enforce requested `memory_gb` in local `CountWhere` execution.
    - PR #385 `python/src/shardloom/client.py`: resolve relative `SHARDLOOM_BIN` paths against
      the client `cwd`, not the Python process cwd.
    - PR #386 `main.rs`: let `vortex-filter` report `rows_selected=0` when metadata proves the
      predicate is false even without execution.
    - PR #446 `main.rs`: include `existing_report_refs` in CG-14 CLI gate output.
    - PR #457 `python/src/shardloom/models.py`: require typed `shardloom.output.v2` slots such as
      `result`, `policy`, certificates, artifacts, lifecycle, and capability snapshot instead of
      defaulting missing keys to empty values.
    - PR #459 `typed_envelope.rs`: restrict execution-certificate reference matching to actual
      certificate keys and avoid matching `*_input_ref`, `*_output_ref`, or fixture IDs.
    - PR #461 `command_family.rs`: classify runtime planning commands such as
      `vortex-memory-plan`, `vortex-adaptive-sizing`, and `vortex-schedule-plan` as runtime
      planning instead of prepared source-backed execution.
    - PR #482 `vortex_primitive_execution.rs`: preserve `emit_error` exit code `2` when forwarding
      parse failures.
    - PR #506 `python/src/shardloom/context.py`: treat `capabilities(scopes=[])` as an explicit
      empty selection, distinct from default scopes.
    - PR #507 `python/src/shardloom/query.py`: honor declared source format for non-Vortex lazy
      sources instead of inferring solely from URI suffix.
  - Required Vortex execution/evidence fixes:
    - PR #376 `traditional_analytics.rs`: preserve nonzero sink chunk size for streaming Vortex
      runs instead of passing `materialization_boundary_rows=0` as sink max chunk size.
    - PR #380 `local_primitives.rs`: allow primitive projection passthrough for primitive dtypes
      without requiring `projection_pushdown_applied`.
    - PR #391 `generalized_filter_execution.rs`: compute `fallback_attempted` from diagnostics in
      unsupported reports instead of hardcoding `false`.
    - PR #393 `local_primitives.rs`: resolve relative request source paths from execution
      context/workspace consistently with fixture matching.
    - PR #396 `encoded_predicate_evaluation.rs`: avoid cloning encoded batches in the one-batch
      bridge and preserve the intended zero-copy behavior.
    - PR #420 `source_backed_encoded_execution.rs`: short-circuit invalid source envelopes before
      filter execution and before projection execution.
    - PR #426 `source_backed_encoded_execution.rs`: report certificate presence independently from
      certification result for execution and Native I/O certificate fields.
    - PR #449 `top_level_facade.rs`: append filter Native I/O transitions for filter-project
      results.
    - PR #451 `runtime_utilization.rs`: include `execute_step_evidence.external_engine_invoked`
      and `fallback_attempted` in aggregate no-fallback checks, and block
      `VortexLayoutAdvisorReport::claim_blocked()` when fallback was attempted.
    - PR #452 `source_backed_benchmark_matrix.rs`: require executable/non-blocked status for
      required lane operations.
  - Required benchmark/docs/API fixes:
    - PR #424 `phased-execution-completed-ledger.md`: remove or close unchecked checklist items
      from the completed-only ledger so it cannot become a second planned queue.
    - PR #448 `docs/rfcs/0040-benchmark-suite-platform-learning-hardening.md`: restate Dask/Trino
      baseline dependency targets as non-implementation design references.
    - PR #473 `benchmarks/traditional_analytics/run.py`: check CLI exit code before treating JSON
      `status` as unsupported, including the duplicated `shardloom_vortex_runner` path.
    - PR #515 `live_engine.rs`: compute late-record counts with a non-impossible predicate under
      `reject_past_watermark`.
    - PR #517 `rest_api_planning.rs`: keep discovery-mode envelope `schema_version` and
      `report_id` canonical to discovery, not the nested contract report.
    - PR #518 `remote_api.rs`: mark unsupported logical stages as unplanned instead of setting
      `native_logical_planned=true` for every non-invalid input.
    - PR #519 `remote_api.rs`: use scenario-specific lifecycle plan handles and avoid emitting
      certified no-fallback artifacts for blocked/non-certified scenarios.
    - PR #520 `remote_api.rs`: reconcile hybrid fixture `event_count` with the detailed counters.
  - Implementation batching: land these as sizeable area slices, not one PR per comment. Start with
    core/report-gate plus CLI/Python/protocol consistency, then Vortex execution/evidence, then
    benchmark/docs/API cleanup.
  - Progress: Batch A fixes are implemented for core/report gates, Python/CLI protocol contracts,
    benchmark harness exit-code handling, CG-14 report output, completed-ledger hygiene, and RFC
    0040 benchmark dependency wording. The current batch addresses PR #366, #424, #428, #433,
    #436, #437, #438, #439, #445, #446, #448, #452, #457, #459, #461, #473, #482, #506, and
    #507, with PR #385 verified stale against the existing client `cwd` resolution behavior.
    Leave Audit-F3 open for the Vortex execution/evidence and API cleanup groups.
  - Verification: each batch needs targeted regression tests for the reviewed behavior, focused CLI
    and Python protocol tests where contracts change, broad fmt/clippy/workspace validation for
    shared surfaces, and a final PR #360+ review-thread state audit before release readiness.
- [x] Audit-F4 RFC 0035 API maturity and REST/server drift reconciliation bundle.
  - Source findings: RFC 0035 defines `API-A9` as production-certified API for a declared workload,
    while implementation now uses `API-A9` for columnar data-plane/standards boundary and `API-A10`
    for production-certified workload API.
  - Required fixes:
    - Reconcile RFC 0035, `RestApiMaturityStage`, and traceability docs so API-A9/API-A10 semantics
      are not contradictory.
    - Decide whether to implement a no-dataset local loopback discovery server slice for
      `GET /v1/health`, `/v1/version`, `/v1/capabilities`, and `/v1/adapters`, or explicitly keep
      the current contract-only/no-listener stance reflected in RFC/architecture traceability.
    - Keep any server slice side-effect-free: no dataset probes, object-store/catalog access,
      execution, Flight/ADBC startup, broker I/O, external engine invocation, or fallback.
  - Verification: maturity ladder snapshots, OpenAPI route/schema checks, contract-only vs
    server-backed behavior tests, and Python/API parity tests.
  - Completed: RFC 0035 now records `API-A9` as the columnar data-plane/standards boundary and
    `API-A10` as production-certified workload API; traceability now marks RFC 0035 partially
    implemented through the Priority 6 contract/report-only lanes and explicitly keeps API-A2 as
    no-listener discovery until a local loopback server slice is implemented.
- [x] Audit-F5 RFC/architecture traceability drift reconciliation bundle.
  - Source findings:
    - `docs/architecture/rfc-phase-traceability.md` still says CG-21/CG-22/CG-23 implementation
      queues live in Planned even though large portions of Priority 4, Priority 5, and Priority 6
      are completed.
    - Foundry media, virtual media-set, and AIP boundary report-only surfaces already exist in core
      unstructured workflow code, while P9 still lists related Foundry surfaces as future work.
    - RFC 0033 DataFrame/ETL UX expectations such as `profile`, `collect`, `to_pandas`, `to_arrow`,
      `write_vortex`, `write_parquet`, SQL, joins, aggregations, windows, schema contracts, and
      data-quality APIs remain incomplete or intentionally unsupported/report-only.
    - Full Foundry package/platform integration remains intentionally deferred to Priority 9.
  - Required fixes:
    - Update traceability to distinguish completed report-only surfaces, remaining unsupported
      runtime work, and intentionally deferred integration work.
    - Add a CG-21 workflow API completeness/report-only unsupported-diagnostic slice for the
      missing DataFrame/ETL user methods before any runtime expansion claim.
    - Inventory existing pre-P9 Foundry report-only code and either connect it to P9 traceability or
      mark it explicitly as pre-P9 boundary posture.
  - Verification: docs consistency checks, Python/CLI unsupported-diagnostic parity tests, and
    no-fallback/no-runtime-effect assertions.
  - Completed: RFC traceability now distinguishes completed CG-21/CG-22/CG-23 report-only lanes
    from blocked runtime/certification claims, RFC 0033 and RFC 0036 are marked partially
    implemented where only report-only surfaces exist, and the missing CG-21 DataFrame/ETL UX
    method parity work is promoted into P7.0 before broader cross-CG closeout.

### Near-term Implementation Priority

Completed checked-off work that used to live in this section is recorded in
`docs/architecture/phased-execution-completed-ledger.md`. Keep this section focused on remaining
actionable work.

Execution slice rule for autonomy: parent priority checkboxes stay unchecked until every child
bundle under that priority is complete. Work proceeds from the first unchecked child bundle, but PRs
should be large enough to ship a usable command/API/report surface with schema, tests, smoke
commands, and docs. Current large-slice order is Audit-F1-F5, P7.0-P7.3, P8.1-P8.3, then
P9.1-P9.5.

- [x] Priority 3.9 - CLI contract closeout and ownership cleanup
  - Outcome: finish the typed command/result envelope and CLI modularity work only to the point that
    every user-facing command emits the same `shardloom.output.v2` contract, no-fallback status,
    side-effect policy, diagnostics, and typed refs/payloads through shared helpers.
  - Slice rule: do not open one-checkbox PRs for helper moves. Batch related command-family cleanup
    into reviewable ownership slices that leave the CLI more reliable or easier to test.
  - Runtime rule: Priority 3.9 may reorganize handlers, fixtures, field builders, and typed payload
    routing; it must not add dataset probing, external-engine execution, materialization, writes,
    network effects, package publishing, server startup, or fallback execution.
  - [x] P3.9A workflow/table CLI ownership closeout.
    - User-visible surface: `schema-plan`, `table-compat-plan`, `table-intelligence-plan`,
      `layout-health-plan`, `compaction-plan`, `plan-import`, `plan-export`, `incremental-plan`,
      and `stateful-reuse-plan` keep stable JSON/text output.
    - Acceptance: workflow/table field builders, fixtures, typed payload hooks, and tests live with
      `workflow_planning.rs`; `main.rs` keeps only routing and truly shared helpers.
    - Verification: focused workflow/table CLI snapshot tests, the
      `cargo test -p shardloom-cli --bin shardloom` command, full workspace fmt/clippy/test, and
      `git diff --check`.
  - [x] P3.9B runtime/optimizer/operational CLI ownership closeout.
    - User-visible surface: `streaming-plan`, `streaming-batch-plan`, `backpressure-plan`,
      `dynamic-work-shaping-plan`, `optimizer-*`, `kernel-registry`, `memory-*`, `retry-*`,
      `cancellation-*`, `commit-*`, and operational gate commands keep stable output.
    - Acceptance: runtime, optimizer, memory/spill, retry/cancel/commit, and operational hardening
      helpers are owned by their command-family modules with no duplicated manual JSON policy.
    - Verification: focused engine/runtime, optimizer, operational, and typed-envelope tests plus
      full workspace fmt/clippy/test.
  - [x] P3.9C Vortex primitive and readiness CLI ownership closeout.
    - User-visible surface: existing Vortex planning/readiness commands and feature-gated local
      primitive execution commands stay runnable with the same explicit opt-in flags.
    - Acceptance: Vortex primitive execution, Vortex planning/readiness, prepared/source-backed
      execution, and Vortex runtime-readiness helpers are module-owned; certificate, Native I/O,
      encoded-read spike, count/project/filter, metadata-kernel, and readiness field groups no
      longer live in `main.rs`.
    - Slice boundary: this is the next single Vortex-facing ownership PR; do not split it into
      per-command helper moves unless a real regression forces a narrower repair. Output, write,
      and commit UX helpers remain grouped with P4.6 instead of being mixed into this readiness
      closeout.
    - Verification: feature-gated local primitive tests, Vortex readiness/planning snapshots,
      typed-envelope snapshots, and full workspace validation.
  - [x] P3.9D typed envelope compatibility lock.
    - User-visible surface: every CLI command, including error paths, returns consistent
      `shardloom.output.v2` JSON when `--format json` is requested.
    - Acceptance: API protocol output declares the compatibility lock; command-family lifecycle
      taxonomy matches the current handler modules; representative JSON matrix coverage exists for
      success, invalid input, unknown command, unsupported, blocked, evidence-incomplete, optional
      Foundry, and certified local execution paths; missing-binary Python parity remains locked.
    - Slice boundary: treat this as the final contract-lock PR before prioritizing user-testable
      P4 workflow execution; only add Foundry fixtures here if the matching report surface exists.
    - Verification: `typed_envelope_contract_snapshots`, Python client protocol tests, CLI API
      protocol snapshots, and full workspace validation.
- [x] Priority 4 - CG-21 user-testable workflow and ETL execution lane
  - Outcome: turn the existing CLI JSON protocol and thin Python wrapper into workflows a user can
    install locally, import, inspect, plan, explain, execute where already certified, and diagnose
    when blocked.
  - Slice rule: each slice must expose a concrete CLI/Python surface, tests, and at least one
    documented smoke command. Prefer slices that combine several related checkboxes into one usable
    workflow over single-field or single-helper changes.
  - Execution rule: runtime work may only use already-approved ShardLoom-native paths. Unsupported
    reads, writes, SQL, DataFrame actions, object-store access, catalogs, external services, and
    external engines must produce deterministic unsupported diagnostics, not fallback execution.
  - [x] P4.1 local smoke and runtime discovery bundle.
    - User-visible surface: `import shardloom`, `ShardLoomClient.from_env()`,
      `ShardLoomClient.from_repo()`, `client.smoke_check()`, `shardloom status --format json`,
      `shardloom capabilities python --format json`, and
      `shardloom capabilities deployment --format json`.
    - Acceptance: smoke output reports Python package version, resolved CLI path, CLI version,
      protocol version, platform, feature gates, package/deployment maturity, and
      `fallback_attempted=false`; import and constructors remain side-effect-free.
    - Verification: Python unit tests, fresh local venv smoke, CLI status/capability snapshots,
      missing-binary/version-mismatch diagnostics, and full workspace validation.
  - [x] P4.2 side-effect-free context and capability API.
    - User-visible surface: `import shardloom as sl; ctx = sl.context(); ctx.capabilities()` plus
      typed Python accessors for adapters, functions, operators, SQL support, deployment,
      certification, materialization boundaries, and unsupported reasons.
    - Acceptance: capability objects distinguish `certified`, `partial`, `planned`, `unsupported`,
      `feature_gated`, `effect_gated`, and `materialization_gated`; all unsupported responses carry
      stable diagnostics, required gates, rewrite suggestions, and no-fallback fields.
    - Verification: Python model/accessor tests, CLI/Python parity snapshots, no-probe smoke tests,
      and protocol compatibility tests.
  - [x] P4.3 lazy workflow/query-builder planning MVP.
    - User-visible surface: `sl.read_vortex`, `sl.read_csv`, `sl.read_json`, `sl.read_parquet`,
      `.filter(...)`, `.select(...)`, `.limit(...)`, `.explain()`, `.estimate()`, `.certify()`, and
      `.unsupported_report()` as lazy plan objects.
    - Acceptance: builders lower to ShardLoom logical-plan or report-only CLI surfaces without
      executing data reads by default; unsupported formats/operators fail deterministically with
      materialization and no-fallback evidence.
    - Verification: Python builder unit tests, golden CLI JSON for explain/estimate/certify paths,
      unsupported-diagnostic parity tests, and no external-engine dependency checks.
  - [x] P4.4 first executable local Vortex workflow.
    - User-visible surface: a documented local `.vortex` fixture workflow that can run count,
      count-where, filter, project, and filter-project only through existing explicit local
      primitive flags and the Python client wrappers.
    - Runnable smoke: add or update a repository-local Python smoke path that a user can run against
      checked-in `.vortex` fixtures and that prints command, status, certificates, work metrics, and
      no-fallback fields.
    - Acceptance: execution emits execution certificates, Native I/O certificates, source/pushdown/
      sink/adapter-fidelity evidence, materialization state, rows/segments work metrics, and
      `fallback_attempted=false`; arbitrary non-fixture targets stay usable only at the current
      uncertified maturity level.
    - Verification: feature-gated local primitive tests, Python wrapper smoke over repository
      fixtures, typed-envelope certified execution snapshots, and full workspace validation.
  - [x] P4.5 local compatibility-source planning and explicit materialization smoke.
    - User-visible surface: CSV, JSON/NDJSON, Parquet, and Arrow IPC planning/smoke helpers that
      can describe schema expectations, decode/materialization boundaries, adapter maturity, and
      Vortex conversion/write prerequisites before any execution claim.
    - Runnable smoke: one documented Python/CLI path must plan at least CSV, JSON/NDJSON, and
      Parquet inputs, showing why each is report-only, compatibility-source, or blocked.
    - Acceptance: compatibility inputs are never described as Vortex-native execution; every output
      includes representation state, fidelity/metadata-loss risk, Native I/O certificate
      requirements, and deterministic blockers for unsupported reads or writes.
    - Verification: adapter registry snapshots, Python live-ETL smoke tests, compatibility-boundary
      CLI tests, and no-fallback dependency invariant tests.
  - [x] P4.6 workflow readiness, output/remote blockers, and evidence UX bundle.
    - User-visible surface: `write_vortex` readiness, compatibility export planning, output target
      preview, temporary-path policy, overwrite/append blockers, commit/recovery readiness,
      table/catalog/object-store/remote-data blockers, migration/correctness/benchmark evidence
      readiness, and certificate/blocker refs exposed consistently through CLI and Python.
    - Runnable smoke: one no-write/default Python smoke must preview output and commit readiness,
      compatibility export, table/catalog/object-store/HTTP/S3/GCS/Azure planning, and
      migration/correctness/benchmark evidence status without reading, writing, probing, or
      materializing data. Actual local artifact writes remain separate explicit commands.
    - Acceptance: safe write, remote-data, and evidence planning are usable before execution; any
      actual local write path must stay policy-gated, idempotency-aware, rollback-aware, and
      certificate-linked. Object-store, catalog, warehouse, and remote-service IO remain blocked
      until lower-level gates prove them.
    - Verification: Vortex output/commit/staged artifact tests, table/catalog/object-store planning
      tests, Python workflow-readiness tests and smoke script, write/remote/evidence blocker
      diagnostics, and full workspace validation.
  - [x] P4.7 end-to-end quickstart and proof bundle.
    - User-visible surface: `python/README.md`, a local quickstart example, and repository smoke
      scripts show the exact install/import/smoke/capability/source-plan/output-readiness/
      execute-supported/diagnose-blocked flow a user can run on the same checkout.
    - Acceptance: docs and scripts separate what is certified, partial, planned, report-only,
      evidence-incomplete, or unsupported; no public package, superiority, SQL/DataFrame
      completeness, object-store production, or Foundry claims are made without evidence.
    - Verification: README command smoke where practical, Python examples, CLI snapshots, and full
      workspace validation.
- [x] Priority 5 - CG-22 three-engine certified data execution fabric
  - [x] CG-22A/B/H engine contract, per-engine matrix, and Python/API UX bundle.
    - Implemented `EngineMode` values `batch`, `live`, `hybrid`, and `auto`, plus
      `Boundedness`, `UpdateMode`, and `OutputMode` vocabulary.
    - Added `EngineSelectionReport` and `engine-selection-plan` so users and agents can see
      requested, allowed, rejected, and selected engine modes with deterministic rejection reasons.
    - Added `EngineCapabilityMatrixReport`, `engine-capability-matrix`, and `capabilities engines`
      so batch/live/hybrid operator, function, source, sink, update, changelog, continuous-view,
      state, checkpoint, global-sort, unbounded-join, and production-claim posture is explicit.
    - Surfaced `engine="auto"|"batch"|"live"|"hybrid"` through Python context/query helpers without
      running commands during construction; explicit engine reports preserve
      `external_engine_invoked=false`, `fallback_attempted=false`, no data reads, no writes, and no
      runtime execution.
  - [x] CG-22C/D/I live source/change, in-memory prototype, and state/freshness certification
        bundle.
    - Added ShardLoom-native `ChangeRecord` with key, operation, sequence, event time, processing
      time, source offset, schema digest, payload reference, metric, and value fields.
    - Added append/upsert/delete/retract/tombstone semantics, fixture event-time watermarking,
      reject-past-watermark late-data policy, retain-until-delete/tombstone state TTL,
      in-memory deterministic checkpoint policy, output changelog vocabulary, and fixture-backed
      streams for filter, project, count, count_where, and simple group_count.
    - Added `live-change-contract-plan`, `live-fixture-run`, and Python context/client wrappers.
      `live-fixture-run` emits state, checkpoint, watermark, lag, output changelog, execution
      certificate, Native I/O certificate, `FreshnessCertificate`, `StateCertificate`,
      `ContinuousViewCertificate`, and no-fallback evidence while keeping broker/object-store
      integrations deferred.
  - [x] CG-22E/F/G hybrid overlay, Vortex micro-segment flush, and layout-health bundle.
    - Added a deterministic `hybrid-overlay-run` fixture that combines declared local Vortex base
      rows with fixture-backed hot deltas, tombstones, deletion-vector entries, snapshot epoch, and
      certified merged results for filter, project, count, count_where, and group_count.
    - Emitted `DeltaOverlayCertificate`, `HotColdContributionReport`, snapshot refs, base snapshot
      id, merged snapshot id, hot changelog range, warm/cold segment counts, tombstone counts,
      freshness lag, micro-segment flush evidence, representation/statistics/deletion/checkpoint/
      commit fields, execution certificate, Native I/O certificate, and no-fallback evidence.
    - Added layout-health bundle evidence for small-segment pressure, tombstone pressure, partition
      skew, stale statistics, and compaction planning without executing maintenance, writes,
      checkpoint writes, commit writes, object-store I/O, local Vortex reads, or fallback execution.
    - Moved hybrid engine selection and capability matrix posture to fixture-level partial support
      while keeping production claims blocked on durable flush writes, object-store commit protocol,
      external catalog discovery, workload correctness evidence, and benchmark evidence.
- [x] Priority 6 - CG-23 REST, event, and remote API surface
  - Outcome: make remote access a contract-first control plane over the already-certified local
    surfaces, with deterministic unsupported diagnostics and no weakening of CLI/Python protocol
    parity.
  - Slice rule: each PR should ship a usable API lane with schema, CLI/Python parity evidence,
    compatibility tests, and documented smoke commands. Do not split by endpoint or enum unless the
    split fixes a concrete regression.
  - Runtime rule: no server may probe datasets, access object stores/catalogs, delegate execution to
    external engines, weaken materialization reporting, or hide fallback status. Discovery and
    plan/validate surfaces stay side-effect-free until an explicit execution lane is certified.
  - [x] P6.1 REST contract, discovery mode, and API maturity bundle.
    - Added `docs/api/shardloom-openapi-v1.yaml` as the checked-in OpenAPI 3.2 `/v1` contract with
      health, version, capabilities, adapters, sources, sinks, plans, queries, results,
      certificates, profiles, benchmarks, migration, lineage, governance, problem-details errors,
      and execution request policy schemas.
    - Added `RestApiContractReport`, `RestApiDiscoveryModeReport`, `rest-api-contract-plan`, and
      contract-only `serve --mode discovery` output. These surfaces report API maturity stages,
      discovery endpoints, represented resources, result policy modes, and
      `fallback_attempted=false` without starting a listener, probing datasets, touching object
      stores/catalogs, resolving credentials, executing queries, writing data, or invoking external
      engines.
    - Exposed Python `ShardLoomClient.rest_api_contract_plan()`,
      `ShardLoomClient.serve_discovery_contract()`, and matching context helpers so users can test
      the CG-23 contract lane from Python or CLI.
  - [x] P6.2 plan/explain/validate/certification-preview API bundle.
    - Added a side-effect-free `RestApiPlanPreviewReport` and `rest-api-plan-preview` CLI command
      for certified-local-batch, partial-hybrid-fixture, blocked-remote-object-store,
      invalid-input, and unsupported-operator scenarios.
    - Extended the checked-in OpenAPI contract with plan handles plus validate, explain, estimate,
      unsupported-report, and certification-preview A3 endpoints and `PlanPreviewResponse` /
      `PlanPreviewStage` schemas.
    - Exposed Python `ShardLoomClient.rest_api_plan_preview()` and
      `ShardLoomContext.rest_api_plan_preview()` typed views with per-stage status,
      problem-details, no-server, no-listener, no-runtime, no-delegation, and no-fallback accessors.
    - Verified that parser, binder, native logical, native physical, execution readiness, evidence
      readiness, and certification stages remain separately inspectable; blocked, invalid-input,
      and unsupported previews emit deterministic diagnostics/problem-details fields without
      execution delegation.
  - [x] P6.3 certified local async lifecycle, result delivery, and certificate bundle.
    - Added `RestApiLocalLifecycleReport` and `rest-api-local-lifecycle` for certified-local-batch,
      cancel-requested, retry-requested, and blocked-uncertified scenarios.
    - Extended the OpenAPI contract with query lifecycle status/cancel/retry/profile/lineage
      endpoints, result pages/JSON Lines/artifact endpoints, artifact lookup, local lifecycle
      schemas, result policy schemas, TTL, retention, cleanup, and evidence refs.
    - Exposed Python `RestApiLocalLifecycle`, `ShardLoomClient.rest_api_local_lifecycle()`, and
      `ShardLoomContext.rest_api_local_lifecycle()` typed views.
    - Verified certified local lifecycle output links result handles to execution certificates,
      Native I/O certificates, materialization reports, profile reports, lineage artifacts, and
      no-fallback evidence; non-certified lifecycle requests remain blocked before runtime; cancel
      and retry fixtures emit deterministic diagnostics; Arrow IPC is classified as a decoded
      columnar boundary while Vortex artifact/object-reference modes stay preferred for
      high-fidelity results.
  - [x] P6.4 live/hybrid event API and streaming evidence bundle.
    - Added `RestApiEventStreamReport`, `rest-api-event-stream`, OpenAPI event endpoints, and a
      checked-in AsyncAPI event contract for SSE-first event streaming with optional WebSocket
      posture.
    - Exposed CloudEvents-style event contracts for progress, state, checkpoint, watermark,
      certificates, lineage, benchmarks, and hybrid hot/cold contribution events with evidence refs
      and certificate refs.
    - Added certified live/hybrid fixture scenarios plus blocked-production-workload and
      broker-requested scenarios so users can inspect certified fixture evidence and see deterministic
      blockers for production/broker paths.
    - Exposed Python `RestApiEventStream`, `ShardLoomClient.rest_api_event_stream()`, and
      `ShardLoomContext.rest_api_event_stream()` typed views.
    - Verified event delivery stays report/contract-only in this lane: no server start, network
      listener, broker I/O, object-store I/O, dataset/catalog probing, credential resolution,
      runtime execution, write I/O, external-engine invocation, delegation, or fallback execution.
  - [x] P6.5 security, governance, observability, and agent API bundle.
    - Added `RestApiSecurityGovernanceReport` and `rest-api-security-governance` for
      safe-local-default, destructive-policy-required, and agent-mcp-discovery scenarios.
    - Exposed local-only, token, mTLS, OIDC, and service-account auth posture; read, plan, execute,
      write, cancel, admin, benchmark, migration, and agent scopes; redaction and audit policy;
      safe MCP resources/tools; and a unified OpenTelemetry/OpenLineage/problem-details/CloudEvents/
      certificate-ref evidence model.
    - Extended the OpenAPI contract with security, governance, observability evidence-model, and MCP
      discovery endpoints plus `SecurityGovernanceResponse` schemas.
    - Exposed Python `RestApiSecurityGovernance`,
      `ShardLoomClient.rest_api_security_governance()`, and
      `ShardLoomContext.rest_api_security_governance()`.
    - Verified credentials remain references, raw secrets are not emitted, destructive operations are
      blocked without explicit policy, MCP tools remain dry-run/explain/estimate/certify by default,
      and no server/listener/probe/credential resolution/audit write/runtime/fallback effects occur.
  - [x] P6.6 columnar data-plane and ecosystem standards boundary bundle.
    - Added `RestApiDataPlaneReport` and `rest-api-data-plane` for artifact-reference-default,
      flight-ticket-requested, adbc-endpoint-requested, and standards-matrix scenarios.
    - Exposed REST result-transfer and large-payload policy for inline JSON, paged JSON, JSON Lines,
      native Vortex artifacts, object references, Arrow IPC decoded-columnar boundaries, and optional
      future Flight/ADBC posture.
    - Classified Iceberg REST Catalog, Polaris, Gravitino, Delta Sharing, Substrait,
      WASI/WebAssembly components, NATS JetStream, Redpanda, Kafka-compatible systems, Paimon, and
      Fluss as interop/reference boundaries without catalog, broker, object-store, external compute,
      or fallback effects.
    - Extended the OpenAPI contract with data-plane, optional Flight/ADBC posture, and standards
      matrix endpoints plus `DataPlaneResponse` schemas.
    - Exposed Python `RestApiDataPlane`, `ShardLoomClient.rest_api_data_plane()`, and
      `ShardLoomContext.rest_api_data_plane()`.
    - Verified REST remains the control plane and proof surface, Flight/ADBC is optional and not
      required for local use/import, every transfer declares materialization/fidelity/result policy,
      decoded-columnar boundaries are explicit, and no server/listener/transport/catalog/broker/
      object-store/runtime/fallback effects occur.
- [ ] Priority 7 - CG-21/CG-22/CG-23 integrated certification closeout
  - Outcome: prove that workflow UX, engine-mode evidence, and remote/API posture agree across CLI,
    Python, and API contracts before any broader support claim is made.
  - Slice rule: group closeout work by proof surface, not by source file. A slice must improve a
    user's ability to understand what can be run, what is blocked, and what evidence is missing.
  - [ ] P7.0 CG-21 workflow API completeness and unsupported-diagnostic parity bundle.
    - User-visible surface: CLI and Python workflow methods for the missing RFC 0033 DataFrame/ETL
      affordances (`profile`, `collect`, `to_pandas`, `to_arrow`, `write_vortex`, `write_parquet`,
      SQL, joins, aggregations, windows, schema contracts, and data-quality checks) return
      deterministic report-only unsupported diagnostics instead of silent gaps.
    - Acceptance: every unsupported method has a stable blocker ID, severity, no-fallback stance,
      required evidence, suggested next action, and matching CLI/Python/API terminology; no data
      read, materialization, write, parser, object-store/table runtime, external engine, or fallback
      execution is introduced.
    - Verification: Python unsupported workflow tests, CLI capability/diagnostic snapshots,
      typed-envelope/API compatibility locks, and no-runtime/no-fallback smoke assertions.
  - [ ] P7.1 cross-CG capability and unsupported-diagnostic parity bundle.
    - User-visible surface: capability discovery shows CG-21 workflow, CG-22 engine mode, and CG-23
      remote API states through CLI, Python, and future REST views.
    - Acceptance: the same blocker has the same identifier, severity, no-fallback stance, required
      evidence, and suggested next action across all surfaces.
    - Verification: cross-CG capability snapshots, unsupported-diagnostic golden fixtures, Python
      parity tests, and typed-envelope/API compatibility locks.
  - [ ] P7.2 workload certification dossier bundle.
    - User-visible surface: workload-scoped certification dossiers that combine CG-5 correctness,
      CG-6 benchmarks, CG-16 execution certificates, CG-19 Native I/O certificates, CG-20 capability
      evidence, CG-21 workflow evidence, CG-22 engine evidence, and CG-23 API evidence.
    - Acceptance: dossiers distinguish certified, partial, planned, report-only, blocked, and
      unsupported posture without creating runtime effects or new dependency requirements.
    - Verification: dossier fixture snapshots, certificate-ref integrity tests, no-runtime smoke,
      and workspace fmt/clippy/test.
  - [ ] P7.3 claim gate and release-readiness closeout bundle.
    - User-visible surface: one closeout command/report that explains which local, API, package,
      benchmark, and integration claims are allowed, blocked, or explicitly out of scope.
    - Acceptance: CG-21, CG-22, and CG-23 remain logically after CG-1 through CG-20 unless pulled
      forward as report-only contract lanes; docs/report-only synthesis preserves no-runtime,
      no-dependency, no-fallback, and no-claim posture.
    - Verification: claim-gate snapshots, README/docs consistency checks, full workspace validation,
      and `git diff --check`.
- [ ] Priority 8 - general availability and external proof-of-use
  - Outcome: make a non-maintainer able to install, import, smoke, inspect capabilities, and run a
    certified local path without relying on unpublished assumptions or hidden local state.
  - Slice rule: package/release PRs must include an install or proof artifact. Documentation-only
    edits are acceptable only when they are tied to runnable smoke commands or release gate fixtures.
  - [ ] P8.1 release identity, packaging contract, and artifact integrity bundle.
    - User-visible surface: public release identity and versioning policy for PyPI `shardloom`,
      conda-forge `shardloom-cli`, `shardloom-python`, `shardloom` metapackage, GitHub Release
      artifacts, GHCR/OCI image posture, and selected crates.io protocol/client crates.
    - Acceptance: release workflow contracts cover Git tag, source archive, platform binaries,
      Python wheel/sdist, Conda recipe/feedstock status, checksums, SBOM, artifact attestation,
      changelog, compatibility matrix, known unsupported paths, and no-fallback release checks.
      Trusted publishing/OIDC is preferred; long-lived tokens, publication, release tags, feedstock
      submission, crates.io publication, OCI pushes, and Marketplace publication remain
      human-approved and release-gated.
    - Verification: package metadata checks, dry-run artifact manifests, checksum/SBOM fixtures,
      release-gate snapshots, and no-secret policy tests.
  - [ ] P8.2 clean install and first-10-minutes proof bundle.
    - User-visible surface: Conda-first clean-environment proof for `shardloom-cli`,
      `shardloom-python`, and `shardloom` metapackage, plus the public first-10-minutes path:
      `conda install shardloom`, `import shardloom`, `ShardLoomClient.from_env().smoke_check()`,
      `client.capabilities()`, `shardloom status --format json`, and
      `shardloom capabilities --format json`.
    - Acceptance: CLI binary resolution, Python import, status/capability output, and
      `fallback_attempted=false` smoke evidence are documented and reproducible from a clean
      environment.
    - Verification: clean-env smoke transcript, Python import tests, CLI resolution diagnostics,
      install docs checks, and workspace validation.
  - [ ] P8.3 external examples, docs, and baseline-comparison boundary bundle.
    - User-visible surface: `examples/local-python-smoke/`, `examples/local-vortex-benchmark/`, and
      `examples/foundry-lightweight-transform/` each include README, environment file, input
      fixture, expected output, expected certificate fields, and known limitations.
    - Acceptance: user-facing docs cover install, quickstart, Python client, CLI, Conda packages,
      Foundry usage, benchmarking, certificates, no-fallback policy, Vortex compatibility, maturity
      statuses, and unsupported diagnostics. Benchmark extras and Spark/DataFusion/DuckDB/Polars/
      pandas comparison tooling stay out of the core install path and are reported as baselines only.
    - Verification: example smoke scripts, docs link checks, expected-output snapshots, dependency
      posture checks, and full workspace validation.
- [ ] Priority 9 - RFC 0036 Foundry integration pack and platform availability
  - Outcome: make Foundry an optional integration pack with certificate-aware staging and proof
    surfaces, not a new core execution engine and not a shortcut around ShardLoom-native evidence.
  - Slice rule: group Foundry work by usable platform lane. Each slice must include package posture,
    report schemas, example or smoke evidence, and explicit no-fallback/external-compute boundaries.
  - Runtime rule: Foundry virtual tables and external compute are workflow handles, baselines,
    or migration/oracle references. ShardLoom-native execution still requires staged/native data plus
    certificates; no Snowflake/Databricks/BigQuery/Spark/Foundry compute pushdown may be reported as
    ShardLoom execution.
  - [ ] P9.1 Foundry package, execution context, and maturity ladder bundle.
    - User-visible surface: `shardloom-foundry` helper package posture, deterministic
      `SHARDLOOM_BIN` resolution, Foundry transform metadata capture, input/output RID capture,
      certificate output writing, benchmark metrics writing, staging/materialization reports, and
      no-fallback propagation without adding execution semantics.
    - Acceptance: the Foundry maturity ladder covers `F0` declared only through `F10`
      workload-certified deployment; `FoundryExecutionContext`, `FoundryDatasetTransactionReport`,
      `FoundryBranchContextReport`, `FoundryPreviewModeReport`, and
      `FoundryReleaseReadinessReport` identify transform, branch, preview/build/incremental mode,
      transactions, package versions, workload constitution, and expected evidence.
    - Verification: package-resolution smoke, maturity matrix snapshots, transform metadata fixtures,
      and no-execution/no-fallback policy tests.
  - [ ] P9.2 dataset source/sink staging, certificate output, and incremental run bundle.
    - User-visible surface: `FoundryDatasetSource`, `FoundryDatasetSink`,
      `FoundryCertificateOutput`, and `FoundryIncrementalRunReport` for staged local files,
      table-compatible outputs, certificate/metrics datasets, optional Vortex artifact sidecars,
      materialization/fidelity reports, commit/recovery status, and batch/live/hybrid evidence
      alignment.
    - Acceptance: Foundry incremental builds are aligned with ShardLoom evidence but are not treated
      as live/hybrid certification by themselves; all sources/sinks keep `fallback_attempted=false`
      and explicit materialization policy.
    - Verification: source/sink schema snapshots, certificate-output fixtures, incremental evidence
      fixtures, commit/recovery blocker tests, and package smoke.
  - [ ] P9.3 Data Health, lineage, governance, and platform boundary bundle.
    - User-visible surface: `FoundryDataHealthBridge`, Data Expectations mapping,
      `FoundryLineageFacet`, `FoundryScheduleBuildReport`, `FoundryDataConnectionBoundaryReport`,
      and `FoundryGovernanceBoundaryReport`.
    - Acceptance: reports cover certificate presence, no-fallback status, Native I/O evidence,
      schema digest, output row requirements, data-quality checks, materialization policy,
      benchmark-claim blockers, datasets, virtual tables, media sets, artifacts, schedules, syncs,
      exports, webhooks, external transforms, credential refs, egress policy, markings,
      organizations, inherited markings, certificate visibility, redaction, export policy, agent
      visibility, and artifact safety.
    - Verification: data-health fixtures, lineage/governance snapshots, redaction checks,
      credential-reference assertions, and no-egress diagnostics.
  - [ ] P9.4 virtual table, S3/Iceberg/media, and external-compute boundary bundle.
    - User-visible surface: `FoundryS3DatasetAdapter`, `FoundryVirtualTableSource`,
      `FoundryVirtualTableSink`, `FoundryVirtualTableRef`, `FoundryExternalComputeBoundaryReport`,
      `FoundryIcebergTableSource`, `FoundryIcebergTableSink`, `FoundryMediaSetSource`, and
      `FoundryMediaSetSink`.
    - Acceptance: S3-compatible dataset access records dataset RID, branch, object key, range-read
      support, multipart/write posture, bytes/request counts, credential mode, and Native I/O
      certificates. Virtual tables for Snowflake, Databricks, BigQuery, S3, ADLS, GCS, Iceberg, and
      similar systems are governed external handles with metadata, staging, update-detection,
      security, and materialization policy. External compute pushdown is classified as baseline,
      oracle, migration reference, or prohibited fallback, never ShardLoom-native execution. Media
      sources/sinks declare MIME/schema, OCR/extraction/model/materialization boundaries,
      provenance/confidence, incremental media status, redaction, and no silent OCR/transcription/
      embedding/model calls.
    - Verification: external-boundary matrix snapshots, materialization/fidelity assertions,
      credential-mode fixtures, media no-silent-model-call tests, and no-fallback policy checks.
  - [ ] P9.5 ontology/functions/model, Compute Module/BYOC, marketplace, and benchmark bundle.
    - User-visible surface: `FoundryOntologyMappingReport`, `FoundryFunctionSurface`,
      `FoundryAipLogicBridge`, `FoundryModelBoundaryReport`, `FoundryScenarioBoundaryReport`,
      `FoundryByocImageReport`, `FoundryComputeModuleSurface`,
      `FoundryComputeModuleReadinessReport`, `FoundryMarketplaceStarterProduct`, and Foundry
      benchmark schema.
    - Acceptance: Compute Modules remain blocked until CG-23 API/security/package evidence exists;
      Marketplace starter product includes Conda dependency instructions, smoke transform, benchmark
      transform, certificate output dataset, Data Expectations bridge, optional virtual-table staging
      example, optional external-compute baseline example, optional Compute Module API example,
      schedule, and docs. Benchmarks label ShardLoom lightweight, Polars lightweight,
      DataFusion/DuckDB baseline, Spark distributed, and Snowflake/Databricks/BigQuery pushdown rows
      separately with compute mode, materialization boundary, certificates, correctness digest, and
      versions.
    - Verification: marketplace fixture smoke, benchmark schema snapshots, model/function boundary
      tests, Compute Module blocker snapshots, and release-readiness policy checks.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

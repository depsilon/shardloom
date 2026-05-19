# ShardLoom Phased Execution Plan

## How to maintain this file
- Keep actionable working items in Planned.
- Keep Completed as a pointer to `docs/architecture/phased-execution-completed-ledger.md`; do not
  place detailed completed session blocks in this file.
- Keep Planned in logical implementation order even when CG or phase numbers are out of order.
- Do not keep a separate Active section; the next autonomous work should be the next unchecked
  Planned checklist item after the queue has been ordered by current dependency and user value.
  If the top item no longer matches the current implementation priority, reorder Planned first.
- Move completed session blocks to the top of
  `docs/architecture/phased-execution-completed-ledger.md` after merge or session completion; do not
  reshuffle older completed history unless the content is incorrect.
- Do not duplicate "current" status in multiple places.
- Do not use stale percentage estimates.
- CG-1 through CG-23 remain competitive gates, not replacement phase IDs.
- External engines are baselines only, never fallback execution.
- For RFC-level phase mapping details, use `docs/architecture/rfc-phase-traceability.md`.

## Planned Item Detail Standard

Every unchecked Planned item must be detailed enough for an autonomous Codex session to execute
without guessing.

A Planned item is sufficiently detailed only if it names:

- Source: governing RFC, architecture doc, benchmark report, issue, PR, or review finding.
- Current state: what exists today and what is still unsupported/report-only.
- Next slice outcome: the exact result expected from the next PR/session.
- User-visible surface: CLI, Python, benchmark, docs, API, capability view, evidence artifact, or
  release gate.
- Implementation scope: files/modules/commands expected to change.
- Evidence required: correctness, benchmark, execution-certificate, Native I/O,
  materialization/decode, policy, no-fallback, release/security evidence as applicable.
- Acceptance: observable conditions that make the item done.
- Verification: exact commands/tests/snapshots expected.
- Non-goals: what must not be implemented in this slice.
- Claim boundary: what can and cannot be claimed after completion.
- Fallback boundary: expected `fallback_attempted=false` and `external_engine_invoked=false`
  behavior.
- Ledger rule: when complete, move the detailed completed session to
  `docs/architecture/phased-execution-completed-ledger.md`.

Do not leave planned work as a bare statement such as "`<thing>` remains incomplete." Convert broad
items into one or more evidence-bearing implementation slices. Split a Planned item when it includes
`full`, `broad`, `general`, `production`, `universal`, `distributed`, `runtime`, `platform`,
`lakehouse`, `object-store`, `SQL/DataFrame`, `claim`, `release`, `Foundry`, or `REST` without an
immediate concrete scope. A split item should use child IDs such as `GAR-0032-A`; each child must be
implementable in one focused PR or explicitly marked `report-only`, `planning-only`, or
`diagnostic-only`.

A Planned item may be checked off only when implementation or deterministic unsupported diagnostics
exist, tests/snapshots/release checks exist, evidence refs are attached where claims are made,
unsupported paths remain explicit, no fallback engine was invoked, completed details are moved to the
completed ledger, and supporting docs are updated without becoming a second active queue.

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
  detailed completed session and historical phase ledgers.
- Supporting architecture docs may contain rationale, inventories, traceability, and historical
  ledgers, but they must not introduce a second "current" queue.
- If a supporting doc discovers new work, add the actionable checklist item here before
  implementation begins.
- If a supporting doc records completed history, keep it clearly labeled as a completed ledger or
  historical note, and do not let it become a current queue.
- Supporting docs must not keep unchecked implementation checklists outside this file and
  `docs/architecture/global-architecture-review.md`. Scope-boundary lists may remain, but real work
  must be carried by a `GAR-*` item below.

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
- `docs/architecture/global-architecture-review.md`
  - Role: checkbox audit of every RFC plus the compute-engine flow against the repo.
  - Status rule: every unchecked item in that review must be mirrored into this Planned queue before
    implementation; checking a review item requires checking off the corresponding phase-plan item
    or moving the completed session to the ledger.
- `docs/architecture/compute-engine-flow-reference.md`
  - Role: canonical end-to-end flow for users, CLI/Python/REST access, adapters, I/O, execution
    modes, sinks, downstream consumers, evidence, and claim gates.
  - Status rule: planned nodes in the flow do not authorize implementation or claims until the
    corresponding item exists in this Planned queue and is completed with evidence.
- `docs/architecture/compute-engine-flow-overhaul-review.md`,
  `benchmark-persistent-runner-decision.md`, and
  `performance-attribution-and-execution-structure.md`
  - Role: historical P7.5 repo-alignment review, persistent-runner decision, and benchmark timing
    attribution reference.
  - Status rule: these files record completed alignment decisions only; current compute-flow follow-up
    is represented by the GAR flow items in this Planned queue.
- `docs/architecture/runtime-evidence-level-tiering.md`
  - Role: GAR-PERF-2A reference for evidence-level runtime tiering across
    `minimal_runtime`, `certified`, and `full_replay`.
  - Status rule: the scoped `traditional-analytics-vortex-batch-run` path now emits first-class
    evidence-level fields. Future Python/API capability views and broader execution envelopes must
    remain represented by later evidence-bearing slices before broader support can be claimed.
- `docs/architecture/evidence-aware-logical-optimizer.md`
  - Role: GAR-PERF-2B reference for the completed report-only optimizer rule registry and optimizer
    trace contract.
  - Status rule: records current rule families, CLI/Python/benchmark trace fields, report-only
    before/after plan-digest placeholders, and no-fallback/claim boundaries. Future runtime
    rewrites, correctness smoke, and claim-grade optimizer promotion must remain represented by
    later evidence-bearing slices.
- `docs/architecture/vortex-scan-pushdown-completion.md`
  - Role: report-only GAR-PERF-2C reference for Vortex Scan API filter/projection/limit pushdown
    completion across prepared/native scenario families.
  - Status rule: defines pushdown evidence and deterministic blocker requirements only. Scan
    request builder work, filter expression lowering, projection mask computation, limit/slice
    pushdown, capability matrix projection, and benchmark row schema changes must remain
    represented by `GAR-PERF-2C` or later evidence-bearing slices.
- `docs/architecture/compressed-encoded-kernel-registry.md`
  - Role: GAR-PERF-2D reference for scoped compressed/encoded kernel registry evidence over
    selective-filter prepared/native rows.
  - Status rule: scoped bitpacked and sequence reader-generated encoded inputs now emit registry
    evidence with deterministic blockers for the remaining initial pairs. Broader encoded-native
    operator coverage, capability-matrix promotion, and claim-grade use must remain represented by
    later evidence-bearing slices.
- `docs/architecture/fused-operator-pipeline.md`
  - Role: GAR-PERF-2E reference for scoped fused local prepared/native operator-pipeline evidence.
  - Status rule: scoped filter/projection/limit, filter/aggregate, and top-k/projection evidence is
    complete with deterministic filter/group-by blockers. Stronger independent unfused runtime
    certificates, broader families, encoded-native promotion, and claim-grade use must remain
    represented by later evidence-bearing slices.
- `docs/architecture/in-process-session-runtime.md`
  - Role: GAR-PERF-2F reference for scoped in-process session-backed prepared/native local-artifact
    runtime evidence and future public `ShardLoomSession` boundaries.
  - Status rule: scoped CLI batch runtime evidence is complete and recorded in the ledger. Python
    client exposure, broader public session APIs, buffer-pool ownership, and claim-grade use must
    remain represented by later evidence-bearing slices.
- `docs/architecture/io-reuse-and-fanout-architecture.md`
  - Role: report-only GAR-IOREUSE-1 reference for universal source-state reuse, decoupled
    Vortex-prepared-state reuse, output-plan reuse, cross-format local fanout, cache invalidation,
    evidence-safe reuse levels, and Foundry generated-output fanout posture.
  - Status rule: defines the
    `InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact`
    architecture and benchmark field vocabulary. GAR-IOREUSE-1A through GAR-IOREUSE-1E have
    established SourceState, VortexPreparedState, OutputPlan, and report-only fanout benchmark row
    contracts plus cache/fingerprint invalidation evidence. Runtime state caches, fanout writers,
    evidence-safe reuse levels, Foundry generated-output smoke, object-store I/O, table commits,
    and claim-grade use must remain represented by `GAR-IOREUSE-1*` or later evidence-bearing
    slices.
- `docs/architecture/allocation-buffer-pool-optimization.md`
  - Role: GAR-PERF-2G reference for scoped allocation/resource-profile evidence and buffer-pool
    blocker semantics across prepared/native local runtime paths.
  - Status rule: current session-backed batch rows emit allocation/resource fields and deterministic
    blockers. Runtime buffer pools, allocator hooks, safe reuse implementation, memory-efficiency
    claims, and claim-grade use remain unclaimed until represented by later evidence-bearing
    slices.
- `docs/architecture/optimized-build-profiles-pgo-benchmark-lane.md`
  - Role: GAR-PERF-2H reference for optimized Cargo build profiles and a reproducible PGO/native
    benchmark lane.
  - Status rule: build-profile vocabulary, Cargo profiles, benchmark row fields, PGO helper flow,
    target-CPU-native boundaries, and release portability rules are complete and recorded in the
    ledger. Actual PGO profile artifacts, benchmark reruns under optimized profiles, and any
    claim-grade use must remain represented by later evidence-bearing slices.
- `docs/architecture/bayesian-performance-layout-advisor.md`
  - Role: GAR-PERF-1D reference for the report-only Bayesian performance and layout advisor
    contract.
  - Status rule: benchmark artifacts now emit `bayesian_advisor_*` fields for advisory-only
    confidence/uncertainty and future mode/reuse/sizing/layout decision surfaces. Runtime
    decisioning, automatic layout writes, fitted posterior models, and claim-grade confidence use
    must remain represented by later evidence-bearing slices before they can affect behavior or
    public claims.
- `docs/architecture/capability-certification-sequencing.md`
  - Role: CG-20 sequencing ledger and implementation-order reference.
  - Status rule: phase-plan checklist owns planned CG-20 work items. Remaining approximate/sketch
    function and certification-scope coverage details are carried by `GAR-0021` and `GAR-0032`.
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
  - Status rule: cleanup must be promoted into this file as a concrete checklist item. Remaining
    diagnostic, terminology, command-registry, traceability, and acceptance-checker details are
    carried by `GAR-0012`, `GAR-0039`, and `GAR-0043`.
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
  - Status rule: documents current executable/report-only/unsupported/planned/prohibited-fallback export
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
- `docs/architecture/evidence-native-generated-execution-observability-confidence.md`
  - Role: report-only GAR-NOVEL-1 reference for generated-source evidence, OpenLineage facets,
    OpenTelemetry trace mapping, and Bayesian confidence.
  - Status rule: describes export and confidence contracts only. Generated-output runtime,
    lineage/telemetry exporters, Bayesian release blockers, and any dependency changes must remain
    represented by `GAR-NOVEL-1`, `GAR-GEN-1`, `GAR-PERF-1D`, `GAR-0018`, `GAR-0029`, or
    release-gate slices before implementation.
- `docs/architecture/adoption-commercial-readiness-friction-reduction.md`
  - Role: report-only GAR-COMMERCIAL-1 reference for adoption friction, one-command local proof,
    package-channel readiness, buyer-facing status, enterprise evidence export, Foundry starter, and
    workflow recipes.
  - Status rule: describes adoption/commercial readiness only. Package publication, release tags,
    OCI pushes, package-channel submissions, Foundry runtime proof, export backends, recipe runtime
    expansion, and public readiness claims must remain represented by `GAR-COMMERCIAL-1`,
    `GAR-0024`, `GAR-0033`, `GAR-0036`, `GAR-NOVEL-1`, or release-gate slices before
    implementation.
- `docs/use-cases/README.md`, `docs/use-cases/use-case-index.yml`, and
  `docs/use-cases/templates/use-case-template.md`
  - Role: non-expert Use Case Atlas for answering "Can ShardLoom do my thing?", "How do I try it?",
    "What evidence do I get?", and "What is not supported yet?" without reading the phase plan,
    RFCs, or benchmark internals.
  - Status rule: may describe current supported, smoke-supported, report-only, planned, blocked, and
    unsupported use-case posture only. Runtime expansion, website status projection, generated page
    output, recipe implementation, and all-capability coverage enforcement must remain represented by
    `GAR-DOCS-1*` or later evidence-bearing slices before implementation.
- `docs/architecture/object-store-request-planner.md`
  - Role: CG-10 request-planning, range/coalescing/scheduling/checkpoint/retry/commit evidence
    reference.
  - Status rule: object-store runtime work is represented by `GAR-0008`, `GAR-0028`, `GAR-0031`,
    and the GAR-COMPAT-1C admission ladder projection in the universal compatibility scoreboard.
- `docs/architecture/table-intelligence-layer.md`
  - Role: CG-9 schema/table/catalog/CDC/layout/compaction evidence reference.
  - Status rule: table/catalog runtime work is represented by `GAR-0020`, `GAR-0028`, and the
    GAR-COMPAT-1D table-format boundary projection in the universal compatibility scoreboard.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md`
  - Role: report-only universal source/sink/adapter/user-surface compatibility map covering local
    files, Vortex, generated/source-free output, databases, warehouses, object stores, table
    formats, REST/Flight/ADBC, and Foundry.
  - Status rule: scoreboard rows classify runtime-supported, smoke-supported, report-only, blocked,
    or not-planned posture only. Actionable compatibility/runtime work remains represented by
    `GAR-COMPAT-1`, `GAR-GEN-1`, `GAR-0008`, `GAR-0020`, `GAR-0028`, `GAR-0031`, and related
    evidence-bearing slices. GAR-COMPAT-1E adds the database/warehouse boundary projection without
    connector runtime.
- `docs/architecture/dynamic-work-shaping.md`,
  `spill-reservation-lifecycle-integration.md`, and `effect-budget-plan.md`
  - Role: runtime shaping, memory/spill lifecycle, and side-effect policy references.
  - Status rule: runtime implementation must be represented by `GAR-0014`, `GAR-0016`,
    `GAR-0019`, or `GAR-0011` before code changes.
- `docs/architecture/correctness-differential-harness.md`,
  `benchmark-competitive-claim-evidence.md`, and `benchmark-suite-catalog.md`
  - Role: correctness, benchmark, claim-evidence, and catalog references.
  - Status rule: claim-grade execution, fuzz/property expansion, comparative reruns, and public
    claims are represented by `GAR-0015`, `GAR-0029`, `GAR-0040`, and `GAR-0041`.
- `docs/architecture/agent-contract-pack.md`
  - Role: agent protocol and no-fallback task contract reference.
  - Status rule: protocol changes must remain represented by `GAR-0010`, `GAR-0037`, or `GAR-0039`.
- `docs/architecture/vortex-upstream-alignment-hardening.md`
  - Role: Vortex compatibility, Scan API, compute-provider, residual-boundary, device,
    extension-type, object-store telemetry, integration-boundary, and benchmark-interoperability
    contract reference.
  - Status rule: contract reference only; provider promotion, Vortex-native execution, and
    dependency changes must remain represented by the relevant GAR provider/runtime/release item
    before implementation.
- `docs/skills/vortex/vortex-first-provider-check.md`
  - Role: Vortex-adjacent implementation guard requiring agents to check upstream Vortex concepts
    and classify decisions before inventing new ShardLoom abstractions.
  - Status rule: process guard only; it does not authorize new Vortex API use, dependency changes,
    runtime behavior, support claims, external engine invocation, or fallback execution.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value,
not by numeric CG order.

Autonomous ordering rule:

1. Finish the unchecked non-runtime closeout queue first.
2. Then work the runtime implementation queue.
3. Runtime queue items must explicitly enable an end-user runtime path, a runtime admission/blocker
   that protects user-visible behavior, or a validator that gates runtime claims. Docs-only or
   report-only work cannot complete a runtime item unless the item is explicitly a runtime-safety
   blocker.

### Global Architecture Review Carry-Forward

Source: `docs/architecture/global-architecture-review.md`.

Scope: every unchecked RFC and compute-flow review item is mirrored here so no planned,
unsupported, or not-claimable architecture work exists only in a supporting document. Complete these
items in logical implementation order, update the global review checkbox when evidence closes, and
move the completed session details to `docs/architecture/phased-execution-completed-ledger.md`.

Default GAR verification for planning-only/docs slices:

```powershell
cargo test -p shardloom-contract-tests --test release_readiness_metadata
cargo test -p shardloom-contract-tests --test traditional_benchmark_harness
git diff --check
```

Code-bearing GAR slices must add the focused Rust/Python/benchmark tests named in the slice and
usually end with:

```powershell
cargo fmt --all -- --check
cargo test --workspace --all-targets
python -m compileall -q python/src python/tests scripts examples
git diff --check
```

#### GAR-P0 - Execution Mode, Provider Admission, And Vortex Spine

P0 slices must preserve the canonical execution-mode vocabulary from
`docs/architecture/compute-engine-flow-reference.md`: `auto`, `compatibility_import_certified`,
`prepared_vortex`, `native_vortex`, and `direct_compatibility_transient`. Benchmark interpretation
must continue to report stage timing fields (`source_read_millis`, `compatibility_parse_millis`,
`compatibility_to_vortex_import_millis`, `vortex_write_millis`, `vortex_reopen_millis`,
`vortex_scan_millis`, `operator_compute_millis`, `result_sink_write_millis`,
`evidence_render_millis`, and `total_runtime_millis`) so compatibility rows are interpreted as
ingest/stage/certification work, not pure query speed. Do not add a hidden global fast-mode toggle.

#### Non-Runtime Closeout Queue

Complete these documentation, capability, security, release, and claim-gate items before starting
new runtime implementation work unless the user explicitly reprioritizes. These items must not add
runtime behavior or support claims.

- [ ] GAR-0010-B DataFrame/notebook and package surface readiness report
  - Source: RFC 0010; RFC 0024; RFC 0032.
  - Current state: package dry-run docs exist; mature DataFrame/notebook surfaces and publication are
    not claimable.
  - Next slice outcome: report-only readiness matrix for DataFrame/notebook APIs, package surface,
    examples, and unsupported diagnostics.
  - User-visible surface: docs, Python capability view, release gate.
  - Implementation scope: docs/report fields, Python package metadata checks, tests.
  - Evidence required: release/package refs and diagnostic/no-fallback refs.
  - Acceptance: readiness report distinguishes installed package smoke from runtime support.
  - Verification: release readiness metadata tests, Python compileall, default GAR verification.
  - Non-goals: no PyPI/Conda publication or DataFrame execution.
  - Fallback/claim boundary: no package-release or DataFrame-runtime claim.
  - Dependencies/blockers: GAR-0024 release/package slices.
- [ ] GAR-0022-A Substrait import/export report-only contract
  - Source: RFC 0022; plan IR docs; rfc-coverage followthrough.
  - Current state: native Plan IR exists; real Substrait import/export and imported-plan execution
    are not implemented.
  - Next slice outcome: deterministic report for parse/export support status, unsupported diagnostics,
    and imported-plan evidence requirements.
  - User-visible surface: CLI plan import/export, docs.
  - Implementation scope: plan portability report, CLI output, tests.
  - Evidence required: diagnostic/no-fallback refs; no execution evidence in this slice.
  - Acceptance: Substrait requests report support status without executing imported plans.
  - Verification: plan portability tests and default GAR verification.
  - Non-goals: no Substrait execution or dependency expansion.
  - Fallback/claim boundary: imported plans are not runtime-supported.
  - Dependencies/blockers: dependency/license approval for any parser library.
- [ ] GAR-0032-E best-default certification gate
  - Source: RFC 0032; operational evidence policy hardening; benchmark-suite catalog.
  - Current state: user capability and sufficiency reports exist; best-default claims are not
    claim-grade.
  - Next slice outcome: gate defining required correctness, benchmark, certificate, Native I/O,
    policy, release, and UX evidence before best-default language is allowed.
  - User-visible surface: CLI capability view, docs, release gate.
  - Implementation scope: certification gate report, tests, docs.
  - Evidence required: all claim evidence categories named in the gate.
  - Acceptance: missing evidence yields `claim_gate_status=not_claim_grade`.
  - Verification: certification gate tests, release readiness metadata tests.
  - Non-goals: no best-default claim.
  - Fallback/claim boundary: no best-default, performance, or replacement claim.
  - Dependencies/blockers: benchmark/correctness/release GAR-P5 slices.
- [ ] GAR-0036-A Foundry package and proof boundary matrix
  - Source: RFC 0036; Foundry integration pack docs; release docs.
  - Current state: local Foundry proof docs exist; production `shardloom-foundry`, package
    publication, service invocation, Artifact Repository publication, Compute Module, virtual-table
    native execution, dataset transaction runtime, and F10 deployment are not certified.
  - Next slice outcome: matrix separating local proof, package readiness, service invocation,
    virtual tables, dataset transaction, and deployment evidence.
  - User-visible surface: Foundry docs, release gate, example outputs.
  - Implementation scope: docs/report fields, proof script metadata, tests.
  - Evidence required: release/package refs, Native I/O refs for any native lane, policy/no-fallback
    refs.
  - Acceptance: Foundry external compute is never reported as ShardLoom execution.
  - Verification: Foundry proof script tests if touched, release readiness metadata tests.
  - Non-goals: no Foundry invocation, publication, or platform credential.
  - Fallback/claim boundary: Foundry remains optional integration, not a fallback engine.
  - Dependencies/blockers: package publication and credentials gates.
#### GAR-P4 - Extension, Governance, And Runtime Policy

- [ ] GAR-0011-A extension manifest and external-effect capability matrix
  - Source: RFC 0011; effect budget plan; RFC 0019.
  - Current state: extension manifests/effect budgets are represented; execution, UDFs, LLM/API calls,
    embeddings, and external effects are unsupported/report-only.
  - Next slice outcome: matrix for extension types, required permissions, materialization/effect
    metadata, and default blockers.
  - User-visible surface: CLI extension plan, capability view, docs.
  - Implementation scope: extension/effect report fields, diagnostics, tests.
  - Evidence required: policy/security/no-fallback refs.
  - Acceptance: all external effects default to blocked with deterministic diagnostics.
  - Verification: extension planning tests, effect budget tests.
  - Non-goals: no extension execution, network call, model call, or embedding runtime.
  - Fallback/claim boundary: no external-effect support claim.
  - Dependencies/blockers: GAR-0019 credential/policy.
- [ ] GAR-0019-A credential lifecycle and policy enforcement gate
  - Source: RFC 0019; operational evidence policy hardening; security docs.
  - Current state: security/policy reports exist; production credential lifecycle and runtime policy
    enforcement are not complete.
  - Next slice outcome: gate for credential resolution, secret loading, redaction, workspace policy,
    runtime permission checks, and unsupported diagnostics.
  - User-visible surface: CLI security/governance plan, release security gate.
  - Implementation scope: security report fields, CLI output, tests.
  - Evidence required: security policy refs, redaction/path-safety refs, no-fallback refs.
  - Acceptance: credential use defaults to denied unless a slice explicitly admits it with evidence.
  - Verification: security/path-safety tests, release security gate tests.
  - Non-goals: no secret loading, network credential use, or production policy runtime.
  - Fallback/claim boundary: no governed production runtime claim.
  - Dependencies/blockers: security release gate.
- [ ] GAR-0019-B sandbox and governance runtime readiness
  - Source: RFC 0019; RFC 0023; effect budget plan.
  - Current state: sandbox/governance concepts exist; sandbox execution is not a production runtime.
  - Next slice outcome: readiness report for sandbox isolation, filesystem/network permissions,
    audit logs, and deny-by-default behavior.
  - User-visible surface: CLI governance plan, docs, release gate.
  - Implementation scope: governance report, diagnostics, tests.
  - Evidence required: security/no-fallback refs and audit artifact refs.
  - Acceptance: sandbox-dependent work remains blocked until isolation evidence exists.
  - Verification: security contract tests and default GAR verification.
  - Non-goals: no sandbox process runtime.
  - Fallback/claim boundary: no sandbox execution claim.
  - Dependencies/blockers: plugin ABI and credential gates.
- [ ] GAR-0023-A plugin ABI loading and UDF sandbox blocker
  - Source: RFC 0023; RFC 0011; RFC 0019.
  - Current state: plugin ABI/sandbox/UDF execution are represented as planned/report-only surfaces.
  - Next slice outcome: ABI loading contract, sandbox evidence requirements, and UDF execution
    blockers.
  - User-visible surface: CLI plugin/extension plan, capability view.
  - Implementation scope: plugin report fields, diagnostics, tests.
  - Evidence required: sandbox refs, policy/no-fallback refs.
  - Acceptance: plugins cannot load or execute without explicit policy and sandbox evidence.
  - Verification: plugin/extension planning tests, security tests.
  - Non-goals: no dynamic loading or UDF execution.
  - Fallback/claim boundary: plugin support remains report-only.
  - Dependencies/blockers: GAR-0019 sandbox and credential gates.

#### GAR-P5 - Correctness, Benchmarks, Claims, And Release

- [ ] GAR-0001B-A engine-replacement claim inventory
  - Source: RFC 0001; RFC 0025; global architecture review.
  - Current state: Spark-displacement/engine-replacement claims are not claimable.
  - Next slice outcome: inventory mapping each replacement claim to required runtime, output,
    correctness, benchmark, certificate, Native I/O, and no-fallback evidence.
  - User-visible surface: release claim gate, docs.
  - Implementation scope: release gate docs/report, tests.
  - Evidence required: all claim categories as checklist refs; no execution evidence in inventory
    slice.
  - Acceptance: missing evidence yields `claim_gate_status=not_claim_grade`.
  - Verification: release readiness metadata tests and default GAR verification.
  - Non-goals: no replacement claim or benchmark rerun.
  - Fallback/claim boundary: no public displacement language.
  - Dependencies/blockers: GAR-0009 and GAR-0041 claim gates.
- [ ] GAR-0009-A Spark-displacement benchmark evidence matrix
  - Source: RFC 0009; benchmark competitive claim evidence; benchmark-suite catalog.
  - Current state: local benchmark evidence exists; broad Spark-displacement evidence and public
    performance claims are gated.
  - Next slice outcome: evidence matrix tying workloads, baselines/oracles, correctness, timing,
    environment, mode, and claim status.
  - User-visible surface: benchmark report, docs, release claim gate.
  - Implementation scope: benchmark metadata/report, docs, contract tests.
  - Evidence required: correctness refs, benchmark refs, policy/no-fallback refs, environment refs.
  - Acceptance: every claim row says claim-grade or not-claim-grade and explains why.
  - Verification: benchmark contract tests, release readiness metadata tests.
  - Non-goals: no public performance claim.
  - Fallback/claim boundary: external engines are comparison baselines/oracles only.
  - Dependencies/blockers: reproducible benchmark reruns and release gate.
- [ ] GAR-0015-A fuzz/property and semantic differential expansion
  - Source: RFC 0015; correctness differential harness; correctness fixture manifest.
  - Current state: selected correctness fixtures exist; fuzz/property expansion is not broad.
  - Next slice outcome: add one fixture family or report-only gap with expected reference artifacts.
  - User-visible surface: correctness harness, docs, release gate.
  - Implementation scope: fixtures, harness metadata, tests.
  - Evidence required: correctness refs, oracle/reference refs, no-fallback refs.
  - Acceptance: selected family has deterministic references or an explicit gap.
  - Verification: correctness harness tests, fixture manifest tests.
  - Non-goals: no superiority/performance claim.
  - Fallback/claim boundary: correctness expansion alone does not create performance claims.
  - Dependencies/blockers: operator/source coverage.
- [ ] GAR-0024-A publication and API/schema stability gate
  - Source: RFC 0024; release docs; workspace feature build matrix.
  - Current state: dry-run package proof exists; first public publication, stable API/schema windows,
    and signing decisions are not complete.
  - Next slice outcome: release gate rows for API/schema compatibility, package identities, signing,
    checksums, SBOM, and publication approval.
  - User-visible surface: release readiness check, docs.
  - Implementation scope: release scripts/docs/tests.
  - Evidence required: release/provenance/security refs, no-fallback refs.
  - Acceptance: gate fails closed without explicit publication evidence.
  - Verification: release readiness tests, dependency audit gate, provenance dry-run tests.
  - Non-goals: no package publication, tags, or signing key use.
  - Fallback/claim boundary: no public release claim.
  - Dependencies/blockers: security/provenance gates.
- [ ] GAR-0025-A competitive replacement sufficiency gate
  - Source: RFC 0025; RFC 0029; RFC 0041.
  - Current state: full competitive replacement is not supported by broad evidence.
  - Next slice outcome: sufficiency gate requiring correctness, benchmark, Native I/O, certificates,
    capability coverage, no-fallback, and release evidence.
  - User-visible surface: release claim gate and docs.
  - Implementation scope: claim gate report, tests.
  - Evidence required: all claim-grade evidence categories.
  - Acceptance: replacement claims fail closed until every required evidence row passes.
  - Verification: release readiness metadata tests and claim gate tests.
  - Non-goals: no replacement claim or runtime expansion.
  - Fallback/claim boundary: `claim_gate_status=not_claim_grade`.
  - Dependencies/blockers: GAR-0009, GAR-0015, GAR-0029, GAR-0041.
- [ ] GAR-0029-A CG-5/CG-6 and stateful reuse evidence expansion
  - Source: RFC 0029; correctness differential harness; benchmark-suite catalog.
  - Current state: current CG-5/CG-6 evidence is scoped; production stateful reuse runtime and
    performance/superiority claims are not broad.
  - Next slice outcome: one evidence expansion for correctness/benchmark/stateful reuse, or a
    deterministic blocker report.
  - User-visible surface: correctness harness, benchmark report, stateful reuse plan.
  - Implementation scope: harness/benchmark metadata, CLI plan, tests.
  - Evidence required: correctness refs, benchmark refs, execution certificates, Native I/O refs,
    no-fallback refs.
  - Acceptance: selected workload has attached evidence or an explicit claim blocker.
  - Verification: correctness and benchmark contract tests, focused stateful reuse tests.
  - Non-goals: no production cache/reuse runtime unless a separate implementation slice admits it.
  - Fallback/claim boundary: no superiority claim.
  - Dependencies/blockers: workload fixtures and Native I/O coverage.
- [ ] GAR-0040-A comparative rerun and managed-platform posture gate
  - Source: RFC 0040; benchmark-suite catalog; benchmark competitive claim evidence.
  - Current state: full comparative reruns, source-backed claim-grade promotion, managed-platform
    lanes, credentials, dependencies, and public performance claims are not enabled.
  - Next slice outcome: gate separating local reruns, external baseline/oracle comparisons,
    managed-platform lanes, credential requirements, and claim blockers.
  - User-visible surface: benchmark report, release claim gate, docs.
  - Implementation scope: benchmark metadata/report, release gate, tests.
  - Evidence required: benchmark refs, environment refs, credential policy refs, no-fallback refs.
  - Acceptance: managed-platform lanes require explicit credentials and remain comparison-only unless
    claim evidence passes.
  - Verification: benchmark contract tests, release readiness metadata tests.
  - Non-goals: no managed-platform run, credential use, dependency addition, or public claim.
  - Fallback/claim boundary: external systems are never ShardLoom execution.
  - Dependencies/blockers: GAR-0019 credential policy and GAR-0041 claim matrix.
- [ ] GAR-0041-A per-claim evidence attachment matrix
  - Source: RFC 0041; workspace feature build matrix; release security gate.
  - Current state: release claims are not claimable until required matrix rows have attached passing
    evidence.
  - Next slice outcome: matrix that binds each public claim to test, benchmark, certificate, Native
    I/O, security, provenance, and unsupported-path evidence.
  - User-visible surface: release gate output, docs.
  - Implementation scope: release check scripts/docs/tests.
  - Evidence required: all evidence categories named per claim.
  - Acceptance: any missing row fails the claim gate.
  - Verification: release readiness tests, workspace feature matrix tests.
  - Non-goals: no new claim or publication.
  - Fallback/claim boundary: claims fail closed by default.
  - Dependencies/blockers: evidence-producing GAR slices.
- [ ] GAR-0043-A hard release-readiness validators and architecture tracker
  - Source: RFC 0043; release security gate; global architecture review.
  - Current state: hard release-readiness gate exists, but final publication/attestation and
    architecture tracker validation need full evidence.
  - Next slice outcome: validator that checks traceability matrix, RFC acceptance, architecture
    tracker status, unsupported paths, and security/provenance evidence.
  - User-visible surface: release readiness script/report, docs.
  - Implementation scope: release scripts, contract tests, docs.
  - Evidence required: release/security/provenance refs, no-fallback refs, architecture review refs.
  - Acceptance: release gate fails closed when global review unchecked items or missing evidence block
    a claim.
  - Verification: release readiness metadata tests, release security gate tests, default GAR
    verification.
  - Non-goals: no publication, tags, secrets, or package upload.
  - Fallback/claim boundary: no final release/public claim.
  - Dependencies/blockers: every required claim/evidence slice.
- [ ] GAR-0043-B publication attestation and final release rehearsal
  - Source: RFC 0043; RFC 0024; release provenance docs.
  - Current state: dry-run/provenance scaffolding exists; actual publication and final attestation
    are not performed.
  - Next slice outcome: no-publication rehearsal that proves package artifacts, checksums, SBOM,
    attestations, and unsupported-path evidence without creating tags or uploads.
  - User-visible surface: release rehearsal report, docs.
  - Implementation scope: release scripts/docs/tests.
  - Evidence required: SBOM/checksum/provenance refs, security refs, no-fallback refs.
  - Acceptance: rehearsal produces local artifacts only and marks publication as human-approved.
  - Verification: release provenance dry-run tests, release readiness tests.
  - Non-goals: no package publication, tag, feedstock, marketplace, or secret use.
  - Fallback/claim boundary: rehearsal does not authorize release claims.
  - Dependencies/blockers: GAR-0043-A validators and GAR-0024 publication gate.

#### Runtime Implementation Queue

The earlier broad runtime rollup queues have been consolidated into the implementation-ready
`GAR-RUNTIME-IMPL-4*` and `GAR-RUNTIME-IMPL-5*` queues below. Work these only after the
unchecked non-runtime closeout items above are complete or explicitly reprioritized by the user.

Runtime completion rule:

- Every runtime item must enable a concrete runtime behavior, runtime admission/blocker, or
  runtime-claim validator that directly protects a usable workflow.
- A docs-only or report-only update cannot complete a runtime item unless the item explicitly says
  it is a runtime-safety blocker or validator.
- Completed runtime details belong in `docs/architecture/phased-execution-completed-ledger.md`, not
  in this live queue.

#### GAR-RUNTIME-IMPL-4 - Final Full-Runtime Implementation Leaf Queue

This queue exists to keep the remaining "fully functional / usable compute engine" work from
hiding inside broad architecture items. Treat these as the explicit runtime implementation slices
that must be worked before any full-runtime readiness claim. Each item below must land runnable
runtime behavior, deterministic runtime admission/blockers, or runtime-claim validation; planning
or documentation updates alone are insufficient.

- [ ] GAR-RUNTIME-IMPL-4D expression, cast, null, string, and date runtime families
  - Source: RFC 0021, SQL/Python local runtime smokes, expression/operator semantics.
  - Current state: expression support exists in scoped smoke paths, but user workflows still lack
    broad typed casts, null predicates, string predicates, and date/time helpers.
  - Next slice outcome: add one implementation PR per expression family: numeric casts,
    null/is-not-null, string equality/prefix/contains where admitted, and date extraction/literals.
  - User-visible surface: SQL/Python query builder, explain output, capability matrix, docs.
  - Implementation scope: expression IR, type coercion policy, null semantics, parser lowering,
    native evaluators, diagnostics.
  - Evidence required: expression family, input/output dtype, null policy, cast status, decoded/
    materialized flags, correctness digest, no-fallback fields.
  - Acceptance: every admitted expression has fixture coverage and unsupported expressions report a
    deterministic diagnostic.
  - Verification: expression unit tests, SQL/Python smoke tests, unsupported snapshots, release
    readiness metadata.
  - Non-goals: no arbitrary UDFs, regex parity, timezone completeness, or ANSI SQL claim.
  - Claim boundary: expression-family support per admitted dtype/operator.
  - Fallback boundary: expression evaluation must remain ShardLoom-native.
  - Dependencies/blockers: expression IR stability, dtype coercion policy, decoded-reference
    fixtures, and SQL/Python lowering.
  - Ledger rule: ledger entry must enumerate expression families, dtypes, and blockers.

- [ ] GAR-RUNTIME-IMPL-4E generated-source builders as ordinary local runtime
  - Source: `GAR-GEN-1`, `GAR-COMPAT-1B`, Use Case Atlas generated-source rows.
  - Current state: source-free/generated-output smokes exist, but `range`, `from_rows`,
    `literal_table`, `calendar`, SQL `VALUES`, and source-free projection are not one coherent
    public runtime surface.
  - Next slice outcome: implement generated-source builders across CLI/Python/SQL with local JSONL
    or Vortex output where admitted.
  - User-visible surface: Python `ctx.range`, `ctx.from_rows`, `ctx.literal_table`, `ctx.calendar`,
    SQL `VALUES`, generated-output recipes.
  - Implementation scope: generator nodes, schema inference, deterministic seed/row-count handling,
    output writer bridge, report/certificate fields.
  - Evidence required: `input_dataset_count=0`, `source_io_performed=false`,
    `generated_source_created=true`, generated source kind/schema/row/plan digest, seed,
    determinism flag, output certificate, no-fallback fields.
  - Acceptance: no-input smoke remains separate; each admitted generator writes local output and
    exposes a GeneratedSourceCertificate.
  - Verification: CLI/Python/SQL generator tests, output smoke, use-case coverage, release
    readiness metadata.
  - Non-goals: no object-store write, Foundry production claim, package publication, or broad
    SQL/DataFrame claim.
  - Claim boundary: local deterministic generated-output runtime only.
  - Fallback boundary: no generated rows or expressions may be produced by an external engine.
  - Dependencies/blockers: generated-source schema contract, local output writer registry,
    expression semantics, and Python/SQL surface admission.
  - Ledger rule: ledger entry must list generator kind, output format, and unsupported generators.

- [ ] GAR-RUNTIME-IMPL-4F local input adapter runtime coverage by format
  - Source: `GAR-IOREUSE-1A`, universal compatibility scoreboard, local input adapter docs.
  - Current state: CSV is the strongest local smoke path; JSONL/JSON, Parquet, Arrow IPC, Avro,
    and ORC do not all have ordinary user-facing SourceState runtime parity.
  - Next slice outcome: promote one local input format at a time into the adapter registry with
    SourceState evidence and deterministic blockers for unsupported formats/features.
  - User-visible surface: CLI/Python read helpers, use cases, capability/status matrix, benchmark
    source-format rows.
  - Implementation scope: format detection, local reader, schema/dtype inference, fingerprinting,
    SourceState digest, decode/materialization evidence.
  - Evidence required: source format/location/fingerprint, schema digest, SourceState id/digest,
    row count/file count/bytes, decode/materialization status, no-fallback fields.
  - Acceptance: each listed format is either runnable with evidence or blocked with actionable
    diagnostics; adapter support never implies Vortex-native execution.
  - Verification: per-format smoke tests, schema snapshot tests, unsupported diagnostics,
    benchmark harness contract tests.
  - Non-goals: no object-store, database, table/lakehouse, or universal adapter claim.
  - Claim boundary: local file support per admitted format and feature subset.
  - Fallback boundary: no external engine may parse, plan, or execute input workloads.
  - Dependencies/blockers: reader dependency/license approval, source fixtures, schema inference
    coverage, and SourceState schema fields.
  - Ledger rule: ledger entry must include the per-format support table.

- [ ] GAR-RUNTIME-IMPL-4G local output writer registry and fanout promotion
  - Source: OutputPlan, result-sink replay proof, cross-format fanout architecture.
  - Current state: scoped local JSONL output exists; CSV, Parquet, Arrow IPC, Vortex, replay proof,
    and multi-output fanout are not ordinary user-facing runtime features.
  - Next slice outcome: add local writer registry and fanout for admitted formats, with per-output
    digest, replay status, and metadata fidelity/loss.
  - User-visible surface: CLI/Python `.write` and `.fanout`, recipes, benchmark
    `io_reuse_and_fanout`, website status.
  - Implementation scope: OutputPlan builder, writers, schema translation, output digests, replay
    verifier, fanout orchestration.
  - Evidence required: output plan id/digest, format/location/schema, write timing, replay status,
    metadata fidelity/loss, correctness digest, no-fallback fields.
  - Acceptance: one admitted input/prepared state can write multiple local outputs; unsupported
    writers and object-store sinks block deterministically.
  - Verification: writer smoke per format, fanout smoke, replay tests, use-case coverage,
    benchmark contract tests.
  - Non-goals: no object-store write, table commit, production sink claim, or performance claim.
  - Claim boundary: local output/fanout support per admitted format.
  - Fallback boundary: compatibility output is export, not external-engine execution.
  - Dependencies/blockers: local writer dependencies, schema translation, replay verifier,
    generated/local/Vortex source evidence, and fanout benchmark fields.
  - Ledger rule: ledger entry must list format combinations and replay proof refs.

- [ ] GAR-RUNTIME-IMPL-4H Vortex prepare/read/write/reopen lifecycle promotion
  - Source: Vortex provider docs, compute-flow reference, prepared/native benchmark evidence.
  - Current state: prepared/native evidence exists in scoped benchmark paths; a simple user
    lifecycle from local source to Vortex artifact, query, write, reopen, and verify remains
    incomplete.
  - Next slice outcome: implement a documented local Vortex lifecycle command and Python helper
    for one admitted operator family.
  - User-visible surface: CLI, Python helper, benchmark rows, compute-flow, Field Guide/status.
  - Implementation scope: VortexPreparedState, local Vortex writer, reopen verifier,
    source-backed scan bridge, digest/certificate reporting.
  - Evidence required: prepared state/artifact refs, layout/encoding/stats summary, write/reopen
    digest, scan fields, decode/materialization status, no-fallback fields.
  - Acceptance: workflow runs without compatibility re-import during query timing; unsupported
    Vortex layouts/features block.
  - Verification: lifecycle smoke, writer/reopen tests, source-backed scan tests, benchmark
    harness contract tests.
  - Non-goals: no object-store Vortex artifact, blanket encoded-native claim, or performance claim.
  - Claim boundary: local Vortex lifecycle for admitted layouts/operators only.
  - Fallback boundary: Vortex query-engine integrations remain prohibited.
  - Dependencies/blockers: Vortex dependency/version gate, local writer/reopen support, scan
    provider admission, and operator coverage.
  - Ledger rule: ledger entry must include artifact refs, operator scope, and reopen proof.

- [ ] GAR-RUNTIME-IMPL-4I Vortex scan pushdown and encoded-predicate runtime completion
  - Source: `GAR-PERF-2C`, Vortex Scan API docs, encoded predicate provider evidence.
  - Current state: source-backed scan and encoded predicate evidence are scoped; pushdown is not
    complete across admitted prepared/native scenarios.
  - Next slice outcome: lower filter, projection, and limit into Vortex Scan where admitted, and
    emit deterministic blockers when a predicate/projection cannot be pushed down.
  - User-visible surface: prepared/native benchmark rows, explain output, capability matrix.
  - Implementation scope: scan request builder, filter expression lowering, projection mask, limit/
    slice pushdown, evidence fields.
  - Evidence required: filter/projection/limit pushdown status, filter/output columns read,
    encoded predicate provider fields, data decoded/materialized, no-fallback fields.
  - Acceptance: supported scenarios avoid reading unused output columns; unsupported pushdown does
    not silently fall back to full materialization.
  - Verification: selective-filter smoke, filter/projection/limit smoke, source-backed scan tests,
    benchmark contract tests.
  - Non-goals: no encoded-native claim from pushdown evidence alone.
  - Claim boundary: pushdown support per admitted predicate/projection/limit shape.
  - Fallback boundary: residual work must be ShardLoom-native or blocked.
  - Dependencies/blockers: Vortex Scan API provider boundary, expression lowering, projection mask
    support, and source-backed scan evidence.
  - Ledger rule: ledger entry must list pushed-down and blocked expression shapes.

- [ ] GAR-RUNTIME-IMPL-4J encoded kernel registry execution pairs
  - Source: `GAR-PERF-2D`, RFC 0021, encoded execution docs.
  - Current state: encoded-kernel evidence exists for selected scoped inputs; broad encoded-native
    operator coverage remains incomplete.
  - Next slice outcome: implement or block one encoding/operator pair at a time, starting with
    bitpacked boolean/integer filters, sequence equality/range, dictionary equality/group-by, and
    constant array count/filter.
  - User-visible surface: benchmark evidence, explain output, capability matrix.
  - Implementation scope: kernel registry, admission policy, encoded evaluator, decoded reference
    comparison, blockers.
  - Evidence required: encoding id, operator family, kernel admitted/executed, canonicalization
    required, decoded/materialized flags, correctness digest, encoded-native claim flag.
  - Acceptance: supported pairs pass decoded-reference correctness; unsupported encodings block
    deterministically.
  - Verification: unit tests per pair, selective-filter/group-by benchmark smoke, capability
    snapshots.
  - Non-goals: no blanket encoded-native, vectorized parity, or performance claim.
  - Claim boundary: encoding/operator-pair support only.
  - Fallback boundary: decoded reference is a test oracle, not runtime fallback.
  - Dependencies/blockers: encoding fixtures, kernel registry admission, decoded-reference
    correctness harness, and benchmark row schema.
  - Ledger rule: ledger entry must enumerate pairs, claim flags, and blockers.

- [ ] GAR-RUNTIME-IMPL-4K unified execution envelope and certificate validators
  - Source: release readiness metadata, benchmark artifact policy, runtime evidence-level docs.
  - Current state: runtime reports have useful fields, but command, Python, benchmark, and website
    envelopes can diverge.
  - Next slice outcome: add a versioned execution-envelope schema and validators for every runtime
    path.
  - User-visible surface: CLI JSON, Python typed reports, benchmark artifacts, website evidence,
    release readiness.
  - Implementation scope: shared schema, adapters, aliases/migrations, readiness checks, website
    renderer updates.
  - Evidence required: execution/engine/evidence mode, source/generated/output refs,
    materialization/decode refs, certificate refs, no-fallback fields, claim gate.
  - Acceptance: missing fallback/certificate/claim fields fail validation; report-only rows cannot
    masquerade as runtime support.
  - Verification: schema contract tests, release readiness metadata, benchmark completeness,
    website readiness, Python typed-report tests.
  - Non-goals: no runtime capability or claim upgrade from schema work alone.
  - Claim boundary: evidence standardization only.
  - Fallback boundary: every envelope must expose `fallback_attempted` and
    `external_engine_invoked`.
  - Dependencies/blockers: stable field naming, compatibility aliases, Python report migration, and
    benchmark/website validators.
  - Ledger rule: ledger entry must record schema version and migrated surfaces.

- [ ] GAR-RUNTIME-IMPL-4L ShardLoomSession, SourceState, PreparedState, and OutputPlan reuse runtime
  - Source: `GAR-IOREUSE-1`, `GAR-PERF-2F`, in-process session runtime docs.
  - Current state: scoped batch/session evidence exists; ordinary user workflows do not yet share a
    reusable session/cache lifecycle.
  - Next slice outcome: implement a scoped in-process `ShardLoomSession` that reuses SourceState,
    VortexPreparedState, schema/dictionary state, and OutputPlan where fingerprints remain valid.
  - User-visible surface: CLI batch/session command, Python context/session, benchmark timing rows.
  - Implementation scope: session lifecycle, cache keys/fingerprints, invalidation policy, cache
    hit/miss evidence, explicit close/cleanup.
  - Evidence required: session id, cache hit/miss, reuse digest/reason, source/prepared/output
    state ids, invalidation reason, no-fallback fields.
  - Acceptance: repeated admitted workflows reuse state safely; stale source/schema/plan changes
    invalidate cache; session state is explicitly scoped and closed.
  - Verification: session smoke, invalidation tests, source/prepared/output reuse tests, benchmark
    harness contract tests.
  - Non-goals: no daemon/service, distributed cache, hidden fast mode, or performance claim.
  - Claim boundary: scoped in-process reuse only.
  - Fallback boundary: cache/session cannot change execution provider to an external engine.
  - Dependencies/blockers: fingerprint/invalidation contract, SourceState/VortexPreparedState/
    OutputPlan ids, explicit session lifecycle, and cache cleanup policy.
  - Ledger rule: ledger entry must list cache artifacts, invalidation rules, and disabled paths.

- [ ] GAR-RUNTIME-IMPL-4M benchmark refresh and runtime claim gate after each promoted workflow
  - Source: `GAR-BENCH-PUB-1`, benchmark publishing runbook, release claim gates.
  - Current state: benchmark publishing is structured, but each newly promoted runtime path needs a
    fresh artifact, scenario coverage, and public claim boundary update.
  - Next slice outcome: require every runtime-promotion PR to update or attach a focused benchmark/
    correctness/evidence artifact and refresh website/docs only when the artifact is claim-safe.
  - User-visible surface: website benchmarks, docs/benchmarks, release readiness, status matrix.
  - Implementation scope: artifact freshness checker, runtime claim matrix, benchmark page
    ingestion, release validators.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, certificate
    refs, correctness refs, no-fallback fields, claim gate.
  - Acceptance: no promoted path is presented publicly without current evidence; stale or incomplete
    artifacts block claim-grade status.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: evidence gate only; claims remain workload-scoped.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: benchmark manifest schema, runtime envelope validators, scenario
    fixtures, and website renderer support.
  - Ledger rule: ledger entry must include artifact refs and public claim status.

- [ ] GAR-RUNTIME-IMPL-4N object-store read admission with local emulator/public fixture proof
  - Source: `GAR-COMPAT-1C`, `GAR-SCALE-1E`, object-store request planner.
  - Current state: object-store planning/report-only surfaces exist; runtime reads are blocked.
  - Next slice outcome: implement URI parse, credential/effect policy, optional listing, byte-range
    read, streaming/full-file read, and SourceState evidence in an approved emulator or public
    no-credential fixture profile.
  - User-visible surface: CLI/Python object-store diagnostics, capability/status pages, use cases.
  - Implementation scope: provider abstraction, effect gate, credential policy, request planner,
    byte-range adapter, local cache boundary, tests.
  - Evidence required: provider/profile, credential/network status, object version/ETag, byte
    ranges, SourceState id, Native I/O certificate, no-fallback fields.
  - Acceptance: public and authenticated read gates are separate; no network probe or credential
    resolution runs by default; unsupported providers fail closed.
  - Verification: policy tests, mocked/emulator read smoke, SourceState snapshot tests, release
    readiness, website status checks.
  - Non-goals: no object-store write, table commit, production object-store claim, or managed
    platform claim.
  - Claim boundary: provider/profile-specific technical-preview read proof only.
  - Fallback boundary: storage provider access does not authorize external query execution.
  - Dependencies/blockers: security/effect policy, provider test harness, dependency/license
    review, and emulator or public no-credential fixture availability.
  - Ledger rule: ledger entry must record provider, credential posture, and proof refs.

- [ ] GAR-RUNTIME-IMPL-4O object-store write and table/lakehouse commit ladder
  - Source: table/lakehouse commit semantics gate, object-store scale ladder.
  - Current state: object-store writes, table metadata/snapshot scans, append, merge/delete, commit,
    rollback, and catalog integration are blocked or report-only.
  - Next slice outcome: after read proof, implement staged write/commit/recovery in an approved
    provider/emulator, then one fixture-backed table metadata/snapshot operation and one append or
    commit rehearsal where admitted.
  - User-visible surface: table/object-store capability views, CLI/Python diagnostics, status/use
    cases, scale benchmark rows.
  - Implementation scope: write staging, commit protocol, idempotency, cleanup/retry, table metadata
    adapter, snapshot reader, manifest writer or commit rehearsal.
  - Evidence required: provider/profile, table format, snapshot id, manifest/data-file counts,
    commit protocol/status, rollback/cleanup status, idempotency key, no-fallback fields.
  - Acceptance: read/write/commit and metadata/read/append/commit are separate gates; fixture proof
    does not imply production lakehouse support.
  - Verification: policy tests, emulator write smoke, table fixture tests, commit rehearsal smoke,
    unsupported diagnostics, release readiness.
  - Non-goals: no blanket S3/GCS/ADLS support, production Iceberg/Delta/Hudi claim, catalog
    service, or production table claim.
  - Claim boundary: provider/table-format operation in declared fixture/profile only.
  - Fallback boundary: no external catalog, lakehouse engine, or query engine executes work.
  - Dependencies/blockers: object-store read proof, commit/recovery policy, table fixtures,
    dependency/license review, and idempotency evidence.
  - Ledger rule: ledger entry must list provider, table format, operation, and blocked behaviors.

- [ ] GAR-RUNTIME-IMPL-4P scale-grade local split, memory, spill, shuffle, and retry runtime
  - Source: `GAR-SCALE-1`, RFC 0014, RFC 0016, RFC 0017.
  - Current state: scale contracts exist, but larger-than-memory, split-parallel, spill, shuffle,
    retry, and idempotent output commit runtime are not claimable.
  - Next slice outcome: implement a declared-resource local scale profile with SplitManifest,
    bounded memory checks, per-split execution, spill/backpressure where admitted, one shuffle
    family, retry/idempotency, and output commit evidence.
  - User-visible surface: scale benchmark profiles, CLI/Python execution envelopes, status page.
  - Implementation scope: split scheduler, memory budget, spill manager, shuffle plan, retry/
    cancellation/recovery, output commit status, scale benchmark rows.
  - Evidence required: scale profile/status, data volume, split/file/partition counts,
    memory/spill/shuffle fields, retry/idempotency, output commit status, correctness digest.
  - Acceptance: larger-than-memory and split-parallel claims require real bytes and correctness
    proof; synthetic metadata cannot become runtime scale claim.
  - Verification: split manifest tests, local stress smoke, spill/backpressure tests, shuffle
    correctness tests, retry/idempotency tests, scale benchmark contract tests.
  - Non-goals: no literal any-volume, Spark replacement, distributed runtime, or object-store scale
    claim without separate proof.
  - Claim boundary: declared local resource envelope only.
  - Fallback boundary: external engines are baselines/oracles only.
  - Dependencies/blockers: SourceState split metadata, operator coverage, spill storage policy,
    shuffle correctness fixtures, and output commit proof.
  - Ledger rule: ledger entry must include resource envelope, data volume, and claim status.

- [ ] GAR-RUNTIME-IMPL-4Q live, hybrid, loopback control-plane, and distributed blockers
  - Source: RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane technical preview only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, and distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-4R adapters, databases, UDFs, extensions, and effectful operations
  - Source: RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, UDFs, plugins, LLM/API/embedding/vector
    effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, and dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-4S clean install public technical-preview usability gate
  - Source: public preview readiness, package-channel matrix, website
    readiness, Use Case Atlas.
  - Current state: runtime slices are being promoted incrementally; final public usability still
    requires clean install proof, docs/website parity, examples, benchmark evidence, and claim gates.
  - Next slice outcome: run a no-publication technical-preview rehearsal from clean checkout or
    local package artifact through CLI/Python workflows, unsupported diagnostics, benchmarks,
    website/status, security/legal, and release metadata.
  - User-visible surface: README, docs/getting-started, website, package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow
    matrix, benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload, tag, production/platform/performance claim, Spark
    replacement claim, object-store/lakehouse/Foundry production claim, or hidden fast mode.
  - Claim boundary: public technical preview only with workload-scoped claims.
  - Fallback boundary: release gates must fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, clean install script, docs/website
    parity, benchmark artifact policy, and security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, release-gate evidence, and
    remaining unsupported paths.

#### GAR-RUNTIME-IMPL-5 - Runtime Coverage Assurance Implementation Slices

This final queue exists to make the "fully functional / usable compute engine" goal explicit at the
end of Planned. These are coverage-assurance slices for runtime surfaces that are easy to overclaim.
If a 4-series item enables the first runtime path, the matching 5-series item must either prove the
surface is broadly usable or split the remaining runtime gaps into smaller implementation slices.
Completing a 5-series item requires evidence, validators, docs/website parity, and a completed-ledger
entry.

- [ ] GAR-RUNTIME-IMPL-5A generated-source end-user runtime builders
  - Source: `GAR-RUNTIME-IMPL-4E`, `GAR-GEN-1`, `GAR-COMPAT-1B`, Use Case Atlas generated-source
    rows.
  - Current state: no-dataset smoke and scoped generated-output paths exist, but `range`,
    `sequence`, `from_rows`, `literal_table`, `calendar`, SQL `VALUES`, SQL literal projection, and
    DataFrame-style source-free output are not complete as ordinary end-user runtime workflows.
  - Next slice outcome: promote one coherent local generated-source workflow set across CLI,
    Python, and SQL/DataFrame admission, writing local output with generated-source evidence.
  - User-visible surface: `ctx.range(...)`, `ctx.from_rows(...)`, `ctx.literal_table(...)`,
    `ctx.calendar(...)`, SQL `VALUES`/literal `SELECT`, CLI generated-source command, recipes,
    website status.
  - Implementation scope: generated-source plan nodes, schema inference, deterministic seed/row
    accounting, local output writer integration, typed Python report fields, unsupported
    diagnostics.
  - Evidence required: `input_dataset_count=0`, `source_io_performed=false`,
    `generated_source_created=true`, generated-source kind/schema/row/plan digests, deterministic
    seed, output certificate, fallback/no-external-engine fields, claim gate.
  - Acceptance: no-input smoke stays separate from generated-output runtime; admitted generated
    workflows write local output and evidence; unsupported generators/sinks block deterministically.
  - Verification: generated-source CLI tests, Python builder tests, SQL literal/VALUES tests,
    output replay smoke, use-case coverage, website readiness.
  - Non-goals: no S3/object-store sink, Foundry production path, public package publication, or
    broad SQL/DataFrame claim.
  - Claim boundary: local deterministic generated-output runtime only.
  - Fallback boundary: generated rows and expressions must be produced by ShardLoom-native code.
  - Dependencies/blockers: generated-source certificate schema, local output writers, expression
    semantics, and Python/CLI envelope parity.
  - Ledger rule: ledger entry must list each admitted builder, output format, evidence refs, and
    blocked generator/sink shapes.

- [ ] GAR-RUNTIME-IMPL-5B SQL frontend runtime ladder
  - Source: `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4C`, `GAR-RUNTIME-IMPL-4D`, RFC 0032.
  - Current state: scoped local CSV SQL smoke paths exist for projection/filter/limit, scalar and
    grouped aggregates, top-N, and one explicit inner equi-join shape; richer expressions, casts,
    dates, strings, windows, subqueries, catalogs, Python/DataFrame joins, multi-key/expression/
    outer/semi/anti/cross joins, and broad planner behavior remain incomplete or blocked.
  - Next slice outcome: implement a staged SQL ladder that admits only supported syntax families
    and emits stable blockers for unsupported syntax.
  - User-visible surface: CLI SQL command, SQL explain/capability output, docs/use-cases, website
    status.
  - Implementation scope: parser/binder/planner admission, local logical plan lowering, expression
    type/null policy, join/order/aggregate blockers, explain snapshots, tests.
  - Evidence required: parser/binder/planner flags, admitted syntax family, before/after plan
    digests, source/output refs, correctness digest, unsupported diagnostic code, no-fallback
    fields, claim gate.
  - Acceptance: each admitted SQL shape executes through ShardLoom-native code only; every
    unsupported SQL construct fails closed with actionable diagnostics.
  - Verification: SQL parser/binder unit tests, CLI smoke per admitted family, unsupported
    diagnostic snapshots, release readiness metadata, benchmark harness where applicable.
  - Non-goals: no ANSI SQL parity, catalog runtime, production SQL claim, or external SQL engine.
  - Claim boundary: syntax-family scoped local SQL runtime only.
  - Fallback boundary: DataFusion, DuckDB, Spark, SQLite, Polars, pandas, and Vortex query-engine
    integrations are prohibited as execution backends.
  - Dependencies/blockers: operator semantics, local adapter registry, output writers, execution
    envelope validators.
  - Ledger rule: ledger entry must enumerate admitted SQL grammar families and blocked families.

- [ ] GAR-RUNTIME-IMPL-5C Python DataFrame and query-builder workflow parity
  - Source: `GAR-RUNTIME-IMPL-4A`, `GAR-RUNTIME-IMPL-4B`, `GAR-RUNTIME-IMPL-4E`, Python README,
    Use Case Atlas.
  - Current state: Python wrapper and selected query-builder methods exist, but complete
    end-to-end generated/local/Vortex workflows and unsupported-method diagnostics are not yet
    ordinary user-grade coverage.
  - Next slice outcome: make one import path support generated, local file, and prepared/native
    Vortex workflows with select/filter/project/limit/aggregate/group/order/write where admitted.
  - User-visible surface: `import shardloom`, context/session object, `LazyFrame`, typed reports,
    getting-started docs, recipes, website use cases.
  - Implementation scope: Python builders, method admission matrix, CLI lowering, typed report
    accessors, examples, packaging smoke.
  - Evidence required: method admission, execution mode, engine mode, source/generated/prepared refs,
    output refs, correctness digest, certificate refs, no-fallback fields, claim gate.
  - Acceptance: a non-expert can run documented Python workflows and inspect evidence without
    architecture docs; unsupported methods are explicit and actionable.
  - Verification: Python unit/integration tests, clean-venv smoke, example smoke, compileall,
    use-case coverage, website readiness.
  - Non-goals: no pandas/Polars backend, notebook production claim, broad DataFrame parity claim, or
    public package upload.
  - Claim boundary: scoped local Python workflow runtime only.
  - Fallback boundary: Python orchestrates ShardLoom runtime and must not compute through external
    engines.
  - Dependencies/blockers: CLI runtime coverage, typed execution envelope, local outputs, generated
    source builders, Vortex lifecycle.
  - Ledger rule: ledger entry must include runnable Python snippets, admitted methods, and blocked
    methods.

- [ ] GAR-RUNTIME-IMPL-5D local input adapter runtime parity
  - Source: `GAR-RUNTIME-IMPL-4F`, `GAR-IOREUSE-1A`, universal compatibility scoreboard.
  - Current state: local CSV and selected local fixtures exist; JSONL/JSON, Parquet, Arrow IPC,
    Avro, ORC, Excel, database files, and unsupported formats are not uniformly represented by
    runtime SourceState adapters.
  - Next slice outcome: promote one local input format at a time into a SourceState adapter registry
    with deterministic blockers for unsupported formats.
  - User-visible surface: CLI/Python read APIs, capability/status views, benchmark rows, use cases.
  - Implementation scope: adapter registry, format detection, schema/dtype inference, fingerprints,
    row-count posture, parse/decode planning, diagnostics.
  - Evidence required: source format/location/fingerprint, SourceState id/digest, schema digest,
    row-count posture, parse/decode/materialization fields, Native I/O certificate posture,
    no-fallback fields.
  - Acceptance: each admitted local format can run at least one certified local workload or explicit
    smoke; unsupported formats produce blockers instead of silent fallback.
  - Verification: adapter snapshot tests, CLI/Python smoke per admitted format, unsupported format
    snapshots, benchmark row contract tests.
  - Non-goals: no object-store, database server, table/lakehouse, or universal adapter claim.
  - Claim boundary: local file adapter support per admitted format only.
  - Fallback boundary: adapters cannot use external engines to parse, plan, or execute user work.
  - Dependencies/blockers: dependency/license review, fixture data, SourceState schema, output
    correctness checks.
  - Ledger rule: ledger entry must list admitted formats, evidence refs, and unsupported formats.

- [ ] GAR-RUNTIME-IMPL-5E local output writers, replay proof, and fanout runtime
  - Source: `GAR-RUNTIME-IMPL-4G`, `GAR-IOREUSE-1C`, `GAR-IOREUSE-1D`, result-sink proof docs.
  - Current state: result-sink evidence exists, but local cross-format output and fanout are not
    complete as ordinary user workflows.
  - Next slice outcome: implement local OutputPlan-backed writes for admitted formats and a
    cross-format fanout smoke with replay/correctness proof.
  - User-visible surface: CLI/Python `write(...)`, recipes, benchmark fanout rows, website status.
  - Implementation scope: OutputPlan registry, writer adapters, schema compatibility, replay proof,
    output digest, fanout benchmark harness.
  - Evidence required: output plan id/digest, format/location/schema, write mode, output bytes/rows,
    replay status, output Native I/O certificate, no-fallback fields, claim gate.
  - Acceptance: one prepared/generated/local source can write multiple admitted local outputs;
    unsupported sinks block; replay proof is explicit where claimable.
  - Verification: writer smoke per format, replay tests, fanout benchmark smoke, output schema
    snapshots.
  - Non-goals: no object-store write, table commit, production sink claim, or performance claim.
  - Claim boundary: local output writer and fanout support per admitted format only.
  - Fallback boundary: output writers translate ShardLoom results and cannot invoke external compute.
  - Dependencies/blockers: OutputPlan schema, local adapter data, result replay harness, generated
    source/local/Vortex source evidence.
  - Ledger rule: ledger entry must list output formats, replay status, and blocked sinks.

- [ ] GAR-RUNTIME-IMPL-5F prepared/native Vortex runtime lifecycle
  - Source: `GAR-RUNTIME-IMPL-4H`, `GAR-RUNTIME-IMPL-4I`, Vortex provider docs, compute-flow
    reference.
  - Current state: prepared/native batch paths and scoped source-backed scan evidence exist; the
    complete prepare/read/write/reopen/scan/pushdown lifecycle is still not broad runtime support.
  - Next slice outcome: make local Vortex artifacts a first-class runtime path with explicit
    preparation, write/reopen, scan, pushdown, materialization/decode, and output evidence.
  - User-visible surface: CLI/Python Vortex workflows, benchmark rows, compute-flow docs, status
    matrix.
  - Implementation scope: Vortex preparation manager, artifact registry, scan request builder,
    source-backed scan adapter, pushdown admission, local output integration.
  - Evidence required: Vortex artifact ref/digest, preparation timing, write/reopen/scan timing,
    pushed-down filters/projections/limits, encoded predicate fields, materialization/decode fields,
    no-fallback fields.
  - Acceptance: prepared/native rows are clearly separated from compatibility import rows; supported
    pushdown avoids unnecessary output-column reads; unsupported pushdown blocks deterministically.
  - Verification: prepared/native batch smoke, source-backed scan tests, filter/projection/limit
    smoke, benchmark harness contract tests.
  - Non-goals: no object-store Vortex artifact, blanket encoded-native claim, or performance claim.
  - Claim boundary: declared local Vortex artifact workflows only.
  - Fallback boundary: Vortex array/scan/source/sink APIs may be native providers; Vortex
    query-engine integrations may not execute unsupported work.
  - Dependencies/blockers: Vortex dependency/version gate, provider boundary, SourceState/
    PreparedState ids, output evidence.
  - Ledger rule: ledger entry must include artifact lifecycle evidence and blocked Vortex paths.

- [ ] GAR-RUNTIME-IMPL-5G physical operator, function, and encoded-kernel coverage
  - Source: `GAR-RUNTIME-IMPL-4D`, `GAR-RUNTIME-IMPL-4J`, RFC 0015, RFC 0016, RFC 0021.
  - Current state: selected residual-native operators exist; broad type/null/string/date/decimal,
    join/window/top-k, fused, and encoded-kernel coverage remains incomplete.
  - Next slice outcome: promote operator families one at a time with decoded-reference correctness,
    unsupported diagnostics, and encoded-kernel admission where available.
  - User-visible surface: CLI/Python/SQL/DataFrame workflows, benchmark rows, capability matrix.
  - Implementation scope: expression IR, scalar/aggregate operators, join/window/top-k operators,
    type coercion, null/string/date policy, encoded kernel registry, blockers.
  - Evidence required: operator/function family, input/output schema, type/null policy, encoding id,
    decoded/materialized flags, correctness digest, encoded-native claim flag, no-fallback fields.
  - Acceptance: each supported operator family has success tests, edge-case tests, unsupported
    diagnostics, and correctness evidence; unsupported encodings block deterministically.
  - Verification: unit/property/correctness tests, fixture manifest checks, encoded-kernel tests,
    benchmark smoke per family.
  - Non-goals: no arbitrary UDFs, ANSI parity, blanket encoded-native claim, or performance claim.
  - Claim boundary: operator/function/encoding-pair support only.
  - Fallback boundary: external engines may be test oracles only, never runtime evaluators.
  - Dependencies/blockers: semantic fixture corpus, expression registry, benchmark row schema,
    decoded-reference harness.
  - Ledger rule: ledger entry must list promoted families, type/null behavior, and blockers.

- [ ] GAR-RUNTIME-IMPL-5H evidence envelope, evidence levels, and claim validators
  - Source: `GAR-RUNTIME-IMPL-4K`, `GAR-PERF-2A`, release readiness metadata, benchmark publishing
    policy.
  - Current state: reports expose many useful fields, but CLI, Python, benchmark, website, and
    release gates can still diverge as runtime surfaces expand.
  - Next slice outcome: add a versioned execution-envelope schema, evidence levels, and validators
    that every runtime path must satisfy.
  - User-visible surface: CLI JSON, Python typed reports, benchmark artifacts, website evidence,
    release readiness.
  - Implementation scope: shared schema, report adapters, typed aliases/migrations, readiness
    checks, website renderer, benchmark completeness gate.
  - Evidence required: execution/engine/evidence mode, source/generated/output refs, certificate
    refs, materialization/decode refs, no-fallback fields, claim gate, evidence level.
  - Acceptance: missing fallback/certificate/claim fields fail validation; `minimal_runtime` cannot
    become claim-grade by accident; report-only rows cannot masquerade as runtime support.
  - Verification: schema contract tests, release readiness metadata, benchmark completeness,
    website readiness, Python typed-report tests.
  - Non-goals: no runtime capability upgrade from schema work alone.
  - Claim boundary: evidence standardization and claim gating only.
  - Fallback boundary: every envelope exposes `fallback_attempted=false` and
    `external_engine_invoked=false` or fails.
  - Dependencies/blockers: stable field names, compatibility aliases, Python report migration,
    benchmark/website validators.
  - Ledger rule: ledger entry must record schema version, migrated surfaces, and validation failures
    now blocked.

- [ ] GAR-RUNTIME-IMPL-5I optimizer, session runtime, reuse, and buffer-pool promotion
  - Source: `GAR-RUNTIME-IMPL-4L`, `GAR-PERF-2B`, `GAR-PERF-2F`, `GAR-PERF-2G`,
    `GAR-IOREUSE-1`.
  - Current state: optimizer traces, source-state reuse, and batch/session evidence exist in scoped
    forms; ordinary workflows do not yet have a reusable session/cache lifecycle.
  - Next slice outcome: implement a scoped in-process session with optimizer trace, SourceState/
    VortexPreparedState/OutputPlan reuse, invalidation, and buffer reuse evidence.
  - User-visible surface: CLI batch/session command, Python context/session, explain output,
    benchmark timing rows.
  - Implementation scope: session lifecycle, optimizer rule registry, cache keys/fingerprints,
    invalidation policy, buffer-pool hooks, explicit close/cleanup.
  - Evidence required: session id, optimizer rules admitted/applied/blocked, before/after plan
    digests, cache hit/miss, reuse digest/reason, invalidation reason, buffer reuse count,
    no-fallback fields.
  - Acceptance: repeated admitted workflows reuse state safely; stale source/schema/plan changes
    invalidate cache; optimizer decisions are explainable and semantics-preserving.
  - Verification: optimizer snapshot tests, session smoke, invalidation tests, source/prepared/output
    reuse tests, benchmark contract tests.
  - Non-goals: no daemon/service, distributed cache, hidden fast mode, or performance claim.
  - Claim boundary: scoped in-process reuse and explainable optimization only.
  - Fallback boundary: optimizer/session/cache cannot change provider to an external engine.
  - Dependencies/blockers: fingerprint contract, plan digest stability, cache cleanup policy,
    envelope validators.
  - Ledger rule: ledger entry must list admitted optimizer rules, reuse artifacts, and invalidation
    rules.

- [ ] GAR-RUNTIME-IMPL-5J benchmark publishing, profile, and claim-grade refresh gate
  - Source: `GAR-RUNTIME-IMPL-4M`, `GAR-BENCH-PUB-1`, benchmark publishing runbook.
  - Current state: benchmark publishing has a structured artifact model, but every runtime
    promotion still needs fresh, profile-scoped evidence and public website/docs rendering.
  - Next slice outcome: require a current benchmark/correctness/evidence artifact for every
    promoted runtime path and block stale or incomplete public claims.
  - User-visible surface: website benchmarks, docs/benchmarks, status page, release readiness.
  - Implementation scope: artifact freshness checker, profile matrix, runtime claim matrix,
    benchmark page ingestion, release validators.
  - Evidence required: benchmark profile/environment, scenario coverage, lane status, correctness
    refs, certificate refs, no-fallback fields, claim gate.
  - Acceptance: promoted paths are not presented publicly without current evidence; missing
    required lanes/scenarios are visible and block claim-grade status.
  - Verification: benchmark artifact completeness checker, website readiness, release readiness,
    traditional benchmark harness tests.
  - Non-goals: no performance/superiority/Spark-replacement claim.
  - Claim boundary: workload-scoped local benchmark evidence only.
  - Fallback boundary: external baseline lanes cannot satisfy ShardLoom-native evidence.
  - Dependencies/blockers: benchmark manifest schema, runtime envelope validators, scenario
    fixtures, website renderer support.
  - Ledger rule: ledger entry must include artifact refs, profile, freshness, and public claim
    status.

- [ ] GAR-RUNTIME-IMPL-5K object-store read runtime admission
  - Source: `GAR-RUNTIME-IMPL-4N`, `GAR-COMPAT-1C`, `GAR-SCALE-1E`,
    `docs/architecture/object-store-request-planner.md`.
  - Current state: object-store planning/report-only surfaces exist; runtime reads, credentials,
    network policy, and provider proofs are blocked.
  - Next slice outcome: implement provider URI parse, effect/credential policy, optional listing,
    byte-range/full-file read, local cache boundary, and SourceState evidence in an approved
    emulator or public no-credential fixture profile.
  - User-visible surface: CLI/Python object-store diagnostics, capability/status pages, use cases.
  - Implementation scope: provider abstraction, policy gate, credential redaction, request planner,
    byte-range adapter, cache boundary, emulator/public-fixture tests.
  - Evidence required: provider/profile, credential/network status, object version/ETag, byte
    ranges, SourceState id, Native I/O certificate, no-fallback fields.
  - Acceptance: public and authenticated read gates are separate; no network probe or credential
    resolution runs by default; unsupported providers fail closed.
  - Verification: policy tests, mocked/emulator read smoke, SourceState snapshot tests, release
    readiness, website status checks.
  - Non-goals: no object-store write, table commit, production object-store claim, or managed
    platform claim.
  - Claim boundary: provider/profile-specific technical-preview read proof only.
  - Fallback boundary: storage provider access does not authorize external query execution.
  - Dependencies/blockers: security/effect policy, provider test harness, dependency/license review,
    emulator or public no-credential fixture.
  - Ledger rule: ledger entry must record provider, credential posture, proof refs, and blocked
    providers.

- [ ] GAR-RUNTIME-IMPL-5L object-store write and table/lakehouse operation ladder
  - Source: `GAR-RUNTIME-IMPL-4O`, `GAR-COMPAT-1D`, `GAR-SCALE-1E`.
  - Current state: object-store writes, table metadata/snapshot scans, append, merge/delete, commit,
    rollback, and catalog integration are blocked or report-only.
  - Next slice outcome: after read proof, implement staged write/commit/recovery in an approved
    profile, then one fixture-backed table metadata/snapshot operation and one append or commit
    rehearsal where admitted.
  - User-visible surface: table/object-store capability views, CLI/Python diagnostics, status/use
    cases, scale benchmark rows.
  - Implementation scope: write staging, commit protocol, idempotency, cleanup/retry, table metadata
    adapter, snapshot reader, manifest writer or commit rehearsal.
  - Evidence required: provider/profile, table format, snapshot id, manifest/data-file counts,
    commit protocol/status, rollback/cleanup status, idempotency key, no-fallback fields.
  - Acceptance: object-store read/write/commit and table metadata/read/append/commit are separate
    gates; fixture proof does not imply production lakehouse support.
  - Verification: policy tests, emulator write smoke, table fixture tests, commit rehearsal smoke,
    unsupported diagnostics, release readiness.
  - Non-goals: no blanket S3/GCS/ADLS support, production Iceberg/Delta/Hudi claim, catalog service,
    or production table claim.
  - Claim boundary: provider/table-format operation in declared fixture/profile only.
  - Fallback boundary: no external catalog, lakehouse engine, or query engine executes work.
  - Dependencies/blockers: object-store read proof, commit/recovery policy, table fixtures,
    dependency/license review, idempotency evidence.
  - Ledger rule: ledger entry must list provider, table format, operation, and blocked behaviors.

- [ ] GAR-RUNTIME-IMPL-5M scale-grade local execution runtime
  - Source: `GAR-RUNTIME-IMPL-4P`, `GAR-SCALE-1`, RFC 0014, RFC 0016, RFC 0017.
  - Current state: scale contracts and evidence fields exist, but larger-than-memory,
    split-parallel, spill, shuffle, retry, and idempotent output commit runtime are not claimable.
  - Next slice outcome: implement a declared-resource local scale profile with SplitManifest,
    bounded memory checks, per-split execution, spill/backpressure where admitted, one shuffle
    family, retry/idempotency, and output commit evidence.
  - User-visible surface: scale benchmark profiles, CLI/Python execution envelopes, status page.
  - Implementation scope: split scheduler, memory budget, spill manager, shuffle plan, retry/
    cancellation/recovery, output commit status, scale benchmark rows.
  - Evidence required: scale profile/status, real data volume, split/file/partition counts,
    memory/spill/shuffle fields, retry/idempotency, output commit status, correctness digest,
    no-fallback fields.
  - Acceptance: larger-than-memory and split-parallel claims require real bytes and correctness
    proof; synthetic metadata cannot become runtime scale claim.
  - Verification: split manifest tests, local stress smoke, spill/backpressure tests, shuffle
    correctness tests, retry/idempotency tests, scale benchmark contract tests.
  - Non-goals: no literal any-volume, Spark replacement, distributed runtime, or object-store scale
    claim without separate proof.
  - Claim boundary: declared local resource envelope only.
  - Fallback boundary: external engines are baselines/oracles only.
  - Dependencies/blockers: SourceState split metadata, operator coverage, spill storage policy,
    shuffle correctness fixtures, output commit proof.
  - Ledger rule: ledger entry must include resource envelope, data volume, and scale claim status.

- [ ] GAR-RUNTIME-IMPL-5N live, hybrid, control-plane, and distributed-runtime promotion
  - Source: `GAR-RUNTIME-IMPL-4Q`, RFC 0034, RFC 0035, `GAR-SCALE-1F`.
  - Current state: batch has local evidence; live/hybrid, REST/event APIs, remote workers, and
    distributed execution are scoped, blocked, or report-only.
  - Next slice outcome: implement engine-mode diagnostics, a local in-memory live/hybrid fixture if
    admitted, opt-in loopback control-plane lifecycle, and fail-closed distributed worker blockers.
  - User-visible surface: CLI/Python engine-mode status, optional local API, compute-flow, website
    status/use cases.
  - Implementation scope: engine-mode admission, local control-plane lifecycle, fixture scheduler,
    API schema, blocker diagnostics, small-result boundary.
  - Evidence required: engine mode, control-plane invoked flag, live/hybrid state, checkpoint/state
    posture, network policy, remote-worker invoked status, no-fallback fields.
  - Acceptance: labels cannot imply unsupported runtime; remote execution never runs accidentally;
    local API is opt-in, loopback-scoped, and evidence-backed.
  - Verification: engine-mode contract tests, fixture workflow tests, API/blocker tests, website
    readiness, release readiness.
  - Non-goals: no production REST service, daemon, broker/state-store runtime, remote workers,
    distributed claim, or exactly-once claim.
  - Claim boundary: fixture/local control-plane technical preview only.
  - Fallback boundary: remote APIs cannot trigger external compute.
  - Dependencies/blockers: lifecycle/security policy, evidence envelope, local API schema,
    loopback-only network guard, distributed blocker diagnostics.
  - Ledger rule: ledger entry must record API surface and blocked live/hybrid/distributed behavior.

- [ ] GAR-RUNTIME-IMPL-5O adapters, databases, UDFs, extensions, and effectful operations
  - Source: `GAR-RUNTIME-IMPL-4R`, RFC 0011, RFC 0023, adapter/governance docs.
  - Current state: databases/warehouses, REST/Flight/ADBC, wrappers/connectors, UDFs, plugins,
    LLM/API/embedding/vector effects, and extension execution are report-only or blocked.
  - Next slice outcome: implement local SQLite import/export if admitted, typed adapter manifests,
    extension inspection, one pure deterministic local scalar UDF fixture if approved, and
    fail-closed diagnostics for networked/effectful paths.
  - User-visible surface: capability views, Python/CLI adapter and extension commands, use cases,
    website status.
  - Implementation scope: connector registry, credential/effect policy, local fixture adapter,
    extension manifest schema, UDF admission, sandbox/effect blockers.
  - Evidence required: connector/extension id/version/digest, credential/network/effect status,
    import/export direction, UDF type/determinism/null contract, runtime flags, no-fallback fields.
  - Acceptance: external systems are never fallback engines; users can inspect adapters/extensions
    safely; effectful operations block by default; admitted UDFs are local, deterministic, typed,
    and evidence-backed.
  - Verification: SQLite/local fixture smoke if admitted, manifest validation tests, UDF blocker
    tests, unsupported network diagnostics, capability snapshots, release readiness.
  - Non-goals: no query pushdown, warehouse execution, arbitrary Python execution, network effects,
    LLM/API calls, plugin marketplace, or production UDF sandbox claim.
  - Claim boundary: scoped local import/export, inspection, or deterministic UDF fixture only.
  - Fallback boundary: adapters/extensions/UDFs must not delegate compute to external engines or
    services.
  - Dependencies/blockers: sandbox/security review, manifest schema, credential/effect policy,
    fixture data, dependency/license review.
  - Ledger rule: ledger entry must separate admitted local behaviors from denied effects.

- [ ] GAR-RUNTIME-IMPL-5P Foundry dev-stack generated-output and transform proof
  - Source: `GAR-COMMERCIAL-1E`, `GAR-IOREUSE-1G`, Foundry proof docs.
  - Current state: Foundry proof remains local/style-only or report-only; no production Foundry
    runtime/package/certified claim exists.
  - Next slice outcome: implement a personal dev-stack proof that imports the local package,
    resolves the CLI, runs source-free generated output and one staged-input transform, writes a
    result dataset and evidence dataset through Foundry-style output APIs, and preserves blocked
    flags.
  - User-visible surface: Foundry proof docs, examples, capability/status pages, release readiness.
  - Implementation scope: local Foundry-style transform wrapper, generated-source workflow,
    staged-input workflow, evidence dataset writer, runtime flag reporting.
  - Evidence required: input/output dataset counts, generated-source certificate, output Native I/O
    certificate, Foundry runtime/compute/Spark invoked flags, staged bytes, no-fallback fields.
  - Acceptance: Foundry can orchestrate a local proof without Spark fallback; evidence dataset
    output is mandatory; direct S3/object-store writes are not used.
  - Verification: local Foundry-style smoke, proof doc checks, release readiness metadata, website
    status checks.
  - Non-goals: no Foundry production support, package publication, marketplace listing, certified
    Foundry claim, or direct object-store path.
  - Claim boundary: local/dev-stack proof only.
  - Fallback boundary: Foundry/Spark compute cannot be reported as ShardLoom execution.
  - Dependencies/blockers: local package proof, generated-source runtime, output evidence writer.
  - Ledger rule: ledger entry must include proof commands, output/evidence refs, and blocked claims.

- [ ] GAR-RUNTIME-IMPL-5Q final public technical-preview usability and website learning gate
  - Source: `GAR-RUNTIME-IMPL-4S`, `GAR-DOCS-1`, `GAR-WEB-ATLAS-1`, public-preview readiness,
    package-channel matrix.
  - Current state: repo, website, and docs are strong, but final usability requires clean install
    proof, examples, website/status parity, benchmark interpretation, security/legal/release checks,
    and a non-expert learning path after runtime slices land.
  - Next slice outcome: run a no-publication technical-preview rehearsal from clean checkout/local
    artifact through CLI/Python workflows, unsupported diagnostics, benchmarks, website/status,
    SECURITY/LICENSE/NOTICE checks, and release metadata.
  - User-visible surface: README, docs/getting-started, website Field Guide/Use Case Atlas/status,
    package metadata, release report.
  - Implementation scope: clean venv install/run script, package dry-run, example smoke matrix,
    benchmark artifact completeness, website build/readiness, security/legal checks, docs link
    validation.
  - Evidence required: install/uninstall commands, smoke outputs, supported/blocked workflow matrix,
    benchmark manifest, website readiness report, package metadata, no-fallback fields.
  - Acceptance: a non-expert can install locally, run admitted workflows, inspect evidence, and see
    unsupported paths without reading phase-plan internals; website pages explain current runtime
    state without overclaiming.
  - Verification: clean venv smoke, cargo fmt/clippy/tests, Python compileall/tests, website
    readiness, static asset validation, benchmark artifact completeness, `git diff --check`.
  - Non-goals: no public package upload, tag, production/platform/performance claim, Spark
    replacement claim, object-store/lakehouse/Foundry production claim, or hidden fast mode.
  - Claim boundary: public technical preview only with workload-scoped claims.
  - Fallback boundary: release gates fail if any supported workflow uses external fallback.
  - Dependencies/blockers: completion of admitted runtime slices, docs/website parity, benchmark
    artifact policy, security/legal checks.
  - Ledger rule: ledger entry must include the exact usability matrix, website readiness evidence,
    release-gate evidence, and remaining unsupported paths.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

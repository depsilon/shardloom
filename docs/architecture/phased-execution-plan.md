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
- `docs/architecture/object-store-request-planner.md`
  - Role: CG-10 request-planning, range/coalescing/scheduling/checkpoint/retry/commit evidence
    reference.
  - Status rule: object-store runtime work is represented by `GAR-0008`, `GAR-0028`, and `GAR-0031`.
- `docs/architecture/table-intelligence-layer.md`
  - Role: CG-9 schema/table/catalog/CDC/layout/compaction evidence reference.
  - Status rule: table/catalog runtime work is represented by `GAR-0020` and `GAR-0028`.
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

#### GAR-P1 - Core Runtime, Operators, And Execution Safety

- [ ] GAR-0012-B envelope status and distributed/object-store diagnostic propagation
  - Source: RFC 0012; diagnostics normalization backlog; object-store request planner.
  - Current state: envelope status derives from diagnostics for current paths; distributed and
    object-store paths need propagation before runtime work.
  - Next slice outcome: tests and report fields proving blocked distributed/object-store diagnostics
    survive JSON/text/Python boundaries.
  - User-visible surface: CLI typed envelope, Python result view.
  - Implementation scope: output envelope, CLI renderer, Python typed model tests.
  - Evidence required: diagnostic refs, policy/no-fallback refs.
  - Acceptance: status matches highest severity and agents do not need to scrape human text.
  - Verification: typed envelope snapshot tests, Python tests, default GAR verification.
  - Non-goals: no object-store or distributed runtime.
  - Fallback/claim boundary: unsupported paths remain explicit and not claim-grade.
  - Dependencies/blockers: GAR-0008 blocker matrix.
- [ ] GAR-0013-A streaming runtime capability and unsupported diagnostics
  - Source: RFC 0013; streaming plan docs; Vortex runtime utilization audit.
  - Current state: streaming plan/backpressure contracts exist; full streaming runtime and
    object-store streaming reads are report-only.
  - Next slice outcome: capability matrix for local streaming, object-store streaming reads, zero-copy,
    zero-decode, and backpressure states.
  - User-visible surface: CLI streaming plan, capability view, docs.
  - Implementation scope: streaming report fields, CLI output, tests.
  - Evidence required: diagnostic refs, materialization/decode refs, policy/no-fallback refs.
  - Acceptance: every streaming family has support status and blocked paths emit deterministic
    diagnostics.
  - Verification: streaming plan snapshot tests, default GAR verification.
  - Non-goals: no object-store streaming runtime or broker.
  - Fallback/claim boundary: streaming runtime claims stay not claim-grade.
  - Dependencies/blockers: object-store provider gate for remote streaming.
- [ ] GAR-0014-A spill/OOM enforcement promotion gate
  - Source: RFC 0014; spill reservation lifecycle integration; workspace feature matrix.
  - Current state: memory admission and synthetic/local constraints exist; broad runtime spill/OOM
    enforcement is not production-gated.
  - Next slice outcome: gate that names required evidence for reservation release, native spill I/O,
    cleanup, allocator integration, and fail-before-OOM behavior.
  - User-visible surface: CLI memory/runtime plan, release readiness gate.
  - Implementation scope: memory gate report, CLI output, contract tests.
  - Evidence required: memory policy refs, no-fallback refs, security/path-safety refs for spill
    artifacts.
  - Acceptance: unsupported spill/OOM paths report blocked evidence without executing writes.
  - Verification: memory gate tests, runtime exploit/path-safety tests if files are touched.
  - Non-goals: no allocator or spill I/O implementation.
  - Fallback/claim boundary: no production memory claim.
  - Dependencies/blockers: release security/path-safety gates.
- [ ] GAR-0016-A adaptive execution and runtime-filter report-only gate
  - Source: RFC 0016; dynamic work shaping; performance attribution docs.
  - Current state: adaptive execution/runtime filters/skew handling are represented as planning
    concepts, not runtime behavior.
  - Next slice outcome: gate listing prerequisites for runtime filters, skew decisions, adaptive
    parallelism, and compaction writes.
  - User-visible surface: CLI optimizer/adaptive plan, docs.
  - Implementation scope: optimizer report fields, CLI output, tests.
  - Evidence required: benchmark refs only for supported lanes; diagnostic and no-fallback refs for
    report-only lanes.
  - Acceptance: runtime-adaptive requests cannot be mistaken for executed behavior.
  - Verification: optimizer plan tests, benchmark contract tests if metadata changes.
  - Non-goals: no adaptive runtime, runtime filters, or compaction writes.
  - Fallback/claim boundary: `support_status=report_only`.
  - Dependencies/blockers: operator/runtime evidence from GAR-0027.
- [ ] GAR-0017-A retry, cancellation, and recovery execution gate
  - Source: RFC 0017; object-store request planner; operational evidence policy hardening.
  - Current state: retry/cancellation/commit planning exists; broad execution is not enabled.
  - Next slice outcome: gate that separates request validation, cancellation signal, retry allowed,
    checkpoint write, cleanup, and commit execution.
  - User-visible surface: CLI retry/recovery/object-store reports, diagnostics.
  - Implementation scope: report fields, CLI output, tests.
  - Evidence required: policy/no-fallback refs, idempotency refs, checkpoint/commit evidence if
    execution is admitted.
  - Acceptance: default gate denies execution and gives deterministic blockers.
  - Verification: retry gate tests, object-store planner tests.
  - Non-goals: no retries, checkpoint writes, cleanup, or commits.
  - Fallback/claim boundary: no fault-tolerance runtime claim.
  - Dependencies/blockers: object-store provider gate and credential policy.
- [ ] GAR-0018-A live profiling and runtime introspection report
  - Source: RFC 0018; operational evidence policy hardening; benchmark-suite catalog.
  - Current state: observability schemas exist; live profiling/distributed introspection are not
    runtime features.
  - Next slice outcome: report-only introspection artifact for local benchmark spans, unsupported
    live profiling, and distributed runtime blockers.
  - User-visible surface: CLI observability plan, benchmark metadata, docs.
  - Implementation scope: observability report fields, benchmark metadata, tests.
  - Evidence required: benchmark refs and diagnostic/no-fallback refs.
  - Acceptance: introspection fields distinguish measured local spans from unsupported live profiling.
  - Verification: observability snapshot tests, benchmark contract tests.
  - Non-goals: no live profiler, collector, distributed tracing backend, or dependency.
  - Fallback/claim boundary: no production profiling claim.
  - Dependencies/blockers: benchmark attribution contract.
- [ ] GAR-0021-A approximate aggregate and sketch function admission
  - Source: RFC 0021; capability-certification sequencing; RFC 0032.
  - Current state: approximate/sketch requirements are documented; registry/state/update kernels are
    not implemented broadly.
  - Next slice outcome: admission/report contract for approximate aggregate registry entries, sketch
    state, merge, serialization, encoded-aware update kernels, exact-reference fixtures, error
    distribution benchmarks, and certificates.
  - User-visible surface: CLI capability discovery, function registry report, docs.
  - Implementation scope: expression/function report types, CLI output, tests.
  - Evidence required: correctness fixture refs, benchmark refs, execution certificate refs, Native
    I/O refs, no-fallback refs.
  - Acceptance: approximate/sketch functions are either admitted with evidence requirements or
    deterministically unsupported.
  - Verification: function registry tests, capability snapshot tests.
  - Non-goals: no sketch runtime implementation in this admission slice.
  - Fallback/claim boundary: no approximate aggregate accuracy/performance claim.
  - Dependencies/blockers: exact-reference fixture design.
- [ ] GAR-0021-B operator/kernel coverage expansion slice
  - Source: RFC 0021; physical operator kernel contracts; Vortex runtime utilization audit.
  - Current state: narrow kernels exist; full function/kernel coverage does not.
  - Next slice outcome: implement or block one concrete operator/kernel family with evidence.
  - User-visible surface: CLI kernel registry/capability report, benchmark row if executable.
  - Implementation scope: kernel registry, one kernel or deterministic blocker, tests.
  - Evidence required: correctness refs, benchmark refs if executed, execution certificate, Native
    I/O refs, materialization/decode refs, no-fallback refs.
  - Acceptance: selected family has support status, diagnostics, and evidence.
  - Verification: focused kernel tests, correctness fixtures, `cargo test --workspace --all-targets`.
  - Non-goals: no UDF/effectful execution.
  - Fallback/claim boundary: claim only the selected kernel family.
  - Dependencies/blockers: GAR-FLOW-2B blocker matrix.
- [ ] GAR-0038-A facade compatibility and legacy boundary matrix
  - Source: RFC 0038; top-level plan/execution facade docs; typed envelope docs.
  - Current state: top-level plan/execution facade exists, but SQL/DataFrame runtime, object-store
    runtime, writes, and legacy facade compatibility are not broad.
  - Next slice outcome: matrix that separates executable facade paths, report-only paths, legacy
    shims, and unsupported paths.
  - User-visible surface: CLI top-level command, Python client, docs.
  - Implementation scope: facade report, CLI/Python typed models, contract tests.
  - Evidence required: diagnostic refs and no-fallback refs; execution evidence only for supported
    paths.
  - Acceptance: facade does not route unsupported work into external engines or hidden fallback.
  - Verification: facade contract tests, Python tests, default GAR verification.
  - Non-goals: no SQL/DataFrame/object-store/write runtime.
  - Fallback/claim boundary: external engines remain baseline/oracle only.
  - Dependencies/blockers: GAR-0039 typed-envelope migration.

#### GAR-P2 - I/O, Tables, Output, And Lakehouse Semantics

- [ ] GAR-0004-A CDC and manifest transaction planning gate
  - Source: RFC 0004; table-intelligence layer; object-store request planner.
  - Current state: CDC planning exists; table/catalog metadata reads, object-store commits, manifest
    serialization, and transaction semantics are not executable broadly.
  - Next slice outcome: gate report for CDC read/write intent, manifest serialization status,
    transaction blockers, and unsupported commit diagnostics.
  - User-visible surface: CLI incremental/table plan, typed envelope, docs.
  - Implementation scope: table/intelligence report fields, CLI output, tests.
  - Evidence required: manifest refs, diagnostic/no-fallback refs, Native I/O refs only for admitted
    paths.
  - Acceptance: CDC and transaction requests are explicitly supported, report-only, or unsupported.
  - Verification: table intelligence tests, incremental plan tests, default GAR verification.
  - Non-goals: no metadata reads, data reads, object-store commits, or transaction writes.
  - Fallback/claim boundary: no CDC/table transaction claim.
  - Dependencies/blockers: GAR-0020 metadata gate and GAR-0028 commit gate.
- [ ] GAR-0005-A local Vortex reader/writer coverage slice
  - Source: RFC 0005; Vortex public API inventory; Vortex upstream alignment hardening.
  - Current state: scoped Vortex local read/write evidence exists; broad reader/writer support and
    general schema/encoding writes are limited.
  - Next slice outcome: add one local Vortex reader/writer schema or encoding lane, or record a
    deterministic upstream blocker.
  - User-visible surface: CLI Vortex output/scan commands, benchmark row, Native I/O certificate.
  - Implementation scope: `shardloom-vortex` read/write path, CLI handler, tests.
  - Evidence required: correctness refs, benchmark refs if measured, Native I/O certificate,
    materialization/decode refs, policy/no-fallback refs.
  - Acceptance: selected lane has certificate-backed status and unsupported neighbors are explicit.
  - Verification: focused Vortex I/O tests, benchmark smoke, `cargo test --workspace --all-targets`.
  - Non-goals: no object-store Vortex I/O or broad writer support.
  - Fallback/claim boundary: claim only the selected local Vortex lane.
  - Dependencies/blockers: upstream Vortex write/read API.
- [ ] GAR-0005-B object-store Vortex I/O and upstream write integration gate
  - Source: RFC 0005; RFC 0008; Vortex upstream alignment hardening.
  - Current state: object-store Vortex I/O and upstream write integration are not enabled.
  - Next slice outcome: report-only gate for object-store Vortex read/write provider requirements,
    credentials, idempotency, and unsupported diagnostics.
  - User-visible surface: CLI Vortex API inventory, object-store plan, release gate.
  - Implementation scope: inventory/gate report fields, tests.
  - Evidence required: policy/no-fallback refs and upstream API refs.
  - Acceptance: object-store Vortex paths cannot be claimed without provider, credential, Native I/O,
    and write evidence.
  - Verification: Vortex API inventory tests, object-store planner tests.
  - Non-goals: no network I/O, write execution, or dependency expansion.
  - Fallback/claim boundary: `support_status=report_only|unsupported`.
  - Dependencies/blockers: GAR-0008 provider gate and GAR-0019 credential policy.
- [ ] GAR-0007-A compatibility output writer capability matrix
  - Source: RFC 0007; RFC 0028; benchmark-suite catalog.
  - Current state: compatibility output writers such as Parquet, Arrow, Iceberg, and Delta are not
    broadly implemented as output sinks.
  - Next slice outcome: matrix for local compatibility output writer support, unsupported formats,
    dependency posture, and claim blockers.
  - User-visible surface: CLI output target plan, docs, release gate.
  - Implementation scope: output target report, docs, contract tests.
  - Evidence required: diagnostic/no-fallback refs, dependency policy refs.
  - Acceptance: every listed format has support status and dependency approval state.
  - Verification: output target tests, release readiness metadata tests.
  - Non-goals: no writer implementation.
  - Fallback/claim boundary: no lakehouse/output writer claim.
  - Dependencies/blockers: dependency/license policy.
- [ ] GAR-0007-B first local compatibility writer smoke
  - Source: GAR-0007-A; RFC 0007; RFC 0028.
  - Current state: writer support is matrixed/report-only after GAR-0007-A.
  - Next slice outcome: implement one local writer smoke lane, such as Parquet or Arrow IPC, with
    scoped evidence if dependency policy allows it.
  - User-visible surface: CLI output command, benchmark/output fixture, docs.
  - Implementation scope: writer adapter, output payload metadata, tests.
  - Evidence required: correctness refs, Native I/O refs, output fidelity refs, policy/no-fallback
    refs, dependency approval.
  - Acceptance: selected writer produces deterministic local output and unsupported formats stay
    explicit.
  - Verification: focused writer tests, fixture roundtrip test, `cargo test --workspace --all-targets`.
  - Non-goals: no Iceberg/Delta table commit or object-store write.
  - Fallback/claim boundary: claim only the selected local writer smoke lane.
  - Dependencies/blockers: GAR-0007-A and dependency/license gate.
- [ ] GAR-0020-A table/catalog metadata admission gate
  - Source: RFC 0020; table-intelligence layer; RFC 0041.
  - Current state: table intelligence reports exist; catalog/table metadata reads and data reads are
    not executed.
  - Next slice outcome: gate for catalog resolution, snapshot/manifest reads, table metadata reads,
    data reads, credential use, and external table-format dependencies.
  - User-visible surface: CLI table-intelligence plan, release readiness gate.
  - Implementation scope: table-intelligence report fields, CLI output, tests.
  - Evidence required: diagnostic/no-fallback refs, dependency policy refs, credential boundary refs.
  - Acceptance: default gate performs no I/O and gives evidence requirements.
  - Verification: table-intelligence tests, release readiness metadata tests.
  - Non-goals: no catalog I/O, table metadata I/O, data reads, or dependency additions.
  - Fallback/claim boundary: table/catalog support remains report-only.
  - Dependencies/blockers: GAR-0019 credentials and GAR-0008 object-store provider gate.
- [ ] GAR-0020-B delete/tombstone, CDC, and maintenance-write execution matrix
  - Source: RFC 0020; RFC 0004; table-intelligence layer.
  - Current state: delete/tombstone, CDC, compaction, and table maintenance are compatibility/planning
    surfaces.
  - Next slice outcome: execution matrix with support status, required fixtures, commit semantics,
    and unsupported diagnostics.
  - User-visible surface: CLI table plan, docs, capability view.
  - Implementation scope: table report fields, fixture metadata, tests.
  - Evidence required: correctness refs, commit refs, Native I/O refs for supported paths,
    no-fallback refs.
  - Acceptance: each operation has status and evidence gaps; unsupported paths do not execute.
  - Verification: table compatibility tests, fixture manifest tests.
  - Non-goals: no maintenance write runtime.
  - Fallback/claim boundary: no table-format execution claim.
  - Dependencies/blockers: GAR-0028 commit semantics.
- [ ] GAR-0028-A object-store and lakehouse commit semantics gate
  - Source: RFC 0028; object-store request planner; table-intelligence layer.
  - Current state: local Vortex staged-output and commit markers exist; object-store/table/lakehouse
    commit semantics are not executed broadly.
  - Next slice outcome: gate for object-store commit, table/catalog commit, sink commit, Foundry
    dataset transaction, upstream Vortex write API, and output-payload fidelity.
  - User-visible surface: CLI commit/output plan, release gate, docs.
  - Implementation scope: commit gate report, CLI output, tests.
  - Evidence required: commit protocol refs, Native I/O refs if executed, policy/no-fallback refs.
  - Acceptance: unsupported commit families are blocked with deterministic diagnostics and no writes.
  - Verification: commit gate tests, object-store planner tests, release readiness metadata tests.
  - Non-goals: no object-store writes, table commits, Foundry transactions, or upstream write calls.
  - Fallback/claim boundary: no production output/lakehouse claim.
  - Dependencies/blockers: GAR-0008, GAR-0020, and GAR-0036.

#### GAR-P3 - User Surfaces, APIs, Adapters, And Workflow

- [ ] GAR-0010-A Python API ergonomics and typed capability view
  - Source: RFC 0010; RFC 0037; typed envelope docs.
  - Current state: Python wrapper and typed views exist; ergonomic runtime APIs and notebook surfaces
    are not mature across planned features.
  - Next slice outcome: Python capability/diagnostic view that exposes supported, report-only, and
    unsupported states without executing unsupported work.
  - User-visible surface: Python package, docs, examples.
  - Implementation scope: Python client/models, docs, tests.
  - Evidence required: diagnostic/no-fallback refs; execution evidence only for already supported
    commands.
  - Acceptance: Python users can inspect mode/support/claim status without scraping CLI text.
  - Verification: Python tests or compileall, focused Rust contract tests, default GAR verification.
  - Non-goals: no broad DataFrame runtime or package publication.
  - Fallback/claim boundary: Python API does not expand runtime claims.
  - Dependencies/blockers: typed envelope migration from GAR-0039.
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
- [ ] GAR-0030-A universal harness execution gate
  - Source: RFC 0030; universal import/deployment baseline harness; RFC 0029.
  - Current state: universal harness is report-only; imported-plan execution needs capability,
    certificate, Native I/O, and no-fallback evidence.
  - Next slice outcome: gate that blocks harness execution until the evidence set is attached.
  - User-visible surface: CLI universal harness plan, docs, release gate.
  - Implementation scope: harness report fields, CLI output, tests.
  - Evidence required: capability refs, execution certificate refs, Native I/O refs, policy/no-fallback
    refs.
  - Acceptance: harness execution cannot be confused with environment/report readiness.
  - Verification: universal harness tests, release readiness metadata tests.
  - Non-goals: no harness execution, external engine invocation, or container publication.
  - Fallback/claim boundary: external baselines remain comparison-only.
  - Dependencies/blockers: GAR-0022 import/export status.
- [ ] GAR-0032-A SQL parser/binder report-only readiness
  - Source: RFC 0032; RFC 0010; rfc-coverage followthrough.
  - Current state: SQL capability concepts exist; SQL parser/binder/execution is not a broad runtime.
  - Next slice outcome: SQL text can be classified into parsed/bound/planned/unsupported diagnostics
    without execution.
  - User-visible surface: CLI capability output, docs, Python capability view.
  - Implementation scope: SQL capability report, diagnostic helpers, tests.
  - Evidence required: diagnostic/no-fallback refs.
  - Acceptance: SQL requests return deterministic support status and no runtime execution.
  - Verification: capability snapshot tests, diagnostic stability tests.
  - Non-goals: no SQL execution.
  - Fallback/claim boundary: `support_status=report_only|unsupported`.
  - Dependencies/blockers: parser dependency approval if a real parser is introduced.
- [ ] GAR-0032-B Python DataFrame method capability matrix
  - Source: RFC 0032; RFC 0037; Python client docs.
  - Current state: Python wrapper exists; broad DataFrame-like methods are not implemented.
  - Next slice outcome: method matrix for filters, projections, joins, aggregates, windows, writes,
    and unsupported diagnostics.
  - User-visible surface: Python API and docs.
  - Implementation scope: Python models/accessors, docs, tests.
  - Evidence required: diagnostic/no-fallback refs; runtime evidence only for existing supported
    methods.
  - Acceptance: every advertised method has a support status and claim boundary.
  - Verification: Python tests/compileall, default GAR verification.
  - Non-goals: no broad DataFrame execution.
  - Fallback/claim boundary: no DataFrame runtime claim.
  - Dependencies/blockers: GAR-0010 Python typed capability view.
- [ ] GAR-0032-C UDF and external-effect blocker matrix
  - Source: RFC 0032; RFC 0011; RFC 0019.
  - Current state: UDF/effectful operations are report-only/unsupported.
  - Next slice outcome: classify UDFs, API calls, LLM calls, embeddings, and external effects with
    permission/effect blockers.
  - User-visible surface: CLI capability view, docs, diagnostics.
  - Implementation scope: capability report, effect budget report, diagnostics, tests.
  - Evidence required: policy/security/no-fallback refs.
  - Acceptance: every external effect defaults to blocked and cannot execute without explicit policy.
  - Verification: effect budget tests, security policy tests.
  - Non-goals: no UDF, network, model, embedding, or external effect execution.
  - Fallback/claim boundary: no external-effect runtime claim.
  - Dependencies/blockers: GAR-0019 credential/policy and GAR-0023 sandbox.
- [ ] GAR-0032-D unstructured/media and universal adapter capability matrix
  - Source: RFC 0032; RFC 0033; RFC 0037.
  - Current state: unstructured/media/universal adapter surfaces are not executable broadly.
  - Next slice outcome: report-only matrix for documents, media, vectors, universal adapters, and
    source/sink metadata.
  - User-visible surface: CLI capability view, docs, Python view.
  - Implementation scope: capability report, docs, tests.
  - Evidence required: diagnostic/no-fallback refs and effect-policy refs.
  - Acceptance: no vector search, media extraction, or model call is implied by capability rows.
  - Verification: capability snapshot tests and default GAR verification.
  - Non-goals: no unstructured runtime.
  - Fallback/claim boundary: `support_status=report_only|unsupported`.
  - Dependencies/blockers: external-effect blocker matrix.
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
- [ ] GAR-0033-A ETL workflow and data-quality capability slice
  - Source: RFC 0033; user data workflow docs; benchmark-suite catalog.
  - Current state: local workflow surfaces exist; mature joins, aggregations, windows, data-quality
    APIs, object-store/table runtime, and production ETL certification are not broad.
  - Next slice outcome: ETL workflow matrix for supported local paths, report-only APIs, and
    unsupported object-store/table paths.
  - User-visible surface: CLI workflow plan, Python docs, examples.
  - Implementation scope: workflow report, Python view, tests.
  - Evidence required: correctness refs for supported local paths, diagnostic/no-fallback refs for
    unsupported paths.
  - Acceptance: production ETL claims are blocked unless all evidence is attached.
  - Verification: workflow/table planning tests, Python compileall if touched.
  - Non-goals: no production ETL runtime or publication.
  - Fallback/claim boundary: local workflow claims only for already certified paths.
  - Dependencies/blockers: operator/table/output GAR slices.
- [ ] GAR-0034-A live/hybrid fabric blocker and freshness gate
  - Source: RFC 0034; live/hybrid event API docs; operational evidence policy hardening.
  - Current state: live/hybrid engines and freshness/exactly-once claims are planning/report-only.
  - Next slice outcome: gate for broker/state-store dependencies, object-store execution, freshness,
    exactly-once, and baseline/oracle boundaries.
  - User-visible surface: CLI live/hybrid plan, docs, release gate.
  - Implementation scope: fabric report fields, diagnostics, tests.
  - Evidence required: policy/no-fallback refs and freshness evidence if any lane is admitted.
  - Acceptance: all live/hybrid runtime claims default to not claim-grade.
  - Verification: live/hybrid contract tests, release readiness metadata tests.
  - Non-goals: no broker, state store, object-store runtime, or streaming production behavior.
  - Fallback/claim boundary: baselines/oracles remain comparison-only.
  - Dependencies/blockers: GAR-0013 streaming and GAR-0008 object-store gates.
- [ ] GAR-0035-A REST server/runtime unsupported contract
  - Source: RFC 0035; execution mode protocol parity; typed envelope docs.
  - Current state: REST/Event/API contracts are documented; HTTP listener, remote execution,
    Flight/ADBC bridge, broker integration, and dependency-expanded server are not implemented.
  - Next slice outcome: unsupported/runtime-readiness contract that mirrors CLI/Python fields and
    blocks server claims.
  - User-visible surface: REST/OpenAPI docs/schema, release readiness gate.
  - Implementation scope: schema/docs, contract tests, release metadata.
  - Evidence required: protocol parity refs and diagnostic/no-fallback refs.
  - Acceptance: REST docs expose shared mode fields and deterministic unsupported diagnostics.
  - Verification: default GAR verification plus schema snapshot tests if generated.
  - Non-goals: no HTTP listener, remote execution, Flight/ADBC, broker, or server dependency.
  - Fallback/claim boundary: REST remains report-only/unsupported for runtime execution.
  - Dependencies/blockers: GAR-FLOW-3A parity report.
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
- [ ] GAR-0037-A wrapper and connector implementation registry
  - Source: RFC 0037; client wrapper architecture docs.
  - Current state: wrapper architecture is documented; generated clients, DB-API, SQLAlchemy, Ibis,
    dbt, Airflow, Dagster, Prefect, MCP, Flight, ADBC, and BI connectors are not implemented.
  - Next slice outcome: registry of wrappers/connectors with support status, transport, evidence,
    and unsupported diagnostics.
  - User-visible surface: docs, CLI capability view, Python package docs.
  - Implementation scope: registry/report docs, tests.
  - Evidence required: protocol parity refs and diagnostic/no-fallback refs.
  - Acceptance: connectors are not advertised as runtime-supported without implementation evidence.
  - Verification: docs/contract tests and Python compileall if models change.
  - Non-goals: no connector implementation.
  - Fallback/claim boundary: no wrapper ecosystem claim.
  - Dependencies/blockers: GAR-0035 REST/transport and GAR-0010 Python API.
- [ ] GAR-0039-A typed envelope migration and legacy field mirror closeout
  - Source: RFC 0039; typed command result envelope docs; agent contract pack.
  - Current state: typed output v2 exists; legacy flat `fields` mirror and some command families
    remain.
  - Next slice outcome: migrate one command family from legacy mirror reliance to typed refs or add
    deterministic blockers where migration is not ready.
  - User-visible surface: CLI JSON output, Python typed models, golden fixtures.
  - Implementation scope: CLI renderer/typed envelope, one command-family handler, Python model/test.
  - Evidence required: typed envelope snapshots and no-fallback refs.
  - Acceptance: migrated family exposes typed payloads without losing backward-compatible fields where
    still required.
  - Verification: typed envelope snapshot tests, Python tests, `cargo test --workspace --all-targets`.
  - Non-goals: no runtime behavior changes.
  - Fallback/claim boundary: output migration cannot imply new support.
  - Dependencies/blockers: command-family priority.
- [ ] GAR-0039-B golden fixtures, Foundry boundary fixture, and helper centralization
  - Source: RFC 0039; repo cleanup backlog; terminology consolidation backlog; diagnostics backlog.
  - Current state: some golden fixtures, Foundry boundary fixture, command/help registry,
    terminology mapping helpers, diagnostic constants, and report field helpers remain pending.
  - Next slice outcome: add one focused fixture/helper centralization slice with tests and no runtime
    behavior.
  - User-visible surface: CLI JSON snapshots, docs, agent contract.
  - Implementation scope: snapshot fixtures, helper module, tests.
  - Evidence required: snapshot refs and no-fallback refs.
  - Acceptance: helpers reduce duplication without changing public semantics.
  - Verification: focused snapshot tests, default GAR verification.
  - Non-goals: no command rename, public schema break, or runtime expansion.
  - Fallback/claim boundary: cleanup does not create support claims.
  - Dependencies/blockers: compatibility expectations for existing CLI outputs.

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

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

# ShardLoom Phased Execution Plan

## How To Maintain This File

- Keep actionable working items in `## Planned`.
- Keep detailed completed session blocks in
  `docs/architecture/phased-execution-completed-ledger.md`; do not place completed narrative here.
- Keep Planned ordered by current dependency and user value, not numeric CG order.
- Do not keep a separate Active section. The next autonomous work is the first unchecked Planned
  checkbox after this file has been reordered.
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
- When a maintainer-provided list, audit, attachment, benchmark finding, or review packet proposes
  new work, review each candidate before adding it here. Classify it as already addressed,
  accepted into a new checklist, merged into an existing checklist, v1 candidate pending
  feasibility, deferred beyond the current product scope, or rejected with a reason. Do not paste
  broad lists verbatim into Planned.
- Production-shift items must state whether they are `required_for_v1`,
  `v1_candidate_pending_feasibility`, `deferred_after_v1`, `documentation_only_for_v1`, or
  `unsupported_boundary_for_v1`. The v1 default is inclusion for anything feasible to complete with
  real runtime behavior, deterministic unsupported diagnostics, safety evidence, and release proof.
  Defer beyond v1 only when the item records a concrete reason such as unavailable external
  platform proof, unresolved safety/security design, missing protocol approval, or scope that would
  make v1 unverifiable.
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
  `deferred_after_v1`, `documentation_only_for_v1`, or `unsupported_boundary_for_v1` for
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
  `docs/release/release-architecture-tracker-gate.md`,
  `docs/release/final-release-rehearsal.md`, and `docs/release/*`.

Reference-doc rule: these files are evidence, guardrails, or inventories. They do not authorize
runtime behavior, support claims, dependency expansion, package publication, external effects, or
fallback execution unless a matching unchecked item below is completed with evidence and moved to
the ledger.

## Planned

Use this section for the next implementation sequence. Keep it ordered by dependency and user value.
The first unchecked checkbox is the next default autonomous slice.

Current autonomous execution order:

### Finished Product v1 Required Work

These items are the reviewed intake from the attached production-shift list dated June 13, 2026.
They are ordered as runtime/product closure first, API/schema stability second, release/package
channels near the end, and public-language cleanup throughout. Already-addressed rows remain
referenced in the completed ledger or existing release gates. Broad platform/runtime ambitions
should be included in v1 when they can be made real, safe, and evidence-backed; defer them only
with a recorded infeasibility reason, not merely because they are broad.

- [ ] `PROD-V1-2A` Result envelope, diagnostics, doctor, and API/schema stability gate.
  - Source: attached production-shift review sections 7 and 12; RFC 0012; RFC 0024;
    `docs/release/publication-api-schema-stability-gate.md`; current release-readiness blockers.
  - Current state: `shardloom.output.v2`, diagnostics, capability reports, and release schemas
    exist in scoped forms, while public API/schema compatibility windows, stable schema fixtures,
    signing/package identity decisions, and publication approval remain blocked.
  - Intake review: accepted schema/freezing work as second-order after v1 runtime scope; merge
    existing publication/API/schema stability gate rather than replacing it.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: control-plane applicable. Stable schemas must expose timing
    surfaces, evidence tiers, capillary/PulseWeave/dynamic decisions where they affect runtime
    evidence, and deterministic unsupported diagnostics without requiring effectful probes.
  - Execution checklist:
    - [ ] Decide whether legacy flat fields remain stable or enter a documented deprecation window.
    - [ ] Add stable schema files for output envelope, diagnostics, fallback status, route fields,
      evidence summary, claim summary, execution certificate, Native I/O certificate, capability
      report, package/release reports, and support bundles.
    - [ ] Add golden JSON fixtures and compatibility tests that fail when stable fields disappear
      or change semantics without migration policy.
    - [ ] Add Python accessor tests for every stable field and CLI text/JSON stability tests for
      user-facing and machine/agent commands.
    - [ ] Add diagnostic-code stability docs, diagnostic-code migration policy, compatibility
      windows, breaking-change approval policy, and migration-notes policy.
    - [ ] Add `shardloom doctor` v1 checks for CLI version, Python package version, package
      channel, feature support, Vortex support, local write support, no-fallback invariant, and
      environment details without effectful probes.
    - [ ] Add support bundle generation and redaction tests.
    - [ ] Unblock publication/API/schema gate rows only after real evidence exists; keep package
      identity, signing, checksums, SBOM, and publication approval blocked until release items
      close.
    - [ ] Move schema/API/doctor closeout and command evidence to the completed ledger.
  - Next outcome: stable v1 fields and diagnostics can be depended on by users and agents.
  - User-visible surface: Python objects, CLI JSON/text output, doctor command, support bundle,
    diagnostics docs, release reports, and API/schema gate.
  - Implementation scope: schema files, Python dataclasses/accessors, CLI output, doctor/support
    bundle scripts, release validators, docs, and tests.
  - Evidence required: golden fixtures, schema diff report, API compatibility report, diagnostic
    snapshots, redaction proof, and no-fallback/effect-free discovery proof.
  - Acceptance: stable v1 schemas have fixtures and compatibility tests; doctor/support bundle are
    safe by default; blocked publication rows remain blocked until explicitly approved.
  - Verification: Python tests, release-script shard, API/schema gate, doctor/support-bundle tests,
    docs validation, and workspace gates for shared output code.
  - Non-goals: no package publication, no stable promise for experimental/internal schemas, no
    network/filesystem probing during doctor/capabilities beyond explicitly admitted local checks.
  - Claim boundary: may claim only v1 API/schema stability for declared stable surfaces.
  - Fallback boundary: diagnostics and doctor must report fallback status without invoking fallback
    engines.
  - Ledger rule: ledger entry must include stable schema list, compatibility windows, fixture refs,
    and blocked publication rows.
- [ ] `PROD-V1-2B` Correctness, conformance, and golden workflow closure for v1.
  - Source: attached production-shift review section 8; RFC 0015; admitted semantics matrix;
    golden workflows; docs/example execution tests; current production usability gate.
  - Current state: golden workflows and admitted semantics evidence exist, but v1 requires every
    supported operation/example to have fixture, digest, no-fallback, replay, and accessor coverage,
    while unsupported rows remain explicit.
  - Intake review: accepted the expanded correctness/conformance matrix; use external engines only
    as explicit test oracles where already allowed, never runtime fallback.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable. Correctness fixtures must cover dynamic admission
    boundaries, metadata-first answers/pruning, capillary split behavior where exposed, and
    timing/evidence separation without relying on performance claims.
  - Execution checklist:
    - [ ] Define the v1 correctness matrix from the scoped front-door, Vortex, source-state, and
      output-sink scope docs.
    - [ ] Ensure every v1-supported operation has fixture tests, semantic conformance rows,
      expected output digests, no-fallback proof, and Python accessor coverage where exposed.
    - [ ] Expand admitted semantics only for v1-supported behavior and keep unsupported rows
      explicit with deterministic blockers.
    - [ ] Add property tests for filters, projections, limits, supported joins, supported
      aggregates, supported sort/top-N, supported subqueries, supported string/temporal/decimal/
      binary functions, and supported output formats.
    - [ ] Add fuzz tests for SQL parsing subset, expression parsing, route selection, output
      writer policy, and manifest/reuse invalidation.
    - [ ] Add replay verification for all golden workflows, docs-example execution tests, Python
      README example tests, website example tests, and failure fixtures for unsupported operations.
    - [ ] Add tests proving unsupported operations never call fallback engines.
    - [ ] Move correctness matrix closeout and command evidence to the completed ledger.
  - Next outcome: v1 support is defined by executable correctness evidence, not doc claims.
  - User-visible surface: docs examples, README examples, Python API, CLI, golden workflows,
    admitted semantics matrix, release readiness.
  - Implementation scope: tests/fixtures, golden workflow definitions, admitted semantics matrix,
    docs-example runner, Python/CLI test coverage, validators.
  - Evidence required: fixture outputs, digests, replay reports, no-fallback proof, negative
    fallback tests, and conformance report.
  - Acceptance: all v1-supported behavior has deterministic correctness evidence; unsupported
    behavior is tested as unsupported; docs examples cannot drift.
  - Verification: Python/Rust tests, golden workflow validator, admitted semantics matrix,
    docs-example proof, release-script shard, and workspace gates.
  - Non-goals: no broad SQL/DataFrame parity, no unsupported operator promotion without decoded
    reference parity, no external runtime fallback.
  - Claim boundary: may claim correctness coverage only for declared v1 operations.
  - Fallback boundary: external engines, if used, are test oracles only and never execute
    ShardLoom runtime work.
  - Ledger rule: ledger entry must include matrix rows, fixture refs, docs-example proof, and
    residual unsupported rows.
- [ ] `PROD-V1-2C` Local memory, resource safety, cancellation, and cleanup closure.
  - Source: attached production-shift review section 9; RFC 0014; RFC 0017; memory/spill/OOM docs;
    output cleanup and prepared-state reuse contracts.
  - Current state: local/synthetic resource constraints and cleanup evidence exist, but broad
    production spill/OOM enforcement remains limited. v1 needs deterministic local resource budgets
    and cleanup for supported routes without claiming distributed spill production support.
  - Intake review: accepted local v1 resource safety as required; defer broad spill/distributed OOM
    production enforcement to future production/runtime items.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: strongly applicable. Resource safety should use dynamic admission
    by data size/shape, capillary bounded work units, PulseWeave bounded in-flight control where
    runtime work is coalesced, metadata-first preflight estimates, and separate hot route versus
    cleanup/proof evidence fields.
  - Execution checklist:
    - [ ] Define v1 memory/resource scope, resource budget config, max input guidance, max output
      or materialization guidance, and deterministic budget-exceeded diagnostics.
    - [ ] Implement or certify runtime memory reservation for v1 operators and release on success,
      error, cancellation, and cleanup paths.
    - [ ] Implement cancellation cleanup and partial-output cleanup for v1 routes and local writes.
    - [ ] Add bounded materialization policy tests, large local input smoke tests within v1 scope,
      and failure-injection tests.
    - [ ] Emit cleanup evidence and resource fields into route/certificate/support reports.
    - [ ] Move resource-safety closeout and residual spill/distributed blockers to the ledger.
  - Next outcome: local v1 routes fail before unsafe materialization/OOM where practical and clean
    up after errors.
  - User-visible surface: CLI/Python errors, diagnostics, doctor/support bundles, output manifests,
    release readiness, docs.
  - Implementation scope: memory/resource policy, route execution, output/prepared cleanup,
    diagnostics, tests, docs.
  - Evidence required: budget tests, cleanup tests, failure-injection reports, no-fallback proof,
    and resource/certificate fields.
  - Acceptance: v1 routes have deterministic budget/cancellation/cleanup behavior; broad spill and
    distributed OOM remain deferred unless separately implemented.
  - Verification: focused Rust/Python tests, failure-injection tests, release-script shard,
    support bundle tests, and workspace gates for runtime changes.
  - Non-goals: no full distributed spill runtime, no platform-level autoscaling/resource claim, no
    hidden external spill engine.
  - Claim boundary: may claim only local v1 resource safety and cleanup behavior.
  - Fallback boundary: resource pressure must fail explicitly, not delegate to external engines.
  - Ledger rule: ledger entry must list budget defaults, tested failures, cleanup evidence, and
    deferred spill/OOM scope.
- [ ] `PROD-V1-2D` Observability, supportability, and troubleshooting closure.
  - Source: attached production-shift review section 10; RFC 0012; release readiness reports;
    benchmark timing-surface contracts; diagnostic docs.
  - Current state: route timing, evidence, and claim fields exist across runtime/benchmark
    surfaces, but v1 supportability needs stable per-route/per-stage fields, resource/output/replay
    fields, route/capability/unsupported explanations, diagnostic-code troubleshooting docs, issue
    templates, and local support bundles.
  - Intake review: accepted local observability/supportability as required; defer OpenTelemetry,
    OpenLineage, network exporters, and remote support uploaders unless v1 explicitly supports
    them.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable. Observability must expose PulseWeave/capillary/dynamic
    decisions when they affect execution, preserve timing-surface/evidence-tier separation, and use
    metadata-first explanation paths that do not execute effectful operations.
  - Execution checklist:
    - [ ] Add stable per-route, per-stage, memory/resource, output/replay, certificate-ref, and
      timing-surface fields for v1-supported routes.
    - [ ] Add route explanation, capability explanation, and unsupported-path explanation output.
    - [ ] Add troubleshooting docs keyed by diagnostic code.
    - [ ] Add local support bundle and local debug bundle commands with redaction tests.
    - [ ] Add issue templates requesting command, JSON envelope, diagnostic code, route id,
      package/CLI version, OS/Python/Rust version, and fallback status.
    - [ ] Preserve local/no-network default behavior and explicitly defer telemetry/exporters.
    - [ ] Move observability/support closeout and command evidence to the completed ledger.
  - Next outcome: users and agents can diagnose v1 route behavior without hidden probes or
    external services.
  - User-visible surface: CLI/Python explain/capability/diagnostic output, docs, issue templates,
    support bundles, website support pages.
  - Implementation scope: diagnostics/explain/capability code, support-bundle scripts, docs/site,
    issue templates, tests.
  - Evidence required: golden output snapshots, redaction tests, issue-template validation,
    no-network/effect-free proof, and route field stability fixtures.
  - Acceptance: v1 diagnostics are stable and actionable; support bundles are local/redacted;
    unsupported paths explain blockers without running effects.
  - Verification: Python/CLI snapshot tests, release-script shard, support bundle redaction tests,
    docs/site validation, and workspace gates for shared diagnostics code.
  - Non-goals: no OpenTelemetry/OpenLineage/network exporter, no remote support upload, no
    effectful doctor probes.
  - Claim boundary: may claim only local supportability for declared v1 surfaces.
  - Fallback boundary: support tooling must not execute fallback engines or unsupported work.
  - Ledger rule: ledger entry must include stable field list, diagnostic docs, bundle command
    evidence, and deferred observability integrations.
- [ ] `PROD-V1-3A` Security, supply-chain, and CI/release-validation hardening for v1.
  - Source: attached production-shift review sections 13 and 14; RFC 0024; dependency/security
    gates; package-channel readiness matrix; CI workflow.
  - Current state: dependency/security, package-channel, release-readiness, and CI matrix gates
    exist, but v1 needs them aligned to the finished-product scope, OS/Python/Rust/feature matrix
    expectations, clean environment proof, artifact retention, and release evidence bundle
    upload. Package publication remains blocked until approval.
  - Intake review: accepted security/supply-chain and CI hardening; many base validators already
    exist and should be extended rather than rebuilt.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: mostly control-plane. CI/release gates must preserve evidence-tier
    separation and require runtime items to report dynamic/capillary/PulseWeave decisions before
    a support or production label can pass.
  - Execution checklist:
    - [ ] Make dependency audit, license classification, forbidden-fallback dependency absence,
      SBOM generation, checksum manifest, provenance, vulnerability scan, and package artifact
      scan required for v1 release readiness.
    - [ ] Decide signing policy and add signing workflow or explicit no-signing rationale.
    - [ ] Add or update release branch/tag protection docs, maintainer approval docs, security
      advisory process, supply-chain incident response, dependency update policy, rollback policy,
      and package yank/delete/deprecate policy.
    - [ ] Ensure no committed long-lived package upload tokens and prefer Trusted Publisher/OIDC
      for PyPI/TestPyPI package channels.
    - [ ] Harden CI matrix: full workspace validation, Python validation/build/package smoke,
      release scripts, OS matrix, Python 3.10-3.13 matrix, Rust MSRV validation, feature matrix,
      clean venv proof, clean Conda proof if in scope, docs-example proof, website-build proof,
      generated artifact sync proof, no-dirty-worktree proof, artifact retention, and release
      evidence bundle upload.
    - [ ] Move security/CI closeout and remaining blocked publication approvals to the completed
      ledger.
  - Next outcome: v1 release readiness depends on repeatable CI and supply-chain evidence, not
    local-only confidence.
  - User-visible surface: CI checks, release readiness reports, package-channel reports, security
    docs, release docs, repository governance.
  - Implementation scope: scripts, CI workflows, release/security docs, package-channel matrix,
    tests, and generated reports.
  - Evidence required: CI reports, security/audit reports, SBOM/checksum/provenance artifacts,
    package scan evidence, approval docs, and no-token proof.
  - Acceptance: v1 release gates fail without required security/supply-chain/CI evidence; package
    publication remains blocked until approvals and channel proof exist.
  - Verification: CI matrix contract, dependency/security gates, release validation evidence,
    package-channel readiness, release readiness, and relevant Python/Rust tests.
  - Non-goals: no package publication, no signing secret creation in repo, no container scanning
    unless containers are v1 scope.
  - Claim boundary: may claim only release-gate hardening until a public release is approved.
  - Fallback boundary: dependency gates must continue rejecting forbidden fallback dependencies.
  - Ledger rule: ledger entry must list hardened gates, required matrices, evidence artifacts, and
    unresolved approval/channel blockers.
- [ ] `PROD-V1-4A` Docs, website, install, and example productization after v1 scope stabilizes.
  - Source: attached production-shift review section 15; README; website Starlight/Astro sources;
    docs/getting-started; public status matrix; benchmark website.
  - Current state: README and website are refreshed around current evidence, but final v1 docs
    should be rebuilt around the stable product scope, install path, first workflow, evidence
    inspection, support matrix, examples, unsupported examples, and troubleshooting.
  - Intake review: accepted docs/website productization but sequence it after runtime/product scope
    items close enough to avoid another stale overhaul. Keep fresh timings, example Python uses,
    and logos; remove stale legacy/conflicting content rather than preserving it.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: mostly presentation/control-plane. Docs must explain
    metadata-first execution, timing-surface separation, evidence tiers, dynamic/capillary/
    PulseWeave choices where user-visible, and avoid implying performance claims without benchmark
    evidence.
  - Execution checklist:
    - [ ] Rewrite README around what ShardLoom does, v1 support, install, first workflow, evidence
      inspection, and unsupported scope.
    - [ ] Separate package-user install docs from source-checkout install docs; add uninstall,
      upgrade, troubleshooting, and support-bundle docs.
    - [ ] Add stable v1 examples: local CSV, local JSONL, local Parquet, local Vortex, prepare
      Vortex, warm prepared query, bounded collect, local output write, evidence inspection, and
      blocker inspection.
    - [ ] Add unsupported examples for broad SQL, unbounded collect, object-store if deferred,
      Foundry if deferred, and UDF/effect if deferred.
    - [ ] Generate supported/unsupported docs from machine-readable matrices.
    - [ ] Update website start page, support matrix, benchmark page, about page, field guide, and
      compute-flow pages to match v1 support and avoid comparative claims unless claim-grade.
    - [ ] Show package install commands only after package channels are live; otherwise keep
      source/local proof commands clearly labeled.
    - [ ] Add docs tests for every copy-paste command and wording validators for external-engine
      claim language.
    - [ ] Move docs/website productization closeout and command evidence to the completed ledger.
  - Next outcome: public docs become a coherent product guide for the scoped v1, not a mixture of
    historical architecture and current evidence.
  - User-visible surface: README, Python README, Starlight docs, Astro pages, benchmark website,
    install/getting-started docs, examples.
  - Implementation scope: docs, website-src, generated website output, examples, docs tests,
    validators, and release docs.
  - Evidence required: docs example execution, website build/readiness, static asset validation,
    wording validator, source/generated sync proof.
  - Acceptance: users can follow v1 docs end to end; unsupported surfaces are explicit; stale or
    conflicting legacy language is removed; benchmark page remains claim-safe.
  - Verification: docs-example tests, website sync/check/build/postbuild/readiness/static assets,
    release-script shard, public-language validator, and `git diff --check`.
  - Non-goals: no package install commands before channels are live; no unsupported platform docs
    marketed as v1 support; no performance superiority language.
  - Claim boundary: docs may claim only v1-supported workflows backed by validators and examples.
  - Fallback boundary: docs must state unsupported work fails explicitly and never fallback
    executes.
  - Ledger rule: ledger entry must list updated public surfaces, tested commands, generated
    artifacts, and any intentionally deferred docs.
- [ ] `PROD-V1-5A` Package-channel readiness and finished-product hard gate.
  - Source: attached production-shift review sections 16 and 17; RFC 0024;
    `docs/release/package-channel-readiness-matrix.json`;
    `scripts/check_release_readiness.py`; final release rehearsal and hard release gates.
  - Current state: package-channel matrices and hard release readiness exist, but public
    publication is blocked. v1 needs a final finished-product readiness aggregator that consumes
    runtime, schema, security, package, docs, benchmark, and approval evidence and fails closed
    without `--allow-blocked` for public release tags.
  - Intake review: accepted package/release channel order and finished-product gate; retain current
    package-channel evidence requirements and mark channels not included in v1 as
    `not_in_v1_scope` only after feasibility review records why they cannot be included.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: control-plane applicable. The finished-product gate must consume
    timing/evidence surface fields and require runtime support rows to have documented
    dynamic/capillary/PulseWeave decisions where applicable; package publication itself should not
    introduce runtime shortcuts.
  - Execution checklist:
    - [ ] Ensure public package publication is blocked until v1 runtime scope, API/schema gate,
      security/provenance, docs, and release validation are ready.
    - [ ] Approve package identities and keep internal Rust crates unpublished unless extracted
      into stable public protocol/client crates.
    - [ ] Implement GitHub pre-release artifacts: source archive, CLI binaries, wheel, sdist,
      checksums, SBOM, provenance, and release notes.
    - [ ] Add TestPyPI upload, clean install, uninstall, and smoke proof before PyPI proof.
    - [ ] Add PyPI clean install, uninstall, and smoke proof only after TestPyPI passes and
      approval exists.
    - [ ] Review Homebrew, Scoop, winget, conda-forge, and GHCR for v1 feasibility; include any
      feasible channel with real artifacts/proof and mark `not_in_v1_scope` only with a recorded
      reason.
    - [ ] Update package-channel matrix row by row with install/uninstall transcript, clean
      environment proof, smoke proof, SBOM, checksum, provenance, rollback/yank/delete/deprecate
      policy, authorization proof, and maintainer approval for every ready channel.
    - [ ] Change `python/pyproject.toml` development classifier only after public package
      readiness is real.
    - [ ] Add `scripts/check_finished_product_readiness.py` consuming production usability, hard
      release readiness, package-channel readiness, API/schema stability, per-claim evidence,
      security, dependency audit, SBOM/checksum/provenance, golden workflows, admitted semantics,
      website readiness, docs-example proof, and CI matrix reports.
    - [ ] Integrate the finished-product gate into public release/tag commands and reserve
      `--allow-blocked` for local dry-run diagnostics.
    - [ ] Add final release approval artifact and post-release verification for package install,
      first-10-minutes, golden workflow, no-fallback smoke, docs links, and website support matrix.
    - [ ] Move finished-product gate and package-channel closeout to the completed ledger.
  - Next outcome: release/package publication has one final fail-closed gate and channel-specific
    proof requirements.
  - User-visible surface: release commands, package channels, package metadata, release notes,
    release readiness reports, website publication, docs.
  - Implementation scope: release scripts, package-channel matrices, CI/release workflows,
    package artifacts, docs, tests, and post-release verification scripts.
  - Evidence required: finished-product readiness report, package-channel reports,
    SBOM/checksum/provenance, TestPyPI/PyPI transcripts, install/uninstall/smoke proofs,
    approval artifact, and post-release verification.
  - Acceptance: public release cannot proceed with blocked v1 runtime rows, failing docs examples,
    missing schema fixtures, missing package proof, unsupported public claims, fallback execution,
    or missing approval; deferred rows pass only with explicit out-of-v1 scope and deterministic
    blockers.
  - Verification: finished-product readiness gate, hard release readiness without `--allow-blocked`
    for release tags, package-channel readiness, release validation evidence, docs/site validation,
    CI required checks, and post-release verification after publication approval.
  - Non-goals: no publication without human approval, no release tag creation during planning, no
    package-channel claim from local dry-run evidence only.
  - Claim boundary: may claim only that the final gate exists until all package/release evidence
    and approval pass.
  - Fallback boundary: package smoke and release verification must assert
    `fallback_attempted=false` and `external_engine_invoked=false` for ShardLoom runtime paths.
  - Ledger rule: ledger entry must include gate inputs, channel rows, approval refs, package
    evidence artifacts, and post-release verification outcomes.

### Open Work Checklist

- [ ] `PERF-RUNTIME-7A` Cold compatibility-to-certified route hot-runtime burn-down.
  - Source: current promoted `full_local` benchmark artifact generated
    `2026-06-13T11:33:10Z` from source revision
    `5743638a9225f479a0096f1c6db51a0068cac68f`; route-share Amdahl table and row-level
    inspection of `website/assets/benchmarks/latest/benchmark-results.json`.
  - Current state: `cold_certified_route` is the only broad multi-ms ShardLoom hot runtime lane:
    hot-route geomean `47.29 ms`, p95 `111.73 ms`, max `250.36 ms`. Included hot stages are
    source admission, source read, parse/decode, Vortex write, and reopen/verify. Current
    stage geomeans are roughly parse/decode `20.13 ms`, Vortex write `16.63 ms`, source read
    `9.58 ms` on rows with source-read timing, and reopen/verify `1.73 ms`; JSONL remains the
    slowest format. The artifact reports mixed external-control movement, so treat this as current
    evidence and optimization direction, not a standalone ShardLoom regression or performance
    improvement claim.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable. Cold-route work should consider dynamic admission for
    source-shape-specific typed builders, capillary windows for bounded source/read/write units,
    PulseWeave-style run-local coalescing for writer/open/reopen work, metadata-first source
    admission, and strict timing-surface separation so publication proof never redefines hot
    runtime.
  - Execution checklist:
    - [x] Add direct selected-tail parsing for canonical benchmark JSONL optional fields so
      nested/dirty/timestamp scenarios avoid scanning and splitting every unselected tail field.
    - [x] Keep non-canonical/whitespace JSONL tails on the existing general scanner to preserve
      fail-closed correctness for irregular JSONL.
    - [x] Add source-role pruning so fact-only cold routes do not read or write dimension source
      artifacts while join routes still require dim evidence explicitly.
    - [x] Add a fixed-schema CSV capillary typed-builder path for canonical unquoted benchmark
      rows, with the existing selected CSV parser retained for quoted/non-canonical rows.
    - [x] Add shared unsigned numeric parsing for canonical CSV/JSONL typed builders, with
      fail-closed range and token validation.
    - [x] Cache the Vortex table/flat layout strategy lazily inside the shared writer context so
      multi-artifact cold writes reuse the same strategy object after the first artifact.
    - [x] Remove no-op/stale cold-route source clutter uncovered by clippy after runtime changes.
    - [x] Run a targeted one-iteration local smoke artifact for changed cold-route JSONL/join
      paths (`target/perf-runtime-7-smoke.json`,
      `target/perf-runtime-7-nested-json-smoke.json`); treat as validation only, not a published
      benchmark claim.
    - [x] Refresh the full-local benchmark artifact after source validation to measure current
      parse/write movement in the promoted website bundle.
    - [x] Promote real writer context/write-plan counters from current source; the refreshed rows
      no longer report default `not_reported_by_engine`/zero writer context attribution when the
      CLI emitted segment/workspace timing.
    - [ ] Assess scenario-specific prepared artifact schema/id-column elision as a follow-up
      design change if final refreshed cold rows remain materially writer-bound; this must include
      native/replay/schema compatibility tests before implementation.
  - Next outcome: reduce real cold-route work without changing timing-surface semantics by
    batching/coalescing Vortex writes, tightening schema-driven typed source builders for text
    formats, and preserving reader-boundary projection for columnar formats. Apply dynamic
    admission only when source shape justifies the optimization, capillary windows for bounded
    ingest/write units, and PulseWeave-style coalescing for run-local writer/reopen work.
  - User-visible surface: benchmark route rows, benchmark stage attribution, route-share Amdahl
    table, website benchmark page, Python/CLI cold ingest path, and release readiness gates.
  - Implementation scope: `shardloom-vortex/src/traditional_analytics.rs`,
    `benchmarks/traditional_analytics/run.py`, benchmark promotion/readiness scripts if fields
    require schema additions, and focused Rust/Python regression tests. Generated benchmark
    artifacts are refreshed only after source behavior and validators pass.
  - Evidence required: correctness parity for compatibility inputs, no-fallback execution
    certificate fields, route timing stage-inclusion evidence, source-read/decode/write/reopen
    split evidence, and benchmark rows grouped by `(route_lane_id, timing_surface)`.
  - Acceptance: cold route still reports `fallback_attempted=false` and
    `external_engine_invoked=false`; hot-runtime total excludes result-sink/evidence render;
    JSONL/text and columnar paths keep projection/typed-builder evidence; Vortex writer context
    rows report useful non-zero writer/open/stage counters where applicable; route-share table no
    longer identifies broad missing attribution before optimization claims.
  - Verification: focused Rust tests for cold compatibility ingest, columnar projection, text
    typed builders, writer/reopen evidence; `cargo fmt --all -- --check`; targeted
    `cargo test -p shardloom-vortex --features vortex-traditional-analytics-benchmark --lib ...`;
    release-script tests covering timing fields; benchmark publication/readiness validators; full
    benchmark refresh only at the end of the cohesive runtime chunk.
  - Non-goals: no Spark/DataFusion/DuckDB/Polars/Velox fallback; no object-store/distributed
    runtime expansion; no public superiority or Spark-displacement claim; no synthetic shortcut
    rows.
  - Claim boundary: may claim only workload-scoped cold-route implementation and evidence
    improvements after benchmark refresh; no performance claim without current artifact evidence.
  - Fallback boundary: all ShardLoom rows must remain native, policy-admitted, and explicit with
    `fallback_attempted=false` and `external_engine_invoked=false`.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    `docs/architecture/phased-execution-completed-ledger.md`.
- [ ] `PERF-RUNTIME-7B` Heavy residual operator tail promotion for multi-key group-by and
  join-aggregate.
  - Source: current promoted `full_local` benchmark artifact row-level timing generated
    `2026-06-13T11:33:10Z`. Heavy hot tails now show cold `multi_key_group_by` geomean
    `66.76 ms` with diagnostic operator compute around `13.03 ms`, cold `join_aggregate`
    geomean `61.34 ms`, and prepared/native `join_aggregate` route geomeans around
    `4.93-7.68 ms` with operator compute around `3.34-4.43 ms`. Other prepared/native heavy
    grouping rows are near zero and look marginal unless future artifacts disagree.
  - Current state: operator mode inventory still reports residual-native operator promotion
    blockers: `residual_native_operator_encoding_promotion`,
    `selective_filter_selection_vector_metric_aggregation`, and
    `compatibility_import_materialization_elimination`. Diagnostic operator fields are visible but
    many operator timings are not additive to selected route totals.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable, but operator semantics come first. Dynamic kernel
    admission should select by cardinality/domain/null shape; encoded/partially encoded kernels
    should be preferred where decoded-reference parity exists. Capillary or PulseWeave controls
    apply around source/preparation/result work, not as a substitute for native operator
    correctness.
  - Execution checklist:
    - [x] Reuse the packed dense group accumulator for `join_aggregate`, replacing the older
      dense-left/per-dimension `HashMap` category accumulator in the hot loop.
    - [x] Add a dense-contiguous dimension-key membership fast path for compact dimension domains.
    - [x] Promote packed join result rendering to runtime code and preserve deterministic label
      coalescing/fail-closed category checks.
    - [x] Remove the obsolete dense-left category accumulator and old BTreeMap renderer instead
      of keeping unused legacy code.
    - [x] Run a targeted one-iteration local smoke for `join + aggregate` across CSV/JSONL cold and
      prepare-batch lanes; treat the result as route validation only until refreshed benchmark rows
      are promoted.
    - [x] Refresh full-local `join_aggregate`, `multi_key_group_by`, and high-cardinality rows to
      classify remaining opportunities as significant or marginal.
    - [x] Add generated dense-category interner admission for canonical `c0..cN` benchmark
      categories so distinct/multi-key/join residual operators avoid repeated string hashing when
      source shape proves dense ordered labels.
    - [ ] If refreshed rows still show multi-ms prepared/native operator spikes, add the next
      native kernel family with decoded-reference parity tests before claiming promotion.
  - Next outcome: promote a cohesive heavy-operator family rather than isolated scenario slivers:
    multi-key grouping, join+aggregate, high-cardinality distinct/group, and their prepared/native
    residual tails. Add encoded or partially encoded kernels where correctness evidence supports
    them; otherwise emit deterministic blocked diagnostics with precise next-step fields.
  - User-visible surface: benchmark operator hot-path candidates, runtime certificates, Python/SQL
    scenario behavior, benchmark route rows, and capability/diagnostic fields.
  - Implementation scope: operator/kernel code in `shardloom-vortex/src/traditional_analytics.rs`
    or extracted local helpers if needed, encoded-kernel evidence fields, route/operator
    diagnostics, and regression tests for nulls, high cardinality, ordering-sensitive top/join
    outputs, and decoded-reference parity.
  - Evidence required: decoded-reference correctness, null/missing-key semantics, no-fallback
    certificates, operator execution mode transition evidence, timing-surface-safe route fields,
    and benchmark rows proving whether the tail changed.
  - Acceptance: supported heavy operator rows no longer remain generic
    `residual_native_operator_not_encoded_native` when a native kernel exists; unsupported shapes
    fail or remain blocked with deterministic blocker codes; prepared/native `join_aggregate`
    spikes are explained by additive timing fields or reduced by native execution; route totals
    remain authoritative.
  - Verification: focused Rust unit/integration tests for heavy operators, decoded-reference
    parity tests, benchmark publication claim gate, route timing instrument readiness, and full
    workspace gates when shared operator contracts move.
  - Non-goals: no broad SQL planner rewrite, no distributed shuffle, no external engine execution,
    no hidden decode-to-Arrow fallback.
  - Claim boundary: encoded/operator improvements may be claimed only per supported operator family
    with correctness and benchmark evidence.
  - Fallback boundary: external engines remain baselines only and never execute ShardLoom work.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.
- [ ] `PERF-RUNTIME-7C` Prepared lookup/create and route-total attribution cleanup.
  - Source: current route-share Amdahl and stage-inclusion tables. `prepare_once_first_query`
    hot-route geomean is `1.45 ms`, dominated by `prepared_state_lookup_or_create` around
    `1.00 ms` (`68.8%` route share). `prepare_once_batch`, warm, and native lanes have very low
    geomeans but still carry diagnostic stage fields larger than selected route totals.
  - Current state: prepared lookup/create is a moderate absolute cost and a large relative cost for
    first-query prepared routes. Route-share rows are optimization-ready, but some diagnostic
    fields are intentionally non-additive and can distract optimization targeting.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable. Use dynamic admission for cache-hit/miss policy,
    metadata-first manifest verification, and PulseWeave-style run-local coalescing for repeated
    dependency-packet checks. Capillary work units apply only if manifest/artifact verification is
    split into bounded source/prepared-state units.
  - Execution checklist:
    - [x] Confirm `preparation_engine_millis` prefers narrow prepared-state/import fields and does
      not use `total_runtime_micros` as the narrow prepare timing source.
    - [x] Keep `prepare_route_total_ms` separate for full route totals.
    - [x] Hash serialized JSON bytes directly for source-admission, prepared-state manifest, and
      index digests to avoid an intermediate UTF-8 string allocation.
    - [x] Run a targeted one-iteration local prepare-batch smoke showing
      `prepared_state_lookup_or_create` remains separate from `prepare_route_total`.
    - [x] Refresh prepared-route benchmark rows to measure whether manifest lookup/create moved.
    - [ ] If lookup remains material, evaluate a manifest/index read-through cache that still
      verifies manifest digest, source fingerprints, artifact fingerprints, native I/O
      certificates, and no-fallback fields before reuse.
  - Next outcome: split manifest lookup, cache-hit, cache-miss create, dependency-packet
    verification, artifact write, and register-update timings into additive and diagnostic fields;
    remove avoidable lookup/create work on cache hits; keep first-query and amortized formulas
    explicit.
  - User-visible surface: prepared-state reuse evidence, benchmark route formulas, Python
    front-door prepared-route examples, and release evidence reports.
  - Implementation scope: prepared-state manifest/register helpers, session cache counters,
    timing field promotion in `benchmarks/traditional_analytics/run.py`, Rust tests for
    cache-hit/miss/stale-packet behavior, and website data fields if schema-safe.
  - Evidence required: cache hit/miss counters, stale-packet rejection evidence, additive timing
    formulas, no result-sink/evidence render in hot-runtime totals, and benchmark rows showing
    lookup/create attribution.
  - Acceptance: first-query prepared route reports precise lookup/create subcomponents; cache-hit
    path avoids unnecessary register/write work; prepared batch amortized route remains formula
    backed; no `total_runtime_micros` fallback is used as a narrow prepare timing source.
  - Verification: focused prepared-state Rust tests, release-script tests for timing promotion,
    publication claim gate, route timing instrument readiness, and targeted benchmark refresh when
    source behavior changes.
  - Non-goals: no package/public release claim, no external cache service, no distributed session
    runtime.
  - Claim boundary: may claim attribution and scoped first-query prepared-route improvements only
    with benchmark evidence.
  - Fallback boundary: prepared-state reuse must remain ShardLoom-native and fail closed on stale
    dependency packets.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.
- [ ] `PERF-RUNTIME-7D` Publication-proof sink/evidence overhead burn-down without redefining hot
  runtime.
  - Source: current promoted `full_local` artifact generated `2026-06-13T11:33:10Z`.
    Publication-proof routes add roughly `3.50-5.13 ms` evidence render and about
    `0.47-0.60 ms` result-sink work to warm/native/prepared lanes; this is significant for
    proof/publication throughput but not a core hot-runtime regression.
  - Current state: `publication_proof` rows are correctly separated from `hot_runtime`, but the
    proof path still spends more time rendering human evidence than executing warm/native queries.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable for proof publication, not hot runtime. Use
    evidence-tier controls and timing-surface separation first, then PulseWeave-style coalescing or
    digest-keyed sidecar reuse for repeated publication records. Capillary units apply only to
    bounded proof-record writes, not to hiding sink/evidence work from publication totals.
  - Execution checklist:
    - [x] Confirm Rust runtime rows emit compact machine evidence and mark human evidence render as
      outside the Rust timed route.
    - [x] Confirm benchmark promotion already writes an incremental publication-proof sidecar with
      reused/written/removed record counts and no-fallback fields.
    - [x] After benchmark promotion, confirm sidecar admission counts and website labels keep
      proof overhead out of hot runtime; the refreshed run wrote `600` proof records, reused `0`,
      and removed `120` stale records because the row digest changed.
    - [ ] Repeat promotion over an unchanged machine-evidence artifact and confirm sidecar reuse
      before claiming publication-proof reuse improvements.
    - [ ] If publication-proof rows still spend multi-ms in repeated human formatting after sidecar
      reuse, optimize the Python/website render surface rather than the ShardLoom hot runtime.
  - Next outcome: coalesce and cache publication-proof render work, reuse machine evidence digests,
    keep full Vortex replay/result-sink timing explicit, and avoid repeating human formatting when
    the compact machine evidence is unchanged.
  - User-visible surface: benchmark website, publication-proof sidecar, release readiness reports,
    and result-sink/evidence-render timing fields.
  - Implementation scope: publication-proof sidecar writer/reuser, benchmark promotion scripts,
    website data ingestion, readiness validators, and Python tests for stale/reused proof records.
  - Evidence required: sidecar reused/written/stale counts, no-fallback proof fields, explicit
    `sink_timing_included_in_route_total=true` for proof surfaces, and unchanged hot-runtime totals.
  - Acceptance: publication-proof rows remain visible and slower for stated reasons; repeated
    publication over unchanged machine evidence reuses proof records; website labels continue to
    distinguish hot route geomean from publication-proof route geomean.
  - Verification: release-script tests, benchmark publication/front-door/readiness validators,
    website readiness, and targeted artifact promotion after source changes.
  - Non-goals: no hiding proof cost in hot runtime, no removal of publication-proof rows, no public
    performance claim from proof-path-only improvements.
  - Claim boundary: may claim only proof-publication overhead reduction or attribution quality,
    not core runtime speed, unless a refreshed artifact proves core runtime changed.
  - Fallback boundary: proof generation must not call external compute engines or use external
    fallback execution.
  - Ledger rule: after merge/session completion, move measured closeout and command evidence to
    the completed ledger.

#### Production Readiness / Release Track

- [ ] `RELEASE-READY-16A` V1 release boundary and unsupported-surface firewall.
  - Source: attached production-readiness review, `README.md`, `docs/release/*`,
    `docs/architecture/runtime-gap-family-burn-down.md`,
    `docs/architecture/scale-readiness-contract.md`, and package/release readiness gates.
  - Current state: ShardLoom is pre-release. V1 should include every feasible runtime/product
    family that can be made real, safe, evidence-backed, and package/release-ready. Any
    object-store, lakehouse/table, Foundry, distributed, live/hybrid, arbitrary extension/effect,
    or platform support that cannot be completed for v1 must remain explicitly unsupported,
    blocked, narrowed, or deferred with a concrete reason.
  - Intake review: revised from the earlier technical-preview framing to an inclusion-first v1
    boundary. Existing unsupported-surface guardrails stay, but they become a fail-closed firewall
    for unfinished or infeasible families, not a default exclusion of broad functionality.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: mostly control-plane applicable. The release envelope should not
    invent runtime optimizations, but validators must require each newly supported runtime family to
    document PulseWeave/capillary/dynamic fit before a support claim is accepted.
  - Execution checklist:
    - [ ] Define the v1 support envelope: local file workflows, current Python/CLI surfaces,
      supported local formats, supported output targets, and every broad runtime/product family
      that is feasible to close with evidence.
    - [ ] Record infeasibility reasons for any broad family narrowed or left outside v1, including
      missing external platform proof, unresolved safety/security design, protocol scope, package
      channel availability, or lack of deterministic fault/recovery evidence.
    - [ ] Normalize README, docs, website, package metadata, release reports, and capability
      outputs so every unsupported production family uses one canonical claim boundary.
    - [ ] Add release validators that fail if production, platform, distributed, Foundry,
      live/hybrid, object-store, lakehouse, or arbitrary extension support is implied without a
      matching production-ready item closed in this plan and ledger.
    - [ ] Add package dry-run evidence showing the v1 package candidate installs, imports, runs
      supported examples, emits no fallback evidence, and does not publish to package channels.
    - [ ] Add user-facing unsupported diagnostics for production-family entrypoints that exist as
      stubs, preview routes, or report-only commands.
    - [ ] Move the closed release-boundary checklist, exact command evidence, and residual
      unsupported production families to the completed ledger after merge.
  - Next outcome: a v1 release candidate can be described by its real supported runtime/product
    families, with any unfinished family explicitly blocked, narrowed, or deferred.
  - User-visible surface: README, package metadata, website, docs, Python/CLI help, capability
    reports, release readiness reports, and benchmark website disclaimers.
  - Implementation scope: release/docs validators, README/site copy, package dry-run scripts,
    Python/CLI capability outputs, and tests in `python/tests` and `shardloom-contract-tests`.
  - Evidence required: package dry-run, local example execution, release readiness validators,
    no-fallback fields, unsupported-surface diagnostics, and claim-boundary snapshots.
  - Acceptance: release reports can pass only for the declared v1 support envelope; feasible broad
    families are promoted into v1-required work, infeasible families carry recorded blockers or
    deferral reasons, any unsupported production claim fails CI, package/install examples run
    locally, and public docs do not imply support outside the declared envelope.
  - Verification: release-script shard, website/docs validation, package smoke, release readiness
    reports, `cargo test --workspace --all-targets`, and targeted Python package import/use tests.
  - Non-goals: no package publication, no unsupported production claim, no broad family support
    without real runtime, safety, package, and release evidence.
  - Claim boundary: may claim only v1 support for explicitly listed workloads/families with
    evidence; unfinished production-family claims remain `not_claim_grade`,
    `unsupported|blocked`, `v1_candidate_pending_feasibility`, or `deferred_after_v1` with reason.
  - Fallback boundary: no Spark/DataFusion/DuckDB/Polars/Velox or external platform fallback may be
    introduced to make preview examples pass.
  - Ledger rule: close only after merge; ledger entry must list release envelope, validators,
    package dry-run artifacts, and unsupported families left open.
- [ ] `PROD-READY-0A` Common production certification gate.
  - Source: attached Common Production Gate, `docs/architecture/runtime-gap-family-burn-down.md`,
    `docs/architecture/capability-certification-sequencing.md`, RFC 0012, RFC 0014, RFC 0015,
    RFC 0017, RFC 0024, and release evidence gates.
  - Current state: production readiness is mapped but not centrally executable. Many surfaces have
    report-only schemas, planned capability rows, or local smoke evidence, but no shared gate that
    certifies a declared workload/environment as production-ready.
  - Intake review: accepted as required before any broad v1 production/support label can pass.
    This gate is not a substitute for runtime implementation; it is the shared evidence contract
    that broad families must satisfy when promoted into v1.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable as a gate contract. Workload declarations must record
    whether dynamic admission/work shaping, capillary work units, PulseWeave runtime control,
    metadata-first execution, and timing/evidence-tier separation were applied, rejected, or not
    applicable, with evidence for the decision.
  - Execution checklist:
    - [ ] Define the production-ready workload declaration schema: workload name, environment,
      data scale, input/output formats, statefulness, effect permissions, security posture, and
      unsupported edge boundary.
    - [ ] Add a shared production gate that requires real runtime execution, correctness evidence,
      Native I/O certificates, execution certificates, fault tolerance, memory/backpressure,
      benchmark evidence, security/governance policy, and release/API stability evidence.
    - [ ] Require deterministic unsupported diagnostics for every out-of-scope path in the same
      workload family.
    - [ ] Add fail-closed validators for production labels in docs, website, package metadata,
      CLI/Python capability reports, and benchmark artifacts.
    - [ ] Add fixture workload declarations for at least one local production-candidate profile and
      prove the gate blocks until all required evidence exists.
    - [ ] Move completed gate contract and command evidence to the ledger after merge.
  - Next outcome: every later production-family item can close against one shared gate instead of
    inventing readiness criteria per PR.
  - User-visible surface: release readiness reports, capability/certification commands, website
    support tables, README status language, benchmark claim gates, and package metadata.
  - Implementation scope: certification structs, release validators, Python/CLI capability
    snapshots, docs/site labels, and contract tests.
  - Evidence required: gate schema snapshots, negative tests for missing evidence, positive fixture
    rows only when all required artifacts exist, and no-fallback proof.
  - Acceptance: production labels cannot pass from report-only artifacts; a declared workload can
    be evaluated deterministically; missing correctness, Native I/O, fault-tolerance, memory,
    benchmark, security, or release evidence blocks production status.
  - Verification: release-script shard, contract tests, CLI snapshot tests, website/docs
    validation, release readiness reports, and full workspace tests if shared certification code
    moves.
  - Non-goals: no runtime family implementation inside this gate; no production claim by defining
    the schema; no package publication.
  - Claim boundary: may claim only that the production gate exists and fails closed until evidence
    is present.
  - Fallback boundary: gate logic must reject external fallback execution and external query-engine
    providers as production evidence.
  - Ledger rule: close only after the gate and validator evidence are merged and documented in the
    completed ledger.
- [ ] `PROD-READY-1A` Production format and local I/O adapter certification.
  - Source: attached Formats/I/O review, `docs/architecture/universal-input-contract.md`,
    `docs/skills/translation-layer.md`, `docs/architecture/vortex-adapter-integration-plan.md`,
    and current traditional benchmark rows.
  - Current state: scoped local evidence exists for CSV, JSONL/NDJSON, Parquet, Arrow IPC, Avro,
    ORC, Vortex, and compatibility outputs, but production-certified adapters require full
    capability, pushdown, fidelity, error-policy, and certificate evidence per declared format
    family.
  - Intake review: accepted as required for v1 because local input/output breadth is central to a
    comprehensive first release.
  - V1 scope classification: `required_for_v1`.
  - ShardLoom technique review: applicable. Format work should consider dynamic parser/reader
    admission by shape, capillary source/preparation/write windows, PulseWeave coalescing for
    repeated local preparation, metadata-first pruning/fingerprint reuse, and evidence-tier
    separation for hot read/write versus proof/publication paths.
  - Execution checklist:
    - [ ] Declare per-format production profiles: Vortex native input/output, CSV/JSONL text,
      Parquet/Arrow IPC columnar, Avro/ORC compatibility, and compatibility output/export targets.
    - [ ] Add parser/reader contracts for malformed rows, encoding/null/coercion rules,
      projection-aware typed builders, nested/complex dtype support, and deterministic blockers.
    - [ ] Add pushdown and fidelity reports for columnar formats: projection/filter/statistics
      support, metadata preservation, layout/statistics loss, and materialization cost.
    - [ ] Add Vortex-native broad read/write certification with metadata/statistics preservation,
      no-fallback Native I/O certificates, and local replay evidence.
    - [ ] Add compatibility output `TranslationReport` coverage for preserved/lost metadata,
      materialization cost, unsupported schema diagnostics, and explicit non-execution-fallback
      boundaries.
    - [ ] Add representative correctness and fuzz/property fixtures for local format edge cases.
    - [ ] Move completed format-family evidence and any unclosed format profiles to the ledger
      after merge.
  - Next outcome: local format support can be promoted from scoped benchmark evidence to declared
    production-candidate adapter profiles without implying object-store/table runtime.
  - User-visible surface: Python/CLI reads/writes, diagnostics, capability reports, benchmark
    format rows, docs, and website support tables.
  - Implementation scope: local input/output adapters, Vortex I/O layer, translation reports,
    benchmark fixtures, Python/CLI examples, and validators.
  - Evidence required: per-format correctness, Native I/O certificates, TranslationReports,
    pushdown/fidelity reports, no-fallback certificates, and benchmark evidence where performance
    or route claims are made.
  - Acceptance: every production-candidate local format has explicit supported shapes, blocked
    shapes, correctness fixtures, certificate evidence, and release-visible support status.
  - Verification: adapter tests, fuzz/property tests where applicable, release-script validators,
    benchmark readiness, website/docs validation, and full workspace tests.
  - Non-goals: no table semantics, no object-store networking, no external execution fallback, no
    broad arbitrary schema support without evidence.
  - Claim boundary: may claim only the declared local format profiles; Iceberg/Delta/Hudi remain
    table runtimes, not file-format support, until their own item closes.
  - Fallback boundary: compatibility readers/writers are translation/export surfaces, never
    fallback execution engines.
  - Ledger rule: ledger entry must list closed format profiles, unsupported shapes, evidence
    artifacts, and benchmark/validator commands.
- [ ] `PROD-READY-1B` Object-store runtime production path.
  - Source: attached Object-Store Runtime review, `docs/architecture/scale-readiness-contract.md`,
    RFC 0017, RFC 0014, `docs/skills/object-store-runtime.md`, and object-store readiness gates.
  - Current state: object-store/table rows are report-only or blocked for listing, byte-range
    reads, streaming reads, writes, staging, commit, credentials, network effects, and production
    certificates.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    object-store workload/backend in v1 if emulator plus approved real-backend proof, credential
    safety, bounded streaming, commit/cleanup, and certificate evidence can close; otherwise
    narrow or defer only with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: strongly applicable. Object-store work should use capillary split
    windows, dynamic request coalescing/prefetch/backpressure admission, PulseWeave task-graph
    control for bounded in-flight work, metadata-first listing/statistics decisions, and explicit
    hot-runtime versus proof/commit timing surfaces.
  - Execution checklist:
    - [ ] Define the first supported object-store workload/environment and review each other
      scheme/backend for v1 feasibility before deferring it.
    - [ ] Implement provider abstraction for selected schemes with credential policy, redaction,
      request signing boundary, and no-probe defaults for explain/estimate/doctor/capabilities.
    - [ ] Add listing, object version/ETag capture, checksum validation, byte-range read, request
      coalescing, prefetch, retry/backoff, rate-limit handling, and bounded streaming reads.
    - [ ] Add staged/multipart writes, commit protocol, rollback/cleanup, idempotency keys, and
      ambiguous commit diagnostics.
    - [ ] Emit object-store Native I/O certificates with request counts, bytes requested/read,
      retries, cache hits, credential posture, and no-fallback fields.
    - [ ] Test against a local emulator and one approved real backend profile before any
      production claim.
    - [ ] Move closed object-store workload evidence and deferred backend scope to the ledger after
      merge.
  - Next outcome: object-store support advances from report-only ladder to a declared,
    certificate-backed runtime path for one scoped environment.
  - User-visible surface: CLI/Python object-store reads/writes, diagnostics, capability reports,
    release readiness, docs, and benchmark scale profiles.
  - Implementation scope: object-store runtime, credential/redaction policy, retry/backoff,
    streaming read/write paths, commit/rollback evidence, tests, and release validators.
  - Evidence required: local emulator tests, approved backend proof, Native I/O certificates,
    request/byte/retry counters, fault-injection tests, cleanup evidence, and no-fallback proof.
  - Acceptance: selected object-store workload can read/write with bounded memory and deterministic
    failure/cleanup behavior; unsupported backends/effects remain blocked before credential or
    network access.
  - Verification: object-store smoke/integration tests, credential redaction tests, fault-injection
    tests, release validators, and benchmark profile evidence before claims.
  - Non-goals: no table semantics, no distributed runtime, no hidden local-file mirroring claim,
    no broad multi-cloud production claim from one backend.
  - Claim boundary: may claim only the declared backend/workload profile with evidence.
  - Fallback boundary: object-store runtime must not use external query engines or platform compute
    as execution fallback.
  - Ledger rule: ledger entry must capture backend profile, credential policy, fault cases,
    certificate artifacts, and unsupported backends.
- [ ] `PROD-READY-1C` Lakehouse/table runtime production path.
  - Source: attached Lakehouse/Table Runtime review, `docs/architecture/scale-readiness-contract.md`,
    `docs/skills/translation-layer.md`, universal input/output contracts, and primary table
    protocol specs to be source-checked before implementation.
  - Current state: table metadata/reporting is separate from table runtime. Metadata reads,
    snapshot listings, or compatibility output rows do not imply scan, append, overwrite,
    merge/update/delete, commit, rollback, schema evolution, or catalog support.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    table protocol/workload in v1 if source-checked specs, scan semantics, write/commit scope,
    rollback/recovery, conflict handling, and no-fallback evidence can close; otherwise narrow or
    defer with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable after source-spec review. Table work should consider
    metadata-first snapshot/manifest pruning, capillary split/manifests for bounded scans and
    commits, dynamic admission for delete/schema/evolution features, and PulseWeave-style
    coordination only where it improves ShardLoom-native task/retry/commit evidence.
  - Execution checklist:
    - [ ] Select the first table protocol/workload profile and document why other protocols remain
      blocked.
    - [ ] Source-check current primary protocol specs before implementation: Iceberg, Delta, Hudi,
      and any chosen catalog such as Iceberg REST, Glue-like, Hive-like, Nessie, Polaris, or
      Gravitino-style APIs.
    - [ ] Implement metadata readers, snapshot/time-travel selection, manifest/log/timeline
      parsing, schema evolution, partition evolution, and delete/tombstone/deletion-vector
      semantics for the selected profile.
    - [ ] Lower table scans into ShardLoom-native splits with Native I/O certificates and
      deterministic unsupported diagnostics for unadmitted table features.
    - [ ] Implement writes only for proven semantics: append/overwrite first; merge/update/delete
      only after correctness, conflict, rollback, and recovery evidence exists.
    - [ ] Add optimistic concurrency/conflict handling, commit/rollback/recovery evidence, and
      TranslationReport coverage for metadata/statistics/layout loss.
    - [ ] Move closed table protocol/workload evidence and deferred protocols to the ledger after
      merge.
  - Next outcome: table support becomes a scoped runtime path instead of metadata/report-only
    posture.
  - User-visible surface: table reads/writes, catalog diagnostics, capability reports, release
    readiness, docs, and benchmark table profiles.
  - Implementation scope: table metadata/runtime modules, catalog adapters, scan planning, commit
    protocol, translation reports, correctness fixtures, and release validators.
  - Evidence required: protocol conformance tests, scan correctness, write/commit/rollback tests,
    conflict handling, Native I/O certificates, TranslationReports, and no-fallback proof.
  - Acceptance: selected table workload can scan and, if in scope, write/commit with explicit
    semantics; unsupported operations fail before hidden materialization or external execution.
  - Verification: protocol fixture tests, local table integration tests, object-store tests if
    remote tables are admitted, release validators, and benchmark evidence for table claims.
  - Non-goals: no blanket Iceberg/Delta/Hudi support, no Foundry virtual-table claim, no external
    warehouse/lakehouse engine fallback.
  - Claim boundary: table claims are protocol/workload-specific and require source-checked specs
    and runtime evidence.
  - Fallback boundary: Spark, DataFusion, DuckDB, Polars, Velox, Trino, warehouse engines, and
    platform compute remain external baselines or handles, never ShardLoom execution.
  - Ledger rule: ledger entry must include selected protocol, supported operations, deferred
    semantics, command evidence, and source/spec review refs.
- [ ] `PROD-READY-1D` Distributed runtime production path.
  - Source: attached Distributed Runtime review, `docs/architecture/scale-readiness-contract.md`,
    RFC 0016, RFC 0017, `docs/skills/object-store-runtime.md`, and split/shuffle readiness docs.
  - Current state: distributed runtime is report-only. No real coordinator/worker service,
    leases, heartbeats, task attempts, remote result fragments, deterministic merge, or
    multi-worker benchmark proof exists.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include a local or scoped
    multi-worker runtime in v1 if coordinator/worker lifecycle, fault cases, deterministic merge,
    cleanup, and benchmark evidence can close; otherwise narrow to deterministic unsupported
    diagnostics with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: strongly applicable. Distributed runtime should be designed around
    capillary task units, PulseWeave runtime control, dynamic work shaping, metadata-first split
    pruning, and explicit execution certificates so later optimization does not require reworking
    the scheduler contract.
  - Execution checklist:
    - [ ] Define the first distributed workload/environment and minimum scale where single-node is
      insufficient.
    - [ ] Implement a local coordinator process/service with worker lifecycle, leases, heartbeats,
      task attempts, cancellation, cleanup, and deterministic diagnostics.
    - [ ] Execute real `SplitManifest` units across workers with bounded memory, result fragments,
      idempotency keys, retries, duplicate-attempt protection, and deterministic merge.
    - [ ] Add shuffle/repartition strategy, skew detection/handling, local combine/global merge,
      and spill/backpressure integration for stateful operators in scope.
    - [ ] Emit distributed execution certificates linking input splits, worker attempts,
      retries/cancellations, fragments, merge output, and no-fallback evidence.
    - [ ] Add fault-injection tests for worker crash, retry, duplicate attempt, partial result,
      cancelled query, stale lease, and cleanup failure.
    - [ ] Add benchmark profile proving correctness and multi-worker benefit for the declared
      workload before any distributed performance claim.
    - [ ] Move completed distributed workload evidence and deferred scale/runtime gaps to the
      ledger after merge.
  - Next outcome: distributed support moves from protocol vocabulary to one certified
    multi-worker runtime path.
  - User-visible surface: CLI/API distributed execution, diagnostics, capability reports,
    benchmark scale profiles, release readiness, and docs.
  - Implementation scope: coordinator/worker runtime, split scheduler, fragment writer/merger,
    retry/cancellation/cleanup, shuffle/backpressure, tests, and benchmark harness.
  - Evidence required: execution certificates, fault-injection results, benchmark evidence,
    correctness parity, cleanup evidence, and no-fallback proof.
  - Acceptance: declared distributed workload completes with deterministic fragments/merge and
    survives fault cases; unsupported distributed shapes fail explicitly.
  - Verification: unit/integration/fault-injection tests, scale benchmark profile, release
    validators, and workspace gates.
  - Non-goals: no distributed claim for every operator, no managed-platform claim, no
    object-store/table support unless those items are separately closed.
  - Claim boundary: may claim only declared multi-worker workload/environment with benchmark and
    fault-tolerance evidence.
  - Fallback boundary: no Ray/Dask/Spark/Flink/Trino or external distributed engine execution.
  - Ledger rule: ledger entry must name workload, worker topology, fault cases, benchmark artifacts,
    and unsupported distributed families.
- [ ] `PROD-READY-1E` Streaming/live/hybrid runtime production path.
  - Source: attached Streaming / Live / Hybrid review, RFC 0034, RFC 0017, RFC 0014,
    optimizer/adaptive execution docs, and live/hybrid capability gates.
  - Current state: CG-22 is a design target with fixture evidence, but there is no production
    streaming runtime, state store, changelog, checkpoint/restore, watermarking, or continuous
    materialized-view semantics.
  - Intake review: accepted as a v1 candidate, not default-deferred. Include the first feasible
    live/hybrid workload in v1 if state, checkpoint, recovery, freshness, output mode, and
    certificate evidence can close; otherwise narrow or defer with a concrete feasibility reason.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable. Live/hybrid work should use capillary micro-segments,
    dynamic mode/update admission, PulseWeave-style bounded work-in-progress where it preserves
    recovery semantics, metadata-first state/freshness checks, and separate timing/evidence
    surfaces for hot update paths versus checkpoint/proof work.
  - Execution checklist:
    - [ ] Define `EngineMode` production semantics for batch/live/hybrid/auto and source
      boundedness/update-mode classification.
    - [ ] Implement change record model: insert/update/delete/retract, event time, processing
      time, watermarks, late data policy, and deterministic unsupported diagnostics.
    - [ ] Implement state store, changelog, checkpoint, restore, hot/warm/cold storage model,
      Vortex micro-segments, cold Vortex segments, and delta overlay with tombstones/deletion
      vectors where admitted.
    - [ ] Define sink output modes: snapshot, append, changelog, materialized view, and freshness
      guarantees.
    - [ ] Emit freshness, state, checkpoint, delta-overlay, and execution certificates with
      no-fallback evidence.
    - [ ] Add recovery/fault tests for restart, late data, duplicate records, partial checkpoint,
      cancellation, and cleanup.
    - [ ] Add benchmark/profile evidence for declared live/hybrid workload before claims.
    - [ ] Move closed live/hybrid workload evidence and unsupported modes to the ledger after
      merge.
  - Next outcome: a scoped live/hybrid runtime can be certified without implying exactly-once or
    arbitrary streaming support.
  - User-visible surface: Python/CLI/API engine-mode selection, diagnostics, capability reports,
    benchmarks, docs, and release readiness.
  - Implementation scope: engine-mode runtime, state/checkpoint store, delta overlay, sink output
    modes, certificates, tests, and benchmark harness.
  - Evidence required: state/checkpoint/freshness certificates, recovery tests, correctness
    fixtures, benchmark evidence, and no-fallback proof.
  - Acceptance: selected live/hybrid workload has deterministic state and recovery behavior;
    unsupported modes fail explicitly; exactly-once is not claimed unless source/state/sink
    idempotency evidence exists.
  - Verification: state/recovery tests, live/hybrid smoke tests, release validators, benchmark
    profile, and workspace gates.
  - Non-goals: no arbitrary streaming connectors, no exactly-once claim by default, no external
    streaming engine fallback.
  - Claim boundary: may claim only the declared live/hybrid workload and delivery semantics proven
    by certificates.
  - Fallback boundary: no Flink/Spark Streaming/Kafka Streams/Ray/Dask or external engine fallback.
  - Ledger rule: ledger entry must list delivery semantics, state model, recovery cases, benchmark
    artifacts, and unsupported modes.
- [ ] `PROD-READY-1F` UDF, plugin, and explicit-effect execution production gate.
  - Source: attached UDF / Plugin / Effect Execution review, RFC 0011, RFC 0023,
    `docs/skills/modular-extensibility.md`, `docs/skills/extension-plugin-sandboxing.md`, and
    security/governance gates.
  - Current state: extension/UDF/effect surfaces are architectural or report-only. Manifest
    inspection, UDF/API/LLM/model/vector execution, network/filesystem/secret effects, and plugin
    runtime are not production-supported.
  - Intake review: accepted as a v1 candidate for safe scoped subsets. Include manifest
    inspection and typed deterministic UDF/plugin/effect classes in v1 where sandboxing, denial,
    audit, timeout/resource, and no-fallback evidence can close; defer dangerous effect classes
    only with explicit safety reasons.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: selectively applicable. Dynamic admission and fail-closed
    capability checks are central; capillary isolation can bound effectful batches; PulseWeave
    applies only to explicit, policy-admitted batching/coalescing and must not hide effects or
    materialization boundaries.
  - Execution checklist:
    - [ ] Define manifest-first extension model with capability, permission, license, provenance,
      effect, determinism, materialization, null behavior, dtype, timeout, memory, CPU, retry,
      idempotency, and audit metadata.
    - [ ] Implement non-executing manifest inspection and capability discovery that cannot run
      extension code.
    - [ ] Implement typed UDF registry for scoped scalar/aggregate/table functions with encoded
      capability vs materialization-required classification.
    - [ ] Add sandboxing policy: Rust-native first where possible, WASM later only after ABI
      review, Python only as an explicit materialization/effect boundary.
    - [ ] Disable network, filesystem, and secret access by default; require explicit policy and
      audit evidence for any effectful operation.
    - [ ] Ensure explain/estimate/doctor/capabilities never execute external effects.
    - [ ] Add security tests for permission denial, timeout, memory/CPU limits, deterministic
      diagnostics, audit output, and no-fallback proof.
    - [ ] Move closed extension/UDF/effect gate evidence and deferred effect classes to the ledger
      after merge.
  - Next outcome: extension/effect execution has a production gate and scoped runtime path rather
    than report-only architecture.
  - User-visible surface: plugin manifests, UDF registration, capability discovery, diagnostics,
    Python/CLI/API execution, docs, and security/release gates.
  - Implementation scope: manifest parser, registry, sandbox policy, execution bridge, diagnostics,
    audit/certificate output, tests, and release validators.
  - Evidence required: manifest inspection tests, security denial tests, typed UDF correctness,
    effect audit certificates, no-fallback proof, and release validators.
  - Acceptance: scoped UDF execution can run only under explicit policy and deterministic contracts;
    external effects are denied by default and never run during discovery/explain/estimate/doctor.
  - Verification: security tests, UDF correctness tests, release-script shard, security governance
    gates, and workspace gates.
  - Non-goals: no arbitrary Python plugin execution, no network/API/LLM/model/vector execution
    without explicit future production item, no hidden materialization fallback.
  - Claim boundary: may claim only the scoped UDF/plugin/effect classes proven by tests and
    certificates.
  - Fallback boundary: plugin/UDF execution must not delegate unsupported plans to external query
    engines or hidden runtimes.
  - Ledger rule: ledger entry must include admitted capability classes, denied effects, sandbox
    policy, security evidence, and unsupported extensions.
- [ ] `PROD-READY-1G` Foundry integration production pack.
  - Source: attached Foundry Integration review, RFC 0036,
    `docs/architecture/scale-readiness-contract.md`, Foundry proof docs, and release/package
    readiness gates.
  - Current state: Foundry support is local/dev-stack proof and optional integration posture only.
    There is no real Foundry Code Repository package/import proof, transform wrapper, dataset
    source/sink certificate, Artifact Repository publication proof, Compute Module runtime, or
    production Foundry evidence dataset output.
  - Intake review: accepted as a v1 candidate if real Foundry environment proof is available.
    Include a scoped Foundry integration pack in v1 if package/import, transform, dataset
    source/sink, governance, and no-fallback evidence can close; otherwise defer only because the
    real platform proof is unavailable or incomplete.
  - V1 scope classification: `v1_candidate_pending_feasibility`.
  - ShardLoom technique review: applicable at the integration boundary. Foundry work should
    consider capillary dataset chunks, dynamic platform-handle admission, PulseWeave-style
    transform/task coalescing only with real platform evidence, metadata-first lineage/governance
    checks, and strict separation of Foundry platform handles from ShardLoom execution.
  - Execution checklist:
    - [ ] Define `shardloom-foundry` package boundary, install/import/CLI resolution, and version
      compatibility inside real Foundry Code Repositories.
    - [ ] Implement transform wrapper that records Foundry execution context, build mode,
      transaction/build refs, dataset RIDs, branches, and no-fallback evidence.
    - [ ] Emit dataset source/sink reports, certificate datasets, metrics datasets, Data Health /
      Expectations bridge evidence, lineage facets, and governance/marking/redaction policy.
    - [ ] Classify virtual tables and external systems explicitly as platform handles or
      external-compute boundaries unless staged/native ShardLoom execution evidence exists.
    - [ ] Add Artifact Repository publication proof before package availability claims.
    - [ ] Add Compute Module support only after REST/control-plane runtime item is real and
      certificate-backed.
    - [ ] Test in real Foundry environment with evidence datasets and deterministic blocked
      diagnostics for unsupported transforms/connectors.
    - [ ] Move closed Foundry workload evidence and deferred platform claims to the ledger after
      merge.
  - Next outcome: Foundry integration moves from local proof posture to a scoped platform
    integration pack with real package/runtime evidence.
  - User-visible surface: Foundry package, transform wrapper, datasets, metrics/certificates,
    lineage/governance output, docs, and release readiness.
  - Implementation scope: optional Foundry package, transform helper, evidence dataset writers,
    governance/lineage reports, release/package validators, docs, and platform tests.
  - Evidence required: real Foundry package/import proof, transform run evidence, dataset
    source/sink reports, governance/redaction evidence, publication proof, and no-fallback proof.
  - Acceptance: selected Foundry workflow runs inside Foundry with ShardLoom-native execution
    evidence; unsupported platform handles remain explicit and non-claim-grade.
  - Verification: Foundry integration tests/proof artifacts, package validators, release readiness
    gates, docs/site validation, and no-fallback checks.
  - Non-goals: no Foundry Spark/Snowflake/Databricks/BigQuery execution as ShardLoom runtime; no
    production Foundry claim from local dev-stack proof; no Compute Module until REST/control plane
    exists.
  - Claim boundary: may claim only the specific Foundry workflow/package path proven in a real
    Foundry environment.
  - Fallback boundary: Foundry virtual tables and external systems are governed handles or
    external-compute boundaries, never fallback execution.
  - Ledger rule: ledger entry must include package/version proof, Foundry environment evidence,
    dataset refs, governance artifacts, and deferred platform claims.

### Remaining work snapshot

| Status | Work | Next decision |
| --- | --- | --- |
| Closed | `RELEASE-PACKAGE-15` | Completed in the ledger with clean-source benchmark publication evidence for source revision `74a2e7d4f77eed0686971518e010463da26f2cdf`; no autonomous implementation item remains. |
| Historical | PR #1174 benchmark row/readiness context, repo-wide audit closeout, release-sequence closeout, and completed benchmark/profile, sub-evidence, user-surface proof | Preserved in `docs/architecture/phased-execution-completed-ledger.md`; do not treat as active work. |
| Current evidence | `full_local` benchmark refresh | Promoted website benchmark bundle generated `2026-06-13T11:33:10Z` from source revision `5743638a9225f479a0096f1c6db51a0068cac68f`; `performance_claim_allowed=false`; use for freshness and optimization direction only. |
| Mapped, not autonomous queue | Unchecked global architecture review rows | Governed by `docs/architecture/global-architecture-review.md` and `docs/architecture/runtime-gap-family-burn-down.md`; promote concrete implementation items here before work begins. |
| Deferred approval/artifact gate | Public release/package approval | Clean local Conda proof, dependency/security/package local-gate evidence, and current benchmark-publication evidence now pass locally; remaining blockers are package-channel approval/proof, publication/API/schema stability approval, and per-claim evidence promotion before any public claim. |

Deferred Non-Runtime Closeout Queue: closed for the prior cleanup batch. Completed non-runtime history
lives in `docs/architecture/phased-execution-completed-ledger.md`; newly reviewed v1
product/release work is now represented by concrete unchecked items above.

### Evidence Pointers

- Current benchmark timing snapshot and PR #1174 route/readiness context are preserved in the
  completed ledger entry `Phase-plan open-queue cleanup and completed-state ledger migration`.
- Performance route, stage, and timing-surface contracts live in
  `docs/architecture/performance-attribution-and-execution-structure.md`.
- Current source/input evidence contracts live in `docs/architecture/universal-input-contract.md`.
- Benchmark artifacts are evidence and optimization direction only:
  `performance_claim_allowed=false`, no Spark-displacement/superiority claim, no package-release
  claim, and no public freshness claim outside the promoted manifest source revision and validation
  evidence being cited.

### Reopen Policy

- Completed `PERF-DESIGN-*` items may return to Planned only as explicit `*R` optimization passes
  when current benchmark rows, validator output, or targeted local simulation identify a measured
  bottleneck.
- A reopened `*R` item must preserve the original closeout contract and add a narrower optimization
  contract: control surface, timing rows/fields proving it is still worth changing, fail-closed
  blocker vocabulary, and benchmark/test evidence.
- Use dynamic admission for repeated dependency/source decisions, PulseWeave for run-local
  coalescing and bounded work-in-progress, and capillary windows for small typed
  source/preparation/sink work units only where the bottleneck shape justifies those controls.
- Current direct open implementation items are the v1 product/release queue, remaining
  `PERF-RUNTIME-*` optimization items, and v1-candidate production-family rows above. Reopen
  completed `PERF-DESIGN-*` or `PERF-DESIGN-*R` passes only with new current artifact, validator,
  CI, UAT simulation, or maintainer-review evidence.

### Global Architecture Review Carry-Forward

- Runtime gap-family burn-down and validator mapping still own historical/global references:
  `GAR-RUNTIME-IMPL-6E` automatic dynamic preparation,
  `GAR-RUNTIME-IMPL-6F` output/fanout conversion,
  `GAR-RUNTIME-IMPL-4R/5O` effectful-operation local fixture/admission closeout,
  `GAR-RUNTIME-IMPL-4D/5G` expression/operator closeout plus `GAR-RUNTIME-IMPL-4D-F1`,
  `GAR-RUNTIME-IMPL-4D-F2` complex dtype,
  `GAR-RUNTIME-IMPL-4D-F3` advanced predicate/subquery, `GAR-RUNTIME-IMPL-6A`, and closed 6D
  runtime breadth families.
- Phase strings retained for routing and validator compatibility:
  `GAR-RUNTIME-IMPL-6D:last_order.broad_sql_grammar`,
  `GAR-RUNTIME-IMPL-6D:last_order.python_dataframe_api_breadth`,
  `GAR-RUNTIME-IMPL-6A compute-engine completion gate and residual blocker burn-down`,
  `GAR-RUNTIME-IMPL-6D:last_order.object_store_lakehouse_runtime`,
  `GAR-RUNTIME-IMPL-6D:last_order.generated_output_platform_runtime`,
  `GAR-RUNTIME-IMPL-6D:last_order.front_door_performance_benchmark_publication`,
  `GAR-RUNTIME-IMPL-6D:last_order.effectful_operations`,
  `GAR-RUNTIME-IMPL-6D:last_order.live_hybrid_runtime`, and
  `GAR-RUNTIME-IMPL-6D:last_order.distributed_spill_oom_runtime`.

### Guardrails

- No Spark, DataFusion, DuckDB, Polars, Velox, Trino, Dask, Ray, pandas, PyArrow, or another engine
  may execute unsupported ShardLoom work as fallback.
- Vortex is the highest-fidelity native input/output target.
- Compatibility inputs and outputs are explicit translation/admission surfaces, not execution
  fallback.
- Unsupported behavior must fail explicitly with deterministic diagnostics.
- Do not make performance, production, package, Spark-displacement, superiority, object-store,
  Foundry, REST, live/hybrid, SQL/DataFrame, or public release claims without the required
  workload-scoped evidence and approval gates.
- Benchmark route analysis must group by `(route_lane_id, timing_surface)` and honor
  `route_timing_stage_inclusion_classes`; diagnostic stage fields must not silently redefine hot
  runtime totals.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.

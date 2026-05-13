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

### Near-term Implementation Priority

Completed checked-off work that used to live in this section is recorded in
`docs/architecture/phased-execution-completed-ledger.md`. Keep this section focused on remaining
actionable work.

Execution slice rule for autonomy: parent priority checkboxes stay unchecked until every child
bundle under that priority is complete. Work proceeds from the first unchecked child bundle, but PRs
should be large enough to ship a usable command/API/report surface with schema, tests, smoke
commands, and docs. Current large-slice order is P6.4, P6.5, P6.6, P7.1-P7.3, P8.1-P8.3, then
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
- [ ] Priority 6 - CG-23 REST, event, and remote API surface
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
  - [ ] P6.5 security, governance, observability, and agent API bundle.
    - User-visible surface: local-only, token, mTLS, OIDC, and service-account auth posture; scopes
      for read, plan, execute, write, cancel, admin, benchmark, migration, and agent operations;
      audit policy; MCP resources/tools for safe agent discovery.
    - Acceptance: credentials stay references, secrets are redacted, destructive operations require
      explicit policy, and MCP tools remain dry-run/explain/estimate/certify by default. OpenTelemetry
      traces/metrics/logs, OpenLineage facets, problem-details errors, CloudEvents, and certificate
      refs map into one evidence model.
    - Verification: policy schema tests, redaction snapshots, audit fixture checks, MCP contract
      fixtures, and no-external-effect diagnostics.
  - [ ] P6.6 columnar data-plane and ecosystem standards boundary bundle.
    - User-visible surface: optional Flight/ADBC posture, large-payload transfer policy, and standards
      classification for Iceberg REST Catalog, Polaris, Gravitino, Delta Sharing, Substrait,
      WASI/WebAssembly components, NATS JetStream, Redpanda, Kafka-compatible systems, Paimon,
      Fluss, and similar systems.
    - Acceptance: REST remains the control plane and proof surface; Flight/ADBC is optional and never
      required for basic local use or import; all transfers declare materialization, fidelity, result
      policy, and no-fallback status.
    - Verification: standards matrix snapshots, decoded-columnar boundary assertions, optional
      dependency posture tests, and full protocol compatibility validation.
- [ ] Priority 7 - CG-21/CG-22/CG-23 integrated certification closeout
  - Outcome: prove that workflow UX, engine-mode evidence, and remote/API posture agree across CLI,
    Python, and API contracts before any broader support claim is made.
  - Slice rule: group closeout work by proof surface, not by source file. A slice must improve a
    user's ability to understand what can be run, what is blocked, and what evidence is missing.
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

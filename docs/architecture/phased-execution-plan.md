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
- [ ] Priority 4 - CG-21 user-testable workflow and ETL execution lane
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
- [ ] Priority 5 - CG-22 three-engine certified data execution fabric
  - [ ] CG-22A engine-mode contract surface
    - [ ] Add `EngineMode` values `batch`, `live`, `hybrid`, and `auto`.
    - [ ] Add `Boundedness`, `UpdateMode`, and `OutputMode` vocabulary and capability discovery.
    - [ ] Emit `EngineSelectionReport` with requested, allowed, and selected engine modes plus
          rejection reasons.
    - [ ] Preserve `external_engine_invoked=false` and `fallback_attempted=false` for every internal
          engine choice.
  - [ ] CG-22B per-engine capability matrix
    - [ ] Track operator/function/source/sink support separately for batch, live, and hybrid.
    - [ ] Distinguish bounded snapshot support, append-only stream support, upsert/delete/tombstone
          support, changelog support, and continuous materialized view support.
    - [ ] Block live/hybrid claims for unsupported global sort, unbounded join, state, checkpoint,
          or output modes.
  - [ ] CG-22C live source/change contract
    - [ ] Define ShardLoom-native `ChangeRecord` with key, operation, sequence, event time,
          processing time, source offset, schema digest, and payload reference.
    - [ ] Add append/upsert/delete/retract/tombstone semantics, watermark policy, late-data policy,
          state TTL, checkpoint policy, and output changelog vocabulary.
    - [ ] Keep broker/runtime integrations as adapters or future dependencies, not core fallback
          execution.
  - [ ] CG-22D narrow in-memory live prototype
    - [ ] Start with fixture-backed bounded streams for filter, project, count, count_where, and
          simple group_count.
    - [ ] Emit state, checkpoint, watermark, lag, output changelog, execution certificate, Native
          I/O certificate, and no-fallback evidence.
    - [ ] Avoid calling file polling real streaming until state, watermark, checkpoint, and recovery
          semantics exist.
  - [ ] CG-22E hybrid base plus delta overlay
    - [ ] Combine a local Vortex base with fixture-backed hot deltas, tombstones, deletion vectors,
          snapshot epoch, and certified merged result.
    - [ ] Emit `DeltaOverlayCertificate`, `HotColdContributionReport`, snapshot certificate refs,
          base snapshot id, hot changelog range, warm/cold segment counts, tombstone counts,
          freshness lag, and no-fallback evidence.
    - [ ] Keep object-store/table production claims blocked until CG-4/CG-9/CG-10 evidence exists.
  - [ ] CG-22F Vortex micro-segment flush
    - [ ] Flush hot append/upsert batches into Vortex micro-segments with local manifest and
          recovery evidence.
    - [ ] Preserve representation, statistics, deletion/tombstone, checkpoint, commit, and Native
          I/O certificate fields.
    - [ ] Keep compaction and table maintenance separate until commit/recovery paths are certified.
  - [ ] CG-22G compaction and layout-health planner
    - [ ] Plan compaction from small-segment pressure, tombstone pressure, partition skew, stale
          statistics, and layout health.
    - [ ] Produce compaction recommendations without executing maintenance until
          write/commit/recovery support is ready.
  - [ ] CG-22H Python/API engine UX
    - [ ] Surface `engine="batch"`, `engine="live"`, `engine="hybrid"`, and `engine="auto"`
          consistently in Python, CLI, and later REST.
    - [ ] Explain why an engine is selected or rejected, including freshness, consistency,
          boundedness, state, memory, sink, and unsupported-feature reasons.
  - [ ] CG-22I state, checkpoint, and freshness certification
    - [ ] Add `FreshnessCertificate`, `StateCertificate`, and `ContinuousViewCertificate` fields for
          watermarks, checkpoint ids, state bytes, changelog offsets, recovery status, lag, output
          mode, and exactly-once/idempotency blockers.
    - [ ] Do not claim exactly-once, freshness, recovery, or continuous-view correctness without
          CG-4/CG-5/CG-8/CG-16 evidence.
- [ ] Priority 6 - CG-23 REST, event, and remote API surface
  - [ ] CG-23A REST API contract surface
    - [ ] Define OpenAPI 3.2 or approved-successor contract files for `/v1` resources before server
          behavior.
    - [ ] Represent health, version, capabilities, adapters, sources, sinks, plans, queries,
          results, certificates, profiles, benchmarks, migration, lineage, and governance resources.
    - [ ] Require engine mode, fallback policy, materialization policy, result policy, and evidence
          policy on execution-capable requests.
    - [ ] Include `fallback_attempted=false` or explicit unsupported/failure reason in every
          response.
    - [ ] Add an API maturity ladder from declared contract through discovery, plan/explain, local
          certified batch lifecycle, result delivery, source/sink certification, live/hybrid events,
          security/governance, and production-certified workload support.
  - [ ] CG-23B REST discovery server
    - [ ] Add optional local `shardloom serve --mode discovery` only after dependency and security
          approval.
    - [ ] Serve health, version, capabilities, adapters, deployment readiness, and no-dataset smoke
          checks.
    - [ ] Prohibit dataset probing, object-store access, catalog access, query execution, and
          fallback in discovery mode.
  - [ ] CG-23C plan/explain/validate API
    - [ ] Add plan handles and validate/explain/estimate/certification-preview endpoints.
    - [ ] Return parser/binder/native logical/native physical/execution/certification stages
          separately.
    - [ ] Use deterministic unsupported diagnostics and problem-details errors without execution
          delegation.
  - [ ] CG-23D async query lifecycle API
    - [ ] Add execute/status/cancel/retry/profile/certificates/lineage/results/artifacts lifecycle
          for already-certified local batch paths first.
    - [ ] Keep non-certified paths blocked or explicitly uncertified.
    - [ ] Link result handles to execution certificates, Native I/O certificates, materialization
          reports, profile reports, and no-fallback evidence.
  - [ ] CG-23E result delivery and spooling
    - [ ] Support inline JSON only for tiny previews and diagnostics.
    - [ ] Add paged JSON and JSON Lines for row-oriented small/medium result or log streams.
    - [ ] Treat Arrow IPC as explicit decoded-columnar boundary unless a future native boundary
          proves otherwise.
    - [ ] Prefer Vortex artifacts or object references for highest-fidelity large analytical
          outputs.
    - [ ] Define result TTL, retention, cleanup, artifact refs, representation state,
          materialization, and fidelity fields.
  - [ ] CG-23F live/hybrid event API
    - [ ] Use SSE for one-way progress events and WebSocket only where bidirectional live
          interaction is required.
    - [ ] Define AsyncAPI event contracts and CloudEvents-style envelopes for progress, state,
          checkpoint, watermark, certificate, lineage, benchmark, and hybrid hot/cold contribution
          events.
    - [ ] Block live/hybrid API certification until CG-22, CG-8, CG-4, and CG-16 evidence exists.
  - [ ] CG-23G security, governance, and API policy
    - [ ] Define local-only, token, mTLS, OIDC, and service-account auth modes before remote
          execution.
    - [ ] Separate scopes for read, plan, execute, write, cancel, admin, benchmark, migration, and
          agent operations.
    - [ ] Keep credentials as references, redact secrets, require explicit destructive-operation
          policies, and audit plan/execute/write/cancel/certify.
  - [ ] CG-23H Flight/ADBC and columnar data-plane bridge
    - [ ] Keep REST as control plane while allowing future Flight tickets or ADBC endpoints for
          high-throughput columnar transfer.
    - [ ] Make Flight/ADBC optional and never required for basic local use or import.
    - [ ] Report Arrow transfers as decoded-columnar materialization unless later certified
          otherwise.
  - [ ] CG-23I MCP agent API
    - [ ] Expose capabilities, schemas, plans, certificates, benchmark reports, and diagnostics as
          safe agent resources.
    - [ ] Keep MCP tools dry-run/explain/estimate/certify by default.
    - [ ] Require explicit policy and credentials for execute, write, cancel, benchmark, and
          migration operations.
    - [ ] Preserve no-fallback and external-effect diagnostics in agent-facing responses.
  - [ ] CG-23J remote standards, lineage, and ecosystem interop posture
    - [ ] Map OpenTelemetry traces/metrics/logs, OpenLineage runs/jobs/datasets/facets,
          problem-details errors, CloudEvents, and certificate refs into a single API evidence
          model.
    - [ ] Treat Iceberg REST Catalog, Polaris, Gravitino, Delta Sharing, Substrait, WASI/WebAssembly
          components, NATS JetStream, Redpanda, Kafka-compatible systems, Paimon, Fluss, and similar
          systems as standards/reference/adapter candidates until a dependency decision and
          capability contract approve runtime use.
    - [ ] Keep REST as control plane and proof surface; large payload transfer must use explicit
          result policies such as Vortex artifact refs, object refs, Arrow IPC boundaries, JSON
          Lines, or future Flight/ADBC tickets.
    - [ ] Do not let remote API support weaken local Python/CLI protocol parity, no-fallback
          execution, materialization reporting, or governance policy.
- [ ] Priority 7 - CG-21/CG-22/CG-23 integrated certification closeout
  - [ ] Add cross-CG capability snapshots proving CG-21 workflow, CG-22 engine mode, and CG-23
        remote API states are visible through capability discovery.
  - [ ] Add cross-CG unsupported diagnostics showing the same blocker across CLI, Python, and future
        REST.
  - [ ] Add workload-scoped certification dossiers that combine CG-5 correctness, CG-6 benchmarks,
        CG-16 execution certificates, CG-19 Native I/O certificates, CG-20 capability evidence,
        CG-21 workflow evidence, CG-22 engine evidence, and CG-23 API evidence.
  - [ ] Keep CG-21, CG-22, and CG-23 logically after the current planned CG-1 through CG-20 work
        unless a later implementation item is explicitly pulled forward as a contract/report-only
        lane.
  - [ ] Preserve no-runtime, no-dependency, no-fallback, and no-claim posture for docs/report-only
        synthesis.
- [ ] Priority 8 - general availability and external proof-of-use
  - [ ] Define public release identity and versioning policy for PyPI `shardloom`, conda-forge
        `shardloom-cli`, `shardloom-python`, `shardloom` metapackage, GitHub Release artifacts,
        GHCR/OCI image posture, and selected crates.io protocol/client crates.
  - [ ] Add release workflow contracts for Git tag, source archive, platform binaries, Python
        wheel/sdist, Conda recipe/feedstock status, checksums, SBOM, artifact attestation,
        changelog, compatibility matrix, known unsupported paths, and no-fallback release checks.
  - [ ] Prefer PyPI trusted publishing/OIDC and prohibit long-lived package tokens in release
        automation unless explicitly approved by maintainers.
  - [ ] Keep package publication, release tags, feedstock submission, crates.io publication, OCI
        pushes, and Marketplace publication human-approved and release-gated.
  - [ ] Make Conda the primary "it just works" path by proving clean-environment installs for
        `shardloom-cli`, `shardloom-python`, and `shardloom` metapackage with CLI binary resolution
        and `fallback_attempted=false` smoke evidence.
  - [ ] Add the public first-10-minutes proof: `conda install shardloom`, `import shardloom`,
        `ShardLoomClient.from_env().smoke_check()`, `client.capabilities()`, `shardloom status
        --format json`, and `shardloom capabilities --format json`.
  - [ ] Add external proof examples with README, environment file, input fixture, expected output,
        expected certificate fields, and known limitations for `examples/local-python-smoke/`,
        `examples/local-vortex-benchmark/`, and `examples/foundry-lightweight-transform/`.
  - [ ] Publish user-facing docs for install, quickstart, Python client, CLI, Conda packages,
        Foundry usage, benchmarking, certificates, no-fallback policy, Vortex compatibility,
        maturity statuses, and unsupported diagnostics.
  - [ ] Keep benchmark extras, Spark/DataFusion/DuckDB/Polars/pandas baselines, and optional
        comparison tooling out of the core install path and report them as baselines only.
- [ ] Priority 9 - RFC 0036 Foundry integration pack and platform availability
  - [ ] Treat Foundry as an optional integration pack, not the primary engine target and not a new
        core engine gate.
  - [ ] Add `shardloom-foundry` helper package posture for deterministic `SHARDLOOM_BIN` resolution,
        Foundry transform metadata capture, input/output RID capture, certificate output writing,
        benchmark metrics writing, staging/materialization reports, and no-fallback propagation
        without adding execution semantics.
  - [ ] Add the Foundry maturity ladder: `F0` declared only, `F1` package/import in Code Repository,
        `F2` smoke transform with CLI resolution, `F3` dataset source/sink staging with certificate
        output, `F4` Data Expectations/Data Health bridge, `F5` lineage and transaction/branch
        evidence, `F6` virtual-table/external-compute boundary awareness, `F7` Marketplace starter
        product, `F8` Compute Module/REST service, `F9` Ontology/AIP/Workshop integration, and `F10`
        workload-certified Foundry deployment.
  - [ ] Add `FoundryExecutionContext`, `FoundryDatasetTransactionReport`,
        `FoundryBranchContextReport`, `FoundryPreviewModeReport`, and
        `FoundryReleaseReadinessReport` surfaces so certificates identify transform, branch,
        preview/build/incremental mode, transactions, package versions, workload constitution, and
        expected evidence.
  - [ ] Add `FoundryDatasetSource`, `FoundryDatasetSink`, and `FoundryCertificateOutput` schema
        surfaces for staged local files, table-compatible outputs, certificate/metrics datasets,
        optional Vortex artifact sidecars, materialization/fidelity reports, commit/recovery status,
        and `fallback_attempted=false`.
  - [ ] Add `FoundryIncrementalRunReport` aligning Foundry incremental builds with ShardLoom
        batch/live/hybrid evidence without treating Foundry incremental mode as live/hybrid
        certification by itself.
  - [ ] Add `FoundryDataHealthBridge` and Data Expectations mapping for certificate presence,
        no-fallback status, Native I/O evidence, schema digest, output row requirements,
        data-quality checks, materialization policy, and benchmark-claim blockers.
  - [ ] Add `FoundryLineageFacet`, `FoundryScheduleBuildReport`, and
        `FoundryDataConnectionBoundaryReport` for datasets, virtual tables, media sets, artifacts,
        schedules, syncs, exports, webhooks, external transforms, credential refs, egress policy,
        and ShardLoom role classification.
  - [ ] Add `FoundryS3DatasetAdapter` posture for future S3-compatible dataset access with dataset
        RID, branch, object key, range-read support, multipart/write support where allowed,
        bytes/request counts, credential mode, and Native I/O certificates.
  - [ ] Add `FoundryVirtualTableSource`, `FoundryVirtualTableSink`, and `FoundryVirtualTableRef`
        surfaces so Snowflake, Databricks, BigQuery, S3, ADLS, GCS, Iceberg, and similar virtual
        tables are governed external handles with metadata, staging, update-detection, security, and
        materialization policy.
  - [ ] Classify Snowflake/Databricks/BigQuery/Foundry Spark/Snowpark/Databricks Connect/Ibis
        compute pushdown through `FoundryExternalComputeBoundaryReport` as baseline, oracle,
        migration reference, or prohibited fallback, never as ShardLoom-native execution.
  - [ ] Add `FoundryIcebergTableSource` and `FoundryIcebergTableSink` posture for catalog/table
        metadata, snapshot/manifest awareness, schema/partition evidence, compatibility reads,
        `TranslationReport` requirements, and commit/recovery evidence.
  - [ ] Add `FoundryMediaSetSource` and `FoundryMediaSetSink` posture for media item refs,
        MIME/schema, OCR/extraction/model/materialization boundaries, provenance/confidence,
        incremental media status, redaction, and explicit no silent
        OCR/transcription/embedding/model calls.
  - [ ] Add Foundry Ontology, Functions, AIP Logic, model, and scenario report-first surfaces:
        `FoundryOntologyMappingReport`, `FoundryFunctionSurface`, `FoundryAipLogicBridge`,
        `FoundryModelBoundaryReport`, and `FoundryScenarioBoundaryReport`.
  - [ ] Add BYOC and Compute Module posture through `FoundryByocImageReport`,
        `FoundryComputeModuleSurface`, and `FoundryComputeModuleReadinessReport`, keeping Compute
        Modules blocked until CG-23 API/security/package evidence exists.
  - [ ] Add `FoundryGovernanceBoundaryReport` for markings, organizations, inherited markings,
        certificate visibility, redaction, export policy, agent visibility, and artifact safety.
  - [ ] Add `FoundryMarketplaceStarterProduct` as an adoption artifact with Conda dependency
        instructions, smoke transform, benchmark transform, certificate output dataset, Data
        Expectations bridge, optional virtual-table staging example, optional external-compute
        baseline example, optional Compute Module API example, schedule, and docs.
  - [ ] Add Foundry benchmark schema and lanes that label ShardLoom lightweight, Polars lightweight,
        DataFusion/DuckDB baseline, Spark distributed, and Snowflake/Databricks/BigQuery pushdown
        rows separately with compute mode, materialization boundary, certificates, correctness
        digest, and versions.
  - [ ] Preserve the central Foundry rule: virtual tables and external compute are first-class
        workflow handles and comparison boundaries, but ShardLoom-native execution requires
        staged/native data plus certificates; no Snowflake/Databricks/BigQuery/Spark/Foundry compute
        pushdown may be reported as ShardLoom execution.

## Completed

Detailed completed session and historical phase ledgers live in
`docs/architecture/phased-execution-completed-ledger.md`.

Keep this section as a pointer only so this file remains the compact autonomous Planned queue. After
a session or merge completes, add the detailed completed block to the ledger file, not below this
pointer.
